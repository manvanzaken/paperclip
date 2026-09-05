"""Risk: manual halt, blacklists and strikes, mismatch guard, balances, funding gate.

Rules of this module:
- `entry_allowed` never raises and fails CLOSED on unknown input (unknown or quote-only venue,
  missing/stale balance cache in live mode); every rejection has a stable reason string that the
  strategy counts in the funnel.
- There is no automatic kill switch in v1 (user decision). Halting is manual: `stop.flag`/`start.flag`
  in DATA_DIR (edge-triggered, consumed on read, stop wins over a simultaneous start) or Telegram
  /stop and /start. The halt survives restarts through the persisted `halted` flag, not the files.
- One position per symbol is NOT gated here: the app drives a held symbol's position instead of
  re-evaluating it (`App.on_quote`), so a held symbol never reaches `entry_allowed`.
"""
from __future__ import annotations

import logging
import math
import time
from pathlib import Path

from .config import Config
from .models import Position

log = logging.getLogger("bbo.risk")

RECENT_OUTCOMES_KEEP = 20          # per route; the win-rate gate reads the ones inside pair_stats_window_s
FUNDING_STALE_GRACE_S = 60.0       # a settle stamp this far in the past is a late poll, further back a dead feed
BALANCE_HEADROOM = 1.05            # notional × 1.05 must be available (conservative: leverage is not modelled)


def pair_key(symbol: str, venue_a: str, venue_b: str) -> str:
    return f"{symbol}|{venue_a}>{venue_b}"


def route_key(symbol: str, venue_x: str, venue_y: str) -> str:
    """Direction-free key for a venue pair (mismatch guard)."""
    return f"{symbol}|" + "|".join(sorted((venue_x, venue_y)))


# ---- tolerant state parsers: a hand-edited or version-skewed `risk` section must never crash start-up ----
def _float_map(raw: object, now: float | None = None) -> dict[str, float]:
    """`{key: float}`; drops unparsable entries and, when `now` is given, expired ones."""
    out: dict[str, float] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        try:
            t = float(v)
        except (TypeError, ValueError):
            log.warning("RISK_STATE_DROP %r=%r", k, v)
            continue
        if math.isfinite(t) and (now is None or t > now):
            out[str(k)] = t
    return out


def _strikes_from(raw: object, now: float, decay_s: float) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        try:
            if isinstance(v, dict):
                n, ts = int(v.get("n", 0)), float(v.get("ts", now))
            else:
                n, ts = int(v), now            # legacy bare count: starts decaying from this load
        except (TypeError, ValueError, OverflowError):   # OverflowError: int(inf) from a JSON `Infinity`
            log.warning("RISK_STATE_DROP pair_strikes %r=%r", k, v)
            continue
        if n > 0 and math.isfinite(ts) and now - ts <= decay_s:   # an inf/NaN stamp would make a strike immortal
            out[str(k)] = {"n": n, "ts": ts}
    return out


def _stats_from(raw: object) -> dict[str, dict]:
    """One bad `recent` entry drops that entry, not the route (dropping the route would unblock it)."""
    out: dict[str, dict] = {}
    if not isinstance(raw, dict):
        return out
    for k, v in raw.items():
        try:
            recent = []
            raw_recent = v.get("recent") or []
            if not isinstance(raw_recent, (list, tuple)):
                log.warning("RISK_STATE_DROP pair_stats %r recent %r", k, raw_recent)
                raw_recent = []
            for item in raw_recent:
                try:
                    ts, won = item
                    ts = float(ts)
                    if math.isfinite(ts):
                        recent.append([ts, bool(won)])
                    else:
                        log.warning("RISK_STATE_DROP pair_stats %r recent %r", k, item)
                except (TypeError, ValueError, OverflowError):
                    log.warning("RISK_STATE_DROP pair_stats %r recent %r", k, item)
            total = float(v.get("total_pnl", 0.0))
            out[str(k)] = {"wins": int(v.get("wins", 0)), "losses": int(v.get("losses", 0)),
                           "total_pnl": total if math.isfinite(total) else 0.0, "recent": recent[-RECENT_OUTCOMES_KEEP:]}
        except (TypeError, ValueError, AttributeError, OverflowError):
            log.warning("RISK_STATE_DROP pair_stats %r=%r", k, v)
    return out


def _mismatch_from(raw: object) -> dict[str, dict]:
    """Accepts the legacy bare list of keys or the current `{key: {ts, spread_pct, tier}}`. A key is
    never dropped over a bad detail: the blacklist is a safety list."""
    if isinstance(raw, list):
        return {str(k): {"ts": 0.0, "spread_pct": 0.0, "tier": "legacy"} for k in raw}
    out: dict[str, dict] = {}
    if isinstance(raw, dict):
        for k, v in raw.items():
            try:
                out[str(k)] = {"ts": float(v.get("ts", 0.0)), "spread_pct": float(v.get("spread_pct", 0.0)),
                               "tier": str(v.get("tier", ""))}
            except (TypeError, ValueError, AttributeError):
                out[str(k)] = {"ts": 0.0, "spread_pct": 0.0, "tier": "legacy"}
    return out


class MismatchGuard:
    """Same-ticker-different-asset detector: |raw mid spread| stays absurd for N consecutive quotes.
    Two tiers: slow (moderately absurd for long) and fast (grossly absurd for a few quotes). A hit is
    persistent for the deployment (spec) and records when/why so the dashboard can show it; lift one
    by hand by removing its key from `mismatch_blacklist` in the state file."""

    def __init__(self, slow_pct: float, slow_n: int, fast_pct: float, fast_n: int,
                 blacklisted: dict[str, dict] | None = None, clock=time.time):
        if not (slow_pct > 0.0 and fast_pct > 0.0) or slow_n < 1 or fast_n < 1:
            raise ValueError(f"mismatch thresholds must be positive: slow {slow_pct}%x{slow_n}, fast {fast_pct}%x{fast_n}")
        self.slow_pct, self.slow_n, self.fast_pct, self.fast_n = slow_pct, slow_n, fast_pct, fast_n
        self.blacklisted: dict[str, dict] = dict(blacklisted or {})   # key -> {"ts", "spread_pct", "tier"}
        self.clock = clock
        self._slow: dict[str, int] = {}
        self._fast: dict[str, int] = {}

    def observe(self, key: str, raw_spread_pct: float) -> bool:
        """Returns True when this observation newly blacklists the pair (NaN never counts as absurd)."""
        if key in self.blacklisted:
            return False
        a = abs(raw_spread_pct)
        self._slow[key] = self._slow.get(key, 0) + 1 if a > self.slow_pct else 0
        self._fast[key] = self._fast.get(key, 0) + 1 if a > self.fast_pct else 0
        tier = "fast" if self._fast[key] >= self.fast_n else "slow" if self._slow[key] >= self.slow_n else ""
        if not tier:
            return False
        self.blacklist(key, raw_spread_pct, tier)
        return True

    def blacklist(self, key: str, spread_pct: float = 0.0, tier: str = "manual") -> None:
        self.blacklisted[key] = {"ts": self.clock(), "spread_pct": spread_pct, "tier": tier}
        self._slow.pop(key, None)
        self._fast.pop(key, None)
        log.warning("MISMATCH_BLACKLIST %s spread=%.2f%% tier=%s", key, spread_pct, tier)

    def is_blacklisted(self, key: str) -> bool:
        return key in self.blacklisted


class RiskManager:
    def __init__(self, cfg: Config, clock=time.time):
        self.cfg = cfg
        self.clock = clock
        self._venues = {v.name: v for v in cfg.venues}
        self.halted = False
        self.halt_reason = ""
        self.pair_strikes: dict[str, dict] = {}         # key -> {"n": strikes, "ts": last strike}
        self.pair_blacklist: dict[str, float] = {}      # key -> until ts
        self.symbol_blacklist: dict[str, float] = {}    # symbol -> until ts
        self.cooldowns: dict[str, float] = {}           # symbol -> until ts
        self.venue_symbol_blacklist: set[str] = set()   # "venue|symbol"
        self.balances: dict[str, dict] = {}             # venue -> {"available","total","locked","ts"}
        self.funding: dict[str, tuple[float, float]] = {}  # "venue|symbol" -> (rate fraction, next_settle_ts)
        self.stale_funding: set[str] = set()            # "venue|symbol" whose settle stamp is in the past (dead feed)
        self.pair_stats: dict[str, dict] = {}           # key -> {"wins","losses","total_pnl","recent": [[ts, won],..]}
        self.mismatch = MismatchGuard(cfg.mismatch_slow_pct, cfg.mismatch_slow_n,
                                      cfg.mismatch_fast_pct, cfg.mismatch_fast_n, clock=clock)
        self._dead_flags: dict[Path, tuple[int, int]] = {}   # undeletable flag -> (inode, mtime_ns): ignored until replaced

    # ---- halt ----------------------------------------------------------------
    def halt(self, reason: str) -> None:
        self.halted, self.halt_reason = True, reason
        self._consume_flags(self.cfg.halt_flag, self.cfg.resume_flag)   # stop wins: a pending start is void

    def resume(self) -> None:
        self.halted, self.halt_reason = False, ""
        self._consume_flags(self.cfg.resume_flag)   # only the start flag: a stop.flag written meanwhile is still honoured

    def _flag_path(self, name: str) -> Path:
        return self.cfg.data_dir / name

    @staticmethod
    def _signature(p: Path) -> tuple[int, int] | None:
        try:
            st = p.lstat()             # lstat: a dangling symlink named stop.flag is still a stop
        except OSError:
            return None
        return st.st_ino, st.st_mtime_ns

    def _flag_present(self, p: Path, files_only: bool) -> bool:
        """`files_only=False` (stop): any path counts — a directory named stop.flag still means stop.
        `files_only=True` (start): only a regular file resumes; anything else is logged once and ignored."""
        try:
            sig = self._signature(p)
            if sig is None:
                self._dead_flags.pop(p, None)
                return False
            if p in self._dead_flags:
                if sig == self._dead_flags[p]:
                    return False       # the very path we could not unlink: keep ignoring it
                del self._dead_flags[p]    # replaced: treat the new one as a fresh flag
            if files_only and not p.is_file():
                log.error("FLAG_NOT_A_FILE %s is not a regular file — ignored until replaced", p)
                self._dead_flags[p] = sig
                return False
            return True
        except OSError as e:
            log.error("FLAG_CHECK_FAILED %s: %s", p, e)
            return False

    def _consume_flags(self, *names: str) -> None:
        for name in names:
            p = self._flag_path(name)
            try:
                p.unlink()
            except FileNotFoundError:
                pass
            except OSError as e:
                log.error("FLAG_UNLINK_FAILED %s: %s — ignoring this flag until it is removed by hand", p, e)
                sig = self._signature(p)
                if sig is not None:
                    self._dead_flags[p] = sig

    def check_flags(self) -> str | None:
        """Dashboard/operator flags in DATA_DIR, edge-triggered and consumed on read: `stop.flag` halts,
        `start.flag` resumes; both present → stop wins. Returns "halt" / "resume" on a transition,
        "halt_noop" / "resume_noop" when the flag asked for the state we are already in, else None."""
        has_stop = self._flag_present(self._flag_path(self.cfg.halt_flag), files_only=False)
        has_start = self._flag_present(self._flag_path(self.cfg.resume_flag), files_only=True)
        if not has_stop and not has_start:
            return None
        if has_stop:
            if has_start:
                log.warning("FLAGS %s and %s both present — stop wins", self.cfg.halt_flag, self.cfg.resume_flag)
            if self.halted:
                self._consume_flags(self.cfg.halt_flag, self.cfg.resume_flag)
                return "halt_noop"
            self.halt("stop.flag")
            return "halt"
        if not self.halted:
            self._consume_flags(self.cfg.resume_flag)
            return "resume_noop"
        self.resume()
        return "resume"

    # ---- gates ---------------------------------------------------------------
    def venue_symbol_blocked(self, venue: str, symbol: str) -> bool:
        if f"{venue}|{symbol}" in self.venue_symbol_blacklist:
            return True
        vc = self._venues.get(venue)
        if vc is None or vc.role != "trade":
            return True                # unknown or quote-only venue: fail closed, never raise from a gate
        return bool(vc.symbol_whitelist) and symbol not in vc.symbol_whitelist

    def _pair_win_rate_blocks(self, key: str, now: float) -> bool:
        st = self.pair_stats.get(key)
        if not st:
            return False
        recent = [won for ts, won in st.get("recent", []) if now - ts <= self.cfg.pair_stats_window_s]
        if len(recent) < self.cfg.pair_min_trades:
            return False
        return sum(1 for won in recent if won) / len(recent) < self.cfg.pair_min_win_rate

    def entry_allowed(self, symbol: str, venue_a: str, venue_b: str, size_usd: float,
                      open_count: int) -> tuple[bool, str]:
        """Cheap/global gates first, then per-route ones. The balance gate compares USDT `available`
        against NOTIONAL × 1.05 (leverage is not modelled: conservative). In live mode a missing or
        stale balance cache fails closed as "balance_unknown"; paper mode stays permissive."""
        now = self.clock()
        if self.halted:
            return False, "halted"
        if venue_a == venue_b or not size_usd > 0.0:
            return False, "invalid"
        if open_count >= self.cfg.max_concurrent:
            return False, "max_concurrent"
        if symbol in self.cfg.blocked_symbols:
            return False, "blocked_symbol"
        if self.cooldowns.get(symbol, 0.0) > now:
            return False, "cooldown"
        if self.symbol_blacklist.get(symbol, 0.0) > now:
            return False, "symbol_blacklist"
        for v in (venue_a, venue_b):
            if self.venue_symbol_blocked(v, symbol):
                return False, "venue_blocked"
        key = pair_key(symbol, venue_a, venue_b)
        if self.pair_blacklist.get(key, 0.0) > now:
            return False, "pair_blacklist"
        if self.mismatch.is_blacklisted(route_key(symbol, venue_a, venue_b)):
            return False, "mismatch"
        if self._pair_win_rate_blocks(key, now):
            return False, "pair_win_rate"
        for v in (venue_a, venue_b):
            bal = self.balances.get(v)
            if bal is None or now - bal.get("ts", 0.0) > self.cfg.balance_max_age_s:
                if self.cfg.mode == "live":
                    return False, "balance_unknown"
            elif bal.get("available", 0.0) < size_usd * BALANCE_HEADROOM:
                return False, "balance"
        return True, "ok"

    def funding_blocks(self, symbol: str, venue_a: str, venue_b: str) -> bool:
        """Rates are FRACTIONS as exchanges deliver them (0.0001 = 0.01 %); positive = longs pay shorts.
        Short on A receives A's rate, long on B pays B's rate. Blocks only when the settlements inside
        `funding_block_s` net to a cost above `funding_block_min_pct` (percent points): the venues settle
        on 4 h / 8 h grids (MEXC `collectCycle`, BloFin `fundingInterval`) with near-identical rates, so a
        zero threshold would block half of all routes over a few thousandths of a basis point. A settle stamp already in the past means
        the feed is dead for that key: treated as unknown (allowed) and reported in `stale_funding`."""
        now = self.clock()
        net, in_window = 0.0, False
        for k, sign in ((f"{venue_a}|{symbol}", 1.0), (f"{venue_b}|{symbol}", -1.0)):
            f = self.funding.get(k)
            if f is None:
                continue
            rate, settle = f
            dt = settle - now
            if dt < -FUNDING_STALE_GRACE_S:
                if k not in self.stale_funding:
                    self.stale_funding.add(k)
                    log.warning("FUNDING_STALE %s settle=%.0f now=%.0f — gate disabled for this key", k, settle, now)
                continue
            self.stale_funding.discard(k)
            if 0.0 <= dt < self.cfg.funding_block_s:
                net += sign * rate
                in_window = True
        return in_window and net * 100.0 < -self.cfg.funding_block_min_pct

    # ---- bookkeeping ---------------------------------------------------------
    def record_strike(self, symbol: str, venue_a: str, venue_b: str) -> bool:
        """A failed/one-legged entry or failed hedge. Strikes older than `strike_decay_s` are forgotten;
        `pair_strikes_to_blacklist` strikes inside that window blacklist the route for `pair_blacklist_s`."""
        now = self.clock()
        key = pair_key(symbol, venue_a, venue_b)
        st = self.pair_strikes.get(key)
        n = (st["n"] if st is not None and now - st["ts"] <= self.cfg.strike_decay_s else 0) + 1
        if n >= self.cfg.pair_strikes_to_blacklist:
            self.pair_blacklist[key] = now + self.cfg.pair_blacklist_s
            self.pair_strikes.pop(key, None)
            log.warning("PAIR_BLACKLIST %s for %.0fs after %d strikes", key, self.cfg.pair_blacklist_s, n)
            return True
        self.pair_strikes[key] = {"n": n, "ts": now}
        return False

    def set_cooldown(self, symbol: str, seconds: float | None = None) -> None:
        self.cooldowns[symbol] = self.clock() + (self.cfg.failed_entry_cooldown_s if seconds is None else seconds)

    def blacklist_venue_symbol(self, venue: str, symbol: str) -> None:
        self.venue_symbol_blacklist.add(f"{venue}|{symbol}")

    def record_close(self, pos: Position, counts_as_trade: bool = True) -> None:
        """Bookkeeping for a closed position. `counts_as_trade=False` (Plan 2 reconciliation closes) leaves the
        win-rate stats alone; a zero-P&L close is neither a win nor a loss."""
        now = self.clock()
        if counts_as_trade:
            key = pair_key(pos.symbol, pos.venue_a, pos.venue_b)
            st = self.pair_stats.setdefault(key, {"wins": 0, "losses": 0, "total_pnl": 0.0, "recent": []})
            st.setdefault("recent", [])
            st["total_pnl"] += pos.net_pnl_usd
            if pos.net_pnl_usd != 0.0:
                won = pos.net_pnl_usd > 0.0
                st["wins" if won else "losses"] += 1
                st["recent"] = (st["recent"] + [[now, won]])[-RECENT_OUTCOMES_KEEP:]
        # a real loss blacklists the symbol even when the close does not count as a trade (reconciliation)
        if pos.size_usd > 0 and pos.net_pnl_usd / pos.size_usd * 100.0 < self.cfg.symbol_loss_pct:
            self.symbol_blacklist[pos.symbol] = now + self.cfg.symbol_loss_blacklist_s
            log.warning("SYMBOL_BLACKLIST %s for %.0fs after %+.4f on $%.2f", pos.symbol,
                        self.cfg.symbol_loss_blacklist_s, pos.net_pnl_usd, pos.size_usd)

    def set_balance(self, venue: str, available: float, total: float) -> None:
        self.balances[venue] = {"available": available, "total": total, "locked": max(0.0, total - available),
                                "ts": self.clock()}

    def set_funding(self, venue: str, rates: dict[str, tuple[float, float]]) -> None:
        """`rates`: symbol -> (rate as a FRACTION, next settlement unix ts)."""
        for symbol, val in rates.items():
            try:
                rate, settle = val
                rate, settle = float(rate), float(settle)
                if not (math.isfinite(rate) and math.isfinite(settle)):
                    raise ValueError("non-finite")      # a NaN rate would silently disable the gate for this route
                self.funding[f"{venue}|{symbol}"] = (rate, settle)
            except (TypeError, ValueError):
                log.warning("FUNDING_BAD %s %s %r", venue, symbol, val)

    # ---- persistence ---------------------------------------------------------
    def to_dict(self) -> dict:
        """A snapshot: nothing here aliases live state (the saver serializes off the event loop)."""
        now = self.clock()
        return {"halted": self.halted, "halt_reason": self.halt_reason,
                "pair_strikes": {k: dict(v) for k, v in self.pair_strikes.items()
                                 if now - v.get("ts", 0.0) <= self.cfg.strike_decay_s},
                "pair_blacklist": {k: t for k, t in self.pair_blacklist.items() if t > now},
                "symbol_blacklist": {k: t for k, t in self.symbol_blacklist.items() if t > now},
                "cooldowns": {k: t for k, t in self.cooldowns.items() if t > now},
                "venue_symbol_blacklist": sorted(self.venue_symbol_blacklist),
                "pair_stats": {k: {**v, "recent": [list(r) for r in v.get("recent", [])]}
                               for k, v in self.pair_stats.items()},
                "mismatch_blacklist": {k: dict(v) for k, v in self.mismatch.blacklisted.items()},
                "stale_funding": sorted(self.stale_funding)}      # diagnostic only: recomputed, not restored by load()

    def load(self, d: object) -> None:
        """Tolerant: a malformed `risk` section degrades to empty risk state (with warnings), never to a
        crash loop under systemd Restart=always."""
        if not isinstance(d, dict):
            if d:
                log.warning("RISK_STATE_IGNORED not an object: %r", type(d).__name__)
            d = {}
        now = self.clock()
        self.halted = bool(d.get("halted", False))
        self.halt_reason = str(d.get("halt_reason") or "")
        self.pair_strikes = _strikes_from(d.get("pair_strikes"), now, self.cfg.strike_decay_s)
        self.pair_blacklist = _float_map(d.get("pair_blacklist"), now)
        self.symbol_blacklist = _float_map(d.get("symbol_blacklist"), now)
        self.cooldowns = _float_map(d.get("cooldowns"), now)
        raw_vs = d.get("venue_symbol_blacklist")
        self.venue_symbol_blacklist = {str(x) for x in raw_vs} if isinstance(raw_vs, list) else set()
        self.pair_stats = _stats_from(d.get("pair_stats"))
        self.mismatch.blacklisted = _mismatch_from(d.get("mismatch_blacklist"))
