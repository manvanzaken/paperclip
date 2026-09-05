"""Risk: manual halt, blacklists and strikes, mismatch guard, balances, funding gate."""
from __future__ import annotations

import time

from .config import Config
from .models import Position


def pair_key(symbol: str, venue_a: str, venue_b: str) -> str:
    return f"{symbol}|{venue_a}>{venue_b}"


def route_key(symbol: str, venue_x: str, venue_y: str) -> str:
    """Direction-free key for a venue pair (mismatch guard)."""
    return f"{symbol}|" + "|".join(sorted((venue_x, venue_y)))


class MismatchGuard:
    """Same-ticker-different-asset detector: |raw spread| stays absurd for N consecutive quotes."""

    def __init__(self, slow_pct: float, slow_n: int, fast_pct: float, fast_n: int,
                 blacklisted: set[str] | None = None):
        self.slow_pct, self.slow_n, self.fast_pct, self.fast_n = slow_pct, slow_n, fast_pct, fast_n
        self.blacklisted: set[str] = set(blacklisted or ())
        self._slow: dict[str, int] = {}
        self._fast: dict[str, int] = {}

    def observe(self, key: str, raw_spread_pct: float) -> bool:
        """Returns True when this observation newly blacklists the pair."""
        a = abs(raw_spread_pct)
        self._slow[key] = self._slow.get(key, 0) + 1 if a > self.slow_pct else 0
        self._fast[key] = self._fast.get(key, 0) + 1 if a > self.fast_pct else 0
        if key not in self.blacklisted and (self._slow[key] >= self.slow_n or self._fast[key] >= self.fast_n):
            self.blacklisted.add(key)
            return True
        return False

    def is_blacklisted(self, key: str) -> bool:
        return key in self.blacklisted


class RiskManager:
    def __init__(self, cfg: Config, clock=time.time):
        self.cfg = cfg
        self.clock = clock
        self.halted = False
        self.halt_reason = ""
        self.pair_strikes: dict[str, int] = {}
        self.pair_blacklist: dict[str, float] = {}      # key -> until ts
        self.symbol_blacklist: dict[str, float] = {}    # symbol -> until ts
        self.cooldowns: dict[str, float] = {}           # symbol -> until ts
        self.venue_symbol_blacklist: set[str] = set()   # "venue|symbol"
        self.balances: dict[str, dict] = {}             # venue -> {"available","total","ts"}
        self.funding: dict[str, tuple[float, float]] = {}  # "venue|symbol" -> (rate, next_settle_ts)
        self.pair_stats: dict[str, dict] = {}           # key -> {"wins","losses","total_pnl"}
        self.mismatch = MismatchGuard(cfg.mismatch_slow_pct, cfg.mismatch_slow_n,
                                      cfg.mismatch_fast_pct, cfg.mismatch_fast_n)

    # ---- halt ----------------------------------------------------------------
    def halt(self, reason: str) -> None:
        self.halted, self.halt_reason = True, reason

    def resume(self) -> None:
        self.halted, self.halt_reason = False, ""

    def check_flags(self) -> str | None:
        """Dashboard flags in DATA_DIR: stop.flag halts, start.flag resumes (and removes both)."""
        stop = self.cfg.data_dir / self.cfg.halt_flag
        start = self.cfg.data_dir / self.cfg.resume_flag
        if start.exists():
            for p in (start, stop):
                try:
                    p.unlink()
                except FileNotFoundError:
                    pass
            if self.halted:
                self.resume()
                return "resume"
            return None
        if stop.exists() and not self.halted:
            self.halt("stop.flag")
            return "halt"
        return None

    # ---- gates ---------------------------------------------------------------
    def venue_symbol_blocked(self, venue: str, symbol: str) -> bool:
        if f"{venue}|{symbol}" in self.venue_symbol_blacklist:
            return True
        wl = self.cfg.venue(venue).symbol_whitelist
        return bool(wl) and symbol not in wl

    def entry_allowed(self, symbol: str, venue_a: str, venue_b: str, size_usd: float,
                      open_count: int) -> tuple[bool, str]:
        now = self.clock()
        if self.halted:
            return False, "halted"
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
        st = self.pair_stats.get(key)
        if st:
            n = st.get("wins", 0) + st.get("losses", 0)
            if n >= 5 and st.get("wins", 0) / n < 0.30:
                return False, "pair_win_rate"
        for v in (venue_a, venue_b):
            bal = self.balances.get(v)
            if bal is not None and bal.get("available", 0.0) < size_usd * 1.05:
                return False, "balance"
        return True, "ok"

    def funding_blocks(self, symbol: str, venue_a: str, venue_b: str) -> bool:
        """Short A receives A's rate, long B pays B's rate. Block when the net of the settlements
        inside the window is negative (positive rate = longs pay shorts)."""
        now = self.clock()
        net, in_window = 0.0, False
        fa = self.funding.get(f"{venue_a}|{symbol}")
        if fa and 0.0 <= fa[1] - now < self.cfg.funding_block_s:
            net += fa[0]
            in_window = True
        fb = self.funding.get(f"{venue_b}|{symbol}")
        if fb and 0.0 <= fb[1] - now < self.cfg.funding_block_s:
            net -= fb[0]
            in_window = True
        return in_window and net < 0.0

    # ---- bookkeeping ---------------------------------------------------------
    def record_strike(self, symbol: str, venue_a: str, venue_b: str) -> bool:
        key = pair_key(symbol, venue_a, venue_b)
        self.pair_strikes[key] = self.pair_strikes.get(key, 0) + 1
        if self.pair_strikes[key] >= self.cfg.pair_strikes_to_blacklist:
            self.pair_blacklist[key] = self.clock() + self.cfg.pair_blacklist_s
            self.pair_strikes[key] = 0
            return True
        return False

    def set_cooldown(self, symbol: str, seconds: float | None = None) -> None:
        self.cooldowns[symbol] = self.clock() + (self.cfg.failed_entry_cooldown_s if seconds is None else seconds)

    def blacklist_venue_symbol(self, venue: str, symbol: str) -> None:
        self.venue_symbol_blacklist.add(f"{venue}|{symbol}")

    def record_close(self, pos: Position) -> None:
        key = pair_key(pos.symbol, pos.venue_a, pos.venue_b)
        st = self.pair_stats.setdefault(key, {"wins": 0, "losses": 0, "total_pnl": 0.0})
        st["total_pnl"] += pos.net_pnl_usd
        st["wins" if pos.net_pnl_usd > 0 else "losses"] += 1
        if pos.size_usd > 0 and pos.net_pnl_usd / pos.size_usd * 100.0 < -0.10:
            self.symbol_blacklist[pos.symbol] = self.clock() + self.cfg.symbol_loss_blacklist_s

    def set_balance(self, venue: str, available: float, total: float) -> None:
        self.balances[venue] = {"available": available, "total": total, "locked": max(0.0, total - available),
                                "ts": self.clock()}

    def set_funding(self, venue: str, rates: dict[str, tuple[float, float]]) -> None:
        for symbol, val in rates.items():
            self.funding[f"{venue}|{symbol}"] = val

    # ---- persistence ---------------------------------------------------------
    def to_dict(self) -> dict:
        now = self.clock()
        return {"halted": self.halted, "halt_reason": self.halt_reason,
                "pair_strikes": dict(self.pair_strikes),
                "pair_blacklist": {k: t for k, t in self.pair_blacklist.items() if t > now},
                "symbol_blacklist": {k: t for k, t in self.symbol_blacklist.items() if t > now},
                "cooldowns": {k: t for k, t in self.cooldowns.items() if t > now},
                "venue_symbol_blacklist": sorted(self.venue_symbol_blacklist),
                "pair_stats": self.pair_stats,
                "mismatch_blacklist": sorted(self.mismatch.blacklisted)}

    def load(self, d: dict) -> None:
        now = self.clock()
        self.halted = bool(d.get("halted", False))
        self.halt_reason = str(d.get("halt_reason", ""))
        self.pair_strikes = {k: int(v) for k, v in d.get("pair_strikes", {}).items()}
        self.pair_blacklist = {k: float(t) for k, t in d.get("pair_blacklist", {}).items() if float(t) > now}
        self.symbol_blacklist = {k: float(t) for k, t in d.get("symbol_blacklist", {}).items() if float(t) > now}
        self.cooldowns = {k: float(t) for k, t in d.get("cooldowns", {}).items() if float(t) > now}
        self.venue_symbol_blacklist = set(d.get("venue_symbol_blacklist", []))
        self.pair_stats = dict(d.get("pair_stats", {}))
        self.mismatch.blacklisted = set(d.get("mismatch_blacklist", []))
