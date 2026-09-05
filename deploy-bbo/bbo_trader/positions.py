"""Position lifecycle: transition table, P&L finalization, PositionBook, StateStore, dashboard state."""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import threading
from datetime import datetime, timezone
from pathlib import Path

from .models import (Position, TT_ENTERING, MAKER_RESTING, HEDGING, OPEN, EXIT_MAKER_RESTING,
                     TT_EXITING, EXIT_HEDGING, DEGRADED, CLOSED, NON_TERMINAL)

log = logging.getLogger("bbo.state")

# OPEN -> CLOSED / DEGRADED exist for reconciliation: a position the venues no longer hold (closed
# externally, liquidated) is booked closed; one we hold but cannot act on becomes DEGRADED.
ALLOWED: dict[str, set[str]] = {
    TT_ENTERING: {OPEN, CLOSED, DEGRADED},
    MAKER_RESTING: {HEDGING, CLOSED, TT_ENTERING},
    HEDGING: {OPEN, CLOSED, DEGRADED},
    OPEN: {TT_EXITING, EXIT_MAKER_RESTING, CLOSED, DEGRADED},
    EXIT_MAKER_RESTING: {EXIT_HEDGING, TT_EXITING, OPEN},
    TT_EXITING: {CLOSED, DEGRADED},
    EXIT_HEDGING: {CLOSED, DEGRADED, TT_EXITING},
    DEGRADED: {CLOSED},
    CLOSED: set(),
}


class InvalidTransition(Exception):
    pass


class StateCorrupt(Exception):
    """The state file exists but cannot be read or parsed. Never treat this as a fresh start: the
    venues may still hold the positions the file described."""


def transition(pos: Position, new_status: str) -> None:
    if new_status not in ALLOWED.get(pos.status, set()):
        raise InvalidTransition(f"#{pos.id} {pos.status} -> {new_status}")
    pos.status = new_status


def finalize_pnl(pos: Position) -> None:
    """Gross = short leg (entry_a - exit_a)/entry_a + long leg (exit_b - entry_b)/entry_b, times matched USD."""
    gross = 0.0
    if pos.entry_price_a > 0 and pos.exit_price_a > 0:
        gross += (pos.entry_price_a - pos.exit_price_a) / pos.entry_price_a * pos.size_usd
    if pos.entry_price_b > 0 and pos.exit_price_b > 0:
        gross += (pos.exit_price_b - pos.entry_price_b) / pos.entry_price_b * pos.size_usd
    pos.gross_pnl_usd = gross
    pos.net_pnl_usd = gross - pos.entry_fees_usd - pos.exit_fees_usd
    if pos.exit_price_a > 0 and pos.exit_price_b > 0:
        pos.exit_spread_pct = (pos.exit_price_a - pos.exit_price_b) / pos.exit_price_b * 100.0


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


class PositionBook:
    """All non-terminal positions live in `open` (resting, hedging, open, exiting, degraded).

    Closed positions are immutable once closed, so their dashboard dicts are cached at close time:
    state saves (up to 2/s) must not re-serialize hundreds of closed positions. `close()` is
    idempotent — the cancel-after-fill / fill-after-cancel races the spec designs for may deliver a
    late terminal event after a position was already booked."""

    EQUITY_POINTS = 2000        # at the 60 s cadence ≈ 33 h of dashboard chart; keeps the state file small

    def __init__(self, closed_keep: int = 200):
        self.open: list[Position] = []
        self.closed: list[Position] = []
        self._closed_dicts: list[dict] = []
        self.next_id = 1
        self.total_trades = 0
        self.total_wins = 0
        self.total_pnl_usd = 0.0
        self.peak_equity = 0.0
        self.max_drawdown_pct = 0.0
        self.equity_history: list[dict] = []   # {"t": iso, "v": equity, "_ts": float} — the dashboard reads t/v
        self.audit: list[dict] = []            # 1000 kept in memory, the last 200 persisted
        self.closed_keep = max(1, closed_keep)
        self.dirty = False

    def new(self, symbol: str, venue_a: str, venue_b: str, status: str, mode: str, **kw) -> Position:
        pos = Position(id=self.next_id, symbol=symbol, venue_a=venue_a, venue_b=venue_b,
                       status=status, mode=mode, **kw)
        self.next_id += 1
        self.open.append(pos)
        self.dirty = True
        return pos

    def get(self, pos_id: int, include_closed: bool = False) -> Position | None:
        for p in self.open:
            if p.id == pos_id:
                return p
        if include_closed:
            for p in self.closed:
                if p.id == pos_id:
                    return p
        return None

    def for_symbol(self, symbol: str) -> list[Position]:
        return [p for p in self.open if p.symbol == symbol]

    def by_status(self, *statuses: str) -> list[Position]:
        return [p for p in self.open if p.status in statuses]

    def resting_counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for p in self.open:
            if p.status in (MAKER_RESTING, EXIT_MAKER_RESTING) and p.maker_venue:
                out[p.maker_venue] = out.get(p.maker_venue, 0) + 1
        return out

    def close(self, pos: Position, exit_reason: str, now: float, counts_as_trade: bool = True) -> None:
        if pos.status == CLOSED:
            return                                     # already booked: a late duplicate event
        transition(pos, CLOSED)
        pos.exit_reason = exit_reason
        pos.exit_time = now
        finalize_pnl(pos)
        if pos in self.open:
            self.open.remove(pos)
        self.closed.append(pos)
        self._closed_dicts.append(pos.to_dict())
        del self.closed[:-self.closed_keep]
        del self._closed_dicts[:-self.closed_keep]
        if counts_as_trade:
            self.total_trades += 1
            self.total_pnl_usd += pos.net_pnl_usd
            if pos.net_pnl_usd > 0:
                self.total_wins += 1
        self.dirty = True

    def discard(self, pos: Position) -> None:
        """Drop a position that never became a trade (nothing filled). Anything that may hold a leg on
        a venue must be closed, not discarded."""
        if pos.status not in (TT_ENTERING, MAKER_RESTING):
            raise InvalidTransition(f"#{pos.id} discard from {pos.status}")
        if pos in self.open:
            self.open.remove(pos)
        self.dirty = True

    def mark_equity(self, equity: float, now: float, every_s: float = 60.0) -> None:
        if equity > self.peak_equity:
            self.peak_equity = equity
        if self.peak_equity > 0:
            dd = (self.peak_equity - equity) / self.peak_equity * 100.0
            self.max_drawdown_pct = max(self.max_drawdown_pct, dd)
        last_ts = float(self.equity_history[-1].get("_ts") or 0.0) if self.equity_history else 0.0   # null-tolerant
        if not self.equity_history or now - last_ts >= every_s:
            self.equity_history.append({"t": _iso(now), "v": round(equity, 4), "_ts": now})
            del self.equity_history[:-self.EQUITY_POINTS]

    def audit_order(self, entry: dict) -> None:
        self.audit.append(entry)
        del self.audit[:-1000]

    def to_dict(self) -> dict:
        return {"next_id": self.next_id, "total_trades": self.total_trades, "total_wins": self.total_wins,
                "total_pnl_usd": self.total_pnl_usd, "peak_equity": self.peak_equity,
                "max_drawdown_pct": self.max_drawdown_pct, "equity_history": self.equity_history[-self.EQUITY_POINTS:],
                "open_positions": [p.to_dict() for p in self.open],
                "closed_positions": list(self._closed_dicts),
                "order_audit_log": self.audit[-200:]}

    def load(self, d: dict) -> None:
        self.total_trades = int(d.get("total_trades", 0))
        self.total_wins = int(d.get("total_wins", 0))
        self.total_pnl_usd = float(d.get("total_pnl_usd", 0.0))
        self.peak_equity = float(d.get("peak_equity", 0.0))
        self.max_drawdown_pct = float(d.get("max_drawdown_pct", 0.0))
        self.equity_history = list(d.get("equity_history", []))[-self.EQUITY_POINTS:]
        self.audit = list(d.get("order_audit_log", []))
        loaded_open = [Position.from_dict(x) for x in d.get("open_positions", [])]
        loaded_closed = [Position.from_dict(x) for x in d.get("closed_positions", [])]
        for p in loaded_open:
            if p.status not in NON_TERMINAL and p.status != CLOSED:
                # a status this build does not know (rollback, hand edit): the venues may still hold it
                log.error("STATE_UNKNOWN_STATUS #%s %r -> DEGRADED (reconcile can still book it)", p.id, p.status)
                p.status = DEGRADED
        stranded_closed = [p for p in loaded_open if p.status == CLOSED]          # CLOSED under open_positions
        stranded_open = [p for p in loaded_closed if p.status in NON_TERMINAL]    # live under closed_positions
        if stranded_closed or stranded_open:
            log.warning("STATE_REPARTITIONED %d closed entries under open, %d live entries under closed",
                        len(stranded_closed), len(stranded_open))
        self.open = [p for p in loaded_open if p.status != CLOSED] + stranded_open
        self.closed = ([p for p in loaded_closed if p.status not in NON_TERMINAL] + stranded_closed)[-self.closed_keep:]
        self._closed_dicts = [p.to_dict() for p in self.closed]
        highest = max((p.id for p in self.open + self.closed), default=0)
        self.next_id = max(int(d.get("next_id", 1)), highest + 1)   # never reuse an id the file still holds


def _sanitize(obj):
    """Replace non-finite floats with None so the dashboard's JSON.parse never chokes; coerce non-primitive keys."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {(k if isinstance(k, (str, int, float, bool)) or k is None else str(k)): _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize(v) for v in obj]
    return obj


def dumps_state(state: dict) -> str:
    try:
        return json.dumps(state, allow_nan=False)
    except (ValueError, TypeError) as e:
        log.error("STATE_UNSERIALIZABLE %r — sanitizing (non-finite -> null, unknown types -> str)", e)
        return json.dumps(_sanitize(state), default=str)


class StateStore:
    """Atomic JSON state. The live file is NEVER absent: write a uniquely named tmp → flush+fsync →
    hard-link the current file to `.bak` → rename tmp over the live file. `save()` is serialized by a
    thread lock and `save_async()` additionally by an asyncio lock, so an overlapping shutdown save
    cannot race the sweep's save. Stale tmp files from a crash are removed at start-up."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.bak = self.path.with_suffix(self.path.suffix + ".bak")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._tlock = threading.Lock()
        for stale in self.path.parent.glob(f"{self.path.name}.*.tmp"):
            stale.unlink(missing_ok=True)

    def save(self, state: dict) -> None:
        payload = dumps_state(state)
        uniq = f"{os.getpid()}.{threading.get_ident()}"
        tmp = self.path.with_suffix(f"{self.path.suffix}.{uniq}.tmp")
        link = self.path.with_suffix(f"{self.path.suffix}.{uniq}.bak.tmp")
        with self._tlock:
            with open(tmp, "w") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            try:
                link.unlink(missing_ok=True)
                os.link(self.path, link)          # the live file itself is never unlinked
                os.replace(link, self.bak)
            except FileNotFoundError:
                pass                              # first save: nothing to back up
            os.replace(tmp, self.path)

    async def save_async(self, state: dict) -> None:
        """Serialize + write off the event loop (state must be a snapshot the loop no longer mutates)."""
        async with self._lock:
            await asyncio.to_thread(self.save, state)

    def _read(self, path: Path) -> dict | None:
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, OSError) as e:
            raise StateCorrupt(f"{path}: {e}") from e
        if not isinstance(data, dict):
            raise StateCorrupt(f"{path}: top-level {type(data).__name__}, expected object")
        return data

    def load(self) -> dict | None:
        """None only when neither the state file nor a backup exists (fresh start). A missing live
        file next to a backup is a torn save, not a fresh start. Raises StateCorrupt otherwise."""
        d = self._read(self.path)
        if d is None and self.bak.exists():
            raise StateCorrupt(f"{self.path} missing but {self.bak} exists — torn save?")
        return d

    def load_backup(self) -> dict | None:
        return self._read(self.bak)


def build_state(book: PositionBook, *, equity: float, cash: float, starting_capital: float, mode: str,
                risk_state: dict, balances: dict, scanner: list, bbo: dict, saved_at: float) -> dict:
    """The real_trader dashboard schema plus `risk` and `bbo` sections. `cash` is realized-only
    (capital + realized P&L): the legacy dashboard shows `cash + Σ open net_pnl_usd`, so `equity`
    must not be passed as `cash` when it already includes unrealized P&L. `kill_switch` mirrors the
    manual halt so the dashboard's status light reflects `stop.flag` / `/stop`."""
    d = book.to_dict()
    d.update({
        "state_saved_at_ts": saved_at,
        "saved_at": _iso(saved_at),
        "cash": cash, "equity": equity, "starting_capital": starting_capital,
        "pair_stats": risk_state.get("pair_stats", {}),
        "blofin_risk_blacklist": sorted(k.split("|", 1)[1] for k in risk_state.get("venue_symbol_blacklist", [])
                                        if k.startswith("blofin|")),
        "symbol_blacklist": risk_state.get("symbol_blacklist", {}),
        "pair_failure_counts": {k: (v.get("n", 0) if isinstance(v, dict) else v)
                                for k, v in risk_state.get("pair_strikes", {}).items()},
        "pair_blacklist": risk_state.get("pair_blacklist", {}),
        "balance_cache": {k: dict(v) for k, v in balances.items()},
        "spread_scanner": scanner,
        "spread_histogram": {},
        "dry_run": mode != "live",
        "kill_switch": bool(risk_state.get("halted")),
        "mode": mode,
        "risk": risk_state,
        "bbo": bbo,
    })
    return d
