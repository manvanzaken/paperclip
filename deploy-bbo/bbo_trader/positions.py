"""Position lifecycle: transition table, P&L finalization, PositionBook, StateStore, dashboard state."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from .models import (Position, TT_ENTERING, MAKER_RESTING, HEDGING, OPEN, EXIT_MAKER_RESTING,
                     TT_EXITING, EXIT_HEDGING, DEGRADED, CLOSED)

ALLOWED: dict[str, set[str]] = {
    TT_ENTERING: {OPEN, CLOSED, DEGRADED},
    MAKER_RESTING: {HEDGING, CLOSED, TT_ENTERING},
    HEDGING: {OPEN, CLOSED, DEGRADED},
    OPEN: {TT_EXITING, EXIT_MAKER_RESTING},
    EXIT_MAKER_RESTING: {EXIT_HEDGING, TT_EXITING, OPEN},
    TT_EXITING: {CLOSED, DEGRADED},
    EXIT_HEDGING: {CLOSED, DEGRADED, TT_EXITING},
    DEGRADED: {CLOSED},
    CLOSED: set(),
}


class InvalidTransition(Exception):
    pass


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


class PositionBook:
    """All non-terminal positions live in `open` (resting, hedging, open, exiting, degraded).

    Closed positions are immutable once closed, so their dashboard dicts are cached at close time:
    state saves (up to 2/s) must not re-serialize hundreds of closed positions."""

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
        self.equity_history: list[dict] = []
        self.audit: list[dict] = []
        self.closed_keep = closed_keep
        self.dirty = False

    def new(self, symbol: str, venue_a: str, venue_b: str, status: str, mode: str, **kw) -> Position:
        pos = Position(id=self.next_id, symbol=symbol, venue_a=venue_a, venue_b=venue_b,
                       status=status, mode=mode, **kw)
        self.next_id += 1
        self.open.append(pos)
        self.dirty = True
        return pos

    def get(self, pos_id: int) -> Position | None:
        for p in self.open:
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
        if pos.status != CLOSED:
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
        """Drop a position that never became a trade (resting maker cancelled with zero fill)."""
        if pos in self.open:
            self.open.remove(pos)
        self.dirty = True

    def mark_equity(self, equity: float, now: float, every_s: float = 60.0) -> None:
        if equity > self.peak_equity:
            self.peak_equity = equity
        if self.peak_equity > 0:
            dd = (self.peak_equity - equity) / self.peak_equity * 100.0
            self.max_drawdown_pct = max(self.max_drawdown_pct, dd)
        if not self.equity_history or now - self.equity_history[-1]["ts"] >= every_s:
            self.equity_history.append({"ts": now, "equity": round(equity, 4)})
            del self.equity_history[:-10000]

    def audit_order(self, entry: dict) -> None:
        self.audit.append(entry)
        del self.audit[:-1000]

    def to_dict(self) -> dict:
        return {"next_id": self.next_id, "total_trades": self.total_trades, "total_wins": self.total_wins,
                "total_pnl_usd": self.total_pnl_usd, "peak_equity": self.peak_equity,
                "max_drawdown_pct": self.max_drawdown_pct, "equity_history": self.equity_history[-10000:],
                "open_positions": [p.to_dict() for p in self.open],
                "closed_positions": list(self._closed_dicts),
                "order_audit_log": self.audit[-200:]}

    def load(self, d: dict) -> None:
        self.next_id = int(d.get("next_id", 1))
        self.total_trades = int(d.get("total_trades", 0))
        self.total_wins = int(d.get("total_wins", 0))
        self.total_pnl_usd = float(d.get("total_pnl_usd", 0.0))
        self.peak_equity = float(d.get("peak_equity", 0.0))
        self.max_drawdown_pct = float(d.get("max_drawdown_pct", 0.0))
        self.equity_history = list(d.get("equity_history", []))
        self.audit = list(d.get("order_audit_log", []))
        self.open = [Position.from_dict(x) for x in d.get("open_positions", [])]
        self.closed = [Position.from_dict(x) for x in d.get("closed_positions", [])][-self.closed_keep:]
        self._closed_dicts = [p.to_dict() for p in self.closed]


class StateStore:
    def __init__(self, path: Path):
        self.path = Path(path)

    def save(self, state: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with open(tmp, "w") as f:
            json.dump(state, f)
        os.replace(tmp, self.path)

    def load(self) -> dict | None:
        try:
            with open(self.path, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, OSError):
            return None


def build_state(book: PositionBook, *, equity: float, cash: float, starting_capital: float, mode: str,
                risk_state: dict, balances: dict, scanner: list, bbo: dict, saved_at: float) -> dict:
    """The real_trader dashboard schema plus `risk` and `bbo` sections."""
    d = book.to_dict()
    d.update({
        "state_saved_at_ts": saved_at,
        "saved_at": datetime.fromtimestamp(saved_at, tz=timezone.utc).isoformat(),
        "cash": cash, "equity": equity, "starting_capital": starting_capital,
        "pair_stats": risk_state.get("pair_stats", {}),
        "blofin_risk_blacklist": sorted(k.split("|", 1)[1] for k in risk_state.get("venue_symbol_blacklist", [])
                                        if k.startswith("blofin|")),
        "symbol_blacklist": risk_state.get("symbol_blacklist", {}),
        "pair_failure_counts": risk_state.get("pair_strikes", {}),
        "pair_blacklist": risk_state.get("pair_blacklist", {}),
        "balance_cache": balances,
        "spread_scanner": scanner,
        "spread_histogram": {},
        "dry_run": mode != "live",
        "kill_switch": False,
        "mode": mode,
        "risk": risk_state,
        "bbo": bbo,
    })
    return d
