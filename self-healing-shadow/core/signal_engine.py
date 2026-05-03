"""Z-score → trade-signal gating.

The heartbeat and Hurst gates run BEFORE the profitability gate so that
aborted entries get tagged with the cheapest possible reason. Each gate
that rejects a signal writes an `ENTRY_ABORTED` journal row with a
single-token `reason` so the health-check error/abort queries stay
simple.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Literal, Optional

from core.orderbook import OrderBook
from core.spread_engine import SpreadEngine
from core.trade_journal import TradeJournal
from heal.heartbeat import HeartbeatMonitor
from heal.hurst_canary import HurstCanary


Side = Literal["buy", "sell"]


@dataclass
class EntrySignal:
    pair: tuple[str, str]
    symbol: str
    z_score: float
    side_a: Side
    side_b: Side
    size_usd: float
    expected_net_profit_usd: float


@dataclass
class ExitSignal:
    reason: Literal["convergence", "stop_loss", "max_hold"]


def _expected_net_profit(
    *,
    current_spread_log: float,
    mean_spread_log: float,
    book_a: OrderBook,
    book_b: OrderBook,
    size_usd: float,
    fee_a_bps: float,
    fee_b_bps: float,
    recovery_fraction: float,
) -> float:
    """Empirical expected net PnL on the trade.

    Components:
      gross    = recovery_fraction * |excess_spread| * size_usd
      slippage = (half_spread_a + half_spread_b) * size_usd
      fees     = 2 * (fee_a + fee_b) * size_usd     # entry + exit, both legs
      net      = gross - slippage - fees

    `current_spread_log - mean_spread_log` is the excess in log-spread
    units; for tight crypto perp spreads this is ~ identical to the
    bps difference (log(1+x) ≈ x for small x).
    """
    if book_a.best_ask is None or book_b.best_ask is None:
        return 0.0
    if book_a.best_bid is None or book_b.best_bid is None:
        return 0.0

    excess_log = abs(current_spread_log - mean_spread_log)
    expected_capture_bps = recovery_fraction * excess_log * 10_000.0
    gross_usd = size_usd * expected_capture_bps / 10_000.0

    # Half-spread cost on each leg (taker crosses the book).
    mid_a = (book_a.best_bid + book_a.best_ask) / 2.0
    mid_b = (book_b.best_bid + book_b.best_ask) / 2.0
    half_spread_a_bps = ((book_a.best_ask - book_a.best_bid) / 2.0 / mid_a) * 10_000.0
    half_spread_b_bps = ((book_b.best_ask - book_b.best_bid) / 2.0 / mid_b) * 10_000.0
    slippage_usd = size_usd * (half_spread_a_bps + half_spread_b_bps) / 10_000.0

    # Round-trip fees on both legs.
    fees_usd = 2.0 * size_usd * (fee_a_bps + fee_b_bps) / 10_000.0

    return gross_usd - slippage_usd - fees_usd


class SignalEngine:
    def __init__(
        self,
        *,
        heartbeat: HeartbeatMonitor,
        canary: HurstCanary,
        journal: TradeJournal,
        spread_engine: SpreadEngine,
        entry_z: float,
        exit_z: float,
        stop_loss_z: float,
        min_net_profit_usd: float,
        max_position_usd: float,
        taker_fees_bps: dict[str, float],
        recovery_fraction: float = 0.5,
        is_quarantined: Optional[Callable[[str], bool]] = None,
    ) -> None:
        self.heartbeat = heartbeat
        self.canary = canary
        self._journal = journal
        self._spread_engine = spread_engine
        self._entry_z = entry_z
        self._exit_z = exit_z
        self._stop_loss_z = stop_loss_z
        self._min_net_profit = min_net_profit_usd
        self._max_position_usd = max_position_usd
        self._fees_bps = taker_fees_bps
        self._recovery_fraction = recovery_fraction
        # Quarantine gate: returns True if the exchange is QUARANTINED
        # by the reconciliation saga. Default no-op (no quarantine).
        self._is_quarantined = is_quarantined or (lambda _ex: False)

    # --- entry ---------------------------------------------------------

    def evaluate(
        self,
        *,
        pair: tuple[str, str],
        symbol: str,
        z_score: float,
        current_spread_log: float,
        book_a: OrderBook,
        book_b: OrderBook,
    ) -> Optional[EntrySignal]:
        if abs(z_score) < self._entry_z:
            return None  # not yet a signal — silent

        a, b = pair

        # Gate 1: heartbeat.
        if not self.heartbeat.is_pair_tradeable(a, b):
            self._abort(pair, symbol, z_score, reason="heartbeat_degraded")
            return None

        # Gate 2: quarantine (exchange has had repeated saga failures).
        if self._is_quarantined(a) or self._is_quarantined(b):
            self._abort(pair, symbol, z_score, reason="exchange_quarantined")
            return None

        # Gate 3: Hurst canary.
        if not self.canary.is_pair_safe(pair):
            self._abort(pair, symbol, z_score, reason="hurst_unsafe")
            return None

        # Gate 4: profitability — needs the rolling mean to compute excess.
        mean_log = self._spread_engine.mean(pair)
        if mean_log is None:
            return None  # not enough samples yet
        size_usd = self._max_position_usd
        net = _expected_net_profit(
            current_spread_log=current_spread_log,
            mean_spread_log=mean_log,
            book_a=book_a,
            book_b=book_b,
            size_usd=size_usd,
            fee_a_bps=self._fees_bps.get(a, 5.0),
            fee_b_bps=self._fees_bps.get(b, 5.0),
            recovery_fraction=self._recovery_fraction,
        )
        if net < self._min_net_profit:
            self._abort(
                pair, symbol, z_score,
                reason="profit_below_threshold",
                expected_pnl_usd=net,
                excess_log=current_spread_log - mean_log,
            )
            return None

        # Z>0: spread A-B is too wide -> short A (sell), long B (buy).
        # Z<0: inverted.
        if z_score > 0:
            side_a, side_b = "sell", "buy"
        else:
            side_a, side_b = "buy", "sell"

        sig = EntrySignal(
            pair=pair,
            symbol=symbol,
            z_score=z_score,
            side_a=side_a,
            side_b=side_b,
            size_usd=size_usd,
            expected_net_profit_usd=net,
        )
        self._journal.log(
            event_type="ENTRY_SIGNAL",
            payload={
                "side_a": side_a, "side_b": side_b,
                "size_usd": size_usd,
                "expected_net_profit_usd": net,
            },
            pair=f"{a}_{b}",
            symbol=symbol,
            z_score=z_score,
            expected_pnl_usd=net,
        )
        return sig

    # --- exit ----------------------------------------------------------

    def evaluate_exit(self, *, z_score: float, stop_loss_hit: bool) -> Optional[ExitSignal]:
        if stop_loss_hit or abs(z_score) >= self._stop_loss_z:
            return ExitSignal(reason="stop_loss")
        if abs(z_score) <= self._exit_z:
            return ExitSignal(reason="convergence")
        return None

    # --- internals -----------------------------------------------------

    def _abort(
        self,
        pair: tuple[str, str],
        symbol: str,
        z_score: float,
        *,
        reason: str,
        expected_pnl_usd: float | None = None,
        excess_log: float | None = None,
    ) -> None:
        a, b = pair
        payload: dict = {"reason": reason}
        if expected_pnl_usd is not None:
            payload["expected_pnl_usd"] = expected_pnl_usd
        if excess_log is not None:
            payload["excess_log"] = excess_log
        self._journal.log(
            event_type="ENTRY_ABORTED",
            payload=payload,
            pair=f"{a}_{b}",
            symbol=symbol,
            z_score=z_score,
            expected_pnl_usd=expected_pnl_usd,
        )
