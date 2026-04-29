"""Z-score → trade-signal gating.

The heartbeat and Hurst gates run BEFORE the profitability gate so that
aborted entries get tagged with the cheapest possible reason. Each gate
that rejects a signal writes an `ENTRY_ABORTED` journal row with a
single-token `reason` so the health-check error/abort queries stay
simple.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

from core.orderbook import OrderBook
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
    reason: Literal["convergence", "stop_loss"]


def _expected_net_profit(
    *,
    z_score: float,
    book_a: OrderBook,
    book_b: OrderBook,
    size_usd: float,
    fee_a_bps: float,
    fee_b_bps: float,
) -> float:
    """Crude expected-PnL estimate: |Z|*one-stddev assumed price move minus fees+slip.

    For paper-mode this is only used to gate trades; we don't need a
    rigorous model. The expected gross is ``|z| * 30bps`` — a coarse
    anchor that lets typical Z=2.5 signals pass the gate at $1k size,
    chosen for the MVP. A real production strategy should replace this
    with an empirical estimate from backtest.
    """
    if book_a.best_ask is None or book_b.best_ask is None:
        return 0.0
    expected_gross_bps = abs(z_score) * 30.0
    gross_usd = size_usd * (expected_gross_bps / 10_000.0)
    # Round-trip fees: pay the taker fee on entry AND exit on both legs.
    fees_usd = 2.0 * size_usd * (fee_a_bps + fee_b_bps) / 10_000.0
    return gross_usd - fees_usd


class SignalEngine:
    def __init__(
        self,
        *,
        heartbeat: HeartbeatMonitor,
        canary: HurstCanary,
        journal: TradeJournal,
        entry_z: float,
        exit_z: float,
        stop_loss_z: float,
        min_net_profit_usd: float,
        max_position_usd: float,
        taker_fees_bps: dict[str, float],
    ) -> None:
        self.heartbeat = heartbeat
        self.canary = canary
        self._journal = journal
        self._entry_z = entry_z
        self._exit_z = exit_z
        self._stop_loss_z = stop_loss_z
        self._min_net_profit = min_net_profit_usd
        self._max_position_usd = max_position_usd
        self._fees_bps = taker_fees_bps

    # --- entry ---------------------------------------------------------

    def evaluate(
        self,
        *,
        pair: tuple[str, str],
        symbol: str,
        z_score: float,
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

        # Gate 2: Hurst canary.
        if not self.canary.is_pair_safe(pair):
            self._abort(pair, symbol, z_score, reason="hurst_unsafe")
            return None

        # Gate 3: profitability.
        size_usd = self._max_position_usd
        net = _expected_net_profit(
            z_score=z_score,
            book_a=book_a,
            book_b=book_b,
            size_usd=size_usd,
            fee_a_bps=self._fees_bps.get(a, 5.0),
            fee_b_bps=self._fees_bps.get(b, 5.0),
        )
        if net < self._min_net_profit:
            self._abort(pair, symbol, z_score, reason="profit_below_threshold")
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
    ) -> None:
        a, b = pair
        self._journal.log(
            event_type="ENTRY_ABORTED",
            payload={"reason": reason},
            pair=f"{a}_{b}",
            symbol=symbol,
            z_score=z_score,
        )
