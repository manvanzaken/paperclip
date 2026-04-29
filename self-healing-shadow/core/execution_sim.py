"""Simulated order execution layer.

Pretends to be a ccxt-style exchange client but never sends a real order.
Walks the live in-memory orderbook to compute a volume-weighted fill
price, applies the per-exchange taker fee, and updates a simulated USDT
balance. Idempotent on `client_order_id` — duplicate submits return the
original order without re-charging.

Failure injection is controlled by `sim_leg_failure_rate`; set > 0 in
tests to exercise the reconciliation saga.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Optional

from .orderbook import OrderBook


class InsufficientLiquidity(Exception):
    """Order size exceeds book depth on the relevant side."""


class SimulatedFailure(Exception):
    """Random failure injected to exercise recovery code paths."""


Side = Literal["buy", "sell"]
OrderState = Literal["pending", "filled", "rejected", "rolled_back"]


@dataclass
class SimOrder:
    client_order_id: str
    exchange: str
    symbol: str
    side: Side
    size_usd: float
    state: OrderState = "pending"
    filled_price: Optional[float] = None
    fee_paid: Optional[float] = None
    slippage_bps: Optional[float] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    filled_at: Optional[datetime] = None


def _walk_book(
    levels: list[tuple[float, float]], size_usd: float
) -> tuple[float, float]:
    """Walk price levels until `size_usd` is consumed.

    Returns `(vwap, units_filled)`. Raises `InsufficientLiquidity` if the
    book has insufficient depth.
    """
    if not levels:
        raise InsufficientLiquidity("empty book")
    remaining = size_usd
    units = 0.0
    cost = 0.0
    for price, level_units in levels:
        level_usd = price * level_units
        if level_usd >= remaining:
            partial_units = remaining / price
            units += partial_units
            cost += remaining
            remaining = 0.0
            break
        units += level_units
        cost += level_usd
        remaining -= level_usd
    if remaining > 0:
        raise InsufficientLiquidity(
            f"need {size_usd}, only {size_usd - remaining} of depth"
        )
    return cost / units, units


class ExecutionSim:
    """In-memory exchange simulator."""

    def __init__(
        self,
        *,
        starting_balances: dict[str, float],
        taker_fees_bps: dict[str, float],
        sim_leg_failure_rate: float = 0.0,
        rng_seed: Optional[int] = None,
    ) -> None:
        self.simulated_balance: dict[str, float] = dict(starting_balances)
        self._taker_fees_bps = dict(taker_fees_bps)
        self._sim_leg_failure_rate = sim_leg_failure_rate
        self._rng = random.Random(rng_seed)
        self._books: dict[tuple[str, str], OrderBook] = {}
        self._ledger: dict[str, SimOrder] = {}

    # --- book wiring ---------------------------------------------------

    def set_book(self, exchange: str, symbol: str, book: OrderBook) -> None:
        self._books[(exchange, symbol)] = book

    # --- order placement ----------------------------------------------

    async def create_order(
        self,
        *,
        exchange: str,
        symbol: str,
        side: Side,
        size_usd: float,
        client_order_id: str,
    ) -> SimOrder:
        # Idempotency: re-submitting the same client_order_id returns
        # the original order without side effects.
        existing = self._ledger.get(client_order_id)
        if existing is not None:
            return existing

        # Failure injection BEFORE any state change.
        if self._rng.random() < self._sim_leg_failure_rate:
            raise SimulatedFailure(f"injected failure on {exchange} {symbol}")

        book = self._books.get((exchange, symbol))
        if book is None:
            raise InsufficientLiquidity(f"no book for {exchange}:{symbol}")

        levels = book.asks if side == "buy" else book.bids
        vwap, _units = _walk_book(levels, size_usd)

        fee_bps = self._taker_fees_bps.get(exchange, 0.0)
        fee_paid = size_usd * fee_bps / 10_000.0

        order = SimOrder(
            client_order_id=client_order_id,
            exchange=exchange,
            symbol=symbol,
            side=side,
            size_usd=size_usd,
            state="filled",
            filled_price=vwap,
            fee_paid=fee_paid,
            slippage_bps=_slippage_bps(book, side, vwap),
            filled_at=datetime.now(timezone.utc),
        )

        if side == "buy":
            self.simulated_balance[exchange] = (
                self.simulated_balance.get(exchange, 0.0) - size_usd - fee_paid
            )
        else:
            self.simulated_balance[exchange] = (
                self.simulated_balance.get(exchange, 0.0) + size_usd - fee_paid
            )

        self._ledger[client_order_id] = order
        return order

    async def fetch_order(self, client_order_id: str) -> Optional[SimOrder]:
        return self._ledger.get(client_order_id)


def _slippage_bps(book: OrderBook, side: Side, vwap: float) -> float:
    """Slippage in bps vs top-of-book."""
    top = book.best_ask if side == "buy" else book.best_bid
    if top is None or top == 0.0:
        return 0.0
    diff = (vwap - top) if side == "buy" else (top - vwap)
    return (diff / top) * 10_000.0
