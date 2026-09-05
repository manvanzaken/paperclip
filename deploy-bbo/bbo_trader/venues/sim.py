"""SimVenue: paper trading over any real public feed. Implements the Trading + PrivateFeed protocols.

Fill model (conservative — the SpreadWatch `post_hedge` rule):
- market orders fill fully at the current touch ± SIM_TAKER_SLIP_BPS after SIM_LATENCY_MS (liquidity "taker");
- post-only orders that would cross the book are rejected like a real venue; resting orders become live
  after the latency and fill only when the OPPOSITE touch crosses the resting price on a quote newer
  than the post, for at most MAKER_TOP_LEVEL_FRAC × that touch's quantity per quote (liquidity "maker");
- cancel/amend are immediate; positions are signed contracts per symbol (one-way accounting).
Order events are pushed synchronously to the registered handler, so an "ack" event can reach the
executor before the REST-style OrderAck returns — exactly what live private WebSockets do."""
from __future__ import annotations

import asyncio
import itertools
import math
import time
from dataclasses import dataclass
from typing import Callable

from ..config import Config
from ..models import BBO, Fees, OrderAck, OrderEvent, VenueSpec
from ..quotes import QuoteBoard
from .base import VenuePosition

TERMINAL = ("filled", "canceled", "rejected")


@dataclass
class SimOrder:
    client_id: str
    order_id: str
    symbol: str
    side: str
    qty: float
    price: float          # 0.0 for market orders
    post_only: bool
    reduce_only: bool
    live_ts: float        # when the order is considered to have reached the venue
    state: str = "ack"
    filled: float = 0.0
    avg_price: float = 0.0
    fee: float = 0.0
    liquidity: str = ""

    @property
    def remaining(self) -> float:
        return round(self.qty - self.filled, 10)


class SimVenue:
    supports_amend = True

    def __init__(self, name: str, cfg: Config, fees: Fees, board: QuoteBoard,
                 specs: dict[str, VenueSpec], clock=time.time):
        self.name = name
        self.cfg = cfg
        self.fees = fees
        self.board = board
        self.specs = specs
        self.clock = clock
        self.latency_s = cfg.sim_latency_ms / 1000.0
        self.slip = cfg.sim_taker_slip_bps / 10_000.0
        self.frac = cfg.maker_top_level_frac
        self._orders: dict[str, SimOrder] = {}
        self._pos: dict[str, float] = {}
        self._cash = cfg.paper_capital_per_venue
        self._handler: Callable[[OrderEvent], None] | None = None
        self._seq = itertools.count(1)
        self._tasks: set[asyncio.Task] = set()

    # ---- PrivateFeed ----------------------------------------------------------
    def set_handler(self, on_event: Callable[[OrderEvent], None]) -> None:
        self._handler = on_event

    async def run(self) -> None:
        await asyncio.Event().wait()   # events are pushed synchronously from fills

    # ---- internals ------------------------------------------------------------
    def _spec(self, symbol: str) -> VenueSpec:
        return self.specs.get(symbol) or VenueSpec(self.name, symbol, symbol, 1.0, 1.0, 1.0, 0.0001)

    def _event(self, o: SimOrder) -> OrderEvent:
        return OrderEvent(self.name, o.client_id, o.order_id, o.state, o.filled, o.avg_price, o.fee,
                          o.liquidity, "", self.clock())

    def _emit(self, o: SimOrder, state: str | None = None) -> None:
        if state:
            o.state = state
        if self._handler:
            self._handler(self._event(o))

    def _fill(self, o: SimOrder, qty: float, price: float, liquidity: str) -> None:
        cs = self._spec(o.symbol).contract_size
        notional = qty * price * cs
        o.avg_price = (o.avg_price * o.filled + price * qty) / (o.filled + qty)
        o.filled = round(o.filled + qty, 10)
        rate = self.fees.taker if liquidity == "taker" else self.fees.maker
        fee = notional * rate / 100.0
        o.fee += fee
        o.liquidity = liquidity
        self._cash -= fee
        signed = qty if o.side == "buy" else -qty
        self._pos[o.symbol] = round(self._pos.get(o.symbol, 0.0) + signed, 10)
        self._emit(o, "filled" if o.remaining <= 1e-12 else "partial")

    def _spawn(self, coro) -> None:
        t = asyncio.get_running_loop().create_task(coro)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _fill_market_later(self, o: SimOrder, price: float) -> None:
        await asyncio.sleep(self.latency_s)
        if o.state in TERMINAL:
            return
        self._fill(o, o.qty, price, "taker")

    # ---- Trading --------------------------------------------------------------
    async def place_market(self, symbol: str, side: str, qty: float, reduce_only: bool, client_id: str) -> OrderAck:
        q = self.board.get(self.name, symbol)
        if q is None or not q.ok:
            return OrderAck(False, error="no quote")
        if qty <= 0:
            return OrderAck(False, error="qty must be > 0")
        o = SimOrder(client_id, f"sim-{next(self._seq)}", symbol, side, qty, 0.0, False, reduce_only,
                     self.clock() + self.latency_s)
        self._orders[client_id] = o
        price = q.ask * (1.0 + self.slip) if side == "buy" else q.bid * (1.0 - self.slip)
        self._spawn(self._fill_market_later(o, price))
        return OrderAck(True, o.order_id, latency_ms=self.latency_s * 1000.0)

    async def place_post_only(self, symbol: str, side: str, qty: float, price: float, reduce_only: bool,
                              client_id: str) -> OrderAck:
        q = self.board.get(self.name, symbol)
        if q is None or not q.ok:
            return OrderAck(False, error="no quote")
        o = SimOrder(client_id, f"sim-{next(self._seq)}", symbol, side, qty, price, True, reduce_only,
                     self.clock() + self.latency_s)
        self._orders[client_id] = o
        if (side == "sell" and price <= q.bid) or (side == "buy" and price >= q.ask):
            self._emit(o, "rejected")
            return OrderAck(False, o.order_id, error="post-only would cross")
        self._emit(o, "ack")
        return OrderAck(True, o.order_id, latency_ms=self.latency_s * 1000.0)

    def on_quote(self, bbo: BBO) -> None:
        """Called by the app for every stored quote of this venue: fill resting orders the touch crossed."""
        if bbo.venue != self.name:
            return
        for o in list(self._orders.values()):
            if o.symbol != bbo.symbol or not o.post_only or o.state not in ("ack", "partial"):
                continue
            if bbo.ts_local < o.live_ts:
                continue
            if o.side == "sell" and bbo.bid >= o.price:
                touch_qty = bbo.bid_qty
            elif o.side == "buy" and bbo.ask <= o.price:
                touch_qty = bbo.ask_qty
            else:
                continue
            lot = self._spec(o.symbol).lot
            cap = max(lot, math.floor(self.frac * touch_qty / lot) * lot)
            qty = min(o.remaining, cap)
            if qty > 0:
                self._fill(o, qty, o.price, "maker")

    async def cancel(self, symbol: str, client_id: str, order_id: str) -> OrderAck:
        o = self._orders.get(client_id)
        if o is None or o.state in TERMINAL:
            return OrderAck(False, order_id, error="terminal")
        self._emit(o, "canceled")
        return OrderAck(True, o.order_id)

    async def amend(self, symbol: str, client_id: str, order_id: str, new_price: float) -> OrderAck:
        o = self._orders.get(client_id)
        if o is None or not o.post_only or o.state not in ("ack", "partial"):
            return OrderAck(False, error="not resting")
        q = self.board.get(self.name, symbol)
        if q and ((o.side == "sell" and new_price <= q.bid) or (o.side == "buy" and new_price >= q.ask)):
            return OrderAck(False, o.order_id, error="post-only would cross")
        o.price = new_price
        o.live_ts = self.clock() + self.latency_s
        return OrderAck(True, o.order_id)

    async def query_order(self, symbol: str, client_id: str, order_id: str) -> OrderEvent | None:
        o = self._orders.get(client_id)
        return self._event(o) if o else None

    async def open_orders(self) -> list[OrderEvent]:
        return [self._event(o) for o in self._orders.values() if o.post_only and o.state in ("ack", "partial")]

    async def positions(self) -> list[VenuePosition]:
        return [VenuePosition(self.name, s, "long" if q > 0 else "short", abs(q))
                for s, q in self._pos.items() if abs(q) > 1e-12]

    async def balance(self) -> dict[str, float]:
        return {"available": self._cash, "total": self._cash}

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        return None