"""SimVenue: paper trading over any real public feed. Implements the Trading + PrivateFeed protocols.

Fill model (conservative — the SpreadWatch `post_hedge` rule):
- market orders are acked at once and fill fully after SIM_LATENCY_MS at the touch that exists THEN
  (± SIM_TAKER_SLIP_BPS): the market moves during the latency, so paper sees real spread decay;
- post-only orders that would cross the book are rejected like a real venue; resting orders become live
  after the latency and fill only when the OPPOSITE touch crosses the resting price on a quote newer than
  the post, for at most MAKER_TOP_LEVEL_FRAC × that touch's quantity — granted once per DISTINCT touch
  (an unchanged book re-pushed ten times a second is not new flow) and never for a sub-lot touch;
- reduce-only fills are clamped to the open position; a close against a flat position is rejected with
  error "nothing to reduce", as MEXC and BloFin do;
- every quote used for a fill must be FRESH (a stale board means "no quote"); cancel/amend are immediate;
- positions are signed contracts per symbol with an average cost basis; realized P&L and fees flow into
  `balance()` (no margin model: available == total). `position_id` is always "" (one-way accounting), so
  a clean paper run says nothing about MEXC hedge-mode position ids.
Order events are pushed synchronously to the registered handler, so an "ack" event can reach the
executor before the REST-style OrderAck returns — exactly what live private WebSockets do."""
from __future__ import annotations

import asyncio
import itertools
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable

from ..config import Config
from ..models import BBO, Fees, OrderAck, OrderEvent, VenueSpec
from ..quotes import QuoteBoard
from .base import VenuePosition

log = logging.getLogger("bbo.sim")

TERMINAL = ("filled", "canceled", "rejected")
MAX_ORDERS = 5000      # terminal orders kept for query_order; the oldest are dropped beyond this


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
    error: str = ""
    last_touch: tuple[float, float] = (0.0, 0.0)   # (price, qty) of the touch that last filled this order

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
        self._resting: dict[str, dict[str, SimOrder]] = {}   # symbol -> client_id -> live post-only order
        self._terminal_ids: deque[str] = deque()
        self._pos: dict[str, float] = {}                    # symbol -> signed contracts
        self._avg: dict[str, float] = {}                    # symbol -> average entry price
        self._cash = cfg.paper_capital_per_venue
        self._handler: Callable[[OrderEvent], None] | None = None
        self._seq = itertools.count(1)
        self._tasks: set[asyncio.Task] = set()
        self._warned_specs: set[str] = set()

    # ---- PrivateFeed ----------------------------------------------------------
    def set_handler(self, on_event: Callable[[OrderEvent], None]) -> None:
        self._handler = on_event

    async def run(self) -> None:
        await asyncio.Event().wait()   # events are pushed synchronously from fills

    # ---- internals ------------------------------------------------------------
    def _spec(self, symbol: str) -> VenueSpec:
        s = self.specs.get(symbol)
        if s is None:
            if symbol not in self._warned_specs:
                self._warned_specs.add(symbol)
                log.warning("SIM_SPEC_MISSING %s %s: contract size 1.0 assumed — fees and P&L may be off by that factor",
                            self.name, symbol)
            return VenueSpec(self.name, symbol, symbol, 1.0, 1.0, 1.0, 0.0001)
        return s

    def _fresh(self, symbol: str) -> BBO | None:
        q = self.board.fresh(self.name, symbol, self.clock())
        return q if q is not None and q.ok else None

    def _event(self, o: SimOrder) -> OrderEvent:
        return OrderEvent(self.name, o.client_id, o.order_id, o.state, o.filled, o.avg_price, o.fee,
                          o.liquidity, "", self.clock(), o.error)

    def _emit(self, o: SimOrder, state: str | None = None) -> None:
        if state:
            o.state = state
        if o.state in TERMINAL:
            self._resting.get(o.symbol, {}).pop(o.client_id, None)
            self._terminal_ids.append(o.client_id)
            while len(self._orders) > MAX_ORDERS and self._terminal_ids:
                self._orders.pop(self._terminal_ids.popleft(), None)
        if self._handler:
            self._handler(self._event(o))

    def _book_position(self, symbol: str, signed: float, price: float, cs: float) -> None:
        """Signed contracts with an average cost basis; a reducing fill realizes P&L into cash."""
        pos = self._pos.get(symbol, 0.0)
        avg = self._avg.get(symbol, 0.0)
        new = round(pos + signed, 10)
        if pos == 0.0 or (pos > 0) == (signed > 0):                  # opening or adding
            self._avg[symbol] = (avg * abs(pos) + price * abs(signed)) / abs(new)
        else:                                                         # reducing, maybe flipping
            closed = min(abs(signed), abs(pos))
            self._cash += (price - avg) * closed * cs * (1.0 if pos > 0 else -1.0)
            if new != 0.0 and (new > 0) != (pos > 0):
                self._avg[symbol] = price                             # the flipped remainder opens here
        if abs(new) < 1e-12:
            self._pos.pop(symbol, None)
            self._avg.pop(symbol, None)
        else:
            self._pos[symbol] = new

    def _fill(self, o: SimOrder, qty: float, price: float, liquidity: str) -> None:
        cs = self._spec(o.symbol).contract_size
        if o.reduce_only:
            pos = self._pos.get(o.symbol, 0.0)
            room = max(0.0, -pos) if o.side == "buy" else max(0.0, pos)
            if room <= 1e-12:
                o.error = "nothing to reduce"
                self._emit(o, "canceled" if o.filled > 0 else "rejected")
                return
            if qty > room:
                log.warning("SIM_REDUCE_CLAMPED %s %s %s %.10g -> %.10g", self.name, o.symbol, o.side, qty, room)
                qty = room
                o.qty = round(o.filled + qty, 10)                     # the venue fills what exists, then the order is done
        notional = qty * price * cs
        o.avg_price = (o.avg_price * o.filled + price * qty) / (o.filled + qty)
        o.filled = round(o.filled + qty, 10)
        rate = self.fees.taker if liquidity == "taker" else self.fees.maker
        fee = notional * rate / 100.0
        o.fee += fee
        o.liquidity = liquidity
        self._cash -= fee
        self._book_position(o.symbol, qty if o.side == "buy" else -qty, price, cs)
        self._emit(o, "filled" if o.remaining <= 1e-12 else "partial")

    def _spawn(self, coro) -> None:
        async def guard():
            try:
                await coro
            except Exception:  # noqa: BLE001
                log.exception("SIM_TASK_ERROR %s", self.name)
        t = asyncio.get_running_loop().create_task(guard())
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    def _taker_price(self, symbol: str, side: str) -> float | None:
        q = self._fresh(symbol)
        if q is None:
            return None
        return q.ask * (1.0 + self.slip) if side == "buy" else q.bid * (1.0 - self.slip)

    async def _fill_market_later(self, o: SimOrder, submit_price: float) -> None:
        await asyncio.sleep(self.latency_s)
        if o.state in TERMINAL:
            return
        # the market moves during the latency: fill at the touch that exists NOW (submit price if the feed died)
        self._fill(o, o.qty, self._taker_price(o.symbol, o.side) or submit_price, "taker")

    # ---- Trading --------------------------------------------------------------
    async def place_market(self, symbol: str, side: str, qty: float, reduce_only: bool, client_id: str) -> OrderAck:
        q = self._fresh(symbol)
        if q is None:
            return OrderAck(False, error="no quote")
        if qty <= 0:
            return OrderAck(False, error="qty must be > 0")
        o = SimOrder(client_id, f"sim-{next(self._seq)}", symbol, side, qty, 0.0, False, reduce_only,
                     self.clock() + self.latency_s)
        self._orders[client_id] = o
        self._emit(o, "ack")                                          # live venues push new/ack before the fill
        price = q.ask * (1.0 + self.slip) if side == "buy" else q.bid * (1.0 - self.slip)
        self._spawn(self._fill_market_later(o, price))
        return OrderAck(True, o.order_id, latency_ms=self.latency_s * 1000.0)

    async def place_post_only(self, symbol: str, side: str, qty: float, price: float, reduce_only: bool,
                              client_id: str) -> OrderAck:
        q = self._fresh(symbol)
        if q is None:
            return OrderAck(False, error="no quote")
        if qty <= 0:
            return OrderAck(False, error="qty must be > 0")
        o = SimOrder(client_id, f"sim-{next(self._seq)}", symbol, side, qty, price, True, reduce_only,
                     self.clock() + self.latency_s)
        self._orders[client_id] = o
        if (side == "sell" and price <= q.bid) or (side == "buy" and price >= q.ask):
            o.error = "post-only would cross"
            self._emit(o, "rejected")
            return OrderAck(False, o.order_id, error="post-only would cross")
        self._resting.setdefault(symbol, {})[client_id] = o
        self._emit(o, "ack")
        return OrderAck(True, o.order_id, latency_ms=self.latency_s * 1000.0)

    def on_quote(self, bbo: BBO) -> None:
        """Called by the app for every stored quote of this venue: fill resting orders the touch crossed."""
        if bbo.venue != self.name:
            return
        for o in list(self._resting.get(bbo.symbol, {}).values()):
            if o.state not in ("ack", "partial") or bbo.ts_local < o.live_ts:
                continue
            if o.side == "sell" and bbo.bid >= o.price:
                touch = (bbo.bid, bbo.bid_qty)
            elif o.side == "buy" and bbo.ask <= o.price:
                touch = (bbo.ask, bbo.ask_qty)
            else:
                continue
            if touch == o.last_touch:
                continue                                              # the same book re-pushed: no new flow
            o.last_touch = touch
            lot = self._spec(o.symbol).lot
            cap = math.floor(self.frac * touch[1] / lot) * lot        # a sub-lot touch fills nothing
            qty = min(o.remaining, cap)
            if qty > 0:
                self._fill(o, qty, o.price, "maker")

    async def cancel(self, symbol: str, client_id: str, order_id: str) -> OrderAck:
        o = self._orders.get(client_id)
        if o is None:
            return OrderAck(False, order_id, error="unknown order")
        if o.state in TERMINAL:
            return OrderAck(False, o.order_id, error="terminal")
        self._emit(o, "canceled")
        return OrderAck(True, o.order_id)

    async def amend(self, symbol: str, client_id: str, order_id: str, new_price: float) -> OrderAck:
        o = self._orders.get(client_id)
        if o is None or not o.post_only or o.state not in ("ack", "partial"):
            return OrderAck(False, order_id, error="not resting")
        q = self._fresh(symbol)
        if q is None:
            return OrderAck(False, o.order_id, error="no quote")
        if (o.side == "sell" and new_price <= q.bid) or (o.side == "buy" and new_price >= q.ask):
            return OrderAck(False, o.order_id, error="post-only would cross")
        o.price = new_price
        o.live_ts = self.clock() + self.latency_s
        o.last_touch = (0.0, 0.0)
        return OrderAck(True, o.order_id)

    async def query_order(self, symbol: str, client_id: str, order_id: str) -> OrderEvent | None:
        o = self._orders.get(client_id)
        return self._event(o) if o else None

    async def open_orders(self) -> list[OrderEvent]:
        return [self._event(o) for by_sym in self._resting.values() for o in by_sym.values()
                if o.state in ("ack", "partial")]

    async def positions(self) -> list[VenuePosition]:
        return [VenuePosition(self.name, s, "long" if q > 0 else "short", abs(q))
                for s, q in self._pos.items() if abs(q) > 1e-12]

    async def balance(self) -> dict[str, float]:
        return {"available": self._cash, "total": self._cash}

    async def set_leverage(self, symbol: str, leverage: int) -> None:
        return None
