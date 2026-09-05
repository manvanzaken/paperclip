"""Executor: turns Intents into orders and OrderEvents into position transitions.

Two flows, used identically for entries (open both legs) and exits (reduce-only on both legs):
- TakerTaker: both legs at market in parallel; each leg awaits its terminal OrderEvent from the private
  feed, falling back to REST `query_order` polling after EVENT_GRACE_S; a market order with no terminal
  state after POLL_MAX_S is assumed filled at the submitted size (MEXC status lag); a one-leg failure
  flattens the filled leg (retry ladder → DEGRADED).
- PeggedMaker: a post-only order rests on the maker venue; the FIRST fill event cancels the remainder and
  every fill event hedges the unhedged delta at market on the other venue; the strategy drives
  requote / cancel / upgrade through intents; when the resting order is terminal and everything is
  hedged the position opens (entry) or closes (exit). Hedge failure → flatten the maker fill.

Order events are matched by clientOrderId, so an event may arrive before the REST ack returns.
All venue calls go through the per-venue RateBudget; hedges, flattens, closes and cancels use the reserve."""
from __future__ import annotations

import asyncio
import itertools
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from .config import Config
from .edge import spread_pct
from .metrics import Metrics
from .models import (Intent, OrderEvent, Position, TT_ENTERING, MAKER_RESTING, HEDGING, OPEN,
                     EXIT_MAKER_RESTING, TT_EXITING, EXIT_HEDGING, DEGRADED)
from .positions import PositionBook, transition
from .quotes import QuoteBoard
from .risk import RiskManager
from .sizing import size_pair, hedge_plan, excess_to_flatten, lots_floor, notional
from .venues.base import Venue

log = logging.getLogger("bbo.exec")

EVENT_GRACE_S = 1.5
POLL_S = 0.3
POLL_MAX_S = 5.0
HEDGE_RETRIES = 3
HEDGE_RETRY_S = 0.2
FLATTEN_LADDER_S = (1.0, 2.0, 5.0, 10.0, 10.0, 10.0)
DEGRADED_RETRY_S = 30.0
MAX_CLOSE_RETRIES = 40           # ~20 min of DEGRADED retries, then the position waits for a human


@dataclass
class LegTrack:
    pos_id: int
    leg: str
    venue: str
    symbol: str
    qty: float
    submitted_ts: float
    last: OrderEvent | None = None
    done: asyncio.Future | None = None
    acked: bool = False       # the venue accepted the order at some point (a later "rejected" is post-rest)


class Executor:
    def __init__(self, cfg: Config, venues: dict[str, Venue], board: QuoteBoard, book: PositionBook,
                 risk: RiskManager, metrics: Metrics, notify: Callable[[str], Awaitable[None]] | None = None,
                 clock=time.time, sleep=asyncio.sleep):
        self.cfg = cfg
        self.venues = venues
        self.board = board
        self.book = book
        self.risk = risk
        self.metrics = metrics
        self.notify = notify
        self.clock = clock
        self._sleep = sleep
        self._tracks: dict[str, LegTrack] = {}
        self._seq = itertools.count(1)
        self._locks: dict[int, asyncio.Lock] = {}
        self._tasks: set[asyncio.Task] = set()
        self.busy: set[str] = set()   # symbols with an entry in flight before the position exists

    # ---- plumbing ----------------------------------------------------------------
    def _new_cid(self, pos: Position, leg: str) -> str:
        cid = f"b{self.cfg.mode[0]}{pos.id}-{leg}-{next(self._seq)}"
        return cid[:32]

    def _lock(self, pos_id: int) -> asyncio.Lock:
        return self._locks.setdefault(pos_id, asyncio.Lock())

    def _spawn(self, coro) -> None:
        t = asyncio.get_running_loop().create_task(self._guard(coro))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def _guard(self, coro) -> None:
        try:
            await coro
        except Exception:  # noqa: BLE001 — never let one event kill the loop
            log.exception("EXEC_TASK_ERROR")

    def _say(self, text: str) -> None:
        if self.notify is not None:
            self._spawn(self.notify(text))

    def _note_rate_limit(self, venue: str, error: str) -> None:
        e = (error or "").lower()
        if "429" in e or "too frequent" in e or "rate limit" in e or "too many" in e:
            self.venues[venue].budget.penalize(self.clock())
            log.warning("RATE_LIMIT %s: %s", venue, error)

    def _spec(self, venue: str, symbol: str):
        return self.venues[venue].specs.get(symbol)

    # ---- order events ------------------------------------------------------------
    def on_order_event(self, ev: OrderEvent) -> None:
        tr = self._tracks.get(ev.client_id)
        if tr is None:
            log.info("ORDER_EVENT_UNKNOWN %s %s %s", ev.venue, ev.client_id, ev.state)
            return
        tr.last = ev
        if ev.state != "rejected":
            tr.acked = True
        pos = self.book.get(tr.pos_id)
        if pos is not None:
            if ev.order_id:
                pos.order_ids[tr.leg] = ev.order_id
            if ev.position_id:
                pos.venue_position_ids[ev.venue] = ev.position_id
            if ev.liquidity:
                pos.fee_liquidity[tr.leg] = ev.liquidity
            if tr.leg == "maker":
                self._on_maker_event(pos, tr, ev)
        if ev.terminal and tr.done is not None and not tr.done.done():
            tr.done.set_result(ev)

    # ---- taker leg -----------------------------------------------------------------
    async def _place_taker(self, pos: Position, venue: str, leg: str, side: str, qty: float,
                           reduce_only: bool, priority: bool = False) -> OrderEvent:
        v = self.venues[venue]
        now = self.clock()
        if not v.budget.try_take("order", now, priority=priority):
            self.metrics.funnel["budget"] += 1
            return OrderEvent(venue, "", "", "rejected", error="budget", ts=now)
        cid = self._new_cid(pos, leg)
        tr = LegTrack(pos.id, leg, venue, pos.symbol, qty, now, done=asyncio.get_running_loop().create_future())
        self._tracks[cid] = tr
        pos.client_ids[leg] = cid
        t0 = asyncio.get_running_loop().time()
        ack = await v.trading.place_market(pos.symbol, side, qty, reduce_only, cid)
        ack_ms = (asyncio.get_running_loop().time() - t0) * 1000.0
        self.metrics.record("submit_to_ack", ack_ms)
        self.book.audit_order({"ts": now, "pos": pos.id, "leg": leg, "venue": venue, "side": side, "qty": qty,
                               "type": "market", "reduce_only": reduce_only, "client_id": cid,
                               "ok": ack.ok, "error": ack.error})
        if not ack.ok:
            self._note_rate_limit(venue, ack.error)
            log.warning("ORDER_REJECTED #%d %s %s: %s", pos.id, venue, leg, ack.error)
            return OrderEvent(venue, cid, "", "rejected", error=ack.error or "rejected", ts=self.clock())
        if ack.order_id:
            pos.order_ids[leg] = ack.order_id
        ev = await self._await_terminal(tr, v, cid, ack.order_id, qty)
        pos.latency_ms[leg] = (asyncio.get_running_loop().time() - t0) * 1000.0
        self.metrics.record("submit_to_fill", pos.latency_ms[leg])
        return ev

    async def _await_terminal(self, tr: LegTrack, v: Venue, cid: str, order_id: str, qty: float) -> OrderEvent:
        try:
            return await asyncio.wait_for(asyncio.shield(tr.done), timeout=EVENT_GRACE_S)
        except asyncio.TimeoutError:
            pass
        loop = asyncio.get_running_loop()
        deadline = loop.time() + POLL_MAX_S
        while loop.time() < deadline:
            if tr.done.done():
                return tr.done.result()
            ev = await v.trading.query_order(tr.symbol, cid, order_id)
            if ev is not None and ev.terminal:
                return ev
            await self._sleep(POLL_S)
        if tr.done.done():
            return tr.done.result()
        log.warning("FILL_ASSUMED %s %s: no terminal state after %.1fs, trusting submitted qty",
                    v.name, cid, POLL_MAX_S)
        last = tr.last
        return OrderEvent(v.name, cid, order_id, "filled", filled_qty=qty,
                          avg_price=last.avg_price if last else 0.0, fee=last.fee if last else 0.0,
                          liquidity="taker", ts=self.clock())

    # ---- TT entry ------------------------------------------------------------------
    async def enter_tt(self, intent: Intent) -> Position | None:
        a, b, sym = intent.venue_a, intent.venue_b, intent.symbol
        qa, qb = self.board.get(a, sym), self.board.get(b, sym)
        spec_a, spec_b = self._spec(a, sym), self._spec(b, sym)
        if qa is None or qb is None or spec_a is None or spec_b is None:
            return None
        legs = size_pair(intent.size_usd, qa.bid, qb.ask, spec_a, spec_b, self.cfg.max_leg_mismatch_pct,
                         min_usd=self.cfg.min_position_usd)
        if legs is None:
            self.metrics.funnel["size_fail"] += 1
            return None
        now = self.clock()
        pos = self.book.new(sym, a, b, TT_ENTERING, "TT", size_usd=legs.matched_usd, qty_a=legs.qty_a,
                            qty_b=legs.qty_b, detect_spread_pct=intent.spread_pct, entry_time=now,
                            signal_ts=intent.ts)
        if intent.ts:
            self.metrics.record("detect_to_submit", (now - intent.ts) * 1000.0)
        log.info("TT_ENTER #%d %s short %s / long %s spread=%.3f%% edge=%.3f%% size=$%.2f",
                 pos.id, sym, a, b, intent.spread_pct, intent.edge_pct, legs.matched_usd)
        return await self._tt_legs(pos)

    async def _tt_legs(self, pos: Position) -> Position | None:
        a, b, sym = pos.venue_a, pos.venue_b, pos.symbol
        spec_a, spec_b = self._spec(a, sym), self._spec(b, sym)
        ev_a, ev_b = await asyncio.gather(
            self._place_taker(pos, a, "entry_a", "sell", pos.qty_a, False),
            self._place_taker(pos, b, "entry_b", "buy", pos.qty_b, False))
        ok_a = ev_a.state == "filled" and ev_a.filled_qty > 0
        ok_b = ev_b.state == "filled" and ev_b.filled_qty > 0
        now = self.clock()
        if ok_a and ok_b:
            pos.filled_a, pos.filled_b = ev_a.filled_qty, ev_b.filled_qty
            pos.entry_price_a, pos.entry_price_b = ev_a.avg_price, ev_b.avg_price
            pos.entry_fees_usd = ev_a.fee + ev_b.fee
            pos.size_usd = min(notional(pos.filled_a, pos.entry_price_a, spec_a),
                               notional(pos.filled_b, pos.entry_price_b, spec_b))
            pos.entry_spread_pct = (spread_pct(pos.entry_price_a, pos.entry_price_b)
                                    if pos.entry_price_a > 0 and pos.entry_price_b > 0 else pos.detect_spread_pct)
            pos.peak_spread_pct = abs(pos.entry_spread_pct)
            pos.entry_time = now
            transition(pos, OPEN)
            if pos.signal_ts:
                self.metrics.record("signal_to_open", (now - pos.signal_ts) * 1000.0)
            log.info("OPEN #%d %s TT fill_spread=%.3f%% size=$%.2f fees=$%.4f lat=%.0f/%.0fms",
                     pos.id, sym, pos.entry_spread_pct, pos.size_usd, pos.entry_fees_usd,
                     pos.latency_ms.get("entry_a", 0), pos.latency_ms.get("entry_b", 0))
            self._say(f"OPEN #{pos.id} {sym} {pos.mode} short {a} / long {b} spread {pos.entry_spread_pct:.3f}% ${pos.size_usd:.2f}")
            if pos.entry_spread_pct < self.cfg.min_fill_spread_pct:
                log.warning("FILL_QUALITY_ABORT #%d %s fill_spread=%.3f%% < %.3f%%", pos.id, sym,
                            pos.entry_spread_pct, self.cfg.min_fill_spread_pct)
                self.risk.set_cooldown(sym)
                await self.exit_tt(pos, "fill_quality_abort")
                return None
            self.book.dirty = True
            return pos
        if ok_a != ok_b:
            leg = "a" if ok_a else "b"
            ev = ev_a if ok_a else ev_b
            log.warning("LEG_DESYNC #%d %s: leg %s filled, other failed (%s)", pos.id, sym, leg,
                        (ev_b if ok_a else ev_a).error)
            if leg == "a":
                pos.filled_a, pos.entry_price_a = ev.filled_qty, ev.avg_price
                pos.size_usd = notional(ev.filled_qty, ev.avg_price, spec_a)
            else:
                pos.filled_b, pos.entry_price_b = ev.filled_qty, ev.avg_price
                pos.size_usd = notional(ev.filled_qty, ev.avg_price, spec_b)
            pos.entry_fees_usd = ev.fee
            self.risk.record_strike(sym, a, b)
            self.risk.set_cooldown(sym)
            if await self._flatten_leg(pos, leg, ev.filled_qty):
                self._close(pos, "failed_entry")
            else:
                transition(pos, DEGRADED)
                pos.degraded_leg = leg
                pos.last_close_attempt = self.clock()
            return None
        log.warning("ENTRY_FAILED #%d %s both legs: %s / %s", pos.id, sym, ev_a.error, ev_b.error)
        self.risk.record_strike(sym, a, b)
        self.risk.set_cooldown(sym)
        self.book.discard(pos)
        return None

    async def _flatten_leg(self, pos: Position, leg: str, qty: float) -> bool:
        """Market-close one entry leg (reduce-only) with the retry ladder. Records exit price/fees."""
        venue = pos.venue_a if leg == "a" else pos.venue_b
        side = "buy" if leg == "a" else "sell"
        for i, delay in enumerate(FLATTEN_LADDER_S):
            ev = await self._place_taker(pos, venue, f"flat_{leg}{i}", side, qty, True, priority=True)
            if ev.state == "filled" and ev.filled_qty > 0:
                self._accumulate_leg(pos, venue, "exit", ev)
                log.info("FLATTEN #%d %s %s ok qty=%s px=%s", pos.id, venue, leg, ev.filled_qty, ev.avg_price)
                return True
            log.warning("FLATTEN #%d %s attempt %d failed: %s", pos.id, venue, i + 1, ev.error)
            await self._sleep(delay)
        return False

    # ---- TM entry (PeggedMaker) -------------------------------------------------------
    async def enter_tm(self, intent: Intent) -> Position | None:
        a, b, sym, mv = intent.venue_a, intent.venue_b, intent.symbol, intent.maker_venue
        qa, qb = self.board.get(a, sym), self.board.get(b, sym)
        spec_a, spec_b = self._spec(a, sym), self._spec(b, sym)
        if qa is None or qb is None or spec_a is None or spec_b is None or mv not in (a, b):
            return None
        px_a = intent.rest_price if mv == a else qa.bid
        px_b = intent.rest_price if mv == b else qb.ask
        legs = size_pair(intent.size_usd, px_a, px_b, spec_a, spec_b, self.cfg.max_leg_mismatch_pct,
                         min_usd=self.cfg.min_position_usd)
        if legs is None:
            self.metrics.funnel["size_fail"] += 1
            return None
        now = self.clock()
        pos = self.book.new(sym, a, b, MAKER_RESTING, "TM", size_usd=legs.matched_usd, qty_a=legs.qty_a,
                            qty_b=legs.qty_b, detect_spread_pct=intent.spread_pct, entry_time=now,
                            signal_ts=intent.ts, maker_venue=mv, maker_side="sell" if mv == a else "buy",
                            maker_qty=legs.qty_a if mv == a else legs.qty_b, maker_rest_price=intent.rest_price)
        if intent.ts:
            self.metrics.record("detect_to_submit", (now - intent.ts) * 1000.0)
        if not await self._post_maker(pos, reduce_only=False):
            self.book.discard(pos)
            return None
        return pos

    async def _post_maker(self, pos: Position, reduce_only: bool, keep_posted_ts: bool = False) -> bool:
        v = self.venues[pos.maker_venue]
        now = self.clock()
        if not v.budget.try_take("order", now):
            self.metrics.funnel["budget"] += 1
            return False
        cid = self._new_cid(pos, "maker")
        self._tracks[cid] = LegTrack(pos.id, "maker", v.name, pos.symbol, pos.maker_qty, now)
        pos.maker_client_id = cid
        pos.client_ids["maker"] = cid
        if not (keep_posted_ts and pos.maker_posted_ts > 0.0):
            pos.maker_posted_ts = now          # a cancel+new requote keeps the original TTL clock
        pos.maker_last_requote_ts = now
        pos.maker_cancel_sent = False
        pos.maker_filled_qty = 0.0
        pos.hedged_qty = 0.0
        pos.maker_fee_usd = 0.0
        pos.maker_avg_price = 0.0
        ack = await v.trading.place_post_only(pos.symbol, pos.maker_side, pos.maker_qty, pos.maker_rest_price,
                                              reduce_only, cid)
        self.book.audit_order({"ts": now, "pos": pos.id, "leg": "maker", "venue": v.name, "side": pos.maker_side,
                               "qty": pos.maker_qty, "price": pos.maker_rest_price, "type": "post_only",
                               "reduce_only": reduce_only, "client_id": cid, "ok": ack.ok, "error": ack.error})
        if not ack.ok:
            self._note_rate_limit(v.name, ack.error)
            if "102127" in (ack.error or ""):
                self.risk.blacklist_venue_symbol(v.name, pos.symbol)
            self.metrics.funnel["maker_rejected"] += 1
            log.info("TM_POST_REJECTED #%d %s %s @%s: %s", pos.id, v.name, pos.maker_side, pos.maker_rest_price, ack.error)
            return False
        if ack.order_id:
            pos.maker_order_id = ack.order_id
            pos.order_ids["maker"] = ack.order_id
        log.info("TM_POST #%d %s %s %s qty=%s @%s reduce_only=%s", pos.id, pos.symbol, v.name, pos.maker_side,
                 pos.maker_qty, pos.maker_rest_price, reduce_only)
        return True

    async def requote(self, pos: Position, new_price: float) -> None:
        v = self.venues[pos.maker_venue]
        now = self.clock()
        if pos.maker_cancel_sent or pos.status not in (MAKER_RESTING, EXIT_MAKER_RESTING):
            return
        if v.trading.supports_amend:
            if not v.budget.try_take("amend", now):
                return
            pos.maker_last_requote_ts = now    # stamped on the attempt: a failing amend must not retry every quote
            ack = await v.trading.amend(pos.symbol, pos.maker_client_id, pos.maker_order_id, new_price)
            if ack.ok:
                log.info("TM_REQUOTE #%d %s %s -> %s", pos.id, v.name, pos.maker_rest_price, new_price)
                pos.maker_rest_price = new_price
            else:
                self._note_rate_limit(v.name, ack.error)
                log.info("TM_REQUOTE_FAILED #%d %s: %s", pos.id, v.name, ack.error)
            return
        if v.budget.available("cancel", now) < 1 or v.budget.available("order", now) < 1:
            return
        pos.requote_pending = True
        pos.requote_price = new_price
        pos.maker_last_requote_ts = now        # on the attempt (see the amend path)
        await self._cancel_maker_order(pos, priority=False)   # a failed cancel clears requote_pending itself

    async def cancel_maker(self, pos: Position, reason: str) -> None:
        log.info("TM_CANCEL #%d %s reason=%s", pos.id, pos.maker_venue, reason)
        await self._cancel_maker_order(pos, priority=True)

    async def upgrade_to_tt(self, pos: Position) -> None:
        if pos.status != MAKER_RESTING:
            return
        log.info("UPGRADE_TT #%d %s", pos.id, pos.symbol)
        pos.upgrade_pending = True
        await self._cancel_maker_order(pos, priority=True)

    async def _cancel_maker_order(self, pos: Position, priority: bool) -> bool:
        if pos.maker_cancel_sent:
            return True
        v = self.venues[pos.maker_venue]
        now = self.clock()
        if not v.budget.try_take("cancel", now, priority=priority):
            return False
        pos.maker_cancel_sent = True
        ack = await v.trading.cancel(pos.symbol, pos.maker_client_id, pos.maker_order_id)
        if ack.ok:
            return True
        self._note_rate_limit(v.name, ack.error)
        # already terminal at the venue (filled or gone)? pull the terminal state ourselves
        ev = await v.trading.query_order(pos.symbol, pos.maker_client_id, pos.maker_order_id)
        if ev is not None and ev.terminal:
            self.on_order_event(ev)
            return False
        # the order may still be live (transport error, venue hiccup): nothing is in flight, let the strategy retry
        log.warning("CANCEL_FAILED #%d %s %s: %s — will retry", pos.id, v.name, pos.maker_client_id, ack.error)
        pos.maker_cancel_sent = False
        pos.requote_pending = False
        pos.upgrade_pending = False
        return False

    def _on_maker_event(self, pos: Position, tr: LegTrack, ev: OrderEvent) -> None:
        if ev.state == "ack" or (ev.state == "rejected" and not tr.acked):
            return          # a pre-rest (post-only would cross) rejection is handled by _post_maker off the OrderAck
        if ev.state == "rejected":
            log.warning("TM_REJECTED_WHILE_RESTING #%d %s %s: %s", pos.id, pos.symbol, pos.maker_venue, ev.error)
        if ev.filled_qty > pos.maker_filled_qty + 1e-12:
            if pos.maker_fill_ts == 0.0:
                pos.maker_fill_ts = self.clock()
                self.metrics.record("post_to_first_fill", (pos.maker_fill_ts - pos.maker_posted_ts) * 1000.0)
            pos.maker_filled_qty = ev.filled_qty
            pos.maker_avg_price = ev.avg_price
            pos.maker_fee_usd = ev.fee
            log.info("TM_FILL #%d %s %s filled=%s/%s @%s", pos.id, pos.symbol, pos.maker_venue,
                     ev.filled_qty, pos.maker_qty, ev.avg_price)
        self._spawn(self._hedge_delta(pos, terminal=ev.terminal))

    def _hedge_side(self, pos: Position, phase: str) -> str:
        maker_is_a = pos.maker_venue == pos.venue_a
        if phase == "entry":
            return "buy" if maker_is_a else "sell"     # maker sold on A → buy B; maker bought on B → sell A
        return "sell" if maker_is_a else "buy"         # maker bought back on A → sell B; maker sold on B → buy A

    async def _hedge_delta(self, pos: Position, terminal: bool) -> None:
        """Hedge whatever the resting order has filled but we have not yet covered. The hedge is
        floored to whole hedge-venue lots, so `hedged_qty` only advances by the maker quantity the
        hedge notional really covers; the residual waits for more fills and, once the resting order
        is terminal, is flattened on the maker venue when it exceeds the mismatch tolerance."""
        async with self._lock(pos.id):
            if pos.status not in (MAKER_RESTING, HEDGING, EXIT_MAKER_RESTING, EXIT_HEDGING):
                return
            phase = "entry" if pos.status in (MAKER_RESTING, HEDGING) else "exit"
            mv = pos.maker_venue
            hv = pos.venue_b if mv == pos.venue_a else pos.venue_a
            spec_m, spec_h = self._spec(mv, pos.symbol), self._spec(hv, pos.symbol)
            unhedged = round(pos.maker_filled_qty - pos.hedged_qty, 10)
            if unhedged > 1e-12:
                if pos.status == MAKER_RESTING:
                    transition(pos, HEDGING)
                elif pos.status == EXIT_MAKER_RESTING:
                    transition(pos, EXIT_HEDGING)
                if not terminal:
                    await self._cancel_maker_order(pos, priority=True)   # first fill cancels the remainder
                side = self._hedge_side(pos, phase)
                q_h = self.board.get(hv, pos.symbol)
                px_h = ((q_h.ask if side == "buy" else q_h.bid) if q_h else pos.maker_avg_price) or pos.maker_avg_price
                plan = hedge_plan(unhedged, pos.maker_avg_price, spec_m, px_h, spec_h)
                if plan.hedge_qty > 0:
                    t_fill = pos.maker_fill_ts or self.clock()
                    ev = None
                    for attempt in range(HEDGE_RETRIES):
                        ev = await self._place_taker(pos, hv, f"hedge_{phase}{attempt}", side, plan.hedge_qty,
                                                     phase == "exit", priority=True)
                        if ev.state == "filled" and ev.filled_qty > 0:
                            break
                        await self._sleep(HEDGE_RETRY_S)
                    if ev is None or not (ev.state == "filled" and ev.filled_qty > 0):
                        log.error("HEDGE_FAILED #%d %s on %s — flattening maker fill", pos.id, pos.symbol, hv)
                        self.risk.record_strike(pos.symbol, pos.venue_a, pos.venue_b)
                        await self._flatten_maker_fill(pos, lots_floor(unhedged, spec_m), phase)
                        pos.hedged_qty = pos.maker_filled_qty
                    else:
                        pos.hedged_qty = round(pos.hedged_qty + plan.covered_maker_qty, 10)
                        self._accumulate_leg(pos, hv, phase, ev)
                        self.metrics.record("fill_to_hedged", (self.clock() - t_fill) * 1000.0)
                        log.info("TM_HEDGE #%d %s %s %s qty=%s @%s covers=%s maker", pos.id, pos.symbol, hv, side,
                                 ev.filled_qty, ev.avg_price, plan.covered_maker_qty)
                residual = round(pos.maker_filled_qty - pos.hedged_qty, 10)
                if residual > 1e-12 and terminal:
                    to_flat = excess_to_flatten(residual, pos.hedged_qty, spec_m, self.cfg.max_leg_mismatch_pct)
                    if to_flat > 0:
                        log.warning("RESIDUAL #%d %s %s maker contracts unhedged (matched %s) — flattening %s",
                                    pos.id, pos.symbol, residual, pos.hedged_qty, to_flat)
                        await self._flatten_maker_fill(pos, to_flat, phase)
                    elif residual <= pos.hedged_qty * self.cfg.max_leg_mismatch_pct / 100.0:
                        log.info("RESIDUAL_ACCEPTED #%d %s %s maker contracts within tolerance", pos.id, pos.symbol, residual)
                    else:
                        log.warning("RESIDUAL_RETAINED #%d %s %s maker contracts below the venue minimum — cannot be sent, exposure retained",
                                    pos.id, pos.symbol, residual)
                    pos.hedged_qty = pos.maker_filled_qty
            tr = self._tracks.get(pos.maker_client_id)
            maker_terminal = tr is not None and tr.last is not None and tr.last.terminal
            if maker_terminal and round(pos.maker_filled_qty - pos.hedged_qty, 10) <= 1e-12:
                await self._finalize_maker(pos, phase)

    async def _flatten_maker_fill(self, pos: Position, qty: float, phase: str) -> None:
        """Undo an unhedgeable/unhedged maker fill on the maker venue itself (reduce-only market)."""
        if qty <= 0:
            return
        side = "buy" if pos.maker_side == "sell" else "sell"
        for i, delay in enumerate(FLATTEN_LADDER_S[:3]):
            ev = await self._place_taker(pos, pos.maker_venue, f"mflat{i}", side, qty, True, priority=True)
            if ev.state == "filled" and ev.filled_qty > 0:
                pos.exit_fees_usd += ev.fee
                pos.maker_filled_qty = round(pos.maker_filled_qty - qty, 10)   # netted out
                log.info("FLATTEN #%d maker leg %s qty=%s", pos.id, pos.maker_venue, qty)
                return
            await self._sleep(delay)
        log.error("FLATTEN_FAILED #%d maker leg %s qty=%s — DEGRADED", pos.id, pos.maker_venue, qty)
        transition(pos, DEGRADED)
        pos.degraded_leg = "a" if pos.maker_venue == pos.venue_a else "b"
        pos.last_close_attempt = self.clock()

    def _accumulate_leg(self, pos: Position, venue: str, phase: str, ev: OrderEvent) -> None:
        is_a = venue == pos.venue_a
        if phase == "entry":
            q_attr, p_attr = ("filled_a", "entry_price_a") if is_a else ("filled_b", "entry_price_b")
            pos.entry_fees_usd += ev.fee
        else:
            q_attr, p_attr = ("exit_filled_a", "exit_price_a") if is_a else ("exit_filled_b", "exit_price_b")
            pos.exit_fees_usd += ev.fee
        q0, p0 = getattr(pos, q_attr), getattr(pos, p_attr)
        q1 = q0 + ev.filled_qty
        setattr(pos, p_attr, (p0 * q0 + ev.avg_price * ev.filled_qty) / q1 if q1 > 0 else 0.0)
        setattr(pos, q_attr, round(q1, 10))

    def _apply_maker_leg(self, pos: Position, phase: str) -> None:
        is_a = pos.maker_venue == pos.venue_a
        if phase == "entry":
            if is_a:
                pos.filled_a, pos.entry_price_a = pos.maker_filled_qty, pos.maker_avg_price
            else:
                pos.filled_b, pos.entry_price_b = pos.maker_filled_qty, pos.maker_avg_price
            pos.entry_fees_usd += pos.maker_fee_usd
        else:
            if is_a:
                pos.exit_filled_a, pos.exit_price_a = pos.maker_filled_qty, pos.maker_avg_price
            else:
                pos.exit_filled_b, pos.exit_price_b = pos.maker_filled_qty, pos.maker_avg_price
            pos.exit_fees_usd += pos.maker_fee_usd
        pos.fee_liquidity["maker"] = "maker"

    async def _finalize_maker(self, pos: Position, phase: str) -> None:
        now = self.clock()
        spec_a, spec_b = self._spec(pos.venue_a, pos.symbol), self._spec(pos.venue_b, pos.symbol)
        if phase == "entry":
            if pos.maker_filled_qty <= 1e-12:
                if pos.requote_pending and pos.status == MAKER_RESTING:
                    pos.requote_pending = False
                    pos.upgrade_pending = False        # an upgrade asked for mid-requote is stale: re-evaluate fresh
                    pos.maker_rest_price = pos.requote_price
                    if await self._post_maker(pos, reduce_only=False, keep_posted_ts=True):
                        return
                if pos.upgrade_pending and pos.status == MAKER_RESTING:
                    pos.upgrade_pending = False
                    pos.mode = "TT"
                    transition(pos, TT_ENTERING)
                    await self._tt_legs(pos)
                    return
                self.metrics.funnel["maker_cancelled"] += 1
                self.book.discard(pos)
                return
            self._apply_maker_leg(pos, "entry")
            pos.size_usd = min(notional(pos.filled_a, pos.entry_price_a, spec_a),
                               notional(pos.filled_b, pos.entry_price_b, spec_b))
            pos.entry_spread_pct = (spread_pct(pos.entry_price_a, pos.entry_price_b)
                                    if pos.entry_price_a > 0 and pos.entry_price_b > 0 else pos.detect_spread_pct)
            pos.peak_spread_pct = abs(pos.entry_spread_pct)
            pos.entry_time = now
            if pos.status == MAKER_RESTING:
                transition(pos, HEDGING)
            transition(pos, OPEN)
            if pos.signal_ts:
                self.metrics.record("signal_to_open", (now - pos.signal_ts) * 1000.0)
            log.info("OPEN #%d %s TM maker=%s fill_spread=%.3f%% size=$%.2f fees=$%.4f", pos.id, pos.symbol,
                     pos.maker_venue, pos.entry_spread_pct, pos.size_usd, pos.entry_fees_usd)
            self._say(f"OPEN #{pos.id} {pos.symbol} TM maker {pos.maker_venue} spread {pos.entry_spread_pct:.3f}% ${pos.size_usd:.2f}")
            self.book.dirty = True
            return
        # exit phase
        if pos.maker_filled_qty <= 1e-12:
            tr = self._tracks.get(pos.maker_client_id)
            if (tr is not None and tr.last is not None and tr.last.state == "rejected"
                    and "nothing to reduce" in (tr.last.error or "")
                    and await self._leg_flat_at_venue(pos, pos.maker_venue)):
                # the venue holds nothing on the maker leg: book it closed at the mark, close the other leg TT
                self._book_leg_flat(pos, "a" if pos.maker_venue == pos.venue_a else "b", pos.maker_venue)
                pos.requote_pending = False
                pos.exit_reason = pos.exit_reason or "venue_flat"
                transition(pos, TT_EXITING)
                await self._close_remainder_tt(pos)
                return
            if pos.requote_pending and pos.status == EXIT_MAKER_RESTING:
                pos.requote_pending = False
                pos.maker_rest_price = pos.requote_price
                if await self._post_maker(pos, reduce_only=True):
                    return
            if pos.exit_reason:
                transition(pos, TT_EXITING)
                await self._close_remainder_tt(pos)
                return
            if pos.status in (EXIT_MAKER_RESTING, EXIT_HEDGING):
                if pos.status == EXIT_HEDGING:
                    transition(pos, TT_EXITING)
                    await self._close_remainder_tt(pos)
                    return
                transition(pos, OPEN)
                pos.maker_venue = ""
            return
        self._apply_maker_leg(pos, "exit")
        if pos.status == EXIT_MAKER_RESTING:
            transition(pos, EXIT_HEDGING)
        rem_a = round(pos.filled_a - pos.exit_filled_a, 10)
        rem_b = round(pos.filled_b - pos.exit_filled_b, 10)
        if rem_a > 1e-9 or rem_b > 1e-9:
            transition(pos, TT_EXITING)
            await self._close_remainder_tt(pos)
            return
        self._close(pos, pos.exit_reason or "take_profit")

    # ---- exits ---------------------------------------------------------------------
    async def exit_tm(self, pos: Position, intent: Intent) -> bool:
        if pos.status != OPEN or intent.maker_venue not in (pos.venue_a, pos.venue_b):
            return False
        mv = intent.maker_venue
        pos.maker_venue = mv
        pos.maker_side = "buy" if mv == pos.venue_a else "sell"
        pos.maker_qty = round((pos.filled_a - pos.exit_filled_a) if mv == pos.venue_a else (pos.filled_b - pos.exit_filled_b), 10)
        pos.maker_rest_price = intent.rest_price
        pos.exit_reason = ""
        pos.exit_mode = "TM"
        pos.maker_fill_ts = 0.0
        pos.upgrade_pending = False
        pos.requote_pending = False
        pos.edge_gone_since = 0.0
        if pos.maker_qty <= 0:
            return False
        transition(pos, EXIT_MAKER_RESTING)
        if not await self._post_maker(pos, reduce_only=True):
            transition(pos, OPEN)
            pos.maker_venue = ""
            return False
        return True

    async def exit_tt(self, pos: Position, reason: str) -> None:
        pos.exit_reason = reason
        if pos.status == EXIT_MAKER_RESTING:
            pos.exit_mode = "TM+TT"
            await self._cancel_maker_order(pos, priority=True)   # finalize closes the remainder TT
            return
        if pos.status != OPEN:
            return
        pos.exit_mode = pos.exit_mode or "TT"
        transition(pos, TT_EXITING)
        log.info("TT_EXIT #%d %s reason=%s", pos.id, pos.symbol, reason)
        await self._close_remainder_tt(pos)

    async def _close_remainder_tt(self, pos: Position) -> None:
        rem_a = round(pos.filled_a - pos.exit_filled_a, 10)
        rem_b = round(pos.filled_b - pos.exit_filled_b, 10)
        jobs = []
        if rem_a > 1e-9:
            jobs.append(("a", self._place_taker(pos, pos.venue_a, "exit_a", "buy", rem_a, True, priority=True)))
        if rem_b > 1e-9:
            jobs.append(("b", self._place_taker(pos, pos.venue_b, "exit_b", "sell", rem_b, True, priority=True)))
        results = await asyncio.gather(*(j[1] for j in jobs)) if jobs else []
        failed = ""
        for (leg, _), ev in zip(jobs, results):
            if ev.state == "filled" and ev.filled_qty > 0:
                self._accumulate_leg(pos, pos.venue_a if leg == "a" else pos.venue_b, "exit", ev)
            else:
                failed += leg
        if failed:
            log.warning("CLOSE_DEGRADED #%d %s legs=%s", pos.id, pos.symbol, failed)
            transition(pos, DEGRADED)
            pos.degraded_leg = "both" if failed == "ab" else failed
            pos.close_retry_count += 1
            pos.last_close_attempt = self.clock()
            return
        self._close(pos, pos.exit_reason or "convergence")

    async def _leg_flat_at_venue(self, pos: Position, venue: str) -> bool:
        """A reduce-only close was refused with "nothing to reduce": does the venue really hold no position?"""
        try:
            held = await self.venues[venue].trading.positions()
        except Exception as e:  # noqa: BLE001
            log.warning("POSITIONS_QUERY_FAILED %s: %r", venue, e)
            return False
        return not any(p.symbol == pos.symbol for p in held)

    def _book_leg_flat(self, pos: Position, leg: str, venue: str) -> None:
        """The venue holds nothing for this leg although we do: book it closed at the mark so the position can
        finish, and say so loudly — our accounting and the venue disagreed (Plan 2 reconciliation owns this)."""
        q = self.board.get(venue, pos.symbol)
        mark = q.mid if q is not None else (pos.entry_price_a if leg == "a" else pos.entry_price_b)
        log.error("LEG_FLAT_AT_VENUE #%d %s leg %s on %s: venue holds no position — booked closed at mark %s",
                  pos.id, pos.symbol, leg, venue, mark)
        if leg == "a":
            pos.exit_filled_a, pos.exit_price_a = pos.filled_a, mark
        else:
            pos.exit_filled_b, pos.exit_price_b = pos.filled_b, mark

    async def retry_degraded(self) -> None:
        now = self.clock()
        for pos in list(self.book.by_status(DEGRADED)):
            if now - pos.last_close_attempt < DEGRADED_RETRY_S:
                continue
            if pos.close_retry_count >= MAX_CLOSE_RETRIES:
                if pos.close_retry_count == MAX_CLOSE_RETRIES:
                    pos.close_retry_count += 1
                    log.error("DEGRADED_STUCK #%d %s: %d close attempts failed — manual intervention needed",
                              pos.id, pos.symbol, MAX_CLOSE_RETRIES)
                    self._say(f"DEGRADED_STUCK #{pos.id} {pos.symbol}: close it by hand")
                continue
            pos.last_close_attempt = now
            pos.close_retry_count += 1
            ok = True
            for leg, venue, side in (("a", pos.venue_a, "buy"), ("b", pos.venue_b, "sell")):
                rem = round((pos.filled_a - pos.exit_filled_a) if leg == "a" else (pos.filled_b - pos.exit_filled_b), 10)
                if rem <= 1e-9:
                    continue
                ev = await self._place_taker(pos, venue, f"retry_{leg}{pos.close_retry_count}", side, rem, True, priority=True)
                if ev.state == "filled" and ev.filled_qty > 0:
                    self._accumulate_leg(pos, venue, "exit", ev)
                elif "nothing to reduce" in (ev.error or "") and await self._leg_flat_at_venue(pos, venue):
                    self._book_leg_flat(pos, leg, venue)
                else:
                    ok = False
            if ok:
                self._close(pos, pos.exit_reason or "recovered")

    def _close(self, pos: Position, reason: str) -> None:
        self.book.close(pos, reason, self.clock())
        self.risk.record_close(pos)
        log.info("CLOSE #%d %s reason=%s pnl=$%+.4f gross=$%+.4f fees=$%.4f exit_spread=%.3f%%", pos.id, pos.symbol,
                 reason, pos.net_pnl_usd, pos.gross_pnl_usd, pos.entry_fees_usd + pos.exit_fees_usd, pos.exit_spread_pct)
        self._say(f"CLOSE #{pos.id} {pos.symbol} {reason} pnl ${pos.net_pnl_usd:+.4f}")

    async def cancel_all_resting(self) -> None:
        for pos in list(self.book.by_status(MAKER_RESTING, EXIT_MAKER_RESTING)):
            await self._cancel_maker_order(pos, priority=True)