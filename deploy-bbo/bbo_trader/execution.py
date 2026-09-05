"""Executor: turns Intents into orders and OrderEvents into position transitions.

Two flows, used identically for entries (open both legs) and exits (reduce-only on both legs):
- TakerTaker: both legs at market in parallel; each leg awaits its terminal OrderEvent from the private
  feed, falling back to REST `query_order` polling after EVENT_GRACE_S; a market order with no terminal
  state after POLL_MAX_S is assumed filled at the submitted size (MEXC status lag); a one-leg failure
  flattens the filled leg (retry ladder → DEGRADED).
- PeggedMaker: a post-only order rests on the maker venue; the FIRST fill event cancels the remainder and
  every fill event hedges the unhedged delta at market on the other venue; the strategy drives
  requote / cancel / upgrade through intents; when the resting order is terminal and everything is
  hedged the position opens (entry) or closes (exit). Entry hedge failure → flatten the maker fill; exit
  hedge failure → the remainder closes taker/taker.

Failure discipline: a venue exception leaves an order IN DOUBT (polled, then reported as rejected with error
"in_doubt" — never assumed filled); every close path decides on the REMAINING quantity, not on "some fill
arrived"; a fill on a superseded maker order (fill-after-cancel) is booked on the leg and the position handed
to `retry_degraded`, never written onto the live order's counters; a reduce-only order refused with
"nothing to reduce" books the leg closed once the venue confirms it holds nothing (LEG_FLAT_AT_VENUE).

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
from .models import (Intent, OrderAck, OrderEvent, Position, TT_ENTERING, MAKER_RESTING, HEDGING, OPEN,
                     EXIT_MAKER_RESTING, TT_EXITING, EXIT_HEDGING, DEGRADED, CLOSED)
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
HEDGING_SWEEP_AFTER_S = 2.0      # a HEDGING position whose maker order is still live this long gets its cancel retried
#                                  (the sweep runs inside retry_degraded, which the App calls from its 500 ms sweep)
MAX_CID_LEN = 32                 # MEXC externalOid / BloFin clientOrderId
MAX_TRACKS = 5000                # maker tracks outlive their position (fill-after-cancel alerts); oldest are dropped


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
    filled_seen: float = 0.0  # cumulative fill already processed for THIS order (stray-fill deltas)
    fee_seen: float = 0.0
    phase: str = ""           # maker orders: "entry" | "exit" as posted — survives pos.maker_venue being cleared


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

    # ---- plumbing ----------------------------------------------------------------
    def _new_cid(self, pos: Position, leg: str) -> str:
        cid = f"b{self.cfg.mode[0]}{pos.id}-{leg}-{next(self._seq)}"
        if len(cid) > MAX_CID_LEN:
            log.error("CID_TRUNCATED %s — idempotency key no longer unique", cid)
        return cid[:MAX_CID_LEN]

    def _forget(self, pos: Position) -> None:
        """Drop the taker tracks and the lock of a finished position. Maker tracks are kept (bounded) so a
        fill-after-cancel on a closed position is still recognised and raised to a human."""
        for cid in [c for c, t in self._tracks.items() if t.pos_id == pos.id and t.leg != "maker"]:
            del self._tracks[cid]
        while len(self._tracks) > MAX_TRACKS:
            self._tracks.pop(next(iter(self._tracks)))
        self._locks.pop(pos.id, None)

    @staticmethod
    def _maker_phase(pos: Position) -> str:
        entry_side = "sell" if pos.maker_venue == pos.venue_a else "buy"
        return "entry" if pos.maker_side == entry_side else "exit"

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
            if ev.filled_qty > 0:
                log.error("ORDER_EVENT_UNKNOWN %s %s %s filled=%s — an order we do not track has filled: reconcile by hand",
                          ev.venue, ev.client_id, ev.state, ev.filled_qty)
                self._say(f"UNKNOWN_FILL {ev.venue} {ev.client_id}: {ev.filled_qty} contracts")
            else:
                log.info("ORDER_EVENT_UNKNOWN %s %s %s", ev.venue, ev.client_id, ev.state)
            return
        tr.last = ev
        if ev.state != "rejected":
            tr.acked = True
        pos = self.book.get(tr.pos_id, include_closed=True)
        if pos is None and ev.filled_qty > tr.filled_seen + 1e-12:      # a discarded position's order filled after all
            log.error("STRAY_FILL_NO_POSITION #%d %s %s: %s contracts on %s — reconcile by hand",
                      tr.pos_id, tr.symbol, tr.venue, ev.filled_qty - tr.filled_seen, ev.client_id)
            self._say(f"STRAY_FILL_NO_POSITION #{tr.pos_id} {tr.symbol} {tr.venue}: {ev.filled_qty - tr.filled_seen} contracts")
            tr.filled_seen = ev.filled_qty
        if pos is not None:
            if ev.order_id:
                pos.order_ids[tr.leg] = ev.order_id
                if tr.leg == "maker" and ev.client_id == pos.maker_client_id:
                    pos.maker_order_id = ev.order_id
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
        """Market order + its terminal event. `OrderAck.ok=False` is a definitive venue rejection (live adapters
        raise on transport/envelope failures instead); an exception leaves the order IN DOUBT — the venue is
        polled for it and, if it stays unknown, the leg is reported rejected with error "in_doubt" rather than
        assumed filled. Callers then flatten/retry with reduce-only orders, which the venue refuses with
        "nothing to reduce" if the doubted order did fill after all (→ LEG_FLAT_AT_VENUE)."""
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
        in_doubt = False
        try:
            ack = await v.trading.place_market(pos.symbol, side, qty, reduce_only, cid)
        except Exception as e:  # noqa: BLE001 — the venue may or may not hold this order
            log.error("ORDER_IN_DOUBT #%d %s %s: %r — polling the venue", pos.id, venue, leg, e)
            self._note_rate_limit(venue, repr(e))
            ack = OrderAck(True, "", error=f"in_doubt: {e!r}")
            in_doubt = True
        ack_ms = (asyncio.get_running_loop().time() - t0) * 1000.0
        self.metrics.record("submit_to_ack", ack_ms)
        self.book.audit_order({"ts": now, "pos": pos.id, "leg": leg, "venue": venue, "side": side, "qty": qty,
                               "type": "market", "reduce_only": reduce_only, "client_id": cid,
                               "ok": ack.ok and not in_doubt, "error": ack.error})
        if not ack.ok:
            self._note_rate_limit(venue, ack.error)
            log.warning("ORDER_REJECTED #%d %s %s: %s", pos.id, venue, leg, ack.error)
            return OrderEvent(venue, cid, "", "rejected", error=ack.error or "rejected", ts=self.clock())
        if ack.order_id:
            pos.order_ids[leg] = ack.order_id
        ev = await self._await_terminal(tr, v, cid, ack.order_id, qty, assume_filled=not in_doubt)
        pos.latency_ms[leg] = (asyncio.get_running_loop().time() - t0) * 1000.0
        if ev.state == "filled":
            self.metrics.record("submit_to_fill", pos.latency_ms[leg])
        return ev

    async def _await_terminal(self, tr: LegTrack, v: Venue, cid: str, order_id: str, qty: float,
                              assume_filled: bool = True) -> OrderEvent:
        try:
            return await asyncio.wait_for(asyncio.shield(tr.done), timeout=EVENT_GRACE_S)
        except asyncio.TimeoutError:
            pass
        loop = asyncio.get_running_loop()
        deadline = loop.time() + POLL_MAX_S
        while loop.time() < deadline:
            if tr.done.done():
                return tr.done.result()
            try:
                ev = await v.trading.query_order(tr.symbol, cid, order_id)
            except Exception as e:  # noqa: BLE001
                log.debug("query_order failed %s %s: %r", v.name, cid, e)
                ev = None
            if ev is not None and ev.terminal:
                return ev
            await self._sleep(POLL_S)
        if tr.done.done():
            return tr.done.result()
        if not assume_filled:
            last = tr.last
            if last is not None and last.filled_qty > 0:      # the feed showed a partial: book what we know
                log.error("ORDER_UNRESOLVED %s %s: in doubt, %s filled so far — booking that, the rest is unknown",
                          v.name, cid, last.filled_qty)
                self._say(f"ORDER_UNRESOLVED {v.name} {cid}: {last.filled_qty} filled, remainder unknown — check the venue")
                return OrderEvent(v.name, cid, order_id, "filled", filled_qty=last.filled_qty, avg_price=last.avg_price,
                                  fee=last.fee, liquidity="taker", ts=self.clock(), error="in_doubt")
            log.error("ORDER_UNRESOLVED %s %s: in doubt and unknown to the venue after %.1fs — treated as rejected; "
                      "reconcile by hand if the venue shows it", v.name, cid, POLL_MAX_S)
            self._say(f"ORDER_UNRESOLVED {v.name} {cid}: check the venue for a stray {tr.leg} order")
            return OrderEvent(v.name, cid, order_id, "rejected", error="in_doubt", ts=self.clock())
        log.warning("FILL_ASSUMED %s %s: no terminal state after %.1fs, trusting submitted qty",
                    v.name, cid, POLL_MAX_S)
        last = tr.last
        return OrderEvent(v.name, cid, order_id, "filled", filled_qty=qty,
                          avg_price=last.avg_price if last else 0.0, fee=last.fee if last else 0.0,
                          liquidity="taker", ts=self.clock())

    def _as_event(self, result, venue: str) -> OrderEvent:
        if isinstance(result, OrderEvent):
            return result
        log.error("LEG_EXCEPTION %s: %r", venue, result)
        return OrderEvent(venue, "", "", "rejected", error=f"exception: {result!r}", ts=self.clock())

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
        res = await asyncio.gather(
            self._place_taker(pos, a, "entry_a", "sell", pos.qty_a, False),
            self._place_taker(pos, b, "entry_b", "buy", pos.qty_b, False), return_exceptions=True)
        ev_a, ev_b = self._as_event(res[0], a), self._as_event(res[1], b)
        ok_a = ev_a.state == "filled" and ev_a.filled_qty > 0
        ok_b = ev_b.state == "filled" and ev_b.filled_qty > 0
        now = self.clock()
        if ok_a:                                   # accumulate: a stray maker fill may already sit on the leg
            self._accumulate_leg(pos, a, "entry", ev_a)
        if ok_b:
            self._accumulate_leg(pos, b, "entry", ev_b)
        if pos.status == DEGRADED:                 # degraded while the legs were in flight: retry_degraded owns it
            self._resize_from_legs(pos, spec_a, spec_b)
            self.book.dirty = True
            return None
        if ok_a and ok_b:
            self._resize_from_legs(pos, spec_a, spec_b)
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
            self._resize_from_legs(pos, spec_a, spec_b)
            self.risk.record_strike(sym, a, b)
            self.risk.set_cooldown(sym)
            rem = round((pos.filled_a - pos.exit_filled_a) if leg == "a" else (pos.filled_b - pos.exit_filled_b), 10)
            if await self._flatten_leg(pos, leg, rem) and pos.status != DEGRADED:
                self._close(pos, "failed_entry")
            elif pos.status != DEGRADED:
                transition(pos, DEGRADED)
                pos.degraded_leg = leg
                pos.last_close_attempt = self.clock()
                self.book.dirty = True
            return None
        log.warning("ENTRY_FAILED #%d %s both legs: %s / %s", pos.id, sym, ev_a.error, ev_b.error)
        self.risk.record_strike(sym, a, b)
        self.risk.set_cooldown(sym)
        if pos.filled_a > 1e-12 or pos.filled_b > 1e-12:       # something (a stray maker fill) is on the books
            if pos.status != DEGRADED:
                transition(pos, DEGRADED)
                pos.degraded_leg = "a" if pos.filled_a > 1e-12 else "b"
                pos.last_close_attempt = self.clock()
                self.book.dirty = True
            return None
        self.book.discard(pos)
        self._forget(pos)
        return None

    async def _flatten_leg(self, pos: Position, leg: str, qty: float) -> bool:
        """Market-close one entry leg (reduce-only) with the retry ladder, continuing with whatever is left
        after a partial fill. Records exit price/fees; False leaves the remainder to retry_degraded."""
        venue = pos.venue_a if leg == "a" else pos.venue_b
        side = "buy" if leg == "a" else "sell"
        remaining = round(qty, 10)
        for i, delay in enumerate(FLATTEN_LADDER_S):
            ev = await self._place_taker(pos, venue, f"flat_{leg}{i}", side, remaining, True, priority=True)
            if ev.state == "filled" and ev.filled_qty > 0:
                self._accumulate_leg(pos, venue, "exit", ev)
                remaining = round(remaining - ev.filled_qty, 10)
                if remaining <= 1e-9:
                    log.info("FLATTEN #%d %s %s ok qty=%s px=%s", pos.id, venue, leg, qty, ev.avg_price)
                    return True
                log.warning("FLATTEN_PARTIAL #%d %s %s filled=%s remaining=%s", pos.id, venue, leg, ev.filled_qty, remaining)
                continue
            if "nothing to reduce" in (ev.error or "") and await self._leg_flat_at_venue(pos, venue):
                self._book_leg_flat(pos, leg, venue)
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
            self._forget(pos)
            return None
        return pos

    async def _post_maker(self, pos: Position, reduce_only: bool, keep_posted_ts: bool = False) -> bool:
        v = self.venues[pos.maker_venue]
        now = self.clock()
        if not v.budget.try_take("order", now):
            self.metrics.funnel["budget"] += 1
            return False
        cid = self._new_cid(pos, "maker")
        self._tracks[cid] = LegTrack(pos.id, "maker", v.name, pos.symbol, pos.maker_qty, now,
                                     phase="exit" if reduce_only else "entry")
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
        pos.maker_booked_qty = 0.0
        pos.maker_booked_fee = 0.0
        try:
            ack = await v.trading.place_post_only(pos.symbol, pos.maker_side, pos.maker_qty, pos.maker_rest_price,
                                                  reduce_only, cid)
        except Exception as e:  # noqa: BLE001 — in doubt: the venue may hold a resting order we did not see acked
            log.error("TM_POST_IN_DOUBT #%d %s: %r — querying the venue", pos.id, v.name, e)
            ack = await self._resolve_doubtful_post(v, pos, cid)
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

    async def _resolve_doubtful_post(self, v: Venue, pos: Position, cid: str) -> OrderAck:
        try:
            ev = await v.trading.query_order(pos.symbol, cid, "")
        except Exception as e:  # noqa: BLE001
            log.error("TM_POST_UNRESOLVED #%d %s %s: %r — check the venue for a stray resting order", pos.id, v.name, cid, e)
            self._say(f"TM_POST_UNRESOLVED #{pos.id} {pos.symbol} {v.name}: check for a stray resting order {cid}")
            return OrderAck(False, error="in_doubt")
        if ev is None or ev.state == "rejected":
            return OrderAck(False, error="in_doubt: not at venue")
        return OrderAck(True, ev.order_id)          # it exists (resting or already filled): events will follow

    async def requote(self, pos: Position, new_price: float) -> None:
        v = self.venues[pos.maker_venue]
        now = self.clock()
        if pos.maker_cancel_sent or pos.status not in (MAKER_RESTING, EXIT_MAKER_RESTING):
            return
        if v.trading.supports_amend:
            if not v.budget.try_take("amend", now):
                return
            pos.maker_last_requote_ts = now    # stamped on the attempt: a failing amend must not retry every quote
            try:
                ack = await v.trading.amend(pos.symbol, pos.maker_client_id, pos.maker_order_id, new_price)
            except Exception as e:  # noqa: BLE001
                ack = OrderAck(False, pos.maker_order_id, error=f"exception: {e!r}")
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
        try:
            ack = await v.trading.cancel(pos.symbol, pos.maker_client_id, pos.maker_order_id)
        except Exception as e:  # noqa: BLE001
            ack = OrderAck(False, pos.maker_order_id, error=f"exception: {e!r}")
        if ack.ok:
            return True
        self._note_rate_limit(v.name, ack.error)
        # already terminal at the venue (filled or gone)? pull the terminal state ourselves
        try:
            ev = await v.trading.query_order(pos.symbol, pos.maker_client_id, pos.maker_order_id)
        except Exception as e:  # noqa: BLE001
            log.debug("query_order failed %s %s: %r", v.name, pos.maker_client_id, e)
            ev = None
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
        delta = round(ev.filled_qty - tr.filled_seen, 10)
        fee_delta = max(0.0, ev.fee - tr.fee_seen)
        tr.filled_seen, tr.fee_seen = max(tr.filled_seen, ev.filled_qty), max(tr.fee_seen, ev.fee)
        if ev.client_id != pos.maker_client_id or pos.status not in (MAKER_RESTING, HEDGING, EXIT_MAKER_RESTING, EXIT_HEDGING):
            # fill-after-cancel on a superseded order, or a position no longer in the maker flow: never write it
            # onto the live order's counters — book the real exposure on the leg (the track remembers the venue
            # and the phase the order was posted in) and hand the position to retry_degraded
            if delta > 1e-12 and pos.status != CLOSED:
                leg = "a" if tr.venue == pos.venue_a else "b"
                why = "superseded order" if ev.client_id != pos.maker_client_id else pos.status
                log.error("TM_STRAY_FILL #%d %s %s: %s contracts @%s on %s (%s) — booked on the %s %s leg for unwinding",
                          pos.id, pos.symbol, tr.venue, delta, ev.avg_price, ev.client_id, why, tr.phase, leg)
                self._accumulate_leg(pos, tr.venue, tr.phase,
                                     OrderEvent(ev.venue, ev.client_id, ev.order_id, ev.state, delta, ev.avg_price, fee_delta))
                self.book.dirty = True
                if pos.status != DEGRADED:
                    self._spawn(self._degrade(pos, leg, "stray maker fill"))
            elif delta > 1e-12:
                log.error("TM_STRAY_FILL_AFTER_CLOSE #%d %s %s: %s contracts on %s — reconcile by hand",
                          pos.id, pos.symbol, tr.venue, delta, ev.client_id)
                self._say(f"STRAY_FILL_AFTER_CLOSE #{pos.id} {pos.symbol} {tr.venue}: {delta} contracts")
            return
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

    async def _degrade(self, pos: Position, leg: str, why: str) -> None:
        """Hand a maker-flow position to retry_degraded: cancel anything still resting, book the live order's
        fill on the leg, walk the state machine to DEGRADED."""
        async with self._lock(pos.id):
            if pos.status in (CLOSED, DEGRADED):
                return
            log.error("DEGRADE #%d %s leg %s: %s", pos.id, pos.symbol, leg, why)
            if pos.status in (MAKER_RESTING, EXIT_MAKER_RESTING):
                await self._cancel_maker_order(pos, priority=True)
                if pos.status in (MAKER_RESTING, EXIT_MAKER_RESTING):      # a fill event may have moved it meanwhile
                    transition(pos, HEDGING if pos.status == MAKER_RESTING else EXIT_HEDGING)
            if pos.status in (HEDGING, EXIT_HEDGING):
                self._apply_maker_leg(pos, self._maker_phase(pos))
            if pos.status != DEGRADED:
                transition(pos, DEGRADED)
            pos.degraded_leg = leg
            pos.last_close_attempt = self.clock()
            self.book.dirty = True
            self._say(f"DEGRADED #{pos.id} {pos.symbol}: {why}")

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
                if not terminal:                                        # first fill cancels the remainder
                    if not await self._cancel_maker_order(pos, priority=True) and not pos.maker_cancel_sent:
                        await self._sleep(HEDGE_RETRY_S)
                        if not await self._cancel_maker_order(pos, priority=True) and not pos.maker_cancel_sent:
                            log.error("HEDGE_CANCEL_FAILED #%d %s: remainder still resting on %s — the sweep retries",
                                      pos.id, pos.symbol, mv)
                side = self._hedge_side(pos, phase)
                q_h = self.board.fresh(hv, pos.symbol, self.clock())
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
                        self.risk.record_strike(pos.symbol, pos.venue_a, pos.venue_b)
                        if phase == "entry":
                            log.error("HEDGE_FAILED #%d %s on %s — flattening the maker fill", pos.id, pos.symbol, hv)
                            await self._flatten_maker_fill(pos, lots_floor(unhedged, spec_m))
                        else:                          # an exit maker fill IS progress: the rest closes taker/taker
                            log.error("HEDGE_FAILED #%d %s exit hedge on %s — the remainder closes taker/taker",
                                      pos.id, pos.symbol, hv)
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
                    if to_flat > 0 and phase == "entry":
                        log.warning("RESIDUAL #%d %s %s maker contracts unhedged (matched %s) — flattening %s",
                                    pos.id, pos.symbol, residual, pos.hedged_qty, to_flat)
                        await self._flatten_maker_fill(pos, to_flat)
                    elif to_flat > 0:
                        log.warning("RESIDUAL #%d %s %s exit maker contracts unhedged — the remainder closes taker/taker",
                                    pos.id, pos.symbol, residual)
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

    async def _flatten_maker_fill(self, pos: Position, qty: float) -> None:
        """Undo an unhedgeable/unhedged ENTRY maker fill on the maker venue itself (reduce-only market). The
        round trip is netted out of the maker leg; its realized P&L lands in `pnl_adjust_usd`."""
        if qty <= 0:
            return
        side = "buy" if pos.maker_side == "sell" else "sell"
        sign = 1.0 if pos.maker_side == "sell" else -1.0
        spec_m = self._spec(pos.maker_venue, pos.symbol)
        cs = spec_m.contract_size if spec_m is not None else 1.0
        remaining = round(qty, 10)
        for i, delay in enumerate(FLATTEN_LADDER_S[:3]):
            ev = await self._place_taker(pos, pos.maker_venue, f"mflat{i}", side, remaining, True, priority=True)
            if ev.state == "filled" and ev.filled_qty > 0:
                # the part of the fill never booked on the leg is realized here; a part already booked (a degrade
                # or stray-fill path ran meanwhile) is booked as the leg's exit fill so the legs stay consistent
                unbooked = max(0.0, min(ev.filled_qty, round(pos.maker_filled_qty - pos.maker_booked_qty, 10)))
                booked_part = round(ev.filled_qty - unbooked, 10)
                if unbooked > 0:
                    pos.pnl_adjust_usd += sign * (pos.maker_avg_price - ev.avg_price) * unbooked * cs
                if booked_part > 1e-12:
                    self._accumulate_leg(pos, pos.maker_venue, "exit",
                                         OrderEvent(ev.venue, ev.client_id, ev.order_id, "filled", booked_part, ev.avg_price, 0.0, "taker"))
                    pos.maker_booked_qty = round(pos.maker_booked_qty - booked_part, 10)
                pos.exit_fees_usd += ev.fee
                pos.maker_filled_qty = round(pos.maker_filled_qty - ev.filled_qty, 10)   # netted out
                remaining = round(remaining - ev.filled_qty, 10)
                log.info("FLATTEN #%d maker leg %s qty=%s @%s", pos.id, pos.maker_venue, ev.filled_qty, ev.avg_price)
                if remaining <= 1e-9:
                    return
                continue
            await self._sleep(delay)
        log.error("FLATTEN_FAILED #%d maker leg %s qty=%s — DEGRADED", pos.id, pos.maker_venue, remaining)
        if pos.status in (MAKER_RESTING, HEDGING, EXIT_MAKER_RESTING, EXIT_HEDGING):
            self._apply_maker_leg(pos, self._maker_phase(pos))     # what is left of the fill must be on the books
        if pos.status in (MAKER_RESTING, EXIT_MAKER_RESTING):
            transition(pos, HEDGING if pos.status == MAKER_RESTING else EXIT_HEDGING)
        if pos.status != DEGRADED:
            transition(pos, DEGRADED)
        pos.degraded_leg = "a" if pos.maker_venue == pos.venue_a else "b"
        pos.last_close_attempt = self.clock()
        self.book.dirty = True

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
        """Book the resting order's fill and fee on its leg — incrementally, so it can be called at finalize, at
        degrade time and after a stray fill without double counting or masking a later fill."""
        dq = max(0.0, round(pos.maker_filled_qty - pos.maker_booked_qty, 10))
        dfee = max(0.0, pos.maker_fee_usd - pos.maker_booked_fee)
        if dq > 1e-12 or dfee > 0:
            self._accumulate_leg(pos, pos.maker_venue, phase,
                                 OrderEvent(pos.maker_venue, pos.maker_client_id, pos.maker_order_id, "filled",
                                            dq, pos.maker_avg_price, dfee, "maker"))
        pos.maker_booked_qty = max(pos.maker_booked_qty, pos.maker_filled_qty)
        pos.maker_booked_fee = max(pos.maker_booked_fee, pos.maker_fee_usd)
        pos.fee_liquidity["maker"] = "maker"

    def _resize_from_legs(self, pos: Position, spec_a, spec_b) -> None:
        """Matched size from the legs actually on the books; a one-legged unwind still has a real size."""
        sizes = [n for n in (notional(pos.filled_a, pos.entry_price_a, spec_a) if spec_a else 0.0,
                             notional(pos.filled_b, pos.entry_price_b, spec_b) if spec_b else 0.0) if n > 0]
        pos.size_usd = min(sizes) if sizes else pos.size_usd

    async def _finalize_maker(self, pos: Position, phase: str) -> None:
        now = self.clock()
        spec_a, spec_b = self._spec(pos.venue_a, pos.symbol), self._spec(pos.venue_b, pos.symbol)
        if phase == "entry":
            if pos.status == DEGRADED:
                self._apply_maker_leg(pos, "entry")         # whatever filled must be on the books for retry_degraded
                self._resize_from_legs(pos, spec_a, spec_b)
                return
            if pos.maker_filled_qty <= 1e-12:
                if pos.status == HEDGING:                   # the fill was flattened: a round trip, not a discard...
                    self._apply_maker_leg(pos, "entry")
                    rem_a = round(pos.filled_a - pos.exit_filled_a, 10)
                    rem_b = round(pos.filled_b - pos.exit_filled_b, 10)
                    if rem_a <= 1e-9 and rem_b <= 1e-9:
                        self._close(pos, "hedge_unwound")
                    else:                                   # ...unless something else (a stray fill) sits on a leg
                        log.error("UNWOUND_BUT_NOT_FLAT #%d %s rem_a=%s rem_b=%s — DEGRADED", pos.id, pos.symbol, rem_a, rem_b)
                        transition(pos, DEGRADED)
                        pos.degraded_leg = "a" if rem_a > 1e-9 else "b"
                        pos.last_close_attempt = self.clock()
                        self.book.dirty = True
                    return
                if pos.requote_pending and pos.status == MAKER_RESTING:
                    pos.requote_pending = False
                    pos.upgrade_pending = False        # an upgrade asked for mid-requote is stale: re-evaluate fresh
                    pos.maker_rest_price = pos.requote_price
                    if await self._post_maker(pos, reduce_only=False, keep_posted_ts=True):
                        return
                if pos.upgrade_pending and pos.status == MAKER_RESTING:
                    pos.upgrade_pending = False
                    pos.mode = "TT"
                    qa, qb = self.board.get(pos.venue_a, pos.symbol), self.board.get(pos.venue_b, pos.symbol)
                    if qa is not None and qb is not None and spec_a is not None and spec_b is not None:
                        legs = size_pair(pos.size_usd, qa.bid, qb.ask, spec_a, spec_b, self.cfg.max_leg_mismatch_pct,
                                         min_usd=self.cfg.min_position_usd)     # re-size at the CURRENT touch
                        if legs is not None:
                            pos.qty_a, pos.qty_b = legs.qty_a, legs.qty_b
                    transition(pos, TT_ENTERING)
                    await self._tt_legs(pos)
                    return
                self.metrics.funnel["maker_cancelled"] += 1
                self.book.discard(pos)
                self._forget(pos)
                return
            self._apply_maker_leg(pos, "entry")
            self._resize_from_legs(pos, spec_a, spec_b)
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
        if pos.status == DEGRADED:
            self._apply_maker_leg(pos, "exit")
            return
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
        if pos.status not in (OPEN, EXIT_MAKER_RESTING):
            log.info("EXIT_IGNORED #%d %s status=%s reason=%s (in flight; the next evaluation retries)",
                     pos.id, pos.symbol, pos.status, reason)
            return
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
        results = await asyncio.gather(*(j[1] for j in jobs), return_exceptions=True) if jobs else []
        for (leg, _), r in zip(jobs, results):
            venue = pos.venue_a if leg == "a" else pos.venue_b
            ev = self._as_event(r, venue)
            if ev.state == "filled" and ev.filled_qty > 0:
                self._accumulate_leg(pos, venue, "exit", ev)
            elif "nothing to reduce" in (ev.error or "") and await self._leg_flat_at_venue(pos, venue):
                self._book_leg_flat(pos, leg, venue)
        failed = ""
        for leg, _ in jobs:                       # decide on what is LEFT, not on "some fill arrived"
            rem = round((pos.filled_a - pos.exit_filled_a) if leg == "a" else (pos.filled_b - pos.exit_filled_b), 10)
            if rem > 1e-9:
                failed += leg
        if failed:
            log.warning("CLOSE_DEGRADED #%d %s legs=%s", pos.id, pos.symbol, failed)
            if pos.status != DEGRADED:
                transition(pos, DEGRADED)
            pos.degraded_leg = "both" if failed == "ab" else failed
            pos.close_retry_count += 1
            pos.last_close_attempt = self.clock()
            self.book.dirty = True
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

    async def _sweep_stuck_hedging(self, now: float) -> None:
        """A HEDGING/EXIT_HEDGING position whose maker order is still live (the cancel inside _hedge_delta failed):
        retry the cancel so no more fills arrive; the fill/cancel events then drive it to finalize."""
        for pos in list(self.book.by_status(HEDGING, EXIT_HEDGING)):
            tr = self._tracks.get(pos.maker_client_id)
            live = tr is not None and (tr.last is None or not tr.last.terminal)
            since = pos.maker_fill_ts or pos.maker_posted_ts
            if live and not pos.maker_cancel_sent and now - since >= HEDGING_SWEEP_AFTER_S:
                log.warning("HEDGING_SWEEP #%d %s: maker order still live on %s — cancelling", pos.id, pos.symbol, pos.maker_venue)
                await self._cancel_maker_order(pos, priority=True)

    async def retry_degraded(self) -> None:
        now = self.clock()
        await self._sweep_stuck_hedging(now)
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
                rem = round((pos.filled_a - pos.exit_filled_a) if leg == "a" else (pos.filled_b - pos.exit_filled_b), 10)
                if rem > 1e-9:                    # a partial close is not a close
                    ok = False
            if ok:
                self._close(pos, pos.exit_reason or "recovered")

    def _close(self, pos: Position, reason: str) -> None:
        if pos.status == CLOSED:
            return                                # book.close is idempotent; the risk stats must be too
        self.book.close(pos, reason, self.clock())
        self.risk.record_close(pos)
        log.info("CLOSE #%d %s reason=%s pnl=$%+.4f gross=$%+.4f fees=$%.4f adj=$%+.4f exit_spread=%.3f%%", pos.id,
                 pos.symbol, reason, pos.net_pnl_usd, pos.gross_pnl_usd, pos.entry_fees_usd + pos.exit_fees_usd,
                 pos.pnl_adjust_usd, pos.exit_spread_pct)
        self._say(f"CLOSE #{pos.id} {pos.symbol} {reason} pnl ${pos.net_pnl_usd:+.4f}")
        self._forget(pos)

    async def cancel_all_resting(self) -> None:
        """Halt: cancel every live maker order, including one left resting by a failed cancel while hedging."""
        for pos in list(self.book.by_status(MAKER_RESTING, EXIT_MAKER_RESTING, HEDGING, EXIT_HEDGING)):
            tr = self._tracks.get(pos.maker_client_id)
            if pos.status in (HEDGING, EXIT_HEDGING) and (tr is None or (tr.last is not None and tr.last.terminal)):
                continue
            await self._cancel_maker_order(pos, priority=True)
