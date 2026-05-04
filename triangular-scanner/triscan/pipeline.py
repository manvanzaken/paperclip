from __future__ import annotations
import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Awaitable, Callable, Dict, List, Optional

from .models import Triangle, LegSide, Book, OpportunityState, Opportunity
from .pricing import gross_multiplier, net_multiplier, simulate_cycle_through_book, binary_search_executable_size
from .rest_poller import Tier1Result
from .ws_manager import WsManager

log = logging.getLogger(__name__)


@dataclass
class PipelineConfig:
    tier1_threshold_pct: float
    tier2_threshold_pct: float
    min_profit_usd: float
    cooldown_sec: int
    max_size_cap_usd: Decimal
    taker_fee_pct: Decimal


@dataclass
class _TriangleState:
    triangle: Triangle
    state: OpportunityState = OpportunityState.IDLE
    below_since_ms: Optional[int] = None
    last_tier1_pct: float = 0.0
    books: Dict[str, Book] = field(default_factory=dict)
    opportunity: Optional[Opportunity] = None
    ws_update_count: int = 0
    book_age_samples: List[int] = field(default_factory=list)


class Pipeline:
    def __init__(
        self,
        triangles: List[Triangle],
        ws_manager: WsManager,
        config: PipelineConfig,
        on_opportunity: Callable[[dict], Awaitable[None]],
    ):
        self._states: Dict[str, _TriangleState] = {t.id: _TriangleState(t) for t in triangles}
        self._symbol_index: Dict[str, List[str]] = {}
        for t in triangles:
            for sym in t.symbols:
                self._symbol_index.setdefault(sym, []).append(t.id)
        self.ws = ws_manager
        self.cfg = config
        self.on_opportunity = on_opportunity

    def state(self, t: Triangle) -> OpportunityState:
        return self._states[t.id].state

    async def handle_tier1(self, results: List[Tier1Result], now_ms: Optional[int] = None) -> None:
        # Use provided now_ms, or derive from result timestamps, or fall back to wall clock.
        if now_ms is not None:
            now = now_ms
        elif results:
            now = max(r.ts_ms for r in results)
        else:
            now = int(time.time() * 1000)
        seen = set()
        for r in results:
            seen.add(r.triangle.id)
            ts = self._states.get(r.triangle.id)
            if ts is None:
                continue
            ts.last_tier1_pct = r.gross_edge_pct
            if r.gross_edge_pct >= self.cfg.tier1_threshold_pct:
                ts.below_since_ms = None
                if ts.state == OpportunityState.IDLE:
                    await self._promote_to_candidate(ts)
            else:
                if ts.below_since_ms is None:
                    ts.below_since_ms = now
        for tid, ts in self._states.items():
            if tid in seen:
                continue
            if ts.state != OpportunityState.IDLE and ts.below_since_ms is None:
                ts.below_since_ms = now

    async def _promote_to_candidate(self, ts: _TriangleState) -> None:
        ts.state = OpportunityState.CANDIDATE
        for sym, _side in ts.triangle.legs:
            await self.ws.acquire(sym, self.handle_book)
        log.info("pipeline: %s IDLE -> CANDIDATE (tier1=%.4f%%)", ts.triangle.id, ts.last_tier1_pct)

    async def _release_to_idle(self, ts: _TriangleState, reason: str) -> None:
        for sym, _side in ts.triangle.legs:
            await self.ws.release(sym)
        if ts.state == OpportunityState.CONFIRMED:
            await self._close_opportunity(ts, reason)
        ts.state = OpportunityState.IDLE
        ts.below_since_ms = None
        ts.books.clear()
        log.info("pipeline: %s -> IDLE (%s)", ts.triangle.id, reason)

    async def handle_book(self, book: Book) -> None:
        for tid in self._symbol_index.get(book.symbol, []):
            ts = self._states[tid]
            if ts.state == OpportunityState.IDLE:
                continue
            ts.books[book.symbol] = book
            ts.ws_update_count += 1
            now = int(time.time() * 1000)
            ts.book_age_samples.append(now - book.ts_ms if book.ts_ms else 0)
            await self._evaluate_tier2(ts, now)

    async def _evaluate_tier2(self, ts: _TriangleState, now_ms: int) -> None:
        legs_books = []
        for sym, side in ts.triangle.legs:
            b = ts.books.get(sym)
            if b is None:
                return
            legs_books.append((sym, side, b))
        touch_legs = [(s, side, b.best_ask().price if side == LegSide.BUY else b.best_bid().price)
                      for s, side, b in legs_books]
        gross = gross_multiplier(touch_legs)
        gross_pct = (gross - 1.0) * 100.0
        net = net_multiplier(gross, fee_pct=self.cfg.taker_fee_pct)
        net_pct = (net - 1.0) * 100.0
        if gross_pct < self.cfg.tier2_threshold_pct:
            if ts.state == OpportunityState.CONFIRMED:
                await self._close_opportunity(ts, reason="edge_decay")
                ts.state = OpportunityState.CANDIDATE
            return

        # Use threshold=0.0 here: the gross_pct gate above already confirmed edge exists;
        # binary search just finds the largest net-positive (after fees) executable size.
        size = binary_search_executable_size(
            legs_books, fee_pct=self.cfg.taker_fee_pct,
            tier2_threshold_pct=0.0,
            max_size_cap_usd=self.cfg.max_size_cap_usd,
        )
        if size <= 0:
            if ts.state == OpportunityState.CONFIRMED:
                await self._close_opportunity(ts, reason="book_thinned")
                ts.state = OpportunityState.CANDIDATE
            return

        result = simulate_cycle_through_book(legs_books, Decimal(str(size)))
        f = float(self.cfg.taker_fee_pct) / 100.0
        net_out = result.output_quote * (1 - f) ** 3
        profit_usd = net_out - result.input_consumed
        if profit_usd < self.cfg.min_profit_usd:
            if ts.state == OpportunityState.CONFIRMED:
                await self._close_opportunity(ts, reason="edge_decay")
                ts.state = OpportunityState.CANDIDATE
            return

        if ts.state == OpportunityState.CANDIDATE:
            opp = Opportunity(
                id=str(uuid.uuid4()),
                triangle_id=ts.triangle.id,
                exchange=ts.triangle.exchange,
                anchor=ts.triangle.anchor,
                legs=ts.triangle.legs,
                opened_at=now_ms,
                open_net_edge_pct=net_pct,
                peak_net_edge_pct=net_pct,
                peak_executable_profit_usd=profit_usd,
                peak_executable_size_usd=float(size),
                peak_at=now_ms,
                bottleneck_leg_at_peak=result.bottleneck_leg,
            )
            ts.opportunity = opp
            ts.state = OpportunityState.CONFIRMED
            ts.ws_update_count = 0
            ts.book_age_samples.clear()
            await self.on_opportunity({
                "type": "OpportunityOpen",
                "opportunity": _opp_to_dict(opp),
            })
            log.info("pipeline: %s CANDIDATE -> CONFIRMED (net=%.4f%% size=$%.2f profit=$%.2f)",
                     ts.triangle.id, net_pct, float(size), profit_usd)
        else:
            opp = ts.opportunity
            if net_pct > opp.peak_net_edge_pct:
                opp.peak_net_edge_pct = net_pct
                opp.peak_executable_profit_usd = profit_usd
                opp.peak_executable_size_usd = float(size)
                opp.peak_at = now_ms
                opp.bottleneck_leg_at_peak = result.bottleneck_leg

    async def _close_opportunity(self, ts: _TriangleState, reason: str) -> None:
        opp = ts.opportunity
        if opp is None:
            return
        now = int(time.time() * 1000)
        opp.closed_at = now
        opp.lifetime_ms = now - opp.opened_at
        opp.close_net_edge_pct = (
            (gross_multiplier([
                (s, side,
                 ts.books[s].best_ask().price if side == LegSide.BUY else ts.books[s].best_bid().price)
                for s, side in ts.triangle.legs
            ]) - 1.0) * 100.0 if all(s in ts.books for s, _ in ts.triangle.legs) else 0.0
        )
        opp.closed_reason = reason
        opp.ws_update_count = ts.ws_update_count
        opp.median_book_age_ms = float(_median(ts.book_age_samples)) if ts.book_age_samples else 0.0
        await self.on_opportunity({
            "type": "OpportunityClosed",
            "opportunity": _opp_to_dict(opp),
        })
        ts.opportunity = None

    async def notify_ws_failed(self, symbol: str) -> None:
        for tid in self._symbol_index.get(symbol, []):
            ts = self._states[tid]
            if ts.state == OpportunityState.CONFIRMED:
                await self._close_opportunity(ts, reason="ws_disconnect")
                ts.state = OpportunityState.CANDIDATE

    async def shutdown(self) -> None:
        for ts in self._states.values():
            if ts.state == OpportunityState.CONFIRMED:
                await self._close_opportunity(ts, reason="manual_stop")
                ts.state = OpportunityState.CANDIDATE

    async def tick(self, now_ms: Optional[int] = None) -> None:
        """Periodic cooldown + housekeeping. Call regularly (every 1s)."""
        now = now_ms if now_ms is not None else int(time.time() * 1000)
        for ts in self._states.values():
            if ts.state == OpportunityState.IDLE:
                continue
            if ts.below_since_ms is not None and (now - ts.below_since_ms) >= self.cfg.cooldown_sec * 1000:
                await self._release_to_idle(ts, reason="edge_decay")


def _opp_to_dict(o: Opportunity) -> dict:
    return {
        "id": o.id,
        "triangle_id": o.triangle_id,
        "exchange": o.exchange,
        "anchor": o.anchor,
        "legs": [(s, side.value) for s, side in o.legs],
        "opened_at": o.opened_at,
        "closed_at": o.closed_at,
        "lifetime_ms": o.lifetime_ms,
        "open_net_edge_pct": o.open_net_edge_pct,
        "close_net_edge_pct": o.close_net_edge_pct,
        "peak_net_edge_pct": o.peak_net_edge_pct,
        "peak_executable_profit_usd": o.peak_executable_profit_usd,
        "peak_executable_size_usd": o.peak_executable_size_usd,
        "peak_at": o.peak_at,
        "bottleneck_leg_at_peak": o.bottleneck_leg_at_peak,
        "ws_update_count": o.ws_update_count,
        "median_book_age_ms": o.median_book_age_ms,
        "closed_reason": o.closed_reason,
    }


def _median(xs: List[int]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs); n = len(s)
    return float(s[n // 2]) if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2.0
