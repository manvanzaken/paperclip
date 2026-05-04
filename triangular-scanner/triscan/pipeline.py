from __future__ import annotations
import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional

from .models import Triangle, LegSide, Book, LiveStatus, OpportunityState, Opportunity
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
    sample_every_n_updates: int = 0


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
        on_state_change: Optional[Callable[[Triangle, OpportunityState, dict], Awaitable[None]]] = None,
        data_dir: Optional[Path] = None,
        top_n: int = 20,
    ):
        self._states: Dict[str, _TriangleState] = {t.id: _TriangleState(t) for t in triangles}
        self._symbol_index: Dict[str, List[str]] = {}
        for t in triangles:
            for sym in t.symbols:
                self._symbol_index.setdefault(sym, []).append(t.id)
        self.ws = ws_manager
        self.cfg = config
        self.on_opportunity = on_opportunity
        self.on_state_change = on_state_change
        self._status_path: Optional[Path] = Path(data_dir) / "triscan_status.json" if data_dir is not None else None
        self._top_n = top_n
        self._status_shutdown = asyncio.Event()

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
        if self.on_state_change is not None:
            try:
                await self.on_state_change(ts.triangle, OpportunityState.CANDIDATE, {})
            except Exception:
                pass

    async def _release_to_idle(self, ts: _TriangleState, reason: str) -> None:
        for sym, _side in ts.triangle.legs:
            await self.ws.release(sym)
        if ts.state == OpportunityState.CONFIRMED:
            await self._close_opportunity(ts, reason)
        ts.state = OpportunityState.IDLE
        ts.below_since_ms = None
        ts.books.clear()
        log.info("pipeline: %s -> IDLE (%s)", ts.triangle.id, reason)
        if self.on_state_change is not None:
            try:
                await self.on_state_change(ts.triangle, OpportunityState.IDLE, {"reason": reason})
            except Exception:
                pass

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
                if self.on_state_change is not None:
                    try:
                        await self.on_state_change(ts.triangle, OpportunityState.CANDIDATE, {"reason": "edge_decay"})
                    except Exception:
                        pass
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
                if self.on_state_change is not None:
                    try:
                        await self.on_state_change(ts.triangle, OpportunityState.CANDIDATE, {"reason": "book_thinned"})
                    except Exception:
                        pass
            return

        result = simulate_cycle_through_book(legs_books, Decimal(str(size)))
        f = float(self.cfg.taker_fee_pct) / 100.0
        net_out = result.output_quote * (1 - f) ** 3
        profit_usd = net_out - result.input_consumed
        if profit_usd < self.cfg.min_profit_usd:
            if ts.state == OpportunityState.CONFIRMED:
                await self._close_opportunity(ts, reason="edge_decay")
                ts.state = OpportunityState.CANDIDATE
                if self.on_state_change is not None:
                    try:
                        await self.on_state_change(ts.triangle, OpportunityState.CANDIDATE, {"reason": "edge_decay"})
                    except Exception:
                        pass
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
            if self.on_state_change is not None:
                try:
                    await self.on_state_change(ts.triangle, OpportunityState.CONFIRMED, {
                        "net_edge_pct": net_pct,
                        "executable_size_usd": float(size),
                        "executable_profit_usd": profit_usd,
                        "bottleneck_leg": result.bottleneck_leg,
                    })
                except Exception:
                    pass
        else:
            opp = ts.opportunity
            if net_pct > opp.peak_net_edge_pct:
                opp.peak_net_edge_pct = net_pct
                opp.peak_executable_profit_usd = profit_usd
                opp.peak_executable_size_usd = float(size)
                opp.peak_at = now_ms
                opp.bottleneck_leg_at_peak = result.bottleneck_leg
            if self.cfg.sample_every_n_updates > 0 and ts.ws_update_count % self.cfg.sample_every_n_updates == 0:
                opp.snapshots.append({
                    "ts_ms": now_ms,
                    "net_edge_pct": net_pct,
                    "executable_size_usd": float(size),
                    "executable_profit_usd": profit_usd,
                    "bottleneck_leg": result.bottleneck_leg,
                })

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

    def _snapshot_status(self) -> LiveStatus:
        from datetime import datetime, timezone
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        confirmed = []
        candidates = []
        for state in self._states.values():
            row = {
                "triangle_id": state.triangle.id,
                "exchange": state.triangle.exchange,
                "net_edge_pct": state.last_tier1_pct,
                "profit_usd": (state.opportunity.peak_executable_profit_usd if state.opportunity else 0.0),
                "size_usd": (state.opportunity.peak_executable_size_usd if state.opportunity else 0.0),
                "age_ms": 0,
                "bottleneck_leg": (state.opportunity.bottleneck_leg_at_peak if state.opportunity else -1),
                "gross_edge_pct": state.last_tier1_pct,
            }
            if state.state == OpportunityState.CONFIRMED:
                confirmed.append(row)
            elif state.state == OpportunityState.CANDIDATE:
                candidates.append(row)
        candidates.sort(key=lambda r: -r["gross_edge_pct"])
        return LiveStatus(
            ts=now_iso,
            confirmed=confirmed,
            candidates=candidates[: self._top_n],
            ws_subscriptions_per_exchange={},
            triangle_count_per_exchange={},
            last_tier1_poll_per_exchange={},
        )

    async def status_writer(self) -> None:
        """Write triscan_status.json every 1s. Returns when shutdown event is set."""
        if self._status_path is None:
            return
        while not self._status_shutdown.is_set():
            try:
                from dataclasses import asdict
                status = self._snapshot_status()
                tmp = self._status_path.with_suffix(".json.tmp")
                tmp.write_text(json.dumps(asdict(status)))
                tmp.replace(self._status_path)
            except Exception:
                log.exception("status_writer failed")
            try:
                await asyncio.wait_for(self._status_shutdown.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass

    async def notify_ws_failed(self, symbol: str) -> None:
        for tid in self._symbol_index.get(symbol, []):
            ts = self._states[tid]
            if ts.state == OpportunityState.CONFIRMED:
                await self._close_opportunity(ts, reason="ws_disconnect")
                ts.state = OpportunityState.CANDIDATE
                if self.on_state_change is not None:
                    try:
                        await self.on_state_change(ts.triangle, OpportunityState.CANDIDATE, {"reason": "ws_disconnect"})
                    except Exception:
                        pass

    async def shutdown(self) -> None:
        self._status_shutdown.set()
        for ts in self._states.values():
            if ts.state == OpportunityState.CONFIRMED:
                await self._close_opportunity(ts, reason="manual_stop")
                ts.state = OpportunityState.CANDIDATE
                if self.on_state_change is not None:
                    try:
                        await self.on_state_change(ts.triangle, OpportunityState.CANDIDATE, {"reason": "manual_stop"})
                    except Exception:
                        pass

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
        "snapshots": o.snapshots,
    }


def _median(xs: List[int]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs); n = len(s)
    return float(s[n // 2]) if n % 2 == 1 else (s[n // 2 - 1] + s[n // 2]) / 2.0
