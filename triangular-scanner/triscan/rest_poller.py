from __future__ import annotations
import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import List

from .models import Triangle, LegSide
from .pricing import gross_multiplier, net_multiplier
from .sources.base import Source

log = logging.getLogger(__name__)


@dataclass
class Tier1Result:
    triangle: Triangle
    gross_edge_pct: float
    net_edge_pct: float
    ts_ms: int


class Tier1Poller:
    def __init__(self, source: Source, triangles: List[Triangle]):
        self.source = source
        self.triangles = triangles
        self._symbols = sorted({sym for t in triangles for sym in t.symbols})

    async def poll_once(self) -> List[Tier1Result]:
        try:
            tickers = await self.source.fetch_tickers(self._symbols)
        except Exception as e:
            log.warning("tier1 fetch_tickers failed for %s: %s", self.source.name, e)
            return []

        out: List[Tier1Result] = []
        for tri in self.triangles:
            legs = []
            ok = True
            for sym, side in tri.legs:
                q = tickers.get(sym)
                if q is None:
                    ok = False
                    break
                price = q.ask if side == LegSide.BUY else q.bid
                legs.append((sym, side, price))
            if not ok:
                continue
            gross = gross_multiplier(legs)
            gross_pct = (gross - 1.0) * 100.0
            net = net_multiplier(gross, fee_pct=Decimal(str(self.source.taker_fee_pct)))
            net_pct = (net - 1.0) * 100.0
            ts = max(tickers[sym].ts_ms for sym, _ in tri.legs)
            out.append(Tier1Result(triangle=tri, gross_edge_pct=gross_pct, net_edge_pct=net_pct, ts_ms=ts))
        return out

    async def run_loop(self, interval_sec: int, on_results):
        while True:
            results = await self.poll_once()
            await on_results(results)
            await asyncio.sleep(interval_sec)
