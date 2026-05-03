"""Shared L2 orderbook dataclass.

Lives in its own tiny module so `data_feed` and `execution_sim` can both
import it without a circular dependency.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class OrderBook:
    """Top-of-book through some depth.

    `bids` should be sorted DESC by price (best first), `asks` ASC.
    Each tuple is `(price, size)`. The `normalised()` helper enforces
    that ordering AND drops zero-size levels (which some exchanges send
    as an instruction to remove a level — they would otherwise corrupt
    walk-book / VWAP calculations).
    """

    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    ts: datetime

    def normalised(self) -> "OrderBook":
        bids = sorted(
            ((p, s) for p, s in self.bids if s > 0),
            key=lambda x: -x[0],
        )
        asks = sorted(
            ((p, s) for p, s in self.asks if s > 0),
            key=lambda x: x[0],
        )
        return OrderBook(bids=bids, asks=asks, ts=self.ts)

    @property
    def best_bid(self) -> float | None:
        for p, s in self.bids:
            if s > 0:
                return max(pp for pp, ss in self.bids if ss > 0)
        return None

    @property
    def best_ask(self) -> float | None:
        for p, s in self.asks:
            if s > 0:
                return min(pp for pp, ss in self.asks if ss > 0)
        return None
