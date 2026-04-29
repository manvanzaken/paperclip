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

    `bids` are sorted DESC by price (best bid first), `asks` ASC (best ask
    first). Each tuple is `(price, size)` where `size` is in base units
    (e.g. BTC contracts), not USD.
    """

    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    ts: datetime

    @property
    def best_bid(self) -> float | None:
        # Some exchanges send levels in non-canonical order. Take the
        # max/min to be safe rather than trust array index 0.
        if not self.bids:
            return None
        return max(p for p, _ in self.bids)

    @property
    def best_ask(self) -> float | None:
        if not self.asks:
            return None
        return min(p for p, _ in self.asks)
