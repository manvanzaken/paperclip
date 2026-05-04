from __future__ import annotations
from dataclasses import dataclass
from typing import List, Iterable
from .models import Triangle, LegSide


@dataclass(frozen=True)
class Market:
    symbol: str
    base: str
    quote: str
    volume_24h_usd: float


def enumerate_triangles(
    exchange: str,
    markets: Iterable[Market],
    anchors: List[str],
    min_volume_usd: float,
) -> List[Triangle]:
    """
    Build all single-venue triangles of the form  Anchor -> X -> Y -> Anchor,
    where each hop maps to a real market on `exchange`.

    For simplicity we only emit triangles where leg 0 and leg 2 quote in `anchor`
    (so leg 0 = BUY base/anchor, leg 2 = SELL base/anchor) and leg 1 is X/Y or Y/X.

    Pairs below `min_volume_usd` are dropped.
    """
    eligible = [m for m in markets if m.volume_24h_usd >= min_volume_usd]
    out: List[Triangle] = []

    for anchor in anchors:
        anchored = [m for m in eligible if m.quote == anchor]
        for m1 in anchored:           # leg 0: BUY m1.base / anchor
            for m3 in anchored:       # leg 2: SELL m3.base / anchor
                if m1.base == m3.base:
                    continue
                leg1 = None
                leg1_side = None
                for m in eligible:
                    if m.base == m1.base and m.quote == m3.base:
                        leg1, leg1_side = m, LegSide.SELL
                        break
                    if m.base == m3.base and m.quote == m1.base:
                        leg1, leg1_side = m, LegSide.BUY
                        break
                if leg1 is None:
                    continue
                tri = Triangle(
                    exchange=exchange,
                    anchor=anchor,
                    legs=(
                        (m1.symbol, LegSide.BUY),
                        (leg1.symbol, leg1_side),
                        (m3.symbol, LegSide.SELL),
                    ),
                )
                out.append(tri)

    seen = set()
    deduped = []
    for t in out:
        if t.id not in seen:
            seen.add(t.id)
            deduped.append(t)
    return deduped
