"""Pure sizing: USD notional → venue contracts, matched pair legs, hedge quantities."""
from __future__ import annotations

import math
from dataclasses import dataclass

from .models import VenueSpec


def contracts_for_usd(usd: float, price: float, spec: VenueSpec) -> float:
    """Largest lot-multiple not exceeding `usd` at `price`; 0.0 when below the venue minimum."""
    if usd <= 0 or price <= 0 or spec.contract_size <= 0 or spec.lot <= 0:
        return 0.0
    raw = usd / (price * spec.contract_size)
    n = round(math.floor(raw / spec.lot + 1e-9) * spec.lot, 10)
    return n if n >= spec.min_qty - 1e-12 else 0.0


def notional(qty: float, price: float, spec: VenueSpec) -> float:
    return qty * price * spec.contract_size


@dataclass(frozen=True)
class LegSizes:
    qty_a: float
    qty_b: float
    notional_a: float
    notional_b: float

    @property
    def mismatch_pct(self) -> float:
        lo = min(self.notional_a, self.notional_b)
        return abs(self.notional_a - self.notional_b) / lo * 100.0 if lo > 0 else float("inf")

    @property
    def matched_usd(self) -> float:
        return min(self.notional_a, self.notional_b)


def size_pair(usd: float, px_a: float, px_b: float, spec_a: VenueSpec, spec_b: VenueSpec,
              max_mismatch_pct: float) -> LegSizes | None:
    """Contracts per leg for `usd` per leg, shrinking the larger leg lot by lot until the
    notionals match within `max_mismatch_pct`. None when a leg cannot be expressed."""
    qa = contracts_for_usd(usd, px_a, spec_a)
    qb = contracts_for_usd(usd, px_b, spec_b)
    if qa <= 0 or qb <= 0:
        return None
    for _ in range(200):
        legs = LegSizes(qa, qb, notional(qa, px_a, spec_a), notional(qb, px_b, spec_b))
        if legs.mismatch_pct <= max_mismatch_pct:
            return legs
        if legs.notional_a > legs.notional_b:
            qa = round(qa - spec_a.lot, 10)
        else:
            qb = round(qb - spec_b.lot, 10)
        if qa < spec_a.min_qty - 1e-12 or qb < spec_b.min_qty - 1e-12:
            return None
    return None


def hedge_qty(filled_qty: float, px_maker: float, spec_maker: VenueSpec,
              px_hedge: float, spec_hedge: VenueSpec) -> float:
    """Hedge-venue contracts matching a maker fill's notional; 0.0 = unhedgeable (below minimum)."""
    return contracts_for_usd(notional(filled_qty, px_maker, spec_maker), px_hedge, spec_hedge)
