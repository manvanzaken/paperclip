"""Pure sizing: USD notional → venue contracts, matched pair legs, hedge plans and residual handling."""
from __future__ import annotations

import math
from dataclasses import dataclass

from .models import VenueSpec


def lots_floor(qty: float, spec: VenueSpec) -> float:
    """Largest lot multiple <= qty; 0.0 when below the venue minimum (or qty is not a finite positive)."""
    if not math.isfinite(qty) or qty <= 0 or not (math.isfinite(spec.lot) and spec.lot > 0):
        return 0.0
    lots = qty / spec.lot
    n = round(math.floor(lots + 1e-9 * max(1.0, lots)) * spec.lot, 10)
    return n if n >= spec.min_qty - 1e-12 else 0.0


def contracts_for_usd(usd: float, price: float, spec: VenueSpec) -> float:
    """Largest lot-multiple not exceeding `usd` at `price`; 0.0 when below the venue minimum."""
    if not (math.isfinite(usd) and math.isfinite(price)) or usd <= 0 or price <= 0 or spec.contract_size <= 0:
        return 0.0
    return lots_floor(usd / (price * spec.contract_size), spec)


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
              max_mismatch_pct: float, min_usd: float = 0.0) -> LegSizes | None:
    """Contracts per leg for `usd` per leg. The larger leg is shrunk onto the smaller one's notional
    (closed-form jump per iteration, so coarse/fine lot combinations converge in a few steps) until the
    notionals match within `max_mismatch_pct`. None when a leg cannot be expressed or the matched
    notional ends up below `min_usd`."""
    qa = contracts_for_usd(usd, px_a, spec_a)
    qb = contracts_for_usd(usd, px_b, spec_b)
    if qa <= 0 or qb <= 0:
        return None
    tol = 1.0 + max_mismatch_pct / 100.0
    for _ in range(64):
        legs = LegSizes(qa, qb, notional(qa, px_a, spec_a), notional(qb, px_b, spec_b))
        if legs.mismatch_pct <= max_mismatch_pct:
            return legs if legs.matched_usd >= min_usd else None
        if legs.notional_a > legs.notional_b:
            nq = contracts_for_usd(legs.notional_b * tol, px_a, spec_a)
            qa = nq if nq < qa else round(qa - spec_a.lot, 10)      # jump, else guarantee progress
        else:
            nq = contracts_for_usd(legs.notional_a * tol, px_b, spec_b)
            qb = nq if nq < qb else round(qb - spec_b.lot, 10)
        if qa < spec_a.min_qty - 1e-12 or qb < spec_b.min_qty - 1e-12:
            return None
    return None


@dataclass(frozen=True)
class HedgePlan:
    hedge_qty: float            # contracts to send on the hedge venue (0.0 = nothing hedgeable yet)
    covered_maker_qty: float    # maker-venue contracts the hedge notional actually covers
    residual_maker_qty: float   # maker contracts still unhedged after this round


def hedge_plan(unhedged_maker_qty: float, px_maker: float, spec_maker: VenueSpec,
               px_hedge: float, spec_hedge: VenueSpec) -> HedgePlan:
    """Hedge-venue contracts for a maker fill, and how much of the fill they really cover (the hedge
    is floored to whole lots, so a residual below one hedge contract stays unhedged and must be
    tracked — never marked hedged)."""
    hq = contracts_for_usd(notional(unhedged_maker_qty, px_maker, spec_maker), px_hedge, spec_hedge)
    if hq <= 0:
        return HedgePlan(0.0, 0.0, unhedged_maker_qty)
    covered = min(unhedged_maker_qty, notional(hq, px_hedge, spec_hedge) / (px_maker * spec_maker.contract_size))
    return HedgePlan(hq, round(covered, 10), round(unhedged_maker_qty - covered, 10))


def hedge_qty(filled_qty: float, px_maker: float, spec_maker: VenueSpec,
              px_hedge: float, spec_hedge: VenueSpec) -> float:
    """Hedge-venue contracts matching a maker fill's notional; 0.0 = unhedgeable (below minimum).
    Simple-case shorthand for hedge_plan(...).hedge_qty; the executor uses hedge_plan."""
    return hedge_plan(filled_qty, px_maker, spec_maker, px_hedge, spec_hedge).hedge_qty


def excess_to_flatten(residual_qty: float, matched_qty: float, spec: VenueSpec, max_mismatch_pct: float) -> float:
    """Maker-venue contracts to flatten from an unhedged residual once the resting order is terminal:
    the residual (rounded down to lots) when nothing is matched or it exceeds the mismatch tolerance
    of the matched quantity. 0.0 means EITHER the residual is within tolerance (accept it as exposure)
    OR it is below the venue minimum and cannot be sent (exposure retained) — callers must tell the two
    apart with `residual <= matched × tolerance` when they report it."""
    if residual_qty <= 0:
        return 0.0
    if matched_qty > 0 and residual_qty <= matched_qty * max_mismatch_pct / 100.0:
        return 0.0
    return lots_floor(residual_qty, spec)
