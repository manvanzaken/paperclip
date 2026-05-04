from __future__ import annotations
from decimal import Decimal
from typing import List, Tuple, Optional
from .models import LegSide, Book


def gross_multiplier(legs: List[Tuple[str, LegSide, Decimal]]) -> float:
    """
    Compute the round-trip multiplier for a 3-leg cycle using touch prices.

    Convention:
      - LegSide.BUY  with price `p` (the ask): 1 unit of quote -> 1/p units of base.
      - LegSide.SELL with price `p` (the bid): 1 unit of base  -> p units of quote.

    Returns the unitless multiplier; >1 means profitable before fees.
    """
    m = Decimal("1")
    for _symbol, side, price in legs:
        if price <= 0:
            return 0.0
        if side == LegSide.BUY:
            m = m / price
        else:
            m = m * price
    return float(m)


def net_multiplier(gross: float, fee_pct: Decimal) -> float:
    """Apply taker fee on each of the 3 legs. fee_pct is in percent (e.g. 0.10 = 0.10%)."""
    f = float(fee_pct) / 100.0
    return gross * (1.0 - f) ** 3


def simulate_cycle_through_book(*a, **kw): raise NotImplementedError


def binary_search_executable_size(*a, **kw): raise NotImplementedError
