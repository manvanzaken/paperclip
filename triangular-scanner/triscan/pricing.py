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


from dataclasses import dataclass


@dataclass
class CycleResult:
    output_quote: float       # final units of starting anchor returned
    input_consumed: float     # how much of the requested input was used
    bottleneck_leg: int       # 0..2 — leg that ran out first (or -1 if fully filled)
    fully_filled: bool


def _walk_buy(asks, quote_in: Decimal) -> Tuple[Decimal, Decimal]:
    """Spend `quote_in` units of quote against asks. Returns (base_out, quote_consumed)."""
    base_out = Decimal("0")
    quote_left = quote_in
    for level in asks:
        if quote_left <= 0:
            break
        max_quote_at_level = level.price * level.size
        if quote_left >= max_quote_at_level:
            base_out += level.size
            quote_left -= max_quote_at_level
        else:
            base_out += quote_left / level.price
            quote_left = Decimal("0")
    return base_out, quote_in - quote_left


def _walk_sell(bids, base_in: Decimal) -> Tuple[Decimal, Decimal]:
    """Sell `base_in` units of base against bids. Returns (quote_out, base_consumed)."""
    quote_out = Decimal("0")
    base_left = base_in
    for level in bids:
        if base_left <= 0:
            break
        if base_left >= level.size:
            quote_out += level.size * level.price
            base_left -= level.size
        else:
            quote_out += base_left * level.price
            base_left = Decimal("0")
    return quote_out, base_in - base_left


def simulate_cycle_through_book(
    legs: List[Tuple[str, LegSide, Book]],
    input_size: Decimal,
) -> CycleResult:
    """
    Simulate spending `input_size` units of the starting anchor through 3 legs of L2 depth.

    Tracks which leg first runs out of liquidity (bottleneck_leg). Quote/base flows
    follow the same convention as gross_multiplier: BUY consumes quote and produces base;
    SELL consumes base and produces quote.

    The first leg consumes the anchor as its quote currency. The second leg's input is the
    base output of leg 0; whether that is leg-1's quote or base depends on leg-1's side.
    For BUY legs the input must be denominated in quote, for SELL legs in base. The
    legs as enumerated are guaranteed to chain currencies correctly by the enumerator.
    """
    if input_size <= 0:
        return CycleResult(0.0, 0.0, -1, False)

    bottleneck = -1
    fully_filled = True
    flow = input_size  # generic carrier

    for i, (_symbol, side, book) in enumerate(legs):
        if side == LegSide.BUY:
            out, consumed = _walk_buy(book.asks, flow)
        else:
            out, consumed = _walk_sell(book.bids, flow)

        if consumed < flow:
            bottleneck = i
            fully_filled = False
            if i == 0:
                input_consumed = consumed
            else:
                input_consumed = input_size * (consumed / flow) if flow > 0 else Decimal("0")
            flow = out
            return CycleResult(
                output_quote=float(_carry_remaining_zero(legs, i, out)),
                input_consumed=float(input_consumed),
                bottleneck_leg=i,
                fully_filled=False,
            )
        flow = out

    return CycleResult(
        output_quote=float(flow),
        input_consumed=float(input_size),
        bottleneck_leg=bottleneck,
        fully_filled=fully_filled,
    )


def _carry_remaining_zero(legs, stopped_at: int, partial_out: Decimal) -> Decimal:
    """If we ran out at leg `stopped_at`, run the remaining legs against `partial_out`
    just so output_quote is at least the partial cycle realized. Walks remaining legs."""
    flow = partial_out
    for j in range(stopped_at + 1, len(legs)):
        _sym, side, book = legs[j]
        if side == LegSide.BUY:
            flow, _ = _walk_buy(book.asks, flow)
        else:
            flow, _ = _walk_sell(book.bids, flow)
    return flow


def binary_search_executable_size(*a, **kw): raise NotImplementedError
