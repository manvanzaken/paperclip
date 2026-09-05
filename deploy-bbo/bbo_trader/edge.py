"""Pure edge math for the two execution modes, maker price pegs and tick rounding.

All spreads, fees and edges are percent points. `qa` is the venue we SELL on (higher bid),
`qb` the venue we BUY on. See the spec section "Strategy: edge math and mode selection".

All entry points assume qa.ok and qb.ok (positive, uncrossed quotes) and qa.symbol == qb.symbol."""
from __future__ import annotations

import math
from dataclasses import dataclass, replace
from decimal import Decimal

from .models import BBO, Fees


@dataclass(frozen=True)
class EdgeParams:
    min_edge_pct: float
    tm_extra_edge_pct: float
    exit_spread_pct: float
    slip_pct: float
    tt_enabled: bool = True
    tm_enabled: bool = True
    maker_venue_policy: str = "best_edge"  # "best_edge" or a venue name


@dataclass(frozen=True)
class PairEval:
    symbol: str
    venue_a: str
    venue_b: str
    spread_tt: float
    edge_tt: float
    spread_tm_a: float   # make on A: rest SELL at ask_A, hedge BUY at ask_B
    edge_tm_a: float
    spread_tm_b: float   # make on B: rest BUY at bid_B, hedge SELL at bid_A
    edge_tm_b: float
    mode: str = ""       # "TT" | "TM" | ""
    maker_venue: str = ""
    edge: float = 0.0    # edge of the chosen mode


def spread_pct(sell_px: float, buy_px: float) -> float:
    return (sell_px - buy_px) / buy_px * 100.0


def exit_spread_tt(qa: BBO, qb: BBO) -> float:
    """What closing at market costs now: buy back A at its ask, sell B at its bid."""
    return spread_pct(qa.ask, qb.bid)


def _base_cost(fa: Fees, fb: Fees, p: EdgeParams) -> float:
    """Conservative exit fees (taker/taker) + exit target + slippage allowance."""
    return fa.taker + fb.taker + p.exit_spread_pct + p.slip_pct


def evaluate_pair(qa: BBO, qb: BBO, fa: Fees, fb: Fees, p: EdgeParams) -> PairEval:
    base = _base_cost(fa, fb, p)
    s_tt = spread_pct(qa.bid, qb.ask)
    s_tm_a = spread_pct(qa.ask, qb.ask)
    s_tm_b = spread_pct(qa.bid, qb.bid)
    pe = PairEval(qa.symbol, qa.venue, qb.venue,
                  s_tt, s_tt - (fa.taker + fb.taker) - base,
                  s_tm_a, s_tm_a - (fa.maker + fb.taker) - base,
                  s_tm_b, s_tm_b - (fa.taker + fb.maker) - base)
    return choose_mode(pe, p)


def choose_mode(pe: PairEval, p: EdgeParams) -> PairEval:
    """TT first (no fill risk); else TM on the venue picked by policy; else nothing."""
    if p.tt_enabled and pe.edge_tt >= p.min_edge_pct:
        return replace(pe, mode="TT", maker_venue="", edge=pe.edge_tt)
    if not p.tm_enabled:
        return replace(pe, mode="", maker_venue="", edge=0.0)
    if p.maker_venue_policy == "best_edge":
        mv, e = ((pe.venue_a, pe.edge_tm_a) if pe.edge_tm_a >= pe.edge_tm_b
                 else (pe.venue_b, pe.edge_tm_b))
    elif p.maker_venue_policy == pe.venue_a:
        mv, e = pe.venue_a, pe.edge_tm_a
    elif p.maker_venue_policy == pe.venue_b:
        mv, e = pe.venue_b, pe.edge_tm_b
    else:
        return replace(pe, mode="", maker_venue="", edge=0.0)
    if e >= p.min_edge_pct + p.tm_extra_edge_pct:
        return replace(pe, mode="TM", maker_venue=mv, edge=e)
    return replace(pe, mode="", maker_venue="", edge=0.0)


def tm_required_pct(maker_fees: Fees, taker_fees: Fees, p: EdgeParams) -> float:
    """Percent the maker fill must clear over the hedge touch to meet the TM edge."""
    return (p.min_edge_pct + p.tm_extra_edge_pct + maker_fees.maker + taker_fees.taker
            + _base_cost(maker_fees, taker_fees, p))


def tick_decimals(tick: float) -> int:
    if not (tick > 0):   # also rejects NaN
        raise ValueError(f"tick must be positive, got {tick!r}")
    return max(0, -Decimal(repr(tick)).normalize().as_tuple().exponent)


def _eps(px: float, tick: float) -> float:
    """Rounding tolerance in ticks: scales with px/tick (float error grows with the ratio) but is capped
    well below one tick so it can never flip a rounding direction."""
    return min(1e-3, max(1e-9, abs(px / tick) * 1e-12))


def round_up(px: float, tick: float) -> float:
    eps = _eps(px, tick)
    return round(math.ceil(px / tick - eps) * tick, tick_decimals(tick))


def round_down(px: float, tick: float) -> float:
    eps = _eps(px, tick)
    return round(math.floor(px / tick + eps) * tick, tick_decimals(tick))


def _postable(px: float, lo: float, hi: float) -> float | None:
    """A post-only price must sit strictly inside (lo, hi) and be positive."""
    return px if 0.0 < px and lo < px < hi else None


def maker_entry_price(qa: BBO, qb: BBO, fa: Fees, fb: Fees, p: EdgeParams, maker_venue: str,
                      tick: float, improve_ticks: int = 0) -> float | None:
    """Resting price for a TM entry. Assumes the caller already confirmed mode == "TM" for this
    maker venue (choose_mode); returns None only when no post-only price is available (it would
    cross, or is not positive)."""
    if maker_venue == qa.venue:  # rest SELL on A, hedge BUY at ask_B
        req = tm_required_pct(fa, fb, p)
        floor_px = qb.ask * (1.0 + req / 100.0)
        px = round_up(max(qa.ask - improve_ticks * tick, floor_px), tick)
        return _postable(px, qa.bid, float("inf"))
    if maker_venue == qb.venue:  # rest BUY on B, hedge SELL at bid_A
        req = tm_required_pct(fb, fa, p)
        cap_px = qa.bid / (1.0 + req / 100.0)
        px = round_down(min(qb.bid + improve_ticks * tick, cap_px), tick)
        return _postable(px, 0.0, qb.ask)
    return None


def maker_exit_price(qa: BBO, qb: BBO, exit_spread_pct: float, maker_venue: str, tick: float,
                     improve_ticks: int = 0) -> float | None:
    """Resting price for a TM exit of a position short A / long B (close = BUY A, SELL B).
    The fill must realize an exit spread <= target against the other venue's live touch."""
    if maker_venue == qa.venue:  # rest BUY on A; hedge SELL B at bid_B: (p - bid_B)/bid_B <= X
        cap_px = qb.bid * (1.0 + exit_spread_pct / 100.0)
        px = round_down(min(qa.bid + improve_ticks * tick, cap_px), tick)
        return _postable(px, 0.0, qa.ask)
    if maker_venue == qb.venue:  # rest SELL on B; hedge BUY A at ask_A: (ask_A - p)/p <= X
        floor_px = qa.ask / (1.0 + exit_spread_pct / 100.0)
        px = round_up(max(qb.ask - improve_ticks * tick, floor_px), tick)
        return _postable(px, qb.bid, float("inf"))
    return None


def needs_requote(working_px: float, new_px: float, tick: float, requote_ticks: int) -> bool:
    if not (tick > 0):   # also rejects NaN
        raise ValueError(f"tick must be positive, got {tick!r}")
    return abs(new_px - working_px) / tick >= requote_ticks - 1e-9


def best_fee_venue(venue_a: str, fa: Fees, venue_b: str, fb: Fees) -> str:
    """Venue where making saves the most (largest taker − maker gap); ties go to A."""
    return venue_a if (fa.taker - fa.maker) >= (fb.taker - fb.maker) else venue_b
