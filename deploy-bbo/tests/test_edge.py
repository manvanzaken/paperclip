import pytest

from bbo_trader.edge import (EdgeParams, evaluate_pair, maker_entry_price, maker_exit_price,
                             exit_spread_tt, needs_requote, best_fee_venue, round_up, round_down,
                             tick_decimals, spread_pct, tm_required_pct)
from bbo_trader.models import Fees
from tests.conftest import mk_bbo, MEXC_FEES, BLOFIN_FEES

P = EdgeParams(min_edge_pct=0.05, tm_extra_edge_pct=0.05, exit_spread_pct=0.15, slip_pct=0.05)


def test_tt_wins_when_spread_clears_all_costs():
    # spec worked example: TT needs spread_tt >= 0.41% on blofin(short)/mexc(long)
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0050, ask=1.0060)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0008)
    pe = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, P)
    assert pe.spread_tt == pytest.approx(spread_pct(1.0050, 1.0008))
    assert pe.edge_tt == pytest.approx(pe.spread_tt - 0.08 - 0.28)
    assert pe.mode == "TT" and pe.maker_venue == "" and pe.edge == pytest.approx(pe.edge_tt)


def test_tm_on_wide_venue_when_tt_does_not_pay():
    # 0.31% raw spread, blofin book 0.2% wide: nothing for TT, TM on blofin clears
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0041, ask=1.0061)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0010)
    pe = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, P)
    assert pe.edge_tt < P.min_edge_pct
    assert pe.edge_tm_a == pytest.approx(spread_pct(1.0061, 1.0010) - 0.04 - 0.28)
    assert pe.edge_tm_b == pytest.approx(spread_pct(1.0041, 1.0000) - 0.06 - 0.28)
    assert pe.mode == "TM" and pe.maker_venue == "blofin" and pe.edge == pytest.approx(pe.edge_tm_a)


def test_policy_forces_maker_venue_or_disables():
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0041, ask=1.0061)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0010)
    forced = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, EdgeParams(0.05, 0.05, 0.15, 0.05, maker_venue_policy="mexc"))
    assert forced.mode == ""                     # mexc-maker edge 0.07 < 0.10
    off = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, EdgeParams(0.05, 0.05, 0.15, 0.05, tm_enabled=False))
    assert off.mode == ""
    foreign = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, EdgeParams(0.05, 0.05, 0.15, 0.05, maker_venue_policy="okx"))
    assert foreign.mode == ""


def test_maker_entry_price_joins_touch_or_floors_at_edge():
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0041, ask=1.0061)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0010)
    # req = 0.05+0.05+0.02(maker blofin)+0.02(taker mexc)+0.28 = 0.42% over mexc ask 1.0010 -> 1.0052042
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001) == pytest.approx(1.0061)
    qa_low = mk_bbo("blofin", "XYZUSDT", bid=1.0041, ask=1.0050)      # touch below the floor -> rest at floor
    assert maker_entry_price(qa_low, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001) == pytest.approx(1.0053)
    # improve one tick: 1.0060 still above the floor and above the bid
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001, improve_ticks=1) == pytest.approx(1.0060)
    # make on mexc (buy): cap = 1.0041 / 1.0044 = 0.99970... -> 0.9997 (below mexc bid, still < ask)
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "mexc", tick=0.0001) == pytest.approx(0.9997)
    # would-cross guard: improving one tick inside a one-tick-wide book would sit on the bid -> None
    qa_tight = mk_bbo("blofin", "XYZUSDT", bid=1.0053, ask=1.0054)
    assert maker_entry_price(qa_tight, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001, improve_ticks=1) is None
    assert maker_entry_price(qa_tight, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001) == pytest.approx(1.0054)
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "okx", tick=0.0001) is None


def test_maker_exit_price_both_sides():
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0020, ask=1.0030)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0005)
    assert exit_spread_tt(qa, qb) == pytest.approx(spread_pct(1.0030, 1.0000))
    # rest BUY on blofin at <= mexc bid × 1.0015 = 1.0015 (below blofin bid, fine)
    assert maker_exit_price(qa, qb, 0.15, "blofin", tick=0.0001) == pytest.approx(1.0015)
    # rest SELL on mexc at >= blofin ask / 1.0015 = 1.001498 -> 1.0015 (above mexc ask)
    assert maker_exit_price(qa, qb, 0.15, "mexc", tick=0.0001) == pytest.approx(1.0015)
    # joining never crosses (px <= bid < ask); improving inside a one-tick book would -> None
    qa2 = mk_bbo("blofin", "XYZUSDT", bid=1.0014, ask=1.0015)
    assert maker_exit_price(qa2, qb, 0.15, "blofin", tick=0.0001) == pytest.approx(1.0014)
    assert maker_exit_price(qa2, qb, 0.15, "blofin", tick=0.0001, improve_ticks=1) is None
    assert maker_exit_price(qa, qb, 0.15, "okx", tick=0.0001) is None


def test_rounding_and_requote_helpers():
    assert tick_decimals(0.0001) == 4 and tick_decimals(1.0) == 0 and tick_decimals(0.5) == 1
    assert round_up(1.00001, 0.0001) == pytest.approx(1.0001)
    assert round_up(1.0001, 0.0001) == pytest.approx(1.0001)      # already on tick
    assert round_down(1.00019, 0.0001) == pytest.approx(1.0001)
    assert needs_requote(1.0000, 1.0001, 0.0001, 1)
    assert not needs_requote(1.0000, 1.00005, 0.0001, 1)
    assert not needs_requote(1.0000, 1.0001, 0.0001, 2)
    assert best_fee_venue("blofin", BLOFIN_FEES, "mexc", MEXC_FEES) == "blofin"   # 0.04 vs 0.02 saving
    assert best_fee_venue("mexc", MEXC_FEES, "blofin", BLOFIN_FEES) == "blofin"


def test_degenerate_quote_never_yields_zero_peg():
    # a glitched/near-zero quote on A must never yield a postable 0.0 peg: the lo=0.0 bound of the BUY
    # branches rejects it (the extra `0.0 < px` clause in _postable is belt-and-braces for the SELL branches)
    qa = mk_bbo("blofin", "XYZUSDT", bid=1e-9, ask=2e-9)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0, ask=1.001)
    assert maker_exit_price(qa, qb, 0.15, "blofin", tick=0.0001) is None
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "mexc", tick=0.0001) is None


def test_requote_detects_one_tick_at_high_price():
    # absolute epsilon breaks down at 6-figure prices; tolerance must scale with tick, not price
    assert needs_requote(105123.45, 105123.46, 0.01, 1) is True
    assert needs_requote(105123.45, 105123.45, 0.01, 1) is False
    with pytest.raises(ValueError):
        needs_requote(105123.45, 105123.46, 0, 1)


def test_tick_decimals_and_rounding_extremes():
    assert tick_decimals(1e-8) == 8
    assert tick_decimals(2.5e-7) == 8
    assert tick_decimals(1e-13) == 13
    assert tick_decimals(100.0) == 0
    # on-grid prices must round-trip through round_up/round_down unchanged at tiny/large scales
    assert round_up(0.00012345, 1e-8) == pytest.approx(0.00012345)
    assert round_down(0.00012345, 1e-8) == pytest.approx(0.00012345)
    assert round_up(1000.000123, 1e-6) == pytest.approx(1000.000123)
    assert round_down(10000.0001, 0.0001) == pytest.approx(10000.0001)
    # these discriminate the relative epsilon from a fixed 1e-9 (which mis-rounds them by a full tick)
    assert round_up(963.443702, 1e-6) == pytest.approx(963.443702)
    assert round_up(306776.34, 0.01) == pytest.approx(306776.34)
    assert round_down(82940652.27, 0.01) == pytest.approx(82940652.27)
    # ...and the epsilon cap keeps extreme px/tick ratios from flipping the rounding direction
    assert round_up(65000.0, 1e-8) == pytest.approx(65000.0) and round_down(65000.0, 1e-8) == pytest.approx(65000.0)
    with pytest.raises(ValueError):
        tick_decimals(0)
    with pytest.raises(ValueError):
        tick_decimals(float("nan"))
    with pytest.raises(ValueError):
        needs_requote(1.0, 1.1, float("nan"), 1)


def test_tm_viable_when_tt_spread_negative():
    # blofin book is 0.6% wide with an inverted (negative) TT spread: TT can't fire, but making
    # on the wide side of blofin still clears the required edge against mexc's ask
    qa = mk_bbo("blofin", "XYZUSDT", bid=0.9995, ask=1.0055)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0000, ask=1.0010)
    pe = evaluate_pair(qa, qb, BLOFIN_FEES, MEXC_FEES, P)
    assert pe.spread_tt < 0
    assert pe.mode == "TM"
    assert pe.maker_venue == "blofin"
    px = maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "blofin", tick=0.0001)
    assert px is not None and px > qa.bid


def test_make_on_b_would_cross_guards():
    # entry make-on-B (BUY on mexc): improving 1 tick inside a one-tick mexc book lands exactly on
    # the ask -> would cross, must reject even though the edge-required cap is not binding here
    qa = mk_bbo("blofin", "XYZUSDT", bid=1.0060, ask=1.0070)
    qb = mk_bbo("mexc", "XYZUSDT", bid=1.0009, ask=1.0010)
    assert maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P, "mexc", tick=0.0001, improve_ticks=1) is None
    # exit make-on-B (SELL on mexc): improving 1 tick inside a one-tick mexc book lands exactly on
    # the bid -> would cross, must reject even though the exit-target floor is not binding here
    qa2 = mk_bbo("blofin", "XYZUSDT", bid=1.0020, ask=1.0025)
    qb2 = mk_bbo("mexc", "XYZUSDT", bid=1.0014, ask=1.0015)
    assert maker_exit_price(qa2, qb2, 0.15, "mexc", tick=0.0001, improve_ticks=1) is None


def test_best_fee_venue_tie_goes_to_a():
    assert best_fee_venue("x", Fees(0.05, 0.02), "y", Fees(0.06, 0.03)) == "x"


def test_peg_properties_on_a_grid():
    """Deterministic sweep (no randomness): every postable peg maker_entry_price/maker_exit_price
    return must (a) sit strictly on the resting-order side of its own book (never cross) and
    (b) actually realize the edge/exit target it was pegged to meet."""
    ticks = (1e-6, 1e-4, 0.01)
    mids = (0.001, 1.0, 250.0, 65000.0)
    width_ticks_opts = (1, 3, 20)
    offset_ticks_opts = (-30, -5, 0, 5, 30)
    checked = 0
    produced: dict[tuple, int] = {}
    for tick in ticks:
        for mid in mids:
            if tick >= mid / 100:
                continue
            for width_ticks in width_ticks_opts:
                qb = mk_bbo("mexc", "XYZUSDT", bid=mid, ask=mid + width_ticks * tick)
                for offset_ticks in offset_ticks_opts:
                    qa_bid = qb.bid + offset_ticks * tick
                    qa_ask = qb.bid + (offset_ticks + width_ticks) * tick
                    if qa_bid <= 0 or qa_ask <= 0 or qb.bid <= 0 or qb.ask <= 0:
                        continue
                    qa = mk_bbo("blofin", "XYZUSDT", bid=qa_bid, ask=qa_ask)
                    for maker_venue in ("blofin", "mexc"):
                        for improve_ticks in (0, 1):
                            checked += 1
                            entry_px = maker_entry_price(qa, qb, BLOFIN_FEES, MEXC_FEES, P,
                                                          maker_venue, tick, improve_ticks)
                            if entry_px is not None:
                                produced[("entry", maker_venue, improve_ticks)] = produced.get(("entry", maker_venue, improve_ticks), 0) + 1
                                if maker_venue == "blofin":
                                    assert entry_px > qa.bid
                                    req = tm_required_pct(BLOFIN_FEES, MEXC_FEES, P)
                                    assert spread_pct(entry_px, qb.ask) >= req - 1e-9
                                else:
                                    assert entry_px < qb.ask
                                    req = tm_required_pct(MEXC_FEES, BLOFIN_FEES, P)
                                    assert spread_pct(qa.bid, entry_px) >= req - 1e-9
                            exit_px = maker_exit_price(qa, qb, 0.15, maker_venue, tick, improve_ticks)
                            if exit_px is not None:
                                produced[("exit", maker_venue, improve_ticks)] = produced.get(("exit", maker_venue, improve_ticks), 0) + 1
                                if maker_venue == "blofin":
                                    assert exit_px < qa.ask
                                    assert spread_pct(exit_px, qb.bid) <= 0.15 + 1e-9
                                else:
                                    assert exit_px > qb.bid
                                    assert spread_pct(qa.ask, exit_px) <= 0.15 + 1e-9
    assert checked > 0
    # every (phase, venue, improve) cell must have produced pegs, or the invariants above were vacuous
    assert len(produced) == 8 and min(produced.values()) > 0
