import pytest

from bbo_trader.edge import (EdgeParams, evaluate_pair, maker_entry_price, maker_exit_price,
                             exit_spread_tt, needs_requote, best_fee_venue, round_up, round_down,
                             tick_decimals, spread_pct)
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
