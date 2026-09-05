import asyncio

import pytest

from bbo_trader.models import Intent, OPEN, CLOSED, MAKER_RESTING, HEDGING, EXIT_MAKER_RESTING, OrderAck
from tests.test_execution_tt import Harness, SYM


async def settle():
    for _ in range(5):
        await asyncio.sleep(0.005)


async def test_tm_entry_rests_fills_hedges_opens(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    intent = Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                    rest_price=1.0061, size_usd=25.0, spread_pct=0.31, edge_pct=0.19, ts=0.0)
    pos = await h.ex.enter_tm(intent)
    assert pos is not None and pos.status == MAKER_RESTING and pos.maker_side == "sell"
    assert pos.maker_qty == 20.0 and pos.qty_b == 2.0 and pos.maker_order_id.startswith("sim-")
    assert len(await h.sim("blofin").open_orders()) == 1
    await asyncio.sleep(0.005)                                   # order becomes live at the venue
    h.quote("blofin", 1.0061, 1.0062, bq=100.0)                  # buyer lifts our ask: 50% of 100 ≥ 20 → full fill
    await settle()
    assert pos.status == OPEN and pos.mode == "TM"
    assert pos.filled_a == 20.0 and pos.entry_price_a == pytest.approx(1.0061)
    assert pos.filled_b == 2.0 and pos.entry_price_b == pytest.approx(1.0010)   # hedge bought mexc ask
    assert pos.hedged_qty == 20.0 and pos.fee_liquidity["maker"] == "maker"
    assert pos.entry_fees_usd == pytest.approx(20 * 1.0061 * 0.02 / 100 + 2 * 1.0010 * 10 * 0.02 / 100)
    assert pos.entry_spread_pct == pytest.approx((1.0061 - 1.0010) / 1.0010 * 100)
    assert h.metrics.hists["fill_to_hedged"].count == 1 and h.metrics.hists["post_to_first_fill"].count == 1
    assert (await h.sim("blofin").positions())[0].side == "short" and (await h.sim("mexc").positions())[0].qty == 2.0


async def test_tm_partial_fill_cancels_remainder_and_opens_partial(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0061, 1.0062, bq=20.0)                   # 50% of 20 → 10 contracts = $10.06 → 1 mexc contract
    await settle()
    assert pos.status == OPEN
    assert pos.filled_a == 10.0 and pos.filled_b == 1.0 and pos.hedged_qty == 10.0
    assert pos.size_usd == pytest.approx(min(10 * 1.0061, 1 * 1.0010 * 10))
    assert await h.sim("blofin").open_orders() == []              # remainder cancelled on first fill
    assert pos.maker_cancel_sent
    # the $10.01 hedge contract covered 9.95 maker contracts; the 0.05 residual is within tolerance
    assert (await h.sim("blofin").positions())[0].qty == 10.0


async def test_tm_residual_beyond_tolerance_is_flattened(tmp_path):
    h = Harness(tmp_path, max_position_usd=31.0)
    h.quote("blofin", 1.0000, 1.0020)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0020, size_usd=31.0))
    assert pos.maker_qty == 30.0 and pos.qty_b == 3.0              # $30.06 vs 3 × $10.01
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0020, 1.0021, bq=50.0)                   # 50% of 50 -> 25 contracts fill, then cancel
    await settle()
    assert pos.status == OPEN
    # $25.05 filled -> 2 mexc contracts ($20.02) cover ~19.98 maker contracts; residual ~5 > 5% -> flattened
    assert pos.filled_b == 2.0 and pos.filled_a == pytest.approx(20.0, abs=0.2)
    assert (await h.sim("blofin").positions())[0].qty == pytest.approx(pos.filled_a)
    assert pos.exit_fees_usd > 0                                 # the flatten paid a taker fee


async def test_tm_cancel_with_nothing_filled_discards(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await h.ex.cancel_maker(pos, "ttl")
    await settle()
    assert h.book.open == [] and h.book.closed == [] and h.metrics.funnel["maker_cancelled"] == 1
    assert await h.sim("blofin").positions() == []


async def test_upgrade_to_tt_after_cancel(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    h.quote("blofin", 1.0055, 1.0061)                            # TT now pays; strategy would emit UPGRADE_TT
    await h.ex.upgrade_to_tt(pos)
    await settle()
    assert pos.status == OPEN and pos.mode == "TT"
    assert pos.entry_price_a == pytest.approx(1.0055) and pos.fee_liquidity["entry_a"] == "taker"


async def test_requote_amends_resting_price(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await h.ex.requote(pos, 1.0063)
    assert pos.maker_rest_price == 1.0063
    assert (await h.sim("blofin").open_orders())[0].client_id == pos.maker_client_id
    assert h.sim("blofin")._orders[pos.maker_client_id].price == 1.0063


async def test_tm_exit_take_profit_and_tt_exit_while_resting(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    assert pos.status == OPEN
    # rest a reduce-only buy on blofin at 1.0015 (take-profit against mexc bid 1.0000 × 1.0015)
    h.quote("blofin", 1.0020, 1.0030)
    h.quote("mexc", 1.0000, 1.0005)
    assert await h.ex.exit_tm(pos, Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin", rest_price=1.0015))
    assert pos.status == EXIT_MAKER_RESTING and pos.maker_side == "buy" and pos.maker_qty == 20.0
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0010, 1.0015, aq=100.0)                  # seller hits our bid → fill 20
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "take_profit" and pos.exit_mode == "TM"
    assert pos.exit_price_a == pytest.approx(1.0015) and pos.exit_price_b == pytest.approx(1.0000)   # hedge sold mexc bid
    assert pos.fee_liquidity["maker"] == "maker" and pos.net_pnl_usd > 0
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []

    # second position: resting exit maker, then a TT exit (convergence) cancels it and closes at market
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos2 = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    h.quote("blofin", 1.0020, 1.0030)
    h.quote("mexc", 1.0000, 1.0005)
    assert await h.ex.exit_tm(pos2, Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin", rest_price=1.0015))
    h.quote("blofin", 1.0004, 1.0006)                            # converged
    h.quote("mexc", 1.0003, 1.0005)
    await h.ex.exit_tt(pos2, "convergence")
    await settle()
    assert pos2.status == CLOSED and pos2.exit_reason == "convergence" and pos2.exit_mode == "TM+TT"
    assert pos2.exit_price_a == pytest.approx(1.0006) and pos2.exit_price_b == pytest.approx(1.0003)
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []
    assert h.book.total_trades == 2


async def test_failed_cancel_leaves_nothing_in_flight(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    sim = h.sim("blofin")
    real_cancel = sim.cancel

    async def bad_cancel(*a, **k):
        return OrderAck(False, error="boom")

    async def no_query(*a, **k):
        return None
    sim.cancel, sim.query_order = bad_cancel, no_query
    await h.ex.cancel_maker(pos, "test")
    assert pos.status == MAKER_RESTING and not pos.maker_cancel_sent          # transport failure: retry allowed
    sim.supports_amend = False
    await h.ex.requote(pos, 1.0063)                                          # cancel+new path fails the same way
    assert not pos.requote_pending and not pos.maker_cancel_sent and pos.maker_rest_price == 1.0061
    await h.ex.upgrade_to_tt(pos)
    assert not pos.upgrade_pending and not pos.maker_cancel_sent
    sim.cancel = real_cancel
    await h.ex.cancel_maker(pos, "test")                                     # the venue is back: cancel goes through
    assert pos.maker_cancel_sent


async def test_exit_maker_rejected_when_venue_is_flat_books_the_leg_and_closes(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0061, 1.0062, bq=100.0)
    await settle()
    assert pos.status == OPEN
    h.sim("blofin")._pos.clear()                                   # the venue lost our short (accounting drift)
    h.sim("blofin")._avg.clear()
    h.quote("blofin", 1.0020, 1.0030)
    h.quote("mexc", 1.0000, 1.0005)
    assert await h.ex.exit_tm(pos, Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                          rest_price=1.0015))
    assert pos.status == EXIT_MAKER_RESTING
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0010, 1.0015, aq=100.0)                    # our reduce-only bid would fill: venue says nothing to reduce
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "venue_flat" and pos.exit_filled_a == pos.filled_a
    assert await h.sim("mexc").positions() == [] and pos.exit_filled_b == pos.filled_b


async def test_stray_fill_during_a_flatten_does_not_close_the_position(tmp_path, monkeypatch):
    from bbo_trader import execution
    from bbo_trader.models import OrderEvent
    from bbo_trader.quotes import QuoteBoard
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    sim = h.sim("blofin")
    sim.supports_amend = False
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    old_cid = pos.maker_client_id
    await h.ex.requote(pos, 1.0063)
    await settle()
    await asyncio.sleep(0.005)
    h.sim("mexc").board = QuoteBoard(2.0)                          # hedge venue unreachable: the fill will be flattened
    h.quote("blofin", 1.0063, 1.0064, bq=100.0)                    # the live order fills 20 (flatten now in flight)
    sim._pos[SYM] = sim._pos.get(SYM, 0.0) - 12.0                  # ...and the cancelled order's fill lands at the venue
    h.ex.on_order_event(OrderEvent("blofin", old_cid, "sim-1", "filled", filled_qty=12.0, avg_price=1.0061,
                                   fee=0.0024, liquidity="maker", ts=h.ex.clock()))
    await settle()
    assert pos.status == "DEGRADED" and pos.filled_a - pos.exit_filled_a == pytest.approx(12.0)   # not hedge_unwound
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and await sim.positions() == [] and h.book.open == []


async def test_stray_fill_during_the_tt_upgrade_is_not_clobbered(tmp_path, monkeypatch):
    from bbo_trader import execution
    from bbo_trader.models import OrderEvent
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    sim = h.sim("blofin")
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    old_cid = pos.maker_client_id
    h.quote("blofin", 1.0060, 1.0065)                              # a TT edge: upgrade
    await h.ex.upgrade_to_tt(pos)
    for _ in range(50):                                            # the cancel finalizes into TT legs in flight
        if pos.status == "TT_ENTERING":
            break
        await asyncio.sleep(0.001)
    assert pos.status == "TT_ENTERING"
    sim._pos[SYM] = sim._pos.get(SYM, 0.0) - 12.0                  # the cancelled maker order filled after all
    h.ex.on_order_event(OrderEvent("blofin", old_cid, "sim-1", "filled", filled_qty=12.0, avg_price=1.0061,
                                   fee=0.0024, liquidity="maker", ts=h.ex.clock()))
    await settle()
    assert pos.status == "DEGRADED" and pos.filled_a >= 12.0       # the TT fill was ADDED to the stray, not written over it
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    for _ in range(3):
        await h.ex.retry_degraded()
        await settle()
    assert pos.status == CLOSED and await sim.positions() == [] and await h.sim("mexc").positions() == []


async def test_straggler_on_a_cancelled_exit_maker_is_booked_as_an_exit_fill(tmp_path, monkeypatch):
    from bbo_trader import execution
    from bbo_trader.models import OrderEvent
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    h.quote("blofin", 1.0020, 1.0030)
    h.quote("mexc", 1.0000, 1.0005)
    assert await h.ex.exit_tm(pos, Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                          rest_price=1.0015))
    cid = pos.maker_client_id
    await h.ex.cancel_maker(pos, "test")
    await settle()
    assert pos.status == OPEN and pos.maker_venue == ""
    sim = h.sim("blofin")
    sim._pos[SYM] += 12.0                                          # the venue filled the cancelled reduce-only buy: short 20 -> 8
    h.ex.on_order_event(OrderEvent("blofin", cid, "sim-2", "filled", filled_qty=12.0, avg_price=1.0015,
                                   fee=0.0024, liquidity="maker", ts=h.ex.clock()))
    await settle()
    assert pos.status == "DEGRADED" and pos.exit_filled_a == 12.0 and pos.filled_a == 20.0 and pos.filled_b == 2.0
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and await sim.positions() == [] and await h.sim("mexc").positions() == []
    assert abs(pos.gross_pnl_usd) < 0.2                            # no fabricated P&L from a mis-booked leg


async def test_stray_fills_after_close_or_discard_alert_a_human(tmp_path):
    from bbo_trader.models import OrderEvent
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    cid = pos.maker_client_id
    await h.ex.cancel_maker(pos, "test")
    await settle()
    assert pos not in h.book.open                                   # nothing filled: discarded
    h.ex.on_order_event(OrderEvent("blofin", cid, "sim-1", "filled", filled_qty=25.0, avg_price=1.0061, fee=0.005,
                                   liquidity="maker", ts=h.ex.clock()))
    await asyncio.sleep(0.01)
    assert any("STRAY_FILL_NO_POSITION" in n for n in h.notes)
    pos2 = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                      rest_price=1.0061, size_usd=25.0))
    cid2 = pos2.maker_client_id
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0061, 1.0062, bq=100.0)
    await settle()
    h.quote("blofin", 1.0010, 1.0012)
    h.quote("mexc", 1.0009, 1.0011)
    await h.ex.exit_tt(pos2, "convergence")
    assert pos2.status == CLOSED
    h.ex.on_order_event(OrderEvent("blofin", cid2, "sim-2", "filled", filled_qty=25.0, avg_price=1.0061, fee=0.005,
                                   liquidity="maker", ts=h.ex.clock()))               # 5 more than we ever booked
    await asyncio.sleep(0.01)
    assert any("STRAY_FILL_AFTER_CLOSE" in n for n in h.notes) and pos2.filled_a == 20.0   # books untouched, human alerted
