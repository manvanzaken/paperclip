import asyncio

import pytest

from bbo_trader.quotes import QuoteBoard
from bbo_trader.venues.sim import SimVenue
from tests.conftest import make_cfg, mk_bbo, mk_spec, BLOFIN_FEES

SYM = "XYZUSDT"


def build(tmp_path, clock):
    cfg = make_cfg(tmp_path, sim_latency_ms=1, sim_taker_slip_bps=2.0, maker_top_level_frac=0.5)
    board = QuoteBoard(cfg.stale_quote_s)
    sim = SimVenue("blofin", cfg, BLOFIN_FEES, board, {SYM: mk_spec("blofin", SYM, contract_size=10.0)}, clock=clock)
    events = []
    sim.set_handler(events.append)
    return cfg, board, sim, events


async def test_market_order_fills_at_touch_with_slip_and_taker_fee(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0000, 1.0010, ts=clock()))
    ack = await sim.place_market(SYM, "buy", 2.0, False, "c1")
    assert ack.ok and ack.order_id == "sim-1"
    await asyncio.sleep(0.02)
    ev = events[-1]
    assert ev.state == "filled" and ev.client_id == "c1" and ev.liquidity == "taker"
    assert ev.avg_price == pytest.approx(1.0010 * 1.0002) and ev.filled_qty == 2.0
    assert ev.fee == pytest.approx(2.0 * ev.avg_price * 10.0 * 0.06 / 100)
    assert (await sim.positions())[0].side == "long" and (await sim.positions())[0].qty == 2.0
    assert (await sim.balance())["available"] == pytest.approx(100.0 - ev.fee)
    assert (await sim.place_market("NOPEUSDT", "buy", 1.0, False, "c2")).ok is False   # no quote


async def test_post_only_rests_then_fills_when_touch_crosses(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0061, ts=clock()))
    ack = await sim.place_post_only(SYM, "sell", 4.0, 1.0061, False, "m1")
    assert ack.ok and events[-1].state == "ack"           # ack event pushed before the return value
    sim.on_quote(mk_bbo("blofin", SYM, 1.0061, 1.0062, bq=4.0, ts=clock()))   # not live yet (latency)
    assert events[-1].state == "ack"
    clock.tick(0.01)
    sim.on_quote(mk_bbo("blofin", SYM, 1.0061, 1.0062, bq=4.0, ts=clock()))   # crosses: 50% of 4 -> 2
    assert events[-1].state == "partial" and events[-1].filled_qty == 2.0 and events[-1].liquidity == "maker"
    assert events[-1].fee == pytest.approx(2.0 * 1.0061 * 10.0 * 0.02 / 100)
    sim.on_quote(mk_bbo("blofin", SYM, 1.0040, 1.0060, ts=clock()))            # no cross -> nothing
    assert events[-1].state == "partial"
    sim.on_quote(mk_bbo("blofin", SYM, 1.0065, 1.0066, bq=1000.0, ts=clock()))
    assert events[-1].state == "filled" and events[-1].filled_qty == 4.0 and events[-1].avg_price == pytest.approx(1.0061)
    assert await sim.open_orders() == []
    assert not (await sim.cancel(SYM, "m1", ack.order_id)).ok                  # already terminal
    assert (await sim.positions())[0].side == "short"


async def test_post_only_would_cross_is_rejected(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0061, ts=clock()))
    ack = await sim.place_post_only(SYM, "sell", 1.0, 1.0041, False, "m1")
    assert not ack.ok and "cross" in ack.error and events[-1].state == "rejected"
    ack2 = await sim.place_post_only(SYM, "buy", 1.0, 1.0061, False, "m2")
    assert not ack2.ok and events[-1].state == "rejected" and events[-1].client_id == "m2"


async def test_cancel_amend_query(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0061, ts=clock()))
    ack = await sim.place_post_only(SYM, "buy", 3.0, 1.0040, False, "m1")
    assert ack.ok and len(await sim.open_orders()) == 1
    assert (await sim.amend(SYM, "m1", ack.order_id, 1.0041)).ok
    assert not (await sim.amend(SYM, "m1", ack.order_id, 1.0061)).ok             # would cross
    q = await sim.query_order(SYM, "m1", ack.order_id)
    assert q.state == "ack" and q.filled_qty == 0.0
    assert (await sim.cancel(SYM, "m1", ack.order_id)).ok
    assert events[-1].state == "canceled" and events[-1].filled_qty == 0.0
    assert await sim.open_orders() == [] and await sim.query_order(SYM, "zzz", "") is None


async def test_taker_fills_at_the_touch_that_exists_after_the_latency(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0000, 1.0010, ts=clock()))
    ack = await sim.place_market(SYM, "buy", 2.0, False, "c1")
    assert ack.ok and events[-1].state == "ack"                         # live venues push new/ack first
    board.set(mk_bbo("blofin", SYM, 1.0100, 1.0110, ts=clock()))        # the market gaps during the latency
    await asyncio.sleep(0.02)
    assert events[-1].state == "filled" and events[-1].avg_price == pytest.approx(1.0110 * 1.0002)   # spread decay is real
    clock.tick(5)                                                        # board stale -> no fills from fantasy prices
    assert (await sim.place_market(SYM, "sell", 1.0, False, "c2")).error == "no quote"
    assert (await sim.place_post_only(SYM, "sell", 1.0, 1.02, False, "m9")).error == "no quote"


async def test_maker_cap_is_per_distinct_touch_and_sub_lot_touch_fills_nothing(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0041, 1.0061, ts=clock()))
    await sim.place_post_only(SYM, "sell", 100.0, 1.0061, False, "m1")
    clock.tick(0.01)
    for _ in range(10):                                                  # an unchanged book re-pushed 10x is not new flow
        sim.on_quote(mk_bbo("blofin", SYM, 1.0061, 1.0062, bq=20.0, ts=clock()))
    assert events[-1].state == "partial" and events[-1].filled_qty == 10.0
    sim.on_quote(mk_bbo("blofin", SYM, 1.0061, 1.0062, bq=0.5, ts=clock()))      # sub-lot touch: nothing
    assert events[-1].filled_qty == 10.0
    sim.on_quote(mk_bbo("blofin", SYM, 1.0061, 1.0062, bq=30.0, ts=clock()))     # a new touch: 15 more
    assert events[-1].filled_qty == 25.0
    sim.on_quote(mk_bbo("mexc", SYM, 1.0070, 1.0071, bq=1000.0, ts=clock()))     # another venue's quote is ignored
    assert events[-1].filled_qty == 25.0
    assert sim._resting[SYM] and len(sim._orders) == 1
    await sim.cancel(SYM, "m1", "sim-1")
    assert sim._resting.get(SYM) == {} and await sim.open_orders() == []


async def test_reduce_only_is_clamped_and_rejected_when_flat(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0000, 1.0010, ts=clock()))
    await sim.place_market(SYM, "buy", 5.0, False, "c1")
    await asyncio.sleep(0.02)
    await sim.place_market(SYM, "sell", 8.0, True, "c2")                # reduce-only 8 against a long 5
    await asyncio.sleep(0.02)
    assert events[-1].state == "filled" and events[-1].filled_qty == 5.0 and await sim.positions() == []
    await sim.place_market(SYM, "sell", 3.0, True, "c3")                # nothing left to reduce
    await asyncio.sleep(0.02)
    assert events[-1].state == "rejected" and events[-1].error == "nothing to reduce" and await sim.positions() == []


async def test_realized_pnl_and_fees_flow_into_balance(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0000, 1.0010, ts=clock()))
    await sim.place_market(SYM, "buy", 2.0, False, "c1")                # 2 x 10 contracts at 1.0010*(1+2bps)
    await asyncio.sleep(0.02)
    entry = events[-1].avg_price
    board.set(mk_bbo("blofin", SYM, 2.0000, 2.0010, ts=clock()))
    await sim.place_market(SYM, "sell", 2.0, True, "c2")
    await asyncio.sleep(0.02)
    exit_px = events[-1].avg_price
    fees = sum(e.fee for e in events if e.state == "filled")
    assert (await sim.balance())["available"] == pytest.approx(100.0 + (exit_px - entry) * 2.0 * 10.0 - fees)


async def test_cancel_inside_latency_prevents_the_fill_and_amend_resets_the_clock(tmp_path, clock):
    cfg, board, sim, events = build(tmp_path, clock)
    board.set(mk_bbo("blofin", SYM, 1.0000, 1.0010, ts=clock()))
    ack = await sim.place_market(SYM, "buy", 2.0, False, "c1")
    assert (await sim.cancel(SYM, "c1", ack.order_id)).ok
    await asyncio.sleep(0.02)
    assert events[-1].state == "canceled" and await sim.positions() == []
    assert (await sim.cancel(SYM, "nope", "x")).error == "unknown order"
    assert not (await sim.place_post_only(SYM, "sell", 0.0, 1.0010, False, "m0")).ok       # qty guard
    ack = await sim.place_post_only(SYM, "sell", 4.0, 1.0012, False, "m1")
    clock.tick(0.01)
    assert (await sim.amend(SYM, "m1", ack.order_id, 1.0011)).ok
    sim.on_quote(mk_bbo("blofin", SYM, 1.0011, 1.0013, bq=100.0, ts=clock()))              # inside the new latency window
    assert events[-1].state == "ack"
    clock.tick(0.01)
    sim.on_quote(mk_bbo("blofin", SYM, 1.0011, 1.0013, bq=100.0, ts=clock()))
    assert events[-1].state == "filled" and events[-1].avg_price == 1.0011
