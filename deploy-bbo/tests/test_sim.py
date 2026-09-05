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