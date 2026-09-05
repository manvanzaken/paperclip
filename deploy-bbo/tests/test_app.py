import asyncio
import json
from collections import Counter
from dataclasses import replace

import pytest

from bbo_trader.app import App, VenueMissing
from bbo_trader.main import legacy_bot_running
from bbo_trader.models import OPEN, CLOSED, MAKER_RESTING
from bbo_trader.positions import StateStore, StateCorrupt
from bbo_trader.strategy import PairEvaluator
from tests.conftest import mk_bbo
from tests.test_execution_tt import Harness, SYM


async def settle(n=8):
    for _ in range(n):
        await asyncio.sleep(0.005)


def build_app(tmp_path, **over) -> App:
    h = Harness(tmp_path, **over)
    fees = {n: v.fees for n, v in h.venues.items()}
    ev = PairEvaluator(h.cfg, h.board, fees, {}, {}, h.risk, h.metrics.funnel)
    app = App(h.cfg, h.venues, h.board, h.book, h.risk, h.metrics, h.ex, ev, StateStore(tmp_path / "real_state.json"))
    for v in h.venues.values():
        v.private.set_handler(h.ex.on_order_event)
    app.apply_universe()                      # specs already on the Venue bundles → universe = {SYM}
    app.harness = h
    return app


def bbo(venue, bid, ask, cs, **kw):
    return mk_bbo(venue, SYM, bid, ask, contract_size=cs, **kw)


async def test_quote_to_tt_position_to_exit_and_state_file(tmp_path):
    app = build_app(tmp_path)
    assert app.universe == {SYM: ["blofin", "mexc"]}
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0))
    app.on_bbo(bbo("blofin", 1.0050, 1.0060, 1.0))          # TT edge appears → entry task
    assert SYM in app._pending_entries
    await settle()
    pos = app.book.open[0]
    assert pos.status == OPEN and pos.mode == "TT" and not app._pending_entries
    app.on_bbo(bbo("blofin", 1.0010, 1.0012, 1.0))          # converged → exit task
    app.on_bbo(bbo("mexc", 1.0009, 1.0011, 10.0))
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "convergence" and app.book.total_trades == 1
    await app.sweep_once()
    state = json.loads((tmp_path / "real_state.json").read_text())
    assert state["closed_positions"][0]["exit_reason"] == "convergence" and state["bbo"]["mode"] == "paper"
    assert state["spread_scanner"][0]["symbol"] == SYM and (tmp_path / "heartbeat_paper").exists()
    assert state["equity"] == pytest.approx(200.0 + pos.net_pnl_usd)


async def test_quote_to_tm_position_and_halt_cancels_resting(tmp_path):
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0010, 10.0))
    app.on_bbo(bbo("blofin", 1.0041, 1.0061, 1.0))          # TM edge on blofin
    await settle()
    pos = app.book.open[0]
    assert pos.status == MAKER_RESTING and pos.maker_venue == "blofin" and pos.maker_rest_price == pytest.approx(1.0061)
    (tmp_path / "stop.flag").write_text("")
    await app.sweep_once()                                   # halt → cancel resting
    await settle()
    assert app.risk.halted and app.book.open == [] and app.metrics.funnel["maker_cancelled"] == 1
    app.on_bbo(bbo("blofin", 1.0041, 1.0062, 1.0))          # no new entries while halted
    assert not app._pending_entries and app.metrics.funnel["halted"] >= 1
    (tmp_path / "start.flag").write_text("")
    await app.sweep_once()
    assert not app.risk.halted


async def test_tm_fill_through_app_and_close_all(tmp_path):
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0010, 10.0))
    app.on_bbo(bbo("blofin", 1.0041, 1.0061, 1.0))
    await settle()
    pos = app.book.open[0]
    await asyncio.sleep(0.005)
    app.on_bbo(bbo("blofin", 1.0061, 1.0062, 1.0, bq=100.0))   # our resting ask is lifted → hedge → OPEN
    await settle()
    assert pos.status == OPEN and pos.mode == "TM" and pos.fee_liquidity["maker"] == "maker"
    await app.handle_command("/close_all")
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "halt" and app.risk.halted


def test_legacy_guard(tmp_path):
    hb = tmp_path / "heartbeat_live"
    assert not legacy_bot_running(hb, 120.0, now=1000.0)
    hb.write_text("1")
    import os
    os.utime(hb, (1000.0, 1000.0))
    assert legacy_bot_running(hb, 120.0, now=1050.0)
    assert not legacy_bot_running(hb, 120.0, now=1200.0)


async def test_live_refuses_corrupt_state_and_paper_falls_back_to_backup(tmp_path):
    (tmp_path / "real_state.json").write_text("{not json")
    app = build_app(tmp_path)                                  # paper: no backup -> fresh start, no exception
    app.load_state()
    assert app.book.open == []
    live = build_app(tmp_path, mode="live")
    with pytest.raises(StateCorrupt):
        live.load_state()


async def test_venue_missing_check_and_drive_guard(tmp_path, caplog):
    app = build_app(tmp_path)
    pos = app.book.new(SYM, "gate", "mexc", OPEN, "TT", size_usd=25.0, entry_time=app.clock())
    app._check_position_venues()                                   # paper: booked closed, not counted as a trade
    assert pos.status == CLOSED and pos.exit_reason == "venue_removed" and app.book.total_trades == 0
    app.book.new(SYM, "gate", "mexc", OPEN, "TT", size_usd=25.0, entry_time=app.clock())
    app.cfg = replace(app.cfg, mode="live")
    with pytest.raises(VenueMissing):
        app._check_position_venues()                               # live: refuse to start, the venue holds it
    app.cfg = replace(app.cfg, mode="paper")
    app._check_position_venues()
    app.book.new(SYM, "blofin", "mexc", OPEN, "TT", size_usd=25.0, entry_time=app.clock())
    app.evaluator.evaluate_exit = lambda *a, **k: 1 / 0            # one broken position...
    await app.sweep_once()                                         # ...stops neither the sweep nor the heartbeat
    assert (tmp_path / "heartbeat_paper").exists() and "DRIVE_ERROR" in caplog.text