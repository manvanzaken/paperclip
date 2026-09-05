import asyncio
import json
from collections import Counter
from dataclasses import replace

import pytest

from bbo_trader.app import App, VenueMissing
from bbo_trader.main import legacy_bot_running
from bbo_trader.models import OPEN, CLOSED, MAKER_RESTING, EXIT_MAKER_RESTING, Intent
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
    assert state["spread_scanner"][0]["symbol"] == SYM and (tmp_path / "bbo_heartbeat_paper").exists()
    assert app._market_data_interval() == 3600.0 and state["bbo"]["connected"] == {}
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


def test_legacy_guard(tmp_path, monkeypatch):
    hb = tmp_path / "heartbeat_live"
    assert not legacy_bot_running(hb, 120.0, now=1000.0)
    hb.write_text("1")
    import os
    os.utime(hb, (1000.0, 1000.0))
    assert legacy_bot_running(hb, 120.0, now=1050.0)
    assert not legacy_bot_running(hb, 120.0, now=1200.0)
    import pathlib

    def denied(self, *a, **k):
        raise PermissionError("denied")
    monkeypatch.setattr(pathlib.Path, "stat", denied)
    assert legacy_bot_running(hb, 120.0, now=1200.0)               # unreadable: assume the legacy bot runs, a human decides


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
    assert (tmp_path / "bbo_heartbeat_paper").exists() and "DRIVE_ERROR" in caplog.text


async def test_quotes_from_the_future_are_refused(tmp_path, caplog):
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0, ts=app.clock() + 5.0))      # a receive time that would never go stale
    assert app.board.get("mexc", SYM) is None and "QUOTE_TS_SKEW" in caplog.text
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0, ts=app.clock() + 0.5))      # within the tolerance: accepted
    assert app.board.get("mexc", SYM) is not None


def _slow(sim, delay=0.05):
    real_pm = sim.place_market

    async def slow_pm(*a, **k):
        await asyncio.sleep(delay)
        return await real_pm(*a, **k)
    sim.place_market = slow_pm


async def test_shutdown_drains_inflight_entries_before_saving(tmp_path):
    app = build_app(tmp_path)
    _slow(app.harness.sim("mexc"))
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0))
    app.on_bbo(bbo("blofin", 1.0050, 1.0060, 1.0))
    assert SYM in app._pending_entries                              # the entry is in flight
    app.running = False
    await app.shutdown()                                            # drains it, then saves
    pos = app.book.open[0]
    assert pos.status == OPEN and pos.filled_a == 20.0 and pos.filled_b == 2.0 and not app._pending_entries
    state = json.loads((tmp_path / "real_state.json").read_text())
    assert state["open_positions"][0]["status"] == OPEN and state["open_positions"][0]["filled_a"] == 20.0
    app.on_bbo(bbo("blofin", 1.0070, 1.0080, 1.0))                # no new entries once stopping
    assert not app._pending_entries


async def test_restored_transient_positions_are_adopted(tmp_path, monkeypatch):
    from bbo_trader import execution
    app = build_app(tmp_path)
    now = app.clock()
    legs = dict(size_usd=25.0, filled_a=20.0, filled_b=2.0, entry_price_a=1.005, entry_price_b=1.0008, entry_time=now)
    for st in ("TT_ENTERING", "HEDGING", "EXIT_HEDGING", "TT_EXITING"):
        app.book.new(SYM, "blofin", "mexc", st, "TT", **legs)
    app.book.new(SYM, "blofin", "mexc", MAKER_RESTING, "TM", size_usd=25.0, maker_venue="blofin", maker_client_id="gone-1",
                 entry_time=now)
    app.book.new(SYM, "blofin", "mexc", EXIT_MAKER_RESTING, "TT", maker_venue="blofin", maker_client_id="gone-2", **legs)
    await app.save_state(now)
    app2 = build_app(tmp_path)
    app2.load_state()
    assert sorted(p.status for p in app2.book.open) == ["DEGRADED"] * 4 + [OPEN]   # resting entry discarded, exit maker reopened
    reopened = [p for p in app2.book.open if p.status == OPEN][0]
    assert reopened.maker_client_id == "" and reopened.maker_venue == ""
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    app2.harness.quote("blofin", 1.0050, 1.0060)
    app2.harness.quote("mexc", 1.0000, 1.0008)
    await app2.sweep_once()                                         # retry_degraded owns them: the venues hold nothing -> booked flat
    await settle()
    assert not [p for p in app2.book.open if p.status == "DEGRADED"]


async def test_close_all_covers_inflight_entries(tmp_path):
    app = build_app(tmp_path)
    _slow(app.harness.sim("mexc"))
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0))
    app.on_bbo(bbo("blofin", 1.0050, 1.0060, 1.0))
    assert SYM in app._pending_entries
    await app.handle_command("/close_all")                          # drains the entry first, then closes it
    await settle()
    pos = app.book.closed[-1]
    assert pos.status == CLOSED and pos.exit_reason == "halt" and app.risk.halted and app.book.open == []
    assert await app.harness.sim("blofin").positions() == [] and await app.harness.sim("mexc").positions() == []


async def test_dying_task_is_reported_and_stops_the_process(tmp_path, caplog):
    app = build_app(tmp_path)

    async def boom():
        raise RuntimeError("feed died")
    t = app._supervise("public:test", asyncio.create_task(boom()))
    await asyncio.gather(t, return_exceptions=True)
    await asyncio.sleep(0.01)
    assert not app.running and "TASK_DIED public:test" in caplog.text


def test_data_dir_collision_with_the_legacy_bot_is_refused(tmp_path):
    from bbo_trader.main import data_dir_collides
    from tests.conftest import make_cfg
    legacy = tmp_path / "legacy"
    legacy.mkdir()
    assert not data_dir_collides(make_cfg(tmp_path / "own", legacy_heartbeat_path=legacy / "heartbeat_live"))
    assert data_dir_collides(make_cfg(legacy, legacy_heartbeat_path=legacy / "heartbeat_live"))


async def test_quote_fallback_refreshes_before_the_stale_boundary(tmp_path):
    import time
    app = build_app(tmp_path)
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    calls = []

    class FakeMarket:
        specs = {}

        async def fetch_bbo(self, symbol):
            calls.append(symbol)
            return mk_bbo("mexc", SYM, 1.0001, 1.0009, contract_size=10.0)
    app.venues["mexc"].market = FakeMarket()
    await app._quote_fallback()
    assert calls == []                                              # fresh leg: no REST call
    app.clock = lambda: time.time() + 1.2                           # the mexc leg is 1.2 s old: past half of the 2 s budget
    await app._quote_fallback()
    assert calls == [SYM] and app.board.get("mexc", SYM).bid == 1.0001   # refreshed before it could read stale


async def test_restored_maker_fills_are_booked_not_discarded(tmp_path, monkeypatch):
    """A save can land between a maker fill and its hedge (MAKER_RESTING with maker_filled_qty), during HEDGING before
    _finalize_maker booked the fill on the leg, or with an exit maker partly filled: none of these may be discarded,
    reopened as if unfilled, or closed as if nothing was held."""
    from bbo_trader import execution
    app = build_app(tmp_path)
    now = app.clock()
    app.book.new(SYM, "blofin", "mexc", MAKER_RESTING, "TM", size_usd=25.0, maker_venue="blofin", maker_side="sell",
                 maker_client_id="gone-1", maker_filled_qty=12.0, maker_avg_price=1.006, maker_fee_usd=0.002, entry_time=now)
    app.book.new(SYM, "blofin", "mexc", "HEDGING", "TM", size_usd=25.0, maker_venue="blofin", maker_side="sell",
                 maker_client_id="gone-2", maker_filled_qty=20.0, maker_avg_price=1.006, hedged_qty=20.0,
                 filled_b=2.0, entry_price_b=1.0008, entry_time=now)          # hedge leg booked, maker leg not yet
    app.book.new(SYM, "blofin", "mexc", EXIT_MAKER_RESTING, "TT", size_usd=25.0, maker_venue="blofin", maker_side="buy",
                 maker_client_id="gone-3", maker_filled_qty=5.0, maker_avg_price=1.001, filled_a=20.0, filled_b=2.0,
                 entry_price_a=1.005, entry_price_b=1.0008, entry_time=now)
    await app.save_state(now)
    app2 = build_app(tmp_path)
    app2.load_state()
    p1, p2, p3 = sorted(app2.book.open, key=lambda p: p.id)
    assert [p.status for p in (p1, p2, p3)] == ["DEGRADED"] * 3
    assert p1.filled_a == 12.0 and p1.entry_price_a == pytest.approx(1.006) and p1.entry_fees_usd == pytest.approx(0.002)
    assert p2.filled_a == 20.0 and p2.filled_b == 2.0                    # the maker leg is on the books before retry_degraded looks
    assert p3.exit_filled_a == 5.0 and p3.filled_a == 20.0               # the exit fill reduced leg a: 15 remain to close
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    app2.harness.quote("blofin", 1.0050, 1.0060)
    app2.harness.quote("mexc", 1.0000, 1.0008)
    await app2.sweep_once()                                              # paper: the venues hold nothing -> booked flat
    await settle()
    assert app2.book.open == [] and app2.book.total_trades == 3


async def test_quote_fallback_abandons_a_hung_fetch_within_the_budget(tmp_path, caplog):
    import time
    app = build_app(tmp_path, stale_quote_s=0.4)                         # refresh at 0.2 s, and give the call 0.2 s
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    calls = []

    class HungMarket:
        specs = {}

        async def fetch_bbo(self, symbol):
            calls.append(symbol)
            await asyncio.sleep(10)
    app.venues["blofin"].market = HungMarket()
    app.clock = lambda: time.time() + 0.3                                # the blofin leg is 0.3 s old: past half the budget
    t0 = time.time()
    first = asyncio.create_task(app._quote_fallback())
    await asyncio.sleep(0.05)
    await app._quote_fallback()                                          # the next tick while the fetch is in flight: no duplicate
    await first
    assert time.time() - t0 < 0.5 and calls == [SYM]                    # abandoned at the bound, not at the adapter's 5 s
    assert app.metrics.funnel["fallback_timeout"] == 1 and "QUOTE_FALLBACK_TIMEOUT" in caplog.text
    assert not app._fallback_inflight


async def test_shutdown_settles_cancelled_makers_before_saving(tmp_path):
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0010, 10.0))
    app.on_bbo(bbo("blofin", 1.0041, 1.0061, 1.0))                       # TM entry rests on blofin
    await settle()
    assert app.book.open[0].status == MAKER_RESTING
    app.running = False
    await app.shutdown()                                                 # the cancel ack is an order event: wait for it, then save
    state = json.loads((tmp_path / "real_state.json").read_text())
    assert state["open_positions"] == [] and app.book.open == []         # not a MAKER_RESTING a restart would report as stuck


async def test_no_new_maker_orders_once_stopping(tmp_path):
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0008, 10.0))
    app.on_bbo(bbo("blofin", 1.0050, 1.0060, 1.0))
    await settle()
    pos = app.book.open[0]
    assert pos.status == OPEN
    app.running = False
    app.evaluator.evaluate_exit = lambda *a, **k: Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc",
                                                         maker_venue="blofin", rest_price=1.0050)
    app._drive(pos)
    await settle()
    assert pos.status == OPEN                                            # no new resting order while shutting down...
    app.evaluator.evaluate_exit = lambda *a, **k: Intent("TT_EXIT", reason="time_stop", symbol=SYM, venue_a="blofin", venue_b="mexc")
    app._drive(pos)
    await settle()
    assert pos.status == CLOSED                                          # ...but a taker exit still goes through


async def test_hung_notify_does_not_hold_the_drain(tmp_path, caplog):
    import time
    app = build_app(tmp_path)

    async def hung(text):
        await asyncio.sleep(10)
    app.executor.notify = hung
    app.executor._say("hello")
    await asyncio.sleep(0)                                               # the send is now sitting in its 10 s sleep
    t0 = time.time()
    await app._drain(2.0)
    assert time.time() - t0 < 0.5 and "DRAIN_TIMEOUT" not in caplog.text
    sends = list(app.executor._notify_tasks)
    assert len(sends) == 1                                               # its own set: the drain waits for order tasks only
    for t in sends:
        t.cancel()
    await asyncio.gather(*sends, return_exceptions=True)


async def test_market_data_refresh_keeps_held_specs_and_retries_fast_when_empty(tmp_path, caplog):
    from tests.conftest import mk_spec
    app = build_app(tmp_path)
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    other = mk_spec("blofin", "OTHERUSDT")

    class DelistingMarket:
        specs = {}

        async def fetch_specs(self):
            return {"OTHERUSDT": other}                                  # SYM delisted while we hold it

        async def fetch_volumes(self):
            return {"OTHERUSDT": 1e6}

    class Public:
        connected = True
        symbols = None

        def set_specs(self, specs):
            pass

        def set_symbols(self, syms):
            self.symbols = list(syms)
    v = app.venues["blofin"]
    v.market, v.public = DelistingMarket(), Public()
    await app.refresh_market_data()
    assert SYM in v.specs and "OTHERUSDT" in v.specs and SYM in v.public.symbols   # kept its spec, stays subscribed
    assert app.universe == {SYM: ["blofin", "mexc"]} and app._market_data_interval() == 3600.0
    app.venues["mexc"].specs.clear()                                     # nothing on two trade venues any more
    await app.refresh_market_data()
    assert app.universe == {} and app._market_data_interval() == 60.0 and "UNIVERSE_EMPTY" in caplog.text


async def test_stale_cancel_puts_the_symbol_on_a_short_cooldown(tmp_path):
    import time
    app = build_app(tmp_path)
    app.on_bbo(bbo("mexc", 1.0000, 1.0010, 10.0))
    app.on_bbo(bbo("blofin", 1.0041, 1.0061, 1.0))
    await settle()
    pos = app.book.open[0]
    assert pos.status == MAKER_RESTING
    app.evaluator.clock = lambda: time.time() + 3.0                      # every leg reads stale to the strategy
    app._drive(pos)
    await settle()
    assert app.book.open == [] and app.metrics.funnel["maker_cancelled"] == 1
    assert 4.0 < app.risk.cooldowns[SYM] - time.time() <= 5.0            # STALE_CANCEL_COOLDOWN_S, not the 60 s entry-failure one
