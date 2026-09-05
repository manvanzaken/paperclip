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
    audit = state["order_audit_log"][0]                              # the legacy dashboard's audit columns are filled
    assert audit["symbol"] == SYM and audit["exchange"] == audit["venue"] and audit["size"] == audit["qty"]
    assert audit["order_id"] == audit["client_id"] and audit["success"] is True and audit["action"] == "market entry_a"
    assert audit["timestamp"].startswith("20") and "T" in audit["timestamp"]


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
    app2._adopt_transients()                                        # run() does this after the first market-data refresh
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
    app._last_rest["mexc"] = app.clock()                            # connection already warm
    await app._quote_fallback()
    await settle()
    assert calls == []                                              # fresh leg: no REST call
    app.clock = lambda: time.time() + 1.2                           # the mexc leg is 1.2 s old: past half of the 2 s budget
    await app._quote_fallback()
    await settle()
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
    app2._adopt_transients()
    p1, p2, p3 = sorted(app2.book.open, key=lambda p: p.id)
    assert [p.status for p in (p1, p2, p3)] == ["DEGRADED"] * 3
    assert p1.filled_a == 12.0 and p1.entry_price_a == pytest.approx(1.006) and p1.entry_fees_usd == pytest.approx(0.002)
    assert p1.size_usd == pytest.approx(12.0 * 1.006)                    # sized by what it really holds (specs known by then)
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
    app = build_app(tmp_path, stale_quote_s=0.4)                         # refresh at 0.2 s, give the call the full 0.4 s
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
    await app._quote_fallback()                                          # spawns the fetch; the tick itself never blocks
    assert time.time() - t0 < 0.05 and len(app._fallback_tasks) == 1
    await asyncio.sleep(0.05)
    await app._quote_fallback()                                          # the next tick while the fetch is in flight: no duplicate
    await asyncio.gather(*app._fallback_tasks)
    assert time.time() - t0 < 0.7 and calls == [SYM]                    # abandoned at the bound, not at the adapter's 5 s
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


async def test_quote_fallback_keeps_one_warm_rest_connection_per_venue(tmp_path):
    import time
    from tests.conftest import mk_spec
    app = build_app(tmp_path)
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    other = "ABCUSDT"
    for name, cs in (("blofin", 1.0), ("mexc", 10.0)):
        app.venues[name].specs[other] = mk_spec(name, other, contract_size=cs)
    app.book.new(other, "blofin", "mexc", OPEN, "TT", size_usd=25.0, filled_a=20.0, filled_b=2.0, entry_time=app.clock())
    seen, inflight, peak = [], [0], [0]

    class SlowMarket:
        specs = {}

        async def fetch_bbo(self, symbol):
            inflight[0] += 1
            peak[0] = max(peak[0], inflight[0])
            await asyncio.sleep(0.02)
            inflight[0] -= 1
            seen.append(symbol)
            return mk_bbo("blofin", symbol, 1.0050, 1.0060, ts=app.clock())
    app.venues["blofin"].market = SlowMarket()
    for sym in (SYM, other):
        app.board.set(mk_bbo("blofin", sym, 1.0050, 1.0060, ts=app.clock()))      # both blofin legs fresh...
    await app._quote_fallback()
    await settle()
    assert seen == [SYM]                                                        # ...one warm-up call for the venue anyway
    await app._quote_fallback()
    await settle()
    assert seen == [SYM]                                                        # warm within REST_WARM_S: nothing
    t1 = time.time() + 1.5                                                      # both legs read 1.5 s old: past half the budget
    app.clock = lambda: t1
    await app._quote_fallback()
    await asyncio.gather(*app._fallback_tasks)
    assert sorted(seen[1:]) == [other, SYM] and peak[0] == 1                    # both refreshed, one call at a time
    assert app.board.get("blofin", SYM).ts_local == t1 and app.metrics.funnel["fallback_ok"] == 3


async def _resting_tm(app):
    app.on_bbo(bbo("mexc", 1.0000, 1.0010, 10.0))
    app.on_bbo(bbo("blofin", 1.0041, 1.0061, 1.0))
    await settle()
    pos = app.book.open[0]
    assert pos.status == MAKER_RESTING
    return pos


def _count_calls(obj, name):
    calls = []
    orig = getattr(obj, name)

    async def wrapped(*a, **k):
        calls.append(a)
        return await orig(*a, **k)
    setattr(obj, name, wrapped)
    return calls


async def test_shutdown_does_not_repost_a_requoted_maker(tmp_path):
    """A requote sends the cancel and returns; its `canceled` event lands during shutdown and finalize would post the
    new price — a fresh order resting at the venue after the sweep that was supposed to clear it."""
    app = build_app(tmp_path)
    pos = await _resting_tm(app)
    app.harness.sim("blofin").supports_amend = False                            # the cancel + new-order requote path
    posts = _count_calls(app.harness.sim("blofin"), "place_post_only")
    app.running = False
    app._spawn(app.executor.requote(pos, 1.0063))
    await app.shutdown()
    assert posts == [] and app.book.open == []                                  # discarded, nothing re-posted
    state = json.loads((tmp_path / "real_state.json").read_text())
    assert state["open_positions"] == [] and app.harness.sim("blofin")._resting.get(SYM, {}) == {}


async def test_shutdown_drops_a_pending_tt_upgrade(tmp_path):
    app = build_app(tmp_path)
    pos = await _resting_tm(app)
    markets = _count_calls(app.harness.sim("blofin"), "place_market") + _count_calls(app.harness.sim("mexc"), "place_market")
    app.running = False
    app._spawn(app.executor.upgrade_to_tt(pos))
    await app.shutdown()
    assert markets == [] and app.book.open == [] and app.book.closed == []      # no new position opens on the way down


async def test_shutdown_flushes_pending_alerts(tmp_path):
    app = build_app(tmp_path)
    delivered = []

    async def slow_notify(text):
        await asyncio.sleep(0.1)
        delivered.append(text)
    app.executor.notify = slow_notify
    app.running = False
    app.executor._say("DEGRADED #1 XYZUSDT: check the venue by hand")
    await app.shutdown()
    assert delivered == ["DEGRADED #1 XYZUSDT: check the venue by hand"]         # the alert survives the process exit


# ---- final-review round --------------------------------------------------------------------------------------

async def test_restored_exit_maker_on_a_removed_venue_is_closed_in_paper(tmp_path):
    app = build_app(tmp_path)
    app.book.new(SYM, "gate", "mexc", EXIT_MAKER_RESTING, "TT", size_usd=25.0, maker_venue="gate", entry_time=app.clock())
    app._check_position_venues()                       # EXIT_MAKER_RESTING -> CLOSED must be legal, or this crash-loops under systemd
    assert app.book.open == [] and app.book.closed[-1].exit_reason == "venue_removed"


async def _stuck_hedging(app):
    """A partial maker fill hedged, the remainder cancelled — but the venue's `canceled` event never arrives."""
    sim = app.harness.sim("blofin")
    orig, dropped = sim._handler, []

    def lossy(ev):
        if ev.state == "canceled":
            dropped.append(ev)
            return
        orig(ev)
    sim._handler = lossy
    pos = await _resting_tm(app)
    await asyncio.sleep(0.005)
    app.on_bbo(bbo("blofin", 1.0061, 1.0062, 1.0, bq=20.0))        # 50 % of 20 -> 10 of the 20 contracts fill
    await settle()
    assert pos.status == "HEDGING" and pos.maker_cancel_sent and dropped and pos.hedged_qty >= 9.9   # residual waits for the terminal
    return pos


async def test_hedging_without_a_terminal_event_is_rescued_by_the_sweep(tmp_path, caplog):
    import time
    app = build_app(tmp_path)
    pos = await _stuck_hedging(app)
    await app.executor.retry_degraded()                             # too early: nothing to do yet
    assert pos.status == "HEDGING"
    app.executor.clock = lambda: time.time() + 3.0                  # past HEDGING_SWEEP_AFTER_S
    await app.executor.retry_degraded()                             # the sweep asks the venue and feeds the terminal state in
    await settle()
    assert pos.status == OPEN and pos.filled_a == 10.0 and pos.filled_b == 1.0 and "pulled canceled" in caplog.text


async def test_hedging_on_a_silent_venue_degrades_after_stuck_s(tmp_path, caplog, monkeypatch):
    import time
    from bbo_trader import execution
    app = build_app(tmp_path)
    pos = await _stuck_hedging(app)
    sim = app.harness.sim("blofin")

    async def silent(*a, **k):
        return None
    sim.query_order = silent
    app.executor.clock = lambda: time.time() + 61.0                 # past HEDGING_STUCK_S
    await app.executor.retry_degraded()
    await settle()
    assert pos.status == "DEGRADED" and "HEDGING_STUCK" in caplog.text and pos.filled_a == 10.0
    assert any("DEGRADED #1" in n for n in app.harness.notes)
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    app.harness.quote("blofin", 1.0061, 1.0062)
    app.harness.quote("mexc", 1.0000, 1.0010)
    await app.executor.retry_degraded()                             # retry_degraded now owns it: both legs closed
    await settle()
    assert pos.status == CLOSED and await sim.positions() == [] and await app.harness.sim("mexc").positions() == []


async def test_naked_exposure_alert_fires_once(tmp_path, caplog):
    app = build_app(tmp_path)
    now = app.clock()
    pos = app.book.new(SYM, "blofin", "mexc", "HEDGING", "TM", size_usd=25.0, maker_venue="blofin", maker_side="sell",
                       maker_client_id="m-1", maker_filled_qty=10.0, hedged_qty=0.0, maker_fill_ts=now - 2.0, entry_time=now)
    await app.executor.retry_degraded()
    await asyncio.sleep(0.01)
    assert f"NAKED_EXPOSURE #{pos.id}" in caplog.text and app.metrics.funnel["naked_exposure"] == 1
    assert any("NAKED_EXPOSURE" in n for n in app.harness.notes)
    await app.executor.retry_degraded()
    assert app.metrics.funnel["naked_exposure"] == 1                 # once per naked window...
    pos.status, pos.maker_filled_qty, pos.hedged_qty, pos.maker_fill_ts = "EXIT_HEDGING", 5.0, 0.0, now - 2.0
    await app.executor.retry_degraded()
    assert app.metrics.funnel["naked_exposure"] == 2                 # ...the exit phase is its own window
    dust = app.book.new(SYM, "blofin", "mexc", "HEDGING", "TM", size_usd=25.0, maker_venue="blofin", maker_side="sell",
                        maker_client_id="m-2", maker_filled_qty=10.02, hedged_qty=10.0, maker_fill_ts=now - 2.0, entry_time=now)
    await app.executor.retry_degraded()
    assert app.metrics.funnel["naked_exposure"] == 2                 # a residual inside the mismatch tolerance is not naked exposure


async def test_fee_mismatch_is_logged_and_alerted_once_per_venue_per_hour(tmp_path, caplog):
    from dataclasses import replace as dc_replace
    from tests.conftest import MEXC_FEES
    app = build_app(tmp_path)
    h = app.harness
    app.venues["mexc"].fees = dc_replace(MEXC_FEES, taker=0.05)       # we believe 0.05 %; the venue charges 0.02 %
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    await asyncio.sleep(0.01)
    assert "FEE_MISMATCH mexc" in caplog.text and "FEE_MISMATCH blofin" not in caplog.text
    assert app.metrics.funnel["fee_mismatch"] == 1 and sum("FEE_MISMATCH" in n for n in h.notes) == 1
    await h.ex.exit_tt(pos, "test")                                  # the exit fill mismatches too: counted, not re-alerted
    await asyncio.sleep(0.01)
    assert app.metrics.funnel["fee_mismatch"] == 2 and sum("FEE_MISMATCH" in n for n in h.notes) == 1


async def test_fill_quality_abort_covers_tt_and_tm_entries(tmp_path, caplog):
    app = build_app(tmp_path, min_fill_spread_pct=0.50)             # every realized entry spread below 0.50 % is refused
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    assert await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0)) is None
    await settle()
    tt = app.book.closed[-1]
    assert tt.exit_reason == "fill_quality_abort" and app.risk.cooldowns.get(SYM, 0) > app.clock()
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []
    (tmp_path / "tm").mkdir()
    app = build_app(tmp_path / "tm", min_fill_spread_pct=0.60)      # fresh books (the TT loss blacklisted the symbol); TM realizes 0.51 %
    h = app.harness
    pos = await _resting_tm(app)                                     # TM: the maker fills, the hedge lands, the spread is poor
    await asyncio.sleep(0.005)
    app.on_bbo(bbo("blofin", 1.0061, 1.0062, 1.0, bq=100.0))
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "fill_quality_abort" and "FILL_QUALITY_ABORT #%d %s TM" % (pos.id, SYM) in caplog.text
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []


async def test_tm_exit_reason_labels_slipped_exits(tmp_path):
    app = build_app(tmp_path)
    pos = app.book.new(SYM, "blofin", "mexc", OPEN, "TT", size_usd=25.0, exit_price_a=1.0010, exit_price_b=1.0000)
    assert app.executor._tm_exit_reason(pos) == "take_profit"        # realized 0.10 % <= 0.15 % target
    pos.exit_price_a = 1.0060                                         # realized 0.60 %: the hedge crossed a market that had moved
    assert app.executor._tm_exit_reason(pos) == "tm_exit_slipped" and app.metrics.funnel["tm_exit_slipped"] == 1


async def test_requote_is_not_double_fired_before_the_task_runs(tmp_path):
    import time
    app = build_app(tmp_path)
    pos = await _resting_tm(app)
    requotes = _count_calls(app.executor, "requote")
    t1 = time.time() + 1.5                                            # past blofin's 1 s min requote gap
    app.clock = app.evaluator.clock = lambda: t1
    app.on_bbo(bbo("blofin", 1.0043, 1.0065, 1.0))                   # the peg moved: one requote
    app.on_bbo(bbo("blofin", 1.0043, 1.0065, 1.0))                   # the same book again before that task ran
    await settle()
    assert len(requotes) == 1 and pos.maker_rest_price != 1.0061


async def test_halt_cancels_a_resting_entry_even_if_the_sweep_cancel_failed(tmp_path):
    app = build_app(tmp_path)
    pos = await _resting_tm(app)
    app.risk.halt("test")                                             # the halt edge's cancel_all_resting did not reach this order
    app._drive(pos)
    await settle()
    assert app.book.open == [] and app.metrics.funnel["maker_cancelled"] == 1


async def test_book_leg_flat_accumulates_a_partial_exit(tmp_path):
    app = build_app(tmp_path)
    h = app.harness
    h.quote("blofin", 1.0050, 1.0060)
    pos = app.book.new(SYM, "blofin", "mexc", OPEN, "TT", size_usd=25.0, filled_a=20.0, exit_filled_a=5.0, exit_price_a=1.0010,
                       entry_time=app.clock())
    app.executor._book_leg_flat(pos, "a", "blofin")
    assert pos.exit_filled_a == 20.0 and pos.exit_price_a == pytest.approx((1.0010 * 5 + 1.0055 * 15) / 20)   # mark = mid


async def test_stuck_hedging_sweep_feeds_partial_progress(tmp_path, caplog):
    """The feed is dead and the cancel was acked but never took effect: the order keeps filling. The sweep's venue query
    must feed that (non-terminal) state in so the new fill is hedged, not stranded when the position degrades."""
    import time
    app = build_app(tmp_path)
    sim = app.harness.sim("blofin")
    real_cancel = sim.cancel

    async def acked_but_ineffective(symbol, client_id, order_id):
        from bbo_trader.models import OrderAck
        return OrderAck(True, order_id)                              # the venue says yes and leaves the order live
    sim.cancel = acked_but_ineffective
    orig = sim._handler
    seen = []

    def dead_after_first_partial(ev):
        if seen:
            return                                                   # nothing reaches us any more
        if ev.state == "partial":
            seen.append(ev)
        orig(ev)
    sim._handler = dead_after_first_partial
    pos = await _resting_tm(app)
    await asyncio.sleep(0.005)
    app.on_bbo(bbo("blofin", 1.0061, 1.0062, 1.0, bq=10.0))        # first partial: 5 of 20 — under one $10 hedge lot
    await settle()
    assert pos.status == "HEDGING" and pos.maker_cancel_sent and pos.maker_filled_qty == 5.0 and pos.hedged_qty == 0.0
    await asyncio.sleep(0.005)
    app.on_bbo(bbo("blofin", 1.0061, 1.0062, 1.0, bq=20.0))        # the still-live order fills 10 more; we never hear it
    await settle()
    assert pos.maker_filled_qty == 5.0 and (await sim.positions())[0].qty == 15.0
    app.executor.clock = lambda: time.time() + 3.0
    await app.executor.retry_degraded()                             # the sweep pulls `partial filled 15` and feeds it in
    await settle()
    assert pos.maker_filled_qty == 15.0 and pos.hedged_qty >= 9.9 and "pulled partial (filled 15.0)" in caplog.text
    sim.cancel = real_cancel
    app.executor.clock = lambda: time.time() + 61.0
    await app.executor.retry_degraded()                             # still no event: DEGRADED with all 15 on the books
    await settle()
    assert pos.status == "DEGRADED" and pos.filled_a == 15.0

