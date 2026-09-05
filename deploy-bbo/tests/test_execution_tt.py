import asyncio

import pytest

from bbo_trader.budget import RateBudget
from bbo_trader.execution import Executor
from bbo_trader.metrics import Metrics
from bbo_trader.models import Intent, OPEN, CLOSED, MAKER_RESTING, HEDGING, OrderAck, OrderEvent
from bbo_trader.positions import PositionBook
from bbo_trader.quotes import QuoteBoard
from bbo_trader.risk import RiskManager
from bbo_trader.venues.base import Venue
from bbo_trader.venues.sim import SimVenue
from tests.conftest import make_cfg, mk_bbo, mk_spec, MEXC_FEES, BLOFIN_FEES

SYM = "XYZUSDT"


class Harness:
    """Two SimVenues (blofin, mexc) wired to an Executor; real time, 1 ms sim latency, no-op sleeps."""

    def __init__(self, tmp_path, **over):
        self.cfg = make_cfg(tmp_path, sim_latency_ms=1, sim_taker_slip_bps=0.0, maker_top_level_frac=0.5, **over)
        self.board = QuoteBoard(self.cfg.stale_quote_s)
        self.book = PositionBook()
        self.risk = RiskManager(self.cfg)
        self.metrics = Metrics()
        self.venues = {}
        for name, fees, cs in (("blofin", BLOFIN_FEES, 1.0), ("mexc", MEXC_FEES, 10.0)):
            vc = self.cfg.venue(name)
            specs = {SYM: mk_spec(name, SYM, contract_size=cs)}
            sim = SimVenue(name, self.cfg, fees, self.board, specs)
            self.venues[name] = Venue(vc, fees, RateBudget(vc.rate_limits), trading=sim, private=sim, specs=specs)
        self.notes = []

        async def notify(text):
            self.notes.append(text)

        async def no_sleep(_s):
            await asyncio.sleep(0)

        self.ex = Executor(self.cfg, self.venues, self.board, self.book, self.risk, self.metrics,
                           notify=notify, sleep=no_sleep)
        for v in self.venues.values():
            v.private.set_handler(self.ex.on_order_event)

    def quote(self, venue, bid, ask, bq=1000.0, aq=1000.0):
        b = mk_bbo(venue, SYM, bid, ask, bq=bq, aq=aq, contract_size=self.venues[venue].specs[SYM].contract_size)
        self.board.set(b)
        self.venues[venue].trading.on_quote(b)
        return b

    def sim(self, venue) -> SimVenue:
        return self.venues[venue].trading


async def test_enter_tt_opens_matched_position(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    intent = Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42, edge_pct=0.06, ts=0.0)
    pos = await h.ex.enter_tt(intent)
    assert pos is not None and pos.status == OPEN and pos.mode == "TT"
    assert pos.filled_a == 20.0 and pos.filled_b == 2.0            # $20 matched: mexc contracts are $10 each
    assert pos.entry_price_a == pytest.approx(1.0050) and pos.entry_price_b == pytest.approx(1.0008)
    assert pos.size_usd == pytest.approx(min(20 * 1.0050, 2 * 1.0008 * 10))
    assert pos.entry_spread_pct == pytest.approx((1.0050 - 1.0008) / 1.0008 * 100)
    assert pos.entry_fees_usd == pytest.approx(20 * 1.0050 * 0.06 / 100 + 2 * 1.0008 * 10 * 0.02 / 100)
    assert pos.fee_liquidity == {"entry_a": "taker", "entry_b": "taker"}
    assert "entry_a" in pos.latency_ms and h.metrics.hists["submit_to_fill"].count == 2
    assert (await h.sim("blofin").positions())[0].side == "short" and (await h.sim("mexc").positions())[0].side == "long"
    await asyncio.sleep(0.01)                                    # notification task runs
    assert h.notes and h.notes[0].startswith("OPEN #1")


async def test_enter_tt_one_leg_fails_flattens_and_records(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, contract_size=10.0))   # board has mexc for sizing...
    h.sim("mexc").board = QuoteBoard(2.0)                                  # ...but the venue itself has no quote → reject
    intent = Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42, ts=0.0)
    assert await h.ex.enter_tt(intent) is None
    assert h.book.open == [] and len(h.book.closed) == 1
    closed = h.book.closed[0]
    assert closed.status == CLOSED and closed.exit_reason == "failed_entry"
    assert closed.exit_price_a > 0 and closed.exit_fees_usd > 0 and closed.net_pnl_usd < 0   # round-trip fees
    assert await h.sim("blofin").positions() == []                       # flat again
    assert h.risk.pair_strikes["XYZUSDT|blofin>mexc"]["n"] == 1 and "XYZUSDT" in h.risk.cooldowns


async def test_exit_tt_closes_and_books_pnl(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42))
    h.quote("blofin", 1.0010, 1.0012)          # converged: buy back at 1.0012
    h.quote("mexc", 1.0009, 1.0011)            # sell long at 1.0009
    await h.ex.exit_tt(pos, "convergence")
    assert pos.status == CLOSED and pos.exit_reason == "convergence" and pos.exit_mode == "TT"
    assert pos.exit_price_a == pytest.approx(1.0012) and pos.exit_price_b == pytest.approx(1.0009)
    assert pos.gross_pnl_usd == pytest.approx(((1.0050 - 1.0012) / 1.0050 + (1.0009 - 1.0008) / 1.0008) * pos.size_usd)
    assert pos.net_pnl_usd == pytest.approx(pos.gross_pnl_usd - pos.entry_fees_usd - pos.exit_fees_usd)
    assert h.book.total_trades == 1 and h.book.total_pnl_usd == pytest.approx(pos.net_pnl_usd)
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []
    assert h.risk.pair_stats["XYZUSDT|blofin>mexc"]["wins"] + h.risk.pair_stats["XYZUSDT|blofin>mexc"]["losses"] == 1


async def test_budget_exhaustion_rejects_entry_but_reserve_allows_close(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    bud = h.venues["blofin"].budget
    now = h.ex.clock()
    while bud.try_take("order", now):
        pass                                                     # burn the non-reserved blofin budget
    assert await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0)) is None
    assert h.metrics.funnel["budget"] == 1 and h.book.open == []  # blofin rejected → mexc leg flattened → failed_entry
    assert h.book.closed[0].exit_reason == "failed_entry"


async def test_retry_degraded_books_a_leg_the_venue_no_longer_holds(tmp_path, monkeypatch):
    from bbo_trader import execution
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42))
    h.sim("mexc")._pos.clear()                                     # the venue lost our long (accounting drift)
    h.sim("mexc")._avg.clear()
    await h.ex.exit_tt(pos, "test")                                # mexc refuses: nothing to reduce -> booked flat
    assert pos.status == CLOSED and pos.exit_filled_b == pos.filled_b and pos.exit_reason == "test"
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    monkeypatch.setattr(execution, "MAX_CLOSE_RETRIES", 1)         # a leg that can never close stops retrying
    h.quote("blofin", 1.0050, 1.0060)
    pos2 = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42))
    h.sim("mexc")._pos.clear()
    h.sim("mexc")._pos[SYM] = -1.0                                 # venue holds a SHORT where we book a long: not flat, not closable
    await h.ex.exit_tt(pos2, "test")
    assert pos2.status == "DEGRADED"
    await h.ex.retry_degraded()
    await h.ex.retry_degraded()
    await asyncio.sleep(0.01)                                      # notification task runs
    assert pos2.status == "DEGRADED" and pos2.close_retry_count == 2 and any("DEGRADED_STUCK" in n for n in h.notes)


async def settle(n=10):
    for _ in range(n):
        await asyncio.sleep(0.005)


async def test_partial_close_stays_degraded_until_the_remainder_is_closed(tmp_path, monkeypatch):
    from bbo_trader import execution
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0, spread_pct=0.42))
    sim = h.sim("blofin")
    real_pm = sim.place_market

    async def fills_four(symbol, side, qty, reduce_only, client_id):            # a thin book: 4 of the 20
        return await real_pm(symbol, side, min(qty, 4.0), reduce_only, client_id)
    sim.place_market = fills_four
    h.quote("blofin", 1.0010, 1.0012)
    h.quote("mexc", 1.0009, 1.0011)
    await h.ex.exit_tt(pos, "convergence")
    assert pos.status == "DEGRADED" and pos.degraded_leg == "a" and pos.exit_filled_a == 4.0
    assert (await sim.positions())[0].qty == 16.0                            # the remainder is still owned, not abandoned
    sim.place_market = real_pm
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and pos.exit_filled_a == 20.0 and await sim.positions() == []


async def test_flatten_leg_continues_after_partial_fills(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.board.set(mk_bbo("mexc", SYM, 1.0000, 1.0008, contract_size=10.0))
    h.sim("mexc").board = QuoteBoard(2.0)                                     # the mexc leg is rejected: no quote
    sim = h.sim("blofin")
    real_pm = sim.place_market

    async def partial(symbol, side, qty, reduce_only, client_id):
        return await real_pm(symbol, side, min(qty, 6.0) if reduce_only else qty, reduce_only, client_id)
    sim.place_market = partial
    assert await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0)) is None
    closed = h.book.closed[-1]
    assert closed.status == CLOSED and closed.exit_reason == "failed_entry" and closed.exit_filled_a == 20.0
    assert await sim.positions() == []                                        # the ladder flattened 6+6+6+2


async def test_venue_exception_on_one_leg_flattens_the_other(tmp_path, monkeypatch):
    from bbo_trader import execution
    monkeypatch.setattr(execution, "EVENT_GRACE_S", 0.01)
    monkeypatch.setattr(execution, "POLL_MAX_S", 0.05)
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)

    async def boom(*a, **k):
        raise RuntimeError("REST timeout")
    h.sim("mexc").place_market = boom                                         # in doubt: polled, then treated as rejected
    assert await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0)) is None
    await asyncio.sleep(0.01)
    assert h.book.open == [] and h.book.closed[-1].exit_reason == "failed_entry"
    assert await h.sim("blofin").positions() == [] and any("ORDER_UNRESOLVED" in n for n in h.notes)


async def test_hedge_failure_with_successful_flatten_closes_as_a_round_trip(tmp_path, caplog):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await asyncio.sleep(0.005)
    h.sim("mexc").board = QuoteBoard(2.0)                                     # the hedge venue cannot see a quote
    h.quote("blofin", 1.0061, 1.0062, bq=100.0)                               # full 20-contract maker fill
    await settle()
    assert pos.status == CLOSED and pos.exit_reason == "hedge_unwound"
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []
    assert pos.net_pnl_usd == pytest.approx(pos.pnl_adjust_usd - pos.entry_fees_usd - pos.exit_fees_usd)
    assert pos.entry_fees_usd > 0 and pos.exit_fees_usd > 0 and pos.pnl_adjust_usd < 0     # bought back one tick higher
    assert "EXEC_TASK_ERROR" not in caplog.text and h.book.open == []


async def test_exit_hedge_failure_closes_the_remainder_taker(tmp_path, caplog, monkeypatch):
    from bbo_trader import execution
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    h.quote("blofin", 1.0020, 1.0030)
    h.quote("mexc", 1.0000, 1.0005)
    assert await h.ex.exit_tm(pos, Intent("TM_EXIT", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                          rest_price=1.0015))
    await asyncio.sleep(0.005)
    real_board = h.sim("mexc").board
    h.sim("mexc").board = QuoteBoard(2.0)                                     # hedge venue unreachable
    h.quote("blofin", 1.0010, 1.0015, aq=100.0)                               # the exit maker fills: short leg closed
    await settle()
    assert pos.status == "DEGRADED" and pos.degraded_leg == "b" and pos.exit_filled_a == 20.0
    assert "FLATTEN" not in caplog.text and "HEDGE_FAILED" in caplog.text     # no futile re-opening order on blofin
    assert await h.sim("blofin").positions() == []
    h.sim("mexc").board = real_board
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and await h.sim("mexc").positions() == []


async def test_cancel_failure_during_hedge_is_retried_by_the_sweep(tmp_path, monkeypatch):
    from bbo_trader import execution
    monkeypatch.setattr(execution, "HEDGING_SWEEP_AFTER_S", 0.0)
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await asyncio.sleep(0.005)
    sim = h.sim("blofin")
    real_cancel, real_query = sim.cancel, sim.query_order

    async def bad_cancel(*a, **k):
        return OrderAck(False, error="boom")

    async def no_query(*a, **k):
        return None
    sim.cancel, sim.query_order = bad_cancel, no_query
    h.quote("blofin", 1.0061, 1.0062, bq=20.0)                                # partial fill of 10: hedged, cancel fails
    await settle()
    assert pos.status == HEDGING and pos.hedged_qty > 9.0 and len(await sim.open_orders()) == 1
    sim.cancel, sim.query_order = real_cancel, real_query
    await h.ex.retry_degraded()                                               # the sweep retries the cancel
    await settle()
    assert pos.status == OPEN and await sim.open_orders() == [] and pos.filled_a == 10.0


async def test_one_legged_unwind_books_the_traded_leg(tmp_path, monkeypatch):
    from bbo_trader import execution
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    await asyncio.sleep(0.005)
    h.quote("blofin", 1.0061, 1.0062, bq=100.0)
    h.sim("mexc").board = QuoteBoard(2.0)                                     # the hedge fails...
    real_pm = h.sim("blofin").place_market

    async def no_market(*a, **k):
        return OrderAck(False, error="venue down")
    h.sim("blofin").place_market = no_market                                  # ...and so does the flatten
    await settle()
    assert pos.status == "DEGRADED" and pos.filled_a == 20.0 and pos.size_usd == pytest.approx(20 * 1.0061)
    h.sim("blofin").place_market = real_pm
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and await h.sim("blofin").positions() == []
    assert pos.gross_pnl_usd == pytest.approx((1.0061 - pos.exit_price_a) / 1.0061 * pos.size_usd)   # not zeroed


async def test_stray_fill_on_a_superseded_maker_order_is_unwound(tmp_path, monkeypatch):
    from bbo_trader import execution
    h = Harness(tmp_path)
    h.quote("blofin", 1.0041, 1.0061)
    h.quote("mexc", 1.0000, 1.0010)
    sim = h.sim("blofin")
    sim.supports_amend = False                                                # MEXC-style cancel+new requote
    pos = await h.ex.enter_tm(Intent("TM_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", maker_venue="blofin",
                                     rest_price=1.0061, size_usd=25.0))
    old_cid = pos.maker_client_id
    await h.ex.requote(pos, 1.0063)
    await settle()
    assert pos.maker_client_id != old_cid and pos.status == MAKER_RESTING
    sim._pos[SYM] = -12.0                                                     # the venue really filled the cancelled order
    h.ex.on_order_event(OrderEvent("blofin", old_cid, "sim-1", "filled", filled_qty=12.0, avg_price=1.0061,
                                   fee=0.0024, liquidity="maker", ts=h.ex.clock()))
    await settle()
    assert pos.status == "DEGRADED" and pos.filled_a == 12.0 and pos.maker_filled_qty == 0.0   # never on the live order's counters
    assert await sim.open_orders() == []                                      # the live order was cancelled by the degrade
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and await sim.positions() == [] and await h.sim("mexc").positions() == []


async def test_close_is_idempotent_for_risk_stats(tmp_path):
    h = Harness(tmp_path)
    h.quote("blofin", 1.0050, 1.0060)
    h.quote("mexc", 1.0000, 1.0008)
    pos = await h.ex.enter_tt(Intent("TT_ENTER", symbol=SYM, venue_a="blofin", venue_b="mexc", size_usd=25.0))
    h.quote("blofin", 1.0010, 1.0012)
    h.quote("mexc", 1.0009, 1.0011)
    await h.ex.exit_tt(pos, "convergence")
    st = dict(h.risk.pair_stats["XYZUSDT|blofin>mexc"])
    h.ex._close(pos, "convergence")                                           # a racing second close
    assert h.risk.pair_stats["XYZUSDT|blofin>mexc"] == st and h.book.total_trades == 1
    assert pos.id not in h.ex._locks and not any(t.pos_id == pos.id for t in h.ex._tracks.values())
