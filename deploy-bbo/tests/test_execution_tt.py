import asyncio

import pytest

from bbo_trader.budget import RateBudget
from bbo_trader.execution import Executor
from bbo_trader.metrics import Metrics
from bbo_trader.models import Intent, OPEN, CLOSED, MAKER_RESTING
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
    await h.ex.exit_tt(pos, "test")
    assert pos.status == "DEGRADED" and pos.degraded_leg == "b"    # mexc refused: nothing to reduce
    monkeypatch.setattr(execution, "DEGRADED_RETRY_S", 0.0)
    await h.ex.retry_degraded()
    assert pos.status == CLOSED and pos.exit_filled_b == pos.filled_b and pos.exit_reason == "test"
    assert await h.sim("blofin").positions() == [] and await h.sim("mexc").positions() == []
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