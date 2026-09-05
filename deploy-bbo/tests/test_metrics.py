import asyncio
import logging

from bbo_trader import metrics as metrics_mod
from bbo_trader.metrics import LatencyHist, Metrics, CoverageWatchdog
from bbo_trader.quotes import QuoteBoard
from tests.conftest import mk_bbo


def test_latency_hist_percentiles_window_and_lifetime():
    h = LatencyHist()
    for v in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        h.add(v)
    d = h.to_dict()
    assert d["n"] == 10 and d["p50"] == 60.0 and d["p95"] == 100.0 and d["max"] == 100.0   # floor(q·n) convention
    assert LatencyHist().to_dict() == {"n": 0, "total": 0, "p50": 0.0, "p95": 0.0, "max": 0.0, "max_ever": 0.0}
    small = LatencyHist(3)
    for v in (1000.0, 1.0, 2.0, 3.0, 4.0):
        small.add(v)
    d = small.to_dict()
    assert d["n"] == 3 and d["max"] == 4.0 and d["total"] == 5 and d["max_ever"] == 1000.0   # window vs lifetime
    small.add(-5.0)                                              # a backwards clock step is clamped, NaN ignored
    small.add(float("nan"))
    assert small.to_dict()["n"] == 3 and small.to_dict()["total"] == 6 and min(small._v) == 0.0


def test_metrics_record_and_funnel_and_shape():
    t = [100.0]
    m = Metrics(mono=lambda: t[0])
    m.record("submit_to_ack", 120.0)
    m.record("submit_to_ack", 80.0)
    m.funnel["below_edge"] += 2
    t[0] = 160.0
    d = m.to_dict()
    assert d["latency_ms"]["submit_to_ack"]["n"] == 2 and d["latency_ms"]["submit_to_ack"]["p50"] == 120.0
    assert d["funnel"] == {"below_edge": 2} and d["uptime_s"] == 60
    assert set(d) == {"latency_ms", "funnel", "loop_lag_ms", "uptime_s", "started_at"}


async def test_loop_lag_sampler_measures_and_summarizes(monkeypatch, caplog):
    m = Metrics()
    real_sleep = asyncio.sleep

    async def slow_sleep(_s):                                     # the loop is "busy": every sleep overruns by ~30 ms
        await real_sleep(0.03)
    monkeypatch.setattr(metrics_mod.asyncio, "sleep", slow_sleep)
    with caplog.at_level(logging.WARNING, logger="bbo.metrics"):
        task = asyncio.create_task(m.sample_loop_lag(interval_s=0.001, warn_ms=10.0, summary_s=0.05))
        await real_sleep(0.2)
        task.cancel()
    d = m.to_dict()["loop_lag_ms"]
    assert d["n"] >= 3 and d["p50"] >= 20.0                        # lag is measured against the requested interval
    lines = [r for r in caplog.records if "LOOP_LAG" in r.getMessage()]
    assert 1 <= len(lines) <= 4 and "over 10 ms" in lines[0].getMessage()   # summarized, not one line per sample


def test_coverage_watchdog_counts_relative_floor_and_grace(clock, caplog):
    board = QuoteBoard(2.0)
    board.set(mk_bbo("mexc", "AUSDT", 1, 1.1, ts=clock()))
    board.set(mk_bbo("mexc", "BUSDT", 1, 1.1, ts=clock()))
    board.set(mk_bbo("blofin", "AUSDT", 1, 1.1, ts=clock() - 5))    # stale
    w = CoverageWatchdog(board, ["mexc", "blofin"], floor=1, clock=clock)
    with caplog.at_level(logging.WARNING, logger="bbo.metrics"):
        assert w.check() == {"mexc": 2, "blofin": 0}                # a dead venue reads 0, never disappears
    assert "FEED_COVERAGE_LOW" not in caplog.text                    # quiet during the start-up grace
    clock.tick(61)
    for i in range(400):
        board.set(mk_bbo("mexc", f"S{i}USDT", 1, 1.1, ts=clock()))
    with caplog.at_level(logging.WARNING, logger="bbo.metrics"):
        w.check()
    assert "FEED_COVERAGE_LOW blofin fresh=0" in caplog.text and "FEED_COVERAGE_LOW mexc" not in caplog.text
    caplog.clear()
    clock.tick(3)                                                    # mexc: 390 of 400 go stale -> under 50 % of its high-water mark
    for i in range(10):
        board.set(mk_bbo("mexc", f"S{i}USDT", 1, 1.1, ts=clock()))
    with caplog.at_level(logging.WARNING, logger="bbo.metrics"):
        assert w.check()["mexc"] == 10
    assert "FEED_COVERAGE_LOW mexc fresh=10 threshold=200" in caplog.text
    assert w.to_dict() == {"fresh": {"mexc": 10, "blofin": 0}, "high": {"mexc": 400, "blofin": 0}, "floor": 1, "frac": 0.5}
