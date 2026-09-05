from bbo_trader.metrics import LatencyHist, Metrics, CoverageWatchdog
from bbo_trader.quotes import QuoteBoard
from tests.conftest import mk_bbo


def test_latency_hist_percentiles():
    h = LatencyHist()
    for v in [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        h.add(v)
    d = h.to_dict()
    assert d["n"] == 10 and d["p50"] == 60.0 and d["p95"] == 100.0 and d["max"] == 100.0
    assert LatencyHist().to_dict() == {"n": 0, "p50": 0.0, "p95": 0.0, "max": 0.0}


def test_metrics_record_and_funnel():
    m = Metrics()
    m.record("submit_to_ack", 120.0)
    m.funnel["below_edge"] += 2
    d = m.to_dict()
    assert d["latency_ms"]["submit_to_ack"]["n"] == 1 and d["funnel"] == {"below_edge": 2}


def test_coverage_watchdog_counts(clock):
    board = QuoteBoard(2.0)
    board.set(mk_bbo("mexc", "AUSDT", 1, 1.1, ts=clock()))
    board.set(mk_bbo("mexc", "BUSDT", 1, 1.1, ts=clock()))
    board.set(mk_bbo("blofin", "AUSDT", 1, 1.1, ts=clock() - 5))    # stale
    w = CoverageWatchdog(board, ["mexc", "blofin"], floor=1, clock=clock)
    assert w.check() == {"mexc": 2, "blofin": 0}