from bbo_trader.quotes import QuoteBoard
from tests.conftest import mk_bbo


def test_set_get_and_staleness():
    board = QuoteBoard(stale_s=2.0, overrides={"hyperliquid": 10.0})
    assert board.set(mk_bbo("mexc", "XYZUSDT", 1.0, 1.001, ts=100.0))
    assert board.set(mk_bbo("blofin", "XYZUSDT", 1.0, 1.002, ts=100.0))
    assert board.set(mk_bbo("hyperliquid", "XYZUSDT", 1.0, 1.003, ts=95.0))
    assert not board.set(mk_bbo("mexc", "ABCUSDT", 1.0, 0.9))          # crossed book ignored
    assert board.get("mexc", "XYZUSDT").ask == 1.001
    assert board.fresh("mexc", "XYZUSDT", now=101.5) is not None
    assert board.fresh("mexc", "XYZUSDT", now=102.5) is None            # 2.5 s old > 2.0
    assert board.fresh("hyperliquid", "XYZUSDT", now=104.0) is not None  # override 10 s
    assert board.fresh_venues("XYZUSDT", now=101.0) == ["blofin", "hyperliquid", "mexc"]
    assert board.fresh_venues("XYZUSDT", now=103.0) == ["hyperliquid"]
    assert board.fresh_counts(now=103.0) == {"mexc": 0, "blofin": 0, "hyperliquid": 1}
    assert board.symbols() == {"XYZUSDT"}
    # staleness is governed by ts_local, never by the venue clock
    assert board.set(mk_bbo("mexc", "PQRUSDT", 1.0, 1.001, ts=100.0, ts_exchange=1.0))
    assert board.fresh("mexc", "PQRUSDT", now=101.0) is not None
    assert board.fresh("mexc", "XYZUSDT", now=102.0) is not None   # boundary: age == stale_s is still fresh
    assert board.set(mk_bbo("mexc", "XYZUSDT", 2.0, 2.001, ts=110.0))  # newest quote overwrites
    assert board.get("mexc", "XYZUSDT").bid == 2.0
