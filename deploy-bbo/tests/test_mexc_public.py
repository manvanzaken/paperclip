import pytest

from bbo_trader.venues import mexc


def test_instrument_mapping_and_subscribe():
    assert mexc.to_instrument("BTCUSDT") == "BTC_USDT" and mexc.to_symbol("BTC_USDT") == "BTCUSDT"
    assert mexc.subscribe(["BTC_USDT"]) == [{"method": "sub.depth.full", "param": {"symbol": "BTC_USDT", "limit": 5}}]


def test_parse_depth_top_level_with_contract_size():
    raw = {"channel": "push.depth.full", "symbol": "XYZ_USDT", "ts": 1700000000123,
           "data": {"bids": [[1.0041, 500, 3], [1.0040, 900, 5]], "asks": [[1.0061, 200, 2], [1.0062, 10, 1]], "version": 7}}
    out = mexc.parse_depth(raw, {"XYZ_USDT": 10.0}, now=42.0)
    assert len(out) == 1
    b = out[0]
    assert (b.venue, b.symbol, b.bid, b.bid_qty, b.ask, b.ask_qty) == ("mexc", "XYZUSDT", 1.0041, 500.0, 1.0061, 200.0)
    assert b.ts_exchange == pytest.approx(1700000000.123) and b.ts_local == 42.0 and b.contract_size == 10.0
    assert mexc.parse_depth({"channel": "pong", "data": 1}, {}, 0.0) == []
    assert mexc.parse_depth({"channel": "push.depth.full", "symbol": "X_USDT", "data": {"bids": [], "asks": []}}, {}, 0.0) == []


def test_parse_specs_tickers_funding():
    specs = mexc.parse_specs({"success": True, "data": [
        {"symbol": "XYZ_USDT", "quoteCoin": "USDT", "state": 0, "contractSize": 10, "volUnit": 1, "minVol": 1, "priceUnit": 0.0001},
        {"symbol": "OLD_USDT", "quoteCoin": "USDT", "state": 1, "contractSize": 1, "volUnit": 1, "minVol": 1, "priceUnit": 0.01},
        {"symbol": "BTC_USDC", "quoteCoin": "USDC", "state": 0, "contractSize": 1, "volUnit": 1, "minVol": 1, "priceUnit": 0.1}]})
    assert list(specs) == ["XYZUSDT"]
    s = specs["XYZUSDT"]
    assert (s.instrument, s.contract_size, s.lot, s.min_qty, s.tick) == ("XYZ_USDT", 10.0, 1.0, 1.0, 0.0001)
    vols = mexc.parse_tickers({"data": [{"symbol": "XYZ_USDT", "amount24": "123456.5", "bid1": 1.0, "ask1": 1.1},
                                        {"symbol": "ABC_USDC", "amount24": "1"}]})
    assert vols == {"XYZUSDT": 123456.5}
    fund = mexc.parse_funding({"data": [{"symbol": "XYZ_USDT", "fundingRate": "0.0001", "nextSettleTime": 1700003600000}]})
    assert fund == {"XYZUSDT": (0.0001, 1700003600.0)}
    b = mexc.parse_depth_rest({"data": {"bids": [[1.0, 5]], "asks": [[1.1, 6]], "timestamp": 1700000000000}}, "XYZ_USDT", 10.0, 1.0)
    assert b.bid == 1.0 and b.ask_qty == 6.0 and b.contract_size == 10.0 and b.symbol == "XYZUSDT"


def test_public_feed_wiring(clock):
    from bbo_trader.config import VenueConfig
    got = []
    feed = mexc.MexcPublic(VenueConfig("mexc", "trade", 0.02, 0.0, max_topics=30), got.append, clock=clock)
    feed.set_specs(mexc.parse_specs({"data": [{"symbol": "XYZ_USDT", "quoteCoin": "USDT", "state": 0, "contractSize": 10,
                                               "volUnit": 1, "minVol": 1, "priceUnit": 0.0001}]}))
    feed.set_symbols(["XYZUSDT"])
    assert feed._runner._insts == ["XYZ_USDT"]
    feed._emit(feed._parse({"channel": "push.depth.full", "symbol": "XYZ_USDT", "ts": 1000,
                            "data": {"bids": [[1.0, 1]], "asks": [[1.1, 1]]}}, {}))
    assert got[0].contract_size == 10.0 and got[0].ts_local == clock()