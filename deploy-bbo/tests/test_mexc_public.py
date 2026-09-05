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
    assert b.ts_exchange == pytest.approx(1700000000.123, abs=1e-6) and b.ts_local == 42.0 and b.contract_size == 10.0
    assert mexc.parse_depth({"channel": "pong", "data": 1}, {}, 0.0) == []
    assert mexc.parse_depth({"channel": "push.depth.full", "symbol": "X_USDT", "data": {"bids": [], "asks": []}}, {}, 0.0) == []


def test_parse_specs_tickers_funding():
    specs = mexc.parse_specs({"success": True, "data": [
        {"symbol": "XYZ_USDT", "quoteCoin": "USDT", "state": 0, "contractSize": 10, "volUnit": 1, "minVol": 5, "priceUnit": 0.0001},
        {"symbol": "OLD_USDT", "quoteCoin": "USDT", "state": 1, "contractSize": 1, "volUnit": 1, "minVol": 1, "priceUnit": 0.01},
        {"symbol": "BTC_USDC", "quoteCoin": "USDC", "state": 0, "contractSize": 1, "volUnit": 1, "minVol": 1, "priceUnit": 0.1}]})
    assert list(specs) == ["XYZUSDT"]
    s = specs["XYZUSDT"]
    assert (s.instrument, s.contract_size, s.lot, s.min_qty, s.tick) == ("XYZ_USDT", 10.0, 1.0, 5.0, 0.0001)   # lot ≠ min
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


# ---- REST client + robustness (fake session; shapes frozen from live captures on 2026-09-05) ----------------
import json
import logging

from bbo_trader.venues.base import VenueError


class _Resp:
    def __init__(self, status, body):
        self.status, self._body = status, body

    async def text(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, status=200, body="{}"):
        self.status, self.body, self.calls = status, body, []

    def get(self, url, **kw):
        self.calls.append((url, kw))
        return _Resp(self.status, self.body)


LIVE_DETAIL_ROW = {"symbol": "BTC_USDT", "quoteCoin": "USDT", "settleCoin": "USDT", "contractSize": 0.0001, "priceUnit": 0.1,
                   "volUnit": 1, "minVol": 1, "maxVol": 400000, "state": 0, "apiAllowed": True, "takerFeeRate": 0.0002,
                   "makerFeeRate": 0, "isNew": False, "isHot": True, "openingTime": 0}
LIVE_TICKER_ROW = {"contractId": 10, "symbol": "BTC_USDT", "lastPrice": 79805, "bid1": 79804.9, "ask1": 79805,
                   "volume24": 229640964, "amount24": 1828276834.31004, "fundingRate": 1.8e-05, "timestamp": 1788624278796}
LIVE_FUNDING_ROW = {"symbol": "BTC_USDT", "fundingRate": 1.8e-05, "collectCycle": 8, "nextSettleTime": 1788652800000,
                    "timestamp": 1788624281857}
LIVE_DEPTH_REST = {"success": True, "code": 0, "data": {"cts": None, "asks": [[79805, 99907, 7], [79805.1, 8556, 4]],
                                                        "bids": [[79804.9, 276151, 5], [79804.8, 9060, 2]],
                                                        "version": 41535524017, "timestamp": 1788624282021}}
LIVE_PUSH = {"symbol": "BTC_USDT", "data": {"cts": 1788624282010, "asks": [[79805, 99907, 7]], "bids": [[79804.9, 276151, 5]],
                                            "version": 41535524017}, "channel": "push.depth.full", "ts": 1788624282021}


def test_live_shapes_round_trip():
    specs = mexc.parse_specs({"success": True, "code": 0, "data": [LIVE_DETAIL_ROW]})
    assert specs["BTCUSDT"].contract_size == 0.0001 and specs["BTCUSDT"].tick == 0.1 and specs["BTCUSDT"].lot == 1.0
    assert mexc.parse_tickers({"success": True, "data": [LIVE_TICKER_ROW]}) == {"BTCUSDT": 1828276834.31004}
    assert mexc.parse_funding({"success": True, "data": [LIVE_FUNDING_ROW]}) == {"BTCUSDT": (1.8e-05, 1788652800.0)}
    b = mexc.parse_depth(LIVE_PUSH, {"BTC_USDT": 0.0001}, now=1.0)[0]
    assert b.bid == 79804.9 and b.ask_qty == 99907.0 and b.ts_exchange == pytest.approx(1788624282.010, abs=1e-6)   # cts, not ts
    assert b.touch_notional("buy") == pytest.approx(99907 * 79805 * 0.0001)
    r = mexc.parse_depth_rest(LIVE_DEPTH_REST, "BTC_USDT", 0.0001, 2.0)
    assert r.bid_qty == 276151.0 and r.ts_exchange == pytest.approx(1788624282.021, abs=1e-6) and r.contract_size == 0.0001


def test_specs_skip_api_disallowed_and_isolate_bad_rows(caplog):
    rows = [LIVE_DETAIL_ROW, dict(LIVE_DETAIL_ROW, symbol="FATCOIN_USDT", apiAllowed=False),
            dict(LIVE_DETAIL_ROW, symbol="BAD_USDT", contractSize="n/a"), dict(LIVE_DETAIL_ROW, symbol="NUL_USDT", priceUnit=None),
            "not-a-row", dict(LIVE_DETAIL_ROW, symbol="OK2_USDT")]
    with caplog.at_level(logging.WARNING, logger="bbo.mexc"):
        specs = mexc.parse_specs({"success": True, "data": rows})
    assert list(specs) == ["BTCUSDT", "OK2USDT"] and "SPEC_ROWS_DROPPED mexc 3 of 6" in caplog.text
    assert mexc.parse_tickers({"data": [dict(LIVE_TICKER_ROW, symbol="BAD_USDT", amount24=None), LIVE_TICKER_ROW, 5]}) == {"BTCUSDT": 1828276834.31004}
    assert mexc.parse_funding({"data": [dict(LIVE_FUNDING_ROW, symbol="BAD_USDT", nextSettleTime="x"), LIVE_FUNDING_ROW]}) == {"BTCUSDT": (1.8e-05, 1788652800.0)}


def test_unknown_instrument_and_malformed_frames_yield_nothing(caplog):
    assert mexc.parse_depth(LIVE_PUSH, {}, 1.0) == []                                    # no contract size → no quote
    assert mexc.parse_depth(dict(LIVE_PUSH, data=[LIVE_PUSH["data"]]), {"BTC_USDT": 1.0}, 1.0) == []
    assert mexc.parse_depth(dict(LIVE_PUSH, data="junk"), {"BTC_USDT": 1.0}, 1.0) == []
    assert mexc.parse_depth({"channel": "push.depth.full", "symbol": "BTC_USDT", "data": {"bids": [[1, 1]], "asks": []}},
                            {"BTC_USDT": 1.0}, 1.0) == []                                 # one-sided book
    assert mexc.parse_depth({"channel": "push.depth.full", "symbol": "BTC_USDT", "ts": 5000,
                             "data": {"bids": [["1.5", "2"]], "asks": [["1.6", "3"]]}}, {"BTC_USDT": 1.0}, 1.0)[0].bid == 1.5
    with pytest.raises(ValueError):                                                        # a NaN price must never become a BBO
        mexc.parse_depth({"channel": "push.depth.full", "symbol": "BTC_USDT", "ts": 5000,
                          "data": {"bids": [[float("nan"), 2]], "asks": [[1.6, 3]]}}, {"BTC_USDT": 1.0}, 1.0)
    from bbo_trader.config import VenueConfig
    got = []
    feed = mexc.MexcPublic(VenueConfig("mexc", "trade", 0.02, 0.0, max_topics=30), got.append)
    state = {}
    with caplog.at_level(logging.WARNING, logger="bbo.mexc"):
        for _ in range(12):
            assert feed._parse({"channel": "rs.error", "data": "Contract [BOGUS_USDT] not exists", "ts": 1}, state) == []
    assert state["errors"] == 12 and caplog.text.count("rs.error") == 2                  # logged at #1 and #10
    with pytest.raises(ValueError):
        mexc.to_instrument("BTCUSDC")                                                    # never map to the wrong contract
    with pytest.raises(ValueError):
        mexc.to_instrument("USDT")


async def test_rest_client_raises_on_error_envelopes_and_never_returns_an_empty_universe():
    for status, body in ((200, json.dumps({"success": False, "code": 510, "message": "request frequency"})),
                         (404, json.dumps({"success": False, "code": 404, "message": "Not Found"})),
                         (429, "Too Many Requests"), (200, "<html>Cloudflare</html>"), (200, json.dumps([1, 2])),
                         (500, json.dumps({"success": True, "code": 0, "data": [LIVE_DETAIL_ROW]}))):   # status alone must fail
        m = mexc.MexcMarket(_Session(status, body))
        with pytest.raises(VenueError):
            await m.fetch_specs()
        with pytest.raises(VenueError):
            await m.fetch_volumes()
    m = mexc.MexcMarket(_Session(200, json.dumps({"success": True, "code": 0, "data": []})))
    with pytest.raises(VenueError, match="no usable contracts"):
        await m.fetch_specs()                                    # an empty spec set must never reach the App
    m = mexc.MexcMarket(_Session(200, json.dumps({"success": True, "code": 0, "data": [LIVE_DETAIL_ROW]})), clock=lambda: 7.0)
    assert list(await m.fetch_specs()) == ["BTCUSDT"]
    m.session = _Session(200, json.dumps(LIVE_DEPTH_REST))
    b = await m.fetch_bbo("BTCUSDT")
    assert b.contract_size == 0.0001 and b.ts_local == 7.0
    url, kw = m.session.calls[0]
    assert url.endswith("/api/v1/contract/depth/BTC_USDT?limit=5") and kw["timeout"].total == 5.0 and "User-Agent" in kw["headers"]
    assert await m.fetch_bbo("NOPEUSDT") is None                 # no spec → no fabricated contract size
