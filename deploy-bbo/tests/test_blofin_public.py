import pytest

from bbo_trader.venues import blofin


def test_instrument_mapping_and_subscribe():
    assert blofin.to_instrument("BTCUSDT") == "BTC-USDT" and blofin.to_symbol("BTC-USDT") == "BTCUSDT"
    assert blofin.subscribe(["A-USDT", "B-USDT"]) == [{"op": "subscribe", "args": [
        {"channel": "books5", "instId": "A-USDT"}, {"channel": "books5", "instId": "B-USDT"}]}]


def test_parse_books5_dict_and_list_shapes():
    raw = {"arg": {"channel": "books5", "instId": "XYZ-USDT"},
           "data": {"bids": [["1.0041", "500"], ["1.0040", "900"]], "asks": [["1.0061", "200"]], "ts": "1700000000123"}}
    out = blofin.parse_books5(raw, {"XYZ-USDT": 0.1}, now=7.0)
    assert len(out) == 1
    b = out[0]
    assert (b.symbol, b.bid, b.bid_qty, b.ask, b.ask_qty, b.contract_size) == ("XYZUSDT", 1.0041, 500.0, 1.0061, 200.0, 0.1)
    assert b.ts_exchange == pytest.approx(1700000000.123, abs=1e-6) and b.ts_local == 7.0
    raw_list = dict(raw, data=[raw["data"]])
    assert len(blofin.parse_books5(raw_list, {"XYZ-USDT": 0.1}, 0.0)) == 1
    assert blofin.parse_books5(raw_list, {}, 0.0) == []                      # unknown instrument: no contract size, no quote
    assert blofin.parse_books5({"event": "subscribe", "arg": {"channel": "books5"}}, {}, 0.0) == []
    assert blofin.parse_books5({"arg": {"channel": "trades", "instId": "X-USDT"}, "data": []}, {}, 0.0) == []


def test_parse_instruments_tickers_funding_books():
    specs = blofin.parse_instruments({"code": "0", "data": [
        {"instId": "XYZ-USDT", "contractValue": "0.1", "lotSize": "1", "minSize": "1", "tickSize": "0.0001", "state": "live"},
        {"instId": "DEAD-USDT", "contractValue": "1", "lotSize": "1", "minSize": "1", "tickSize": "0.01", "state": "suspend"},
        {"instId": "BTC-USDC", "contractValue": "1", "lotSize": "1", "minSize": "1", "tickSize": "0.1", "state": "live"}]})
    assert list(specs) == ["XYZUSDT"]
    s = specs["XYZUSDT"]
    assert (s.instrument, s.contract_size, s.lot, s.min_qty, s.tick) == ("XYZ-USDT", 0.1, 1.0, 1.0, 0.0001)
    vols = blofin.parse_tickers({"data": [{"instId": "XYZ-USDT", "last": "2.0", "volCurrency24h": "1000", "bidPrice": "1.9", "askPrice": "2.1"}]})
    assert vols == {"XYZUSDT": 2000.0}
    fund = blofin.parse_funding({"data": [{"instId": "XYZ-USDT", "fundingRate": "-0.0002", "fundingTime": "1700003600000"}]})
    assert fund == {"XYZUSDT": (-0.0002, 1700003600.0)}
    b = blofin.parse_books_rest({"data": [{"bids": [["1.0", "5"]], "asks": [["1.1", "6"]], "ts": "1700000000000"}]}, "XYZ-USDT", 0.1, 1.0)
    assert b.bid == 1.0 and b.ask_qty == 6.0 and b.contract_size == 0.1


def test_public_feed_wiring(clock):
    from bbo_trader.config import VenueConfig
    got = []
    feed = blofin.BlofinPublic(VenueConfig("blofin", "trade", 0.06, 0.02, max_topics=50), got.append, clock=clock)
    feed.set_specs(blofin.parse_instruments({"data": [{"instId": "XYZ-USDT", "contractValue": "0.1", "lotSize": "1",
                                                        "minSize": "1", "tickSize": "0.0001", "state": "live"}]}))
    feed.set_symbols(["XYZUSDT"])
    assert feed._runner._insts == ["XYZ-USDT"]
    feed._emit(feed._parse({"arg": {"channel": "books5", "instId": "XYZ-USDT"},
                            "data": {"bids": [["1", "1"]], "asks": [["1.1", "1"]], "ts": "1000"}}, {}))
    assert got[0].contract_size == 0.1 and got[0].ts_local == clock()


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


LIVE_INSTRUMENT = {"instId": "BTC-USDT", "baseCurrency": "BTC", "quoteCurrency": "USDT", "contractValue": "0.0001",
                   "listTime": "1755507600000", "maxLeverage": "125", "minSize": "1", "lotSize": "1", "tickSize": "0.1",
                   "instType": "SWAP", "contractType": "linear", "state": "live", "settleCurrency": "USDT", "offTime": ""}
LIVE_TICKER = {"instId": "BTC-USDT", "last": "79758.8", "askPrice": "79755.2", "askSize": "26613", "bidPrice": "79755.1",
               "bidSize": "1935", "volCurrency24h": "18.9118", "vol24h": "189118", "ts": "1788625085021"}
LIVE_FUNDING = {"instId": "HOLO-USDT", "fundingRate": "0.000077429202847014", "fundingTime": "1788638400000",
                "fundingInterval": "4", "fundingIntervalUnit": "hour", "fundingRateCap": "0.03", "fundingRateFloor": "-0.03"}
LIVE_BOOKS_REST = {"code": "0", "msg": "success", "data": [{"asks": [["79738.8", "5976"], ["79738.9", "256"]],
                                                            "bids": [["79738.7", "6316"], ["79738.6", "209"]], "ts": "1788625095610"}]}
LIVE_PUSH = {"arg": {"channel": "books5", "instId": "BTC-USDT"}, "action": "snapshot",
             "data": {"asks": [["79666.5", "1233"]], "bids": [["79666.4", "980"]], "ts": "1788599375999"}}


def test_live_shapes_round_trip():
    specs = blofin.parse_instruments({"code": "0", "msg": "success", "data": [LIVE_INSTRUMENT]})
    assert specs["BTCUSDT"].contract_size == 0.0001 and specs["BTCUSDT"].tick == 0.1 and specs["BTCUSDT"].lot == 1.0
    assert blofin.parse_tickers({"code": "0", "data": [LIVE_TICKER]}) == {"BTCUSDT": pytest.approx(18.9118 * 79758.8)}
    assert blofin.parse_funding({"code": "0", "data": [LIVE_FUNDING]}) == {"HOLOUSDT": (pytest.approx(7.7429202847014e-05), 1788638400.0)}
    b = blofin.parse_books5(LIVE_PUSH, {"BTC-USDT": 0.0001}, now=1.0)[0]
    assert b.bid == 79666.4 and b.ask_qty == 1233.0 and b.ts_exchange == pytest.approx(1788599375.999, abs=1e-6)
    assert b.touch_notional("buy") == pytest.approx(1233 * 79666.5 * 0.0001)
    r = blofin.parse_books_rest(LIVE_BOOKS_REST, "BTC-USDT", 0.0001, 2.0)
    assert r.bid_qty == 6316.0 and r.ts_exchange == pytest.approx(1788625095.610, abs=1e-6) and r.contract_size == 0.0001


def test_instruments_isolate_bad_rows_and_unknown_instruments_yield_nothing(caplog):
    rows = [LIVE_INSTRUMENT, dict(LIVE_INSTRUMENT, instId="BAD-USDT", contractValue="n/a"),
            dict(LIVE_INSTRUMENT, instId="NUL-USDT", tickSize=None), "not-a-row", dict(LIVE_INSTRUMENT, instId="OK2-USDT"),
            dict(LIVE_INSTRUMENT, instId="BTC-USDC", quoteCurrency="USDC")]
    with caplog.at_level(logging.WARNING, logger="bbo.blofin"):
        specs = blofin.parse_instruments({"code": "0", "data": rows})
    assert list(specs) == ["BTCUSDT", "OK2USDT"] and "SPEC_ROWS_DROPPED blofin 3 of 6" in caplog.text
    assert blofin.parse_tickers({"data": [dict(LIVE_TICKER, instId="BAD-USDT", last=None), LIVE_TICKER, 5]}) == {"BTCUSDT": pytest.approx(18.9118 * 79758.8)}
    assert blofin.parse_funding({"data": [dict(LIVE_FUNDING, instId="BAD-USDT", fundingTime=""), LIVE_FUNDING]}) == {"HOLOUSDT": (pytest.approx(7.7429202847014e-05), 1788638400.0)}
    assert blofin.parse_books5(LIVE_PUSH, {}, 1.0) == []                                  # no contract size → no quote
    assert blofin.parse_books5(dict(LIVE_PUSH, data="junk"), {"BTC-USDT": 1.0}, 1.0) == []
    assert blofin.parse_books5(dict(LIVE_PUSH, data={"bids": [], "asks": [["1", "1"]], "ts": "1"}), {"BTC-USDT": 1.0}, 1.0) == []
    from bbo_trader.config import VenueConfig
    feed = blofin.BlofinPublic(VenueConfig("blofin", "trade", 0.06, 0.02, max_topics=50), lambda b: None)
    state = {}
    with caplog.at_level(logging.WARNING, logger="bbo.blofin"):
        for _ in range(12):
            assert feed._parse({"event": "error", "code": "60018", "msg": "Wrong URL or channel:books5,instId:BOGUS-USDT doesn't exist"}, state) == []
    assert state["errors"] == 12 and caplog.text.count("error event") == 2
    with pytest.raises(ValueError):
        blofin.to_instrument("BTCUSDC")


async def test_rest_client_raises_on_error_envelopes_and_never_returns_an_empty_universe():
    for status, body in ((200, json.dumps({"code": "152002", "msg": "Parameter instId error."})),
                         (200, json.dumps({"code": "429", "msg": "Too Many Requests"})),
                         (503, "<html>maintenance</html>"), (200, "not json"), (200, json.dumps([1])),
                         (500, json.dumps({"code": "0", "msg": "success", "data": [LIVE_INSTRUMENT]}))):   # status alone must fail
        m = blofin.BlofinMarket(_Session(status, body))
        with pytest.raises(VenueError):
            await m.fetch_specs()
        with pytest.raises(VenueError):
            await m.fetch_funding()
    m = blofin.BlofinMarket(_Session(200, json.dumps({"code": "0", "msg": "success", "data": []})))
    with pytest.raises(VenueError, match="no usable instruments"):
        await m.fetch_specs()
    m = blofin.BlofinMarket(_Session(200, json.dumps({"code": "0", "msg": "success", "data": [LIVE_INSTRUMENT]})), clock=lambda: 7.0)
    assert list(await m.fetch_specs()) == ["BTCUSDT"]
    m.session = _Session(200, json.dumps(LIVE_BOOKS_REST))
    b = await m.fetch_bbo("BTCUSDT")
    assert b.contract_size == 0.0001 and b.ts_local == 7.0
    url, kw = m.session.calls[0]
    assert url.endswith("/api/v1/market/books?instId=BTC-USDT&size=5") and kw["timeout"].total == 5.0 and "User-Agent" in kw["headers"]
    assert await m.fetch_bbo("NOPEUSDT") is None