from decimal import Decimal
from unittest.mock import AsyncMock, patch
import pytest
from triscan.sources.bybit import BybitSource


@pytest.mark.asyncio
async def test_bybit_fetch_markets():
    info = {"result": {"list": [
        {"symbol": "BTCUSDT", "status": "Trading", "baseCoin": "BTC", "quoteCoin": "USDT"},
        {"symbol": "ETHBTC",  "status": "Trading", "baseCoin": "ETH", "quoteCoin": "BTC"},
        {"symbol": "OFFUSDT", "status": "Closed",  "baseCoin": "OFF", "quoteCoin": "USDT"},
    ]}}
    tickers = {"result": {"list": [
        {"symbol": "BTCUSDT", "turnover24h": "5000000", "bid1Price": "60000", "ask1Price": "60001"},
        {"symbol": "ETHBTC",  "turnover24h": "1500000", "bid1Price": "0.05",  "ask1Price": "0.0501"},
    ]}}
    src = BybitSource(name="bybit", taker_fee_pct=0.10, rest_url="https://api.bybit.com", ws_url="")
    with patch.object(src, "_get_json", new=AsyncMock(side_effect=[info, tickers])):
        markets = await src.fetch_markets()
    syms = {m.symbol for m in markets}
    assert "BTC/USDT" in syms
    assert "OFF/USDT" not in syms


@pytest.mark.asyncio
async def test_bybit_fetch_tickers():
    tickers = {"result": {"list": [
        {"symbol": "BTCUSDT", "bid1Price": "60000", "ask1Price": "60001"},
    ]}}
    src = BybitSource(name="bybit", taker_fee_pct=0.10, rest_url="https://api.bybit.com", ws_url="")
    with patch.object(src, "_get_json", new=AsyncMock(return_value=tickers)):
        t = await src.fetch_tickers(["BTC/USDT"])
    assert t["BTC/USDT"].ask == Decimal("60001")
