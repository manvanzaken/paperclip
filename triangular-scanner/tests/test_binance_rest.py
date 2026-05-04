from decimal import Decimal
from unittest.mock import AsyncMock, patch
import pytest
from triscan.sources.binance import BinanceSource


@pytest.mark.asyncio
async def test_binance_fetch_markets():
    info = {"symbols": [
        {"symbol": "BTCUSDT", "status": "TRADING", "baseAsset": "BTC", "quoteAsset": "USDT", "isSpotTradingAllowed": True},
        {"symbol": "ETHBTC",  "status": "TRADING", "baseAsset": "ETH", "quoteAsset": "BTC",  "isSpotTradingAllowed": True},
        {"symbol": "OFFLINE", "status": "BREAK",   "baseAsset": "OFF", "quoteAsset": "USDT", "isSpotTradingAllowed": True},
    ]}
    vol = [
        {"symbol": "BTCUSDT", "quoteVolume": "9000000"},
        {"symbol": "ETHBTC",  "quoteVolume": "1500000"},
    ]
    src = BinanceSource(name="binance", taker_fee_pct=0.10, rest_url="https://api.binance.com", ws_url="")
    with patch.object(src, "_get_json", new=AsyncMock(side_effect=[info, vol])):
        markets = await src.fetch_markets()
    syms = {m.symbol for m in markets}
    assert "BTC/USDT" in syms
    assert "ETH/BTC" in syms
    assert "OFF/USDT" not in syms


@pytest.mark.asyncio
async def test_binance_fetch_tickers():
    rows = [
        {"symbol": "BTCUSDT", "bidPrice": "60000.0", "askPrice": "60001.0"},
    ]
    src = BinanceSource(name="binance", taker_fee_pct=0.10, rest_url="https://api.binance.com", ws_url="")
    with patch.object(src, "_get_json", new=AsyncMock(return_value=rows)):
        t = await src.fetch_tickers(["BTC/USDT"])
    assert t["BTC/USDT"].bid == Decimal("60000.0")
