from decimal import Decimal
from unittest.mock import AsyncMock, patch
import pytest
from triscan.sources.kucoin import KucoinSource


@pytest.mark.asyncio
async def test_kucoin_fetch_markets():
    sym = {"data": [
        {"symbol": "BTC-USDT", "baseCurrency": "BTC", "quoteCurrency": "USDT", "enableTrading": True},
        {"symbol": "ETH-BTC",  "baseCurrency": "ETH", "quoteCurrency": "BTC",  "enableTrading": True},
        {"symbol": "OFF-USDT", "baseCurrency": "OFF", "quoteCurrency": "USDT", "enableTrading": False},
    ]}
    stats = {"data": {"ticker": [
        {"symbol": "BTC-USDT", "volValue": "5000000"},
        {"symbol": "ETH-BTC",  "volValue": "1500000"},
    ]}}
    src = KucoinSource(name="kucoin", taker_fee_pct=0.10, rest_url="https://api.kucoin.com", ws_url="")
    with patch.object(src, "_get_json", new=AsyncMock(side_effect=[sym, stats])):
        markets = await src.fetch_markets()
    syms = {m.symbol for m in markets}
    assert "BTC/USDT" in syms
    assert "OFF/USDT" not in syms


@pytest.mark.asyncio
async def test_kucoin_fetch_tickers():
    stats = {"data": {"ticker": [
        {"symbol": "BTC-USDT", "buy": "60000", "sell": "60001"},
    ]}}
    src = KucoinSource(name="kucoin", taker_fee_pct=0.10, rest_url="https://api.kucoin.com", ws_url="")
    with patch.object(src, "_get_json", new=AsyncMock(return_value=stats)):
        t = await src.fetch_tickers(["BTC/USDT"])
    assert t["BTC/USDT"].ask == Decimal("60001")
