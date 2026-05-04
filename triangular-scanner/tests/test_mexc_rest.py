import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from triscan.sources.mexc import MexcSource


@pytest.mark.asyncio
async def test_fetch_markets_filters_by_status_and_quote():
    fake_resp = {
        "symbols": [
            {"symbol": "BTCUSDT", "status": "1", "baseAsset": "BTC", "quoteAsset": "USDT"},
            {"symbol": "ETHUSDT", "status": "1", "baseAsset": "ETH", "quoteAsset": "USDT"},
            {"symbol": "DEADCOIN", "status": "0", "baseAsset": "DEAD", "quoteAsset": "USDT"},
        ]
    }
    fake_24h = [
        {"symbol": "BTCUSDT", "quoteVolume": "5000000"},
        {"symbol": "ETHUSDT", "quoteVolume": "2000000"},
    ]
    src = MexcSource(name="mexc", taker_fee_pct=0.05, rest_url="https://api.mexc.com", ws_url="")
    with patch.object(src, "_get_json", new=AsyncMock(side_effect=[fake_resp, fake_24h])):
        markets = await src.fetch_markets()
    syms = {m.symbol for m in markets}
    assert "BTC/USDT" in syms
    assert "ETH/USDT" in syms
    assert "DEAD/USDT" not in syms
    btc = next(m for m in markets if m.symbol == "BTC/USDT")
    assert btc.volume_24h_usd == 5_000_000


@pytest.mark.asyncio
async def test_fetch_tickers_parses_book_ticker():
    fake = [
        {"symbol": "BTCUSDT", "bidPrice": "60000", "askPrice": "60001"},
        {"symbol": "ETHUSDT", "bidPrice": "3000",  "askPrice": "3001"},
    ]
    src = MexcSource(name="mexc", taker_fee_pct=0.05, rest_url="https://api.mexc.com", ws_url="")
    with patch.object(src, "_get_json", new=AsyncMock(return_value=fake)):
        tickers = await src.fetch_tickers(["BTC/USDT", "ETH/USDT"])
    assert tickers["BTC/USDT"].bid == Decimal("60000")
    assert tickers["BTC/USDT"].ask == Decimal("60001")
    assert tickers["ETH/USDT"].ask == Decimal("3001")
