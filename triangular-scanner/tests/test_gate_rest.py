from decimal import Decimal
from unittest.mock import AsyncMock, patch
import pytest
from triscan.sources.gate import GateSource


@pytest.mark.asyncio
async def test_gate_fetch_markets():
    pairs = [
        {"id": "BTC_USDT", "base": "BTC", "quote": "USDT", "trade_status": "tradable"},
        {"id": "ETH_BTC",  "base": "ETH", "quote": "BTC",  "trade_status": "tradable"},
        {"id": "OFF_USDT", "base": "OFF", "quote": "USDT", "trade_status": "untradable"},
    ]
    tickers = [
        {"currency_pair": "BTC_USDT", "highest_bid": "60000", "lowest_ask": "60001", "quote_volume": "5000000"},
        {"currency_pair": "ETH_BTC",  "highest_bid": "0.04999", "lowest_ask": "0.05000", "quote_volume": "1500000"},
    ]
    src = GateSource(name="gate", taker_fee_pct=0.10, rest_url="https://api.gateio.ws", ws_url="")
    with patch.object(src, "_get_json", new=AsyncMock(side_effect=[pairs, tickers])):
        markets = await src.fetch_markets()
    syms = {m.symbol for m in markets}
    assert "BTC/USDT" in syms
    assert "OFF/USDT" not in syms


@pytest.mark.asyncio
async def test_gate_fetch_tickers():
    tickers = [
        {"currency_pair": "BTC_USDT", "highest_bid": "60000", "lowest_ask": "60001"},
    ]
    src = GateSource(name="gate", taker_fee_pct=0.10, rest_url="https://api.gateio.ws", ws_url="")
    with patch.object(src, "_get_json", new=AsyncMock(return_value=tickers)):
        t = await src.fetch_tickers(["BTC/USDT"])
    assert t["BTC/USDT"].ask == Decimal("60001")
