import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock
import pytest
from triscan.models import Quote, LegSide, Triangle
from triscan.rest_poller import Tier1Poller, Tier1Result


@pytest.mark.asyncio
async def test_poller_emits_gross_edges_for_triangles():
    src = AsyncMock()
    src.name = "mexc"
    src.taker_fee_pct = 0.05
    src.fetch_tickers.return_value = {
        "BTC/USDT": Quote("mexc", "BTC/USDT", Decimal("60000"), Decimal("60001"), ts_ms=1),
        "ETH/BTC":  Quote("mexc", "ETH/BTC",  Decimal("0.04999"), Decimal("0.05000"), ts_ms=1),
        "ETH/USDT": Quote("mexc", "ETH/USDT", Decimal("3010"),    Decimal("3011"),    ts_ms=1),
    }
    triangle = Triangle(
        exchange="mexc", anchor="USDT",
        legs=(("BTC/USDT", LegSide.BUY), ("ETH/BTC", LegSide.BUY), ("ETH/USDT", LegSide.SELL)),
    )
    poller = Tier1Poller(source=src, triangles=[triangle])
    results = await poller.poll_once()
    assert len(results) == 1
    r = results[0]
    assert isinstance(r, Tier1Result)
    assert r.triangle.id == triangle.id
    assert r.gross_edge_pct > 0


@pytest.mark.asyncio
async def test_poller_skips_triangle_with_missing_quote():
    src = AsyncMock()
    src.name = "mexc"
    src.taker_fee_pct = 0.05
    src.fetch_tickers.return_value = {}
    triangle = Triangle(
        exchange="mexc", anchor="USDT",
        legs=(("BTC/USDT", LegSide.BUY), ("ETH/BTC", LegSide.BUY), ("ETH/USDT", LegSide.SELL)),
    )
    poller = Tier1Poller(source=src, triangles=[triangle])
    results = await poller.poll_once()
    assert results == []
