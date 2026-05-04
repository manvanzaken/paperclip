import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
import pytest
from triscan.models import Triangle, LegSide, Book, BookLevel, OpportunityState
from triscan.pipeline import Pipeline, PipelineConfig
from triscan.rest_poller import Tier1Result


def _book(symbol, bid_p, bid_s, ask_p, ask_s, seq=1, ts_ms=1):
    return Book("binance", symbol, [BookLevel(Decimal(str(bid_p)), Decimal(str(bid_s)))],
                [BookLevel(Decimal(str(ask_p)), Decimal(str(ask_s)))], ts_ms=ts_ms, seq=seq)


@pytest.mark.asyncio
async def test_idle_to_candidate_when_tier1_threshold_met():
    tri = Triangle("binance", "USDT",
                   (("BTC/USDT", LegSide.BUY), ("ETH/BTC", LegSide.BUY), ("ETH/USDT", LegSide.SELL)))
    ws = MagicMock()
    ws.acquire = AsyncMock(return_value=True)
    ws.release = AsyncMock()
    cfg = PipelineConfig(tier1_threshold_pct=0.5, tier2_threshold_pct=0.1,
                         min_profit_usd=1.0, cooldown_sec=60, max_size_cap_usd=Decimal("10000"),
                         taker_fee_pct=Decimal("0.10"))
    p = Pipeline(triangles=[tri], ws_manager=ws, config=cfg, on_opportunity=AsyncMock())

    await p.handle_tier1([Tier1Result(triangle=tri, gross_edge_pct=0.6, net_edge_pct=0.3, ts_ms=1)])
    assert p.state(tri) == OpportunityState.CANDIDATE
    assert ws.acquire.await_count == 3


@pytest.mark.asyncio
async def test_candidate_to_idle_after_cooldown():
    tri = Triangle("binance", "USDT",
                   (("BTC/USDT", LegSide.BUY), ("ETH/BTC", LegSide.BUY), ("ETH/USDT", LegSide.SELL)))
    ws = MagicMock()
    ws.acquire = AsyncMock(return_value=True); ws.release = AsyncMock()
    cfg = PipelineConfig(tier1_threshold_pct=0.5, tier2_threshold_pct=0.1,
                         min_profit_usd=1.0, cooldown_sec=0, max_size_cap_usd=Decimal("10000"),
                         taker_fee_pct=Decimal("0.10"))
    p = Pipeline([tri], ws, cfg, on_opportunity=AsyncMock())
    await p.handle_tier1([Tier1Result(tri, 0.6, 0.3, 1)])
    await p.handle_tier1([Tier1Result(tri, 0.1, 0.0, 2)])
    await p.tick(now_ms=10)
    assert p.state(tri) == OpportunityState.IDLE
    assert ws.release.await_count == 3


@pytest.mark.asyncio
async def test_candidate_to_confirmed_emits_opportunity_open():
    tri = Triangle("binance", "USDT",
                   (("BTC/USDT", LegSide.BUY), ("ETH/BTC", LegSide.BUY), ("ETH/USDT", LegSide.SELL)))
    ws = MagicMock(); ws.acquire = AsyncMock(return_value=True); ws.release = AsyncMock()
    on_opp = AsyncMock()
    cfg = PipelineConfig(tier1_threshold_pct=0.5, tier2_threshold_pct=0.1,
                         min_profit_usd=1.0, cooldown_sec=60, max_size_cap_usd=Decimal("10000"),
                         taker_fee_pct=Decimal("0.10"))
    p = Pipeline([tri], ws, cfg, on_opportunity=on_opp)
    await p.handle_tier1([Tier1Result(tri, 0.6, 0.3, 1)])

    await p.handle_book(_book("BTC/USDT", 59999, 10, 60000, 10, seq=1))
    await p.handle_book(_book("ETH/BTC",  0.04999, 100, 0.05000, 100, seq=1))
    await p.handle_book(_book("ETH/USDT", 3010, 1000, 3011, 1000, seq=1))

    assert p.state(tri) == OpportunityState.CONFIRMED
    on_opp.assert_awaited()
    args, _ = on_opp.await_args
    evt = args[0]
    assert evt["type"] == "OpportunityOpen"
