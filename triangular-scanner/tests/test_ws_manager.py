import asyncio
from unittest.mock import AsyncMock, MagicMock
import pytest
from triscan.ws_manager import WsManager


@pytest.mark.asyncio
async def test_acquire_creates_one_subscription_per_symbol():
    cancel_calls = {"BTC/USDT": 0}
    async def cancel():
        cancel_calls["BTC/USDT"] += 1

    src = AsyncMock()
    src.name = "binance"
    src.subscribe_book = AsyncMock(return_value=cancel)

    mgr = WsManager(source=src, max_subscriptions=10)
    on_update = AsyncMock()
    await mgr.acquire("BTC/USDT", on_update)
    await mgr.acquire("BTC/USDT", on_update)
    assert src.subscribe_book.await_count == 1
    assert mgr.refcount("BTC/USDT") == 2


@pytest.mark.asyncio
async def test_release_drops_subscription_on_zero_refcount():
    cancel = AsyncMock()
    src = AsyncMock()
    src.name = "binance"
    src.subscribe_book = AsyncMock(return_value=cancel)

    mgr = WsManager(source=src, max_subscriptions=10)
    on_update = AsyncMock()
    await mgr.acquire("BTC/USDT", on_update)
    await mgr.acquire("BTC/USDT", on_update)
    await mgr.release("BTC/USDT")
    assert mgr.refcount("BTC/USDT") == 1
    cancel.assert_not_awaited()
    await mgr.release("BTC/USDT")
    assert mgr.refcount("BTC/USDT") == 0
    cancel.assert_awaited_once()


@pytest.mark.asyncio
async def test_backpressure_rejects_when_at_cap():
    src = AsyncMock()
    src.name = "binance"
    src.subscribe_book = AsyncMock(return_value=AsyncMock())
    mgr = WsManager(source=src, max_subscriptions=2)
    await mgr.acquire("A/USDT", AsyncMock())
    await mgr.acquire("B/USDT", AsyncMock())
    accepted = await mgr.acquire("C/USDT", AsyncMock())
    assert accepted is False
    assert mgr.refcount("C/USDT") == 0
