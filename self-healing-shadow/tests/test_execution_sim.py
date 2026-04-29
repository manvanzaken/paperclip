"""Tests for core.execution_sim."""

from datetime import datetime, timezone

import pytest

from core.execution_sim import (
    ExecutionSim,
    InsufficientLiquidity,
    SimulatedFailure,
)
from core.orderbook import OrderBook


def book(bids: list[tuple[float, float]], asks: list[tuple[float, float]]) -> OrderBook:
    return OrderBook(bids=bids, asks=asks, ts=datetime.now(timezone.utc))


def make_sim(
    *,
    starting_balances: dict[str, float] | None = None,
    taker_fees_bps: dict[str, float] | None = None,
    sim_leg_failure_rate: float = 0.0,
    rng_seed: int = 42,
) -> ExecutionSim:
    return ExecutionSim(
        starting_balances=starting_balances or {"mexc": 1000.0, "binance": 1000.0},
        taker_fees_bps=taker_fees_bps or {"mexc": 2.0, "binance": 5.0},
        sim_leg_failure_rate=sim_leg_failure_rate,
        rng_seed=rng_seed,
    )


class TestBookWalk:
    @pytest.mark.asyncio
    async def test_buy_fills_at_vwap_of_asks(self):
        sim = make_sim()
        sim.set_book("mexc", "BTC/USDT:USDT", book(
            bids=[(99.0, 100.0)],
            asks=[(100.0, 5.0), (101.0, 5.0)],
        ))
        # size_usd=750 at ask=100 -> consume 5 (= 500 usd) + 2.5 at 101 (= 252.5 usd)
        # That overshoots 750 -> 5 at 100 + (250/101) at 101.
        # Actually we walk to fill exactly 750 USD: 500 at 100 + 250 at 101.
        # VWAP = (500 + 250) / (5 + 250/101) ≈ 100.333...
        order = await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="buy", size_usd=750.0, client_order_id="x1",
        )
        assert order.state == "filled"
        # VWAP check:
        usd_at_100 = 500.0
        units_at_100 = 5.0
        usd_at_101 = 250.0
        units_at_101 = 250.0 / 101.0
        expected_vwap = (usd_at_100 + usd_at_101) / (units_at_100 + units_at_101)
        assert order.filled_price == pytest.approx(expected_vwap, rel=1e-6)

    @pytest.mark.asyncio
    async def test_sell_fills_at_vwap_of_bids(self):
        sim = make_sim()
        sim.set_book("mexc", "BTC/USDT:USDT", book(
            bids=[(100.0, 5.0), (99.0, 10.0)],
            asks=[(101.0, 5.0)],
        ))
        order = await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="sell", size_usd=600.0, client_order_id="x2",
        )
        # 500 at 100 + 100 at 99 -> VWAP = 600 / (5 + 100/99)
        units = 5.0 + (100.0 / 99.0)
        expected_vwap = 600.0 / units
        assert order.filled_price == pytest.approx(expected_vwap, rel=1e-6)

    @pytest.mark.asyncio
    async def test_insufficient_depth_raises(self):
        sim = make_sim()
        sim.set_book("mexc", "BTC/USDT:USDT", book(
            bids=[(99.0, 100.0)],
            asks=[(100.0, 1.0)],  # only 100 USD of asks
        ))
        with pytest.raises(InsufficientLiquidity):
            await sim.create_order(
                exchange="mexc", symbol="BTC/USDT:USDT",
                side="buy", size_usd=500.0, client_order_id="x3",
            )

    @pytest.mark.asyncio
    async def test_no_book_raises(self):
        sim = make_sim()
        with pytest.raises(InsufficientLiquidity):
            await sim.create_order(
                exchange="mexc", symbol="BTC/USDT:USDT",
                side="buy", size_usd=500.0, client_order_id="x4",
            )


class TestFeesAndBalances:
    @pytest.mark.asyncio
    async def test_taker_fee_applied(self):
        sim = make_sim(taker_fees_bps={"mexc": 2.0, "binance": 5.0})
        sim.set_book("mexc", "BTC/USDT:USDT", book(
            bids=[(99.0, 100.0)], asks=[(100.0, 100.0)],
        ))
        order = await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="buy", size_usd=1000.0, client_order_id="f1",
        )
        # 2 bps on $1000 = $0.20.
        assert order.fee_paid == pytest.approx(0.20)

    @pytest.mark.asyncio
    async def test_balance_decrements_on_buy(self):
        sim = make_sim(starting_balances={"mexc": 1000.0})
        sim.set_book("mexc", "BTC/USDT:USDT", book(
            bids=[(99.0, 100.0)], asks=[(100.0, 100.0)],
        ))
        await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="buy", size_usd=500.0, client_order_id="b1",
        )
        # 500 + 2bps fee = 500.10
        assert sim.simulated_balance["mexc"] == pytest.approx(499.90)

    @pytest.mark.asyncio
    async def test_balance_credits_on_sell(self):
        sim = make_sim(starting_balances={"mexc": 1000.0})
        sim.set_book("mexc", "BTC/USDT:USDT", book(
            bids=[(100.0, 100.0)], asks=[(101.0, 100.0)],
        ))
        await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="sell", size_usd=500.0, client_order_id="s1",
        )
        # +500 - 0.10 fee
        assert sim.simulated_balance["mexc"] == pytest.approx(1499.90)


class TestLedgerAndIdempotency:
    @pytest.mark.asyncio
    async def test_fetch_order_returns_filled_order(self):
        sim = make_sim()
        sim.set_book("mexc", "BTC/USDT:USDT", book(
            bids=[(99.0, 100.0)], asks=[(100.0, 100.0)],
        ))
        order = await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="buy", size_usd=100.0, client_order_id="L1",
        )
        fetched = await sim.fetch_order("L1")
        assert fetched is order

    @pytest.mark.asyncio
    async def test_fetch_unknown_order_returns_none(self):
        sim = make_sim()
        assert await sim.fetch_order("never-existed") is None

    @pytest.mark.asyncio
    async def test_duplicate_client_order_id_is_idempotent(self):
        sim = make_sim()
        sim.set_book("mexc", "BTC/USDT:USDT", book(
            bids=[(99.0, 100.0)], asks=[(100.0, 100.0)],
        ))
        first = await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="buy", size_usd=100.0, client_order_id="dup",
        )
        second = await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="buy", size_usd=100.0, client_order_id="dup",
        )
        # Same client_order_id -> same order returned, balance not decremented twice.
        assert second is first


class TestSimulatedFailure:
    @pytest.mark.asyncio
    async def test_failure_rate_one_always_fails(self):
        sim = make_sim(sim_leg_failure_rate=1.0)
        sim.set_book("mexc", "BTC/USDT:USDT", book(
            bids=[(99.0, 100.0)], asks=[(100.0, 100.0)],
        ))
        with pytest.raises(SimulatedFailure):
            await sim.create_order(
                exchange="mexc", symbol="BTC/USDT:USDT",
                side="buy", size_usd=100.0, client_order_id="F1",
            )

    @pytest.mark.asyncio
    async def test_failure_rate_zero_never_fails(self):
        sim = make_sim(sim_leg_failure_rate=0.0)
        sim.set_book("mexc", "BTC/USDT:USDT", book(
            bids=[(99.0, 100.0)], asks=[(100.0, 100.0)],
        ))
        for i in range(20):
            order = await sim.create_order(
                exchange="mexc", symbol="BTC/USDT:USDT",
                side="buy", size_usd=10.0, client_order_id=f"OK{i}",
            )
            assert order.state == "filled"

    @pytest.mark.asyncio
    async def test_failed_order_does_not_decrement_balance(self):
        sim = make_sim(
            starting_balances={"mexc": 1000.0},
            sim_leg_failure_rate=1.0,
        )
        sim.set_book("mexc", "BTC/USDT:USDT", book(
            bids=[(99.0, 100.0)], asks=[(100.0, 100.0)],
        ))
        with pytest.raises(SimulatedFailure):
            await sim.create_order(
                exchange="mexc", symbol="BTC/USDT:USDT",
                side="buy", size_usd=500.0, client_order_id="N1",
            )
        assert sim.simulated_balance["mexc"] == 1000.0
