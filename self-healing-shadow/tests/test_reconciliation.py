"""Tests for heal.reconciliation."""

from datetime import datetime, timezone

import pytest

from core.execution_sim import ExecutionSim, SimOrder, SimulatedFailure
from core.orderbook import OrderBook
from core.trade_journal import TradeJournal
from heal.reconciliation import (
    Diagnosis,
    ReconciliationSaga,
    classify_exception,
)


def book() -> OrderBook:
    return OrderBook(
        bids=[(99.0, 100.0)],
        asks=[(100.0, 100.0)],
        ts=datetime.now(timezone.utc),
    )


@pytest.fixture
def journal(tmp_path):
    j = TradeJournal(tmp_path / "j.sqlite")
    yield j
    j.close()


@pytest.fixture
def sim():
    s = ExecutionSim(
        starting_balances={"mexc": 1000.0, "binance": 1000.0},
        taker_fees_bps={"mexc": 2.0, "binance": 5.0},
    )
    s.set_book("mexc", "BTC/USDT:USDT", book())
    s.set_book("binance", "BTC/USDT:USDT", book())
    return s


class TestClassifyException:
    def test_classifies_simulated_failure_as_network(self):
        assert classify_exception(SimulatedFailure("x")) is Diagnosis.NETWORK_ERROR

    def test_classifies_value_error_with_balance_text(self):
        assert (
            classify_exception(ValueError("InsufficientBalance for mexc"))
            is Diagnosis.INSUFFICIENT_BALANCE
        )

    def test_classifies_rate_limit(self):
        assert (
            classify_exception(Exception("RateLimitExceeded: 429"))
            is Diagnosis.RATE_LIMIT
        )

    def test_unknown_falls_through(self):
        assert classify_exception(RuntimeError("???")) is Diagnosis.UNKNOWN


class TestSaga:
    @pytest.mark.asyncio
    async def test_both_filled_returns_success(self, sim, journal):
        a = await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="buy", size_usd=100.0, client_order_id="A",
        )
        b = await sim.create_order(
            exchange="binance", symbol="BTC/USDT:USDT",
            side="sell", size_usd=100.0, client_order_id="B",
        )
        saga = ReconciliationSaga(sim=sim, journal=journal)
        result = await saga.run(
            position_id="t-1", leg_a=a, leg_b=b,
            client_order_id_a="A", client_order_id_b="B",
        )
        # Both legs filled is not actually a saga case — caller skips
        # the saga in that path — but defensive: should still return ok.
        assert result.both_filled is True
        assert result.diagnosis is None

    @pytest.mark.asyncio
    async def test_failed_leg_triggers_rollback(self, sim, journal):
        # Leg A filled, leg B raised SimulatedFailure.
        a = await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="buy", size_usd=100.0, client_order_id="A",
        )
        leg_b_exc = SimulatedFailure("network down")

        saga = ReconciliationSaga(sim=sim, journal=journal)
        result = await saga.run(
            position_id="t-1", leg_a=a, leg_b=leg_b_exc,
            client_order_id_a="A", client_order_id_b="B-never-placed",
        )
        assert result.both_filled is False
        assert result.diagnosis is Diagnosis.NETWORK_ERROR
        assert result.rolled_back_leg == "A"

    @pytest.mark.asyncio
    async def test_verify_step_finds_actually_filled_order(self, sim, journal):
        # Both legs filled but B's response was lost — saga's Verify
        # step should find it via fetch_order and NOT rollback.
        a = await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="buy", size_usd=100.0, client_order_id="A",
        )
        b = await sim.create_order(
            exchange="binance", symbol="BTC/USDT:USDT",
            side="sell", size_usd=100.0, client_order_id="B",
        )
        # Caller saw an exception (lost response) but the order is in
        # the ledger. Saga should detect and skip rollback.
        leg_b_exc = SimulatedFailure("response lost")

        saga = ReconciliationSaga(sim=sim, journal=journal)
        result = await saga.run(
            position_id="t-1", leg_a=a, leg_b=leg_b_exc,
            client_order_id_a="A", client_order_id_b="B",
        )
        assert result.both_filled is True
        assert result.rolled_back_leg is None

    @pytest.mark.asyncio
    async def test_saga_logs_each_step(self, sim, journal):
        a = await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="buy", size_usd=100.0, client_order_id="A",
        )
        leg_b_exc = SimulatedFailure("x")
        saga = ReconciliationSaga(sim=sim, journal=journal)
        await saga.run(
            position_id="t-1", leg_a=a, leg_b=leg_b_exc,
            client_order_id_a="A", client_order_id_b="B",
        )
        journal.flush()
        rows = journal.conn.execute(
            "SELECT event_type FROM journal WHERE event_type='SAGA_STEP'"
        ).fetchall()
        # Detect, Verify, Rollback, Diagnose, Heal -> at least 5 SAGA_STEP rows.
        assert len(rows) >= 5

    @pytest.mark.asyncio
    async def test_rollback_closes_filled_leg(self, sim, journal):
        a = await sim.create_order(
            exchange="mexc", symbol="BTC/USDT:USDT",
            side="buy", size_usd=100.0, client_order_id="A",
        )
        balance_before_rollback = sim.simulated_balance["mexc"]
        leg_b_exc = SimulatedFailure("x")

        saga = ReconciliationSaga(sim=sim, journal=journal)
        await saga.run(
            position_id="t-1", leg_a=a, leg_b=leg_b_exc,
            client_order_id_a="A", client_order_id_b="B",
        )
        # Rollback was a SELL on mexc -> balance increased back ~ to start.
        assert sim.simulated_balance["mexc"] > balance_before_rollback


class TestQuarantineCounter:
    @pytest.mark.asyncio
    async def test_consecutive_network_failures_trigger_quarantine(self, sim, journal):
        saga = ReconciliationSaga(
            sim=sim, journal=journal, quarantine_after=3,
        )
        for i in range(3):
            a = await sim.create_order(
                exchange="mexc", symbol="BTC/USDT:USDT",
                side="buy", size_usd=10.0, client_order_id=f"A{i}",
            )
            await saga.run(
                position_id=f"t-{i}",
                leg_a=a, leg_b=SimulatedFailure("net"),
                client_order_id_a=f"A{i}", client_order_id_b=f"B{i}",
            )
        assert saga.exchange_quarantined("binance")

    @pytest.mark.asyncio
    async def test_success_resets_failure_counter(self, sim, journal):
        saga = ReconciliationSaga(
            sim=sim, journal=journal, quarantine_after=3,
        )
        # Two failures.
        for i in range(2):
            a = await sim.create_order(
                exchange="mexc", symbol="BTC/USDT:USDT",
                side="buy", size_usd=10.0, client_order_id=f"A{i}",
            )
            await saga.run(
                position_id=f"t-{i}",
                leg_a=a, leg_b=SimulatedFailure("net"),
                client_order_id_a=f"A{i}", client_order_id_b=f"B{i}",
            )
        # One success on the suspect exchange.
        saga.note_success("binance")
        # Two more failures -> still under threshold (3).
        for i in range(2, 4):
            a = await sim.create_order(
                exchange="mexc", symbol="BTC/USDT:USDT",
                side="buy", size_usd=10.0, client_order_id=f"A{i}",
            )
            await saga.run(
                position_id=f"t-{i}",
                leg_a=a, leg_b=SimulatedFailure("net"),
                client_order_id_a=f"A{i}", client_order_id_b=f"B{i}",
            )
        assert not saga.exchange_quarantined("binance")
