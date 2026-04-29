"""Tests for heal.health_check."""

from datetime import timedelta

import pytest
from freezegun import freeze_time

from core.state_machine import Position, PositionState
from core.trade_journal import TradeJournal
from heal.health_check import TradingHealthCheck


@pytest.fixture
def journal(tmp_path):
    j = TradeJournal(tmp_path / "j.sqlite")
    yield j
    j.close()


def make_check(
    journal,
    *,
    balances: dict[str, float] | None = None,
    min_order_usd: float = 100.0,
    open_positions: list[Position] | None = None,
) -> TradingHealthCheck:
    return TradingHealthCheck(
        journal=journal,
        get_balances=lambda: balances or {"mexc": 1000.0, "binance": 1000.0},
        min_order_usd=min_order_usd,
        get_open_positions=lambda: open_positions or [],
        drought_threshold=timedelta(hours=72),
        error_rate_threshold=20,
        orphan_threshold=timedelta(seconds=60),
    )


class TestHealthy:
    @pytest.mark.asyncio
    async def test_clean_state_returns_continue(self, journal):
        with freeze_time("2026-04-29 10:00:00"):
            journal.log(event_type="FILL", payload={})
            journal.flush()
            check = make_check(journal)
            status = await check.run_check()
        assert status.is_healthy
        assert status.recommended_action == "CONTINUE"


class TestTradeDrought:
    @pytest.mark.asyncio
    async def test_no_fills_ever_does_not_trigger(self, journal):
        # Bot just started — no fills yet, but no drought either.
        check = make_check(journal)
        status = await check.run_check()
        assert status.is_healthy

    @pytest.mark.asyncio
    async def test_old_fill_triggers_drought(self, journal):
        with freeze_time("2026-04-26 10:00:00"):
            journal.log(event_type="FILL", payload={})
            journal.flush()
        with freeze_time("2026-04-29 11:00:00"):
            check = make_check(journal)
            status = await check.run_check()
        assert not status.is_healthy
        assert status.recommended_action == "REOPTIMIZE_PARAMETERS"


class TestErrorRate:
    @pytest.mark.asyncio
    async def test_high_error_rate_triggers_safe_mode(self, journal):
        with freeze_time("2026-04-29 10:00:00"):
            for _ in range(25):
                journal.log(event_type="ERROR", payload={})
            journal.log(event_type="FILL", payload={})  # avoid drought
            journal.flush()
            check = make_check(journal)
            status = await check.run_check()
        assert not status.is_healthy
        assert status.recommended_action == "ENTER_SAFE_MODE"


class TestCapitalDepletion:
    @pytest.mark.asyncio
    async def test_low_balance_triggers_force_exit(self, journal):
        with freeze_time("2026-04-29 10:00:00"):
            journal.log(event_type="FILL", payload={})  # avoid drought
            journal.flush()
            check = make_check(
                journal,
                balances={"mexc": 50.0, "binance": 1000.0},  # below 100
                min_order_usd=100.0,
            )
            status = await check.run_check()
        assert not status.is_healthy
        assert status.recommended_action == "FORCE_EXIT_OLDEST"


class TestOrphanedPositions:
    @pytest.mark.asyncio
    async def test_long_reconciling_triggers_immediate_recon(self, journal):
        with freeze_time("2026-04-29 10:00:00") as f:
            journal.log(event_type="FILL", payload={})
            journal.flush()
            p = Position(id="x", pair=("a", "b"), symbol="BTC/USDT:USDT")
            p.transition(PositionState.SIGNAL_DETECTED, reason="z>2")
            p.transition(PositionState.PROFITABILITY_CHECK, reason="next")
            p.transition(PositionState.EXECUTING, reason="ok")
            p.transition(PositionState.RECONCILING, reason="check")
            f.tick(timedelta(seconds=120))
            check = make_check(journal, open_positions=[p])
            status = await check.run_check()
        assert not status.is_healthy
        # Orphan check has highest priority among unhealthy actions.
        assert status.recommended_action == "RECONCILE_IMMEDIATELY"


class TestPriority:
    @pytest.mark.asyncio
    async def test_orphan_outranks_drought(self, journal):
        # Both drought AND orphan present; orphan should win.
        with freeze_time("2026-04-26 10:00:00"):
            journal.log(event_type="FILL", payload={})
            journal.flush()
        with freeze_time("2026-04-29 11:00:00") as f:
            p = Position(id="x", pair=("a", "b"), symbol="BTC/USDT:USDT")
            p.transition(PositionState.SIGNAL_DETECTED, reason="z>2")
            p.transition(PositionState.PROFITABILITY_CHECK, reason="next")
            p.transition(PositionState.EXECUTING, reason="ok")
            p.transition(PositionState.RECONCILING, reason="check")
            f.tick(timedelta(seconds=120))
            check = make_check(journal, open_positions=[p])
            status = await check.run_check()
        assert status.recommended_action == "RECONCILE_IMMEDIATELY"
