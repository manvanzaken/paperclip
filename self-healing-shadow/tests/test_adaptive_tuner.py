"""Tests for heal.adaptive_tuner."""

from datetime import timedelta

import pytest
import yaml
from freezegun import freeze_time

from core.trade_journal import TradeJournal
from heal.adaptive_tuner import AdaptiveTuner, GUARDRAILS, NudgeResult


@pytest.fixture
def journal(tmp_path):
    j = TradeJournal(tmp_path / "j.sqlite")
    yield j
    j.close()


def make_tuner(tmp_path, journal, *, base_cfg=None) -> AdaptiveTuner:
    base = base_cfg or {
        "strategy": {
            "entry_z": 2.0,
            "min_net_profit_usd": 0.50,
        },
        "exchanges": {
            "mexc":   {"max_position_usd": 1000},
            "binance": {"max_position_usd": 1000},
            "bybit":  {"max_position_usd": 1000},
        },
    }
    return AdaptiveTuner(
        journal=journal,
        base_config=base,
        overrides_path=tmp_path / "overrides.yaml",
    )


def log_aborts(journal, n: int, *, reason: str = "profit_below_threshold"):
    for _ in range(n):
        journal.log(event_type="ENTRY_ABORTED", payload={"reason": reason})


def log_fills(journal, n: int):
    for _ in range(n):
        journal.log(event_type="FILL", payload={"position_id": "x"})


def log_signals(journal, n: int):
    """Signals are the *denominator* of abort-rate calculations."""
    for _ in range(n):
        journal.log(event_type="ENTRY_SIGNAL", payload={})


def log_saga_failures(journal, n: int, *, exchange: str):
    for _ in range(n):
        journal.log(
            event_type="SAGA_STEP",
            payload={"step": "Heal", "action": "MARK_DEGRADED", "exchange": exchange},
        )


def log_saga_success(journal, n: int, *, exchange: str):
    for _ in range(n):
        journal.log(
            event_type="FILL",
            payload={"position_id": "x", "exchange_a": exchange},
            pair=f"{exchange}_other",
        )


class TestNoOpWhenHealthy:
    @pytest.mark.asyncio
    async def test_no_overrides_written_when_no_rule_fires(self, tmp_path, journal):
        log_signals(journal, 100)
        log_aborts(journal, 30)  # 30% abort rate, normal
        log_fills(journal, 5)
        journal.flush()
        tuner = make_tuner(tmp_path, journal)
        result = await tuner.run_once()
        assert result.changes == {}
        # No overrides file should be written (or it should be empty).
        path = tuner.overrides_path
        if path.exists():
            assert (yaml.safe_load(path.read_text()) or {}) == {}


class TestRule1ProfitGateLowering:
    @pytest.mark.asyncio
    async def test_lowers_min_net_profit_when_almost_all_aborts_are_profit(
        self, tmp_path, journal,
    ):
        log_signals(journal, 100)
        log_aborts(journal, 95, reason="profit_below_threshold")
        journal.flush()
        tuner = make_tuner(tmp_path, journal)
        result = await tuner.run_once()
        # Old=0.50; step = 0.05 (since old < 0.10 trigger); new = 0.45.
        assert result.changes["strategy.min_net_profit_usd"] == pytest.approx(0.45)

    @pytest.mark.asyncio
    async def test_guardrail_min_floor_respected(self, tmp_path, journal):
        log_signals(journal, 100)
        log_aborts(journal, 95, reason="profit_below_threshold")
        journal.flush()
        # Start already at the floor.
        tuner = make_tuner(
            tmp_path, journal,
            base_cfg={
                "strategy": {
                    "entry_z": 2.0,
                    "min_net_profit_usd": GUARDRAILS["min_net_profit_usd"][0],
                },
                "exchanges": {},
            },
        )
        result = await tuner.run_once()
        assert "strategy.min_net_profit_usd" not in result.changes


class TestRule2EntryZLowering:
    @pytest.mark.asyncio
    async def test_lowers_entry_z_when_no_fills_and_no_hurst_block(
        self, tmp_path, journal,
    ):
        log_signals(journal, 100)
        # No fills.
        log_aborts(journal, 40, reason="profit_below_threshold")
        # No hurst aborts.
        journal.flush()
        tuner = make_tuner(tmp_path, journal)
        result = await tuner.run_once()
        assert result.changes["strategy.entry_z"] == pytest.approx(1.9)

    @pytest.mark.asyncio
    async def test_does_not_lower_entry_z_when_hurst_blocking_a_lot(
        self, tmp_path, journal,
    ):
        log_signals(journal, 100)
        log_aborts(journal, 50, reason="hurst_unsafe")
        journal.flush()
        tuner = make_tuner(tmp_path, journal)
        result = await tuner.run_once()
        # Rule 2 must not lower z below 2.0; rule 4 may raise it.
        new_z = result.changes.get("strategy.entry_z", 2.0)
        assert new_z >= 2.0


class TestRule3EntryZRaising:
    @pytest.mark.asyncio
    async def test_raises_entry_z_when_too_many_fills(self, tmp_path, journal):
        log_signals(journal, 100)
        log_fills(journal, 25)
        journal.flush()
        tuner = make_tuner(tmp_path, journal)
        result = await tuner.run_once()
        assert result.changes["strategy.entry_z"] == pytest.approx(2.1)


class TestRule4HurstAbortsRaiseZ:
    @pytest.mark.asyncio
    async def test_raises_entry_z_when_hurst_aborts_dominate(self, tmp_path, journal):
        log_signals(journal, 100)
        log_aborts(journal, 35, reason="hurst_unsafe")
        # Avoid rule 2 firing (no fills, low hurst): need fills > 0 OR hurst high
        log_fills(journal, 1)
        journal.flush()
        tuner = make_tuner(tmp_path, journal)
        result = await tuner.run_once()
        assert result.changes["strategy.entry_z"] == pytest.approx(2.1)


class TestRule5ShrinkBadExchange:
    @pytest.mark.asyncio
    async def test_shrinks_position_size_when_exchange_has_many_failures(
        self, tmp_path, journal,
    ):
        log_signals(journal, 100)
        log_fills(journal, 5)
        log_saga_failures(journal, 4, exchange="bybit")
        journal.flush()
        tuner = make_tuner(tmp_path, journal)
        result = await tuner.run_once()
        # 1000 * 0.5 = 500
        assert result.changes["exchanges.bybit.max_position_usd"] == pytest.approx(500.0)

    @pytest.mark.asyncio
    async def test_floor_respected_for_exchange_size(self, tmp_path, journal):
        log_signals(journal, 100)
        log_fills(journal, 5)
        log_saga_failures(journal, 4, exchange="bybit")
        journal.flush()
        floor = GUARDRAILS["exchange_max_position_usd"][0]
        tuner = make_tuner(
            tmp_path, journal,
            base_cfg={
                "strategy": {"entry_z": 2.0, "min_net_profit_usd": 0.5},
                "exchanges": {"bybit": {"max_position_usd": floor}},
            },
        )
        result = await tuner.run_once()
        # Already at floor -> no change.
        assert "exchanges.bybit.max_position_usd" not in result.changes


class TestPersistence:
    @pytest.mark.asyncio
    async def test_writes_overrides_atomically(self, tmp_path, journal):
        log_signals(journal, 100)
        log_aborts(journal, 95, reason="profit_below_threshold")
        journal.flush()
        tuner = make_tuner(tmp_path, journal)
        await tuner.run_once()
        path = tuner.overrides_path
        assert path.exists()
        loaded = yaml.safe_load(path.read_text())
        assert loaded["strategy"]["min_net_profit_usd"] == pytest.approx(0.45)

    @pytest.mark.asyncio
    async def test_subsequent_run_compounds_on_existing_overrides(
        self, tmp_path, journal,
    ):
        log_signals(journal, 100)
        log_aborts(journal, 95, reason="profit_below_threshold")
        journal.flush()
        tuner = make_tuner(tmp_path, journal)
        await tuner.run_once()      # 0.50 - 10% step (=0.05) -> 0.45
        await tuner.run_once()      # 0.45 - 10% step (=0.045) -> 0.405
        loaded = yaml.safe_load(tuner.overrides_path.read_text())
        assert loaded["strategy"]["min_net_profit_usd"] == pytest.approx(0.405)

    @pytest.mark.asyncio
    async def test_logs_param_nudge_event(self, tmp_path, journal):
        log_signals(journal, 100)
        log_aborts(journal, 95, reason="profit_below_threshold")
        journal.flush()
        tuner = make_tuner(tmp_path, journal)
        await tuner.run_once()
        journal.flush()
        rows = journal.conn.execute(
            "SELECT count(*) FROM journal WHERE event_type='PARAM_NUDGE'"
        ).fetchone()
        assert rows[0] >= 1
