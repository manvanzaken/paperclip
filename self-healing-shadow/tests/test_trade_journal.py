"""Tests for core.trade_journal."""

from datetime import datetime, timedelta, timezone

import pytest
from freezegun import freeze_time

from core.trade_journal import TradeJournal


@pytest.fixture
def journal(tmp_path):
    j = TradeJournal(tmp_path / "j.sqlite")
    yield j
    j.close()


class TestSchema:
    def test_init_creates_journal_table(self, journal):
        cur = journal.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='journal'"
        )
        assert cur.fetchone() is not None


class TestLogAndFlush:
    def test_log_and_flush_persists(self, journal):
        journal.log(
            event_type="ENTRY_SIGNAL",
            payload={"foo": "bar"},
            pair="mexc_binance",
            symbol="BTC/USDT:USDT",
            z_score=2.5,
            spread_bps=8.0,
            expected_pnl_usd=1.5,
        )
        journal.flush()
        rows = journal.conn.execute(
            "SELECT event_type, pair, z_score FROM journal"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "ENTRY_SIGNAL"
        assert rows[0][1] == "mexc_binance"
        assert rows[0][2] == 2.5

    def test_log_payload_round_trips_as_json(self, journal):
        payload = {"nested": {"a": [1, 2, 3]}, "x": None}
        journal.log(event_type="ERROR", payload=payload)
        journal.flush()
        cur = journal.conn.execute("SELECT payload_json FROM journal")
        (raw,) = cur.fetchone()
        import json
        assert json.loads(raw) == payload

    def test_aborted_entries_are_logged(self, journal):
        journal.log(
            event_type="ENTRY_ABORTED",
            payload={"reason": "profit_below_threshold"},
            pair="mexc_binance",
        )
        journal.flush()
        cnt = journal.count_aborts(reason="profit_below_threshold")
        assert cnt == 1

    def test_flush_is_no_op_when_queue_empty(self, journal):
        journal.flush()  # should not raise
        rows = journal.conn.execute("SELECT COUNT(*) FROM journal").fetchone()
        assert rows[0] == 0


class TestQueries:
    def test_last_successful_trade_returns_none_initially(self, journal):
        assert journal.last_successful_trade() is None

    def test_last_successful_trade_returns_most_recent_fill(self, journal):
        with freeze_time("2026-04-29 10:00:00"):
            journal.log(event_type="FILL", payload={})
            journal.flush()
        with freeze_time("2026-04-29 11:00:00"):
            journal.log(event_type="FILL", payload={})
            journal.flush()
        ts = journal.last_successful_trade()
        assert ts is not None
        assert ts.hour == 11

    def test_count_errors_in_window(self, journal):
        with freeze_time("2026-04-29 10:00:00") as f:
            for _ in range(3):
                journal.log(event_type="ERROR", payload={})
            journal.flush()
            f.tick(timedelta(minutes=30))
            assert journal.count_errors(since=timedelta(hours=1)) == 3
            f.tick(timedelta(hours=1))
            # Now those errors are >1h old; window of 1h returns 0.
            assert journal.count_errors(since=timedelta(hours=1)) == 0

    def test_count_aborts_filters_by_reason(self, journal):
        journal.log(event_type="ENTRY_ABORTED", payload={"reason": "fees"})
        journal.log(event_type="ENTRY_ABORTED", payload={"reason": "depth"})
        journal.log(event_type="ENTRY_ABORTED", payload={"reason": "fees"})
        journal.flush()
        assert journal.count_aborts(reason="fees") == 2
        assert journal.count_aborts(reason="depth") == 1
        assert journal.count_aborts() == 3
