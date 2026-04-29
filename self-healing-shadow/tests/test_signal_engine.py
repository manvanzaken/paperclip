"""Tests for core.signal_engine."""

from datetime import datetime, timezone

import pytest

from core.orderbook import OrderBook
from core.signal_engine import EntrySignal, ExitSignal, SignalEngine
from core.trade_journal import TradeJournal
from heal.heartbeat import HeartbeatMonitor
from heal.hurst_canary import HurstCanary


def book(mid: float, depth: float = 1000.0) -> OrderBook:
    return OrderBook(
        bids=[(mid - 0.5, depth)],
        asks=[(mid + 0.5, depth)],
        ts=datetime.now(timezone.utc),
    )


@pytest.fixture
def journal(tmp_path):
    j = TradeJournal(tmp_path / "j.sqlite")
    yield j
    j.close()


def make_engine(journal, **overrides):
    hb = HeartbeatMonitor(stale_threshold_sec=60.0)
    hb.on_ws_message("a")
    hb.on_ws_message("b")
    canary = HurstCanary(window=200, threshold=0.5)
    cfg = dict(
        entry_z=2.0,
        exit_z=0.5,
        stop_loss_z=4.0,
        min_net_profit_usd=5.0,
        max_position_usd=1000.0,
        taker_fees_bps={"a": 2.0, "b": 5.0},
    )
    cfg.update(overrides)
    return SignalEngine(
        heartbeat=hb,
        canary=canary,
        journal=journal,
        **cfg,
    )


class TestEntryGating:
    def test_no_signal_below_entry_z(self, journal):
        eng = make_engine(journal)
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=1.5,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert sig is None

    def test_entry_signal_above_threshold(self, journal):
        eng = make_engine(journal)
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=2.5,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert isinstance(sig, EntrySignal)
        # Z is positive -> short on A, long on B (PDF1 §1.2 entry table).
        assert sig.side_a == "sell"
        assert sig.side_b == "buy"

    def test_negative_z_inverts_sides(self, journal):
        eng = make_engine(journal)
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=-2.5,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert isinstance(sig, EntrySignal)
        assert sig.side_a == "buy"
        assert sig.side_b == "sell"

    def test_blocked_when_exchange_degraded(self, journal):
        eng = make_engine(journal)
        # Force B degraded.
        eng.heartbeat.exchange_status["b"] = "DEGRADED"
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=2.5,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert sig is None
        # Aborted entry should be journaled.
        journal.flush()
        assert journal.count_aborts(reason="heartbeat_degraded") == 1

    def test_blocked_when_canary_unsafe(self, journal):
        eng = make_engine(journal)
        # Feed a strong trend so the canary marks the pair unsafe.
        for i in range(400):
            eng.canary.update(("a", "b"), float(i))
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=2.5,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert sig is None
        journal.flush()
        assert journal.count_aborts(reason="hurst_unsafe") == 1


class TestProfitabilityGate:
    def test_blocked_when_net_profit_below_threshold(self, journal):
        # Fees alone make the trade losing.
        eng = make_engine(
            journal,
            min_net_profit_usd=999.0,  # impossibly high
            max_position_usd=100.0,
        )
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=2.5,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert sig is None
        journal.flush()
        assert journal.count_aborts(reason="profit_below_threshold") == 1


class TestExitSignals:
    def test_exit_when_z_back_to_threshold(self, journal):
        eng = make_engine(journal)
        sig = eng.evaluate_exit(z_score=0.3, stop_loss_hit=False)
        assert isinstance(sig, ExitSignal)
        assert sig.reason == "convergence"

    def test_no_exit_when_z_still_wide(self, journal):
        eng = make_engine(journal)
        sig = eng.evaluate_exit(z_score=1.5, stop_loss_hit=False)
        assert sig is None

    def test_emergency_exit_at_stop_loss(self, journal):
        eng = make_engine(journal)
        sig = eng.evaluate_exit(z_score=4.5, stop_loss_hit=True)
        assert isinstance(sig, ExitSignal)
        assert sig.reason == "stop_loss"
