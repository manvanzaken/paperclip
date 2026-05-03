"""Tests for core.signal_engine."""

from datetime import datetime, timezone

import pytest

from core.orderbook import OrderBook
from core.signal_engine import EntrySignal, ExitSignal, SignalEngine
from core.spread_engine import SpreadEngine
from core.trade_journal import TradeJournal
from heal.heartbeat import HeartbeatMonitor
from heal.hurst_canary import HurstCanary


def book(mid: float, depth: float = 1000.0, spread_bps: float = 2.0) -> OrderBook:
    half = mid * spread_bps / 10_000.0 / 2.0
    return OrderBook(
        bids=[(mid - half, depth)],
        asks=[(mid + half, depth)],
        ts=datetime.now(timezone.utc),
    )


@pytest.fixture
def journal(tmp_path):
    j = TradeJournal(tmp_path / "j.sqlite")
    yield j
    j.close()


def primed_spread_engine(*, mean_log: float = 0.0, lookback: int = 5) -> SpreadEngine:
    """A SpreadEngine pre-filled so .mean() returns ~mean_log."""
    eng = SpreadEngine(lookback_window=lookback)
    for _ in range(lookback):
        eng.update(pair=("a", "b"), spread=mean_log)
    return eng


def make_engine(journal, *, spread_engine=None, is_quarantined=None, **overrides):
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
        recovery_fraction=0.5,
    )
    cfg.update(overrides)
    return SignalEngine(
        heartbeat=hb,
        canary=canary,
        journal=journal,
        spread_engine=spread_engine or primed_spread_engine(mean_log=0.0),
        is_quarantined=is_quarantined,
        **cfg,
    )


class TestEntryGating:
    def test_no_signal_below_entry_z(self, journal):
        eng = make_engine(journal)
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=1.5,
            current_spread_log=0.005,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert sig is None

    def test_entry_signal_above_threshold(self, journal):
        # excess = 0.005 - 0 = 0.005 = 50bps; recovery 0.5 -> 25bps gross.
        # On $1000: gross = $2.50; half-spreads ~1bp each = $0.20; fees 2*(2+5)=14bps=$1.40
        # net = 2.50 - 0.20 - 1.40 = $0.90  -> below default min_net_profit_usd=5
        # So lower threshold for this test:
        eng = make_engine(journal, min_net_profit_usd=0.5)
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=2.5,
            current_spread_log=0.005,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert isinstance(sig, EntrySignal)
        assert sig.side_a == "sell"
        assert sig.side_b == "buy"

    def test_negative_z_inverts_sides(self, journal):
        eng = make_engine(journal, min_net_profit_usd=0.5)
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=-2.5,
            current_spread_log=-0.005,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert isinstance(sig, EntrySignal)
        assert sig.side_a == "buy"
        assert sig.side_b == "sell"

    def test_blocked_when_exchange_degraded(self, journal):
        eng = make_engine(journal, min_net_profit_usd=0.5)
        eng.heartbeat.exchange_status["b"] = "DEGRADED"
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=2.5,
            current_spread_log=0.005,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert sig is None
        journal.flush()
        assert journal.count_aborts(reason="heartbeat_degraded") == 1

    def test_blocked_when_canary_unsafe(self, journal):
        eng = make_engine(journal, min_net_profit_usd=0.5)
        for i in range(400):
            eng.canary.update(("a", "b"), float(i))
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=2.5,
            current_spread_log=0.005,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert sig is None
        journal.flush()
        assert journal.count_aborts(reason="hurst_unsafe") == 1

    def test_blocked_when_exchange_quarantined(self, journal):
        eng = make_engine(
            journal,
            min_net_profit_usd=0.5,
            is_quarantined=lambda ex: ex == "b",
        )
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=2.5,
            current_spread_log=0.005,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert sig is None
        journal.flush()
        assert journal.count_aborts(reason="exchange_quarantined") == 1

    def test_blocked_when_spread_window_not_full(self, journal):
        # SpreadEngine never updated -> mean is None -> evaluate returns None silently.
        empty_eng = SpreadEngine(lookback_window=200)
        eng = make_engine(journal, spread_engine=empty_eng, min_net_profit_usd=0.5)
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=2.5,
            current_spread_log=0.005,
            book_a=book(100.0),
            book_b=book(101.0),
        )
        assert sig is None


class TestProfitabilityGate:
    def test_blocked_when_net_profit_below_threshold(self, journal):
        eng = make_engine(
            journal,
            min_net_profit_usd=999.0,  # impossibly high
            max_position_usd=100.0,
        )
        sig = eng.evaluate(
            pair=("a", "b"),
            symbol="BTC/USDT:USDT",
            z_score=2.5,
            current_spread_log=0.005,
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
