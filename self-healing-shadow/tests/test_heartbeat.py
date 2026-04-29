"""Tests for heal.heartbeat."""

from datetime import timedelta

from freezegun import freeze_time

from heal.heartbeat import HeartbeatMonitor


class TestStaleness:
    def test_unknown_exchange_is_not_healthy(self):
        hb = HeartbeatMonitor(stale_threshold_sec=10.0)
        # Never received a message -> not tradeable.
        assert hb.is_pair_tradeable("a", "b") is False

    def test_fresh_message_makes_exchange_healthy(self):
        hb = HeartbeatMonitor(stale_threshold_sec=10.0)
        with freeze_time("2026-04-29 10:00:00"):
            hb.on_ws_message("a")
            hb.on_ws_message("b")
            hb.evaluate_now()
        assert hb.exchange_status["a"] == "HEALTHY"
        assert hb.exchange_status["b"] == "HEALTHY"
        assert hb.is_pair_tradeable("a", "b") is True

    def test_stale_exchange_marked_degraded(self):
        hb = HeartbeatMonitor(stale_threshold_sec=10.0)
        with freeze_time("2026-04-29 10:00:00") as f:
            hb.on_ws_message("a")
            hb.on_ws_message("b")
            f.tick(timedelta(seconds=15))
            hb.evaluate_now()
            assert hb.exchange_status["a"] == "DEGRADED"
            assert hb.exchange_status["b"] == "DEGRADED"
            assert hb.is_pair_tradeable("a", "b") is False


class TestHealing:
    def test_new_message_after_degraded_heals(self):
        hb = HeartbeatMonitor(stale_threshold_sec=5.0)
        with freeze_time("2026-04-29 10:00:00") as f:
            hb.on_ws_message("a")
            f.tick(timedelta(seconds=10))
            hb.evaluate_now()
            assert hb.exchange_status["a"] == "DEGRADED"
            f.tick(timedelta(seconds=1))
            hb.on_ws_message("a")
            assert hb.exchange_status["a"] == "HEALTHY"


class TestPairGate:
    def test_pair_tradeable_only_when_both_healthy(self):
        hb = HeartbeatMonitor(stale_threshold_sec=10.0)
        with freeze_time("2026-04-29 10:00:00") as f:
            hb.on_ws_message("a")
            hb.on_ws_message("b")
            f.tick(timedelta(seconds=11))
            # `a` gets a fresh message; `b` is stale.
            hb.on_ws_message("a")
            hb.evaluate_now()
            assert hb.is_pair_tradeable("a", "b") is False

    def test_pair_tradeable_symmetric(self):
        hb = HeartbeatMonitor(stale_threshold_sec=10.0)
        with freeze_time("2026-04-29 10:00:00"):
            hb.on_ws_message("a")
            hb.on_ws_message("b")
            hb.evaluate_now()
            assert hb.is_pair_tradeable("a", "b") == hb.is_pair_tradeable("b", "a")
