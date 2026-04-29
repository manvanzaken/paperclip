"""Tests for core.spread_engine."""

import math

import pytest

from core.spread_engine import SpreadEngine, mid_price, log_spread


class TestMidPrice:
    def test_mid_price_is_mean_of_bid_and_ask(self):
        assert mid_price(bid=100.0, ask=102.0) == 101.0

    def test_mid_price_with_tight_spread(self):
        assert mid_price(bid=67450.0, ask=67450.5) == pytest.approx(67450.25)

    def test_mid_price_returns_none_when_bid_above_ask(self):
        # Crossed books happen IRL during WS incremental-update glitches.
        assert mid_price(bid=100.0, ask=99.0) is None


class TestLogSpread:
    def test_log_spread_is_log_difference(self):
        # ln(101) - ln(100) ≈ 0.00995
        assert log_spread(price_a=101.0, price_b=100.0) == pytest.approx(
            math.log(101.0) - math.log(100.0)
        )

    def test_log_spread_is_zero_when_prices_equal(self):
        assert log_spread(price_a=100.0, price_b=100.0) == 0.0

    def test_log_spread_is_negative_when_a_below_b(self):
        assert log_spread(price_a=99.0, price_b=100.0) < 0


class TestSpreadEngine:
    def test_engine_returns_none_zscore_when_window_not_full(self):
        eng = SpreadEngine(lookback_window=10)
        eng.update(pair=("A", "B"), spread=0.001)
        assert eng.zscore(("A", "B")) is None

    def test_engine_zscore_zero_for_constant_series(self):
        eng = SpreadEngine(lookback_window=5)
        for _ in range(5):
            eng.update(pair=("A", "B"), spread=0.001)
        # Constant series -> stddev = 0; convention: return 0.0 not NaN/inf.
        assert eng.zscore(("A", "B")) == 0.0

    def test_engine_zscore_for_known_series(self):
        # Series 1,2,3,4,5: mean=3, std=sqrt(2)≈1.4142.
        # Latest value 5 -> Z = (5 - 3) / 1.4142 ≈ 1.4142.
        eng = SpreadEngine(lookback_window=5)
        for v in [1.0, 2.0, 3.0, 4.0, 5.0]:
            eng.update(pair=("A", "B"), spread=v)
        z = eng.zscore(("A", "B"))
        assert z == pytest.approx(math.sqrt(2.0), abs=1e-6)

    def test_engine_window_drops_old_values(self):
        # Once window is full, oldest values must be dropped.
        eng = SpreadEngine(lookback_window=3)
        for v in [100.0, 200.0, 300.0]:
            eng.update(pair=("A", "B"), spread=v)
        # Push 4 more values; oldest (100, 200, 300) should be gone.
        for v in [10.0, 10.0, 10.0]:
            eng.update(pair=("A", "B"), spread=v)
        # Now constant 10 -> Z = 0.
        assert eng.zscore(("A", "B")) == 0.0

    def test_engine_isolates_pairs(self):
        eng = SpreadEngine(lookback_window=3)
        for v in [1.0, 2.0, 3.0]:
            eng.update(pair=("A", "B"), spread=v)
        eng.update(pair=("C", "D"), spread=999.0)
        # Pair (A,B) should not see (C,D) values.
        z_ab = eng.zscore(("A", "B"))
        assert z_ab == pytest.approx(math.sqrt(3.0 / 2.0), abs=1e-6)  # mean=2, std=sqrt(2/3)*... actually compute below
        # mean=2, std=sqrt(((1-2)^2+(2-2)^2+(3-2)^2)/3)=sqrt(2/3); Z=(3-2)/sqrt(2/3)=sqrt(3/2)

    def test_engine_pair_order_does_not_matter(self):
        # ("A", "B") and ("B", "A") are the same pair (canonical ordering).
        eng = SpreadEngine(lookback_window=3)
        eng.update(pair=("A", "B"), spread=1.0)
        eng.update(pair=("B", "A"), spread=2.0)
        eng.update(pair=("A", "B"), spread=3.0)
        # All three values should land on the same pair.
        z = eng.zscore(("B", "A"))
        assert z is not None  # window full
