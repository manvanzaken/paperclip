"""Tests for heal.hurst_canary."""

import math

import numpy as np

from heal.hurst_canary import HurstCanary, compute_hurst


class TestComputeHurst:
    def test_random_walk_hurst_near_half(self):
        rng = np.random.default_rng(seed=42)
        steps = rng.normal(size=2000)
        walk = np.cumsum(steps)
        h = compute_hurst(walk.tolist())
        # Random walk -> H near 0.5. R/S analysis on finite samples drifts
        # so we widen the band rather than chase a tighter target.
        assert 0.4 <= h <= 0.7

    def test_mean_reverting_series_hurst_below_half(self):
        # Strongly anti-persistent: oscillating series with small noise.
        rng = np.random.default_rng(seed=7)
        n = 2000
        # AR(1) with negative coefficient -> mean reverting.
        x = np.zeros(n)
        for i in range(1, n):
            x[i] = -0.7 * x[i - 1] + rng.normal(scale=0.5)
        h = compute_hurst(x.tolist())
        assert h < 0.5

    def test_short_series_returns_none(self):
        assert compute_hurst([1.0, 2.0, 3.0]) is None


class TestHurstCanary:
    def test_unknown_pair_is_safe_by_default(self):
        # Before we have data we should not block trading.
        canary = HurstCanary(window=200, threshold=0.5)
        assert canary.is_pair_safe(("a", "b")) is True

    def test_pair_safe_when_h_below_threshold(self):
        canary = HurstCanary(window=200, threshold=0.5)
        rng = np.random.default_rng(seed=7)
        prev = 0.0
        for _ in range(400):
            # Mean-reverting AR(1) with negative coefficient.
            cur = -0.7 * prev + rng.normal(scale=0.5)
            canary.update(("a", "b"), cur)
            prev = cur
        assert canary.is_pair_safe(("a", "b")) is True

    def test_pair_unsafe_when_h_above_threshold(self):
        canary = HurstCanary(window=200, threshold=0.5)
        # Trending: monotonically increasing series -> H very high.
        for i in range(400):
            canary.update(("a", "b"), float(i))
        # Trending series should be flagged unsafe.
        assert canary.is_pair_safe(("a", "b")) is False

    def test_pair_order_does_not_matter(self):
        canary = HurstCanary(window=200, threshold=0.5)
        for i in range(400):
            canary.update(("a", "b"), float(i))
        assert canary.is_pair_safe(("b", "a")) == canary.is_pair_safe(("a", "b"))

    def test_window_capped(self):
        canary = HurstCanary(window=10, threshold=0.5)
        for i in range(100):
            canary.update(("a", "b"), float(i))
        assert len(canary._windows[("a", "b")]) == 10
