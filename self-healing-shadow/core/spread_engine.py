"""Spread, log-spread, and rolling Z-score per exchange pair.

Pure computation — no I/O, no async. The engine maintains an O(window)
deque per pair; Z-score uses population standard deviation (ddof=0)
because the rolling window IS the population we care about.
"""

from __future__ import annotations

import math
from collections import deque
from typing import Deque, Optional

import numpy as np


def mid_price(*, bid: float, ask: float) -> float | None:
    """Mean of best bid and best ask.

    Returns ``None`` on a crossed book (bid > ask). Crossings happen in
    practice when an exchange WS sends an incremental delta whose merge
    transiently inverts the book — a data condition, not a caller bug,
    so we skip the tick rather than blow up the loop.
    """
    if bid > ask:
        return None
    return (bid + ask) / 2.0


def log_spread(*, price_a: float, price_b: float) -> float:
    """ln(price_a) - ln(price_b). Variance-stabilising spread per PDF1 §1.1."""
    return math.log(price_a) - math.log(price_b)


def _canonical_pair(pair: tuple[str, str]) -> tuple[str, str]:
    """Pair identity is order-independent: ('A','B') == ('B','A')."""
    a, b = pair
    return (a, b) if a <= b else (b, a)


class SpreadEngine:
    """Rolling-window Z-score engine, one window per canonical pair."""

    def __init__(self, lookback_window: int) -> None:
        if lookback_window < 2:
            raise ValueError("lookback_window must be >= 2")
        self.lookback_window = lookback_window
        self._windows: dict[tuple[str, str], Deque[float]] = {}

    def update(self, *, pair: tuple[str, str], spread: float) -> None:
        key = _canonical_pair(pair)
        window = self._windows.get(key)
        if window is None:
            window = deque(maxlen=self.lookback_window)
            self._windows[key] = window
        window.append(spread)

    def zscore(self, pair: tuple[str, str]) -> Optional[float]:
        """Return the Z-score of the most recent spread for `pair`.

        None if the window has not yet filled. Returns 0.0 (not NaN) when
        the standard deviation is zero, so callers do not have to special-case.
        """
        key = _canonical_pair(pair)
        window = self._windows.get(key)
        if window is None or len(window) < self.lookback_window:
            return None
        arr = np.asarray(window, dtype=float)
        mean = arr.mean()
        std = arr.std(ddof=0)
        if std == 0.0:
            return 0.0
        return float((arr[-1] - mean) / std)
