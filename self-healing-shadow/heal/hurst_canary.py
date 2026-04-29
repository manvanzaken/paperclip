"""Rolling Hurst-exponent canary.

A spread that has historically been mean-reverting can become trending
when the market regime shifts. The Hurst exponent is a cheap indicator:
H < 0.5 → anti-persistent (mean-reverting, safe to trade convergence);
H ≈ 0.5 → random walk (neither); H > 0.5 → trending (dangerous).

The signal engine consults `is_pair_safe()` before emitting an entry
signal. Pairs without enough samples yet are treated as safe so the
bot is not paralysed at startup.
"""

from __future__ import annotations

from collections import deque
from typing import Deque, Optional

import numpy as np

try:
    from hurst import compute_Hc as _compute_Hc
except ImportError:  # pragma: no cover
    _compute_Hc = None


def _canonical_pair(pair: tuple[str, str]) -> tuple[str, str]:
    a, b = pair
    return (a, b) if a <= b else (b, a)


def compute_hurst(series: list[float]) -> Optional[float]:
    """Return Hurst exponent over R/S analysis. None if too few points."""
    if len(series) < 100:
        return None
    if _compute_Hc is None:
        return None
    arr = np.asarray(series, dtype=float)
    # Hurst library refuses constant series — guard.
    if arr.std() == 0.0:
        return 0.5
    # `kind="random_walk"` expects level data (e.g. a spread series), which
    # is what we feed in. Other kinds either expect returns ("change") or
    # require strictly positive values ("price").
    try:
        h, _c, _data = _compute_Hc(arr, kind="random_walk", simplified=True)
    except (FloatingPointError, ValueError):
        # Pathological series (e.g. perfect linear trend produces undefined
        # R/S ratios). Treat as "definitely not mean-reverting".
        return 1.0
    return float(h)


class HurstCanary:
    def __init__(self, *, window: int = 200, threshold: float = 0.5) -> None:
        self.window = window
        self.threshold = threshold
        self._windows: dict[tuple[str, str], Deque[float]] = {}

    def update(self, pair: tuple[str, str], value: float) -> None:
        key = _canonical_pair(pair)
        dq = self._windows.get(key)
        if dq is None:
            dq = deque(maxlen=self.window)
            self._windows[key] = dq
        dq.append(value)

    def is_pair_safe(self, pair: tuple[str, str]) -> bool:
        key = _canonical_pair(pair)
        dq = self._windows.get(key)
        if dq is None or len(dq) < self.window:
            return True  # not enough data -> don't block
        h = compute_hurst(list(dq))
        if h is None:
            return True
        return h < self.threshold
