"""Pure rate budgets: sliding-window token buckets with a reserve for risk-reducing calls."""
from __future__ import annotations

from collections import deque

from .config import RateLimits


class TokenBucket:
    def __init__(self, capacity: int, window_s: float, reserve: int = 0):
        self.capacity = capacity
        self.window_s = window_s
        self.reserve = reserve
        self._stamps: deque[float] = deque()
        self._penalty_until = 0.0

    def _prune(self, now: float) -> None:
        while self._stamps and self._stamps[0] <= now - self.window_s:
            self._stamps.popleft()

    def available(self, now: float, priority: bool = False) -> int:
        """Tokens a caller may take now. Non-priority callers cannot touch the reserve."""
        self._prune(now)
        cap = self.capacity // 2 if now < self._penalty_until else self.capacity
        free = cap - len(self._stamps)
        return free if priority else free - self.reserve

    def try_take(self, now: float, n: int = 1, priority: bool = False) -> bool:
        if self.available(now, priority) < n:
            return False
        for _ in range(n):
            self._stamps.append(now)
        return True

    def penalize(self, now: float, seconds: float) -> None:
        """Halve capacity for `seconds` (after a 429 / 'too frequent')."""
        self._penalty_until = now + seconds


class RateBudget:
    """Per-venue budget. kind: 'order' | 'amend' (orders bucket) | 'cancel' (cancels bucket,
    or the same bucket when the venue shares one limit across trading endpoints)."""

    def __init__(self, limits: RateLimits):
        self._orders = TokenBucket(limits.orders, limits.window_s, limits.reserve)
        self._cancels = (self._orders if limits.shared
                         else TokenBucket(limits.cancels, limits.window_s, limits.reserve))

    def _bucket(self, kind: str) -> TokenBucket:
        return self._cancels if kind == "cancel" else self._orders

    def available(self, kind: str, now: float, priority: bool = False) -> int:
        return self._bucket(kind).available(now, priority)

    def try_take(self, kind: str, now: float, n: int = 1, priority: bool = False) -> bool:
        return self._bucket(kind).try_take(now, n, priority)

    def penalize(self, now: float, seconds: float = 60.0) -> None:
        self._orders.penalize(now, seconds)
        self._cancels.penalize(now, seconds)

    def to_dict(self, now: float) -> dict:
        return {"orders_free": self._orders.available(now, priority=True),
                "cancels_free": self._cancels.available(now, priority=True)}
