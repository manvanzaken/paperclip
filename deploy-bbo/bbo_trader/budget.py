"""Pure rate budgets: sliding-window token buckets with a reserve for risk-reducing calls."""
from __future__ import annotations

from collections import deque
from typing import Literal

from .config import RateLimits

Kind = Literal["order", "amend", "cancel"]


class TokenBucket:
    """Sliding-window budget. `reserve` tokens are only spendable by priority callers (hedges, closes,
    flattens, cancels). A penalty (after a 429) halves the capacity for NON-priority callers only:
    entries and requotes pause while risk-reducing calls keep the full window — the venue, not our
    bucket, is the last line of defence for those."""

    def __init__(self, capacity: int, window_s: float, reserve: int = 0):
        self.capacity = capacity
        self.window_s = window_s
        self.reserve = reserve
        self._stamps: deque[float] = deque()
        self._penalty_until = 0.0

    def _prune(self, now: float) -> None:
        # strict: a token taken exactly window_s ago still counts against "N per window"
        while self._stamps and self._stamps[0] < now - self.window_s:
            self._stamps.popleft()

    def penalized(self, now: float) -> bool:
        return now < self._penalty_until

    def available(self, now: float, priority: bool = False) -> int:
        """Tokens a caller may take now (may be negative after a penalty). Non-priority callers
        cannot touch the reserve and see the halved capacity while penalized."""
        self._prune(now)
        used = len(self._stamps)
        if priority:
            return self.capacity - used
        cap = self.capacity // 2 if self.penalized(now) else self.capacity
        return cap - used - self.reserve

    def try_take(self, now: float, n: int = 1, priority: bool = False) -> bool:
        if self.available(now, priority) < n:
            return False
        for _ in range(n):
            self._stamps.append(now)
        return True

    def penalize(self, now: float, seconds: float) -> None:
        """Halve non-priority capacity for `seconds` (after a 429 / 'too frequent')."""
        self._penalty_until = now + seconds


class RateBudget:
    """Per-venue budget. kind: 'order' | 'amend' (orders bucket) | 'cancel' (cancels bucket,
    or the same bucket when the venue shares one limit across trading endpoints)."""

    def __init__(self, limits: RateLimits):
        self.shared = limits.shared
        self._orders = TokenBucket(limits.orders, limits.window_s, limits.reserve)
        self._cancels = (self._orders if limits.shared
                         else TokenBucket(limits.cancels, limits.window_s, limits.reserve))

    def _bucket(self, kind: Kind) -> TokenBucket:
        if kind == "cancel":
            return self._cancels
        if kind in ("order", "amend"):
            return self._orders
        raise ValueError(f"unknown budget kind: {kind!r}")

    def available(self, kind: Kind, now: float, priority: bool = False) -> int:
        return self._bucket(kind).available(now, priority)

    def try_take(self, kind: Kind, now: float, n: int = 1, priority: bool = False) -> bool:
        return self._bucket(kind).try_take(now, n, priority)

    def penalize(self, now: float, seconds: float = 60.0) -> None:
        self._orders.penalize(now, seconds)
        self._cancels.penalize(now, seconds)

    def to_dict(self, now: float) -> dict[str, object]:
        return {"orders_free": max(0, self._orders.available(now, priority=True)),
                "cancels_free": max(0, self._cancels.available(now, priority=True)),
                "shared": self.shared, "penalized": self._orders.penalized(now)}
