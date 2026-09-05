from __future__ import annotations

import asyncio
import time


class RateLimiter:
    """Async token-bucket rate limiter."""

    def __init__(self, requests_per_minute: int):
        self.rpm = requests_per_minute
        self.interval = 60.0 / requests_per_minute
        self.tokens = float(requests_per_minute)
        self.max_tokens = float(requests_per_minute)
        self.last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        new_tokens = elapsed / self.interval
        self.tokens = min(self.max_tokens, self.tokens + new_tokens)
        self.last_refill = now

    async def acquire(self):
        async with self._lock:
            self._refill()
            if self.tokens < 1.0:
                wait_time = (1.0 - self.tokens) * self.interval
                await asyncio.sleep(wait_time)
                self._refill()
            self.tokens -= 1.0


# Pre-configured limiters per source
RATE_LIMITS: dict[str, int] = {
    "binance": 1200,
    "dexscreener": 300,
    "coingecko": 10,
    "jupiter": 600,
    "ccxt_default": 120,
}


_limiters: dict[str, RateLimiter] = {}


def get_limiter(source: str) -> RateLimiter:
    if source not in _limiters:
        rpm = RATE_LIMITS.get(source, 60)
        _limiters[source] = RateLimiter(rpm)
    return _limiters[source]
