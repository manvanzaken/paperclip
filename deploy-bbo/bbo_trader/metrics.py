"""Latency histograms, the rejection funnel, event-loop lag sampling and the feed coverage watchdog."""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter, deque

log = logging.getLogger("bbo.metrics")


class LatencyHist:
    def __init__(self, maxlen: int = 1000):
        self._v: deque[float] = deque(maxlen=maxlen)

    def add(self, ms: float) -> None:
        self._v.append(float(ms))

    def _pct(self, q: float) -> float:
        if not self._v:
            return 0.0
        s = sorted(self._v)
        return s[min(len(s) - 1, int(q * len(s)))]

    @property
    def count(self) -> int:
        return len(self._v)

    def to_dict(self) -> dict:
        return {"n": self.count, "p50": round(self._pct(0.50), 1), "p95": round(self._pct(0.95), 1),
                "max": round(max(self._v), 1) if self._v else 0.0}


class Metrics:
    def __init__(self):
        self.hists: dict[str, LatencyHist] = {}
        self.funnel: Counter = Counter()
        self.loop_lag = LatencyHist(600)
        self.started = time.time()

    def record(self, stage: str, ms: float) -> None:
        self.hists.setdefault(stage, LatencyHist()).add(ms)

    async def sample_loop_lag(self, interval_s: float = 1.0) -> None:
        loop = asyncio.get_running_loop()
        while True:
            t0 = loop.time()
            await asyncio.sleep(interval_s)
            lag_ms = (loop.time() - t0 - interval_s) * 1000.0
            self.loop_lag.add(max(0.0, lag_ms))
            if lag_ms > 50.0:
                log.warning("LOOP_LAG %.0f ms", lag_ms)

    def to_dict(self) -> dict:
        return {"latency_ms": {k: h.to_dict() for k, h in self.hists.items()},
                "funnel": dict(self.funnel), "loop_lag_ms": self.loop_lag.to_dict(),
                "uptime_s": round(time.time() - self.started)}


class CoverageWatchdog:
    """Logs fresh-quote counts per venue every interval and warns when a venue is under its floor."""

    def __init__(self, board, venues: list[str], floor: int = 10, interval_s: float = 60.0, clock=time.time):
        self.board, self.venues, self.floor, self.interval_s, self.clock = board, venues, floor, interval_s, clock
        self.last: dict[str, int] = {}

    def check(self) -> dict[str, int]:
        counts = self.board.fresh_counts(self.clock())
        self.last = {v: counts.get(v, 0) for v in self.venues}
        log.info("FEED_COVERAGE %s", " ".join(f"{v}={n}" for v, n in self.last.items()))
        for v, n in self.last.items():
            if n < self.floor:
                log.warning("FEED_COVERAGE_LOW %s fresh=%d floor=%d", v, n, self.floor)
        return self.last

    async def run(self) -> None:
        while True:
            await asyncio.sleep(self.interval_s)
            self.check()