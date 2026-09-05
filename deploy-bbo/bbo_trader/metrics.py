"""Latency histograms, the rejection funnel, event-loop lag sampling and the feed coverage watchdog.

Everything here lands in the state file's `bbo` section for the dashboard, so the numbers say what they
are: histogram `n`/`max` describe the sliding window, `total`/`max_ever` the process lifetime."""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import Counter, deque

log = logging.getLogger("bbo.metrics")


class LatencyHist:
    def __init__(self, maxlen: int = 1000):
        self._v: deque[float] = deque(maxlen=maxlen)
        self._total = 0
        self._max_ever = 0.0

    def add(self, ms: float) -> None:
        ms = float(ms)
        if not math.isfinite(ms):
            return
        ms = max(0.0, ms)                 # a backwards clock step is not a negative latency
        self._v.append(ms)
        self._total += 1
        self._max_ever = max(self._max_ever, ms)

    def _pct(self, q: float) -> float:
        """Index floor(q·n) of the sorted window — one rank above nearest-rank, so p95 == max while n <= 20."""
        if not self._v:
            return 0.0
        s = sorted(self._v)
        return s[min(len(s) - 1, int(q * len(s)))]

    @property
    def count(self) -> int:
        return len(self._v)

    def to_dict(self) -> dict:
        return {"n": self.count, "total": self._total, "p50": round(self._pct(0.50), 1),
                "p95": round(self._pct(0.95), 1), "max": round(max(self._v), 1) if self._v else 0.0,
                "max_ever": round(self._max_ever, 1)}


class Metrics:
    def __init__(self, mono=time.monotonic):
        self.hists: dict[str, LatencyHist] = {}
        self.funnel: Counter = Counter()
        self.loop_lag = LatencyHist(600)
        self._mono = mono
        self.started_mono = mono()
        self.started = time.time()

    def record(self, stage: str, ms: float) -> None:
        self.hists.setdefault(stage, LatencyHist()).add(ms)

    async def sample_loop_lag(self, interval_s: float = 1.0, warn_ms: float = 50.0, summary_s: float = 60.0) -> None:
        """Samples the event-loop lag every `interval_s`; one LOOP_LAG summary line per `summary_s` at most
        (a lag storm must not bury the trading log under one warning per second)."""
        loop = asyncio.get_running_loop()
        over = samples = 0
        worst = 0.0
        window_start = loop.time()
        while True:
            t0 = loop.time()
            await asyncio.sleep(interval_s)
            lag_ms = max(0.0, (loop.time() - t0 - interval_s) * 1000.0)
            self.loop_lag.add(lag_ms)
            samples += 1
            if lag_ms > warn_ms:
                over += 1
                worst = max(worst, lag_ms)
            if loop.time() - window_start >= summary_s:
                if over:
                    log.warning("LOOP_LAG %d of %d samples over %.0f ms in the last %.0fs, worst %.0f ms",
                                over, samples, warn_ms, loop.time() - window_start, worst)
                over = samples = 0
                worst = 0.0
                window_start = loop.time()

    def to_dict(self) -> dict:
        return {"latency_ms": {k: h.to_dict() for k, h in self.hists.items()},
                "funnel": dict(self.funnel), "loop_lag_ms": self.loop_lag.to_dict(),
                "uptime_s": round(self._mono() - self.started_mono), "started_at": self.started}


class CoverageWatchdog:
    """Fresh-quote counts per CONFIGURED venue (a dead venue reads 0, it never disappears). Warns when a venue
    is under max(floor, frac × its own high-water mark): an absolute floor alone is silent when 390 of 400
    symbols go stale and permanently noisy on a 10-pair venue. Quiet for `grace_s` after start so the first
    minute of connecting does not raise an alarm. The App schedules `check()`; `to_dict()` goes to the state file."""

    def __init__(self, board, venues: list[str], floor: int = 10, frac: float = 0.5, grace_s: float = 60.0,
                 clock=time.time):
        self.board, self.venues, self.floor, self.frac, self.grace_s, self.clock = board, list(venues), floor, frac, grace_s, clock
        self.last: dict[str, int] = {v: 0 for v in self.venues}
        self.high: dict[str, int] = {v: 0 for v in self.venues}
        self._t0 = clock()

    def check(self) -> dict[str, int]:
        now = self.clock()
        counts = self.board.fresh_counts(now)
        self.last = {v: counts.get(v, 0) for v in self.venues}
        log.info("FEED_COVERAGE %s", " ".join(f"{v}={n}" for v, n in self.last.items()))
        for v, n in self.last.items():
            self.high[v] = max(self.high[v], n)
            threshold = max(float(self.floor), self.frac * self.high[v])
            if n < threshold and now - self._t0 >= self.grace_s:
                log.warning("FEED_COVERAGE_LOW %s fresh=%d threshold=%.0f (floor=%d, high=%d)", v, n, threshold,
                            self.floor, self.high[v])
        return self.last

    def to_dict(self) -> dict:
        return {"fresh": dict(self.last), "high": dict(self.high), "floor": self.floor, "frac": self.frac}
