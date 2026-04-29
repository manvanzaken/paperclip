"""WebSocket heartbeat monitor.

Each per-exchange WS feeder calls `on_ws_message(exchange)` on every
inbound frame. The 1 Hz `monitor_loop` checks whether each exchange has
gone silent for longer than `stale_threshold_sec` and flips its status
to `DEGRADED`. The next message after that automatically heals the
exchange back to `HEALTHY`.

`is_pair_tradeable(a, b)` is the hard gate the signal engine uses
before emitting any entry signal.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Literal

log = logging.getLogger(__name__)

ExchangeStatus = Literal["HEALTHY", "DEGRADED"]


class HeartbeatMonitor:
    def __init__(self, *, stale_threshold_sec: float = 10.0) -> None:
        self.stale_threshold = stale_threshold_sec
        self.last_update: dict[str, datetime] = {}
        self.exchange_status: dict[str, ExchangeStatus] = {}

    def on_ws_message(self, exchange: str) -> None:
        self.last_update[exchange] = datetime.now(timezone.utc)
        prev = self.exchange_status.get(exchange)
        if prev != "HEALTHY":
            if prev == "DEGRADED":
                log.info("[HEALED] %s back to HEALTHY", exchange)
            self.exchange_status[exchange] = "HEALTHY"

    def evaluate_now(self) -> None:
        """Check staleness once, synchronously. Used by tests and the loop."""
        now = datetime.now(timezone.utc)
        for exchange, last in self.last_update.items():
            age = (now - last).total_seconds()
            if age > self.stale_threshold:
                if self.exchange_status.get(exchange) != "DEGRADED":
                    log.warning(
                        "[DEGRADED] %s no data for %.1fs", exchange, age
                    )
                    self.exchange_status[exchange] = "DEGRADED"

    async def monitor_loop(self) -> None:
        while True:
            self.evaluate_now()
            await asyncio.sleep(1.0)

    def is_pair_tradeable(self, a: str, b: str) -> bool:
        return (
            self.exchange_status.get(a) == "HEALTHY"
            and self.exchange_status.get(b) == "HEALTHY"
        )
