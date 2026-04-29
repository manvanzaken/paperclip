"""Periodic economic-health probe.

The PDF (PDF2 §1.1) calls out the difference between *process* health
("the script is running, no exceptions") and *economic* health ("the bot
is actually creating value"). This module covers the four economic
metrics: trade drought, error rate, capital depletion, orphaned
positions.

The check returns a `HealthStatus` whose `recommended_action` the main
loop dispatches on. Priority order when multiple issues exist:

    ORPHANED > CAPITAL_DEPLETED > HIGH_ERROR_RATE > TRADE_DROUGHT

Higher-priority issues mask lower-priority ones because their actions
already imply the lower remediation (e.g. reconciling clears the
positions whose orphan-status would also have triggered the drought).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Literal

from core.state_machine import Position, PositionState
from core.trade_journal import TradeJournal


HealthAction = Literal[
    "CONTINUE",
    "REOPTIMIZE_PARAMETERS",
    "FORCE_EXIT_OLDEST",
    "RECONCILE_IMMEDIATELY",
    "ENTER_SAFE_MODE",
]


@dataclass
class HealthStatus:
    is_healthy: bool
    issues: list[str]
    recommended_action: HealthAction


class TradingHealthCheck:
    def __init__(
        self,
        *,
        journal: TradeJournal,
        get_balances: Callable[[], dict[str, float]],
        min_order_usd: float,
        get_open_positions: Callable[[], list[Position]],
        drought_threshold: timedelta = timedelta(hours=72),
        error_rate_threshold: int = 20,
        orphan_threshold: timedelta = timedelta(seconds=60),
        interval_sec: float = 300.0,
    ) -> None:
        self._journal = journal
        self._get_balances = get_balances
        self._min_order_usd = min_order_usd
        self._get_open_positions = get_open_positions
        self._drought_threshold = drought_threshold
        self._error_rate_threshold = error_rate_threshold
        self._orphan_threshold = orphan_threshold
        self._interval_sec = interval_sec

    async def run_check(self) -> HealthStatus:
        issues: list[str] = []

        # 1. Trade drought.
        last_fill = self._journal.last_successful_trade()
        now = datetime.now(timezone.utc)
        drought = (
            last_fill is not None
            and (now - last_fill) > self._drought_threshold
        )
        if drought:
            issues.append("TRADE_DROUGHT")

        # 2. Error rate.
        errors = self._journal.count_errors(since=timedelta(hours=1))
        high_error = errors > self._error_rate_threshold
        if high_error:
            issues.append(f"HIGH_ERROR_RATE:{errors}")

        # 3. Capital depletion.
        depleted: list[str] = []
        for ex, bal in self._get_balances().items():
            if bal < self._min_order_usd:
                depleted.append(ex)
        if depleted:
            issues.append(f"CAPITAL_DEPLETED:{','.join(depleted)}")

        # 4. Orphaned positions: stuck in RECONCILING too long.
        orphans: list[str] = []
        for p in self._get_open_positions():
            if p.state is PositionState.RECONCILING and p.state_history:
                # Find the timestamp we entered RECONCILING (last entry).
                last = p.state_history[-1]
                _, _, ts, _ = last
                if (now - ts) > self._orphan_threshold:
                    orphans.append(p.id)
        if orphans:
            issues.append(f"ORPHANED:{','.join(orphans)}")

        if not issues:
            return HealthStatus(True, [], "CONTINUE")

        # Priority dispatch: orphan > capital > error rate > drought.
        if any(i.startswith("ORPHANED") for i in issues):
            action: HealthAction = "RECONCILE_IMMEDIATELY"
        elif any(i.startswith("CAPITAL_DEPLETED") for i in issues):
            action = "FORCE_EXIT_OLDEST"
        elif any(i.startswith("HIGH_ERROR_RATE") for i in issues):
            action = "ENTER_SAFE_MODE"
        else:
            action = "REOPTIMIZE_PARAMETERS"

        return HealthStatus(False, issues, action)

    async def monitor_loop(self, on_status: Callable[[HealthStatus], None]) -> None:
        while True:
            status = await self.run_check()
            on_status(status)
            await asyncio.sleep(self._interval_sec)
