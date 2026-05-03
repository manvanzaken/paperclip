"""5-step reconciliation saga (PDF2 §5.1).

When `asyncio.gather` returns one filled order and one exception, we
cannot blindly roll back the filled leg — the "exception" might just be
a lost response for an order that actually succeeded. The saga walks:

    1. Detect    — caller hands us the two results.
    2. Verify    — ask the (simulated) exchange via fetch_order(client_order_id).
    3. Rollback  — only if Verify confirms the leg is unfilled.
    4. Diagnose  — classify the exception.
    5. Heal      — apply the policy for that diagnosis class.

In paper-mode the "exchange" is `ExecutionSim`. The saga is just as
useful here as it would be in production because the simulator can
inject failures via `sim_leg_failure_rate`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from core.execution_sim import (
    ExecutionSim,
    SimOrder,
    SimulatedFailure,
)
from core.trade_journal import TradeJournal

log = logging.getLogger(__name__)


class Diagnosis(Enum):
    NETWORK_ERROR = "NETWORK_ERROR"
    RATE_LIMIT = "RATE_LIMIT"
    INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
    UNKNOWN = "UNKNOWN"


def classify_exception(exc: BaseException) -> Diagnosis:
    if isinstance(exc, SimulatedFailure):
        # Default tag for the test simulator.
        return Diagnosis.NETWORK_ERROR
    text = str(exc)
    if "InsufficientBalance" in text or "Insufficient balance" in text:
        return Diagnosis.INSUFFICIENT_BALANCE
    if "RateLimit" in text or "429" in text:
        return Diagnosis.RATE_LIMIT
    if "Network" in text or "timed out" in text or "timeout" in text:
        return Diagnosis.NETWORK_ERROR
    return Diagnosis.UNKNOWN


@dataclass
class SagaResult:
    both_filled: bool
    diagnosis: Optional[Diagnosis] = None
    rolled_back_leg: Optional[str] = None  # "A" or "B"
    issues: list[str] = field(default_factory=list)


class ReconciliationSaga:
    def __init__(
        self,
        *,
        sim: ExecutionSim,
        journal: TradeJournal,
        quarantine_after: int = 5,
    ) -> None:
        self._sim = sim
        self._journal = journal
        self._quarantine_after = quarantine_after
        self._consecutive_failures: dict[str, int] = {}
        self._quarantined: set[str] = set()

    # --- public API ----------------------------------------------------

    def exchange_quarantined(self, exchange: str) -> bool:
        return exchange in self._quarantined

    def note_success(self, exchange: str) -> None:
        self._consecutive_failures.pop(exchange, None)

    async def run(
        self,
        *,
        position_id: str,
        exchange_a: str,
        exchange_b: str,
        leg_a: SimOrder | BaseException,
        leg_b: SimOrder | BaseException,
        client_order_id_a: str,
        client_order_id_b: str,
    ) -> SagaResult:
        # 1. Detect.
        self._log_step(
            "Detect",
            position_id=position_id,
            leg_a_ok=isinstance(leg_a, SimOrder),
            leg_b_ok=isinstance(leg_b, SimOrder),
            leg_a_err=None if isinstance(leg_a, SimOrder) else f"{type(leg_a).__name__}: {leg_a}",
            leg_b_err=None if isinstance(leg_b, SimOrder) else f"{type(leg_b).__name__}: {leg_b}",
        )

        a_filled, leg_a_order = await self._verify(client_order_id_a, leg_a)
        b_filled, leg_b_order = await self._verify(client_order_id_b, leg_b)
        self._log_step(
            "Verify",
            a_filled=a_filled,
            b_filled=b_filled,
            position_id=position_id,
        )

        if a_filled and b_filled:
            return SagaResult(both_filled=True)

        # 2/3. Rollback the filled leg, if any.
        rolled_back_leg: Optional[str] = None
        if a_filled and not b_filled and leg_a_order is not None:
            await self._rollback(leg_a_order, position_id=position_id)
            rolled_back_leg = "A"
        elif b_filled and not a_filled and leg_b_order is not None:
            await self._rollback(leg_b_order, position_id=position_id)
            rolled_back_leg = "B"
        else:
            # Neither leg is confirmed filled — nothing to rollback.
            self._log_step("Rollback", outcome="nothing_to_rollback", position_id=position_id)

        # 4. Diagnose: the suspect is whichever leg failed; the caller
        # passed both exchange identifiers explicitly so there is no
        # inference required.
        failing_exc: Optional[BaseException] = None
        suspect_exchange: Optional[str] = None
        if not a_filled and isinstance(leg_a, BaseException):
            failing_exc = leg_a
            suspect_exchange = exchange_a
        elif not b_filled and isinstance(leg_b, BaseException):
            failing_exc = leg_b
            suspect_exchange = exchange_b

        diagnosis = classify_exception(failing_exc) if failing_exc else Diagnosis.UNKNOWN
        self._log_step(
            "Diagnose",
            diagnosis=diagnosis.value,
            suspect_exchange=suspect_exchange,
            position_id=position_id,
        )

        # 5. Heal.
        await self._heal(diagnosis, suspect_exchange, position_id=position_id)

        return SagaResult(
            both_filled=False,
            diagnosis=diagnosis,
            rolled_back_leg=rolled_back_leg,
        )

    # --- step internals ------------------------------------------------

    async def _verify(
        self, client_order_id: str, leg: SimOrder | BaseException
    ) -> tuple[bool, Optional[SimOrder]]:
        # If caller already has a filled SimOrder, trust it.
        if isinstance(leg, SimOrder) and leg.state == "filled":
            return True, leg
        # Otherwise look up by client_order_id — the order may actually
        # have filled and the response was lost.
        looked_up = await self._sim.fetch_order(client_order_id)
        if looked_up is not None and looked_up.state == "filled":
            return True, looked_up
        return False, None

    async def _rollback(self, order: SimOrder, *, position_id: str) -> None:
        opposite = "sell" if order.side == "buy" else "buy"
        rollback_id = f"rollback-{order.client_order_id}"
        try:
            rb = await self._sim.create_order(
                exchange=order.exchange,
                symbol=order.symbol,
                side=opposite,
                size_usd=order.size_usd,
                client_order_id=rollback_id,
            )
            order.state = "rolled_back"
            self._log_step(
                "Rollback",
                outcome="closed_filled_leg",
                exchange=order.exchange,
                rollback_price=rb.filled_price,
                position_id=position_id,
            )
        except Exception as exc:
            log.error("rollback failed: %s", exc)
            self._log_step(
                "Rollback",
                outcome="ERROR",
                error=str(exc),
                position_id=position_id,
            )

    async def _heal(
        self,
        diagnosis: Diagnosis,
        suspect_exchange: Optional[str],
        *,
        position_id: str,
    ) -> None:
        if suspect_exchange is None:
            self._log_step("Heal", diagnosis=diagnosis.value, action="none", position_id=position_id)
            return

        if diagnosis is Diagnosis.NETWORK_ERROR:
            n = self._consecutive_failures.get(suspect_exchange, 0) + 1
            self._consecutive_failures[suspect_exchange] = n
            if n >= self._quarantine_after:
                self._quarantined.add(suspect_exchange)
                self._log_step(
                    "Heal",
                    action="QUARANTINE",
                    exchange=suspect_exchange,
                    consecutive=n,
                    position_id=position_id,
                )
            else:
                self._log_step(
                    "Heal",
                    action="MARK_DEGRADED",
                    exchange=suspect_exchange,
                    consecutive=n,
                    position_id=position_id,
                )
        elif diagnosis is Diagnosis.RATE_LIMIT:
            self._log_step(
                "Heal",
                action="HALVE_RATE_LIMIT",
                exchange=suspect_exchange,
                position_id=position_id,
            )
        elif diagnosis is Diagnosis.INSUFFICIENT_BALANCE:
            self._log_step(
                "Heal",
                action="SHRINK_POSITION_SIZE",
                exchange=suspect_exchange,
                position_id=position_id,
            )
        else:
            self._log_step(
                "Heal",
                action="MARK_DEGRADED",
                exchange=suspect_exchange,
                position_id=position_id,
            )

    # --- logging -------------------------------------------------------

    def _log_step(self, step: str, **payload: object) -> None:
        self._journal.log(
            event_type="SAGA_STEP",
            payload={"step": step, **payload},
        )
