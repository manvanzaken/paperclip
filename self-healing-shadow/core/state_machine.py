"""Per-trade finite state machine.

States and `ALLOWED_TRANSITIONS` are the canonical source of truth — see
the design doc, section "State machine". Every transition is validated;
violations raise `InvalidTransition` so logic bugs surface loudly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional


class PositionState(Enum):
    SCANNING = "SCANNING"
    SIGNAL_DETECTED = "SIGNAL_DETECTED"
    PROFITABILITY_CHECK = "PROFITABILITY_CHECK"
    EXECUTING = "EXECUTING"
    RECONCILING = "RECONCILING"
    POSITION_OPEN = "POSITION_OPEN"
    MONITORING = "MONITORING"
    CLOSING = "CLOSING"
    EMERGENCY_EXIT = "EMERGENCY_EXIT"
    ROLLBACK = "ROLLBACK"


# Source of truth: every key must be a PositionState; values are the
# states each one may transition INTO.
ALLOWED_TRANSITIONS: dict[PositionState, set[PositionState]] = {
    PositionState.SCANNING:            {PositionState.SIGNAL_DETECTED},
    PositionState.SIGNAL_DETECTED:     {PositionState.PROFITABILITY_CHECK},
    PositionState.PROFITABILITY_CHECK: {PositionState.SCANNING, PositionState.EXECUTING},
    PositionState.EXECUTING:           {PositionState.RECONCILING},
    PositionState.RECONCILING:         {PositionState.POSITION_OPEN, PositionState.ROLLBACK},
    PositionState.POSITION_OPEN:       {PositionState.MONITORING},
    PositionState.MONITORING:          {PositionState.CLOSING, PositionState.EMERGENCY_EXIT},
    PositionState.CLOSING:             {PositionState.SCANNING},
    PositionState.EMERGENCY_EXIT:      {PositionState.SCANNING},
    PositionState.ROLLBACK:            {PositionState.SCANNING},
}


class InvalidTransition(Exception):
    """Raised when a state transition is not in ALLOWED_TRANSITIONS."""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Position:
    id: str
    pair: tuple[str, str]
    symbol: str
    state: PositionState = PositionState.SCANNING
    state_history: list[tuple[PositionState, PositionState, datetime, str]] = field(
        default_factory=list
    )
    # Per-leg fill metadata, populated as the FSM progresses.
    leg_a_order_id: Optional[str] = None
    leg_b_order_id: Optional[str] = None
    entry_z: Optional[float] = None
    opened_at: Optional[datetime] = None

    def transition(self, new_state: PositionState, *, reason: str) -> None:
        if new_state not in ALLOWED_TRANSITIONS[self.state]:
            raise InvalidTransition(
                f"{self.state.name} -> {new_state.name} not allowed"
            )
        old = self.state
        self.state = new_state
        self.state_history.append((old, new_state, _utcnow(), reason))
