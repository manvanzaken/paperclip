"""Tests for core.state_machine."""

import pytest

from core.state_machine import (
    ALLOWED_TRANSITIONS,
    InvalidTransition,
    Position,
    PositionState,
)


def make_position(state: PositionState = PositionState.SCANNING) -> Position:
    return Position(
        id="t-1",
        pair=("mexc", "binance"),
        symbol="BTC/USDT:USDT",
        state=state,
    )


class TestAllowedTransitions:
    def test_allowed_transitions_keys_cover_every_state(self):
        # Every state must appear as a key (even terminal ones can have empty set).
        for s in PositionState:
            assert s in ALLOWED_TRANSITIONS

    def test_canonical_happy_path_is_walkable(self):
        path = [
            PositionState.SCANNING,
            PositionState.SIGNAL_DETECTED,
            PositionState.PROFITABILITY_CHECK,
            PositionState.EXECUTING,
            PositionState.RECONCILING,
            PositionState.POSITION_OPEN,
            PositionState.MONITORING,
            PositionState.CLOSING,
            PositionState.SCANNING,
        ]
        for a, b in zip(path, path[1:]):
            assert b in ALLOWED_TRANSITIONS[a], f"{a.name} -> {b.name} should be allowed"


class TestPositionTransition:
    def test_valid_transition_changes_state(self):
        p = make_position()
        p.transition(PositionState.SIGNAL_DETECTED, reason="z>2")
        assert p.state is PositionState.SIGNAL_DETECTED

    def test_invalid_transition_raises_and_keeps_state(self):
        p = make_position(PositionState.SCANNING)
        with pytest.raises(InvalidTransition):
            p.transition(PositionState.POSITION_OPEN, reason="bogus")
        assert p.state is PositionState.SCANNING

    def test_transition_appends_history(self):
        p = make_position()
        p.transition(PositionState.SIGNAL_DETECTED, reason="z>2")
        p.transition(PositionState.PROFITABILITY_CHECK, reason="next")
        assert len(p.state_history) == 2
        old, new, ts, reason = p.state_history[0]
        assert old is PositionState.SCANNING
        assert new is PositionState.SIGNAL_DETECTED
        assert reason == "z>2"
        assert ts is not None

    def test_rollback_path(self):
        p = make_position(PositionState.RECONCILING)
        p.transition(PositionState.ROLLBACK, reason="leg failed")
        p.transition(PositionState.SCANNING, reason="rolled back")
        assert p.state is PositionState.SCANNING

    def test_emergency_exit_path(self):
        p = make_position(PositionState.MONITORING)
        p.transition(PositionState.EMERGENCY_EXIT, reason="stop loss")
        p.transition(PositionState.SCANNING, reason="closed")
        assert p.state is PositionState.SCANNING

    def test_profitability_check_can_return_to_scanning(self):
        p = make_position(PositionState.PROFITABILITY_CHECK)
        p.transition(PositionState.SCANNING, reason="profit too low")
        assert p.state is PositionState.SCANNING

    def test_profitability_check_to_executing(self):
        p = make_position(PositionState.PROFITABILITY_CHECK)
        p.transition(PositionState.EXECUTING, reason="profitable")
        assert p.state is PositionState.EXECUTING


class TestExhaustiveInvalidTransitions:
    def test_every_disallowed_pair_raises(self):
        all_states = list(PositionState)
        bad_pairs = [
            (a, b) for a in all_states for b in all_states
            if b not in ALLOWED_TRANSITIONS[a]
        ]
        assert bad_pairs, "should have at least one disallowed pair"
        for a, b in bad_pairs:
            p = make_position(a)
            with pytest.raises(InvalidTransition):
                p.transition(b, reason="exhaustive check")
