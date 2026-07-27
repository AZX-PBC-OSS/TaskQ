"""Guard tests for state-machine / JobStatus drift.

Ensures that every ``JobStatus`` literal is accounted for in the
transition table and classified as either terminal or non-terminal.
Fails fast if a new state is added to one place but not the others.
"""

from typing import get_args

from taskq.backend._protocol import JOB_STATUS_VALUES, JobStatus
from taskq.backend.statemachine import (
    ACTIVE_STATUSES,
    TERMINAL_STATUSES,
    VALID_TRANSITIONS,
)

_ALL_JOB_STATUSES: frozenset[str] = JOB_STATUS_VALUES


# ── drift detection ────────────────────────────────────────────────────


def test_job_status_values_matches_literal() -> None:
    """JOB_STATUS_VALUES (the runtime validation set) is derived from the
    JobStatus Literal — and still covers every Literal member."""
    assert frozenset(get_args(JobStatus.__value__)) == JOB_STATUS_VALUES


def test_job_status_literals_match_transition_table() -> None:
    """Every JobStatus literal has a VALID_TRANSITIONS entry and vice versa."""
    assert frozenset(VALID_TRANSITIONS) == _ALL_JOB_STATUSES


def test_terminal_and_active_union_covers_all_statuses() -> None:
    """Every status is classified as terminal or non-terminal — none fall through the cracks."""
    assert frozenset(VALID_TRANSITIONS) == TERMINAL_STATUSES | ACTIVE_STATUSES


def test_terminal_and_active_are_disjoint() -> None:
    """No status is classified as both terminal and non-terminal."""
    assert frozenset() == TERMINAL_STATUSES & ACTIVE_STATUSES


# ── pinned membership regression ──────────────────────────────────────


def test_terminal_statuses_pinned_membership() -> None:
    """Pin the exact terminal set so an accidental reclassification is caught even though the complement-based tests would still pass."""
    assert (
        frozenset({"succeeded", "failed", "cancelled", "crashed", "abandoned"}) == TERMINAL_STATUSES
    )


def test_active_statuses_pinned_membership() -> None:
    """Pin the exact active set so an accidental reclassification is caught even though the complement-based tests would still pass."""
    assert frozenset({"pending", "scheduled", "running"}) == ACTIVE_STATUSES


# ── ACTIVE_STATUSES is a derivation, not a hand-maintained set ────────


def test_active_statuses_equals_complement_derivation() -> None:
    """ACTIVE_STATUSES must remain *derived* — if it is ever replaced by a
    hand-maintained set that drifts from the complement rule, this fails."""
    assert frozenset(VALID_TRANSITIONS) - TERMINAL_STATUSES == ACTIVE_STATUSES


def test_active_derivation_extends_for_hypothetical_non_terminal_state() -> None:
    """Prove the auto-extension property: adding a new non-terminal state
    to the transition table flows into the derived active set with no
    second edit (``JobFilter(active=True)`` picks it up automatically)."""
    extended: dict[JobStatus, frozenset[JobStatus]] = {
        **VALID_TRANSITIONS,
        "throttled": frozenset({"pending"}),  # type: ignore[dict-item]  # Why: hypothetical state, deliberately not a JobStatus
    }
    derived = frozenset(extended) - TERMINAL_STATUSES
    assert derived == ACTIVE_STATUSES | {"throttled"}


def test_active_derivation_excludes_hypothetical_terminal_state() -> None:
    """The mirror image: a new *terminal* state must not leak into the
    derived active set."""
    extended: dict[JobStatus, frozenset[JobStatus]] = {
        **VALID_TRANSITIONS,
        "expired": frozenset(),  # type: ignore[dict-item]  # Why: hypothetical state, deliberately not a JobStatus
    }
    derived = frozenset(extended) - (TERMINAL_STATUSES | {"expired"})
    assert "expired" not in derived
    assert derived == ACTIVE_STATUSES
