"""Guard tests for state-machine / JobStatus drift.

Ensures that every ``JobStatus`` literal is accounted for in the
transition table and classified as either terminal or non-terminal.
Fails fast if a new state is added to one place but not the others.
"""

from typing import get_args

from taskq.backend._protocol import JobStatus
from taskq.backend.statemachine import (
    ACTIVE_STATUSES,
    TERMINAL_STATUSES,
    VALID_TRANSITIONS,
)

# PEP-695 ``type`` aliases are ``TypeAliasType`` objects; ``get_args``
# returns ``()`` on the alias itself — unwrap via ``__value__`` to reach
# the ``Literal[...]`` and enumerate its members at runtime.
_ALL_JOB_STATUSES: frozenset[str] = frozenset(get_args(JobStatus.__value__))


# ── drift detection ────────────────────────────────────────────────────


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
