"""Unit tests for the job state machine ().

Covers:
- every cell of the 8x8 transition matrix is verified
- assert_valid_transition raises IllegalStateTransition with correct message
- parametrize over every illegal cell; each raises IllegalStateTransition
"""

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobStatus
from taskq.backend.statemachine import (
    TERMINAL_STATUSES,
    VALID_TRANSITIONS,
    assert_valid_transition,
)
from taskq.exceptions import IllegalStateTransition

ALL_STATUSES: list[JobStatus] = [
    "pending",
    "scheduled",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "crashed",
    "abandoned",
]


# ── complete 8x8 transition matrix ──────────────────────────────


class TestTransitionMatrix:
    """Verify every cell of the 8x8 transition matrix against"""

    @pytest.mark.parametrize(
        ("from_status", "to_status"),
        [
            # pending → {running, cancelled, failed}
            ("pending", "running"),
            ("pending", "cancelled"),
            ("pending", "failed"),
            # scheduled → {pending, cancelled, failed}
            ("scheduled", "pending"),
            ("scheduled", "cancelled"),
            ("scheduled", "failed"),
            # running → {succeeded, failed, cancelled, crashed, abandoned, scheduled}
            ("running", "succeeded"),
            ("running", "failed"),
            ("running", "cancelled"),
            ("running", "crashed"),
            ("running", "abandoned"),
            ("running", "scheduled"),
        ],
        ids=lambda v: v,
    )
    def test_valid_transition_in_matrix(self, from_status: JobStatus, to_status: JobStatus) -> None:
        """Legal (from, to) pair must appear in VALID_TRANSITIONS."""
        assert to_status in VALID_TRANSITIONS[from_status]

    @pytest.mark.parametrize(
        ("from_status", "to_status"),
        [
            # pending — only running, cancelled, and failed (sweep) allowed
            ("pending", "pending"),
            ("pending", "scheduled"),
            ("pending", "succeeded"),
            ("pending", "crashed"),
            ("pending", "abandoned"),
            # scheduled — only pending, cancelled, and failed (sweep) allowed
            ("scheduled", "scheduled"),
            ("scheduled", "running"),
            ("scheduled", "succeeded"),
            ("scheduled", "crashed"),
            ("scheduled", "abandoned"),
            # running — only the 6 listed above allowed
            ("running", "running"),
            ("running", "pending"),
            # terminal statuses — nothing allowed
            ("succeeded", "pending"),
            ("succeeded", "scheduled"),
            ("succeeded", "running"),
            ("succeeded", "succeeded"),
            ("succeeded", "failed"),
            ("succeeded", "cancelled"),
            ("succeeded", "crashed"),
            ("succeeded", "abandoned"),
            ("failed", "pending"),
            ("failed", "scheduled"),
            ("failed", "running"),
            ("failed", "succeeded"),
            ("failed", "failed"),
            ("failed", "cancelled"),
            ("failed", "crashed"),
            ("failed", "abandoned"),
            ("cancelled", "pending"),
            ("cancelled", "scheduled"),
            ("cancelled", "running"),
            ("cancelled", "succeeded"),
            ("cancelled", "failed"),
            ("cancelled", "cancelled"),
            ("cancelled", "crashed"),
            ("cancelled", "abandoned"),
            ("crashed", "pending"),
            ("crashed", "scheduled"),
            ("crashed", "running"),
            ("crashed", "succeeded"),
            ("crashed", "failed"),
            ("crashed", "cancelled"),
            ("crashed", "crashed"),
            ("crashed", "abandoned"),
            ("abandoned", "pending"),
            ("abandoned", "scheduled"),
            ("abandoned", "running"),
            ("abandoned", "succeeded"),
            ("abandoned", "failed"),
            ("abandoned", "cancelled"),
            ("abandoned", "crashed"),
            ("abandoned", "abandoned"),
        ],
        ids=lambda v: v,
    )
    def test_invalid_transition_not_in_matrix(
        self, from_status: JobStatus, to_status: JobStatus
    ) -> None:
        """Illegal (from, to) pair must NOT appear in VALID_TRANSITIONS."""
        assert to_status not in VALID_TRANSITIONS[from_status]

    def test_every_status_is_a_key(self) -> None:
        """Every JobStatus value must appear as a key in VALID_TRANSITIONS."""
        for status in ALL_STATUSES:
            assert status in VALID_TRANSITIONS, f"{status!r} missing from VALID_TRANSITIONS keys"

    def test_terminal_statuses_have_empty_frozenset(self) -> None:
        """Every terminal status must map to an empty frozenset."""
        for status in TERMINAL_STATUSES:
            assert VALID_TRANSITIONS[status] == frozenset(), (
                f"terminal {status!r} must map to empty frozenset"
            )

    def test_pending_not_in_running_transitions(self) -> None:
        """Worker-written retries go to 'scheduled', not 'pending'.

        The canonical Python code block does not include the
        running → pending arc; worker-facing retries target 'scheduled'.
        The diagram shows a running → pending arrow for the recovery
        sweep path, but that sweep operates outside the normal worker
        write path and is not encoded in VALID_TRANSITIONS.
        """
        assert "pending" not in VALID_TRANSITIONS["running"]

    def test_scheduled_in_running_transitions(self) -> None:
        """'scheduled' must be a valid target from 'running' (retry/snooze)."""
        assert "scheduled" in VALID_TRANSITIONS["running"]

    def test_terminal_statuses_set_matches_keys(self) -> None:
        """TERMINAL_STATUSES must contain exactly the five terminal statuses."""
        assert (
            frozenset({"succeeded", "failed", "cancelled", "crashed", "abandoned"})
            == TERMINAL_STATUSES
        )


# ── IllegalStateTransition raised with correct message ──────────


class TestAssertValidTransition:
    """assert_valid_transition raises IllegalStateTransition with
    a message naming both statuses and the job_id.
    """

    def test_raises_for_impossible_transition(self) -> None:
        """Succeeded → running is impossible; IllegalStateTransition raised."""
        job_id = new_uuid()
        with pytest.raises(IllegalStateTransition) as exc_info:
            assert_valid_transition(
                from_status="succeeded",
                to_status="running",
                job_id=job_id,
            )
        exc = exc_info.value
        assert exc.from_status == "succeeded"
        assert exc.to_status == "running"
        assert exc.job_id == job_id
        # Message must name both statuses and the job_id
        msg = str(exc)
        assert "succeeded" in msg
        assert "running" in msg
        assert str(job_id) in msg

    def test_does_not_raise_for_valid_transition(self) -> None:
        """A legal transition must not raise."""
        assert_valid_transition(
            from_status="running",
            to_status="succeeded",
            job_id=new_uuid(),
        )


# ── every illegal cell raises IllegalStateTransition ────────────


def _illegal_pairs() -> list[tuple[JobStatus, JobStatus]]:
    """Build every (from, to) pair where to is NOT in VALID_TRANSITIONS[from]."""
    pairs: list[tuple[JobStatus, JobStatus]] = []
    for from_status in ALL_STATUSES:
        for to_status in ALL_STATUSES:
            if to_status not in VALID_TRANSITIONS[from_status]:
                pairs.append((from_status, to_status))
    return pairs


_ILLEGAL_PAIRS = _illegal_pairs()


@pytest.mark.parametrize(
    ("from_status", "to_status"),
    _ILLEGAL_PAIRS,
    ids=[f"{f}->{t}" for f, t in _ILLEGAL_PAIRS],
)
def test_illegal_transition_raises(from_status: JobStatus, to_status: JobStatus) -> None:
    """Every illegal (from, to) pair raises IllegalStateTransition."""
    job_id = new_uuid()
    with pytest.raises(IllegalStateTransition):
        assert_valid_transition(
            from_status=from_status,
            to_status=to_status,
            job_id=job_id,
        )


# ── VALID_TRANSITIONS golden-dict equality test ─────────────────────────


def test_valid_transitions_matches_golden() -> None:
    """VALID_TRANSITIONS must match the diagram exactly.

    If any transition is added, removed, or modified without updating
    this golden dict, the equality assertion fails with a diff that
    names the offending status key.
    """
    golden: dict[JobStatus, frozenset[JobStatus]] = {
        "pending": frozenset({"running", "cancelled", "failed"}),
        "scheduled": frozenset({"pending", "cancelled", "failed"}),
        "running": frozenset(
            {"succeeded", "failed", "cancelled", "crashed", "abandoned", "scheduled"}
        ),
        "succeeded": frozenset(),
        "failed": frozenset(),
        "cancelled": frozenset(),
        "crashed": frozenset(),
        "abandoned": frozenset(),
    }
    assert golden == VALID_TRANSITIONS
