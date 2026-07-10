"""Canonical encoding of the job state machine.

Provides ``VALID_TRANSITIONS``, ``TERMINAL_STATUSES``, and
``assert_valid_transition`` — the application-level fast-path check
that catches obvious bugs before they reach the SQL WHERE clause.
The SQL clause remains the authoritative serialization gate.
"""

from taskq.backend._protocol import JobId, JobStatus
from taskq.exceptions import IllegalStateTransition

__all__ = [
    "TERMINAL_STATUSES",
    "VALID_TRANSITIONS",
    "JobStatus",
    "assert_valid_transition",
]

TERMINAL_STATUSES: frozenset[JobStatus] = frozenset(
    {"succeeded", "failed", "cancelled", "crashed", "abandoned"}
)

VALID_TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    "pending": frozenset(
        {"running", "cancelled", "failed"}
    ),  # failed only via deadline-exceeded sweep
    "scheduled": frozenset(
        {"pending", "cancelled", "failed"}
    ),  # failed only via deadline-exceeded sweep
    "running": frozenset({"succeeded", "failed", "cancelled", "crashed", "abandoned", "scheduled"}),
    "succeeded": frozenset(),
    "failed": frozenset(),
    "cancelled": frozenset(),
    "crashed": frozenset(),
    "abandoned": frozenset(),
}


def assert_valid_transition(
    from_status: JobStatus,
    to_status: JobStatus,
    job_id: JobId,
) -> None:
    """Raise :class:`~taskq.exceptions.IllegalStateTransition` if the
    transition is not in ``VALID_TRANSITIONS``.

    Parameter order: ``(from_status, to_status, job_id)``.
    """
    if to_status not in VALID_TRANSITIONS[from_status]:
        raise IllegalStateTransition(job_id, from_status, to_status)
