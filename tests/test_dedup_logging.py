"""Tests for dedup behavioural outcomes on InMemoryBackend.

Covers:
  - unique_for dedup returns existing row
  - idempotency_key dedup returns existing row
  - fresh insert creates a new row (no dedup)
  - the dedup log lines match the PG path's unified contract:
    ``enqueue_deduplicated`` carrying ``status``, warning on a terminal
    target, info on a live one (PG side pinned in
    tests/test_silent_failure_guards.py)
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import structlog

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey, IdentityKey, JobFilter
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


def _keyed_args(key: IdempotencyKey) -> EnqueueArgs:
    """The repeated shape of the idempotency-key dedup pins below."""
    return EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        idempotency_key=key,
    )


def _unique_for_args(identity: IdentityKey) -> EnqueueArgs:
    """The repeated shape of the unique_for dedup pin below."""
    return EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        identity_key=identity,
        unique_for=timedelta(minutes=15),
        unique_states=("pending", "scheduled", "running"),
    )


def _sole_dedup_line(captured: list[dict[str, Any]]) -> dict[str, Any]:
    """The single ``enqueue_deduplicated`` line from a captured log run."""
    dedup_lines = [e for e in captured if e.get("event") == "enqueue_deduplicated"]
    assert dedup_lines, f"no dedup log line emitted; captured={captured}"
    return dedup_lines[0]


async def test_unique_for_dedup_returns_existing_row() -> None:
    backend = _make_backend()
    identity = IdentityKey("account:99")

    row1 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            identity_key=identity,
            unique_for=timedelta(minutes=15),
            unique_states=("pending", "scheduled", "running"),
        )
    )

    row2 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            identity_key=identity,
            unique_for=timedelta(minutes=15),
            unique_states=("pending", "scheduled", "running"),
        )
    )

    assert row1.id == row2.id


async def test_idempotency_key_dedup_returns_existing_row() -> None:
    backend = _make_backend()
    key = IdempotencyKey("dedup-key-1")

    row1 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            idempotency_key=key,
        )
    )

    row2 = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            idempotency_key=key,
        )
    )

    assert row1.id == row2.id


async def test_fresh_insert_creates_new_row() -> None:
    backend = _make_backend()

    row = await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_START,
            identity_key=IdentityKey("account:42"),
            unique_for=timedelta(minutes=15),
        )
    )

    assert row.id is not None


# ── dedup log parity with the PG path ────────────────────────────────────


async def test_idempotency_dedup_onto_terminal_job_warns_with_status() -> None:
    """A dedup hit whose target job is terminal must warn and name its status.

    The PG contract is pinned in tests/test_silent_failure_guards.py
    (TestTerminalDedupIsObservable); the mirror must emit the same
    ``enqueue_deduplicated`` event, carry the same ``status`` field, and be
    louder for a dead target than for a live one.
    """
    backend = _make_backend()
    key = IdempotencyKey("terminal-dedup-mem")

    first = await backend.enqueue(_keyed_args(key))
    cancelled = await backend.cancel_where(JobFilter(actor="test_actor"), reason="dedup-log-pin")
    assert cancelled.cancelled_ids == (first.id,), (
        "precondition: the target job must be terminal, or the pin asserts nothing"
    )

    with structlog.testing.capture_logs() as captured:
        second = await backend.enqueue(_keyed_args(key))

    assert second.id == first.id, "precondition: the terminal row still dedupes"

    line = _sole_dedup_line(captured)
    assert line.get("status") == "cancelled", (
        f"dedup onto a terminal job must record the target's status; got {line!r}"
    )
    assert line.get("log_level") == "warning", (
        "dedup onto a terminal job must be louder than a live-job hit; "
        f"got log_level={line.get('log_level')!r}"
    )


async def test_idempotency_dedup_onto_live_job_stays_info() -> None:
    """A dedup hit on a still-pending job is normal single-flight operation
    and must stay at info — carrying the status on every hit, not only
    terminal ones."""
    backend = _make_backend()
    key = IdempotencyKey("live-dedup-mem")

    first = await backend.enqueue(_keyed_args(key))

    with structlog.testing.capture_logs() as captured:
        second = await backend.enqueue(_keyed_args(key))

    assert second.id == first.id, "precondition: the live row dedupes"

    line = _sole_dedup_line(captured)
    assert line.get("log_level") == "info", (
        f"dedup onto a live job must stay at info; got log_level={line.get('log_level')!r}"
    )
    assert line.get("status") == "pending", (
        "the dedup line must carry the target's status on every hit, "
        f"not only terminal ones; got {line!r}"
    )


async def test_unique_for_dedup_line_matches_the_unified_contract() -> None:
    """The unique_for arm emits the same unified ``enqueue_deduplicated``
    event, carrying the target's status at info — matching the PG path,
    whose unique_for preflight only matches rows in ``unique_states``
    (active by default), so a hit there is always normal single-flight
    operation."""
    backend = _make_backend()
    identity = IdentityKey("account:7")

    first = await backend.enqueue(_unique_for_args(identity))

    with structlog.testing.capture_logs() as captured:
        second = await backend.enqueue(_unique_for_args(identity))

    assert second.id == first.id, "precondition: the unique_for row dedupes"

    line = _sole_dedup_line(captured)
    assert line.get("log_level") == "info"
    assert line.get("status") == "pending", f"dedup line missing status; got {line!r}"
    assert line.get("dedup_reason") == "unique_for"
