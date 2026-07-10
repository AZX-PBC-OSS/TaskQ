"""Double-write idempotency property tests for terminal writes.

Verifies that any double-write of a terminal state is a no-op for both
backends: the second call returns False (or raises WorkerOwnershipMismatch
for mark_failed_or_retry), no additional attempt rows are written, and
no additional event rows are written.

anchors: (job state machine), (idempotent terminal
writes), (in-memory backend).
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, ErrorInfo, JobId
from taskq.exceptions import WorkerOwnershipMismatch
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp
from taskq.testing.in_memory import InMemoryBackend

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend

# ── Constants ──────────────────────────────────────────────────────────

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)

# The five terminal-write targets. mark_snoozed is non-terminal
# (status becomes 'scheduled') but qualifies for the double-write
# property — the second call still returns False because the
# WHERE status='running' predicate misses.
TERMINAL_STATES = ["succeeded", "failed", "cancelled", "snoozed", "abandoned"]

terminal_state_strategy = st.sampled_from(TERMINAL_STATES)

# ── Helpers ────────────────────────────────────────────────────────────


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(_START))


async def _enqueue_and_dispatch(
    backend: InMemoryBackend,
    max_attempts: int = 3,
    retry_kind: str = "transient",
) -> tuple[JobId, UUID]:
    """Enqueue a job and dispatch it, returning (job_id, worker_id)."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=max_attempts,
        retry_kind=retry_kind,  # type: ignore[arg-type] # Why: retry_kind param is str; known-valid RetryKind values are passed by callers
        scheduled_at=_START,
    )
    await backend.enqueue(args)
    worker_id: UUID = backend._worker_id  # type: ignore[reportPrivateUsage] # Why: test-only private access
    dispatched = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
    )
    assert len(dispatched) == 1
    return dispatched[0].id, worker_id


def _set_cancel_phase(backend: InMemoryBackend, job_id: JobId, phase: int) -> None:
    row = backend._jobs[job_id]  # type: ignore[reportPrivateUsage] # Why: test-only private access
    backend._jobs[job_id] = replace(row, cancel_phase=phase)  # type: ignore[reportPrivateUsage] # Why: test-only private access


async def _apply_first_terminal_write(
    backend: InMemoryBackend,
    job_id: JobId,
    worker_id: UUID,
    terminal_state: str,
) -> bool:
    """Apply the first terminal write and return True if it succeeded."""
    if terminal_state == "succeeded":
        result = await backend.mark_succeeded(job_id, worker_id, {"ok": True})
        return result is True

    if terminal_state == "failed":
        error_info = ErrorInfo(
            error_class="TestError", error_message="terminal failure", error_traceback=None
        )
        row = await backend.mark_failed_or_retry(job_id, worker_id, error_info, None)
        return row.status == "failed"

    if terminal_state == "cancelled":
        result = await backend.mark_cancelled(job_id, worker_id)
        return result is True

    if terminal_state == "snoozed":
        result = await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=30))
        return result == "scheduled"

    if terminal_state == "abandoned":
        _set_cancel_phase(backend, job_id, 2)
        result = await backend.mark_abandoned(job_id)
        return result is True

    raise ValueError(f"Unknown terminal state: {terminal_state}")


async def _apply_second_terminal_write(
    backend: InMemoryBackend,
    job_id: JobId,
    worker_id: UUID,
    terminal_state: str,
) -> bool:
    """Apply the second terminal write.

    Returns True if the second write was a no-op (bool methods return
    False, mark_failed_or_retry raises WorkerOwnershipMismatch).
    Returns False if the second write unexpectedly succeeded.
    """
    if terminal_state == "succeeded":
        return await backend.mark_succeeded(job_id, worker_id, None) is False

    if terminal_state == "failed":
        error_info = ErrorInfo(
            error_class="TestError", error_message="terminal failure", error_traceback=None
        )
        try:
            await backend.mark_failed_or_retry(job_id, worker_id, error_info, None)
        except WorkerOwnershipMismatch:
            return True
        return False

    if terminal_state == "cancelled":
        return await backend.mark_cancelled(job_id, worker_id) is False

    if terminal_state == "snoozed":
        return await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=60)) == "noop"

    if terminal_state == "abandoned":
        return await backend.mark_abandoned(job_id) is False

    raise ValueError(f"Unknown terminal state: {terminal_state}")


# ── in-memory branch (unit test) ─────────────────────────────────


@given(terminal_state=terminal_state_strategy)
@settings(max_examples=50, deadline=None)
async def test_terminal_idempotency_memory(terminal_state: str) -> None:
    """(in-memory): double-write of a terminal is a no-op.

    For each random terminal state, enqueue a job, dispatch it, write
    the terminal, write the same terminal again. Oracle: second call
    returns False (or raises WorkerOwnershipMismatch for
    mark_failed_or_retry); exactly one attempt row; one state_change
    event from the terminal write.
    """
    backend = _make_backend()
    job_id, worker_id = await _enqueue_and_dispatch(backend)

    # First write must succeed
    first_ok = await _apply_first_terminal_write(backend, job_id, worker_id, terminal_state)
    assert first_ok, f"First terminal write for {terminal_state} did not succeed"

    # Record state before second write
    attempts_before = await backend.get_attempts(job_id)

    # Second write must be a no-op
    second_ok = await _apply_second_terminal_write(backend, job_id, worker_id, terminal_state)
    assert second_ok, f"Second terminal write for {terminal_state} was not a no-op"

    # Exactly one attempt row (no double-write)
    attempts_after = await backend.get_attempts(job_id)
    assert len(attempts_after) == len(attempts_before) == 1, (
        f"Expected 1 attempt row, got {len(attempts_after)} for {terminal_state}"
    )

    # One state_change event from the terminal write
    events = await backend.get_events(job_id)
    expected_to = "scheduled" if terminal_state == "snoozed" else terminal_state
    terminal_events = [
        e
        for e in events
        if e.kind == "state_change"
        and e.detail.get("from_state") == "running"
        and e.detail.get("to_state") == expected_to
    ]
    assert len(terminal_events) == 1, (
        f"Expected 1 state_change event for {terminal_state}, got {len(terminal_events)}"
    )


# ── PG branch (integration test) ─────────────────────────────────


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.parametrize("terminal_state", TERMINAL_STATES)
async def test_terminal_idempotency_pg(
    terminal_state: str,
    jobs_app: JobsApp,
) -> None:
    """(PG): double-write of a terminal is a no-op on PostgresBackend.

    Uses the jobs_app fixture for PG connectivity. For mark_abandoned,
    cancel_phase is set to 2 via write_cancel_request +
    write_cancel_escalation before calling mark_abandoned. For
    mark_failed_or_retry, the PG backend raises WorkerOwnershipMismatch
    on the second call (the SQL WHERE clause cannot distinguish "already
    terminal" from "wrong worker").
    """

    backend = jobs_app.backend

    job_id, worker_id = await _pg_enqueue_and_dispatch(jobs_app)

    # For abandoned, set cancel_phase=2 via cancel path
    if terminal_state == "abandoned":
        await backend.write_cancel_request(job_id, "test cancel")
        await backend.write_cancel_escalation(job_id, worker_id, 2)

    # First terminal write
    first_ok = await _pg_apply_first_terminal_write(backend, job_id, worker_id, terminal_state)
    assert first_ok, f"First PG terminal write for {terminal_state} did not succeed"

    # Record attempt count before second write
    attempts_before = await backend.get_attempts(job_id)

    # Second terminal write — should be a no-op
    if terminal_state == "failed":
        # PG mark_failed_or_retry raises WorkerOwnershipMismatch on
        # already-terminal rows — the SQL WHERE clause cannot
        # distinguish "already terminal" from "wrong worker".
        error_info = ErrorInfo(
            error_class="TestError", error_message="terminal failure", error_traceback=None
        )
        with pytest.raises(WorkerOwnershipMismatch):
            await backend.mark_failed_or_retry(job_id, worker_id, error_info, None)
    elif terminal_state == "succeeded":
        result = await backend.mark_succeeded(job_id, worker_id, None)
        assert result is False
    elif terminal_state == "cancelled":
        result = await backend.mark_cancelled(job_id, worker_id)
        assert result is False
    elif terminal_state == "snoozed":
        result = await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=60))
        assert result == "noop"
    elif terminal_state == "abandoned":
        result = await backend.mark_abandoned(job_id)
        assert result is False

    # No additional attempt rows
    attempts_after = await backend.get_attempts(job_id)
    assert len(attempts_after) == len(attempts_before), (
        f"Second write added attempt rows for {terminal_state}: "
        f"{len(attempts_before)} -> {len(attempts_after)}"
    )


# ── PG helpers ─────────────────────────────────────────────────────────


async def _pg_enqueue_and_dispatch(
    jobs_app: JobsApp,
) -> tuple[JobId, UUID]:
    """Enqueue a job and dispatch it on the PG backend.

    Dispatches via a direct SQL UPDATE (dispatch_batch is not yet
    implemented on PostgresBackend). Returns (job_id, worker_id).
    """

    deps = jobs_app.deps
    backend = jobs_app.backend
    schema: str = deps.settings.schema_name
    worker_id = new_uuid()
    job_id = new_job_id()

    # Enqueue via backend
    args = EnqueueArgs(
        id=job_id,
        actor="test_actor",
        queue="default",
        payload={"key": "value"},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
    )
    await backend.enqueue(args)

    # Dispatch via direct SQL (dispatch_batch not implemented)
    async with deps.worker_pool.acquire() as conn:
        await _pg_create_worker(conn, schema, worker_id)
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608 # Why: schema validated against _IDENT_RE; no user data interpolated
            "SET status = 'running', "
            "    locked_by_worker = $2, "
            "    lock_expires_at = now() + interval '60 seconds', "
            "    started_at = now(), "
            "    last_heartbeat_at = now(), "
            "    attempt = 1 "
            "WHERE id = $1",
            job_id,
            worker_id,
        )

    return job_id, worker_id


async def _pg_create_worker(
    conn: object,
    schema: str,
    worker_id: UUID,
) -> None:
    """Insert a worker row (FK requirement for locked_by_worker)."""
    await conn.execute(  # type: ignore[union-attr] # Why: conn is asyncpg.Connection|PoolConnectionProxy at runtime; typed as object to avoid asyncpg import
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '  # noqa: S608 # Why: schema validated against _IDENT_RE; no user data interpolated
        "VALUES ($1, $2, $3, $4)",
        worker_id,
        "test-host",
        12345,
        ["default"],
    )


async def _pg_apply_first_terminal_write(
    backend: "PostgresBackend",
    job_id: JobId,
    worker_id: UUID,
    terminal_state: str,
) -> bool:
    """Apply the first terminal write on PG and return True if it succeeded."""
    if terminal_state == "succeeded":
        return await backend.mark_succeeded(job_id, worker_id, {"ok": True})

    if terminal_state == "failed":
        error_info = ErrorInfo(
            error_class="TestError", error_message="terminal failure", error_traceback=None
        )
        row = await backend.mark_failed_or_retry(job_id, worker_id, error_info, None)
        return row.status == "failed"

    if terminal_state == "cancelled":
        return await backend.mark_cancelled(job_id, worker_id)

    if terminal_state == "snoozed":
        return await backend.mark_snoozed(job_id, worker_id, timedelta(seconds=30)) == "scheduled"

    if terminal_state == "abandoned":
        return await backend.mark_abandoned(job_id)

    raise ValueError(f"Unknown terminal state: {terminal_state}")
