"""Regression: a retried attempt must start with a clean cancellation slate.

A cancel that escalates to ``cancel_phase = 2`` in the same moment the actor
raises an ordinary retryable exception used to survive the retry write: the
retry paths rewrote status/scheduled_at but left ``cancel_phase`` and
``cancel_requested_at`` in place.  Retries reuse the SAME job row, so the
next attempt was dispatched already at FORCED — the cancel controller's
PG-observation fast-advance then skips straight past phase 2 without ever
calling ``task.cancel()``, and the job can no longer be cancelled.

The crash-reclaim sweep (``_SWEEP_1_SQL``) and ``isolate_self`` already reset
both columns on their retry arm; these tests pin the same rule for the
consumer's own retry, snooze and retry-after writes, on both backends.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import CancelPhase, ErrorInfo, JobId
from taskq.backend.postgres import PostgresBackend
from taskq.testing.in_memory import InMemoryBackend

pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)
_ERROR = ErrorInfo(
    error_class="TransientError",
    error_message="boom",
    error_traceback=None,
)


async def _enqueue_and_dispatch(backend: Backend) -> tuple[JobId, UUID]:
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="actor_a",
            queue="default",
            payload={"k": "v"},
            max_attempts=5,
            retry_kind="transient",
            scheduled_at=_START,
        )
    )
    worker_id = await _worker_of(backend)
    return job_id, await _dispatch(backend, job_id, worker_id)


async def _worker_of(backend: Backend) -> UUID:
    """A worker id that exists in the backend's ``workers`` table."""
    if isinstance(backend, InMemoryBackend):
        return backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: canonical worker identity for InMemoryBackend; mirrors tests/test_backend_equivalence.py
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors tests/test_backend_equivalence.py
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
    worker_id = new_uuid()
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) VALUES ($1, $2, $3, $4)',  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; every value is $N-bound
            worker_id,
            "test-host",
            12345,
            ["default"],
        )
    return worker_id


async def _dispatch(backend: Backend, job_id: JobId, worker_id: UUID) -> UUID:
    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=10,
        lock_lease=_LOCK_LEASE,
    )
    assert job_id in {row.id for row in dispatched}
    return worker_id


async def _force_cancel_escalated(backend: Backend, job_id: JobId) -> None:
    """Put the running job in the state a phase-2 escalation leaves behind."""
    if isinstance(backend, InMemoryBackend):
        row = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: forcing a race-window state the public API cannot reach directly; mirrors tests/test_backend_equivalence.py
        backend._jobs[job_id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: same
            row,
            cancel_phase=CancelPhase.FORCED,
            cancel_requested_at=datetime.now(UTC),
        )
        return
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors tests/test_backend_equivalence.py
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; the job id is $1-bound
            "SET cancel_phase = 2, cancel_requested_at = clock_timestamp() WHERE id = $1",
            job_id,
        )


async def _assert_clean_slate_on_next_attempt(backend: Backend, job_id: JobId) -> None:
    """The retried row, and the attempt dispatched from it, carry no cancel state."""
    row = await backend.get(job_id)
    assert row is not None
    assert row.status in ("pending", "scheduled"), row.status
    assert row.cancel_phase == CancelPhase.NONE
    assert row.cancel_requested_at is None

    worker_id = await _dispatch(backend, job_id, await _worker_of(backend))
    flags = await backend.poll_cancel_flags(worker_id)
    assert [f for f in flags if f.job_id == job_id] == [], (
        "redispatched attempt was born with a stale cancel flag"
    )


async def test_mark_failed_or_retry_clears_cancel_state(backend_pair: Backend) -> None:
    job_id, worker_id = await _enqueue_and_dispatch(backend_pair)
    await _force_cancel_escalated(backend_pair, job_id)

    await backend_pair.mark_failed_or_retry(job_id, worker_id, _ERROR, timedelta(0))

    await _assert_clean_slate_on_next_attempt(backend_pair, job_id)


async def test_mark_snoozed_clears_cancel_state(backend_pair: Backend) -> None:
    job_id, worker_id = await _enqueue_and_dispatch(backend_pair)
    await _force_cancel_escalated(backend_pair, job_id)

    outcome = await backend_pair.mark_snoozed(job_id, worker_id, timedelta(0))
    assert outcome == "scheduled"

    await _assert_clean_slate_on_next_attempt(backend_pair, job_id)


@pytest.mark.parametrize("consume_budget", [True, False])
async def test_mark_retry_after_clears_cancel_state(
    backend_pair: Backend, consume_budget: bool
) -> None:
    job_id, worker_id = await _enqueue_and_dispatch(backend_pair)
    await _force_cancel_escalated(backend_pair, job_id)

    outcome = await backend_pair.mark_retry_after(
        job_id, worker_id, timedelta(0), consume_budget=consume_budget
    )
    assert outcome == "scheduled"

    await _assert_clean_slate_on_next_attempt(backend_pair, job_id)


async def test_terminal_failure_preserves_cancel_state(backend_pair: Backend) -> None:
    """The reset is scoped to retries — a terminal fail keeps the audit trail."""
    job_id, worker_id = await _enqueue_and_dispatch(backend_pair)
    await _force_cancel_escalated(backend_pair, job_id)

    await backend_pair.mark_failed_or_retry(job_id, worker_id, _ERROR, None)

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status == "failed"
    assert row.cancel_phase == CancelPhase.FORCED
    assert row.cancel_requested_at is not None
