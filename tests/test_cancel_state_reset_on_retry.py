"""A retry write may only land on a row with a clean cancel slate, and a
cancel in flight must survive the actor's own write attempts.

The history here has two layers:

- A cancel that escalated to ``cancel_phase = 2`` in the same moment the
  actor raised an ordinary retryable exception used to survive the retry
  write, leaving ``cancel_phase`` and ``cancel_requested_at`` on the row.
  Retries reuse the SAME job row, so the next attempt was dispatched
  already at FORCED - the cancel controller's fast-advance then skipped
  straight past phase 2 without ever calling ``task.cancel()``, and the
  job could no longer be cancelled.
- Clearing the columns on the retry arm fixed that but introduced the
  mirror-image defect: a retryable failure landing during the cooperative
  grace WIPED the operator's acknowledged cancel (cancel_phase back to 0,
  cancel_requested_at back to NULL), disarmed the phase-2 escalation
  (guarded on cancel_phase = 1), and the actor ran again.

The merged contract: every arm that resets the cancel columns fences on
``cancel_phase = 0`` first, so a reset can only ever land on a row whose
columns were already clean, and an operator cancel in flight wins over
the infrastructure retry:

- ``mark_failed_or_retry``'s retry arm REFUSES a row carrying a cancel
  phase (the same ownership-mismatch no-op path a wrong-worker write
  takes, on both backends; the failure attempt is not retried by this
  path - the operator's cancel wins). The row stays 'running' with the
  audit columns intact, the cancel ladder still sees the in-flight
  request, and the escalation terminalises the job on schedule. A clean
  retry lands with a clean slate trivially: the fence guarantees the
  reset never raced an operator.
- The three deferral arms refuse a row carrying a cancel phase entirely
  (``noop``; the operator's cancel wins over the snooze) - the refuse
  leaves the audit columns intact for the cancel ladder to finish, and
  the clean slate comes from the interrupt release, not the deferral.
  These pins cover the FORCED escalation state; the COOPERATIVE twins
  live in tests/test_cancel_fence_arms.py.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import CancelPhase, ErrorInfo, JobId
from taskq.backend.postgres import PostgresBackend
from taskq.constants import MIN_DEFERRAL_INTERVAL
from taskq.exceptions import WorkerOwnershipMismatch
from taskq.testing.clock import FakeClock
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


async def _force_cancel_escalated(backend: Backend, job_id: JobId, phase: CancelPhase) -> None:
    """Put the running job in the state a cancel in flight leaves behind
    (phase 1 right after the operator's request, phase 2 after escalation)."""
    if isinstance(backend, InMemoryBackend):
        row = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: forcing a race-window state the public API cannot reach directly; mirrors tests/test_backend_equivalence.py
        backend._jobs[job_id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: same
            row,
            cancel_phase=phase,
            cancel_requested_at=datetime.now(UTC),
        )
        return
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors tests/test_backend_equivalence.py
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs yield PoolConnectionProxy | Unknown
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; the job id is $1-bound
            "SET cancel_phase = $2, cancel_requested_at = clock_timestamp() WHERE id = $1",
            job_id,
            int(phase),
        )


async def _make_deferred_row_dispatchable(backend: Backend, job_id: JobId) -> None:
    """A zero-delay non-consuming deferral is floored one interval out as
    ``scheduled`` (the deferral floor keeps it from monopolizing dispatch
    order), so make it due and promote it - exactly the wake + promotion
    pair the leader performs - before the next dispatch claims it."""
    if isinstance(backend, InMemoryBackend):
        cast("FakeClock", backend._clock).advance(  # pyright: ignore[reportPrivateUsage]  # Why: the in-memory fixture backends are FakeClock-backed; the Clock protocol does not carry advance().
            MIN_DEFERRAL_INTERVAL
        )
        await backend.scheduled_to_pending()
        return
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors _worker_of above.
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same.
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs, as above.
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608  # Why: schema is fixture-derived and _IDENT_RE-validated; the id is $-bound.
            "SET scheduled_at = clock_timestamp() - interval '1 second' "
            "WHERE id = $1",
            job_id,
        )
    await backend.scheduled_to_pending()


async def _assert_clean_slate_on_next_attempt(backend: Backend, job_id: JobId) -> None:
    """The retried row, and the attempt dispatched from it, carry no cancel state."""
    row = await backend.get(job_id)
    assert row is not None
    assert row.status in ("pending", "scheduled"), row.status
    assert row.cancel_phase == CancelPhase.NONE
    assert row.cancel_requested_at is None

    if row.status == "scheduled":
        await _make_deferred_row_dispatchable(backend, job_id)
    worker_id = await _dispatch(backend, job_id, await _worker_of(backend))
    flags = await backend.poll_cancel_flags(worker_id)
    assert [f for f in flags if f.job_id == job_id] == [], (
        "redispatched attempt was born with a stale cancel flag"
    )


@pytest.mark.parametrize("phase", [CancelPhase.COOPERATIVE, CancelPhase.FORCED])
async def test_mark_failed_or_retry_refuses_a_row_carrying_a_cancel_phase(
    backend_pair: Backend,
    phase: CancelPhase,
) -> None:
    """A retryable failure landing while an operator cancel is in flight
    must not reschedule the job.

    The retry arm's ``cancel_phase = 0`` fence declines the phase-carrying
    row: the write no-ops through the same ownership-mismatch path a
    wrong-worker write takes, the operator's audit columns survive, and
    the cancel ladder still sees the in-flight request (the phase-2
    escalation stays armed). The failure attempt is not retried by this
    path, which is the operator's cancel winning over the infrastructure
    retry.
    """
    job_id, worker_id = await _enqueue_and_dispatch(backend_pair)
    await _force_cancel_escalated(backend_pair, job_id, phase)

    with pytest.raises(WorkerOwnershipMismatch):
        await backend_pair.mark_failed_or_retry(job_id, worker_id, _ERROR, timedelta(0), attempt=1)

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status == "running", (
        "the retry write landed on a phase-carrying row and rescheduled it: "
        "the operator's acknowledged cancel was wiped and the actor would "
        "run again"
    )
    assert row.cancel_phase == phase, (
        "the operator's audit columns are never wiped by the retry arm"
    )
    assert row.cancel_requested_at is not None

    # The escalation controller still sees the in-flight cancel.
    flags = await backend_pair.poll_cancel_flags(worker_id)
    assert [f.job_id for f in flags if f.job_id == job_id] != [], (
        "the cancel ladder lost sight of the in-flight cancel: the retry "
        "write disarmed the phase-2 escalation"
    )


async def test_mark_failed_or_retry_retries_a_clean_row(
    backend_pair: Backend,
) -> None:
    """The control: a retryable failure with no cancel in flight retries
    normally, and the retried row carries a clean cancel slate (trivially
    so behind the fence - the reset can no longer race an operator)."""
    job_id, worker_id = await _enqueue_and_dispatch(backend_pair)

    await backend_pair.mark_failed_or_retry(job_id, worker_id, _ERROR, timedelta(0), attempt=1)

    await _assert_clean_slate_on_next_attempt(backend_pair, job_id)


async def test_mark_snoozed_refuses_a_row_carrying_a_cancel_phase(
    backend_pair: Backend,
) -> None:
    job_id, worker_id = await _enqueue_and_dispatch(backend_pair)
    await _force_cancel_escalated(backend_pair, job_id, CancelPhase.FORCED)

    outcome = await backend_pair.mark_snoozed(job_id, worker_id, timedelta(0), attempt=1)
    assert outcome == "noop"

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status == "running"
    assert row.cancel_phase == CancelPhase.FORCED
    assert row.cancel_requested_at is not None


@pytest.mark.parametrize("consume_budget", [True, False])
async def test_mark_retry_after_refuses_a_row_carrying_a_cancel_phase(
    backend_pair: Backend, consume_budget: bool
) -> None:
    job_id, worker_id = await _enqueue_and_dispatch(backend_pair)
    await _force_cancel_escalated(backend_pair, job_id, CancelPhase.FORCED)

    outcome = await backend_pair.mark_retry_after(
        job_id, worker_id, timedelta(0), consume_budget=consume_budget, attempt=1
    )
    assert outcome == "noop"

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status == "running"
    assert row.cancel_phase == CancelPhase.FORCED
    assert row.cancel_requested_at is not None


async def test_terminal_failure_preserves_cancel_state(backend_pair: Backend) -> None:
    """The reset is scoped to retries - a terminal fail keeps the audit trail."""
    job_id, worker_id = await _enqueue_and_dispatch(backend_pair)
    await _force_cancel_escalated(backend_pair, job_id, CancelPhase.FORCED)

    await backend_pair.mark_failed_or_retry(job_id, worker_id, _ERROR, None, attempt=1)

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status == "failed"
    assert row.cancel_phase == CancelPhase.FORCED
    assert row.cancel_requested_at is not None
