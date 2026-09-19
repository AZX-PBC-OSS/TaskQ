"""A crash reclaim must respect the job's retry-kind budget, on both backends.

Reclaim is the one budget arbiter that runs without the job's actor
having said anything. When a lease expires, the backend decides on the
job's behalf whether there is another attempt to give it. The consumer's
own failure path (``mark_failed_or_retry``) and the deferral arms
(``mark_snoozed``) both read ``retry_kind`` as the primary budget
dimension: an ``indefinite`` job retries until its ``schedule_to_close``
deadline, and ``max_attempts`` is documented as ignored for its retry
decision. These tests hold reclaim to the same contract.

The operator stake: ``indefinite`` is the kind chosen for work that must
outlive transient failure - waiting out a downstream outage, a long
retry window against a flaky third party. Terminalising such a job on a
worker crash, while its deadline budget is still open, turns a routine
pod restart into silent data loss with no cause recorded on the row.

Attempt accounting through a reclaim is pinned here too: reclaim must
leave the counter where it is (dispatch owns the increment) so a later
retry cannot revisit a spent ``(job, attempt)`` identity, which the
``job_attempts`` primary key forbids.
"""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend import Backend, EnqueueArgs
from taskq.backend._protocol import ErrorInfo, JobId
from taskq.backend.postgres import PostgresBackend
from taskq.testing.in_memory import InMemoryBackend

pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LOCK_LEASE = timedelta(seconds=60)
_GRACE = timedelta(seconds=30)
_ERROR = ErrorInfo(
    error_class="TransientError",
    error_message="boom",
    error_traceback=None,
)


async def _worker_of(backend: Backend) -> UUID:
    """A worker id that exists in the backend's ``workers`` table."""
    if isinstance(backend, InMemoryBackend):
        return backend._worker_id  # pyright: ignore[reportPrivateUsage]  # Why: canonical worker identity for InMemoryBackend; mirrors tests/test_cancel_state_reset_on_retry.py
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors tests/test_cancel_state_reset_on_retry.py
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


async def _enqueue(
    backend: Backend,
    *,
    retry_kind: str,
    max_attempts: int,
    schedule_to_close: datetime | None = None,
) -> JobId:
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="actor_a",
            queue="default",
            payload={"k": "v"},
            max_attempts=max_attempts,
            retry_kind=retry_kind,
            scheduled_at=_START,
            schedule_to_close=schedule_to_close,
        )
    )
    return job_id


async def _dispatch(backend: Backend, job_id: JobId, worker_id: UUID) -> None:
    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=10,
        lock_lease=_LOCK_LEASE,
    )
    assert job_id in {row.id for row in dispatched}, (
        "the scenario requires the job to have been claimed for an attempt"
    )


async def _expire_lease(backend: Backend, job_id: JobId) -> None:
    """Age the job's lease into the past - the state a crashed worker
    leaves behind, with no terminal write ever arriving."""
    if isinstance(backend, InMemoryBackend):
        row = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: forcing the crashed-holder state the public API cannot reach directly; mirrors tests/test_cancel_state_reset_on_retry.py
        backend._jobs[job_id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: same
            row,
            lock_expires_at=backend._clock.now() - timedelta(seconds=10),  # pyright: ignore[reportPrivateUsage]  # Why: the reclaim predicate is arbitrated by the backend's own clock, so the seed must be in that domain
        )
        return
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors _worker_of above
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs, as above
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; the id is $-bound
            "SET lock_expires_at = clock_timestamp() - interval '10 seconds' WHERE id = $1",
            job_id,
        )


async def _run_attempts_to(backend: Backend, job_id: JobId, target_attempt: int) -> None:
    """Drive the job through real claim/fail cycles until its counter
    reaches *target_attempt*, leaving it running at that attempt."""
    worker_id = await _worker_of(backend)
    while True:
        await _dispatch(backend, job_id, worker_id)
        row = await backend.get(job_id)
        assert row is not None
        if row.attempt >= target_attempt:
            return
        await backend.mark_failed_or_retry(
            job_id, worker_id, _ERROR, timedelta(0), attempt=row.attempt
        )
        await _make_due(backend, job_id)


async def _make_due(backend: Backend, job_id: JobId) -> None:
    """Promote a job deferred by the retry floor back into the pending set -
    the wake plus promotion the leader performs."""
    if isinstance(backend, InMemoryBackend):
        row = backend._jobs[job_id]  # pyright: ignore[reportPrivateUsage]  # Why: nudging scheduled_at is the deterministic twin of PG's clock-relative update below
        backend._jobs[job_id] = replace(  # pyright: ignore[reportPrivateUsage]  # Why: same
            row,
            scheduled_at=backend._clock.now() - timedelta(seconds=1),  # pyright: ignore[reportPrivateUsage]  # Why: the promotion predicate reads the backend's own clock
        )
        await backend.scheduled_to_pending()
        return
    assert isinstance(backend, PostgresBackend)
    schema: str = backend._schema_name  # pyright: ignore[reportPrivateUsage]  # Why: PG-path helper mirrors _worker_of above
    pool = backend._worker_pool  # pyright: ignore[reportPrivateUsage]  # Why: same
    async with pool.acquire() as conn:  # pyright: ignore[reportUnknownVariableType]  # Why: asyncpg stubs, as above
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # noqa: S608 # Why: schema is fixture-derived and _IDENT_RE-validated; the id is $-bound
            "SET scheduled_at = clock_timestamp() - interval '1 second' WHERE id = $1",
            job_id,
        )
    await backend.scheduled_to_pending()


async def test_reclaim_hands_back_an_indefinite_job_past_max_attempts(
    backend_pair: Backend,
) -> None:
    """An ``indefinite`` job whose worker crashed after more attempts than
    ``max_attempts`` must be handed back, not terminalised.

    ``max_attempts`` does not bound an ``indefinite`` job's retries - its
    ``schedule_to_close`` deadline does, and here that deadline is hours
    away. A caller who chose this kind precisely so the work would
    survive transient failure must not lose it to a worker crash.
    """
    deadline = datetime.now(UTC) + timedelta(hours=6)
    job_id = await _enqueue(
        backend_pair,
        retry_kind="indefinite",
        max_attempts=2,
        schedule_to_close=deadline,
    )
    await _run_attempts_to(backend_pair, job_id, target_attempt=3)

    before = await backend_pair.get(job_id)
    assert before is not None
    assert before.attempt > before.max_attempts, (
        "the scenario requires an indefinite job that has already run more "
        "attempts than max_attempts - the state only this kind reaches"
    )

    await _expire_lease(backend_pair, job_id)
    reclaimed = await backend_pair.reclaim_expired_locks(_GRACE, _GRACE)
    assert reclaimed == 1

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status in ("pending", "scheduled"), (
        f"a crash reclaim terminalised an indefinite job as {row.status!r} while "
        f"its schedule_to_close deadline was still open. max_attempts is not this "
        f"kind's budget, so the job had attempts left to give; the operator sees "
        f"work configured to outlive failure killed by a pod restart, with "
        f"error_class={row.error_class!r} on the row"
    )
    assert row.finished_at is None, (
        "a job handed back for another attempt must not carry a finished_at"
    )


async def test_reclaim_terminalises_a_transient_job_that_spent_its_budget(
    backend_pair: Backend,
) -> None:
    """The complement: a ``transient`` job whose budget really is spent is
    terminalised by reclaim, so the indefinite carve-out cannot be read as
    "reclaim never ends a job"."""
    job_id = await _enqueue(backend_pair, retry_kind="transient", max_attempts=2)
    await _run_attempts_to(backend_pair, job_id, target_attempt=2)

    await _expire_lease(backend_pair, job_id)
    reclaimed = await backend_pair.reclaim_expired_locks(_GRACE, _GRACE)
    assert reclaimed == 1

    row = await backend_pair.get(job_id)
    assert row is not None
    assert row.status == "crashed", (
        f"a transient job at its max_attempts must be terminalised by reclaim, got {row.status!r}"
    )
    assert row.finished_at is not None


async def test_reclaim_leaves_the_attempt_counter_for_dispatch_to_advance(
    backend_pair: Backend,
) -> None:
    """A reclaimed job must come back at the attempt it was reclaimed at,
    and the next claim must advance it.

    Attempt numbers are the identity of an execution - ``job_attempts`` is
    keyed on ``(job_id, attempt)``. If reclaim advanced the counter as
    well as dispatch, a job would skip identities; if reclaim rolled it
    back, the next terminal write would land on a key the reclaim's own
    audit row already holds and the write would fail.
    """
    job_id = await _enqueue(backend_pair, retry_kind="transient", max_attempts=5)
    await _run_attempts_to(backend_pair, job_id, target_attempt=2)

    await _expire_lease(backend_pair, job_id)
    assert await backend_pair.reclaim_expired_locks(_GRACE, _GRACE) == 1

    reclaimed_row = await backend_pair.get(job_id)
    assert reclaimed_row is not None
    assert reclaimed_row.attempt == 2, (
        f"reclaim moved the attempt counter to {reclaimed_row.attempt}; the "
        f"counter belongs to dispatch, and a reclaim that shifts it makes the "
        f"reclaimed attempt's audit row disagree with the row it describes"
    )

    await _make_due(backend_pair, job_id)
    worker_id = await _worker_of(backend_pair)
    await _dispatch(backend_pair, job_id, worker_id)

    next_row = await backend_pair.get(job_id)
    assert next_row is not None
    assert next_row.attempt == 3, (
        f"the attempt after a reclaim must be a fresh identity, got "
        f"{next_row.attempt} following a reclaim at 2"
    )

    written = await backend_pair.mark_failed_or_retry(
        job_id, worker_id, _ERROR, timedelta(0), attempt=next_row.attempt
    )
    assert written is not None, (
        "the terminal write for the attempt following a reclaim did not land - a "
        "retry after a reclaim must not collide with the reclaim's own audit row"
    )

    attempts = await backend_pair.get_attempts(job_id)
    numbers = [a.attempt for a in attempts]
    assert len(numbers) == len(set(numbers)), (
        f"a (job, attempt) identity was recorded twice across the reclaim: {numbers}"
    )
    assert numbers == sorted(numbers), (
        f"attempt identities must be recorded in increasing order, got {numbers}"
    )
