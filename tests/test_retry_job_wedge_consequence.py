# ruff: noqa: S608  # Why: schema is the fixture's validated identifier throughout; every value is $-bound.

"""CONTRACT UNDER ATTACK: the admin ``retry_job`` escape hatch leaves the
re-run job resolvable by the queue's own machinery.

``retry_job`` keeps ``attempt`` monotonic (never reset — the Oban/River
admin-retry precedent) and raises the ``max_attempts`` ceiling so the
budget gates open, so a re-dispatched job climbs to fresh attempt
numbers and no attempt-row write revisits a spent epoch's
``job_attempts`` PRIMARY KEY (job_id, attempt). This file pins the
consequence contracts that motivated the fix; a reintroduced epoch
reset would make every one of them fire again:

- the worker's terminal-write infra family
  (``_TERMINAL_WRITE_INFRA_EXCEPTIONS``, which admits every
  ``asyncpg.PostgresError`` and therefore ``UniqueViolationError``)
  must not swallow a revisited-key collision and strand the job
  ``running`` on a lease only the reclaim sweep can expire;
- the production reclaim entry (``reclaim_expired_locks``) resolves a
  lease-expired running row instead of dying on the same spent key;
- the actor does not re-execute per lease period while the row sits
  wedged.

Every step of both cycles runs through production paths: enqueue,
dispatch, the real consumer (``consume_one_job`` — the actor executes
and its failure write goes through the real handler and fused terminal
statement), ``retry_job``, and ``reclaim_expired_locks``.
"""

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.backend.clock import SystemClock
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.pg import create_worker
from taskq.worker._consumer import consume_one_job

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    from taskq.backend.postgres import PostgresBackend

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    PostgresBackend = object  # pyright: ignore[reportInvalidTypeForm]  # Why: runtime fallback — PostgresBackend is TYPE_CHECKING-only so the test import surface stays asyncpg-free at collection time.
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm]  # Why: runtime fallback — asyncpg is TYPE_CHECKING-only in test modules, matching tests/test_snooze_round_trip_pg.py.

pytestmark = pytest.mark.integration

_LOCK_LEASE = timedelta(minutes=5)
_GRACE = timedelta(seconds=30)
_RECLAIM_CYCLES = 3


async def _expire_lease(conn: _Conn, schema: str, job_id: UUID) -> None:
    """Put the row's lock lease in the past (time control only — the same
    shape ``create_running_job``'s ``lock_expires_at`` parameter seeds)."""
    await conn.execute(
        f"""UPDATE "{schema}".jobs
        SET lock_expires_at = now() - interval '10 seconds'
        WHERE id = $1""",
        job_id,
    )


async def _wake_scheduled(conn: _Conn, schema: str, job_id: UUID) -> None:
    """Fast-forward a retry backoff so promotion+dispatch can claim the row
    without waiting real seconds (time control only)."""
    await conn.execute(
        f"""UPDATE "{schema}".jobs
        SET scheduled_at = now() - interval '1 second'
        WHERE id = $1""",
        job_id,
    )


def _failing_actor(
    calls: list[int],
) -> Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]]:
    """An actor whose body is the observable side effect: every execution
    appends to *calls* before failing."""

    async def actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        calls.append(len(calls) + 1)
        raise RuntimeError("wedge-cycle boom")

    return actor


async def _consume(
    backend: PostgresBackend,
    job: JobRow,
    worker_id: UUID,
    run_actor: Callable[[JobRow, JobContext[BaseModel]], Awaitable[object]],
    *,
    max_attempts: int,
) -> str:
    """Run the production consumer over one dispatched job — the same call
    the in-memory ``run_until_drained`` makes, here against PG so the
    actor's failure goes through the real handler, fused terminal
    statement, and terminal-write infra classification."""
    return await consume_one_job(
        backend,
        job,
        worker_id,
        run_actor=run_actor,
        actor_config=StubActorConfig(
            retry=RetryPolicy(kind="transient", max_attempts=max_attempts, jitter=0.0)
        ),
        payload_type=EmptyPayload,
        clock=SystemClock(),
    )


async def _enqueue_failing_job(backend: PostgresBackend, *, max_attempts: int) -> UUID:
    """Enqueue an immediately-dispatchable failing job on ``test_actor``
    (a DEFAULT_ACTORS row, so the dispatch CTE admits it)."""
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test_actor",
            queue="default",
            payload={},
            max_attempts=max_attempts,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )
    return job_id


async def test_epoch2_collision_must_not_strand_job_running(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """One re-run cycle on a single-attempt job: terminal at attempt 1 →
    retry_job (attempt stays 1, ceiling rises to 2) → re-dispatch climbs
    to the fresh attempt 2 → the actor executes and its failure write
    lands at the fresh (job_id, 2) key. Under a reintroduced epoch reset
    the re-dispatch would revisit the spent (job_id, 1) key; the consumer
    must not swallow that collision as terminal-write infra and leave the
    row running."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    calls: list[int] = []

    job_id = await _enqueue_failing_job(backend, max_attempts=1)
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    # Epoch 1: the terminal write lands the spent key (job_id, 1).
    rows = await backend.dispatch_batch(worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE)
    assert [r.id for r in rows] == [job_id]
    await _consume(backend, rows[0], worker_id, _failing_actor(calls), max_attempts=1)
    spent = await backend.get(job_id)
    assert spent is not None and spent.status == "failed" and spent.attempt == 1
    assert [a.attempt for a in await backend.get_attempts(job_id)] == [1]

    # The operator's escape hatch: attempt stays at its spent value (the
    # admin-retry precedent never resets the counter) and the ceiling
    # rises so the budget gates open; the row re-pends.
    assert await backend.retry_job(job_id)
    repended = await backend.get(job_id)
    assert repended is not None and repended.status == "pending"
    assert repended.attempt == 1, (
        "MONOTONIC-ATTEMPT CONTRACT: retry_job must never reset the attempt "
        f"counter — observed {repended.attempt!r}. A reset revisits the spent "
        "epoch's attempt numbers and every attempt-row writer collides on "
        "job_attempts_pkey (the wedge this suite pins)."
    )
    assert repended.max_attempts == 2, (
        "CEILING-RAISE CONTRACT: retry_job must raise max_attempts to "
        "GREATEST(max_attempts, attempt + 1) so the budget gates open for the "
        f"re-run — observed max_attempts={repended.max_attempts!r}."
    )

    # Epoch 2: dispatch climbs to the fresh attempt number 2 — the spent 1
    # is never revisited — and the actor re-executes.
    rows = await backend.dispatch_batch(worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE)
    assert [r.id for r in rows] == [job_id]
    claimed = await backend.get(job_id)
    assert claimed is not None and claimed.status == "running" and claimed.attempt == 2
    outcome = await _consume(backend, rows[0], worker_id, _failing_actor(calls), max_attempts=1)
    assert len(calls) == 2, "the actor must have executed in both epochs"

    after = await backend.get(job_id)
    attempts_after = await backend.get_attempts(job_id)
    assert after is not None, "the job row vanished"
    assert after.status != "running", (
        "the epoch-2 terminal write collided on the spent (job_id, attempt) "
        "key and the consumer swallowed it as terminal-write infra "
        "(_TERMINAL_WRITE_INFRA_EXCEPTIONS admits asyncpg.PostgresError): "
        f"it reported outcome={outcome!r} while writing nothing — the "
        f"actor's failure is unrecorded (error_class={after.error_class!r}), "
        f"the row is stranded running on a lease only the reclaim sweep can "
        f"expire, and every attempt-row writer revisits the same spent key. "
        f"observed: status={after.status!r} attempt={after.attempt} "
        f"attempt_rows={[a.attempt for a in attempts_after]} "
        f"actor_calls={len(calls)}"
    )


async def test_epoch2_wedge_must_be_resolvable_by_reclaim_sweep(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """The full cycle on a retryable job whose whole attempt budget
    is spent: attempts 1..3 terminal → retry_job (attempt stays 3,
    ceiling rises to 4) → re-dispatch climbs to the fresh attempt 4 →
    the actor executes and the failure write lands at the fresh
    (job_id, 4) key → the lease expires → the production reclaim entry
    runs, one cycle per lease period, each followed by a wake+dispatch
    probe that claims whatever the sweep actually re-pended. The row
    must be resolvable — under a reintroduced epoch reset the re-run
    write collides on the spent (job_id, 1) key, the row strands
    running, and every reclaim cycle dies on the same spent key; the
    re-pend branch is exactly the machinery that would re-execute the
    actor per lease period — and the row must not still be running
    after it."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    calls: list[int] = []
    actor = _failing_actor(calls)

    job_id = await _enqueue_failing_job(backend, max_attempts=3)
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)

    # Epoch 1: spend the whole attempt budget through the real consumer.
    for attempt_no in (1, 2, 3):
        if attempt_no > 1:
            async with deps.worker_pool.acquire() as conn:
                await _wake_scheduled(conn, schema, job_id)
            await backend.scheduled_to_pending()
        rows = await backend.dispatch_batch(worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE)
        assert [r.id for r in rows] == [job_id]
        await _consume(backend, rows[0], worker_id, actor, max_attempts=3)
    exhausted = await backend.get(job_id)
    assert exhausted is not None and exhausted.status == "failed" and exhausted.attempt == 3
    assert sorted(a.attempt for a in await backend.get_attempts(job_id)) == [1, 2, 3]

    # The escape hatch, then the re-run: the actor executes a fourth time
    # and its failure write lands at the fresh (job_id, 4) key — the spent
    # 1..3 are never revisited.
    assert await backend.retry_job(job_id)
    rows = await backend.dispatch_batch(worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE)
    assert [r.id for r in rows] == [job_id]
    await _consume(backend, rows[0], worker_id, actor, max_attempts=3)
    assert len(calls) == 4, "the actor must have executed once per epoch-1 attempt plus the re-run"

    # The lease expires; the reclaim machinery gets its chance, repeatedly.
    async with deps.worker_pool.acquire() as conn:
        await _expire_lease(conn, schema, job_id)

    sweep_errors: list[Exception] = []
    probe_claimed: list[str] = []
    for _ in range(_RECLAIM_CYCLES):
        try:
            await backend.reclaim_expired_locks(_GRACE, _GRACE)
        except Exception as exc:
            sweep_errors.append(exc)
        async with deps.worker_pool.acquire() as conn:
            await _wake_scheduled(conn, schema, job_id)
        await backend.scheduled_to_pending()
        claimed = await backend.dispatch_batch(
            worker_id, ["default"], limit=1, lock_lease=_LOCK_LEASE
        )
        if claimed:
            probe_claimed.append(str(claimed[0].id))
            await _consume(backend, claimed[0], worker_id, actor, max_attempts=3)

    final = await backend.get(job_id)
    final_attempts = await backend.get_attempts(job_id)
    assert final is not None, "the job row vanished"
    assert sweep_errors == [], (
        "the production reclaim entry died on the wedged row instead of "
        "resolving it — the sweep's own job_attempts INSERT revisits the "
        "same spent (job_id, attempt) key, its transaction rolls back, and "
        "the row can never leave running: no re-pend, no terminal label, "
        "no crash label can ever land, and the actor's fourth execution "
        "stays unrecorded. observed: "
        f"sweep_errors={[repr(e) for e in sweep_errors]} "
        f"status={final.status!r} attempt={final.attempt} "
        f"attempt_rows={sorted(a.attempt for a in final_attempts)} "
        f"actor_calls={len(calls)} probes_claimed={probe_claimed}"
    )
    assert final.status != "running", (
        "the reclaim cycles left the row running: every lease period the "
        "sweep retries the same spent key, so the row never leaves running "
        "and the job is a silent zombie. observed: "
        f"status={final.status!r} attempt={final.attempt} "
        f"attempt_rows={sorted(a.attempt for a in final_attempts)} "
        f"actor_calls={len(calls)} probes_claimed={probe_claimed}"
    )
