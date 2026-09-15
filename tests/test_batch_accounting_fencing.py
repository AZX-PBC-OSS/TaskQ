# ruff: noqa: S608  # Why: schema is a per-test fixture identifier, not user input; every value is $-bound.

"""Batch accounting must move only for attempts that actually terminated.

A batch's ``consecutive_failures`` streak is the input to its abort
policy: a batch created with ``AbortBatchAfter(n)`` stops dispatching
further members once *n* member jobs fail back to back. A ``succeeded``
member resets that streak to zero.

The streak therefore has to answer one question truthfully — *did a
member job really reach a successful terminal state?* — and a reclaimed
attempt is precisely the case where the honest answer is no. When a
worker stalls past its lock lease, the leader's sweep re-pends the job
and the fleet re-dispatches it at a newer attempt, the stalled worker's
handler eventually finishes and issues a terminal write that is fenced
on ``(id, status, locked_by_worker, attempt)``. That write lands on no
row. The job is still running somewhere else; nothing about it has been
decided. The attempt must not be allowed to move batch state.

Two operator-visible harms follow if it does. A batch whose members are
failing steadily never reaches its abort threshold, because every
reclaim-fenced ghost success resets the streak — the operator configured
a circuit breaker and it silently never trips, so a batch that should
have stopped keeps dispatching work against a broken downstream. And the
same member is counted twice: once by the ghost, once by the live
attempt that really does terminate.

The tests here pin both halves of the contract on real Postgres. The
positive half is not optional: a member that legitimately succeeds must
still reset the streak, so the guard cannot be satisfied by refusing to
count anything.
"""

from dataclasses import replace as _dc_replace
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.backend.clock import Clock, SystemClock
from taskq.batch import apply_batch_terminal_outcome
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.testing.actor import StubActorConfig
from taskq.testing.fixtures import JobsApp
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import create_worker
from taskq.worker._consumer import consume_one_job

pytestmark = pytest.mark.integration

_ACTOR = "batch_accounting_member"
_QUEUE = "default"
_FAILURE_THRESHOLD = 2


class _Payload(BaseModel):
    member: int = 0


def _config() -> StubActorConfig:
    return StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))


async def _create_batch(backend: object, batch_id: UUID) -> None:
    await backend.create_batch(  # type: ignore[attr-defined]  # Why: clean_jobs_app.backend is a PostgresBackend; the fixture's NamedTuple types it as object at runtime.
        batch_id,
        queue=_QUEUE,
        expected_size=4,
        failure_threshold=_FAILURE_THRESHOLD,
        finalizer_job_id=None,
        originating_actor=None,
    )


async def _enqueue_member(backend: object, batch_id: UUID, member: int) -> JobRow:
    return await backend.enqueue(  # type: ignore[attr-defined]  # Why: see _create_batch.
        EnqueueArgs(
            id=new_job_id(),
            actor=_ACTOR,
            queue=_QUEUE,
            payload={"member": member},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=None,
            metadata={"batch_id": str(batch_id)},
        )
    )


async def _claim(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    job_id: UUID,
) -> None:
    """Take ownership of a row the way a dispatch round does."""
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status='running', attempt=attempt+1, "
        f"locked_by_worker=$1, lock_expires_at=now()+interval '60 seconds', "
        f"started_at=now(), last_heartbeat_at=now() WHERE id=$2",
        worker_id,
        job_id,
    )


async def _reclaim_to_newer_attempt(
    conn: asyncpg.Connection,
    schema: str,
    job_id: UUID,
    new_worker_id: UUID,
) -> None:
    """What the fleet does while the stalled handler is still running.

    The lease expired, the leader's sweep re-pended the row, and the job
    was re-dispatched at a newer attempt — here to a different worker.
    This is the row state the stalled handler's late terminal write
    arrives into.
    """
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status='running', attempt=attempt+1, "
        f"locked_by_worker=$1, lock_expires_at=now()+interval '60 seconds', "
        f"started_at=now(), last_heartbeat_at=now() WHERE id=$2",
        new_worker_id,
        job_id,
    )


def _handler_row(job_id: UUID, attempt: int, worker_id: UUID, batch_id: UUID) -> JobRow:
    """The job-row snapshot a handler carries through its attempt."""
    return _dc_replace(
        make_job_row(actor=_ACTOR, payload={"member": 1}),
        id=job_id,
        attempt=attempt,
        locked_by_worker=worker_id,
        metadata={"batch_id": str(batch_id)},
    )


async def _drive_streak_to(backend: object, batch_id: UUID, failures: int) -> None:
    """Put the batch one failure short of its abort threshold.

    Each failure is a distinct member reaching a real terminal failure —
    the ordinary way a streak builds — applied through the production
    batch hook.
    """
    for _ in range(failures):
        member = _dc_replace(
            make_job_row(actor=_ACTOR, status="failed"),
            metadata={"batch_id": str(batch_id)},
        )
        await apply_batch_terminal_outcome(backend, member, "failed")  # type: ignore[arg-type]  # Why: see _create_batch.


async def test_reclaimed_attempt_does_not_reset_the_batch_failure_streak(
    clean_jobs_app: JobsApp,
) -> None:
    """A fenced success must leave the batch's abort streak untouched.

    A batch is one failure short of the threshold its operator set. A
    member's worker stalls past its lease; the row is reclaimed and
    re-dispatched at a newer attempt on another worker. The stalled
    handler finishes and issues a terminal write, which is fenced and
    lands on no row — the member has not succeeded, and is at that moment
    still running elsewhere.

    If that ghost success resets the streak, the operator's abort policy
    is defeated by a reclaim: the batch keeps dispatching members against
    whatever is failing, and the circuit breaker they configured never
    trips. The streak must stay where the real failures left it.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    live_worker_id = new_uuid()

    batch_id = new_uuid()
    await _create_batch(backend, batch_id)
    job = await _enqueue_member(backend, batch_id, member=1)

    await _drive_streak_to(backend, batch_id, _FAILURE_THRESHOLD - 1)
    before = await backend.get_batch(batch_id)
    assert before is not None
    assert before.consecutive_failures == _FAILURE_THRESHOLD - 1, (
        "fixture broken: the batch did not reach the pre-threshold streak"
    )

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await create_worker(conn, schema, live_worker_id)
        await _claim(conn, schema, worker_id, job.id)

    claimed = await backend.get(job.id)
    assert claimed is not None
    job_row = _handler_row(job.id, claimed.attempt, worker_id, batch_id)

    async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        # The reclaim runs on its own connection, exactly as the leader's
        # sweep and the other worker's dispatch round would.
        async with deps.worker_pool.acquire() as sweep_conn:
            await _reclaim_to_newer_attempt(sweep_conn, schema, job.id, live_worker_id)
        return {"ok": True}

    clock: Clock = SystemClock()
    outcome = await consume_one_job(
        backend,
        job_row,
        worker_id,
        run_actor=run_actor,
        actor_config=_config(),
        payload_type=_Payload,
        clock=clock,
    )
    await apply_batch_terminal_outcome(backend, job_row, outcome)

    row = await backend.get(job.id)
    assert row is not None
    assert row.status == "running", (
        "fixture broken: the fenced write terminalised a row that had been "
        "reclaimed and re-dispatched at a newer attempt"
    )
    assert row.locked_by_worker == live_worker_id, (
        "fixture broken: the reclaim did not hand the row to the live worker"
    )

    after = await backend.get_batch(batch_id)
    assert after is not None
    assert after.consecutive_failures == _FAILURE_THRESHOLD - 1, (
        "a reclaimed attempt whose terminal write matched no row reset the "
        f"batch's consecutive-failure streak to {after.consecutive_failures}: "
        "the member never succeeded — it is still running on another worker — "
        "yet the operator's abort threshold was pushed back out of reach, so "
        "a batch that should stop keeps dispatching work"
    )
    assert after.status == "active", (
        "the fenced attempt moved the batch out of the active state although "
        "the member it claimed to terminate is still running"
    )


async def test_successful_member_still_resets_the_batch_failure_streak(
    clean_jobs_app: JobsApp,
) -> None:
    """The inverse: a member that really succeeds must reset the streak.

    The fencing guard must reject ghost outcomes without also swallowing
    real ones. A member that runs to completion under its own lease and
    whose terminal write lands has succeeded, and the abort streak it
    interrupts must be cleared — otherwise a batch with one intermittent
    failure per run aborts on a streak that never actually occurred.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    batch_id = new_uuid()
    await _create_batch(backend, batch_id)
    job = await _enqueue_member(backend, batch_id, member=2)

    await _drive_streak_to(backend, batch_id, _FAILURE_THRESHOLD - 1)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _claim(conn, schema, worker_id, job.id)

    claimed = await backend.get(job.id)
    assert claimed is not None
    job_row = _handler_row(job.id, claimed.attempt, worker_id, batch_id)

    async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        return {"ok": True}

    clock: Clock = SystemClock()
    outcome = await consume_one_job(
        backend,
        job_row,
        worker_id,
        run_actor=run_actor,
        actor_config=_config(),
        payload_type=_Payload,
        clock=clock,
    )
    await apply_batch_terminal_outcome(backend, job_row, outcome)

    assert outcome == "succeeded", (
        f"a member that ran under its own lease reported {outcome!r} — a "
        "legitimately successful job must be reported succeeded"
    )
    row = await backend.get(job.id)
    assert row is not None
    assert row.status == "succeeded", (
        f"a member that ran under its own lease is {row.status!r}: the "
        "terminal write did not land for an attempt that owned its row"
    )

    after = await backend.get_batch(batch_id)
    assert after is not None
    assert after.consecutive_failures == 0, (
        "a member that really succeeded left the batch's consecutive-failure "
        f"streak at {after.consecutive_failures}: the streak no longer "
        "describes consecutive failures, so the batch aborts on a run of "
        "failures that was in fact broken by a success"
    )


async def test_same_member_is_counted_once_across_a_reclaim(
    clean_jobs_app: JobsApp,
) -> None:
    """One member job must move batch accounting exactly once.

    The stalled handler's fenced write and the live attempt's real write
    both describe the same member. Together they must advance the batch's
    accounting by one member's worth, not two: the operator's view of how
    many members are done, and of the failure streak, has to count jobs,
    not attempts.

    An earlier member has already failed, so the streak stands at one
    before this member runs. The reclaimed member then legitimately fails
    on its live attempt, which must take the streak to two: the ghost
    success must not have cleared the earlier failure, and must not itself
    be counted.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    stalled_worker_id = new_uuid()
    live_worker_id = new_uuid()

    batch_id = new_uuid()
    await _create_batch(backend, batch_id)
    job = await _enqueue_member(backend, batch_id, member=3)

    await _drive_streak_to(backend, batch_id, 1)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, stalled_worker_id)
        await create_worker(conn, schema, live_worker_id)
        await _claim(conn, schema, stalled_worker_id, job.id)

    claimed = await backend.get(job.id)
    assert claimed is not None
    stalled_row = _handler_row(job.id, claimed.attempt, stalled_worker_id, batch_id)

    async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        async with deps.worker_pool.acquire() as sweep_conn:
            await _reclaim_to_newer_attempt(sweep_conn, schema, job.id, live_worker_id)
        return {"ok": True}

    clock: Clock = SystemClock()
    ghost_outcome = await consume_one_job(
        backend,
        stalled_row,
        stalled_worker_id,
        run_actor=run_actor,
        actor_config=_config(),
        payload_type=_Payload,
        clock=clock,
    )
    await apply_batch_terminal_outcome(backend, stalled_row, ghost_outcome)

    # The live attempt now runs to a real terminal failure, under the
    # lease it actually holds.
    live = await backend.get(job.id)
    assert live is not None
    live_row = _dc_replace(
        _handler_row(job.id, live.attempt, live_worker_id, batch_id),
        max_attempts=1,
    )

    async def failing_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        raise RuntimeError("downstream unavailable")

    live_outcome = await consume_one_job(
        backend,
        live_row,
        live_worker_id,
        run_actor=failing_actor,
        actor_config=StubActorConfig(
            retry=RetryPolicy(kind="transient", max_attempts=1, jitter=0.0)
        ),
        payload_type=_Payload,
        clock=clock,
    )
    await apply_batch_terminal_outcome(backend, live_row, live_outcome)

    final = await backend.get(job.id)
    assert final is not None
    assert final.status == "failed", (
        f"fixture broken: the live attempt left the member {final.status!r} "
        "instead of reaching a terminal failure"
    )

    after = await backend.get_batch(batch_id)
    assert after is not None
    assert after.consecutive_failures == 2, (
        "an earlier failure plus one member job that was reclaimed and then "
        f"failed once left the batch's failure streak at "
        f"{after.consecutive_failures} instead of 2: "
        "batch accounting is counting attempts rather than jobs, so a fleet "
        "with ordinary lease reclaims either aborts batches early or never "
        "aborts them at all"
    )
