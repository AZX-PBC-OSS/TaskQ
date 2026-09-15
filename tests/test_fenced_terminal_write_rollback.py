# ruff: noqa: S608  # Why: schema is a per-test fixed identifier, not user input; every value is $-bound.

"""A terminal write that matches no row must roll back the attempt whole.

When a job's lease is reclaimed mid-attempt — the worker stalled past its
lock lease, the leader's sweep re-pended the row, the job was re-dispatched
at a newer attempt — the original handler is still running. Its terminal
write is fenced on ``(id, status, locked_by_worker, attempt)`` and lands on
no row.

The fence stopping the *status* write is only half the guarantee. The
attempt is a unit of work: the actor's own writes, its sub-job enqueues,
the success hook, and the success state-change event all belong to the same
outcome. If the fence no-ops but the surrounding transaction still commits,
the operator sees an attempt that was reclaimed and re-run from scratch,
yet whose first run's side effects are durably in the database — the job ran
twice and half of the first run survived. If the success hook fires on a
write that landed nowhere, downstream systems are told a job succeeded that
is at that moment still running somewhere else.

So the contract is: a fenced terminal write reports the attempt did not
terminate, the actor's transactional side effects do not commit, and no
success hook and no success event fire. Exactly one coherent terminal
state, side effects committed exactly once or not at all, never half.
"""

import asyncio
from contextlib import suppress
from dataclasses import replace as _dc_replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel, TypeAdapter

from taskq._ids import new_job_id, new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import EnqueueArgs, JobRow
from taskq.backend.clock import Clock, SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.testing.actor import StubActorConfig
from taskq.testing.fixtures import JobsApp
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import create_running_job, create_worker
from taskq.worker._consumer import consume_one_job

pytestmark = pytest.mark.integration


class _Payload(BaseModel):
    name: str = "parent"


class _Result(BaseModel):
    ok: bool = True


def _child_ref() -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()

    return ActorRef(
        name="fenced_child_actor",
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


_CHILD = _child_ref()


async def _enqueue_parent(backend: PostgresBackend) -> JobRow:
    return await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="fenced_parent_actor",
            queue="default",
            payload={"name": "parent"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=None,
        )
    )


async def _dispatch(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    job_id: UUID,
) -> None:
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status='running', attempt=attempt+1, "
        f"locked_by_worker=$1, lock_expires_at=now()+interval '60 seconds', "
        f"started_at=now(), last_heartbeat_at=now() WHERE id=$2 AND status='pending'",
        worker_id,
        job_id,
    )


async def _reclaim_to_newer_attempt(
    conn: asyncpg.Connection,
    schema: str,
    job_id: UUID,
    new_worker_id: UUID,
) -> None:
    """Simulate what the fleet does while the original handler still runs.

    The leader's sweep re-pends a job whose lease expired and it is then
    re-dispatched — to another worker here — at a newer attempt. This is
    the state the original handler's late terminal write arrives into.
    """
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status='running', attempt=attempt+1, "
        f"locked_by_worker=$1, lock_expires_at=now()+interval '60 seconds', "
        f"started_at=now(), last_heartbeat_at=now() WHERE id=$2",
        new_worker_id,
        job_id,
    )


async def _count_children(conn: asyncpg.Connection, schema: str) -> int:
    row = await conn.fetchrow(
        f'SELECT count(*) AS cnt FROM "{schema}".jobs WHERE actor = $1',
        "fenced_child_actor",
    )
    assert row is not None
    return int(row["cnt"])


async def _job_row(conn: asyncpg.Connection, schema: str, job_id: UUID) -> asyncpg.Record | None:
    return await conn.fetchrow(
        f'SELECT status, attempt, locked_by_worker, result FROM "{schema}".jobs WHERE id = $1',
        job_id,
    )


async def test_fenced_transactional_success_rolls_back_actor_side_effects(
    clean_jobs_app: JobsApp,
) -> None:
    """A reclaimed attempt's transactional writes must not commit.

    The actor enqueues a sub-job inside the job's own transaction and
    returns. Before the terminal write, the lease is reclaimed and the job
    re-dispatched at a newer attempt on another worker, so the write is
    fenced. Because the sub-job INSERT shares that transaction, it must go
    with it: the operator must never find a child job durably committed for
    an attempt the system decided never terminated, while the live attempt
    runs the actor again and enqueues the child a second time.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    other_worker_id = new_uuid()

    parent = await _enqueue_parent(backend)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await create_worker(conn, schema, other_worker_id)
        await _dispatch(conn, schema, worker_id, parent.id)

    dispatched = await backend.get(parent.id)
    assert dispatched is not None

    success_hook_calls: list[UUID] = []

    async def _on_success(job: JobRow, result: object) -> None:
        success_hook_calls.append(job.id)

    async with deps.worker_pool.acquire() as transaction_conn:
        enqueuer = SubJobEnqueuer(
            loop_scope_resolved={asyncpg.Connection: transaction_conn},
            worker_pool=deps.worker_pool,
            backend=backend,
        )

        job_row = _dc_replace(
            make_job_row(actor="fenced_parent_actor", payload={"name": "parent"}),
            id=parent.id,
            attempt=dispatched.attempt,
            locked_by_worker=worker_id,
        )

        async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            await enqueuer.enqueue(_CHILD, _Payload())
            # The reclaim happens on a separate connection, outside this
            # job's transaction — exactly as another worker's would.
            async with deps.worker_pool.acquire() as sweep_conn:
                await _reclaim_to_newer_attempt(sweep_conn, schema, parent.id, other_worker_id)
            return {"ok": True}

        clock: Clock = SystemClock()
        outcome: object = None
        with suppress(asyncio.CancelledError):
            outcome = await consume_one_job(
                backend,
                job_row,
                worker_id,
                run_actor=run_actor,
                actor_config=StubActorConfig(
                    retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
                    on_success=_on_success,
                ),
                payload_type=_Payload,
                clock=clock,
                enqueuer=enqueuer,
                transaction_conn=transaction_conn,
            )

    async with deps.worker_pool.acquire() as check_conn:
        children = await _count_children(check_conn, schema)
        row = await _job_row(check_conn, schema, parent.id)

    assert row is not None
    assert row["locked_by_worker"] == other_worker_id, (
        "fixture broken: the reclaim did not take ownership of the row"
    )
    assert row["status"] == "running", (
        "the fenced write terminalised a job that had been reclaimed and "
        "re-dispatched at a newer attempt — the live attempt lost its row"
    )
    assert children == 0, (
        "the reclaimed attempt's sub-job INSERT committed even though its "
        "terminal write matched no row: the actor's unit of work was "
        "committed half, and the live attempt will enqueue the child again"
    )
    assert success_hook_calls == [], (
        "the success hook fired for an attempt whose terminal write landed on "
        "no row — downstream systems were told a job succeeded while it was "
        "still running elsewhere"
    )
    assert outcome != "succeeded", (
        f"the attempt reported {outcome!r} although its terminal write was "
        "fenced and applied to no row"
    )


async def test_fenced_autonomous_success_does_not_report_success(
    clean_jobs_app: JobsApp,
) -> None:
    """The autonomous path must not claim success on a write that landed nowhere.

    Without a LOOP-scope connection the terminal write runs on its own
    connection, so there is no actor transaction to roll back — but the
    reporting half of the guarantee still holds. A fenced write means this
    attempt did not terminate the job: no success hook, and no ``succeeded``
    outcome for the dispatcher's span and metrics to record.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    other_worker_id = new_uuid()

    parent = await _enqueue_parent(backend)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await create_worker(conn, schema, other_worker_id)
        await _dispatch(conn, schema, worker_id, parent.id)

    dispatched = await backend.get(parent.id)
    assert dispatched is not None

    success_hook_calls: list[UUID] = []

    async def _on_success(job: JobRow, result: object) -> None:
        success_hook_calls.append(job.id)

    job_row = _dc_replace(
        make_job_row(actor="fenced_parent_actor", payload={"name": "parent"}),
        id=parent.id,
        attempt=dispatched.attempt,
        locked_by_worker=worker_id,
    )

    async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        async with deps.worker_pool.acquire() as sweep_conn:
            await _reclaim_to_newer_attempt(sweep_conn, schema, parent.id, other_worker_id)
        return {"ok": True}

    clock: Clock = SystemClock()
    outcome: object = None
    with suppress(asyncio.CancelledError):
        outcome = await consume_one_job(
            backend,
            job_row,
            worker_id,
            run_actor=run_actor,
            actor_config=StubActorConfig(
                retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
                on_success=_on_success,
            ),
            payload_type=_Payload,
            clock=clock,
        )

    async with deps.worker_pool.acquire() as check_conn:
        row = await _job_row(check_conn, schema, parent.id)

    assert row is not None
    assert row["status"] == "running", (
        "the fenced write terminalised a reclaimed job on the autonomous path"
    )
    assert success_hook_calls == [], (
        "the success hook fired for an attempt whose terminal write landed on no row"
    )
    assert outcome != "succeeded", (
        f"the attempt reported {outcome!r} although its terminal write was fenced"
    )


async def test_expired_lock_sweep_converges_over_repeated_ticks(
    clean_jobs_app: JobsApp,
) -> None:
    """A sweep must drain its backlog, not fail forever on the same row.

    The convergence half of robustness: after a fleet-wide stall leaves many
    jobs with expired leases, repeated sweep ticks must reduce the backlog to
    zero and then stay quiet. A sweep that wedges on one row — a collision it
    retries identically every tick — leaves the whole backlog stranded behind
    it and the queue never recovers without operator intervention.
    """
    deps = clean_jobs_app.deps
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    expired = datetime.now(UTC) - timedelta(seconds=30)
    job_count = 25

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        for _ in range(job_count):
            await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=expired,
                max_attempts=3,
                retry_kind="transient",
            )

        ticks = 0
        reclaimed_total = 0
        remaining = job_count
        # A bounded sweep reclaims a batch per tick; convergence means the
        # remaining backlog reaches zero within a tick budget generous
        # relative to the batch bound, never that one tick does it all.
        while ticks < 50:
            ticks += 1
            reclaimed_total += await PostgresBackend.sweep_expired_locks(
                conn,
                timedelta(seconds=30),
                timedelta(seconds=30),
                schema=schema,
            )
            remaining = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '
                f"WHERE status = 'running' AND lock_expires_at < now()",
            )
            if remaining == 0:
                break

        assert remaining == 0, (
            f"after {ticks} sweep ticks {remaining} expired-lease jobs remain: "
            "the sweep is not converging, so a stalled fleet's backlog stays "
            "stranded until an operator intervenes"
        )
        assert reclaimed_total >= job_count, (
            f"the sweep reported {reclaimed_total} reclaims for {job_count} "
            "expired jobs — rows were cleared without being accounted for"
        )

        # Having converged, the sweep must go quiet: a sweep that keeps
        # reporting work on an empty backlog is burning the leader's budget
        # every tick and hides real reclaims in the noise.
        idle = await PostgresBackend.sweep_expired_locks(
            conn,
            timedelta(seconds=30),
            timedelta(seconds=30),
            schema=schema,
        )
        assert idle == 0, f"a sweep over a drained backlog still reported {idle} reclaims"
