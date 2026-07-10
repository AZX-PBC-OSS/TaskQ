"""PG integration tests for sub-enqueue transactional behavior.

Uses testcontainers PostgreSQL to verify that the sub-enqueue transactional
guarantee holds under real PG MVCC snapshot
visibility: child job INSERTs share the parent's transaction on the LOOP-scope
connection, and roll back atomically when the parent raises.

Scenarios:
  Transactional sub-job: parent succeeds, child visible
  Transactional sub-job: parent raises, child NOT in PG
  Snooze: child re-enqueued outside tx; parent transitions to scheduled
  Autonomous fallback: child persists even if parent fails
  Explicit connection override: child enqueued independently
  Startup warning fires when no LOOP-scope DB connection registered
"""

import asyncio
from contextlib import suppress
from dataclasses import replace as _dc_replace
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel, TypeAdapter

from taskq._ids import new_job_id, new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import (
    EnqueueArgs,
    JobRow,
)
from taskq.backend.clock import Clock, SystemClock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.exceptions import Snooze
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import default_actor_config
from taskq.testing.fixtures import JobsApp
from taskq.testing.jobs import make_job_row
from taskq.testing.pg import create_worker
from taskq.testing.spy import WarningSpy
from taskq.worker._consumer import consume_one_job

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend

pytestmark = pytest.mark.integration

_NOW = datetime(2025, 1, 1, tzinfo=UTC)


class _ParentPayload(BaseModel):
    name: str = "parent"


class _ChildPayload(BaseModel):
    name: str = "child"


class _Result(BaseModel):
    ok: bool = True


def _child_actor_ref(
    *,
    name: str = "child_actor",
    queue: str = "default",
) -> ActorRef[_ChildPayload, _Result]:
    async def _handler(payload: _ChildPayload) -> _Result:
        return _Result()

    return ActorRef(
        name=name,
        queue=queue,
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_ChildPayload,
        result_adapter=TypeAdapter(_Result),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


_CHILD_REF = _child_actor_ref()


async def _dispatch_job(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    job_id: UUID,
) -> None:
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status='running', attempt=attempt+1, locked_by_worker=$1, lock_expires_at=now()+interval '60 seconds', started_at=now(), last_heartbeat_at=now() WHERE id=$2 AND status='pending'",  # noqa: S608 # Why: schema validated by WorkerSettings; asyncpg has no parameter binding for identifiers
        worker_id,
        job_id,
    )


async def _enqueue_parent(backend: "PostgresBackend") -> JobRow:
    return await backend.enqueue(
        EnqueueArgs(
            id=new_job_id(),
            actor="parent_actor",
            queue="default",
            payload={"name": "parent"},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC),
        )
    )


async def _count_jobs(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str | None = None,
    status: str | None = None,
) -> int:
    where_parts: list[str] = []
    args: list[object] = []
    if actor is not None:
        where_parts.append("actor = $1")
        args.append(actor)
    if status is not None:
        idx = len(args) + 1
        where_parts.append(f"status = ${idx}")
        args.append(status)
    where = " AND ".join(where_parts) if where_parts else "TRUE"
    row = await conn.fetchrow(
        f'SELECT count(*) AS cnt FROM "{schema}".jobs WHERE {where}',  # noqa: S608 # Why: test-only; schema validated by WorkerSettings
        *args,
    )
    assert row is not None
    return row["cnt"]


async def _get_job_status(
    conn: asyncpg.Connection,
    schema: str,
    job_id: UUID,
) -> str | None:
    row = await conn.fetchrow(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1',  # noqa: S608 # Why: test-only; schema validated by WorkerSettings
        job_id,
    )
    if row is None:
        return None
    return row["status"]


# ── Transactional sub-job: parent succeeds, child visible ──────


async def test_ti1_parent_succeeds_child_visible(
    jobs_app: JobsApp,
) -> None:
    """Parent succeeds with LOOP-scope conn; child row is visible in PG."""

    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    parent_job = await _enqueue_parent(backend)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, parent_job.id)

    row = await backend.get(parent_job.id)
    assert row is not None

    async with deps.worker_pool.acquire() as loop_conn:
        enqueuer = SubJobEnqueuer(
            loop_scope_resolved={asyncpg.Connection: loop_conn},
            worker_pool=deps.worker_pool,
            backend=backend,
        )

        job_row = _dc_replace(
            make_job_row(actor="parent_actor", payload={"name": "parent"}),
            id=parent_job.id,
            locked_by_worker=worker_id,
        )

        async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            await enqueuer.enqueue(_CHILD_REF, _ChildPayload())
            return {"ok": True}

        clk: Clock = SystemClock()
        with suppress(asyncio.CancelledError):
            await consume_one_job(
                backend,
                job_row,
                worker_id,
                run_actor=run_actor,
                actor_config=default_actor_config(),
                payload_type=_ParentPayload,
                clock=clk,
                enqueuer=enqueuer,
                loop_conn=loop_conn,
            )

    async with deps.worker_pool.acquire() as check_conn:
        child_count = await _count_jobs(check_conn, schema, actor="child_actor", status="pending")
        parent_status = await _get_job_status(check_conn, schema, parent_job.id)

    assert child_count == 1, "child row should exist in PG with status='pending'"
    assert parent_status == "succeeded", "parent should have status='succeeded'"


# ── Transactional sub-job: parent raises, child NOT in PG ──────


async def test_ti2_parent_raises_child_not_in_pg(
    jobs_app: JobsApp,
) -> None:
    """Parent raises after enqueueing child; child NOT in PG (rolled back)."""

    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    parent_job = await _enqueue_parent(backend)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, parent_job.id)

    async with deps.worker_pool.acquire() as loop_conn:
        enqueuer = SubJobEnqueuer(
            loop_scope_resolved={asyncpg.Connection: loop_conn},
            worker_pool=deps.worker_pool,
            backend=backend,
        )

        job_row = _dc_replace(
            make_job_row(actor="parent_actor", payload={"name": "parent"}),
            id=parent_job.id,
            locked_by_worker=worker_id,
        )

        async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            await enqueuer.enqueue(_CHILD_REF, _ChildPayload())
            raise RuntimeError("parent failed")

        clk: Clock = SystemClock()
        with suppress(asyncio.CancelledError):
            await consume_one_job(
                backend,
                job_row,
                worker_id,
                run_actor=run_actor,
                actor_config=default_actor_config(),
                payload_type=_ParentPayload,
                clock=clk,
                enqueuer=enqueuer,
                loop_conn=loop_conn,
            )

    async with deps.worker_pool.acquire() as check_conn:
        child_count = await _count_jobs(check_conn, schema, actor="child_actor")
        parent_status = await _get_job_status(check_conn, schema, parent_job.id)

    assert child_count == 0, "child row should NOT exist in PG after rollback"
    assert parent_status in ("failed", "scheduled"), (
        f"parent should be failed or scheduled for retry, got {parent_status!r}"
    )


# ── Snooze: savepoint rollback + child re-enqueue; parent transitions to scheduled ──


async def test_ti3_snooze_re_enqueues_child(
    jobs_app: JobsApp,
) -> None:
    """Parent enqueues child then raises Snooze; child re-enqueued outside tx, parent scheduled."""

    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    parent_job = await _enqueue_parent(backend)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, parent_job.id)

    async with deps.worker_pool.acquire() as loop_conn:
        enqueuer = SubJobEnqueuer(
            loop_scope_resolved={asyncpg.Connection: loop_conn},
            worker_pool=deps.worker_pool,
            backend=backend,
        )

        job_row = _dc_replace(
            make_job_row(actor="parent_actor", payload={"name": "parent"}),
            id=parent_job.id,
            locked_by_worker=worker_id,
        )

        async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            await enqueuer.enqueue(_CHILD_REF, _ChildPayload())
            raise Snooze(timedelta(seconds=60))

        clk: Clock = SystemClock()
        with suppress(asyncio.CancelledError):
            await consume_one_job(
                backend,
                job_row,
                worker_id,
                run_actor=run_actor,
                actor_config=default_actor_config(),
                payload_type=_ParentPayload,
                clock=clk,
                enqueuer=enqueuer,
                loop_conn=loop_conn,
            )

    async with deps.worker_pool.acquire() as check_conn:
        child_count = await _count_jobs(check_conn, schema, actor="child_actor")
        parent_row = await check_conn.fetchrow(
            f'SELECT status, scheduled_at FROM "{schema}".jobs WHERE id = $1',  # noqa: S608 # Why: test-only; schema validated by WorkerSettings
            parent_job.id,
        )

    assert child_count == 1, "child row should exist in PG (re-enqueued outside tx) after Snooze"
    assert parent_row is not None
    assert parent_row["status"] == "scheduled", (
        f"parent should be 'scheduled' after Snooze, got {parent_row['status']!r}"
    )


# ── Autonomous fallback: child persists even if parent fails ──


async def test_ti4_autonomous_fallback_child_persists(
    jobs_app: JobsApp,
) -> None:
    """No LOOP-scope conn; child is committed autonomously even if parent raises."""

    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    parent_job = await _enqueue_parent(backend)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, parent_job.id)

    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None,
        worker_pool=deps.worker_pool,
        backend=backend,
    )

    job_row = _dc_replace(
        make_job_row(actor="parent_actor", payload={"name": "parent"}),
        id=parent_job.id,
        locked_by_worker=worker_id,
    )

    async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
        await _ctx.jobs.enqueue(_CHILD_REF, _ChildPayload())
        raise RuntimeError("parent failed")

    clk: Clock = SystemClock()
    with suppress(asyncio.CancelledError):
        await consume_one_job(
            backend,
            job_row,
            worker_id,
            run_actor=run_actor,
            actor_config=default_actor_config(),
            payload_type=_ParentPayload,
            clock=clk,
            enqueuer=enqueuer,
            loop_conn=None,
        )

    async with deps.worker_pool.acquire() as check_conn:
        child_count = await _count_jobs(check_conn, schema, actor="child_actor")

    assert child_count >= 1, (
        "child row should exist in PG (autonomous commit, independent of parent)"
    )


# ── Explicit connection override: child enqueued independently ──


async def test_ti5_explicit_connection_override_child_persists(
    jobs_app: JobsApp,
) -> None:
    """Parent passes connection= to enqueue; child commits independently."""

    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    parent_job = await _enqueue_parent(backend)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
        await _dispatch_job(conn, schema, worker_id, parent_job.id)

    async with deps.worker_pool.acquire() as loop_conn, deps.worker_pool.acquire() as separate_conn:
        enqueuer = SubJobEnqueuer(
            loop_scope_resolved={asyncpg.Connection: loop_conn},
            worker_pool=deps.worker_pool,
            backend=backend,
        )

        job_row = _dc_replace(
            make_job_row(actor="parent_actor", payload={"name": "parent"}),
            id=parent_job.id,
            locked_by_worker=worker_id,
        )

        async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            await _ctx.jobs.enqueue(
                _CHILD_REF,
                _ChildPayload(),
                connection=separate_conn,
            )
            raise RuntimeError("parent failed")

        clk: Clock = SystemClock()
        with suppress(asyncio.CancelledError):
            await consume_one_job(
                backend,
                job_row,
                worker_id,
                run_actor=run_actor,
                actor_config=default_actor_config(),
                payload_type=_ParentPayload,
                clock=clk,
                enqueuer=enqueuer,
                loop_conn=loop_conn,
            )

    async with deps.worker_pool.acquire() as check_conn:
        child_count = await _count_jobs(check_conn, schema, actor="child_actor")

    assert child_count >= 1, "child row should exist in PG (independent commit from explicit conn)"


# ── Startup warning fires when no LOOP-scope DB connection registered ──


async def test_ti6_startup_warning_no_loop_scope_conn(pg_dsn: str) -> None:
    """Starting a worker with no LOOP-scope Connection emits the startup warning."""
    from taskq._di.scopes import LoopScope
    from taskq.worker.run import _emit_sub_enqueue_startup_warnings

    settings = WorkerSettings.load_from_dict({"pg_dsn": pg_dsn, "schema_name": "taskq_test"})

    def _stub_resolver(func: object) -> object:
        return None

    loop_scope = LoopScope(resolver=_stub_resolver)

    actor_registry = {"parent_actor": _child_actor_ref(name="parent_actor")}

    spy = WarningSpy()
    _emit_sub_enqueue_startup_warnings(loop_scope, settings, actor_registry, spy)
    assert spy.warning_count >= 1
