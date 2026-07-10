"""Tests for per-identity serialization and sub-enqueue DI shape.

``identity_key`` enforces a "one job per identity at a time" invariant:
``identity = lambda p: f"account:{{p.account_id}}"`` — each account
serialises to at most one running job at a time, in addition to
TaskQ's own ``max_concurrent`` cap. The dispatch CTE's
``running_identities`` NOT EXISTS clause is the load-bearing
constraint: even when ``max_concurrent`` allows more, only one job
per ``(actor, identity_key)`` pair transitions to ``running``.

The sub-enqueue DI shape tests verify the DI wiring and transaction
atomicity that actors using a secondary datastore client rely on:
LOOP-scope ``asyncpg.Connection`` + LOOP-scope mock graph-database
client + ``ctx.jobs.enqueue`` inside actor body. The stub
``Neo4jClientProtocol`` is a minimal Protocol (``save``, ``query``
async methods only) intended to exercise the DI registration and
call pattern only — it is not a full client interface.

See the dispatch CTE at ``dispatch.py:141-144`` (NOT EXISTS on
``running_identities``).
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import replace as _dc_replace
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel, TypeAdapter

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq._ids import new_job_id, new_uuid
from taskq.actor import ActorRef
from taskq.backend._protocol import (
    EnqueueArgs,
    JobRow,
)
from taskq.backend.clock import Clock, SystemClock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import default_actor_config
from taskq.testing.fixtures import JobsApp
from taskq.testing.jobs import make_enqueue_args, make_job_row
from taskq.testing.pg import create_worker
from taskq.worker._consumer import consume_one_job

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)


async def _count_running(conn: asyncpg.Connection, schema: str, actor: str) -> int:
    row = await conn.fetchrow(
        f"SELECT count(*) AS cnt FROM \"{schema}\".jobs WHERE status = 'running' AND actor = $1",
        actor,
    )
    assert row is not None
    return row["cnt"]


async def _count_jobs(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str | None = None,
) -> int:
    where = "actor = $1" if actor is not None else "TRUE"
    args: list[object] = [actor] if actor is not None else []
    row = await conn.fetchrow(
        f'SELECT count(*) AS cnt FROM "{schema}".jobs WHERE {where}',
        *args,
    )
    assert row is not None
    return row["cnt"]


async def _dispatch_job(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    job_id: UUID,
) -> None:
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status='running', attempt=attempt+1, locked_by_worker=$1, lock_expires_at=now()+interval '60 seconds', started_at=now(), last_heartbeat_at=now() WHERE id=$2 AND status='pending'",  # Why: schema validated by WorkerSettings; asyncpg has no parameter binding for identifiers
        worker_id,
        job_id,
    )


# ── Neo4jClientProtocol stub (secondary-datastore DI shape) ────────


class Neo4jClientProtocol(Protocol):
    """Stub Protocol for a graph-database client registered via DI.

    ``save`` and ``query`` are async methods matching the minimal shape
    a real client would expose. This does NOT replicate a full
    Neo4j client interface — it exercises the DI registration and call
    pattern only.
    """

    async def save(self, entity_id: str, *, conn: object) -> None: ...
    async def query(self, entity_id: str) -> list[dict[str, object]]: ...

    recorded_saves: list[str]


class _StubNeo4jClient:
    """In-memory Neo4jClient stub that records save calls."""

    def __init__(self) -> None:
        self.recorded_saves: list[str] = []

    async def save(self, entity_id: str, *, conn: object) -> None:
        self.recorded_saves.append(entity_id)

    async def query(self, entity_id: str) -> list[dict[str, object]]:
        return []


class _FailingNeo4jClient:
    """Neo4jClient stub that raises on save."""

    def __init__(self) -> None:
        self.recorded_saves: list[str] = []

    async def save(self, entity_id: str, *, conn: object) -> None:
        raise RuntimeError("neo4j save failed")

    async def query(self, entity_id: str) -> list[dict[str, object]]:
        return []


# ── actor payloads ────────────────────────────────────────────────


class _PropertyPayload(BaseModel):
    property_id: str = "site-42"


class _EnrichPayload(BaseModel):
    property_id: str = "site-42"


class _EmptyResult(BaseModel):
    pass


def _enrich_actor_ref() -> ActorRef[_EnrichPayload, _EmptyResult]:
    async def _handler(payload: _EnrichPayload) -> _EmptyResult:
        return _EmptyResult()

    return ActorRef(
        name="enrich_property",
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_EnrichPayload,
        result_adapter=TypeAdapter(_EmptyResult),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
    )


_ENRICH_REF = _enrich_actor_ref()


async def _make_loop_scope_with_conn_and_neo4j(
    pool: asyncpg.Pool,
    neo4j_client: Neo4jClientProtocol,
) -> tuple[ProviderRegistry, ProcessScope, ThreadScope, LoopScope]:
    """Build DI scopes with LOOP-scope asyncpg.Connection and Neo4jClientProtocol."""
    settings = WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgresql://placeholder",
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
        },
    )
    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)

    async def _make_conn() -> AsyncIterator[asyncpg.Connection]:  # type: ignore[override] # Why: pool.acquire() returns PoolConnectionProxy; pyright sees covariant mismatch on async generator return type
        async with pool.acquire() as conn:
            yield conn  # type: ignore[misc] # Why: same

    registry.register_factory(asyncpg.Connection, Scope.LOOP, _make_conn)
    registry.register_value(Neo4jClientProtocol, Scope.LOOP, neo4j_client)
    registry.validate()

    scope_containers: dict[Scope, object] = {}
    resolver = make_resolver(registry, scope_containers)  # type: ignore[arg-type] # Why: make_resolver expects dict[Scope, ScopeContainerProtocol]; scope_containers holds concrete subclasses that satisfy the Protocol

    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope

    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)

    return registry, process_scope, thread_scope, loop_scope


# ── identity serialization test ────────────────────────────────────


@pytest.mark.asyncio
async def test_identity_key_serialization(clean_jobs_app: JobsApp) -> None:
    """At most 1 job per identity-key in ``running``.

    Enqueues 5 jobs with ``identity_key="account:42"`` for an actor
    with ``max_concurrent=10`` (cap not binding), then runs 2 dispatch
    rounds sequentially with ``limit=1``. After the first dispatch
    puts one identity in running, the second round sees that identity
    in ``running_identities`` and dispatches 0 — satisfying the
    per-identity serialization invariant even though the actor cap
    allows 10.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '
            "VALUES ($1, $2, $3, $4::jsonb)",
            "sites",
            10,
            "default",
            "{}",
        )

    for _i in range(5):
        await backend.enqueue(
            make_enqueue_args(actor="sites", identity_key="account:42", payload={"x": 1})
        )

    round_1 = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=1,
        lock_lease=_LEASE,
    )
    await asyncio.sleep(0.05)

    round_2 = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=1,
        lock_lease=_LEASE,
    )

    total_dispatched = len(round_1) + len(round_2)

    async with deps.worker_pool.acquire() as conn:
        running = await _count_running(conn, schema, "sites")

    assert len(round_1) == 1, f"expected first round to dispatch 1, got {len(round_1)}"
    assert len(round_2) == 0, (
        f"expected second round dispatch 0 (identity running), got {len(round_2)}"
    )
    assert total_dispatched >= 1, "expected at least one job dispatched"
    assert running <= 1, (
        f"identity serialization violated: {running} running for identity 'account:42'"
    )


# ── sub-enqueue DI shape tests ────────────────────────────────────────


async def test_sub_enqueue_db_write_then_actor_raises(
    clean_jobs_app: JobsApp,
) -> None:
    """DB write succeeded but actor raised after — child NOT in PG, PG writes rolled back."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    neo4j = _StubNeo4jClient()

    _registry, process_scope, thread_scope, loop_scope = await _make_loop_scope_with_conn_and_neo4j(
        deps.worker_pool, neo4j
    )

    try:
        resolved = loop_scope.resolved_cache()
        loop_conn_raw = resolved.get(asyncpg.Connection)
        assert isinstance(loop_conn_raw, asyncpg.Connection)
        loop_conn: asyncpg.Connection = loop_conn_raw  # type: ignore[assignment] # Why: PoolConnectionProxy is assignable to Connection at runtime but pyright sees a static mismatch

        enqueuer = SubJobEnqueuer(
            loop_scope_resolved=resolved,
            worker_pool=deps.worker_pool,
            backend=backend,
        )

        parent_job = await backend.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="process_property",
                queue="default",
                payload={"property_id": "site-42"},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            )
        )

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            await _dispatch_job(conn, schema, worker_id, parent_job.id)

        job_row = _dc_replace(
            make_job_row(actor="process_property", payload={"property_id": "site-42"}),
            id=parent_job.id,
            locked_by_worker=worker_id,
        )

        async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            neo4j_stub: object = resolved.get(Neo4jClientProtocol)
            assert isinstance(neo4j_stub, _StubNeo4jClient)
            await neo4j_stub.save("site-42", conn=loop_conn)
            await enqueuer.enqueue(_ENRICH_REF, _EnrichPayload())
            raise RuntimeError("actor raised after DB write")

        clk: Clock = SystemClock()
        with suppress(asyncio.CancelledError):
            await consume_one_job(
                backend,
                job_row,
                worker_id,
                run_actor=run_actor,
                actor_config=default_actor_config(),
                payload_type=_PropertyPayload,
                clock=clk,
                enqueuer=enqueuer,
                loop_conn=loop_conn,
            )

        async with deps.worker_pool.acquire() as check_conn:
            child_count = await _count_jobs(check_conn, schema, actor="enrich_property")

        assert child_count == 0, "child row should NOT exist in PG — PG writes rolled back"
    finally:
        await loop_scope.shutdown()
        await thread_scope.shutdown()
        await process_scope.shutdown()


async def test_sub_enqueue_neo4j_failure(
    clean_jobs_app: JobsApp,
) -> None:
    """Neo4j-style failure — child NOT in PG, rollback clean."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    neo4j = _FailingNeo4jClient()

    _registry, process_scope, thread_scope, loop_scope = await _make_loop_scope_with_conn_and_neo4j(
        deps.worker_pool, neo4j
    )

    try:
        resolved = loop_scope.resolved_cache()
        loop_conn_raw = resolved.get(asyncpg.Connection)
        assert isinstance(loop_conn_raw, asyncpg.Connection)
        loop_conn: asyncpg.Connection = loop_conn_raw  # type: ignore[assignment] # Why: PoolConnectionProxy is assignable to Connection at runtime but pyright sees a static mismatch

        enqueuer = SubJobEnqueuer(
            loop_scope_resolved=resolved,
            worker_pool=deps.worker_pool,
            backend=backend,
        )

        parent_job = await backend.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="process_property",
                queue="default",
                payload={"property_id": "site-42"},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            )
        )

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            await _dispatch_job(conn, schema, worker_id, parent_job.id)

        job_row = _dc_replace(
            make_job_row(actor="process_property", payload={"property_id": "site-42"}),
            id=parent_job.id,
            locked_by_worker=worker_id,
        )

        async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            neo4j_stub: object = resolved.get(Neo4jClientProtocol)
            assert isinstance(neo4j_stub, _FailingNeo4jClient)
            await neo4j_stub.save("site-42", conn=loop_conn)
            await enqueuer.enqueue(_ENRICH_REF, _EnrichPayload())
            return {"ok": True}

        clk: Clock = SystemClock()
        with suppress(asyncio.CancelledError):
            await consume_one_job(
                backend,
                job_row,
                worker_id,
                run_actor=run_actor,
                actor_config=default_actor_config(),
                payload_type=_PropertyPayload,
                clock=clk,
                enqueuer=enqueuer,
                loop_conn=loop_conn,
            )

        async with deps.worker_pool.acquire() as check_conn:
            child_count = await _count_jobs(check_conn, schema, actor="enrich_property")

        assert child_count == 0, "child row should NOT exist in PG after Neo4j failure rollback"
    finally:
        await loop_scope.shutdown()
        await thread_scope.shutdown()
        await process_scope.shutdown()


async def test_sub_enqueue_success(
    clean_jobs_app: JobsApp,
) -> None:
    """Success — Neo4j stub recorded save AND child row in PG."""
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()
    neo4j = _StubNeo4jClient()

    _registry, process_scope, thread_scope, loop_scope = await _make_loop_scope_with_conn_and_neo4j(
        deps.worker_pool, neo4j
    )

    try:
        resolved = loop_scope.resolved_cache()
        loop_conn_raw = resolved.get(asyncpg.Connection)
        assert isinstance(loop_conn_raw, asyncpg.Connection)
        loop_conn: asyncpg.Connection = loop_conn_raw  # type: ignore[assignment] # Why: PoolConnectionProxy is assignable to Connection at runtime but pyright sees a static mismatch

        enqueuer = SubJobEnqueuer(
            loop_scope_resolved=resolved,
            worker_pool=deps.worker_pool,
            backend=backend,
        )

        parent_job = await backend.enqueue(
            EnqueueArgs(
                id=new_job_id(),
                actor="process_property",
                queue="default",
                payload={"property_id": "site-42"},
                max_attempts=3,
                retry_kind="transient",
                scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            )
        )

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
            await _dispatch_job(conn, schema, worker_id, parent_job.id)

        job_row = _dc_replace(
            make_job_row(actor="process_property", payload={"property_id": "site-42"}),
            id=parent_job.id,
            locked_by_worker=worker_id,
        )

        async def run_actor(_job: JobRow, _ctx: JobContext[BaseModel]) -> object:
            neo4j_stub: object = resolved.get(Neo4jClientProtocol)
            assert isinstance(neo4j_stub, _StubNeo4jClient)
            await neo4j_stub.save("site-42", conn=loop_conn)
            await enqueuer.enqueue(_ENRICH_REF, _EnrichPayload())
            return {"ok": True}

        clk: Clock = SystemClock()
        with suppress(asyncio.CancelledError):
            await consume_one_job(
                backend,
                job_row,
                worker_id,
                run_actor=run_actor,
                actor_config=default_actor_config(),
                payload_type=_PropertyPayload,
                clock=clk,
                enqueuer=enqueuer,
                loop_conn=loop_conn,
            )

        async with deps.worker_pool.acquire() as check_conn:
            child_count = await _count_jobs(check_conn, schema, actor="enrich_property")
            parent_status = await check_conn.fetchval(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',
                parent_job.id,
            )

        assert "site-42" in neo4j.recorded_saves, "Neo4j save should have been recorded"
        assert child_count >= 1, "child row should exist in PG"
        assert parent_status == "succeeded", "parent should be succeeded"
    finally:
        await loop_scope.shutdown()
        await thread_scope.shutdown()
        await process_scope.shutdown()
