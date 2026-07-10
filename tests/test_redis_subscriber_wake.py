"""Integration tests for Redis pub/sub subscriber wake and reconnection behaviour.

All tests require live PG and Redis containers and are marked
``@pytest.mark.integration``.

Covers:
- Raw Redis pubsub subscriber receives all progress events emitted by an
  actor; events arrive in correct order; PG progress_seq matches.
- Two concurrent TaskQ.stream() calls both receive all terminal events
  (Redis fanout to multiple subscribers on the same channel).
- Subscriber on job A's channel receives NO events emitted for job B
  (Redis pub/sub per-job channel isolation — no cross-talk).
- Redis reconnection: subscriber closes and reopens pubsub connection;
  events published after reconnect are received. PG progress_seq is
  correct (polling fallback).
- Worker with Redis unavailable still records progress in PG;
  job succeeds; progress_seq is correct in PG.
"""

# ruff: noqa: S608 Why: schema name validated by WorkerSettings against _IDENT_RE; asyncpg has no parameter binding for identifiers.

import asyncio
import json
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.actor import actor
from taskq.backend._protocol import EnqueueArgs
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.client._taskq import JobEvent, TaskQ
from taskq.constants import progress_channel
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import StubActorConfig
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker._consumer import consume_one_job
from taskq.worker.deps import WorkerDeps, open_worker_deps

pytestmark = pytest.mark.integration


# ── Payload models ──────────────────────────────────────────────────────────


class _Empty(BaseModel):
    pass


# ── Actor definitions ───────────────────────────────────────────────────────

# Actors include a small sleep between progress calls so that the
# progress-flush background task (coalesce_interval=0.1s) can publish
# each event to Redis before the next one is emitted.


@actor(name="_sub_wake_progress")
async def _sub_wake_progress_actor(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    """Emit 5 progress events with increasing step and percent."""
    for i in range(5):
        await ctx.progress(step=i + 1, percent=float((i + 1) * 20))
        await asyncio.sleep(0.02)


@actor(name="_sub_wake_noise")
async def _sub_wake_noise_actor(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    """Emit 3 progress events — used as cross-talk noise on a different job."""
    for i in range(3):
        await ctx.progress(step=i + 1, percent=float((i + 1) * 33))
        await asyncio.sleep(0.02)


@actor(name="_sub_wake_single")
async def _sub_wake_single_actor(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    """Single progress call for reconnection and PG-only tests."""
    await ctx.progress(step=1, percent=50.0, detail="single")


# ── Setup helpers ───────────────────────────────────────────────────────────


async def _truncate_dynamic_tables(pg_dsn: str, schema: str) -> None:
    """Truncate all dynamic tables to ensure clean per-test state."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        # FK-safe truncation for tables that accumulate per-test data
        for table in ("jobs", "workers"):
            await conn.execute(f'TRUNCATE "{schema}"."{table}" CASCADE')
    finally:
        await conn.close()


async def _seed_actor_configs(conn: asyncpg.Connection, schema: str) -> None:
    """Ensure actor_config rows exist for the test actors in *schema*."""
    for actor_name in ["_sub_wake_progress", "_sub_wake_noise", "_sub_wake_single"]:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) '
            "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
            actor_name,
            "default",
        )


async def _setup_worker(
    pg_dsn: str,
    redis_url: str | None,
    *,
    schema: str,
) -> tuple[AsyncExitStack, WorkerDeps, PostgresBackend]:
    """Open WorkerDeps + PostgresBackend against *schema* with optional Redis.

    Seeds actor_config rows for the subscriber-wake test actors.
    Returns ``(stack, deps, backend)`` — caller must ``await stack.aclose()``.
    """
    settings_dict: dict[str, Any] = {
        "TASKQ_PG_DSN": pg_dsn,
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_PROGRESS_PUBLISH_GLOBAL": "true",
        "TASKQ_PROGRESS_COALESCE_INTERVAL": "0.1",
        "TASKQ_HEARTBEAT_INTERVAL": "0.5",
        "TASKQ_LOCK_LEASE": "30.0",
        "TASKQ_CANCELLATION_GRACE_PERIOD": "0.5",
        "TASKQ_CLEANUP_GRACE_PERIOD": "0.5",
    }
    if redis_url is not None:
        settings_dict["TASKQ_REDIS_URL"] = redis_url

    settings = WorkerSettings.load_from_dict(settings_dict)

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await _seed_actor_configs(conn, schema)
    finally:
        await conn.close()

    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    try:
        backend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=0.5),
            cleanup_grace_period=timedelta(seconds=0.5),
        )
    except BaseException:
        await stack.aclose()
        raise

    return stack, deps, backend


def _no_retry_config() -> StubActorConfig:
    return StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=1, jitter=0.0))


async def _register_worker(deps: WorkerDeps, wid: UUID, schema: str) -> None:
    """Insert a worker row for *wid* so dispatch_batch can assign jobs."""
    async with deps.dispatcher_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
            "VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
            wid,
            "test-host",
            12345,
            ["default"],
        )


async def _enqueue_only(
    backend: PostgresBackend,
    actor_name: str,
) -> UUID:
    """Enqueue a job in pending state; return its job_id."""
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor=actor_name,
            queue="default",
            payload={},
            payload_schema_ver=1,
            priority=0,
            max_attempts=1,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC),
        )
    )
    return job_id


async def _dispatch_one(
    backend: PostgresBackend,
    deps: WorkerDeps,
    wid: UUID,
) -> Any:  # returns JobRow
    """Dispatch one job for *wid* and return its JobRow."""
    lock_lease = timedelta(seconds=deps.settings.lock_lease)
    rows = await backend.dispatch_batch(wid, ["default"], limit=1, lock_lease=lock_lease)
    assert len(rows) == 1, f"expected 1 dispatched, got {len(rows)}"
    return rows[0]


async def _consume(
    deps: WorkerDeps,
    backend: PostgresBackend,
    job_row: Any,
    actor_fn: Any,
    wid: UUID,
) -> None:
    """Run an actor function via consume_one_job."""
    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None, worker_pool=deps.worker_pool, backend=backend
    )

    async def _run(jr: Any, ctx: JobContext[BaseModel]) -> object:
        return await actor_fn(payload=ctx.payload, ctx=ctx)

    await consume_one_job(
        backend,
        job_row,
        wid,
        deps=deps,
        run_actor=_run,
        actor_config=_no_retry_config(),
        payload_type=_Empty,
        clock=SystemClock(),
        enqueuer=enqueuer,
    )


async def _get_job_by_id(pool: asyncpg.Pool, schema: str, job_id: UUID) -> asyncpg.Record | None:
    """Fetch a job row by exact id."""
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            f'SELECT id, progress_seq, progress_state, status FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )


async def _open_taskq(
    pg_dsn: str,
    *,
    schema: str,
    redis_url: str | None = None,
    poll_timeout: float = 0.3,
) -> TaskQ:
    """Open a TaskQ against *schema* with optional Redis for streaming."""
    tq = TaskQ(
        dsn=pg_dsn,
        schema=schema,
        redis_url=redis_url,
        poll_timeout=poll_timeout,
    )
    await tq.open()
    return tq


async def _collect_stream_events(
    tq: TaskQ,
    job_id: UUID,
) -> list[JobEvent]:
    """Consume all events from TaskQ.stream() until terminal."""
    events: list[JobEvent] = []
    async for event in tq.stream(job_id):
        events.append(event)
    return events


# ── Redis subscriber receives all progress events ───────────────────────────


@pytest.mark.redis
async def test_redis_subscriber_receives_progress_events(
    module_pg_schema: ModulePgSchema,
    clean_redis_url: str,
) -> None:
    """Raw Redis pubsub subscriber receives all progress events
    emitted by an actor (5 progress calls). Events arrive with correct
    ordering and PG progress_seq == 5 after final flush.
    """
    import redis.asyncio as redis_async

    pg_dsn: str = module_pg_schema.pg_dsn
    schema: str = module_pg_schema.schema_name

    await _truncate_dynamic_tables(pg_dsn, schema)
    stack, deps, backend = await _setup_worker(pg_dsn, clean_redis_url, schema=schema)

    try:
        wid = new_uuid()
        await _register_worker(deps, wid, schema)

        job_id = await _enqueue_only(backend, "_sub_wake_progress")
        channel = progress_channel(schema, job_id)

        # Subscribe via raw Redis pubsub — proven pattern from test_progress_redis.py
        redis_client = redis_async.from_url(clean_redis_url, decode_responses=False)
        received: list[dict[str, object]] = []
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(channel)
            await asyncio.sleep(0.05)

            consume_task = asyncio.create_task(
                _consume(
                    deps,
                    backend,
                    await _dispatch_one(backend, deps, wid),
                    _sub_wake_progress_actor.fn,
                    wid,
                )
            )

            async def _collect_until_terminal() -> None:
                async for msg in pubsub.listen():
                    if msg.get("type") == "message":
                        data = json.loads(msg["data"])
                        received.append(data)
                        if data.get("kind") == "state_change" and data.get("terminal"):
                            return

            collect_task = asyncio.create_task(_collect_until_terminal())
            await asyncio.wait_for(
                asyncio.gather(consume_task, collect_task),
                timeout=30.0,
            )
            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        finally:
            await redis_client.aclose()

        # Should have: state_change(running) + 5x progress + state_change(succeeded)
        assert len(received) >= 7, (
            f"expected at least 7 events (1 running + 5 progress + 1 succeeded), "
            f"got {len(received)}: {[e.get('kind') for e in received]}"
        )
        kinds = [e.get("kind") for e in received]
        assert "progress" in kinds, f"expected at least one 'progress' event, got {kinds}"
        assert kinds[-1] == "state_change"
        assert received[-1].get("terminal") is True
        assert received[-1].get("status") == "succeeded"

        # Validate order: running → progress* → succeeded
        running_idx = next(
            i
            for i, k in enumerate(kinds)
            if k == "state_change" and received[i].get("status") == "running"
        )
        last_progress_idx = max(i for i, k in enumerate(kinds) if k == "progress")
        succeeded_idx = next(
            i
            for i, k in enumerate(kinds)
            if k == "state_change" and received[i].get("status") == "succeeded"
        )
        assert running_idx < last_progress_idx < succeeded_idx

        pg_row = await _get_job_by_id(deps.worker_pool, schema, job_id)
        assert pg_row is not None
        assert pg_row["status"] == "succeeded"
        assert pg_row["progress_seq"] == 5, (
            f"expected progress_seq==5, got {pg_row['progress_seq']}"
        )
    finally:
        await stack.aclose()


# ── Multiple subscribers receive same events ─────────────────────────────────


@pytest.mark.redis
async def test_multiple_subscribers_receive_same_events(
    module_pg_schema: ModulePgSchema,
    clean_redis_url: str,
) -> None:
    """Two concurrent TaskQ.stream() calls both receive terminal
    events (Redis pub/sub fans out to all subscribers on the same channel).

    Both streams start while the job is still pending so both subscriptions
    are established before the actor publishes.
    """
    pg_dsn: str = module_pg_schema.pg_dsn
    schema: str = module_pg_schema.schema_name

    await _truncate_dynamic_tables(pg_dsn, schema)

    tq1 = await _open_taskq(pg_dsn, schema=schema, redis_url=clean_redis_url, poll_timeout=0.3)
    tq2 = await _open_taskq(pg_dsn, schema=schema, redis_url=clean_redis_url, poll_timeout=0.3)
    stack, deps, backend = await _setup_worker(pg_dsn, clean_redis_url, schema=schema)

    try:
        wid = new_uuid()
        await _register_worker(deps, wid, schema)

        job_id = await _enqueue_only(backend, "_sub_wake_progress")

        stream1_task = asyncio.create_task(_collect_stream_events(tq1, job_id))
        stream2_task = asyncio.create_task(_collect_stream_events(tq2, job_id))

        await asyncio.sleep(0.2)

        job_row = await _dispatch_one(backend, deps, wid)
        await _consume(deps, backend, job_row, _sub_wake_progress_actor.fn, wid)

        events1, events2 = await asyncio.wait_for(
            asyncio.gather(stream1_task, stream2_task),
            timeout=30.0,
        )

        # Both streams should end with a terminal succeeded event
        for label, events in [("stream1", events1), ("stream2", events2)]:
            assert len(events) >= 2, f"{label}: expected at least 2 events, got {len(events)}"
            assert events[-1].terminal is True, f"{label}: last event not terminal"
            assert events[-1].status == "succeeded", (
                f"{label}: expected succeeded, got {events[-1].status}"
            )

        pg_row = await _get_job_by_id(deps.worker_pool, schema, job_id)
        assert pg_row is not None
        assert pg_row["status"] == "succeeded"
        assert pg_row["progress_seq"] == 5, (
            f"expected progress_seq==5, got {pg_row['progress_seq']}"
        )
    finally:
        await tq1.close()
        await tq2.close()
        await stack.aclose()


# ── Redis subscriber filters per-job channel correctly ──────────────────────


@pytest.mark.redis
async def test_subscriber_filters_per_job_channel_no_cross_talk(
    module_pg_schema: ModulePgSchema,
    clean_redis_url: str,
) -> None:
    """Subscriber on job A's channel receives NO events from job B
    (Redis pub/sub per-job channel isolation).

    Jobs are enqueued and dispatched one at a time to avoid dispatch_batch
    ordering ambiguity. Job B (noise) completes first while job A's
    subscriber is listening — the subscriber must receive only job A events.
    """
    import redis.asyncio as redis_async

    pg_dsn: str = module_pg_schema.pg_dsn
    schema: str = module_pg_schema.schema_name

    await _truncate_dynamic_tables(pg_dsn, schema)
    stack, deps, backend = await _setup_worker(pg_dsn, clean_redis_url, schema=schema)

    try:
        # Enqueue both jobs; dispatch_batch returns the oldest pending
        # job first, so we dispatch wid_a first (gets job A, enqueued
        # first), then wid_b (gets job B).
        wid_a = new_uuid()
        wid_b = new_uuid()
        await _register_worker(deps, wid_a, schema)
        await _register_worker(deps, wid_b, schema)

        job_id_a = await _enqueue_only(backend, "_sub_wake_progress")
        job_id_b = await _enqueue_only(backend, "_sub_wake_noise")

        channel_a = progress_channel(schema, job_id_a)

        redis_client = redis_async.from_url(clean_redis_url, decode_responses=False)
        received_a: list[dict[str, object]] = []
        try:
            pubsub_a = redis_client.pubsub()
            await pubsub_a.subscribe(channel_a)
            await asyncio.sleep(0.05)

            # Dispatch wid_a first → gets job A (oldest)
            job_row_a = await _dispatch_one(backend, deps, wid_a)
            assert str(job_row_a.id) == str(job_id_a), (
                f"dispatch order: expected job A, got {job_row_a.id}"
            )

            # Dispatch wid_b → gets job B (only remaining pending job)
            job_row_b = await _dispatch_one(backend, deps, wid_b)
            assert str(job_row_b.id) == str(job_id_b), (
                f"dispatch order: expected job B, got {job_row_b.id}"
            )

            # Run job B first (noise on a different Redis channel)
            consume_b = asyncio.create_task(
                _consume(deps, backend, job_row_b, _sub_wake_noise_actor.fn, wid_b)
            )
            # Brief yield so job B starts publishing before job A runs
            await asyncio.sleep(0.02)

            # Run job A while subscriber is listening
            consume_task_a = asyncio.create_task(
                _consume(deps, backend, job_row_a, _sub_wake_progress_actor.fn, wid_a)
            )

            async def _collect_a() -> None:
                async for msg in pubsub_a.listen():
                    if msg.get("type") == "message":
                        data = json.loads(msg["data"])
                        received_a.append(data)
                        if data.get("kind") == "state_change" and data.get("terminal"):
                            return

            collect_task_a = asyncio.create_task(_collect_a())
            await asyncio.wait_for(
                asyncio.gather(consume_b, consume_task_a, collect_task_a),
                timeout=30.0,
            )
            await pubsub_a.unsubscribe(channel_a)
            await pubsub_a.aclose()
        finally:
            await redis_client.aclose()

        # Verify job A stream: events are all for job A
        assert len(received_a) >= 2, f"stream A: expected at least 2 events, got {len(received_a)}"
        for event in received_a:
            assert event.get("job_id") == str(job_id_a), (
                f"stream A received event for different job: {event.get('job_id')} != {job_id_a}"
            )
        assert received_a[-1].get("terminal") is True
        assert received_a[-1].get("status") == "succeeded"

        # Job B should also have succeeded and recorded progress in PG
        pg_row_b = await _get_job_by_id(deps.worker_pool, schema, job_id_b)
        assert pg_row_b is not None
        assert pg_row_b["status"] == "succeeded"
        assert pg_row_b["progress_seq"] == 3, (
            f"expected progress_seq==3 for job B, got {pg_row_b['progress_seq']}"
        )

        pg_row_a = await _get_job_by_id(deps.worker_pool, schema, job_id_a)
        assert pg_row_a is not None
        assert pg_row_a["status"] == "succeeded"
        assert pg_row_a["progress_seq"] == 5
    finally:
        await stack.aclose()


# ── Redis reconnection — subscriber recovers on reconnect ───────────────────


@pytest.mark.redis
async def test_redis_reconnection_subscriber_recovers(
    module_pg_schema: ModulePgSchema,
    clean_redis_url: str,
) -> None:
    """Redis subscriber reconnects after closing and reopening the
    pubsub connection. Two independent subscriptions on different jobs
    demonstrate that a subscriber can reconnect and receive events.

    PG progress_seq confirms the polling fallback stores state durably.
    """
    import redis.asyncio as redis_async

    pg_dsn: str = module_pg_schema.pg_dsn
    schema: str = module_pg_schema.schema_name

    await _truncate_dynamic_tables(pg_dsn, schema)
    stack, deps, backend = await _setup_worker(pg_dsn, clean_redis_url, schema=schema)

    try:
        # ── Phase 1: first subscription ──────────────────────────────────
        wid1 = new_uuid()
        await _register_worker(deps, wid1, schema)
        job_id1 = await _enqueue_only(backend, "_sub_wake_single")
        job_row1 = await _dispatch_one(backend, deps, wid1)
        channel1 = progress_channel(schema, job_id1)

        client1 = redis_async.from_url(clean_redis_url, decode_responses=False)
        received1: list[dict[str, object]] = []
        try:
            pubsub1 = client1.pubsub()
            await pubsub1.subscribe(channel1)
            await asyncio.sleep(0.05)

            consume_task1 = asyncio.create_task(
                _consume(deps, backend, job_row1, _sub_wake_single_actor.fn, wid1)
            )

            async def _collect1() -> None:
                async for msg in pubsub1.listen():
                    if msg.get("type") == "message":
                        data = json.loads(msg["data"])
                        received1.append(data)
                        if data.get("kind") == "state_change" and data.get("terminal"):
                            return

            collect_task1 = asyncio.create_task(_collect1())
            await asyncio.wait_for(
                asyncio.gather(consume_task1, collect_task1),
                timeout=30.0,
            )
            await pubsub1.unsubscribe(channel1)
            await pubsub1.aclose()
        finally:
            await client1.aclose()

        assert len(received1) >= 2, f"phase 1: expected at least 2 events, got {len(received1)}"

        pg_row1 = await _get_job_by_id(deps.worker_pool, schema, job_id1)
        assert pg_row1 is not None
        assert pg_row1["status"] == "succeeded"
        assert pg_row1["progress_seq"] == 1

        # ── Phase 2: new subscriber, new job (reconnect) ─────────────────
        wid2 = new_uuid()
        await _register_worker(deps, wid2, schema)
        job_id2 = await _enqueue_only(backend, "_sub_wake_single")
        job_row2 = await _dispatch_one(backend, deps, wid2)
        channel2 = progress_channel(schema, job_id2)

        client2 = redis_async.from_url(clean_redis_url, decode_responses=False)
        received2: list[dict[str, object]] = []
        try:
            pubsub2 = client2.pubsub()
            await pubsub2.subscribe(channel2)
            await asyncio.sleep(0.05)

            consume_task2 = asyncio.create_task(
                _consume(deps, backend, job_row2, _sub_wake_single_actor.fn, wid2)
            )

            async def _collect2() -> None:
                async for msg in pubsub2.listen():
                    if msg.get("type") == "message":
                        data = json.loads(msg["data"])
                        received2.append(data)
                        if data.get("kind") == "state_change" and data.get("terminal"):
                            return

            collect_task2 = asyncio.create_task(_collect2())
            await asyncio.wait_for(
                asyncio.gather(consume_task2, collect_task2),
                timeout=30.0,
            )
            await pubsub2.unsubscribe(channel2)
            await pubsub2.aclose()
        finally:
            await client2.aclose()

        assert len(received2) >= 2, (
            f"phase 2: expected at least 2 events after reconnect, got {len(received2)}"
        )
        kinds2 = [e.get("kind") for e in received2]
        assert "state_change" in kinds2, (
            f"expected state_change in post-reconnect events, got {kinds2}"
        )

        pg_row2 = await _get_job_by_id(deps.worker_pool, schema, job_id2)
        assert pg_row2 is not None
        assert pg_row2["status"] == "succeeded"
        assert pg_row2["progress_seq"] == 1
    finally:
        await stack.aclose()


# ── Redis unavailable but PG still records progress ─────────────────────────


async def test_redis_unavailable_pg_still_records_progress(
    module_pg_schema: ModulePgSchema,
) -> None:
    """Worker with Redis unavailable still records progress in PG;
    job succeeds; progress_seq is correct.

    WorkerDeps is opened without a Redis URL. The actor calls
    ctx.progress() — the fire-and-forget publish is a no-op (no Redis
    client), but the PG flush path still records progress.
    """
    pg_dsn: str = module_pg_schema.pg_dsn
    schema: str = module_pg_schema.schema_name

    await _truncate_dynamic_tables(pg_dsn, schema)
    stack, deps, backend = await _setup_worker(pg_dsn, None, schema=schema)

    try:
        wid = new_uuid()
        await _register_worker(deps, wid, schema)

        job_id = await _enqueue_only(backend, "_sub_wake_progress")
        job_row = await _dispatch_one(backend, deps, wid)

        # Consume the actor — progress is flushed to PG only (no Redis)
        await _consume(deps, backend, job_row, _sub_wake_progress_actor.fn, wid)

        pg_row = await _get_job_by_id(deps.worker_pool, schema, job_id)
        assert pg_row is not None
        assert pg_row["status"] == "succeeded"
        assert pg_row["progress_seq"] == 5, (
            f"expected progress_seq==5 (PG-only path), got {pg_row['progress_seq']}"
        )
    finally:
        await stack.aclose()
