"""Integration tests for Redis progress pub/sub.

All tests require a live Redis container and are marked @pytest.mark.integration.

Test plan
---------
Actor calls ctx.progress() 100 times; subscriber receives all 100 kind='progress'
        events; PG progress_seq == 100 after final flush; status = 'succeeded'.
Subscribe before enqueue; actor calls ctx.progress(step=1) then returns; events
        arrive in order: kind='progress', kind='state_change'(succeeded, terminal=True).
First non-subscribe message is NOT a progress event (subscribe happens before job).
Redis publish call raises after 1st; channel='per_job' label on
        taskq.progress.publish_failures counter.
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

import taskq.obs._otel as otel_mod
from taskq._ids import new_job_id, new_uuid
from taskq.actor import actor
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.constants import progress_channel
from taskq.context import JobContext
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import StubActorConfig
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.otel import counter_data_points, setup_meter
from taskq.worker._consumer import consume_one_job
from taskq.worker.deps import WorkerDeps, open_worker_deps

pytestmark = [pytest.mark.integration, pytest.mark.redis]

# ── Payload models ──────────────────────────────────────────────────────────


class _Empty(BaseModel):
    pass


# ── Actor definitions ───────────────────────────────────────────────────────


@actor(name="_progress_redis_hundred")
async def _progress_hundred_actor(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    for i in range(100):
        await ctx.progress(step=i + 1, percent=float(i + 1))


@actor(name="_progress_redis_single")
async def _progress_single_actor(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    await ctx.progress(step=1, percent=50.0, detail="one-shot")


@actor(name="_progress_redis_three")
async def _progress_three_actor(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    for i in range(3):
        await ctx.progress(step=i + 1)


# ── Setup helpers ───────────────────────────────────────────────────────────


async def _setup_worker(
    pg_dsn: str,
    redis_url: str,
    *,
    schema: str,
) -> tuple[AsyncExitStack, WorkerDeps, PostgresBackend]:
    from taskq.migrate import apply_pending

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_REDIS_URL": redis_url,
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "true",
            "TASKQ_PROGRESS_COALESCE_INTERVAL": "0.1",
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            "TASKQ_LOCK_LEASE": "30.0",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.5",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.5",
        }
    )

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING',
            "_progress_redis_hundred",
            "default",
        )
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING',
            "_progress_redis_single",
            "default",
        )
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING',
            "_progress_redis_three",
            "default",
        )
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


async def _enqueue_and_dispatch(
    deps: WorkerDeps,
    backend: PostgresBackend,
    actor_name: str,
    wid: UUID,
) -> Any:  # returns JobRow
    from taskq.backend._protocol import EnqueueArgs

    schema = deps.settings.schema_name

    async with deps.dispatcher_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
            "VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
            wid,
            "test-host",
            12345,
            ["default"],
        )

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


async def _get_job_row(pool: asyncpg.Pool, schema: str, actor_name: str) -> asyncpg.Record:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT id, progress_seq, progress_state, status "
            f'FROM "{schema}".jobs WHERE actor = $1 '
            "ORDER BY created_at DESC LIMIT 1",
            actor_name,
        )
    assert row is not None
    return row


# ── 100 progress events ──────────────────────────────────────────────


async def test_ti2_hundred_progress_events(
    pg_dsn: str, redis_url: str, module_pg_schema: ModulePgSchema
) -> None:
    """Actor calls ctx.progress() 100 times; subscriber receives all 100 events.

    Oracle: Redis subscriber on per-job channel receives exactly 100 kind='progress'
    events; PG progress_seq == 100 after final flush; status = 'succeeded'.
    """
    import redis.asyncio as redis_async

    stack, deps, backend = await _setup_worker(
        pg_dsn, redis_url, schema=module_pg_schema.schema_name
    )
    try:
        wid = new_uuid()
        job_row = await _enqueue_and_dispatch(deps, backend, "_progress_redis_hundred", wid)
        job_id: UUID = job_row.id
        channel = progress_channel(deps.settings.schema_name, job_id)

        redis_client = redis_async.from_url(redis_url, decode_responses=False)
        received_events: list[dict[str, object]] = []
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(channel)
            # Brief pause to let the subscribe ack arrive
            await asyncio.sleep(0.05)

            async def _collect_all(expected: int) -> None:
                async for msg in pubsub.listen():
                    if msg.get("type") == "message":
                        data = json.loads(msg["data"])
                        if data.get("kind") == "progress":
                            received_events.append(data)
                        if len(received_events) >= expected:
                            return

            consume_task = asyncio.create_task(
                _consume(deps, backend, job_row, _progress_hundred_actor.fn, wid)
            )
            collect_task = asyncio.create_task(_collect_all(100))

            await asyncio.wait_for(
                asyncio.gather(consume_task, collect_task),
                timeout=30.0,
            )

            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        finally:
            await redis_client.aclose()

        assert len(received_events) == 100, (
            f"expected 100 progress events, got {len(received_events)}"
        )

        pg_row = await _get_job_row(
            deps.worker_pool, deps.settings.schema_name, "_progress_redis_hundred"
        )
        assert pg_row["status"] == "succeeded"
        assert pg_row["progress_seq"] == 100, (
            f"expected progress_seq==100, got {pg_row['progress_seq']}"
        )
    finally:
        await stack.aclose()


# ── Ordered events: progress then state_change ──────────────────────


async def test_ti3_event_ordering_progress_then_succeeded(
    pg_dsn: str, redis_url: str, module_pg_schema: ModulePgSchema
) -> None:
    """Events arrive in order: state_change(running), kind='progress',
    kind='state_change'(succeeded).

    Subscribe before enqueuing. Actor calls ctx.progress(step=1) then returns.

    Oracle: subscriber receives at least one kind='progress' event preceded by
    a kind='state_change' with status='running', and followed by kind='state_change'
    with status='succeeded' and terminal=True; PG status='succeeded'.
    """
    import redis.asyncio as redis_async

    stack, deps, backend = await _setup_worker(
        pg_dsn, redis_url, schema=module_pg_schema.schema_name
    )
    try:
        wid = new_uuid()
        job_row = await _enqueue_and_dispatch(deps, backend, "_progress_redis_single", wid)
        job_id: UUID = job_row.id
        channel = progress_channel(deps.settings.schema_name, job_id)

        redis_client = redis_async.from_url(redis_url, decode_responses=False)
        ordered_events: list[dict[str, object]] = []
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(channel)
            await asyncio.sleep(0.05)

            async def _collect_until_terminal() -> None:
                async for msg in pubsub.listen():
                    if msg.get("type") == "message":
                        data = json.loads(msg["data"])
                        ordered_events.append(data)
                        if data.get("kind") == "state_change" and data.get("terminal") is True:
                            return

            consume_task = asyncio.create_task(
                _consume(deps, backend, job_row, _progress_single_actor.fn, wid)
            )
            collect_task = asyncio.create_task(_collect_until_terminal())

            await asyncio.wait_for(
                asyncio.gather(consume_task, collect_task),
                timeout=30.0,
            )

            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        finally:
            await redis_client.aclose()

        kinds = [e.get("kind") for e in ordered_events]
        assert "progress" in kinds, f"expected at least one 'progress' event, got {kinds}"
        assert kinds[-1] == "state_change", f"last event should be 'state_change', got {kinds[-1]}"

        terminal_event = ordered_events[-1]
        assert terminal_event.get("status") == "succeeded"
        assert terminal_event.get("terminal") is True

        running_events = [
            e
            for e in ordered_events
            if e.get("kind") == "state_change" and e.get("status") == "running"
        ]
        assert len(running_events) >= 1, "expected at least one state_change(running) event"

        running_idx = next(
            i
            for i, e in enumerate(ordered_events)
            if e.get("kind") == "state_change" and e.get("status") == "running"
        )
        last_progress_idx = max(i for i, k in enumerate(kinds) if k == "progress")
        succeeded_idx = next(
            i
            for i, e in enumerate(ordered_events)
            if e.get("kind") == "state_change" and e.get("status") == "succeeded"
        )
        assert running_idx < last_progress_idx < succeeded_idx, (
            "expected state_change(running) < progress < state_change(succeeded)"
        )

        pg_row = await _get_job_row(
            deps.worker_pool, deps.settings.schema_name, "_progress_redis_single"
        )
        assert pg_row["status"] == "succeeded"
    finally:
        await stack.aclose()


# ── First non-subscribe message is not a progress event ────────────


async def test_ti3b_first_message_is_state_change_running(
    pg_dsn: str, redis_url: str, module_pg_schema: ModulePgSchema
) -> None:
    """When subscribing before the job starts, the first real message
    received must be a kind='state_change' with status='running' — published
    after the job is dispatched and before the actor body runs.

    This confirms subscribe-before-start guarantees no missed events and that
    the 'running' state_change event is the first message published.
    """
    import redis.asyncio as redis_async

    stack, deps, backend = await _setup_worker(
        pg_dsn, redis_url, schema=module_pg_schema.schema_name
    )
    try:
        wid = new_uuid()

        job_row = await _enqueue_and_dispatch(deps, backend, "_progress_redis_single", wid)
        job_id: UUID = job_row.id
        channel = progress_channel(deps.settings.schema_name, job_id)

        redis_client = redis_async.from_url(redis_url, decode_responses=False)
        first_real_message: list[dict[str, object]] = []
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(channel)
            await asyncio.sleep(0.05)

            async def _get_first_message() -> None:
                async for msg in pubsub.listen():
                    if msg.get("type") == "message":
                        first_real_message.append(json.loads(msg["data"]))
                        return

            consume_task = asyncio.create_task(
                _consume(deps, backend, job_row, _progress_single_actor.fn, wid)
            )
            collect_task = asyncio.create_task(_get_first_message())

            await asyncio.wait_for(
                asyncio.gather(consume_task, collect_task),
                timeout=30.0,
            )

            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        finally:
            await redis_client.aclose()

        assert len(first_real_message) == 1, "expected exactly one first message"
        first = first_real_message[0]
        assert first.get("kind") == "state_change", (
            f"expected first message kind='state_change', got {first.get('kind')}"
        )
        assert first.get("status") == "running", (
            f"expected first message status='running', got {first.get('status')}"
        )
        assert first.get("terminal") is False
    finally:
        await stack.aclose()


# ── Redis publish raises after 1st call ─────────────────────────────


async def test_tc1_publish_failure_counter_labeled_per_job(
    pg_dsn: str,
    redis_url: str,
    monkeypatch: pytest.MonkeyPatch,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Redis publish raises after the 1st call.

    Oracle: taskq.progress.publish_failures counter has a data point with
    channel='per_job' attribute.
    """
    reader = setup_meter(monkeypatch)
    monkeypatch.setattr(
        otel_mod,
        "_progress_publish_failures",
        otel_mod.get_meter().create_counter("taskq.progress.publish_failures"),
    )

    stack, deps, backend = await _setup_worker(
        pg_dsn, redis_url, schema=module_pg_schema.schema_name
    )
    try:
        # Inject failure after the 1st publish
        if deps.redis_client is not None:
            original_publish = deps.redis_client.publish
            call_count: list[int] = [0]

            async def _fail_after_one(channel: str, message: bytes | str) -> int:
                call_count[0] += 1
                if call_count[0] > 1:
                    raise ConnectionError("simulated Redis failure")
                return await original_publish(channel, message)

            monkeypatch.setattr(deps.redis_client, "publish", _fail_after_one)

        wid = new_uuid()
        job_row = await _enqueue_and_dispatch(deps, backend, "_progress_redis_three", wid)
        await _consume(deps, backend, job_row, _progress_three_actor.fn, wid)

        points = counter_data_points(reader, "taskq.progress.publish_failures")
        assert points, "expected at least one data point on taskq.progress.publish_failures"

        channel_labels = {
            str(p.attributes.get("channel")) for p in points if p.attributes is not None
        }
        assert "per_job" in channel_labels, (
            f"expected 'per_job' in channel labels, got {channel_labels}"
        )
    finally:
        await stack.aclose()
