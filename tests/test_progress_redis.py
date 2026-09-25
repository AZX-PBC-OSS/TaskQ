"""Integration tests for Redis progress pub/sub.

All tests require a live Redis container and are marked @pytest.mark.integration.

Test plan
---------
Actor calls ctx.progress() 100 times; the publish gate coalesces: the
        subscriber receives at most 2 kind='progress' events (the first
        call's and the latched latest one, see JobContext.progress), the
        last carrying the final seq; PG progress_seq == 100 after final
        flush; status = 'succeeded'.
Subscribe before enqueue; actor calls ctx.progress(step=1) then returns; events
        arrive in order: kind='progress', kind='state_change'(succeeded, terminal=True).
First non-subscribe message is NOT a progress event (subscribe happens before job).
Redis publish round trip raises after 1st; channel='per_job' label on
        taskq.progress.publish_failures counter.
"""

# ruff: noqa: S608 Why: schema name validated by WorkerSettings against _IDENT_RE; asyncpg has no parameter binding for identifiers.

import asyncio
import contextlib
import json
from contextlib import AsyncExitStack
from datetime import timedelta
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
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
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
            # None = immediate, in the SERVER clock domain. An absolute
            # datetime.now() races the enqueue SQL's status boundary
            # (COALESCE($n, clock_timestamp()) > clock_timestamp()) -
            # with the testcontainer clock a fraction of a millisecond
            # behind the host, a warm asyncpg statement cache (sub-ms
            # sample→execute latency, as in the parallel suite) lands the
            # row 'scheduled', which the dispatch_batch below cannot
            # claim.
            scheduled_at=None,
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
    """Actor calls ctx.progress() 100 times; the publish gate coalesces.

    Oracle: Redis subscriber on per-job channel receives at most 2
    kind='progress' events (the first call publishes; calls racing that
    in-flight publish latch on the buffer and the running task re-publishes
    the latched latest event), the LAST event carries the final seq and
    step; the dispatch's running transition consumed seq 1, so the 100
    progress calls run 2..101 and the final publish carries 101; PG
    progress_seq == 101 after the terminal write consumed its own seq
    past the last progress event; status = 'succeeded'.
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

        # socket_timeout explicit: redis-py 8 defaults it to 5s and the default fires on a
        # fresh connection's handshake read under a co-tenant-stretched runner
        # (Timeout reading from localhost:..., the subscribe ack never arriving in
        # time); the scenario's real bounds are the test's own wait_for windows.
        redis_client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
        received_events: list[dict[str, object]] = []
        try:
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(channel)
            # Brief pause to let the subscribe ack arrive
            await asyncio.sleep(0.05)

            async def _collect_while_consuming() -> None:
                async for msg in pubsub.listen():
                    if msg.get("type") == "message":
                        data = json.loads(msg["data"])
                        if data.get("kind") == "progress":
                            received_events.append(data)

            consume_task = asyncio.create_task(
                _consume(deps, backend, job_row, _progress_hundred_actor.fn, wid)
            )
            collect_task = asyncio.create_task(_collect_while_consuming())

            await asyncio.wait_for(consume_task, timeout=30.0)
            # The coalesced latch publishes when the in-flight round trip
            # lands, right after the actor returned; give it a moment.
            await asyncio.sleep(0.5)
            collect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await collect_task

            await pubsub.unsubscribe(channel)
            await pubsub.aclose()
        finally:
            await redis_client.aclose()

        assert 1 <= len(received_events) <= 2, (
            f"expected the first and the coalesced final publish at most, "
            f"got {len(received_events)}"
        )
        last_event = received_events[-1]
        assert last_event["seq"] == 101, (
            f"expected the final publish to carry seq 101 (the running "
            f"transition consumed seq 1), got {last_event['seq']}"
        )
        assert last_event["step"] == 100

        pg_row = await _get_job_row(
            deps.worker_pool, deps.settings.schema_name, "_progress_redis_hundred"
        )
        assert pg_row["status"] == "succeeded"
        assert pg_row["progress_seq"] == 102, (
            f"expected progress_seq==102 (the terminal write consumed one "
            f"past the last progress event at 101), got {pg_row['progress_seq']}"
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

        # socket_timeout explicit: redis-py 8 defaults it to 5s and the default fires on a
        # fresh connection's handshake read under a co-tenant-stretched runner
        # (Timeout reading from localhost:..., the subscribe ack never arriving in
        # time); the scenario's real bounds are the test's own wait_for windows.
        redis_client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
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
    received must be a kind='state_change' with status='running' - published
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

        # socket_timeout explicit: redis-py 8 defaults it to 5s and the default fires on a
        # fresh connection's handshake read under a co-tenant-stretched runner
        # (Timeout reading from localhost:..., the subscribe ack never arriving in
        # time); the scenario's real bounds are the test's own wait_for windows.
        redis_client = redis_async.from_url(redis_url, decode_responses=False, socket_timeout=None)
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


# ── Redis publish raises after 1st round trip ──────────────────────────


class _FailingPipeline:
    """Pipeline stand-in whose ``execute`` raises.

    Simulates a failed pipelined dual-channel publish round trip - the
    surface progress events actually go through when
    ``progress_publish_global`` is on (one pipeline, one execute, both
    channels; ``client.publish`` is never called on that path).
    """

    def __init__(self, error: Exception) -> None:
        self._error = error

    def publish(self, *_args: object) -> None:
        return None

    async def execute(self) -> list[int]:
        raise self._error

    async def __aenter__(self) -> "_FailingPipeline":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None


async def test_tc1_publish_failure_counter_labeled_per_job(
    pg_dsn: str,
    redis_url: str,
    monkeypatch: pytest.MonkeyPatch,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Redis publish raises after the 1st round trip.

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
        # Inject failure after the 1st publish round trip: each progress
        # event is one pipelined execute carrying both channels, so the
        # failure is injected at the pipeline, not at client.publish
        # (which the pipelined path never calls).
        if deps.redis_client is not None:
            original_pipeline = deps.redis_client.pipeline
            round_trips: list[int] = [0]

            def _fail_after_one(transaction: bool = True, shard_hint: str | None = None) -> object:
                round_trips[0] += 1
                if round_trips[0] > 1:
                    return _FailingPipeline(ConnectionError("simulated Redis failure"))
                return original_pipeline(transaction=transaction, shard_hint=shard_hint)

            monkeypatch.setattr(deps.redis_client, "pipeline", _fail_after_one)

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
