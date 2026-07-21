"""Integration tests for progress flush to Postgres.

All tests require a live Postgres container and are marked @pytest.mark.integration.

Test plan
---------
Full round-trip: actor calls ctx.progress(...), PG row updated after job succeeds.
Coalesced flush timing: progress_flush_loop writes during actor sleep.
Redis publish failure + PG still written: dead Redis URL, job succeeds.
Redis disconnect mid-stream (simulated via monkeypatch): PG has all updates.
Crash mid-progress: crash-flush writes partial progress before mark_failed.
PG unavailable during periodic flush: flush task logs error, recovers, job completes.
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
from taskq.backend._protocol import EnqueueArgs
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.progress._flush import progress_flush_loop
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import StubActorConfig
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.otel import counter_value, setup_meter
from taskq.worker._consumer import consume_one_job
from taskq.worker.deps import WorkerDeps, open_worker_deps

pytestmark = pytest.mark.integration

# ── Payload models ──────────────────────────────────────────────────────────


class _Empty(BaseModel):
    pass


# ── Actor definitions ───────────────────────────────────────────────────────


@actor(name="_progress_pg_basic")
async def _progress_basic_actor(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    await ctx.progress(step=1, percent=50.0, detail="halfway", data={"count": 42})


@actor(name="_progress_pg_sleep")
async def _progress_sleep_actor(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    """Calls progress then sleeps, allowing flush loop to fire mid-execution."""
    await ctx.progress(step=1, percent=10.0, detail="started")
    await asyncio.sleep(0.3)


@actor(name="_progress_pg_redis_fail")
async def _progress_redis_fail_actor(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    await ctx.progress(step=1, percent=25.0, detail="redis-down")


@actor(name="_progress_pg_multi")
async def _progress_multi_actor(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    for i in range(5):
        await ctx.progress(step=i + 1, percent=float((i + 1) * 20))


@actor(name="_progress_pg_crash")
async def _progress_crash_actor(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    await ctx.progress(step=1, percent=10.0, detail="before_crash")
    raise RuntimeError("boom")


# ── Setup helpers ───────────────────────────────────────────────────────────


async def _setup_worker(
    pg_dsn: str,
    *,
    schema: str,
    redis_url: str | None = None,
) -> tuple[AsyncExitStack, WorkerDeps, PostgresBackend]:
    from taskq.migrate import apply_pending

    extra: dict[str, str] = {}
    if redis_url is not None:
        extra["TASKQ_REDIS_URL"] = redis_url
        extra["TASKQ_PROGRESS_PUBLISH_GLOBAL"] = "true"
    extra["TASKQ_PROGRESS_COALESCE_INTERVAL"] = "0.1"

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            "TASKQ_LOCK_LEASE": "30.0",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.5",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.5",
            **extra,
        }
    )

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
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


async def _run_job(
    deps: WorkerDeps,
    backend: PostgresBackend,
    actor_ref: Any,  # ActorRef — typed as Any to avoid complex generic spelling
    *,
    worker_id: UUID | None = None,
) -> None:
    """Enqueue, dispatch to running, then consume_one_job."""
    wid = worker_id or new_uuid()
    schema = deps.settings.schema_name

    # Insert a worker row (required by FK on some PG versions)
    async with deps.dispatcher_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
            "VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
            wid,
            "test-host",
            12345,
            ["default"],
        )
        # Insert actor_config row — required by per_actor_capacity CTE for dispatch
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING',
            actor_ref.name,
            "default",
        )

    lock_lease = timedelta(seconds=deps.settings.lock_lease)
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor=actor_ref.name,
            queue="default",
            payload={},
            payload_schema_ver=1,
            priority=0,
            max_attempts=1,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )

    rows = await backend.dispatch_batch(wid, ["default"], limit=1, lock_lease=lock_lease)
    assert len(rows) == 1, "expected exactly one dispatched job"
    job_row = rows[0]

    enqueuer = SubJobEnqueuer(
        loop_scope_resolved=None, worker_pool=deps.worker_pool, backend=backend
    )

    async def _run(jr: Any, ctx: JobContext[BaseModel]) -> object:
        return await actor_ref.fn(payload=ctx.payload, ctx=ctx)

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


async def _query_progress_row(
    pool: asyncpg.Pool,
    schema: str,
    job_id: UUID,
) -> asyncpg.Record:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT progress_seq, progress_state, status FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
    assert row is not None, f"no job row found for {job_id}"
    return row


async def _get_job_id_from_pool(pool: asyncpg.Pool, schema: str, actor_name: str) -> UUID:
    """Return the first job id with the given actor name."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT id FROM "{schema}".jobs WHERE actor = $1 ORDER BY created_at DESC LIMIT 1',
            actor_name,
        )
    assert row is not None
    return UUID(str(row["id"]))


# ── Full round-trip ──────────────────────────────────────────────────


async def test_ti1_progress_round_trip(pg_dsn: str, module_pg_schema: ModulePgSchema) -> None:
    """Actor calls ctx.progress(); PG row updated after job succeeds.

    Oracle: progress_seq >= 1, progress_state contains step/percent/detail.
    """
    stack, deps, backend = await _setup_worker(pg_dsn, schema=module_pg_schema.schema_name)
    try:
        await _run_job(deps, backend, _progress_basic_actor)

        job_id = await _get_job_id_from_pool(
            deps.worker_pool, deps.settings.schema_name, "_progress_pg_basic"
        )
        row = await _query_progress_row(deps.worker_pool, deps.settings.schema_name, job_id)

        assert row["progress_seq"] >= 1, f"expected progress_seq >= 1, got {row['progress_seq']}"
        assert row["status"] == "succeeded"

        state: dict[str, object] = json.loads(row["progress_state"])
        assert state.get("step") == 1
        assert state.get("percent") == 50.0
        assert state.get("detail") == "halfway"
    finally:
        await stack.aclose()


# ── Coalesced flush timing ──────────────────────────────────────────


async def test_ti4_coalesced_flush_fires_during_actor_sleep(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> None:
    """progress_flush_loop writes to PG during the actor's sleep.

    The actor calls ctx.progress() then sleeps 0.3 s. The flush loop runs
    with coalesce_interval=0.1 s in a background task.

    Oracle: querying PG via a separate connection mid-execution shows
    progress_seq=1 before the job ends.
    """
    stack, deps, backend = await _setup_worker(pg_dsn, schema=module_pg_schema.schema_name)
    try:
        wid = new_uuid()
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
            # Insert actor_config row — required by per_actor_capacity CTE for dispatch
            await conn.execute(
                f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING',
                "_progress_pg_sleep",
                "default",
            )

        job_id = new_job_id()
        await backend.enqueue(
            EnqueueArgs(
                id=job_id,
                actor="_progress_pg_sleep",
                queue="default",
                payload={},
                payload_schema_ver=1,
                priority=0,
                max_attempts=1,
                retry_kind="transient",
                scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
            )
        )

        lock_lease = timedelta(seconds=deps.settings.lock_lease)
        rows = await backend.dispatch_batch(wid, ["default"], limit=1, lock_lease=lock_lease)
        assert len(rows) == 1
        job_row = rows[0]

        enqueuer = SubJobEnqueuer(
            loop_scope_resolved=None, worker_pool=deps.worker_pool, backend=backend
        )

        mid_seq: list[int] = []
        shutdown = asyncio.Event()

        # Separate connection for mid-execution PG poll
        probe_conn = await asyncpg.connect(str(deps.settings.pg_dsn))
        try:

            async def _poll_mid_execution() -> None:
                """Poll PG until progress_seq > 0 or give up after 2 s."""
                for _ in range(40):
                    r = await probe_conn.fetchrow(
                        f'SELECT progress_seq FROM "{schema}".jobs WHERE id = $1',
                        job_id,
                    )
                    if r is not None and r["progress_seq"] > 0:
                        mid_seq.append(r["progress_seq"])
                        return
                    await asyncio.sleep(0.05)

            async def _run_actor(jr: Any, ctx: JobContext[BaseModel]) -> object:
                return await _progress_sleep_actor.fn(payload=ctx.payload, ctx=ctx)  # pyright: ignore[reportGeneralTypeIssues] # Why: ActorRef.fn is typed Callable[..., object] (sync or async); at runtime this actor is async.

            flush_task = asyncio.create_task(
                progress_flush_loop(
                    lambda: deps.worker_pool,
                    schema,
                    wid,
                    deps.progress_buffers,
                    0.1,  # fast coalesce for test
                    shutdown,
                )
            )
            poll_task = asyncio.create_task(_poll_mid_execution())

            await consume_one_job(
                backend,
                job_row,
                wid,
                deps=deps,
                run_actor=_run_actor,
                actor_config=_no_retry_config(),
                payload_type=_Empty,
                clock=SystemClock(),
                enqueuer=enqueuer,
            )

            await poll_task
            shutdown.set()
            await flush_task
        finally:
            await probe_conn.close()

        assert mid_seq, (
            "progress_seq was never > 0 during the actor sleep — flush loop did not fire"
        )
        assert mid_seq[0] >= 1
    finally:
        await stack.aclose()


# ── Redis publish failure + PG still written ───────────────────────


@pytest.mark.redis
async def test_ti5_redis_failure_pg_written(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch, module_pg_schema: ModulePgSchema
) -> None:
    """Dead Redis URL; publish silently fails; PG updated; job succeeds.

    Oracle: progress_state updated, taskq.progress.publish_failures counter >= 1,
    job status = 'succeeded'.
    """
    reader = setup_meter(monkeypatch)
    # Wire the progress publish failure counter into the test-scoped meter so
    # counter_value(reader, "taskq.progress.publish_failures") is observable.
    monkeypatch.setattr(
        otel_mod,
        "_progress_publish_failures",
        otel_mod.get_meter().create_counter("taskq.progress.publish_failures"),
    )

    # Use a dead Redis URL
    dead_url = "redis://127.0.0.1:19999/0"
    stack, deps, backend = await _setup_worker(
        pg_dsn, redis_url=dead_url, schema=module_pg_schema.schema_name
    )
    try:
        await _run_job(deps, backend, _progress_redis_fail_actor)

        job_id = await _get_job_id_from_pool(
            deps.worker_pool, deps.settings.schema_name, "_progress_pg_redis_fail"
        )
        row = await _query_progress_row(deps.worker_pool, deps.settings.schema_name, job_id)

        assert row["status"] == "succeeded", f"expected succeeded, got {row['status']}"
        assert row["progress_seq"] >= 1

        failures = counter_value(reader, "taskq.progress.publish_failures")
        assert failures >= 1, f"expected >= 1 publish_failures, got {failures}"
    finally:
        await stack.aclose()


# ── Redis disconnect mid-stream (simulated) ─────────────────────────


@pytest.mark.redis
async def test_ti6_redis_disconnect_mid_stream_pg_complete(
    pg_dsn: str, redis_url: str, monkeypatch: pytest.MonkeyPatch, module_pg_schema: ModulePgSchema
) -> None:
    """Redis publish raises after the 3rd call; PG has all 5 updates; job succeeds.

    Rather than stopping the session-scoped Redis container, we monkeypatch
    the publish method on the redis client to raise after N calls.

    Oracle: progress_seq == 5 in PG, job status = 'succeeded'.
    """
    setup_meter(monkeypatch)
    monkeypatch.setattr(
        otel_mod,
        "_progress_publish_failures",
        otel_mod.get_meter().create_counter("taskq.progress.publish_failures"),
    )

    stack, deps, backend = await _setup_worker(
        pg_dsn, redis_url=redis_url, schema=module_pg_schema.schema_name
    )
    try:
        # Inject a publish error after the 3rd call
        if deps.redis_client is not None:
            original_publish = deps.redis_client.publish
            call_count: list[int] = [0]

            async def _flaky_publish(channel: str, message: bytes | str) -> int:
                call_count[0] += 1
                if call_count[0] > 3:
                    raise ConnectionError("simulated Redis disconnect")
                return await original_publish(channel, message)

            monkeypatch.setattr(deps.redis_client, "publish", _flaky_publish)

        await _run_job(deps, backend, _progress_multi_actor)

        job_id = await _get_job_id_from_pool(
            deps.worker_pool, deps.settings.schema_name, "_progress_pg_multi"
        )
        row = await _query_progress_row(deps.worker_pool, deps.settings.schema_name, job_id)

        assert row["status"] == "succeeded", f"expected succeeded, got {row['status']}"
        # All 5 ctx.progress() calls must have landed in PG (pre-terminal flush)
        assert row["progress_seq"] == 5, f"expected progress_seq==5, got {row['progress_seq']}"
    finally:
        await stack.aclose()


# ── Crash mid-progress ────────────────────────────────────────────


async def test_ti7_crash_mid_progress_flush_fires(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> None:
    """Actor calls ctx.progress(step=1) then raises RuntimeError.

    The crash-flush (finally block) must write progress_state before
    mark_failed runs.

    Oracle: progress_seq >= 1, job status = 'failed'.
    """
    stack, deps, backend = await _setup_worker(pg_dsn, schema=module_pg_schema.schema_name)
    try:
        await _run_job(deps, backend, _progress_crash_actor)

        job_id = await _get_job_id_from_pool(
            deps.worker_pool, deps.settings.schema_name, "_progress_pg_crash"
        )
        row = await _query_progress_row(deps.worker_pool, deps.settings.schema_name, job_id)

        assert row["status"] == "failed", f"expected failed, got {row['status']}"
        assert row["progress_seq"] >= 1, (
            f"crash-flush should have written seq >= 1, got {row['progress_seq']}"
        )
    finally:
        await stack.aclose()


# ── PG unavailable during periodic flush ─────────────────────────


async def test_tc2_pg_unavailable_during_flush_recovers(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch, module_pg_schema: ModulePgSchema
) -> None:
    """_flush_buffer raises PostgresError on the first flush tick, then succeeds.

    The flush loop logs ERROR but does not crash. On the next tick the buffer
    is flushed successfully.

    Oracle: flush eventually succeeds (buffer.dirty=False after recovery).
    """
    import taskq.progress._flush as flush_mod

    stack, deps, _backend = await _setup_worker(pg_dsn, schema=module_pg_schema.schema_name)
    try:
        from taskq.progress._buffer import _ProgressBuffer
        from taskq.testing.pg import create_running_job, create_worker

        wid = new_uuid()
        schema = deps.settings.schema_name

        async with deps.worker_pool.acquire() as conn:
            await create_worker(conn, schema, wid)
            job_id = await create_running_job(conn, schema, wid)

        buf = _ProgressBuffer(job_id=job_id, base_seq=0)
        buf.pending_seq_delta = 2
        buf.pending_state["step"] = 7
        buf.dirty = True
        deps.progress_buffers[job_id] = buf

        # Wrap the real _flush_buffer to inject a one-shot PG error
        original_flush_buffer = flush_mod._flush_buffer  # type: ignore[attr-defined] # Why: accessing private flush function for test-controlled injection.
        call_count: list[int] = [0]

        async def _flaky_flush_buffer(*args: Any, **kwargs: Any) -> None:
            call_count[0] += 1
            if call_count[0] == 1:
                raise asyncpg.PostgresError("simulated PG overload")
            await original_flush_buffer(*args, **kwargs)

        monkeypatch.setattr(flush_mod, "_flush_buffer", _flaky_flush_buffer)

        shutdown = asyncio.Event()

        async def _stop_when_clean() -> None:
            for _ in range(100):
                if not buf.dirty:
                    break
                await asyncio.sleep(0.02)
            shutdown.set()

        await asyncio.gather(
            progress_flush_loop(
                lambda: deps.worker_pool, schema, wid, deps.progress_buffers, 0.05, shutdown
            ),
            _stop_when_clean(),
        )

        # First call raised; second call succeeded; buffer is now clean
        assert call_count[0] >= 2, "expected at least one failure + one success"
        assert not buf.dirty, "buffer should be clean after recovery flush"
    finally:
        await stack.aclose()
