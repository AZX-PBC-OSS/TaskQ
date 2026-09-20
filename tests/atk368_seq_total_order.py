"""ATK-368: executable attacks on the seq total-order contract (PR 368).

Every assertion here is an OBSERVED BEHAVIOR: the event stream a consumer
receives (order, duplicates, terminal-last), the catch-up cursor a
reconnecting consumer is handed (never rewinds, never skips a delivered
event's position), and terminal row states. Internals (buffers, deltas,
row columns) are read only to ARRANGE scenarios or to stand in for the
resume cursor the SSE endpoint hands out; no assertion keys on them.

Surfaces:

1. Flush/consume interleaving hammer (fast, fake merge pool): progress
   deliveries race both flush surfaces mid-flight; the delivered stream
   never repeats a seq, never rewinds, the catch-up cursor never rewinds,
   and the terminal event lands strictly after everything delivered.

2. PG hammer (integration): real Postgres, real flush loop at the smallest
   coalesce interval, jobs with random progress cadences, retries and a
   snooze, zero-progress jobs; a reconnecting consumer samples the catch-up
   cursor continuously. Asserts the delivered stream per job is duplicate-
   free and strictly ordered with the terminal event last; the cursor never
   rewinds; a zero-progress job's cursor stays UNCHANGED while it runs (the
   documented zero-progress window: a consumer that saw `running` and
   reconnects finds the snapshot it had before).

3. Worker-death duplicate window (integration): after a worker dies, the
   next attempt re-delivers seqs the stream already carried when the dead
   worker's writes never landed (the documented window), and delivers none
   when they did land. Verifies the doc's scoping in BOTH directions, as
   stream behavior.
"""

import asyncio
import json
import random
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
from taskq.context import JobContext
from taskq.exceptions import Snooze
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._events import ProgressEvent
from taskq.progress._flush import (
    _flush_buffer,
    _flush_buffer_immediate,
    _flush_dirty_set,
)
from taskq.progress._publish import _publish_state_change_event
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker._consumer import consume_one_job
from taskq.worker.deps import WorkerDeps, open_worker_deps

pytestmark = pytest.mark.integration

# ruff: noqa: S311  Why: the hammer needs reproducible randomness, not crypto.
# ruff: noqa: S608  Why: schema names come from the validated WorkerSettings/module fixture; asyncpg has no parameter binding for identifiers.


# ── shared helpers ──────────────────────────────────────────────────────────


class _RecordingRedis:
    """Records every published payload in delivery order."""

    def __init__(self) -> None:
        self.published: list[dict[str, object]] = []

    async def publish(self, channel: str, payload: str) -> int:
        self.published.append(json.loads(payload))
        return 1


def _wire(events: list[dict[str, object]], job_id: UUID) -> list[dict[str, object]]:
    return [e for e in events if e.get("job_id") == str(job_id)]


def _assert_stream_well_ordered(events: list[dict[str, object]], label: str) -> None:
    """The behavior every consumer of the stream relies on: delivered seqs
    never repeat (no event is mistakable for a duplicate), never rewind in
    delivery order, and the terminal state_change lands last with a seq
    strictly past everything before it."""
    seqs = [int(e["seq"]) for e in events]
    assert len(seqs) == len(set(seqs)), f"{label}: duplicate seq delivered: {seqs}"
    assert seqs == sorted(seqs), f"{label}: delivered stream rewound: {seqs}"
    last = events[-1]
    assert last["kind"] == "state_change" and last.get("terminal") is True, (
        f"{label}: the stream does not end with the terminal event: "
        f"{[(e['kind'], e.get('status')) for e in events]}"
    )


# ── attack 1: flush/consume interleaving hammer (fast) ─────────────────────


class _YieldMergeConn:
    """fetchrow applies the real ``row + delta`` merge, yielding first so
    progress calls land while the flush statement is 'suspended'."""

    def __init__(self, row_seq: list[int], rng: random.Random) -> None:
        self._row_seq = row_seq
        self._rng = rng

    async def fetchrow(self, _sql: str, *args: object) -> dict[str, object]:
        await asyncio.sleep(0)
        if self._rng.random() < 0.5:
            await asyncio.sleep(0)
        deltas = args[1]
        assert isinstance(deltas, list)
        self._row_seq[0] += sum(deltas)
        return {"id": args[0], "progress_seq": self._row_seq[0]}

    async def fetch(self, _sql: str, *args: object) -> list[dict[str, object]]:
        await asyncio.sleep(0)
        if self._rng.random() < 0.5:
            await asyncio.sleep(0)
        job_ids = args[0]
        deltas = args[1]
        assert isinstance(job_ids, list) and isinstance(deltas, list)
        rows = []
        for job_id, delta in zip(job_ids, deltas, strict=True):
            self._row_seq[0] += delta
            rows.append({"id": job_id, "progress_seq": self._row_seq[0]})
        return rows


class _AcquiredConn:
    def __init__(self, conn: _YieldMergeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _YieldMergeConn:
        return self._conn

    async def __aexit__(self, *_exc: object) -> None:
        return None


class _YieldPool:
    def __init__(self, row_seq: list[int], rng: random.Random) -> None:
        self._row_seq = row_seq
        self._rng = rng

    def acquire(self) -> _AcquiredConn:
        return _AcquiredConn(_YieldMergeConn(self._row_seq, self._rng))


_JOB = UUID("00000000-0000-0000-0000-00000000abcd")
_WORKER = new_uuid()


def _settings(schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )


@pytest.mark.asyncio
async def test_atk1_flush_consume_interleave_hammer() -> None:
    """Looped interleavings of progress deliveries, tick flushes and
    immediate flushes, with progress landing mid-flush: the stream a
    consumer sees never repeats a seq and never rewinds; the catch-up
    cursor a reconnecting consumer would be handed never rewinds and ends
    exactly at the terminal event's delivered seq."""
    rng = random.Random(368)
    rounds = 120
    schema = "taskq_atk1"  # the fake pool never parses SQL; any name dozes here

    async def progress_calls(buffer: _ProgressBuffer, wire: list[int], n: int) -> None:
        for _ in range(n):
            # The exact stream effect of a ctx.progress call: the next seq
            # is delivered on the wire.
            buffer.pending_seq_delta += 1
            buffer.dirty = True
            wire.append(buffer.base_seq + buffer.pending_seq_delta)
            await asyncio.sleep(0)

    async def flusher(pool: _YieldPool, buffers: dict[UUID, _ProgressBuffer]) -> None:
        for _ in range(rng.randint(2, 6)):
            if rng.random() < 0.5:
                dirty = [(jid, buf) for jid, buf in buffers.items() if buf.dirty]
                await _flush_dirty_set(pool, schema, _WORKER, buffers, dirty)
            else:
                await _flush_buffer_immediate(pool, schema, _JOB, _WORKER, buffers)
            await asyncio.sleep(0)

    for round_no in range(rounds):
        cursor = [0]  # stands in for the resume cursor a reconnector gets
        pool = _YieldPool(cursor, rng)
        buffer = _ProgressBuffer(job_id=_JOB, base_seq=0, attempt=1)
        buffers: dict[UUID, _ProgressBuffer] = {_JOB: buffer}
        wire: list[int] = []
        cursor_samples: list[int] = []
        n_calls = rng.randint(5, 40)

        async def cursor_sampler(cur: list[int], samples: list[int], n: int) -> None:
            # A reconnecting consumer polling the catch-up snapshot.
            for _ in range(n):
                samples.append(cur[0])
                await asyncio.sleep(0)

        await asyncio.gather(
            progress_calls(buffer, wire, n_calls),
            flusher(pool, buffers),
            cursor_sampler(cursor, cursor_samples, n_calls + 10),
            progress_calls(buffer, wire, n_calls // 2),
        )

        # The catch-up cursor a reconnecting consumer sees never rewinds:
        # resuming never hands back an older position.
        assert cursor_samples == sorted(cursor_samples), (
            f"round {round_no}: resume cursor rewound: {cursor_samples}"
        )
        # The cursor never runs AHEAD of what the stream delivered: a
        # consumer resuming at the cursor cannot already have seen every
        # event and then find the snapshot past the newest delivery.
        head = buffer.base_seq + buffer.pending_seq_delta
        assert cursor[0] <= head, (
            f"round {round_no}: cursor skipped past the newest delivery: "
            f"cursor={cursor[0]} newest={head}"
        )

        # Drain, then deliver the terminal event exactly as the consumer's
        # terminal path does, and assert the WHOLE delivered stream.
        await _flush_buffer_immediate(pool, schema, _JOB, _WORKER, buffers)
        redis_client = _RecordingRedis()
        from taskq.progress._buffer import _terminal_seq_and_state

        seq, state = _terminal_seq_and_state(buffer)
        await _publish_state_change_event(
            redis_client,
            _settings(schema),
            _JOB,
            "hammer_actor",
            buffers,
            status="succeeded",
            terminal=True,
            _override_seq=seq,
            _override_pending_state=state,
        )
        delivered = [
            {"seq": s, "kind": "progress", "terminal": False} for s in wire
        ] + redis_client.published
        # The terminal mark write's absolute SET lands the consumed seq
        # durably (stand-in for the mark_* write the terminal path issues).
        cursor[0] = seq
        _assert_stream_well_ordered(delivered, f"round {round_no}")
        # A consumer resuming after the terminal is handed a cursor exactly
        # at the terminal event's seq: nothing delivered is ahead of the
        # cursor, nothing behind it is missing a durable write.
        assert cursor[0] == int(redis_client.published[0]["seq"]), (
            f"round {round_no}: resume cursor {cursor[0]} != terminal delivered "
            f"seq {redis_client.published[0]['seq']}"
        )


# ── attack 2: PG hammer with forced mid-stream flushes and reconnects ──────


async def _setup_worker(
    pg_dsn: str,
    *,
    schema: str,
) -> tuple[AsyncExitStack, WorkerDeps, PostgresBackend]:
    from taskq.migrate import apply_pending

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_PROGRESS_COALESCE_INTERVAL": "0.1",
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            "TASKQ_LOCK_LEASE": "30.0",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.5",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.5",
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "1.2",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "0.5",
        }
    )

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    backend = PostgresBackend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=0.5),
        cleanup_grace_period=timedelta(seconds=0.5),
    )
    return stack, deps, backend


class _Empty(BaseModel):
    pass


@actor(name="_atk368_chatty")
async def _atk368_chatty(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    rng = random.Random(ctx.job_id.int % 2**31)
    for i in range(rng.randint(5, 15)):
        await asyncio.sleep(rng.randint(3, 15) / 1000)
        await ctx.progress(step=i, percent=float(i), detail=f"d{i}")


@actor(name="_atk368_flaky")
async def _atk368_flaky(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    await ctx.progress(step=1, percent=25.0)
    await asyncio.sleep(0.01)
    await ctx.progress(step=2, percent=50.0)
    if ctx.attempt < 3:
        raise RuntimeError(f"transient boom {ctx.attempt}")
    await ctx.progress(step=3, percent=100.0)


@actor(name="_atk368_silent")
async def _atk368_silent(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    await asyncio.sleep(0.4)


@actor(name="_atk368_snoozer")
async def _atk368_snoozer(payload: _Empty, ctx: JobContext[_Empty]) -> None:
    await ctx.progress(step=1, percent=50.0)
    # Snooze refunds the attempt increment, so key off snooze_count.
    if ctx.snooze_count < 1:
        raise Snooze(timedelta(seconds=0.05))
    await ctx.progress(step=2, percent=100.0)


def _cfg(name: str) -> Any:
    from taskq.testing.actor import StubActorConfig

    if name in ("_atk368_flaky", "_atk368_snoozer"):
        return StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))
    return StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=1, jitter=0.0))


async def _seed_rows(deps: WorkerDeps, worker_id: UUID, actor_names: list[str]) -> None:
    schema = deps.settings.schema_name
    async with deps.dispatcher_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
            "VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
            worker_id,
            "atk-host",
            4242,
            ["default"],
        )
        for name in actor_names:
            await conn.execute(
                f'INSERT INTO "{schema}".actor_config (actor, queue) '
                "VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
                name,
                "default",
            )


async def _enqueue(
    backend: PostgresBackend,
    actor_name: str,
    *,
    max_attempts: int = 1,
) -> UUID:
    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor=actor_name,
            queue="default",
            payload={},
            payload_schema_ver=1,
            priority=0,
            max_attempts=max_attempts,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        )
    )
    return job_id


async def _resume_cursor(pool: asyncpg.Pool, schema: str, job_id: UUID) -> int:
    """The catch-up cursor a reconnecting consumer is handed (the row's
    durable snapshot position)."""
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT progress_seq FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
    assert row is not None
    return int(row["progress_seq"])


@pytest.mark.asyncio
async def test_atk2_pg_hammer_total_order_under_forced_flushes(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> None:
    """Many jobs, random progress cadences, retries, a snooze, zero-progress
    jobs, a flush loop at the smallest coalesce interval, and a reconnecting
    consumer sampling the catch-up cursor every few ms.

    Asserted: each job's delivered stream is duplicate-free and strictly
    ordered with the terminal event last; the catch-up cursor never rewinds
    and ends exactly at the terminal event's delivered seq; a zero-progress
    job's cursor stays UNCHANGED for its whole running phase (a consumer
    that saw `running` and reconnects finds the snapshot it already had —
    the documented zero-progress window)."""
    schema = module_pg_schema.schema_name
    stack, deps, backend = await _setup_worker(pg_dsn, schema=schema)
    async with stack:
        worker_id = new_uuid()
        names = [
            "_atk368_chatty",
            "_atk368_flaky",
            "_atk368_silent",
            "_atk368_snoozer",
        ]
        await _seed_rows(deps, worker_id, names)

        redis_client = _RecordingRedis()
        # Inline publishes: delivery order is publication order, the same
        # determinism the existing pins rely on.
        deps.redis_client = redis_client  # type: ignore[assignment]
        deps.pending_publish_tasks = None  # type: ignore[assignment]

        shutdown = asyncio.Event()
        flush_loop = asyncio.create_task(_flush_loop_shim(deps, worker_id, shutdown))

        actor_refs: dict[str, Any] = {
            "_atk368_chatty": _atk368_chatty,
            "_atk368_flaky": _atk368_flaky,
            "_atk368_silent": _atk368_silent,
            "_atk368_snoozer": _atk368_snoozer,
        }

        # Reconnecting consumer: samples each job's catch-up cursor
        # continuously.
        stop_sampling = asyncio.Event()
        samples: dict[UUID, list[int]] = {}
        job_ids: dict[str, UUID] = {}

        async def sampler() -> None:
            while not stop_sampling.is_set():
                for jid in list(samples):
                    samples[jid].append(await _resume_cursor(deps.dispatcher_pool, schema, jid))
                await asyncio.sleep(0.005)

        sampler_task = asyncio.create_task(sampler())

        enqueuer = SubJobEnqueuer(
            loop_scope_resolved=None, worker_pool=deps.worker_pool, backend=backend
        )

        async def run_job(actor_name: str) -> UUID:
            """Enqueue just this job, then drive it to a terminal state,
            re-claiming across retries/snoozes."""
            attempts = 3 if actor_name in ("_atk368_flaky", "_atk368_snoozer") else 1
            job_id = await _enqueue(backend, actor_name, max_attempts=attempts)
            job_ids[actor_name] = job_id
            samples[job_id] = []

            ref = actor_refs[actor_name]
            cfg = _cfg(actor_name)

            async def _run(jr: Any, ctx: JobContext[BaseModel]) -> object:
                return await ref.fn(payload=ctx.payload, ctx=ctx)

            for _ in range(6):
                rows = await backend.dispatch_batch(
                    worker_id,
                    ["default"],
                    limit=1,
                    lock_lease=timedelta(seconds=30),
                )
                assert rows and rows[0].id == job_id, (
                    f"{actor_name}: dispatch admitted the wrong job"
                )
                job = rows[0]
                await consume_one_job(
                    backend,
                    job,
                    worker_id,
                    deps=deps,
                    run_actor=_run,
                    actor_config=cfg,
                    payload_type=_Empty,
                    clock=SystemClock(),
                    enqueuer=enqueuer,
                )
                row = await backend.get(job_id)
                assert row is not None
                if row.status in ("succeeded", "failed", "cancelled"):
                    return job_id
                # scheduled again (retry/snooze): pull its scheduled_at into
                # the past, then re-pend, so the next dispatch admits it.
                async with deps.dispatcher_pool.acquire() as conn:
                    await conn.execute(
                        f'UPDATE "{schema}".jobs SET scheduled_at = '
                        f"statement_timestamp() - interval '1 seconds' WHERE id = $1",
                        job_id,
                    )
                await backend.scheduled_to_pending()

            row = await backend.get(job_id)
            assert row is not None
            assert row.status in ("succeeded", "failed"), (
                f"{actor_name} never reached a terminal state: {row.status}"
            )
            return job_id

        for name in names:
            await run_job(name)
        stop_sampling.set()
        await sampler_task
        shutdown.set()
        await flush_loop

        for name, jid in job_ids.items():
            events = _wire(redis_client.published, jid)
            # Retry arms publish terminal=True per ATTEMPT (the documented
            # vocabulary); the job's terminal event is the LAST delivered
            # event. _assert_stream_well_ordered checks duplicates, rewind
            # and terminal-last on the delivered stream.
            _assert_stream_well_ordered(events, name)

            # The catch-up cursor never rewinds, and a consumer resuming
            # after the terminal is handed exactly the terminal event's seq:
            # nothing delivered sits ahead of the cursor.
            assert samples[jid] == sorted(samples[jid]), (
                f"{name}: resume cursor rewound: {samples[jid]}"
            )
            cursor = await _resume_cursor(deps.dispatcher_pool, schema, jid)
            terminal_seq = int(events[-1]["seq"])  # type: ignore[reportUnknownArgumentType]
            assert cursor == terminal_seq, (
                f"{name}: resume cursor {cursor} != terminal delivered seq {terminal_seq}"
            )

        # Zero-progress window, at documented strength: the silent job's
        # stream delivers `running` at seq 1, but its catch-up cursor NEVER
        # reads 1 mid-run — a consumer that saw `running` and reconnects
        # before the terminal finds the snapshot it already had (0), then
        # jumps straight to the terminal position.
        silent_id = job_ids["_atk368_silent"]
        silent_events = _wire(redis_client.published, silent_id)
        running_events = [
            e
            for e in silent_events
            if e.get("kind") == "state_change" and e.get("status") == "running"
        ]
        assert len(running_events) == 1 and int(running_events[0]["seq"]) == 1
        cursor_values = set(samples[silent_id])
        assert 1 not in cursor_values, (
            f"the running event's seq became a resume cursor mid-run: {samples[silent_id]}"
        )
        assert cursor_values == {0, 2}, (
            f"silent job's resume cursor did anything but hold then jump to "
            f"the terminal: {samples[silent_id]}"
        )


async def _flush_loop_shim(deps: WorkerDeps, worker_id: UUID, shutdown: asyncio.Event) -> None:
    from taskq.progress._flush import progress_flush_loop

    await progress_flush_loop(
        lambda: deps.dispatcher_pool,
        deps.settings.schema_name,
        worker_id,
        deps.progress_buffers,
        0.1,
        shutdown,
    )


# ── attack 3: the worker-death duplicate window, both directions ───────────


async def _death_setup(
    pg_dsn: str, schema: str
) -> tuple[AsyncExitStack, WorkerDeps, PostgresBackend, _RecordingRedis]:
    stack, deps, backend = await _setup_worker(pg_dsn, schema=schema)
    redis_client = _RecordingRedis()
    deps.redis_client = redis_client  # type: ignore[assignment]
    deps.pending_publish_tasks = None  # type: ignore[assignment]
    return stack, deps, backend, redis_client


async def _publish_progress_wire(
    redis_client: _RecordingRedis, schema: str, job_id: UUID, seq: int
) -> None:
    """Put one progress event on the wire exactly as the real publish does."""
    event = ProgressEvent(
        kind="progress",
        job_id=job_id,
        actor="_atk368_doomed",
        ts=datetime.now(UTC),
        seq=seq,
        status="running",
        step=seq,
    )
    from taskq.constants import progress_channel

    await redis_client.publish(
        progress_channel(schema, job_id), event.model_dump_json(exclude_none=True)
    )


async def _expire_lease_and_reclaim(
    deps: WorkerDeps, backend: PostgresBackend, schema: str, job_id: UUID
) -> None:
    """Kill the holder: expire its lease, then run the reclaim sweep."""
    async with deps.dispatcher_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET lock_expires_at = '
            f"statement_timestamp() - interval '1 seconds' WHERE id = $1",
            job_id,
        )
    reclaimed = await backend.reclaim_expired_locks(timedelta(seconds=0), timedelta(seconds=0))
    assert reclaimed == 1, "the dead holder's row was not reclaimed"


async def _redispatch(
    backend: PostgresBackend, schema: str, deps: WorkerDeps, job_id: UUID, worker_id: UUID
) -> Any:
    async with deps.dispatcher_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
            "VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
            worker_id,
            "atk-host-b",
            4243,
            ["default"],
        )
        await conn.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = '
            f"statement_timestamp() - interval '1 seconds' WHERE id = $1",
            job_id,
        )
    await backend.scheduled_to_pending()  # no-op when the sweep re-pended directly
    rows = await backend.dispatch_batch(
        worker_id, ["default"], limit=1, lock_lease=timedelta(seconds=30)
    )
    assert rows and rows[0].id == job_id
    return rows[0]


async def _consume_success(
    backend: PostgresBackend, deps: WorkerDeps, job: Any, worker_id: UUID
) -> None:
    from taskq.testing.actor import default_actor_config

    async def _run(jr: Any, ctx: JobContext[BaseModel]) -> object:
        await ctx.progress(step=10, percent=99.0)
        return None

    await consume_one_job(
        backend,
        job,
        worker_id,
        deps=deps,
        run_actor=_run,
        actor_config=default_actor_config(),
        payload_type=_Empty,
        clock=SystemClock(),
    )


@pytest.mark.asyncio
async def test_atk3a_worker_death_unflushed_delta_republishes_carried_seqs(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> None:
    """UNFLUSHED death: the dead worker delivered progress seqs 1..3 on the
    stream but its writes never landed, so the resume cursor still reads 0.
    The reclaimed job's next attempt then RE-DELIVERS seq 1 (as `running`,
    a different payload) — the documented worker-death window, observed on
    the stream. Within EACH attempt's segment the delivery stays strictly
    ordered and duplicate-free, and the post-terminal resume cursor equals
    the terminal event's delivered seq."""
    schema = module_pg_schema.schema_name
    stack, deps, backend, redis_client = await _death_setup(pg_dsn, schema=schema)
    async with stack:
        worker_a = new_uuid()
        worker_b = new_uuid()
        await _seed_rows(deps, worker_a, ["_atk368_chatty"])
        await _seed_rows(deps, worker_b, ["_atk368_chatty"])

        job_id = await _enqueue(backend, "_atk368_chatty", max_attempts=3)
        rows = await backend.dispatch_batch(
            worker_a, ["default"], limit=1, lock_lease=timedelta(seconds=30)
        )
        assert rows and rows[0].id == job_id
        job = rows[0]
        assert job.attempt == 1

        # Worker A's in-memory stream: the running transition consumes 1,
        # two progress events at 2 and 3; the buffer dies BEFORE any write
        # lands (arrangement: the cursor must still read 0).
        buffers_a: dict[UUID, _ProgressBuffer] = {
            job_id: _ProgressBuffer(job_id=job_id, base_seq=job.progress_seq, attempt=job.attempt)
        }
        from taskq.progress._buffer import _consume_state_change_seq

        _consume_state_change_seq(buffers_a[job_id])  # the running transition
        await _publish_progress_wire(redis_client, schema, job_id, 1)
        for _seq in (2, 3):
            buffers_a[job_id].pending_seq_delta += 1
            buffers_a[job_id].dirty = True
            await _publish_progress_wire(redis_client, schema, job_id, _seq)
        assert await _resume_cursor(deps.dispatcher_pool, schema, job_id) == 0, (
            "arrangement: the dead worker's writes must not have landed"
        )

        await _expire_lease_and_reclaim(deps, backend, schema, job_id)
        row = await backend.get(job_id)
        assert row is not None and row.status == "pending"

        job_b = await _redispatch(backend, schema, deps, job_id, worker_b)
        assert job_b.attempt == 2
        await _consume_success(backend, deps, job_b, worker_b)

        events = _wire(redis_client.published, job_id)
        seqs = [int(e["seq"]) for e in events]
        # THE DOCUMENTED WINDOW, observed: worker B's `running` re-delivers
        # seq 1, which worker A's stream already carried with a different
        # payload.
        assert seqs[:4] == [1, 2, 3, 1], f"expected the scoped replay, got {seqs}"
        assert events[3]["kind"] == "state_change" and events[3]["status"] == "running"
        assert events[0]["kind"] == "progress"
        # Within EACH attempt's segment the delivery is strictly ordered and
        # duplicate-free (the worker-death window is the ONLY scoping crack).
        a_events, b_events = events[:3], events[3:]
        a_seqs = [int(e["seq"]) for e in a_events]
        assert a_seqs == sorted(a_seqs) and len(a_seqs) == len(set(a_seqs)), (
            f"worker A's segment itself broke order: {a_seqs}"
        )
        _assert_stream_well_ordered(b_events, "worker B segment")
        # Terminal row state: succeeded, and a consumer resuming after the
        # terminal is handed the terminal event's delivered seq.
        row = await backend.get(job_id)
        assert row is not None and row.status == "succeeded"
        cursor = await _resume_cursor(deps.dispatcher_pool, schema, job_id)
        assert cursor == seqs[-1], f"resume cursor {cursor} != terminal delivered seq {seqs[-1]}"


@pytest.mark.asyncio
async def test_atk3b_worker_death_flushed_delta_republishes_nothing_carried(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> None:
    """FLUSHED death: the dead worker's writes DID land (the resume cursor
    reads 3, the position its stream already delivered). The reclaimed
    job's next attempt re-delivers NOTHING already carried: the whole
    stream across the death is strictly ordered and duplicate-free — 'deduping
    by seq is safe for events whose durable write landed', at its true
    strength."""
    schema = module_pg_schema.schema_name
    stack, deps, backend, redis_client = await _death_setup(pg_dsn, schema=schema)
    async with stack:
        worker_a = new_uuid()
        worker_b = new_uuid()
        await _seed_rows(deps, worker_a, ["_atk368_chatty"])
        await _seed_rows(deps, worker_b, ["_atk368_chatty"])

        job_id = await _enqueue(backend, "_atk368_chatty", max_attempts=3)
        rows = await backend.dispatch_batch(
            worker_a, ["default"], limit=1, lock_lease=timedelta(seconds=30)
        )
        assert rows and rows[0].id == job_id
        job = rows[0]

        buffers_a: dict[UUID, _ProgressBuffer] = {
            job_id: _ProgressBuffer(job_id=job_id, base_seq=job.progress_seq, attempt=job.attempt)
        }
        from taskq.progress._buffer import _consume_state_change_seq

        _consume_state_change_seq(buffers_a[job_id])
        await _publish_progress_wire(redis_client, schema, job_id, 1)
        for _seq in (2, 3):
            buffers_a[job_id].pending_seq_delta += 1
            buffers_a[job_id].dirty = True
            await _publish_progress_wire(redis_client, schema, job_id, _seq)

        # This time the write LANDS before death: the cursor adopts 3.
        await _flush_buffer(
            deps.dispatcher_pool,
            schema,
            job_id,
            worker_a,
            buffers_a[job_id],
            buffers_a,
        )
        assert await _resume_cursor(deps.dispatcher_pool, schema, job_id) == 3, (
            "arrangement: the dead worker's writes must have landed"
        )

        await _expire_lease_and_reclaim(deps, backend, schema, job_id)
        job_b = await _redispatch(backend, schema, deps, job_id, worker_b)
        assert job_b.attempt == 2
        await _consume_success(backend, deps, job_b, worker_b)

        events = _wire(redis_client.published, job_id)
        # No seq whose durable write landed was ever re-delivered: the whole
        # stream across the death behaves as one strict order.
        _assert_stream_well_ordered(events, "cross-death stream")
        seqs = [int(e["seq"]) for e in events]
        assert seqs == [1, 2, 3, 4, 5, 6], f"landed-write replay leaked: {seqs}"
        assert events[3]["status"] == "running" and events[5]["terminal"] is True
        # Terminal row state + the post-terminal resume cursor.
        row = await backend.get(job_id)
        assert row is not None and row.status == "succeeded"
        cursor = await _resume_cursor(deps.dispatcher_pool, schema, job_id)
        assert cursor == seqs[-1]
