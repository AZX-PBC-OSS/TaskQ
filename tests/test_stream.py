"""Unit, negative, and integration tests for TaskQ.stream().

Unit and negative tests run with ``pytest -m "not integration"``
- no Docker, no testcontainers. They use :class:`InMemoryBackend` and
class:`~taskq.testing.clock.FakeClock` from
mod:`taskq.testing.fixtures` - the in-memory backend IS the unit-test
substitute for the real one.

Integration tests require Docker / testcontainers and are
individually decorated with ``@pytest.mark.integration`` so they are
skipped by ``pytest -m "not integration"``.

Covers:
- stream() on an already-terminal job yields one event and returns.
- stream() on a non-existent job_id raises KeyError.
- _row_to_event maps terminal/non-terminal statuses correctly.
- redis_url and redis_client are mutually exclusive.
- TaskQ without redis is importable without [redis] extra.
- stream() outside open() raises RuntimeError.
- stream() on a job_id that never exists raises KeyError.
- PG transport - stream terminates on job completion.
- PG transport - all status transitions appear in stream events.
- PG transport - no dedicated connection is held per stream.
- PG transport - a terminal write is observed within a second by default.
- PG transport - poll-timeout path yields terminal event.
- Redis transport - stream terminates on job completion.
- Redis transport - progress events and monotonic progress_seq.
- Redis transport - malformed message is skipped, stream continues.
"""

import asyncio
import dataclasses
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId, JobRow, JobStatus
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.client._jobs import JobsClient
from taskq.client._taskq import JobEvent, TaskQ, _row_to_event
from taskq.settings import TaskQSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row

_UNIT_SCHEMA_LABEL = "taskq_test"
_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


def _make_client(backend: InMemoryBackend) -> JobsClient:
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _UNIT_SCHEMA_LABEL})
    return JobsClient(backend, settings=settings)


def _inject_tq(
    backend: InMemoryBackend, *, dsn: str | None = "postgresql://localhost/test"
) -> TaskQ:
    """Construct a TaskQ with the in-memory backend injected for unit testing.

    TaskQ.open() hardcodes PostgresBackend, so we bypass construction
    and inject the client directly - the same pattern used in
    test_taskq_stream.py.
    """
    tq = TaskQ.__new__(TaskQ)
    tq._dsn = dsn
    tq._pool = None
    tq._schema = _UNIT_SCHEMA_LABEL
    tq._min_pool_size = 1
    tq._max_pool_size = 5
    tq._redis_url = None
    tq._redis_client = None
    tq._poll_timeout = 30.0
    tq._owns_pool = True
    tq._client = _make_client(backend)
    return tq


def _row(
    *,
    status: JobStatus = "running",
    progress_seq: int = 0,
    progress_state: dict[str, object] | None = None,
) -> JobRow:
    row = make_job_row(status=status, progress_seq=progress_seq)
    return dataclasses.replace(
        row,
        progress_state=progress_state if progress_state is not None else row.progress_state,
    )


# ── _row_to_event maps terminal/non-terminal statuses correctly ──


@pytest.mark.parametrize("status", sorted(TERMINAL_STATUSES))
def test_row_to_event_terminal(status: JobStatus) -> None:
    """_row_to_event maps each terminal status to terminal=True."""
    row = _row(status=status)
    event = _row_to_event(row)
    assert event.terminal is True
    assert event.status == status


@pytest.mark.parametrize("status", ["pending", "scheduled", "running"])
def test_row_to_event_non_terminal(status: JobStatus) -> None:
    """_row_to_event maps each non-terminal status to terminal=False."""
    row = _row(status=status)
    event = _row_to_event(row)
    assert event.terminal is False
    assert event.status == status


def test_row_to_event_preserves_fields() -> None:
    """_row_to_event carries all relevant fields from the row."""
    row = _row(status="running", progress_seq=5, progress_state={"step": 1, "percent": 50})
    event = _row_to_event(row)
    assert event.job_id == row.id
    assert event.status == row.status
    assert event.progress_state == row.progress_state
    assert event.progress_seq == row.progress_seq
    assert event.terminal is False


# ── already-terminal job yields one event and returns ─────────────


@pytest.mark.parametrize("status", sorted(TERMINAL_STATUSES))
async def test_stream_terminal_job_yields_one_event(status: JobStatus) -> None:
    """stream() on a job already in a terminal status yields exactly
    one JobEvent with terminal=True and returns.
    """
    backend = _make_backend()
    row = await backend.enqueue(_enqueue_args())
    terminal_row = dataclasses.replace(row, status=status)
    backend._jobs[terminal_row.id] = terminal_row

    tq = _inject_tq(backend)
    events: list[JobEvent] = []
    async for event in tq.stream(terminal_row.id):
        events.append(event)

    assert len(events) == 1
    assert events[0].terminal is True
    assert events[0].status == status


# ── non-existent job_id raises KeyError ───────────────────────────


async def test_stream_nonexistent_job_raises_key_error() -> None:
    """stream() on a non-existent job_id raises KeyError."""
    backend = _make_backend()
    tq = _inject_tq(backend)

    with pytest.raises(KeyError):
        async for _ in tq.stream(new_job_id()):
            pass


# ── redis_url and redis_client are mutually exclusive ─────────────


def test_redis_url_and_redis_client_mutually_exclusive() -> None:
    """TaskQ(redis_url=..., redis_client=...) raises ValueError
    naming both conflicting parameters.
    """
    from unittest.mock import MagicMock

    with pytest.raises(ValueError, match=r"redis_url.*redis_client|redis_client.*redis_url"):
        TaskQ(
            dsn="postgresql://localhost/test",
            redis_url="redis://localhost",
            redis_client=MagicMock(),
        )


# ── TaskQ without redis is importable without [redis] extra ──────


def test_taskq_without_redis_importable() -> None:
    """TaskQ(dsn=...) without any redis arguments does not raise
    AttributeError or ImportError at construction time.
    """
    tq = TaskQ(dsn="postgresql://localhost/test")
    assert tq._poll_timeout == 30.0


# ── stream() outside open() raises RuntimeError ──────────────────


async def test_stream_before_open_raises_runtime_error() -> None:
    """stream() called before tq.open() raises RuntimeError
    referencing tq.open().
    """
    tq = TaskQ(dsn="postgresql://localhost/test")
    with pytest.raises(RuntimeError, match=r"tq\.open"):
        async for _ in tq.stream(new_job_id()):
            pass


# ── stream() on a job_id that never exists raises KeyError ────────


async def test_stream_on_uuid_that_never_exists_raises_key_error() -> None:
    """stream() on a new_uuid() that never exists raises KeyError
    (open TaskQ with an in-memory backend that has no jobs).
    """
    backend = _make_backend()
    tq = _inject_tq(backend)

    with pytest.raises(KeyError):
        async for _ in tq.stream(new_job_id()):
            pass


# ── poll_timeout storage ─────────────────────────────────────────────────


def test_poll_timeout_stored() -> None:
    """TaskQ(dsn=..., poll_timeout=5.0) stores _poll_timeout == 5.0."""
    tq = TaskQ(dsn="postgresql://localhost/test", poll_timeout=5.0)
    assert tq._poll_timeout == 5.0


# ── Helper ───────────────────────────────────────────────────────────────


def _enqueue_args(
    *,
    actor: str = "test_actor",
    queue: str = "default",
) -> EnqueueArgs:
    """Create EnqueueArgs for the InMemoryBackend."""
    from taskq._ids import new_job_id

    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_START,
        priority=0,
        metadata={},
    )


# ═══════════════════════════════════════════════════════════════════════════
# Integration tests - require Docker / testcontainers
# ═══════════════════════════════════════════════════════════════════════════


# ruff: noqa: S608 Why: schema name validated by WorkerSettings against _IDENT_RE; asyncpg has no parameter binding for identifiers.


def _pg_enqueue_args(
    *,
    actor: str = "stream_test_actor",
    queue: str = "default",
) -> EnqueueArgs:
    """Create EnqueueArgs for PostgresBackend integration tests."""
    return EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue=queue,
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC) - timedelta(seconds=1),
        priority=0,
        metadata={},
    )


async def _open_taskq_pg(
    pg_dsn: str,
    *,
    schema: str,
    poll_timeout: float = 0.5,
    redis_url: str | None = None,
) -> tuple[TaskQ, UUID]:
    """Open a TaskQ against the PG container with schema isolation.

    Returns (tq, worker_id) where worker_id is the registered worker UUID.
    The TaskQ creates its own pool; the internal PostgresBackend is
    accessible via ``tq._client.backend``.
    """
    from taskq.migrate import apply_pending

    tq_kwargs: dict[str, Any] = {
        "dsn": pg_dsn,
        "schema": schema,
        "poll_timeout": poll_timeout,
    }
    if redis_url is not None:
        tq_kwargs["redis_url"] = redis_url

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    tq = TaskQ(**tq_kwargs)
    await tq.open()

    worker_id = new_uuid()
    assert tq._client is not None
    async with tq._pool.acquire() as c:  # type: ignore[union-attr] # Why: tq._pool is set by open()
        await c.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
            "VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING",
            worker_id,
            "test-host",
            12345,
            ["default"],
        )

    return tq, worker_id


async def _enqueue_job(
    backend: Any,
    *,
    actor: str = "stream_test_actor",
    queue: str = "default",
) -> JobId:
    """Enqueue a job and return its id."""
    args = _pg_enqueue_args(actor=actor, queue=queue)
    row = await backend.enqueue(args)
    return JobId(row.id)


async def _dispatch_to_running(
    pool: asyncpg.Pool,
    schema: str,
    job_id: JobId,
    worker_id: UUID,
) -> None:
    """Transition a job from pending/scheduled to running via direct SQL.

    Simulates what dispatch_batch does: sets status, locked_by_worker,
    lock_expires_at, started_at, last_heartbeat_at, and increments attempt
    and claim_epoch (the claim advances both; the terminal write then
    fences on the epoch this simulated claim stamped).
    """
    async with pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".jobs SET '
            "status = 'running', "
            "locked_by_worker = $1, "
            "lock_expires_at = now() + interval '60 seconds', "
            "started_at = now(), "
            "last_heartbeat_at = now(), "
            "attempt = attempt + 1, "
            "claim_epoch = claim_epoch + 1 "
            "WHERE id = $2 AND status IN ('pending', 'scheduled')",
            worker_id,
            job_id,
        )


async def _count_listen_connections(pool: asyncpg.Pool) -> int:
    """Count connections registered as LISTENers in this database.

    The match is anchored (``LIKE 'LISTEN%'``): pg_stat_activity.query
    holds a backend's LAST statement, so a substring match also hits the
    counting query's own backend and every idle pool connection whose
    last statement merely mentioned the word - noise that cancels out in
    a single-shot count and skews a repeated poll.
    """
    row = await pool.fetchval(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE query LIKE 'LISTEN%' AND datname = current_database()"
    )
    return row


async def _count_sessions(pool: asyncpg.Pool) -> int:
    """Count client sessions in this database - the observable for "did the
    stream open a connection of its own"; the pool's sessions are in the
    baseline, so a stream that holds one shows up as an increase."""
    return await pool.fetchval(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE datname = current_database() AND backend_type = 'client backend'"
    )


# ── PG transport - stream terminates on job completion ──────────────


@pytest.mark.integration
async def test_ti1_pg_stream_terminates_on_job_completion(pg_dsn: str) -> None:
    """PG transport - stream yields terminal event and exits when
    the job reaches succeeded status.
    """
    tq, worker_id = await _open_taskq_pg(pg_dsn, schema=f"tst_{new_base62()}".lower())
    try:
        assert tq._client is not None
        backend = tq._client.backend
        job_id = await _enqueue_job(backend)
        await _dispatch_to_running(tq._pool, tq._schema, job_id, worker_id)

        async def _collect() -> list[JobEvent]:
            events: list[JobEvent] = []
            async for event in tq.stream(job_id):
                events.append(event)
            return events

        task = asyncio.create_task(_collect())

        await asyncio.sleep(0.1)
        await backend.mark_succeeded(
            job_id,
            worker_id,
            result=None,
            progress_seq=0,
            progress_state=None,
            attempt=1,
            claim_epoch=1,
        )

        events = await asyncio.wait_for(task, timeout=5.0)
        assert len(events) >= 1
        assert events[-1].terminal is True
        assert events[-1].status == "succeeded"
    finally:
        await tq.close()


# ── PG transport - all status transitions appear in stream events ──


@pytest.mark.integration
async def test_ti2_pg_all_status_transitions_appear(pg_dsn: str) -> None:
    """PG transport - stream yields events for pending → running →
    succeeded with at least one event per status. The transport samples
    the row, so each state is held until the stream has reported it: a
    state that outlives the poll interval is always observed.
    """
    tq, worker_id = await _open_taskq_pg(pg_dsn, schema=f"tst_{new_base62()}".lower())
    try:
        assert tq._client is not None
        backend = tq._client.backend
        job_id = await _enqueue_job(backend)

        events: list[JobEvent] = []

        async def _collect() -> list[JobEvent]:
            async for event in tq.stream(job_id):
                events.append(event)
            return events

        task = asyncio.create_task(_collect())
        await wait_for_condition(
            lambda: any(e.status == "pending" for e in events),
            description="the stream never reported the pending snapshot",
        )

        await _dispatch_to_running(tq._pool, tq._schema, job_id, worker_id)
        await wait_for_condition(
            lambda: any(e.status == "running" for e in events),
            description="the stream never reported the running transition",
        )

        await backend.mark_succeeded(
            job_id,
            worker_id,
            result=None,
            progress_seq=0,
            progress_state=None,
            attempt=1,
            claim_epoch=1,
        )

        events = await asyncio.wait_for(task, timeout=5.0)
        statuses = [e.status for e in events]
        assert "pending" in statuses
        assert "running" in statuses
        assert "succeeded" in statuses
        assert events[-1].terminal is True
    finally:
        await tq.close()


# ── PG transport - no dedicated connection per stream ──────────────


@pytest.mark.integration
async def test_ti3_pg_stream_holds_no_dedicated_connection(pg_dsn: str) -> None:
    """PG transport - a stream in flight adds no session of its own: the
    row is polled through the client's pool, so a page of streaming
    viewers costs no Postgres connections beyond that pool."""
    tq, worker_id = await _open_taskq_pg(pg_dsn, schema=f"tst_{new_base62()}".lower())
    try:
        assert tq._client is not None
        backend = tq._client.backend
        pool = tq._pool
        assert pool is not None
        job_id = await _enqueue_job(backend)
        await _dispatch_to_running(pool, tq._schema, job_id, worker_id)

        baseline_sessions = await _count_sessions(pool)
        baseline_listeners = await _count_listen_connections(pool)

        events: list[JobEvent] = []

        async def _collect() -> None:
            async for event in tq.stream(job_id):
                events.append(event)

        task = asyncio.create_task(_collect())
        # The stream's first poll proves it is in flight before the counts
        # are read; a fixed sleep would race its startup.
        await wait_for_condition(
            lambda: len(events) >= 1,
            description="the stream never yielded its initial snapshot",
        )
        await asyncio.sleep(0.6)  # past one poll interval: the transport is mid-loop

        assert await _count_sessions(pool) <= baseline_sessions
        assert await _count_listen_connections(pool) == baseline_listeners

        await backend.mark_succeeded(
            job_id,
            worker_id,
            result=None,
            progress_seq=0,
            progress_state=None,
            attempt=1,
            claim_epoch=1,
        )
        await asyncio.wait_for(task, timeout=5.0)
        assert events[-1].terminal is True
    finally:
        await tq.close()


# ── PG transport - a terminal write is observed within a second ─────


@pytest.mark.integration
async def test_ti4_pg_terminal_write_observed_within_one_second(pg_dsn: str) -> None:
    """PG transport - with the DEFAULT ``poll_timeout`` (30 s), a terminal
    write is observed within a second: the poll cadence is the latency
    contract, and nothing on Postgres announces the write to shortcut it."""
    tq, worker_id = await _open_taskq_pg(
        pg_dsn, schema=f"tst_{new_base62()}".lower(), poll_timeout=30.0
    )
    try:
        assert tq._client is not None
        backend = tq._client.backend
        job_id = await _enqueue_job(backend)
        await _dispatch_to_running(tq._pool, tq._schema, job_id, worker_id)

        events: list[JobEvent] = []
        observed_terminal_at: float | None = None
        loop = asyncio.get_running_loop()

        async def _collect() -> None:
            nonlocal observed_terminal_at
            async for event in tq.stream(job_id):
                events.append(event)
                if event.terminal:
                    observed_terminal_at = loop.time()

        task = asyncio.create_task(_collect())
        await wait_for_condition(
            lambda: len(events) >= 1,
            description="the stream never yielded its initial snapshot",
        )

        written_at = loop.time()
        await backend.mark_succeeded(
            job_id,
            worker_id,
            result=None,
            progress_seq=0,
            progress_state=None,
            attempt=1,
            claim_epoch=1,
        )
        await asyncio.wait_for(task, timeout=5.0)

        assert events[-1].status == "succeeded"
        assert observed_terminal_at is not None
        assert observed_terminal_at - written_at < 1.0
    finally:
        await tq.close()


# ── PG transport - poll-timeout path yields terminal event ──────────


@pytest.mark.integration
async def test_ti5_pg_poll_timeout_path_yields_terminal(pg_dsn: str) -> None:
    """PG transport - with a short poll_timeout, the stream still
    receives the terminal event even when the worker is delayed.
    """
    tq, worker_id = await _open_taskq_pg(
        pg_dsn, schema=f"tst_{new_base62()}".lower(), poll_timeout=0.1
    )
    try:
        assert tq._client is not None
        backend = tq._client.backend
        job_id = await _enqueue_job(backend)
        await _dispatch_to_running(tq._pool, tq._schema, job_id, worker_id)

        async def _collect() -> list[JobEvent]:
            events: list[JobEvent] = []
            async for event in tq.stream(job_id):
                events.append(event)
            return events

        task = asyncio.create_task(_collect())

        await asyncio.sleep(0.2)

        await backend.mark_succeeded(
            job_id,
            worker_id,
            result=None,
            progress_seq=0,
            progress_state=None,
            attempt=1,
            claim_epoch=1,
        )

        events = await asyncio.wait_for(task, timeout=5.0)
        assert events[-1].terminal is True
        assert events[-1].status == "succeeded"
    finally:
        await tq.close()


# ── Redis transport - stream terminates on job completion ───────────


@pytest.mark.integration
@pytest.mark.redis
async def test_ti6_redis_stream_terminates_on_job_completion(pg_dsn: str, redis_url: str) -> None:
    """Redis transport - stream yields terminal event and exits when
    the job reaches succeeded status. Confirms the Redis transport was used.
    """
    tq, worker_id = await _open_taskq_pg(
        pg_dsn, schema=f"tst_{new_base62()}".lower(), redis_url=redis_url, poll_timeout=0.5
    )
    try:
        assert tq._client is not None
        assert tq._client._redis_client is not None

        backend = tq._client.backend
        job_id = await _enqueue_job(backend)
        await _dispatch_to_running(tq._pool, tq._schema, job_id, worker_id)

        async def _collect() -> list[JobEvent]:
            events: list[JobEvent] = []
            async for event in tq.stream(job_id):
                events.append(event)
            return events

        task = asyncio.create_task(_collect())
        await asyncio.sleep(0.1)

        await backend.mark_succeeded(
            job_id,
            worker_id,
            result=None,
            progress_seq=0,
            progress_state=None,
            attempt=1,
            claim_epoch=1,
        )

        events = await asyncio.wait_for(task, timeout=5.0)
        assert len(events) >= 1
        assert events[-1].terminal is True
        assert events[-1].status == "succeeded"
    finally:
        await tq.close()


# ── Redis transport - progress events and monotonic progress_seq ────


@pytest.mark.integration
@pytest.mark.redis
async def test_ti7_redis_progress_events_monotonic_seq(pg_dsn: str, redis_url: str) -> None:
    """Redis transport - progress updates produce events with
    monotonically increasing progress_seq values and correct progress_state.
    """
    tq, worker_id = await _open_taskq_pg(
        pg_dsn, schema=f"tst_{new_base62()}".lower(), redis_url=redis_url, poll_timeout=0.5
    )
    try:
        assert tq._client is not None
        assert tq._client._redis_client is not None

        backend = tq._client.backend
        job_id = await _enqueue_job(backend)
        await _dispatch_to_running(tq._pool, tq._schema, job_id, worker_id)

        async def _collect() -> list[JobEvent]:
            events: list[JobEvent] = []
            async for event in tq.stream(job_id):
                events.append(event)
            return events

        task = asyncio.create_task(_collect())
        await asyncio.sleep(0.1)

        import redis.asyncio as redis_async

        from taskq._json import dumps_str
        from taskq.progress._events import ProgressEvent

        async with redis_async.from_url(
            redis_url, decode_responses=False, socket_timeout=None
        ) as raw_redis:
            channel_name = f"taskq:{tq._schema}:progress:{job_id}"

            schema = tq._schema
            pool = tq._pool
            assert pool is not None
            for i in range(1, 4):
                progress_state = {"step": i, "percent": float(i * 25)}
                async with pool.acquire() as conn:
                    await conn.execute(
                        f'UPDATE "{schema}".jobs '
                        "SET progress_state = $1::jsonb, progress_seq = $2 "
                        "WHERE id = $3 AND status = 'running'",
                        dumps_str(progress_state),
                        i,
                        job_id,
                    )

                event = ProgressEvent(
                    kind="progress",
                    job_id=job_id,
                    actor="stream_test_actor",
                    ts=datetime.now(UTC),
                    seq=i,
                    status="running",
                    step=i,
                    percent=float(i * 25),
                    terminal=False,
                )
                await raw_redis.publish(channel_name, event.model_dump_json(exclude_none=True))
                await asyncio.sleep(0.3)

            await backend.mark_succeeded(
                job_id,
                worker_id,
                result=None,
                progress_seq=3,
                progress_state=None,
                attempt=1,
                claim_epoch=1,
            )

            events = await asyncio.wait_for(task, timeout=5.0)
            assert events[-1].terminal is True

            progress_events = [e for e in events if not e.terminal]
            assert len(progress_events) >= 2, (
                f"expected at least 2 progress events, got {len(progress_events)}"
            )

            seqs = [e.progress_seq for e in progress_events]
            for i in range(1, len(seqs)):
                assert seqs[i] >= seqs[i - 1], f"progress_seq not monotonic: {seqs}"

            states = [e.progress_state for e in progress_events]
            step_values: list[int] = [
                s["step"]
                for s in states
                if s.get("step") is not None  # type: ignore[reportAssignmentType] # Why: dict[str,object] values are object; narrowing via s.get() guard is not enough for pyright. Casting or a type guard would be more precise, but this is test-only narrowing for a known schema.
            ]
            assert len(step_values) >= 1, "expected at least 1 step value in progress_state"
            for i in range(1, len(step_values)):
                assert step_values[i] > step_values[i - 1], f"steps not increasing: {step_values}"
    finally:
        await tq.close()


# ── Redis transport - malformed message is skipped ──────────────────


@pytest.mark.integration
@pytest.mark.redis
async def test_ti8_redis_malformed_message_skipped(pg_dsn: str, redis_url: str) -> None:
    """Redis transport - a malformed message on the progress channel
    is skipped and the stream continues, eventually receiving the terminal
    event.
    """
    tq, worker_id = await _open_taskq_pg(
        pg_dsn, schema=f"tst_{new_base62()}".lower(), redis_url=redis_url, poll_timeout=0.5
    )
    try:
        assert tq._client is not None
        assert tq._client._redis_client is not None

        backend = tq._client.backend
        job_id = await _enqueue_job(backend)
        await _dispatch_to_running(tq._pool, tq._schema, job_id, worker_id)

        import redis.asyncio as redis_async

        async with redis_async.from_url(
            redis_url, decode_responses=False, socket_timeout=None
        ) as raw_redis:
            channel_name = f"taskq:{tq._schema}:progress:{job_id}"
            await raw_redis.publish(channel_name, b"this is not valid json {{{")
            await asyncio.sleep(0.05)

            async def _collect() -> list[JobEvent]:
                events: list[JobEvent] = []
                async for event in tq.stream(job_id):
                    events.append(event)
                return events

            task = asyncio.create_task(_collect())
            await asyncio.sleep(0.1)

            await backend.mark_succeeded(
                job_id,
                worker_id,
                result=None,
                progress_seq=0,
                progress_state=None,
                attempt=1,
                claim_epoch=1,
            )

            events = await asyncio.wait_for(task, timeout=5.0)
            assert events[-1].terminal is True
            assert events[-1].status == "succeeded"
    finally:
        await tq.close()
