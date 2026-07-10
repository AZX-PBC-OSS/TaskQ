"""Unit, negative, and integration tests for TaskQ.stream().

Unit and negative tests run with ``pytest -m "not integration"``
— no Docker, no testcontainers. They use :class:`InMemoryBackend` and
class:`~taskq.testing.clock.FakeClock` from
mod:`taskq.testing.fixtures` — the in-memory backend IS the unit-test
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
- PG transport — stream terminates on job completion.
- PG transport — all status transitions appear in stream events.
- PG transport — dedicated LISTEN connection is closed after stream exits.
- PG transport — break inside async for closes LISTEN connection.
- PG transport — poll-timeout path yields terminal event.
- Redis transport — stream terminates on job completion.
- Redis transport — progress events and monotonic progress_seq.
- Redis transport — malformed message is skipped, stream continues.
- Chaos — PG LISTEN connection dropped mid-stream, stream recovers.
"""

import asyncio
import dataclasses
import shutil
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID, uuid4

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs, JobId, JobRow, JobStatus
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.client._jobs import JobsClient
from taskq.client._taskq import JobEvent, TaskQ, _row_to_event
from taskq.settings import TaskQSettings
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
    and inject the client directly — the same pattern used in
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
        async for _ in tq.stream(cast(JobId, uuid4())):
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
        async for _ in tq.stream(cast(JobId, uuid4())):
            pass


# ── stream() on a job_id that never exists raises KeyError ────────


async def test_stream_on_uuid_that_never_exists_raises_key_error() -> None:
    """stream() on a uuid4() that never exists raises KeyError
    (open TaskQ with an in-memory backend that has no jobs).
    """
    backend = _make_backend()
    tq = _inject_tq(backend)

    with pytest.raises(KeyError):
        async for _ in tq.stream(cast(JobId, uuid4())):
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
# Integration tests — require Docker / testcontainers
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
    lock_expires_at, started_at, last_heartbeat_at, and increments attempt.
    Also issues pg_notify on the wake channel so LISTEN-based streams
    detect the state change.
    """
    from taskq.constants import wake_channel

    channel = wake_channel(schema)
    async with pool.acquire() as conn, conn.transaction():
        await conn.execute(
            f'UPDATE "{schema}".jobs SET '
            "status = 'running', "
            "locked_by_worker = $1, "
            "lock_expires_at = now() + interval '60 seconds', "
            "started_at = now(), "
            "last_heartbeat_at = now(), "
            "attempt = attempt + 1 "
            "WHERE id = $2 AND status IN ('pending', 'scheduled')",
            worker_id,
            job_id,
        )
        await conn.execute(f"SELECT pg_notify('{channel}', '')")


async def _count_listen_connections(pool: asyncpg.Pool) -> int:
    """Count connections with an active LISTEN on the wake channel."""
    row = await pool.fetchval(
        "SELECT count(*) FROM pg_stat_activity "
        "WHERE query LIKE '%LISTEN%' AND datname = current_database()"
    )
    return row


# ── PG transport — stream terminates on job completion ──────────────


@pytest.mark.integration
async def test_ti1_pg_stream_terminates_on_job_completion(pg_dsn: str) -> None:
    """PG transport — stream yields terminal event and exits when
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
            job_id, worker_id, result=None, progress_seq=0, progress_state=None
        )

        events = await asyncio.wait_for(task, timeout=5.0)
        assert len(events) >= 1
        assert events[-1].terminal is True
        assert events[-1].status == "succeeded"
    finally:
        await tq.close()


# ── PG transport — all status transitions appear in stream events ──


@pytest.mark.integration
async def test_ti2_pg_all_status_transitions_appear(pg_dsn: str) -> None:
    """PG transport — stream yields events for pending → running →
    succeeded with at least one event per status.
    """
    tq, worker_id = await _open_taskq_pg(pg_dsn, schema=f"tst_{new_base62()}".lower())
    try:
        assert tq._client is not None
        backend = tq._client.backend
        job_id = await _enqueue_job(backend)

        async def _collect() -> list[JobEvent]:
            events: list[JobEvent] = []
            async for event in tq.stream(job_id):
                events.append(event)
            return events

        task = asyncio.create_task(_collect())
        await asyncio.sleep(0.1)

        await _dispatch_to_running(tq._pool, tq._schema, job_id, worker_id)
        await asyncio.sleep(0.1)

        await backend.mark_succeeded(
            job_id, worker_id, result=None, progress_seq=0, progress_state=None
        )

        events = await asyncio.wait_for(task, timeout=5.0)
        statuses = [e.status for e in events]
        assert "pending" in statuses
        assert "running" in statuses
        assert "succeeded" in statuses
        assert events[-1].terminal is True
    finally:
        await tq.close()


# ── PG transport — LISTEN connection closed after stream exits ──────


@pytest.mark.integration
async def test_ti3_pg_listen_connection_closed_after_stream(pg_dsn: str) -> None:
    """PG transport — dedicated LISTEN connection is released after
    the stream exits normally.
    """
    tq, worker_id = await _open_taskq_pg(pg_dsn, schema=f"tst_{new_base62()}".lower())
    try:
        assert tq._client is not None
        backend = tq._client.backend
        pool = tq._pool
        assert pool is not None
        job_id = await _enqueue_job(backend)
        await _dispatch_to_running(pool, tq._schema, job_id, worker_id)

        baseline = await _count_listen_connections(pool)

        async def _collect() -> list[JobEvent]:
            events: list[JobEvent] = []
            async for event in tq.stream(job_id):
                events.append(event)
            return events

        task = asyncio.create_task(_collect())
        await asyncio.sleep(0.1)

        during = await _count_listen_connections(pool)
        assert during > baseline

        await backend.mark_succeeded(
            job_id, worker_id, result=None, progress_seq=0, progress_state=None
        )

        events = await asyncio.wait_for(task, timeout=5.0)
        assert events[-1].terminal is True

        await asyncio.sleep(0.3)
        after = await _count_listen_connections(pool)
        assert after <= baseline
    finally:
        await tq.close()


# ── PG transport — break inside async for closes LISTEN connection ──


@pytest.mark.integration
async def test_ti4_pg_break_closes_listen_connection(pg_dsn: str) -> None:
    """PG transport — breaking out of the async for loop releases
    the dedicated LISTEN connection (no leak).
    """
    tq, worker_id = await _open_taskq_pg(pg_dsn, schema=f"tst_{new_base62()}".lower())
    try:
        assert tq._client is not None
        backend = tq._client.backend
        pool = tq._pool
        assert pool is not None
        job_id = await _enqueue_job(backend)
        await _dispatch_to_running(pool, tq._schema, job_id, worker_id)

        baseline = await _count_listen_connections(pool)

        events: list[JobEvent] = []
        async for event in tq.stream(job_id):
            events.append(event)
            break

        await asyncio.sleep(0.3)
        after = await _count_listen_connections(pool)
        assert after <= baseline
    finally:
        await tq.close()


# ── PG transport — poll-timeout path yields terminal event ──────────


@pytest.mark.integration
async def test_ti5_pg_poll_timeout_path_yields_terminal(pg_dsn: str) -> None:
    """PG transport — with a short poll_timeout, the stream still
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
            job_id, worker_id, result=None, progress_seq=0, progress_state=None
        )

        events = await asyncio.wait_for(task, timeout=5.0)
        assert events[-1].terminal is True
        assert events[-1].status == "succeeded"
    finally:
        await tq.close()


# ── Redis transport — stream terminates on job completion ───────────


@pytest.mark.integration
@pytest.mark.redis
async def test_ti6_redis_stream_terminates_on_job_completion(pg_dsn: str, redis_url: str) -> None:
    """Redis transport — stream yields terminal event and exits when
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
            job_id, worker_id, result=None, progress_seq=0, progress_state=None
        )

        events = await asyncio.wait_for(task, timeout=5.0)
        assert len(events) >= 1
        assert events[-1].terminal is True
        assert events[-1].status == "succeeded"
    finally:
        await tq.close()


# ── Redis transport — progress events and monotonic progress_seq ────


@pytest.mark.integration
@pytest.mark.redis
async def test_ti7_redis_progress_events_monotonic_seq(pg_dsn: str, redis_url: str) -> None:
    """Redis transport — progress updates produce events with
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

        async with redis_async.from_url(redis_url, decode_responses=False) as raw_redis:
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
                job_id, worker_id, result=None, progress_seq=3, progress_state=None
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


# ── Redis transport — malformed message is skipped ──────────────────


@pytest.mark.integration
@pytest.mark.redis
async def test_ti8_redis_malformed_message_skipped(pg_dsn: str, redis_url: str) -> None:
    """Redis transport — a malformed message on the progress channel
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

        async with redis_async.from_url(redis_url, decode_responses=False) as raw_redis:
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
                job_id, worker_id, result=None, progress_seq=0, progress_state=None
            )

            events = await asyncio.wait_for(task, timeout=5.0)
            assert events[-1].terminal is True
            assert events[-1].status == "succeeded"
    finally:
        await tq.close()


# ── Chaos — PG LISTEN connection dropped mid-stream ────────────────


@pytest.mark.integration
async def test_tc1_pg_listen_connection_dropped_stream_recovers(
    pg_dsn: str,
) -> None:
    """Chaos — pg_terminate_backend kills the dedicated LISTEN
    connection mid-stream. The stream does NOT propagate an unhandled
    exception and eventually yields the terminal event via the
    poll-timeout re-fetch path.
    """
    tq, worker_id = await _open_taskq_pg(
        pg_dsn, schema=f"tst_{new_base62()}".lower(), poll_timeout=0.3
    )
    try:
        assert tq._client is not None
        backend = tq._client.backend
        pool = tq._pool
        assert pool is not None
        job_id = await _enqueue_job(backend)

        async def _collect() -> list[JobEvent]:
            events: list[JobEvent] = []
            async for event in tq.stream(job_id):
                events.append(event)
            return events

        task = asyncio.create_task(_collect())
        await asyncio.sleep(0.1)

        pool = tq._pool
        assert pool is not None

        async with pool.acquire() as conn:
            listen_pids = await conn.fetch(
                "SELECT pid FROM pg_stat_activity "
                "WHERE query LIKE '%LISTEN%' AND datname = current_database() "
                "AND pid != pg_backend_pid()"
            )
        pids_to_kill = [row["pid"] for row in listen_pids]

        for pid in pids_to_kill:
            psql_path = shutil.which("psql")
            assert psql_path is not None, "psql not found on PATH"
            proc = await asyncio.create_subprocess_exec(
                psql_path,
                pg_dsn,
                "-c",
                f"SELECT pg_terminate_backend({int(pid)})",
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await proc.wait()

        await asyncio.sleep(0.2)

        await _dispatch_to_running(pool, tq._schema, job_id, worker_id)
        await asyncio.sleep(0.1)

        await backend.mark_succeeded(
            job_id, worker_id, result=None, progress_seq=0, progress_state=None
        )

        events = await asyncio.wait_for(task, timeout=10.0)
        assert events[-1].terminal is True
        assert events[-1].status == "succeeded"
    finally:
        await tq.close()
