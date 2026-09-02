"""Unit tests for TaskQ.stream() and _row_to_event.

Covers:
- stream() on an already-terminal job yields one event and returns.
- stream() on a non-existent job_id raises KeyError.
- _row_to_event maps terminal statuses to terminal=True and
  non-terminal statuses to terminal=False.
- stream() called outside async with block raises RuntimeError.
- Redis transport: get_message loop yields JobEvent on state change,
  skips malformed messages, terminates on terminal.
- PG transport: RuntimeError when dsn is None (pool-only construction).
- Bounded owned-LISTEN-conn closes (#37): _stream_pg/_watch_reclaims_pg
  teardown and watch_reclaims reconnect paths bound close() via
  close_conn_bounded — a dead PG cannot wedge the generator.
"""

import asyncio
import contextlib
import dataclasses
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import asyncpg
import pytest
import structlog
from pydantic import TypeAdapter

from taskq.backend._protocol import Backend, EventRow, JobId, JobRow, JobStatus
from taskq.client._jobs import JobsClient
from taskq.client._taskq import (
    JobEvent,
    TaskQ,
    _row_to_event,
    _stream_redis,
    _watch_reclaims_pg,
)
from taskq.progress._events import ProgressEvent
from taskq.settings import TaskQSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args, make_job_row

_RA = TypeAdapter(type(None))

_SCHEMA_LABEL = "taskq_test"
_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000001")
_ACTOR = "test_actor"

_START = datetime(2025, 1, 1, tzinfo=UTC)

ALL_STATUSES: list[JobStatus] = [
    "pending",
    "scheduled",
    "running",
    "succeeded",
    "failed",
    "cancelled",
    "crashed",
    "abandoned",
]

TERMINAL_STATUSES: list[JobStatus] = [
    "succeeded",
    "failed",
    "cancelled",
    "crashed",
    "abandoned",
]

NON_TERMINAL_STATUSES: list[JobStatus] = [
    "pending",
    "scheduled",
    "running",
]


def _row(
    *,
    status: JobStatus = "running",
    progress_seq: int = 0,
    job_id: UUID = _JOB_ID,
    progress_state: dict[str, object] | None = None,
) -> JobRow:
    row = make_job_row(
        status=status,
        progress_seq=progress_seq,
        actor=_ACTOR,
    )
    return dataclasses.replace(
        row,
        id=cast(JobId, job_id),
        progress_state=progress_state if progress_state is not None else row.progress_state,
    )


def _stub_backend(
    *,
    rows: list[JobRow],
) -> Backend:
    """Build a stub Backend where ``get`` returns successive rows."""
    remaining = list(rows)

    backend = AsyncMock(spec=Backend)

    async def _get(job_id: JobId) -> JobRow | None:
        if remaining:
            return remaining.pop(0)
        return None

    backend.get = _get
    return backend


def _make_client(
    backend: Backend,
    *,
    redis_client: object | None = None,
) -> JobsClient:
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    client = JobsClient(backend, settings=settings)
    if redis_client is not None:
        client._redis_client = redis_client  # type: ignore[assignment] # Why: test-only injection of mock redis client
    return client


# ── _row_to_event ────────────────────────────────────────────────


@pytest.mark.parametrize("status", TERMINAL_STATUSES)
def test_row_to_event_terminal_statuses(status: JobStatus) -> None:
    """_row_to_event maps terminal statuses to terminal=True."""
    row = _row(status=status)
    event = _row_to_event(row)
    assert event.terminal is True
    assert event.status == status


@pytest.mark.parametrize("status", NON_TERMINAL_STATUSES)
def test_row_to_event_non_terminal_statuses(status: JobStatus) -> None:
    """_row_to_event maps non-terminal statuses to terminal=False."""
    row = _row(status=status)
    event = _row_to_event(row)
    assert event.terminal is False
    assert event.status == status


def test_row_to_event_preserves_fields() -> None:
    """_row_to_event carries all relevant fields from the row."""
    row = _row(
        status="running",
        progress_seq=5,
        progress_state={"step": 1, "percent": 50},
    )
    event = _row_to_event(row)
    assert event.job_id == row.id
    assert event.status == "running"
    assert event.progress_seq == 5
    assert event.progress_state == {"step": 1, "percent": 50}
    assert event.terminal is False


# ── stream on terminal job ───────────────────────────────────────


async def test_stream_terminal_job_yields_one_event() -> None:
    """stream() on a job already terminal yields one event and returns."""
    row = _row(status="succeeded", progress_seq=1)
    backend = _stub_backend(rows=[row])
    client = _make_client(backend)

    tq = TaskQ.__new__(TaskQ)
    tq._client = client
    tq._redis_client = None
    tq._dsn = None
    tq._schema = _SCHEMA_LABEL
    tq._poll_timeout = 30.0

    events: list[JobEvent] = []
    async for event in tq.stream(cast(JobId, _JOB_ID)):
        events.append(event)

    assert len(events) == 1
    assert events[0].terminal is True
    assert events[0].status == "succeeded"


# ── stream on non-existent job ────────────────────────────────────


async def test_stream_nonexistent_job_raises_key_error() -> None:
    """stream() on a non-existent job_id raises KeyError."""
    backend = _stub_backend(rows=[])
    client = _make_client(backend)

    tq = TaskQ.__new__(TaskQ)
    tq._client = client
    tq._redis_client = None
    tq._dsn = None
    tq._schema = _SCHEMA_LABEL
    tq._poll_timeout = 30.0

    with pytest.raises(KeyError):
        async for _ in tq.stream(cast(JobId, _JOB_ID)):
            pass


# ── stream before open ────────────────────────────────────────────


async def test_stream_before_open_raises_runtime_error() -> None:
    """stream() called outside async with block raises RuntimeError."""
    tq = TaskQ(dsn="postgresql://user:pw@host/db")
    with pytest.raises(RuntimeError, match=r"tq\.open"):
        async for _ in tq.stream(cast(JobId, _JOB_ID)):
            pass


# ── PG transport: dsn is None raises RuntimeError ────────────────────────


async def test_stream_pg_raises_when_dsn_none() -> None:
    """PG LISTEN transport raises RuntimeError when no LISTEN source is
    provided (pool-only construction with no ``pg_conn_factory`` / ``listen_conn``).
    """
    row = _row(status="running", progress_seq=0)
    backend = _stub_backend(rows=[row])
    client = _make_client(backend)

    tq = TaskQ.__new__(TaskQ)
    tq._client = client
    tq._redis_client = None
    tq._dsn = None
    tq._pg_conn_factory = None
    tq._listen_conn = None
    tq._schema = _SCHEMA_LABEL
    tq._poll_timeout = 30.0

    with pytest.raises(RuntimeError, match="LISTEN transport source"):
        async for _ in tq.stream(cast(JobId, _JOB_ID)):
            pass


# ── PG transport: pg_conn_factory / listen_conn hooks ────────────────────


class _FakeListenConn:
    """Fake asyncpg.Connection for the LISTEN transport.

    Yields one state change then a terminal status, so the stream exits.
    """

    def __init__(self) -> None:
        self.closed = False
        self.removed: list[str] = []

    async def add_listener(self, channel: str, callback: object) -> None:
        pass

    async def remove_listener(self, channel: str, callback: object) -> None:
        self.removed.append(channel)

    async def close(self) -> None:
        self.closed = True


async def test_stream_pg_with_pg_conn_factory_closes_conn() -> None:
    """pg_conn_factory produces a TaskQ-owned conn that is closed in finally."""
    rows = [_row(status="running", progress_seq=0), _row(status="succeeded", progress_seq=1)]
    backend = _stub_backend(rows=rows)
    client = _make_client(backend)

    fake_conn = _FakeListenConn()

    async def factory() -> "asyncpg.Connection":
        return cast("asyncpg.Connection", fake_conn)

    tq = TaskQ.__new__(TaskQ)
    tq._client = client
    tq._redis_client = None
    tq._dsn = None
    tq._pg_conn_factory = factory
    tq._listen_conn = None
    tq._schema = _SCHEMA_LABEL
    tq._poll_timeout = 30.0

    events = [e async for e in tq.stream(cast(JobId, _JOB_ID))]
    assert len(events) == 2
    assert events[1].terminal is True
    # Factory-produced → closed
    assert fake_conn.closed


async def test_stream_pg_with_listen_conn_does_not_close() -> None:
    """listen_conn is caller-owned; it is NOT closed by the stream."""
    rows = [_row(status="running", progress_seq=0), _row(status="succeeded", progress_seq=1)]
    backend = _stub_backend(rows=rows)
    client = _make_client(backend)

    fake_conn = _FakeListenConn()

    tq = TaskQ.__new__(TaskQ)
    tq._client = client
    tq._redis_client = None
    tq._dsn = None
    tq._pg_conn_factory = None
    tq._listen_conn = cast("asyncpg.Connection", fake_conn)
    tq._schema = _SCHEMA_LABEL
    tq._poll_timeout = 30.0

    events = [e async for e in tq.stream(cast(JobId, _JOB_ID))]
    assert events[-1].terminal is True
    # Caller-owned → NOT closed
    assert not fake_conn.closed


async def test_taskq_init_rejects_pg_conn_factory_and_listen_conn() -> None:
    """TaskQ.__init__ rejects providing both pg_conn_factory and listen_conn."""
    with pytest.raises(ValueError, match=r"pg_conn_factory.*listen_conn"):
        TaskQ(pool=object(), pg_conn_factory=lambda: None, listen_conn=object())  # type: ignore[arg-type]


# ── Bounded owned-LISTEN-conn closes (#37) ───────────────────────────────
#
# asyncpg's Connection.close() passes no timeout underneath, so against a
# dead PG it can hang forever — contextlib.suppress(Exception) catches
# errors but cannot stop a call that never returns. These tests pin that
# every TaskQ-owned LISTEN-conn close in _stream_pg / _watch_reclaims_pg
# (teardown AND the watch_reclaims reconnect error paths) goes through
# close_conn_bounded: after the bound the conn is terminated and the
# surrounding flow continues. CLOSE_TIMEOUT_SECS is shrunk via the
# module-global monkeypatch seam (read at call time).


class _FakeHungCloseListenConn(_FakeListenConn):
    """LISTEN conn whose close() hangs until terminate() releases the gate.

    Mirrors the _FakeHungClosePool convention in tests/test_taskq_client.py:
    asyncpg is a C extension, so spec-mocks cannot express a hang gate.
    """

    def __init__(self) -> None:
        super().__init__()
        self.close_wait = asyncio.Event()  # starts cleared → close() hangs
        self.close_calls = 0
        self.terminated = False

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.close_wait.set()


async def test_stream_pg_finally_bounds_hung_owned_conn_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_stream_pg teardown: an owned conn whose close() hangs (dead PG at
    stream end) must not wedge generator finalization — the close is
    bounded, then the conn is terminated."""
    import taskq.client._taskq as taskq_mod

    monkeypatch.setattr(taskq_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    rows = [_row(status="running", progress_seq=0), _row(status="succeeded", progress_seq=1)]
    backend = _stub_backend(rows=rows)
    client = _make_client(backend)

    fake_conn = _FakeHungCloseListenConn()

    async def factory() -> asyncpg.Connection:
        return cast(asyncpg.Connection, fake_conn)

    tq = TaskQ.__new__(TaskQ)
    tq._client = client
    tq._redis_client = None
    tq._dsn = None
    tq._pg_conn_factory = factory
    tq._listen_conn = None
    tq._schema = _SCHEMA_LABEL
    tq._poll_timeout = 30.0

    # Why the outer timeout: pre-fix the finally awaits conn.close()
    # unbounded, so the RED state would hang forever instead of failing fast.
    async with asyncio.timeout(5):
        events = [e async for e in tq.stream(cast(JobId, _JOB_ID))]

    assert len(events) == 2
    assert events[1].terminal is True
    assert fake_conn.close_calls == 1
    assert fake_conn.terminated is True


# ── watch_reclaims: bounded owned-conn closes (#37) ──────────────────────
#
# The minimal _watch_reclaims_pg harness helpers are replicated from
# tests/test_watch_reclaims.py — the same convention already used for
# _FakeListenConn, which is duplicated across both files.


class _FakeHungCloseWatchConn:
    """Full _watch_reclaims_pg conn fake whose close() can hang on a gate.

    Interaction surface mirrors tests/test_watch_reclaims.py's
    _FakeListenConn (kill/_die fire termination listeners; detection goes
    through is_closed(), never an exception); the close() hang gate mirrors
    _FakeHungClosePool. terminate() releases the gate, mirroring the real
    Connection whose terminate() kills the session immediately.
    """

    def __init__(self, *, close_hangs: bool = True) -> None:
        self._closed = False
        self._notify_callbacks: list[tuple[str, Any]] = []
        self._termination_listeners: list[Any] = []
        self.listener_channels: list[str] = []
        self.close_calls = 0
        self.close_wait = asyncio.Event()
        if not close_hangs:
            self.close_wait.set()
        self.terminated = False

    async def add_listener(self, channel: str, callback: Any) -> None:
        if self._closed:
            raise asyncpg.InterfaceError("connection is closed")
        self.listener_channels.append(channel)
        self._notify_callbacks.append((channel, callback))

    async def remove_listener(self, channel: str, callback: Any) -> None:
        self._notify_callbacks = [
            (ch, cb) for ch, cb in self._notify_callbacks if not (ch == channel and cb is callback)
        ]

    def add_termination_listener(self, callback: Any) -> None:
        self._termination_listeners.append(callback)

    def remove_termination_listener(self, callback: Any) -> None:
        if callback in self._termination_listeners:
            self._termination_listeners.remove(callback)

    def is_closed(self) -> bool:
        return self._closed

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        self._die()

    def terminate(self) -> None:
        self.terminated = True
        self.close_wait.set()
        self._die()

    def kill(self) -> None:
        """Simulate pg_terminate_backend: the conn dies server-side."""
        self._die()

    def _die(self) -> None:
        if self._closed:
            return
        self._closed = True
        for cb in list(self._termination_listeners):
            cb(self)


class _ReconnectAddListenerFailsConn(_FakeHungCloseWatchConn):
    """Reconnect candidate whose add_listener raises — a failed reconnect."""

    async def add_listener(self, channel: str, callback: Any) -> None:
        raise asyncpg.InterfaceError("pg still down")


_WATCH_GRACE = timedelta(seconds=30)


def _make_watch_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


async def _make_running_row(backend: InMemoryBackend) -> JobId:
    """Enqueue a job, flip it to running with an expired lock, then reclaim
    it — leaves one crash-reclaim event in the backend's event log for
    _watch_reclaims_pg to observe. Mirrors tests/test_watch_reclaims.py."""
    args = make_enqueue_args(
        actor="stream_watch_test_actor",
        queue="default",
        payload={},
        scheduled_at=_START,
        max_attempts=3,
        retry_kind="transient",  # type: ignore[arg-type]  # Why: test helper accepts the same RetryKind literals as the real EnqueueArgs
        priority=0,
        schedule_to_close=None,
    )
    row = await backend.enqueue(args)
    job_id = row.id

    worker_id = backend._worker_id  # type: ignore[reportPrivateUsage]  # Why: test-only private access
    running_row = dataclasses.replace(
        row,
        status="running",
        locked_by_worker=worker_id,
        lock_expires_at=_START - timedelta(seconds=1),
        started_at=_START,
        last_heartbeat_at=_START,
    )
    backend._jobs[job_id] = running_row  # type: ignore[reportPrivateUsage]  # Why: test-only private access

    backend.advance_clock_to(_START + timedelta(seconds=1))
    count = await backend.reclaim_expired_locks(_WATCH_GRACE, _WATCH_GRACE)
    assert count == 1
    return job_id


async def _collect(gen: Any, *, n: int) -> list[EventRow]:
    """Collect exactly *n* items from an unbounded async generator, then
    close it (aclosing semantics → the generator's finally runs)."""
    collected: list[EventRow] = []
    async with contextlib.aclosing(gen) as agen:
        async for evt in agen:
            collected.append(evt)
            if len(collected) >= n:
                break
    return collected


async def test_watch_reclaims_failed_reconnect_bounds_hung_new_conn_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failed-reconnect close (mid-run): a new conn that failed LISTEN setup
    against a dead PG can hang close(); an unbounded close would wedge the
    degraded poll loop — the only live delivery path. The bounded close
    terminates the failed conn and the loop keeps polling (failed_attempts
    increments, delivery continues)."""
    import taskq.client._taskq as taskq_mod

    monkeypatch.setattr(taskq_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    monkeypatch.setattr(taskq_mod, "_RECONNECT_POLL_INTERVAL", 1)
    backend = _make_watch_backend()
    client = _make_client(backend)
    conns: list[_FakeHungCloseWatchConn] = []

    async def _factory() -> _FakeHungCloseWatchConn:
        conn: _FakeHungCloseWatchConn = (
            _FakeHungCloseWatchConn(close_hangs=False)
            if not conns
            else _ReconnectAddListenerFailsConn()
        )
        conns.append(conn)
        return conn

    gen = _watch_reclaims_pg(
        None,
        _SCHEMA_LABEL,
        client,
        0.02,
        pg_conn_factory=_factory,  # type: ignore[arg-type]  # Why: fake conn stand-in for asyncpg.Connection
    )
    with structlog.testing.capture_logs() as captured:
        task = asyncio.create_task(_collect(gen, n=1))
        try:
            await asyncio.sleep(0.05)  # initial conn opened, LISTEN registered
            assert len(conns) == 1
            conns[0].kill()  # into the owned-conn poll/reconnect fallback
            await asyncio.sleep(0.5)  # several failed reconnect attempts
            await _make_running_row(backend)
            events = await asyncio.wait_for(task, timeout=5.0)
        finally:
            # Unwedge the RED state: pre-fix the generator is parked in the
            # unbounded close; releasing the gates lets cancellation unwind.
            for conn in conns:
                conn.close_wait.set()
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    assert len(events) == 1
    failed = conns[1:]
    assert len(failed) >= 2, (
        "poll loop wedged after a failed reconnect — failed_attempts must "
        "increment and the loop must keep polling"
    )
    assert all(c.terminated for c in failed if c.close_calls > 0)
    assert any(e["event"] == "watch_reclaims-reconnect-still-failing" for e in captured)


async def test_watch_reclaims_reconnect_swap_bounds_hung_old_conn_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reconnect-swap close (mid-run): the OLD conn being swapped out was
    already diagnosed dead — the sharpest close()-hang case. The bounded
    close terminates it (not leaked), the swap completes, and the generator
    logs 'watch_reclaims-listen-reconnected' and resumes LISTEN-driven
    delivery."""
    import taskq.client._taskq as taskq_mod

    monkeypatch.setattr(taskq_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    monkeypatch.setattr(taskq_mod, "_RECONNECT_POLL_INTERVAL", 2)
    backend = _make_watch_backend()
    client = _make_client(backend)
    conns: list[_FakeHungCloseWatchConn] = []

    async def _factory() -> _FakeHungCloseWatchConn:
        conn = _FakeHungCloseWatchConn(close_hangs=not conns)
        conns.append(conn)
        return conn

    gen = _watch_reclaims_pg(
        None,
        _SCHEMA_LABEL,
        client,
        0.02,
        pg_conn_factory=_factory,  # type: ignore[arg-type]  # Why: fake conn stand-in for asyncpg.Connection
    )
    with structlog.testing.capture_logs() as captured:
        task = asyncio.create_task(_collect(gen, n=1))
        try:
            await asyncio.sleep(0.05)  # initial conn opened, LISTEN registered
            assert len(conns) == 1
            conns[0].kill()
            await asyncio.sleep(0.3)  # detection + reconnect + bounded old-conn close
            await _make_running_row(backend)
            # Why shield: pre-fix the generator wedges in the swap close, and
            # cancelling the collect task would re-wedge it in the generator's
            # own finally (same hung conn) — the outer timeout would never
            # return. Shield keeps the RED fail-fast (TimeoutError); the
            # finally below then releases the gates so the task unwinds.
            events = await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
        finally:
            for conn in conns:
                conn.close_wait.set()
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    assert len(events) == 1
    assert len(conns) == 2
    assert conns[0].terminated is True, "dead swapped-out conn must be terminated, not leaked"
    assert conns[1].listener_channels, "reconnected conn never registered LISTEN"
    assert any(e["event"] == "watch_reclaims-listen-reconnected" for e in captured)


async def test_watch_reclaims_finally_bounds_hung_owned_conn_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_watch_reclaims_pg teardown: closing the generator (consumer done)
    closes the owned conn — a hung close() against a dead PG must not wedge
    finalization; the bounded close terminates the conn instead."""
    import taskq.client._taskq as taskq_mod

    monkeypatch.setattr(taskq_mod, "CLOSE_TIMEOUT_SECS", 0.05)
    backend = _make_watch_backend()
    await _make_running_row(backend)
    client = _make_client(backend)
    conn = _FakeHungCloseWatchConn(close_hangs=True)

    async def _factory() -> _FakeHungCloseWatchConn:
        return conn

    gen = _watch_reclaims_pg(
        None,
        _SCHEMA_LABEL,
        client,
        0.02,
        pg_conn_factory=_factory,  # type: ignore[arg-type]  # Why: fake conn stand-in for asyncpg.Connection
    )
    # Why the outer timeout: pre-fix the finally awaits conn.close()
    # unbounded, so the RED state would hang forever instead of failing fast.
    events = await asyncio.wait_for(_collect(gen, n=1), timeout=5.0)

    assert len(events) == 1
    assert conn.close_calls == 1
    assert conn.terminated is True


# ── Redis transport: _stream_redis ───────────────────────────────────────


def _make_pubsub_get_message_mock(
    messages: list[dict[str, object] | None],
) -> AsyncMock:
    """Build a mock pubsub with ``get_message`` returning successive items.

    ``None`` entries simulate timeout (no message available).
    """
    pubsub = AsyncMock()
    remaining = list(messages)

    async def _get_message(
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109 # Why: mock signature matches redis-py PubSub.get_message API; not an actual async boundary
    ) -> dict[str, object] | None:
        if remaining:
            return remaining.pop(0)
        return None

    pubsub.get_message = _get_message
    pubsub.subscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()
    pubsub.aclose = AsyncMock()
    return pubsub


def _progress_event_bytes(
    *,
    seq: int = 1,
    kind: str = "progress",
    status: str = "running",
    terminal: bool = False,
) -> bytes:
    event = ProgressEvent(
        kind=kind,  # type: ignore[arg-type] # Why: test-only construction with known-valid values
        job_id=_JOB_ID,
        actor=_ACTOR,
        ts=datetime.now(UTC),
        seq=seq,
        status=status,
        terminal=terminal,
    )
    return event.model_dump_json(exclude_none=True).encode("utf-8")


async def test_stream_redis_yields_job_events_on_state_change() -> None:
    """Redis transport yields JobEvent when backend.get() detects a change
    after a ProgressEvent arrives on the channel.
    """
    row_running = _row(status="running", progress_seq=1)
    row_succeeded = _row(status="succeeded", progress_seq=2)
    backend = _stub_backend(rows=[row_running, row_succeeded])
    client = _make_client(backend)

    messages: list[dict[str, object] | None] = [
        {"type": "message", "data": _progress_event_bytes(seq=1, status="running")},
        {
            "type": "message",
            "data": _progress_event_bytes(seq=2, status="succeeded", terminal=True),
        },
    ]
    pubsub = _make_pubsub_get_message_mock(messages)
    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    events: list[JobEvent] = []
    async for event in _stream_redis(
        redis_client,
        _SCHEMA_LABEL,
        cast(JobId, _JOB_ID),
        client,
        30.0,
    ):
        events.append(event)

    assert len(events) == 2
    assert events[0].status == "running"
    assert events[0].terminal is False
    assert events[1].status == "succeeded"
    assert events[1].terminal is True

    pubsub.subscribe.assert_awaited_once()
    pubsub.unsubscribe.assert_awaited_once()
    pubsub.aclose.assert_awaited_once()


async def test_stream_redis_skips_malformed_messages() -> None:
    """Malformed messages are logged at warning and skipped; stream continues."""
    row_running = _row(status="running", progress_seq=1)
    row_succeeded = _row(status="succeeded", progress_seq=2)
    backend = _stub_backend(rows=[row_running, row_succeeded])
    client = _make_client(backend)

    messages: list[dict[str, object] | None] = [
        {"type": "message", "data": b"not valid json"},
        {"type": "message", "data": _progress_event_bytes(seq=1, status="running")},
        {
            "type": "message",
            "data": _progress_event_bytes(seq=2, status="succeeded", terminal=True),
        },
    ]
    pubsub = _make_pubsub_get_message_mock(messages)
    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    events: list[JobEvent] = []
    async for event in _stream_redis(
        redis_client,
        _SCHEMA_LABEL,
        cast(JobId, _JOB_ID),
        client,
        30.0,
    ):
        events.append(event)

    assert len(events) == 2
    assert events[1].terminal is True


async def test_stream_redis_timeout_triggers_re_fetch() -> None:
    """When get_message returns None (timeout), the backend is re-fetched
    and a state change is yielded if detected.
    """
    row_running = _row(status="running", progress_seq=1)
    row_succeeded = _row(status="succeeded", progress_seq=2)
    backend = _stub_backend(rows=[row_running, row_succeeded])
    client = _make_client(backend)

    messages: list[dict[str, object] | None] = [
        None,
    ]
    pubsub = _make_pubsub_get_message_mock(messages)
    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    events: list[JobEvent] = []
    async for event in _stream_redis(
        redis_client,
        _SCHEMA_LABEL,
        cast(JobId, _JOB_ID),
        client,
        0.01,
    ):
        events.append(event)

    assert len(events) >= 1
    assert events[0].status == "running"


async def test_stream_redis_skips_data_none() -> None:
    """Messages with data=None are skipped without error."""
    row_running = _row(status="running", progress_seq=1)
    row_succeeded = _row(status="succeeded", progress_seq=2)
    backend = _stub_backend(rows=[row_running, row_succeeded])
    client = _make_client(backend)

    messages: list[dict[str, object] | None] = [
        {"type": "message", "data": None},
        {"type": "message", "data": _progress_event_bytes(seq=1, status="running")},
        {
            "type": "message",
            "data": _progress_event_bytes(seq=2, status="succeeded", terminal=True),
        },
    ]
    pubsub = _make_pubsub_get_message_mock(messages)
    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    events: list[JobEvent] = []
    async for event in _stream_redis(
        redis_client,
        _SCHEMA_LABEL,
        cast(JobId, _JOB_ID),
        client,
        30.0,
    ):
        events.append(event)

    assert len(events) == 2


async def test_stream_redis_no_duplicate_on_same_state() -> None:
    """When a Redis message arrives but backend.get() returns unchanged state,
    no event is yielded for that message. Only the state change from the
    initial sentinel triggers the first yield.
    """
    row_running = _row(status="running", progress_seq=1)
    row_running_2 = _row(status="running", progress_seq=1)
    row_succeeded = _row(status="succeeded", progress_seq=2)
    backend = _stub_backend(rows=[row_running, row_running_2, row_succeeded])
    client = _make_client(backend)

    messages: list[dict[str, object] | None] = [
        {"type": "message", "data": _progress_event_bytes(seq=1, status="running")},
        {"type": "message", "data": _progress_event_bytes(seq=1, status="running")},
        {
            "type": "message",
            "data": _progress_event_bytes(seq=2, status="succeeded", terminal=True),
        },
    ]
    pubsub = _make_pubsub_get_message_mock(messages)
    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    events: list[JobEvent] = []
    async for event in _stream_redis(
        redis_client,
        _SCHEMA_LABEL,
        cast(JobId, _JOB_ID),
        client,
        30.0,
    ):
        events.append(event)

    assert len(events) == 2
    assert events[0].status == "running"
    assert events[0].terminal is False
    assert events[1].status == "succeeded"
    assert events[1].terminal is True


async def test_stream_redis_cleanup_on_terminal() -> None:
    """Pubsub unsubscribe and aclose are called in finally on terminal."""
    row_succeeded = _row(status="succeeded", progress_seq=1)
    backend = _stub_backend(rows=[row_succeeded])
    client = _make_client(backend)

    messages: list[dict[str, object] | None] = [
        {
            "type": "message",
            "data": _progress_event_bytes(seq=1, status="succeeded", terminal=True),
        },
    ]
    pubsub = _make_pubsub_get_message_mock(messages)
    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    events: list[JobEvent] = []
    async for event in _stream_redis(
        redis_client,
        _SCHEMA_LABEL,
        cast(JobId, _JOB_ID),
        client,
        30.0,
    ):
        events.append(event)

    assert events[0].terminal is True
    pubsub.unsubscribe.assert_awaited_once()
    pubsub.aclose.assert_awaited_once()


async def test_stream_no_duplicate_initial_snapshot_via_redis() -> None:
    """stream() must not yield a duplicate of the initial snapshot when
    delegating to a transport helper. The initial row is yielded once
    by stream() itself; the transport helper must seed its dedup state
    from the already-yielded row so the first re-fetch does not
    produce a duplicate.
    """
    row_running = _row(status="running", progress_seq=0)
    row_succeeded = _row(status="succeeded", progress_seq=1)
    backend = _stub_backend(rows=[row_running, row_succeeded])
    client = _make_client(backend, redis_client=MagicMock(spec=["pubsub"]))

    messages: list[dict[str, object] | None] = [
        {"type": "message", "data": _progress_event_bytes(seq=0, status="running")},
        {
            "type": "message",
            "data": _progress_event_bytes(seq=1, status="succeeded", terminal=True),
        },
    ]
    pubsub = _make_pubsub_get_message_mock(messages)
    redis_mock = client._redis_client
    assert redis_mock is not None
    redis_mock.pubsub.return_value = pubsub  # type: ignore[reportAttributeAccessIssue] # Why: MagicMock method attribute assignment for test-only stub

    tq = TaskQ.__new__(TaskQ)
    tq._client = client
    tq._redis_client = client._redis_client
    tq._dsn = None
    tq._schema = _SCHEMA_LABEL
    tq._poll_timeout = 30.0

    events: list[JobEvent] = []
    async for event in tq.stream(cast(JobId, _JOB_ID)):
        events.append(event)

    assert len(events) == 2
    assert events[0].status == "running"
    assert events[0].progress_seq == 0
    assert events[1].status == "succeeded"
    assert events[1].terminal is True
