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
- Bounded owned-LISTEN-conn closes: _stream_pg/_watch_reclaims_pg
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
from taskq.client._transport import pg_poll_event_stream
from taskq.exceptions import StreamUnavailable
from taskq.progress._events import ProgressEvent
from taskq.settings import TaskQSettings
from taskq.testing.assertions import wait_for
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


def _timed_stub_backend(rows: list[JobRow]) -> tuple[Backend, list[float]]:
    """Stub Backend whose ``get`` returns successive rows and records the loop
    time of every call - the observable for "how often does the client hit
    the database"."""
    remaining = list(rows)
    fetched_at: list[float] = []
    backend = AsyncMock(spec=Backend)

    async def _get(job_id: JobId) -> JobRow | None:
        fetched_at.append(asyncio.get_running_loop().time())
        return remaining.pop(0) if remaining else None

    backend.get = _get
    return backend, fetched_at


def _pool_only_taskq(client: JobsClient, *, poll_timeout: float) -> TaskQ:
    """A TaskQ built on a caller-owned pool: no DSN, no LISTEN source."""
    tq = TaskQ.__new__(TaskQ)
    tq._client = client
    tq._redis_client = None
    tq._dsn = None
    tq._pg_conn_factory = None
    tq._listen_conn = None
    tq._schema = _SCHEMA_LABEL
    tq._poll_timeout = poll_timeout
    return tq


async def test_stream_pg_streams_in_pool_only_mode() -> None:
    """The Postgres transport reads the job row through the client's own
    pool, so a pool-only TaskQ (no DSN, no ``pg_conn_factory`` /
    ``listen_conn``) streams like any other."""
    rows = [_row(status="running", progress_seq=0), _row(status="succeeded", progress_seq=1)]
    backend, _ = _timed_stub_backend(rows)
    tq = _pool_only_taskq(_make_client(backend), poll_timeout=0.05)

    events = [e async for e in tq.stream(cast(JobId, _JOB_ID))]

    assert [e.status for e in events] == ["running", "succeeded"]
    assert events[-1].terminal is True


async def test_stream_pg_gives_up_after_the_failure_budget_with_its_cause() -> None:
    """Failures that span the budget are not a blip: the stream ends with
    StreamUnavailable naming the job, the run length and the last error,
    instead of polling a dead database forever behind warnings."""
    from taskq.client import _transport

    readings = iter([0.0, 12.0, 24.0, 31.0])

    async def _fetch_row() -> JobRow:
        raise asyncpg.InterfaceError("connection closed")

    with structlog.testing.capture_logs() as captured, pytest.raises(StreamUnavailable) as info:
        async for _ in pg_poll_event_stream(
            _fetch_row,
            lambda row, _status_changed: _row_to_event(row),
            job_id=cast(JobId, _JOB_ID),
            poll_interval=_transport.POLL_INTERVAL_FLOOR_SECS,
            failure_budget=30.0,
            clock=lambda: next(readings),
        ):
            pytest.fail("no event can be produced by a fetch that always fails")

    exc = info.value
    assert exc.job_id == _JOB_ID
    assert exc.consecutive_failures == 4
    assert exc.elapsed == 31.0
    assert isinstance(exc.__cause__, asyncpg.InterfaceError)
    assert "InterfaceError" in str(exc)
    assert [e["event"] for e in captured] == ["stream-poll-error"] * 3 + ["stream-poll-abandoned"]
    abandoned = captured[-1]
    assert abandoned["job_id"] == str(_JOB_ID)
    assert abandoned["consecutive_failures"] == 4
    assert abandoned["elapsed_secs"] == 31.0
    assert "error" not in abandoned


async def test_stream_pg_failure_budget_resets_on_a_successful_read() -> None:
    """A successful read ends the failure run: two runs each shorter than
    the budget never add up to it, even when their total does."""
    from taskq.client import _transport

    readings = iter([0.0, 20.0, 100.0, 120.0])
    calls = {"n": 0}
    rows = [_row(status="running", progress_seq=1), _row(status="succeeded", progress_seq=2)]

    async def _fetch_row() -> JobRow:
        calls["n"] += 1
        if calls["n"] in (1, 2, 4, 5):
            raise OSError("reset")
        return rows.pop(0)

    events = [
        event
        async for event in pg_poll_event_stream(
            _fetch_row,
            lambda row, _status_changed: _row_to_event(row),
            job_id=cast(JobId, _JOB_ID),
            poll_interval=_transport.POLL_INTERVAL_FLOOR_SECS,
            failure_budget=30.0,
            clock=lambda: next(readings),
        )
    ]
    assert [e.status for e in events] == ["running", "succeeded"]


async def test_stream_pg_non_infra_errors_propagate_unchanged() -> None:
    """Only pool and connection failures are retried; a KeyError from the
    row fetch (TaskQ.stream's vanished-row contract) or a programming
    error propagates immediately."""

    async def _fetch_row() -> JobRow:
        raise KeyError(_JOB_ID)

    with pytest.raises(KeyError):
        async for _ in pg_poll_event_stream(
            _fetch_row,
            lambda row, _status_changed: _row_to_event(row),
            job_id=cast(JobId, _JOB_ID),
            poll_interval=0.01,
        ):
            pytest.fail("unreachable")


async def test_stream_pg_poll_interval_is_floored_and_jittered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A caller asking for a 1 ms cadence gets the transport's floor, and
    each wait is spread over ±20% of it so streams do not poll in lockstep."""
    from taskq.client import _transport

    waits: list[float] = []

    async def _recording_sleep(delay: float, result: object = None) -> object:
        waits.append(delay)
        return result

    monkeypatch.setattr(asyncio, "sleep", _recording_sleep)
    rows = [_row(status="running", progress_seq=n) for n in range(1, 40)]
    rows.append(_row(status="succeeded", progress_seq=40))

    async def _fetch_row() -> JobRow:
        return rows.pop(0)

    events = [
        event
        async for event in pg_poll_event_stream(
            _fetch_row,
            lambda row, _status_changed: _row_to_event(row),
            job_id=cast(JobId, _JOB_ID),
            poll_interval=0.001,
        )
    ]
    assert events[-1].terminal is True
    floor = _transport.POLL_INTERVAL_FLOOR_SECS
    lo, hi = (
        floor * (1 - _transport.POLL_JITTER_FRACTION),
        floor * (1 + _transport.POLL_JITTER_FRACTION),
    )
    assert len(waits) == 40
    assert all(lo <= w <= hi for w in waits), waits
    assert len(set(waits)) > 1, "every wait was identical: no jitter"


async def test_stream_pg_fetches_once_before_the_first_wait() -> None:
    """The row fetched to produce the initial snapshot is the one the
    transport starts from: the next read of the database happens only after
    the first poll interval, never back-to-back with the snapshot."""
    rows = [_row(status="running", progress_seq=0), _row(status="succeeded", progress_seq=1)]
    backend, fetched_at = _timed_stub_backend(rows)
    tq = _pool_only_taskq(_make_client(backend), poll_timeout=0.2)

    events = [e async for e in tq.stream(cast(JobId, _JOB_ID))]

    assert events[-1].terminal is True
    assert len(fetched_at) == 2
    assert fetched_at[1] - fetched_at[0] >= 0.15, fetched_at


async def test_stream_pg_observes_a_change_within_one_second_by_default() -> None:
    """Nothing on the Postgres transport announces a job's progress or
    terminal write, so the poll cadence IS the observation latency: with the
    default ``poll_timeout`` a change is seen within a second, not thirty."""
    rows = [_row(status="running", progress_seq=0), _row(status="succeeded", progress_seq=1)]
    backend, fetched_at = _timed_stub_backend(rows)
    tq = _pool_only_taskq(_make_client(backend), poll_timeout=30.0)

    async with asyncio.timeout(5):
        events = [e async for e in tq.stream(cast(JobId, _JOB_ID))]

    assert events[-1].terminal is True
    assert fetched_at[1] - fetched_at[0] < 1.0, fetched_at


async def test_stream_pg_opens_no_dedicated_connection() -> None:
    """A LISTEN source configured for ``watch_reclaims`` is not consumed by
    ``stream()``: the transport holds no connection of its own, so a page of
    streaming viewers costs no Postgres sessions beyond the pool."""
    rows = [_row(status="running", progress_seq=0), _row(status="succeeded", progress_seq=1)]
    backend, _ = _timed_stub_backend(rows)
    factory_calls = 0

    async def factory() -> "asyncpg.Connection":
        nonlocal factory_calls
        factory_calls += 1
        raise AssertionError("stream() must not open a dedicated connection")

    tq = _pool_only_taskq(_make_client(backend), poll_timeout=0.05)
    tq._pg_conn_factory = factory

    events = [e async for e in tq.stream(cast(JobId, _JOB_ID))]

    assert events[-1].terminal is True
    assert factory_calls == 0


async def test_taskq_init_rejects_pg_conn_factory_and_listen_conn() -> None:
    """TaskQ.__init__ rejects providing both pg_conn_factory and listen_conn."""
    with pytest.raises(ValueError, match=r"pg_conn_factory.*listen_conn"):
        TaskQ(pool=object(), pg_conn_factory=lambda: None, listen_conn=object())  # type: ignore[arg-type]


# ── PG transport: transient poll errors ──────────────────────────────────
#
# The poll transport re-reads the row through the client's pool, so a pool
# blip (connection reset, pool-acquire refusal) surfaces as an exception
# out of the fetch. The stream must survive it the way the LISTEN
# transport did, not kill the caller's async for.


async def test_stream_pg_survives_a_transient_poll_error_and_recovers() -> None:
    """A pool or connection error on one poll does not end the stream: the
    failure is logged once, the loop retries after the next interval, and
    the terminal event still arrives."""
    rows = [_row(status="succeeded", progress_seq=1)]
    calls = {"n": 0}

    async def _fetch_row() -> JobRow:
        calls["n"] += 1
        if calls["n"] == 1:
            raise asyncpg.InterfaceError("connection closed")
        return rows.pop(0)

    with structlog.testing.capture_logs() as captured:
        events = [
            event
            async for event in pg_poll_event_stream(
                _fetch_row,
                lambda row, _status_changed: _row_to_event(row),
                job_id=cast(JobId, _JOB_ID),
                poll_interval=0.01,
            )
        ]

    assert [e.status for e in events] == ["succeeded"]
    assert events[-1].terminal is True
    blips = [e for e in captured if e["event"] == "stream-poll-error"]
    assert len(blips) == 1
    assert blips[0]["job_id"] == str(_JOB_ID)
    assert blips[0]["error_type"] == "InterfaceError"
    # The exception's message (and the exception object itself) never reach
    # the log: server error text can quote row data.
    assert "error" not in blips[0]


async def test_stream_pg_poll_errors_never_escape_the_generator() -> None:
    """Consecutive blips are all survived: no exception escapes to the
    caller's async for, and events resume once the fetch does."""
    rows = [_row(status="running", progress_seq=0), _row(status="succeeded", progress_seq=1)]
    calls = {"n": 0}

    async def _fetch_row() -> JobRow:
        calls["n"] += 1
        if calls["n"] <= 3:
            raise OSError("connection reset by peer")
        return rows.pop(0)

    events = [
        event
        async for event in pg_poll_event_stream(
            _fetch_row,
            lambda row, _status_changed: _row_to_event(row),
            job_id=cast(JobId, _JOB_ID),
            poll_interval=0.01,
        )
    ]

    assert [e.status for e in events] == ["running", "succeeded"]
    assert events[-1].terminal is True


# ── Bounded owned-LISTEN-conn closes ─────────────────────────────────────
#
# asyncpg's Connection.close() passes no timeout underneath, so against a
# dead PG it can hang forever — contextlib.suppress(Exception) catches
# errors but cannot stop a call that never returns. These tests pin that
# every TaskQ-owned LISTEN-conn close in _watch_reclaims_pg (teardown AND
# the reconnect error paths) goes through close_conn_bounded: after the
# bound the conn is terminated and the surrounding flow continues.
# CLOSE_TIMEOUT_SECS is shrunk via the module-global monkeypatch seam
# (read at call time).


# ── watch_reclaims: bounded owned-conn closes ────────────────────────────
#
# The minimal _watch_reclaims_pg harness helpers are replicated from
# tests/test_watch_reclaims.py.


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
        # Why an event alongside the channel list: the list is the
        # assertion surface; the event is the WAIT surface — the watch
        # generator registers LISTEN on its own task, and a test that
        # needs "LISTEN registered" can await this instead of sleeping a
        # fixed interval that races the generator's startup under load.
        self.listening = asyncio.Event()
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
        self.listening.set()

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
            # Event-driven gate instead of a fixed 0.05s sleep: one
            # deterministic yield runs the generator's first step (the
            # factory call completes inline, so conns[0] exists), and the
            # conn fires `listening` exactly at its LISTEN registration —
            # killing before that point would take a different failure
            # path than the one under test.
            await asyncio.sleep(0)
            await wait_for(conns[0].listening, timeout=5.0)
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
    assert any(e["event"] == "watch-reclaims-reconnect-still-failing" for e in captured)


async def test_watch_reclaims_reconnect_swap_bounds_hung_old_conn_close(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reconnect-swap close (mid-run): the OLD conn being swapped out was
    already diagnosed dead — the sharpest close()-hang case. The bounded
    close terminates it (not leaked), the swap completes, and the generator
    logs 'watch-reclaims-listen-reconnected' and resumes LISTEN-driven
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
            # Event-driven gate instead of a fixed 0.05s sleep: one
            # deterministic yield runs the generator's first step (the
            # factory call completes inline, so conns[0] exists), and the
            # conn fires `listening` exactly at its LISTEN registration —
            # killing before that point would take a different failure
            # path than the one under test.
            await asyncio.sleep(0)
            await wait_for(conns[0].listening, timeout=5.0)
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
    assert any(e["event"] == "watch-reclaims-listen-reconnected" for e in captured)


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


# ── orjson-backed JSON response class ───────────────────────────────────────

pytest.importorskip("starlette")

import json as _stdlib_json  # noqa: E402  # Why: test-only import — the byte-equality oracle for the response-class contract

from starlette.responses import JSONResponse  # noqa: E402


def test_orjson_response_class_is_json_response_subclass() -> None:
    """orjson_response_class() returns a cached JSONResponse subclass — a
    drop-in replacement for starlette's stdlib-json JSONResponse in FastAPI
    routes."""
    from taskq.client._taskq import orjson_response_class

    cls = orjson_response_class()
    assert issubclass(cls, JSONResponse)
    assert cls is orjson_response_class(), "the class must be cached, not rebuilt per call"


def test_orjson_response_body_byte_equal_to_stdlib_for_json_safe_payloads() -> None:
    """For every JSON-representable value the orjson-backed render() is
    byte-identical to starlette's stdlib-json render() — same body bytes,
    same application/json content-type — so swapping the class in cannot
    change what HTTP clients see."""
    from taskq.client._taskq import orjson_response_class

    payloads: list[object] = [
        {"k": "héllo ⟨日本⟩ 🎉", "n": None, "b": True, "f": False},
        {"nested": {"list": [1, 2.5, -0.0, 1e30, "", [], {}]}},
        {"unicode_key_é": 'value\twith"escapes\\and/chars'},
        {"empty": {}},
        [],
        "top-level string",
        None,
    ]
    cls = orjson_response_class()
    for content in payloads:
        ours = cls(content)
        theirs = JSONResponse(content)
        assert ours.body == theirs.body, f"body mismatch for {content!r}"
        assert ours.media_type == "application/json"
        assert ours.headers["content-type"] == "application/json"


def test_orjson_response_body_matches_taskq_json_dumps() -> None:
    """render() output is exactly taskq._json.dumps output — the project
    rule that serialization flows through the orjson-backed helper, never
    stdlib json."""
    from taskq._json import dumps as taskq_dumps
    from taskq.client._taskq import orjson_response_class

    cls = orjson_response_class()
    content = {"job_id": "abc", "progress_state": {"pct": 50}, "terminal": False}
    assert cls(content).body == taskq_dumps(content)
    assert isinstance(cls(content).body, bytes)


def test_orjson_response_renders_datetime_instead_of_raising() -> None:
    """Datetimes (a realistic progress_state value) serialize to ISO-8601
    instead of raising TypeError the way stdlib json.dumps does."""
    from datetime import UTC, datetime

    from taskq.client._taskq import orjson_response_class

    cls = orjson_response_class()
    body = cls({"at": datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)}).body
    assert body == b'{"at":"2025-01-01T12:00:00Z"}'
    # And the stdlib baseline really cannot do this — the divergence is the point.
    with pytest.raises(TypeError):
        JSONResponse({"at": datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)})


def test_orjson_response_render_routes_through_taskq_json() -> None:
    """render() must not fall back to stdlib json: its output for a
    non-ASCII payload equals the taskq._json form (raw UTF-8, compact
    separators), which is byte-identical to stdlib's ensure_ascii=False
    form — the contract the byte-equality test above pins."""
    import taskq.client._taskq as taskq_module

    content: dict[str, object] = {"k": "héllo"}
    rendered = taskq_module.orjson_response_class().render(
        None,
        content,  # type: ignore[arg-type]  # Why: unbound call to exercise render() without __init__'s own render invocation
    )
    assert rendered == _stdlib_json.dumps(
        content, ensure_ascii=False, allow_nan=False, separators=(",", ":")
    ).encode("utf-8")


# ── Deserialise errors never carry the payload ──────────────────────────


def test_parse_progress_event_logs_locations_and_count_not_the_payload() -> None:
    """A message that fails validation is reported by the failing field
    locations and their count. pydantic's ValidationError repr embeds
    input_value - the message payload, a user's own progress data - and
    that must never reach the log."""
    from taskq.client._transport import parse_progress_event

    raw = '{"kind":"progress","seq":"SENTINEL-PAYLOAD","status":"running"}'
    with structlog.testing.capture_logs() as logs:
        assert parse_progress_event(raw, job_id=cast(JobId, _JOB_ID)) is None
    entry = next(log for log in logs if log["event"] == "stream-event-deserialise-error")
    assert entry["error_type"] == "ValidationError"
    assert entry["error_count"] == len(entry["locations"])
    assert "seq" in entry["locations"]
    assert "job_id" in entry["locations"]
    assert "SENTINEL-PAYLOAD" not in repr(entry)
    assert "input_value" not in repr(entry)


def test_parse_progress_event_reports_invalid_json_as_one_error() -> None:
    from taskq.client._transport import parse_progress_event

    with structlog.testing.capture_logs() as logs:
        assert parse_progress_event("not json {", job_id=cast(JobId, _JOB_ID)) is None
    entry = next(log for log in logs if log["event"] == "stream-event-deserialise-error")
    assert entry["error_count"] == 1
    assert "not json" not in repr(entry)
