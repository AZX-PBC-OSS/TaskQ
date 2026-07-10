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
"""

import dataclasses
from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import TypeAdapter

from taskq.backend._protocol import Backend, JobId, JobRow, JobStatus
from taskq.client._jobs import JobsClient
from taskq.client._taskq import JobEvent, TaskQ, _row_to_event, _stream_redis
from taskq.progress._events import ProgressEvent
from taskq.settings import TaskQSettings
from taskq.testing.jobs import make_job_row

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
    """PG LISTEN transport raises RuntimeError when dsn is None
    (pool-only construction).
    """
    row = _row(status="running", progress_seq=0)
    backend = _stub_backend(rows=[row])
    client = _make_client(backend)

    tq = TaskQ.__new__(TaskQ)
    tq._client = client
    tq._redis_client = None
    tq._dsn = None
    tq._schema = _SCHEMA_LABEL
    tq._poll_timeout = 30.0

    with pytest.raises(RuntimeError, match="DSN"):
        async for _ in tq.stream(cast(JobId, _JOB_ID)):
            pass


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
