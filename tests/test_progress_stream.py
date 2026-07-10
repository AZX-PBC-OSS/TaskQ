"""Unit tests for JobHandle.progress_stream().

Covers:
- InMemoryBackend raises NotImplementedError
- PG fallback path: yields progress and state_change events, stops on terminal
- Redis pub/sub path: yields events from channel, skips subscribe confirmations,
  discards malformed messages, stops on terminal
- JobsClient lifecycle: _open_redis, close, redis_client passthrough to JobHandle
"""

import dataclasses
from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import TypeAdapter

from taskq.backend._protocol import Backend, JobId, JobRow, JobStatus
from taskq.client._handle import JobHandle
from taskq.progress._events import ProgressEvent
from taskq.settings import TaskQSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row

_RA = TypeAdapter(type(None))

_SCHEMA_LABEL = "taskq_test"
_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000001")
_ACTOR = "test_actor"

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _row(
    *,
    status: JobStatus = "running",
    progress_seq: int = 0,
    job_id: UUID = _JOB_ID,
) -> JobRow:
    row = make_job_row(
        status=status,
        progress_seq=progress_seq,
        actor=_ACTOR,
    )
    return dataclasses.replace(row, id=cast(JobId, job_id))


def _handle_from_backend(
    backend: Backend,
    *,
    row: JobRow | None = None,
    redis_client: object | None = None,
    settings: TaskQSettings | None = None,
) -> JobHandle[None]:
    return JobHandle(
        backend=backend,
        row=row or _row(),
        result_adapter=_RA,
        was_existing=False,
        _redis_client=redis_client,
        _settings=settings,
    )


def _handle_with_client(  # pyright: ignore[reportUnusedFunction] # Why: test helper called by parametrized test cases via fixtures.
    backend: Backend,
    *,
    row: JobRow | None = None,
    redis_client: object | None = None,
    settings: TaskQSettings | None = None,
) -> JobHandle[None]:
    from taskq.client._jobs import JobsClient

    client = JobsClient(backend, settings=settings)
    if redis_client is not None:
        client._redis_client = redis_client  # type: ignore[assignment] # Why: test-only injection of mock redis client
    return JobHandle(
        client=client,
        row=row or _row(),
        result_adapter=_RA,
        was_existing=False,
        _redis_client=redis_client,
        _settings=settings,
    )


def _stub_backend(
    *,
    rows: list[JobRow],
) -> Backend:
    """Build a stub Backend where ``get`` returns successive rows.

    Each call to ``get`` pops from the front of *rows*. All other
    methods raise ``NotImplementedError`` — only ``get`` is needed
    for the PG fallback path.
    """
    remaining = list(rows)
    backend = AsyncMock(spec=Backend)

    async def _get(job_id: JobId) -> JobRow | None:
        if remaining:
            return remaining.pop(0)
        return None

    backend.get = _get
    return backend


# ── InMemoryBackend raises NotImplementedError ────────────────────────────


async def test_progress_stream_in_memory_raises() -> None:
    backend = InMemoryBackend(clock=FakeClock(_START))
    handle = _handle_from_backend(backend)

    with pytest.raises(NotImplementedError, match="progress_stream requires Redis"):
        async for _ in handle.progress_stream():
            pass


# ── PG fallback path ────────────────────────────────────────────────────


async def test_pg_fallback_yields_state_change_on_status_change() -> None:
    row_running = _row(status="running", progress_seq=0, job_id=_JOB_ID)
    row_succeeded = _row(status="succeeded", progress_seq=1, job_id=_JOB_ID)
    backend = _stub_backend(rows=[row_running, row_succeeded])
    handle = _handle_from_backend(backend, row=_row(status="running", job_id=_JOB_ID))

    events: list[ProgressEvent] = []
    async for event in handle.progress_stream():
        events.append(event)

    assert len(events) >= 1
    final = events[-1]
    assert final.kind == "state_change"
    assert final.terminal is True
    assert final.status == "succeeded"


async def test_pg_fallback_yields_progress_on_seq_change() -> None:
    row_running_0 = _row(status="running", progress_seq=0, job_id=_JOB_ID)
    row_running_1 = _row(status="running", progress_seq=1, job_id=_JOB_ID)
    row_succeeded = _row(status="succeeded", progress_seq=2, job_id=_JOB_ID)
    backend = _stub_backend(rows=[row_running_0, row_running_1, row_succeeded])
    handle = _handle_from_backend(backend, row=_row(status="running", job_id=_JOB_ID))

    events: list[ProgressEvent] = []
    async for event in handle.progress_stream():
        events.append(event)

    kinds = [e.kind for e in events]
    assert "progress" in kinds
    final = events[-1]
    assert final.terminal is True
    assert final.kind == "state_change"


async def test_pg_fallback_stops_on_terminal() -> None:
    row_succeeded = _row(status="succeeded", progress_seq=0, job_id=_JOB_ID)
    backend = _stub_backend(rows=[row_succeeded])
    handle = _handle_from_backend(backend, row=_row(status="succeeded", job_id=_JOB_ID))

    events: list[ProgressEvent] = []
    async for event in handle.progress_stream():
        events.append(event)

    assert len(events) == 1
    assert events[0].terminal is True
    assert events[0].status == "succeeded"


async def test_pg_fallback_returns_when_job_disappears() -> None:
    backend = _stub_backend(rows=[])
    handle = _handle_from_backend(backend, row=_row(status="running", job_id=_JOB_ID))

    events: list[ProgressEvent] = []
    async for event in handle.progress_stream():
        events.append(event)

    assert len(events) == 0


# ── Redis pub/sub path ──────────────────────────────────────────────────


def _make_pubsub_mock(messages: list[dict[str, object]]) -> AsyncMock:
    pubsub = AsyncMock()
    _remaining = list(messages)

    async def _get_message(
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109
    ) -> dict[str, object] | None:
        if _remaining:
            return _remaining.pop(0)
        return None

    pubsub.get_message = _get_message
    pubsub.listen = AsyncMock()
    pubsub.subscribe = AsyncMock()
    pubsub.unsubscribe = AsyncMock()
    pubsub.__aenter__ = AsyncMock(return_value=pubsub)
    pubsub.__aexit__ = AsyncMock(return_value=False)
    return pubsub


def _progress_event_bytes(
    *,
    seq: int = 1,
    kind: str = "progress",
    status: str = "running",
    terminal: bool = False,
    step: int | None = None,
    percent: float | None = None,
    detail: str | None = None,
) -> bytes:
    event = ProgressEvent(
        kind=kind,  # type: ignore[arg-type] # Why: test-only construction with known-valid values
        job_id=_JOB_ID,
        actor=_ACTOR,
        ts=datetime.now(UTC),
        seq=seq,
        status=status,
        step=step,
        percent=percent,
        detail=detail,
        terminal=terminal,
    )
    return event.model_dump_json(exclude_none=True).encode("utf-8")


async def test_redis_yields_progress_events() -> None:
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    backend = _stub_backend(rows=[])
    row = _row(status="running", progress_seq=0, job_id=_JOB_ID)

    messages = [
        {"type": "subscribe", "data": None, "channel": f"taskq:{_SCHEMA_LABEL}:progress:{_JOB_ID}"},
        {
            "type": "message",
            "data": _progress_event_bytes(seq=1, kind="progress", status="running"),
        },
        {
            "type": "message",
            "data": _progress_event_bytes(seq=2, kind="progress", status="running", step=1),
        },
        {
            "type": "message",
            "data": _progress_event_bytes(
                seq=3, kind="state_change", status="succeeded", terminal=True
            ),
        },
    ]
    pubsub = _make_pubsub_mock(messages)

    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    handle = _handle_from_backend(backend, row=row, redis_client=redis_client, settings=settings)

    events: list[ProgressEvent] = []
    async for event in handle.progress_stream():
        events.append(event)

    assert len(events) == 3
    assert events[0].seq == 1
    assert events[0].kind == "progress"
    assert events[1].seq == 2
    assert events[1].step == 1
    assert events[2].kind == "state_change"
    assert events[2].terminal is True


async def test_redis_skips_subscribe_confirmation() -> None:
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    backend = _stub_backend(rows=[])
    row = _row(status="running", progress_seq=0, job_id=_JOB_ID)

    messages = [
        {"type": "subscribe", "data": None, "channel": f"taskq:{_SCHEMA_LABEL}:progress:{_JOB_ID}"},
        {
            "type": "message",
            "data": _progress_event_bytes(seq=1, kind="progress", status="running"),
        },
        {
            "type": "message",
            "data": _progress_event_bytes(
                seq=2, kind="state_change", status="succeeded", terminal=True
            ),
        },
    ]
    pubsub = _make_pubsub_mock(messages)

    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    handle = _handle_from_backend(backend, row=row, redis_client=redis_client, settings=settings)

    events: list[ProgressEvent] = []
    async for event in handle.progress_stream():
        events.append(event)

    assert len(events) == 2


async def test_redis_discards_malformed_messages() -> None:
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    backend = _stub_backend(rows=[])
    row = _row(status="running", progress_seq=0, job_id=_JOB_ID)

    messages = [
        {"type": "subscribe", "data": None, "channel": f"taskq:{_SCHEMA_LABEL}:progress:{_JOB_ID}"},
        {"type": "message", "data": b"not valid json"},
        {
            "type": "message",
            "data": _progress_event_bytes(seq=1, kind="progress", status="running"),
        },
        {
            "type": "message",
            "data": _progress_event_bytes(
                seq=2, kind="state_change", status="succeeded", terminal=True
            ),
        },
    ]
    pubsub = _make_pubsub_mock(messages)

    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    handle = _handle_from_backend(backend, row=row, redis_client=redis_client, settings=settings)

    events: list[ProgressEvent] = []
    async for event in handle.progress_stream():
        events.append(event)

    assert len(events) == 2


async def test_redis_deduplicates_by_seq() -> None:
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    backend = _stub_backend(rows=[])
    row = _row(status="running", progress_seq=0, job_id=_JOB_ID)

    messages = [
        {"type": "subscribe", "data": None, "channel": f"taskq:{_SCHEMA_LABEL}:progress:{_JOB_ID}"},
        {
            "type": "message",
            "data": _progress_event_bytes(seq=1, kind="progress", status="running"),
        },
        {
            "type": "message",
            "data": _progress_event_bytes(seq=1, kind="progress", status="running"),
        },
        {
            "type": "message",
            "data": _progress_event_bytes(
                seq=2, kind="state_change", status="succeeded", terminal=True
            ),
        },
    ]
    pubsub = _make_pubsub_mock(messages)

    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    handle = _handle_from_backend(backend, row=row, redis_client=redis_client, settings=settings)

    events: list[ProgressEvent] = []
    async for event in handle.progress_stream():
        events.append(event)

    assert len(events) == 2
    assert events[0].seq == 1
    assert events[1].seq == 2


async def test_redis_state_change_not_deduplicated_by_seq() -> None:
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    backend = _stub_backend(rows=[])
    row = _row(status="running", progress_seq=0, job_id=_JOB_ID)

    messages = [
        {"type": "subscribe", "data": None, "channel": f"taskq:{_SCHEMA_LABEL}:progress:{_JOB_ID}"},
        {
            "type": "message",
            "data": _progress_event_bytes(seq=1, kind="progress", status="running"),
        },
        {
            "type": "message",
            "data": _progress_event_bytes(seq=1, kind="state_change", status="running"),
        },
        {
            "type": "message",
            "data": _progress_event_bytes(
                seq=2, kind="state_change", status="succeeded", terminal=True
            ),
        },
    ]
    pubsub = _make_pubsub_mock(messages)

    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    handle = _handle_from_backend(backend, row=row, redis_client=redis_client, settings=settings)

    events: list[ProgressEvent] = []
    async for event in handle.progress_stream():
        events.append(event)

    assert len(events) == 3
    assert events[0].kind == "progress"
    assert events[1].kind == "state_change"
    assert events[2].kind == "state_change"


# ── JobsClient lifecycle ────────────────────────────────────────────────


async def test_jobs_client_close_no_redis() -> None:
    from taskq.client._jobs import JobsClient

    backend = InMemoryBackend(clock=FakeClock(_START))
    client = JobsClient(backend)
    await client.close()


async def test_jobs_client_open_redis_none_when_no_url() -> None:
    from taskq.client._jobs import JobsClient

    backend = InMemoryBackend(clock=FakeClock(_START))
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    client = JobsClient(backend, settings=settings)
    await client._open_redis(settings)

    assert client._redis_client is None
    await client.close()


async def test_jobs_client_enqueue_passes_redis_and_settings_to_handle() -> None:
    from taskq.client._jobs import JobsClient

    backend = InMemoryBackend(clock=FakeClock(_START))
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    client = JobsClient(backend, settings=settings)

    row = _row(status="pending")
    handle = JobHandle(
        client=client,
        row=row,
        result_adapter=_RA,
        was_existing=False,
        _redis_client=client._redis_client,
        _settings=client._settings,
    )
    assert handle._redis_client is None
    assert handle._handle_settings == settings
    await client.close()
