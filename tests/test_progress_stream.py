"""Unit tests for JobHandle.progress_stream().

Covers:
- InMemoryBackend raises NotImplementedError
- PG fallback path: yields progress and state_change events, stops on terminal
- Redis pub/sub path: yields events from channel, skips subscribe confirmations,
  discards malformed messages, stops on terminal
- Redis path snapshot replay: a live row's current state is the FIRST event
- redis_event_stream's on_timeout failure budget: infra errors are retried
  inside a 30 s budget, non-infra errors propagate untouched
- the same failure budget on the decode_message leg (one shared run:
  either leg's infra failures spend it, either leg's success resets it)
- JobsClient lifecycle: _open_redis, close, redis_client passthrough to JobHandle
"""

import asyncio
import dataclasses
from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import asyncpg
import pytest
import structlog.testing
from pydantic import TypeAdapter

from taskq.backend._protocol import Backend, JobId, JobRow, JobStatus
from taskq.client._handle import JobHandle
from taskq.exceptions import StreamUnavailable
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
    methods raise ``NotImplementedError`` - only ``get`` is needed
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
    """A row that vanishes mid-stream ends the stream without a terminal
    event - and says so: the operator sees which job went missing rather
    than a stream that quietly stopped."""
    backend = _stub_backend(rows=[])
    handle = _handle_from_backend(backend, row=_row(status="running", job_id=_JOB_ID))

    events: list[ProgressEvent] = []
    with structlog.testing.capture_logs() as logs:
        async for event in handle.progress_stream():
            events.append(event)

    assert len(events) == 0
    entry = next(log for log in logs if log["event"] == "progress-stream-job-missing")
    assert entry["job_id"] == str(_JOB_ID)
    assert entry["log_level"] == "warning"


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
    with structlog.testing.capture_logs() as logs:
        async for event in handle.progress_stream():
            events.append(event)

    assert len(events) == 2
    # Discarded, never silently: a publisher emitting garbage on the job's
    # channel is a defect an operator has to be able to see.
    entry = next(log for log in logs if log["event"] == "stream-event-deserialise-error")
    assert entry["job_id"] == str(_JOB_ID)
    assert entry["log_level"] == "warning"
    # The failing locations and their count are enough to trace a bad
    # publisher; the payload itself (pydantic's input_value) never
    # reaches the log, as it is the user's own progress data.
    assert entry["error_type"] == "ValidationError"
    assert entry["error_count"] >= 1
    assert isinstance(entry["locations"], list) and entry["locations"]
    assert "error" not in entry
    assert "input_value" not in repr(entry)


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


# ── Redis path: the snapshot replay at subscribe ────────────────────────


async def test_redis_replays_running_row_snapshot_before_the_live_tail() -> None:
    """A re-attach to a LIVE job gets the row's current snapshot as the
    FIRST event, then the live tail: a fresh consumer (a transport cut is
    routine) must not starve until the actor's next publish — minutes on
    a slow actor. The parity contract is ``TaskQ.stream``'s: the first
    event is the row as it stands when the stream is opened."""
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    # The snapshot read: running with three published ticks while the
    # consumer was away. The handle's row is what the consumer last
    # knew: running, nothing published yet.
    snapshot = _row(status="running", progress_seq=3, job_id=_JOB_ID)
    backend = _stub_backend(rows=[snapshot])
    row = _row(status="running", progress_seq=0, job_id=_JOB_ID)

    messages = [
        {"type": "subscribe", "data": None, "channel": f"taskq:{_SCHEMA_LABEL}:progress:{_JOB_ID}"},
        {
            "type": "message",
            "data": _progress_event_bytes(seq=4, kind="progress", status="running", step=1),
        },
        {
            "type": "message",
            "data": _progress_event_bytes(
                seq=5, kind="state_change", status="succeeded", terminal=True
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
    # The FIRST event is the row's current snapshot, a plain progress
    # event, terminal False — then the live tail flows.
    assert (events[0].kind, events[0].seq, events[0].terminal) == ("progress", 3, False)
    assert events[0].status == "running"
    assert events[1].seq == 4
    assert events[1].step == 1
    assert events[2].kind == "state_change"
    assert events[2].terminal is True


async def test_redis_replays_snapshot_when_only_the_status_moved() -> None:
    """A job that moved pending → running while the consumer was away,
    before its first progress tick (``progress_seq == 0``), still
    replays: the status moved past the row the handle holds. The replay
    is labeled state_change — the status moved — matching the PG
    fallback's first event for the same transition."""
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    snapshot = _row(status="running", progress_seq=0, job_id=_JOB_ID)
    backend = _stub_backend(rows=[snapshot])
    row = _row(status="pending", progress_seq=0, job_id=_JOB_ID)

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

    assert len(events) == 3
    assert (events[0].kind, events[0].seq, events[0].status) == ("state_change", 0, "running")
    assert events[0].terminal is False
    assert events[2].terminal is True


async def test_redis_terminal_row_still_yields_and_returns() -> None:
    """An already-terminal job yields exactly its terminal event and
    returns — today's behavior, unchanged by the snapshot replay (the
    subscription is never opened)."""
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    backend = _stub_backend(rows=[_row(status="succeeded", progress_seq=3, job_id=_JOB_ID)])
    row = _row(status="running", progress_seq=0, job_id=_JOB_ID)

    pubsub = _make_pubsub_mock([])  # never reached: the terminal return precedes the subscribe
    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    handle = _handle_from_backend(backend, row=row, redis_client=redis_client, settings=settings)

    events: list[ProgressEvent] = []
    async for event in handle.progress_stream():
        events.append(event)

    assert len(events) == 1
    assert events[0].terminal is True
    assert events[0].status == "succeeded"
    assert events[0].kind == "state_change"


async def test_redis_no_phantom_snapshot_for_a_contentless_row() -> None:
    """A non-terminal row with nothing to replay — ``progress_seq == 0``
    and no status change from the row the handle holds — yields nothing
    extra: the first event is the live tail, never a phantom duplicate
    of state the consumer already holds."""
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    snapshot = _row(status="running", progress_seq=0, job_id=_JOB_ID)
    backend = _stub_backend(rows=[snapshot])
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

    assert [event.seq for event in events] == [1, 2]


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


# ── Bounded pubsub close in redis_event_stream (review N5) ────────────


class _HungPubSub:
    """Fake redis PubSub whose aclose() hangs on a gate (dead broker).

    get_message always returns None (poll timeout), so the stream only
    terminates via the caller's terminal ``on_timeout`` event - after which
    the finally's pubsub close is what wedges unguarded.
    """

    def __init__(self) -> None:
        self.aclose_calls = 0
        self.unsubscribed = False
        self._aclose_wait = asyncio.Event()  # never set - aclose() hangs forever

    async def subscribe(self, channel: str) -> None:
        pass

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message signature.
    ) -> None:
        return None

    async def unsubscribe(self, channel: str) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        self.aclose_calls += 1
        await self._aclose_wait.wait()


async def test_transport_bounds_hung_pubsub_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """redis_event_stream's finally: a hung pubsub aclose() (dead broker) is
    bounded - the stream logs ``redis-teardown-close-timeout``
    (label=client-transport) and completes instead of wedging the stream
    finalizer (review N5). Docker-free fake pubsub driven through the REAL
    redis_event_stream."""
    import taskq.client._transport as transport_mod
    from taskq.client._transport import redis_event_stream

    # Why raising=False: unguarded the module has no CLOSE_TIMEOUT_SECS seam,
    # so the RED state must demonstrate the finalizer wedge (outer timeout),
    # not an AttributeError from the shrink.
    monkeypatch.setattr(transport_mod, "CLOSE_TIMEOUT_SECS", 0.05, raising=False)
    pubsub = _HungPubSub()
    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub

    terminal_event = ProgressEvent(
        kind="state_change",
        job_id=_JOB_ID,
        actor=_ACTOR,
        ts=datetime.now(UTC),
        seq=1,
        status="succeeded",
        terminal=True,
    )

    async def _decode(raw: str) -> ProgressEvent | None:
        return None

    async def _on_timeout() -> ProgressEvent | None:
        return terminal_event

    # Why the outer timeout: unguarded, the finally awaited pubsub.aclose()
    # unbounded, so the RED state wedges here instead of failing fast.
    with structlog.testing.capture_logs() as captured:
        async with asyncio.timeout(5):
            async for _ in redis_event_stream(
                redis_client, "chan", 0.01, _decode, _on_timeout, job_id=cast(JobId, _JOB_ID)
            ):
                pass

    assert pubsub.unsubscribed is True  # suppress kept on unsubscribe
    assert pubsub.aclose_calls == 1
    timeout_events = [e for e in captured if e.get("event") == "redis-teardown-close-timeout"]
    assert len(timeout_events) == 1, f"expected 1 redis timeout event, got {captured!r}"
    assert timeout_events[0].get("label") == "client-transport", (
        f"expected label=client-transport on the timeout event, got {timeout_events[0]!r}"
    )


# ── The on_timeout failure budget in redis_event_stream ───────────────


class _SilentPubSub:
    """Fake redis PubSub whose channel never speaks (every get_message
    times out), with a benign aclose - the stream lives entirely on its
    on_timeout callback, the shape of a silent phase."""

    def __init__(self) -> None:
        self.unsubscribed = False

    async def subscribe(self, channel: str) -> None:
        pass

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message signature.
    ) -> None:
        # Yield to the loop the way a real socket read does; a bare
        # ``return None`` would busy-spin the loop.
        await asyncio.sleep(0.01)
        return None

    async def unsubscribe(self, channel: str) -> None:
        self.unsubscribed = True

    async def aclose(self) -> None:
        pass


def _silent_redis_client() -> MagicMock:
    pubsub = _SilentPubSub()
    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = pubsub
    return redis_client


def _terminal_event() -> ProgressEvent:
    return ProgressEvent(
        kind="state_change",
        job_id=_JOB_ID,
        actor=_ACTOR,
        ts=datetime.now(UTC),
        seq=1,
        status="succeeded",
        terminal=True,
    )


async def test_progress_stream_survives_a_transient_blip_on_the_timeout_refetch() -> None:
    """One transient DB blip during a silent phase does not kill the
    stream: the on-timeout re-read fails once (warned), the next one
    succeeds and the terminal event still ends the stream.

    Confusion prevented: an unguarded ``await on_timeout()`` propagated a
    single pool/connection error straight into the consumer's ``async
    for`` — the stream died mid-run on a blip that clears in seconds.
    Red-proven: on the unguarded transport this test ends with the raw
    InterfaceError instead of the terminal event.
    """
    settings = TaskQSettings.load_from_dict({"TASKQ_SCHEMA_NAME": _SCHEMA_LABEL})
    row = _row(status="running", progress_seq=0, job_id=_JOB_ID)
    backend = AsyncMock(spec=Backend)

    calls: list[int] = []

    async def _get(job_id: JobId) -> JobRow | None:
        calls.append(1)
        if len(calls) == 1:
            return row  # the snapshot read: running, nothing to replay
        if len(calls) == 2:
            raise asyncpg.InterfaceError("connection closed")  # the blip
        return _row(status="succeeded", progress_seq=1, job_id=_JOB_ID)  # the DB is back

    backend.get = _get
    handle = _handle_from_backend(
        backend, row=row, redis_client=_silent_redis_client(), settings=settings
    )

    events: list[ProgressEvent] = []
    with structlog.testing.capture_logs() as captured:
        async for event in handle.progress_stream():
            events.append(event)

    assert len(events) == 1
    assert events[0].terminal is True
    assert events[0].status == "succeeded"
    warnings = [e for e in captured if e["event"] == "stream-poll-error"]
    assert len(warnings) == 1
    assert warnings[0]["error_type"] == "InterfaceError"
    assert warnings[0]["log_level"] == "warning"


async def test_redis_on_timeout_infra_error_within_budget_keeps_the_stream_alive() -> None:
    """An infra error out of on_timeout inside the failure budget is
    warned once and retried on the next poll timeout - the stream
    continues to its terminal event."""
    from taskq.client._transport import redis_event_stream

    readings = iter([0.0])
    calls: list[int] = []

    async def _decode(raw: str) -> ProgressEvent | None:
        return None

    async def _on_timeout() -> ProgressEvent | None:
        calls.append(1)
        if len(calls) == 1:
            raise asyncpg.InterfaceError("connection closed")
        return _terminal_event()

    with structlog.testing.capture_logs() as captured:
        events = [
            event
            async for event in redis_event_stream(
                _silent_redis_client(),
                "chan",
                0.01,
                _decode,
                _on_timeout,
                job_id=cast(JobId, _JOB_ID),
                clock=lambda: next(readings),
            )
        ]

    assert len(events) == 1
    assert events[0].terminal is True
    failures = [e for e in captured if e["event"] == "stream-poll-error"]
    assert len(failures) == 1
    assert failures[0]["error_type"] == "InterfaceError"
    assert failures[0]["log_level"] == "warning"


async def test_redis_on_timeout_infra_errors_past_the_budget_raise_stream_unavailable() -> None:
    """Consecutive infra failures spanning the budget end the stream with
    StreamUnavailable - the documented degradation signal - after one
    warning per failure, the PG poll transport's exact contract."""
    from taskq.client._transport import redis_event_stream

    readings = iter([0.0, 12.0, 24.0, 31.0])

    async def _decode(raw: str) -> ProgressEvent | None:
        return None

    async def _on_timeout() -> ProgressEvent | None:
        raise asyncpg.InterfaceError("connection closed")

    with (
        structlog.testing.capture_logs() as captured,
        pytest.raises(StreamUnavailable) as info,
    ):
        async for _ in redis_event_stream(
            _silent_redis_client(),
            "chan",
            0.01,
            _decode,
            _on_timeout,
            job_id=cast(JobId, _JOB_ID),
            clock=lambda: next(readings),
        ):
            pytest.fail("no event can be produced by a refetch that always fails")

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
    # The message text (server errors can quote data) never reaches the log.
    assert "error" not in abandoned


async def test_redis_on_timeout_key_error_propagates_untouched() -> None:
    """A KeyError out of on_timeout is not an infra error and is never
    budgeted: it propagates immediately - TaskQ.stream's _refetch
    deliberately raises it for a vanished row (its vanished-row
    contract, pinned at test_taskq_client.py)."""
    from taskq.client._transport import redis_event_stream

    async def _decode(raw: str) -> ProgressEvent | None:
        return None

    async def _on_timeout() -> ProgressEvent | None:
        raise KeyError(_JOB_ID)

    with (
        structlog.testing.capture_logs() as captured,
        pytest.raises(KeyError),
    ):
        async for _ in redis_event_stream(
            _silent_redis_client(),
            "chan",
            0.01,
            _decode,
            _on_timeout,
            job_id=cast(JobId, _JOB_ID),
        ):
            pytest.fail("no event can be produced by a refetch that always fails")

    assert not [
        e for e in captured if e["event"] in ("stream-poll-error", "stream-poll-abandoned")
    ], "a KeyError is a contract signal, not an infra failure - it must not be budgeted"


async def test_redis_on_timeout_successful_read_resets_the_failure_run() -> None:
    """A successful on_timeout read ends the failure run: two runs each
    shorter than the budget never add up to it, even when their total
    does. (Without the reset, the second run's clock reading - 100 s
    after the first failure - would abandon the stream.)"""
    from taskq.client._transport import redis_event_stream

    readings = iter([0.0, 100.0])
    calls: list[int] = []

    async def _decode(raw: str) -> ProgressEvent | None:
        return None

    async def _on_timeout() -> ProgressEvent | None:
        calls.append(1)
        if len(calls) == 1:
            raise OSError("reset")
        if len(calls) == 2:
            return None  # a healthy re-read: the row is unchanged
        if len(calls) == 3:
            raise OSError("again")
        return _terminal_event()

    with structlog.testing.capture_logs() as captured:
        events = [
            event
            async for event in redis_event_stream(
                _silent_redis_client(),
                "chan",
                0.01,
                _decode,
                _on_timeout,
                job_id=cast(JobId, _JOB_ID),
                clock=lambda: next(readings),
            )
        ]

    assert len(events) == 1
    assert events[0].terminal is True
    failures = [e for e in captured if e["event"] == "stream-poll-error"]
    assert len(failures) == 2
    assert not [e for e in captured if e["event"] == "stream-poll-abandoned"]


# ── The decode_message failure budget in redis_event_stream ────────────
#
# ``TaskQ.stream``'s decode callback re-reads the row through the backend
# (``decode → _refetch → backend.get``), so it fails with the same class
# of infra errors the on_timeout re-read fails with. The budget is the
# stream's, not a leg's: failures on either leg spend the same run, a
# success on either leg resets it, and a non-infra error (KeyError, the
# vanished-row contract) propagates untouched from either.


def _message_client(messages: list[dict[str, object]]) -> MagicMock:
    """A redis client whose pubsub delivers *messages*, then goes silent."""
    redis_client = MagicMock(spec=["pubsub"])
    redis_client.pubsub.return_value = _make_pubsub_mock(messages)
    return redis_client


def _message(data: bytes) -> dict[str, object]:
    return {"type": "message", "data": data}


async def test_redis_decode_infra_error_within_budget_keeps_the_stream_alive() -> None:
    """An infra error out of decode_message inside the failure budget is
    warned once and the stream continues to the next message - the
    on_timeout leg's exact contract, on the message leg."""
    from taskq.client._transport import redis_event_stream

    readings = iter([0.0])
    calls: list[int] = []

    async def _decode(raw: str) -> ProgressEvent | None:
        calls.append(1)
        if len(calls) == 1:
            raise asyncpg.InterfaceError("connection closed")
        return _terminal_event()

    client = _message_client(
        [_message(b"{"), _message(_terminal_event().model_dump_json().encode())]
    )

    with structlog.testing.capture_logs() as captured:
        events = [
            event
            async for event in redis_event_stream(
                client,
                "chan",
                0.01,
                _decode,
                None,
                job_id=cast(JobId, _JOB_ID),
                clock=lambda: next(readings),
            )
        ]

    assert len(events) == 1
    assert events[0].terminal is True
    failures = [e for e in captured if e["event"] == "stream-poll-error"]
    assert len(failures) == 1
    assert failures[0]["error_type"] == "InterfaceError"
    assert failures[0]["log_level"] == "warning"


async def test_redis_decode_infra_errors_past_the_budget_raise_stream_unavailable() -> None:
    """Consecutive decode_message failures spanning the budget end the
    stream with StreamUnavailable - the documented degradation signal -
    after one warning per failure. Unbudgeted, a decode leg that reads
    through the backend dies on its first pool blip: the raw
    InterfaceError propagates into the consumer's ``async for`` and the
    30 s survive-and-retry the refetch leg enjoys never happens."""
    from taskq.client._transport import redis_event_stream

    readings = iter([0.0, 12.0, 24.0, 31.0])

    async def _decode(raw: str) -> ProgressEvent | None:
        raise asyncpg.InterfaceError("connection closed")

    client = _message_client([_message(b"{")] * 4)

    with (
        structlog.testing.capture_logs() as captured,
        pytest.raises(StreamUnavailable) as info,
    ):
        async for _ in redis_event_stream(
            client,
            "chan",
            0.01,
            _decode,
            None,
            job_id=cast(JobId, _JOB_ID),
            clock=lambda: next(readings),
        ):
            pytest.fail("no event can be produced by a decode that always fails")

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
    # The message text (server errors can quote data) never reaches the log.
    assert "error" not in abandoned


async def test_redis_decode_key_error_propagates_untouched() -> None:
    """A KeyError out of decode_message is not an infra error and is
    never budgeted: it propagates immediately - ``TaskQ.stream``'s decode
    re-reads the row through _refetch, whose vanished-row contract
    deliberately raises it."""
    from taskq.client._transport import redis_event_stream

    async def _decode(raw: str) -> ProgressEvent | None:
        raise KeyError(_JOB_ID)

    client = _message_client([_message(b"{")])

    with (
        structlog.testing.capture_logs() as captured,
        pytest.raises(KeyError),
    ):
        async for _ in redis_event_stream(
            client,
            "chan",
            0.01,
            _decode,
            None,
            job_id=cast(JobId, _JOB_ID),
        ):
            pytest.fail("no event can be produced by a decode that always fails")

    assert not [
        e for e in captured if e["event"] in ("stream-poll-error", "stream-poll-abandoned")
    ], "a KeyError is a contract signal, not an infra failure - it must not be budgeted"


async def test_redis_decode_success_resets_the_failure_run() -> None:
    """A decoded message ends the failure run: two runs each shorter than
    the budget never add up to it, even when their total does. (Without
    the reset, the second run's clock reading - 100 s after the first
    failure - would abandon the stream.)"""
    from taskq.client._transport import redis_event_stream

    readings = iter([0.0, 100.0])
    calls: list[int] = []

    async def _decode(raw: str) -> ProgressEvent | None:
        calls.append(1)
        if len(calls) == 1:
            raise OSError("reset")
        if len(calls) == 2:
            return None  # a healthy read of a message that is not one of ours
        if len(calls) == 3:
            raise OSError("again")
        return _terminal_event()

    client = _message_client([_message(b"{")] * 4)

    with structlog.testing.capture_logs() as captured:
        events = [
            event
            async for event in redis_event_stream(
                client,
                "chan",
                0.01,
                _decode,
                None,
                job_id=cast(JobId, _JOB_ID),
                clock=lambda: next(readings),
            )
        ]

    assert len(events) == 1
    assert events[0].terminal is True
    failures = [e for e in captured if e["event"] == "stream-poll-error"]
    assert len(failures) == 2
    assert not [e for e in captured if e["event"] == "stream-poll-abandoned"]


async def test_redis_message_and_timeout_legs_share_one_failure_budget() -> None:
    """Failures alternate across the two legs and still exhaust the one
    budget: a decode failure and an on_timeout failure are the same
    stream's failures against the same backing store, so neither leg's
    failures can hide from the budget the other leg is spending."""
    from taskq.client._transport import redis_event_stream

    readings = iter([0.0, 12.0, 24.0, 31.0])

    async def _decode(raw: str) -> ProgressEvent | None:
        raise asyncpg.InterfaceError("decode leg down")

    async def _on_timeout() -> ProgressEvent | None:
        raise asyncpg.InterfaceError("timeout leg down")

    client = _message_client([_message(b"{")] * 2)

    with (
        structlog.testing.capture_logs() as captured,
        pytest.raises(StreamUnavailable) as info,
    ):
        async for _ in redis_event_stream(
            client,
            "chan",
            0.01,
            _decode,
            _on_timeout,
            job_id=cast(JobId, _JOB_ID),
            clock=lambda: next(readings),
        ):
            pytest.fail("no event can be produced by a transport that always fails")

    exc = info.value
    assert exc.consecutive_failures == 4
    assert exc.elapsed == 31.0
    assert [e["event"] for e in captured] == ["stream-poll-error"] * 3 + ["stream-poll-abandoned"]
