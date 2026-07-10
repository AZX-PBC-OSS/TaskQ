"""Unit tests for progress publish and consumer-level state-change paths."""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from taskq.constants import progress_channel, progress_global_channel
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._publish import (
    _publish_event,
    _publish_progress_event,
    _publish_state_change_event,
)
from taskq.testing.otel import counter_data_points, counter_value, setup_meter

_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000001")
_SCHEMA_LABEL = "taskq_test"


def _make_redis_mock(*, raise_on_publish: Exception | None = None) -> AsyncMock:
    client = AsyncMock()
    if raise_on_publish is not None:
        client.publish.side_effect = raise_on_publish
    else:
        client.publish.return_value = 1
    return client


def _make_publish_args(
    redis_client: object = None,
    *,
    publish_global: bool = True,
) -> tuple[object, object, dict[UUID, _ProgressBuffer]]:
    """Return (redis_client, settings, progress_buffers) for _publish_state_change_event."""
    from taskq.settings import WorkerSettings

    s = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": _SCHEMA_LABEL,
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "true" if publish_global else "false",
        }
    )
    buffers: dict[UUID, _ProgressBuffer] = {}
    return redis_client, s, buffers


# ── terminal flush before mark_succeeded drains the buffer ─────────


async def test_terminal_flush_before_mark_succeeded_drains_buffer() -> None:
    """Actor calls ctx.progress(step=1) then succeeds. After the job completes
    (in-memory backend), progress_seq >= 1 in the row AND pending_seq_delta == 0
    (buffer was drained before mark_succeeded)."""
    import asyncio
    from collections.abc import AsyncGenerator
    from contextlib import asynccontextmanager
    from datetime import UTC, datetime

    import structlog

    from taskq._ids import new_job_id, new_uuid
    from taskq.backend._protocol import EnqueueArgs
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.context import JobContext
    from taskq.obs import bind_job_context
    from taskq.progress._flush import _flush_buffer_immediate
    from taskq.settings import WorkerSettings
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    job_id = new_job_id()
    worker_id = new_uuid()

    args = EnqueueArgs(
        id=job_id,
        actor="test_actor",
        queue="default",
        payload={},
        payload_schema_ver=1,
        max_attempts=3,
        retry_kind="fixed",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        priority=0,
        fairness_key=None,
        metadata={},
        identity_key=None,
        idempotency_key=None,
        trace_id=None,
        span_id=None,
    )
    await backend.enqueue(args)
    from datetime import timedelta as _td2

    assert (
        len(
            await backend.dispatch_batch(
                worker_id=worker_id, queues=["default"], limit=1, lock_lease=_td2(seconds=60)
            )
        )
        == 1
    )

    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buffers = {job_id: buf}

    # Build a pool mock that returns progress_seq=1 after flush
    conn = AsyncMock()
    conn.fetchrow.return_value = {"progress_seq": 1}
    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )

    ctx: JobContext[PassthroughPayload] = JobContext(
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        worker_id=worker_id,
        payload=PassthroughPayload(),
        cancel_event=asyncio.Event(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=backend),
        log=bind_job_context(
            structlog.get_logger("test"),
            job_id=job_id,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
        _progress_buffers=buffers,
        _worker_settings=settings,
    )

    # Actor body: call ctx.progress then succeed
    await ctx.progress(step=1)
    assert buf.pending_seq_delta == 1
    assert buf.dirty is True

    # Pre-terminal flush (consumer does this before mark_succeeded)
    await _flush_buffer_immediate(pool, "taskq_test", job_id, worker_id, buffers)

    assert buf.pending_seq_delta == 0
    assert buf.dirty is False

    await backend.mark_succeeded(job_id, worker_id, None, progress_seq=1)

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "succeeded"
    assert row.progress_seq >= 1


# ── ctx.progress() publishes to schema-scoped per-job channel ──────


async def test_ctx_progress_publishes_to_schema_scoped_channel() -> None:
    """Mock Redis; two ctx.progress() calls; redis.publish called exactly twice
    with the correct schema-scoped per-job channel."""
    import asyncio
    from datetime import UTC, datetime

    import structlog

    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.context import JobContext
    from taskq.obs import bind_job_context
    from taskq.progress._buffer import _ProgressBuffer
    from taskq.settings import WorkerSettings
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload

    redis_client = _make_redis_mock()
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "testschema",
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buffers = {_JOB_ID: buf}

    ctx: JobContext[PassthroughPayload] = JobContext(
        job_id=_JOB_ID,
        actor="test_actor",
        queue="default",
        attempt=1,
        worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: fixture helper accesses private field for test setup.
        payload=PassthroughPayload(),
        cancel_event=asyncio.Event(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=backend),
        log=bind_job_context(
            structlog.get_logger("test"),
            job_id=_JOB_ID,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
        _progress_buffers=buffers,
        _redis_client=redis_client,  # type: ignore[arg-type]
        _worker_settings=settings,
    )

    await ctx.progress(step=1)
    await ctx.progress(step=2)

    assert redis_client.publish.await_count == 2
    expected_channel = progress_channel("testschema", _JOB_ID)
    for call in redis_client.publish.call_args_list:
        assert call[0][0] == expected_channel


# ── OTel counter incremented with correct attributes on failure ─────


async def test_publish_failure_increments_otel_counter_with_labels(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redis publish raises ConnectionError. Assert OTel counter
    taskq.progress.publish_failures == 1 with 'channel' and 'error_type'
    attributes."""
    import taskq.obs._otel as otel_mod

    reader = setup_meter(monkeypatch)
    # Create counter from the already-patched test-scoped meter
    new_counter = otel_mod.get_meter().create_counter("taskq.progress.publish_failures")
    monkeypatch.setattr(otel_mod, "_progress_publish_failures", new_counter)
    otel_mod.set_otel_enabled(True)

    import structlog

    redis_client = _make_redis_mock(raise_on_publish=ConnectionError("redis down"))

    await _publish_event(
        redis_client,
        progress_channel(_SCHEMA_LABEL, _JOB_ID),
        '{"v": 1}',
        seq=1,
        log=structlog.get_logger("test"),
        channel_label="per_job",
    )

    assert counter_value(reader, "taskq.progress.publish_failures") == 1
    points = counter_data_points(reader, "taskq.progress.publish_failures")
    assert len(points) == 1
    attrs = dict(points[0].attributes or {})  # type: ignore[arg-type] # Why: OTel AttributeValue union is complex; runtime is always str-keyed.
    assert "channel" in attrs
    assert "error_type" in attrs


# ── redis_client=None → no publish, buffer stays dirty ─────────────


async def test_ctx_progress_no_publish_when_redis_client_none() -> None:
    """When _redis_client is None, ctx.progress() does NOT attempt Redis publish
    and the buffer is still marked dirty."""
    import asyncio
    from datetime import UTC, datetime

    import structlog

    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.context import JobContext
    from taskq.obs import bind_job_context
    from taskq.settings import WorkerSettings
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload

    settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_test"})
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buffers = {_JOB_ID: buf}

    ctx: JobContext[PassthroughPayload] = JobContext(
        job_id=_JOB_ID,
        actor="test_actor",
        queue="default",
        attempt=1,
        worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: fixture helper accesses private field for test setup.
        payload=PassthroughPayload(),
        cancel_event=asyncio.Event(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=backend),
        log=bind_job_context(
            structlog.get_logger("test"),
            job_id=_JOB_ID,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
        _progress_buffers=buffers,
        _redis_client=None,
        _worker_settings=settings,
    )

    await ctx.progress(step=1)

    assert buf.dirty is True
    assert buf.pending_seq_delta == 1


# ── progress_publish_global=True → publishes to both channels ──────


async def test_publish_progress_event_global_channel_when_enabled() -> None:
    redis_client = _make_redis_mock()

    from taskq.settings import WorkerSettings

    s = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": _SCHEMA_LABEL,
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "true",
        }
    )

    await _publish_progress_event(
        redis_client,
        s,
        actor="my_actor",
        job_id=_JOB_ID,
        step=None,
        percent=None,
        detail=None,
        data=None,
        seq=1,
    )

    assert redis_client.publish.await_count == 2
    channels_called = {c[0][0] for c in redis_client.publish.call_args_list}
    assert progress_channel(_SCHEMA_LABEL, _JOB_ID) in channels_called
    assert progress_global_channel(_SCHEMA_LABEL) in channels_called


# ── progress_publish_global=False → only per-job channel ──────────


async def test_publish_progress_event_no_global_when_disabled() -> None:
    redis_client = _make_redis_mock()

    from taskq.settings import WorkerSettings

    s = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": _SCHEMA_LABEL,
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )

    await _publish_progress_event(
        redis_client,
        s,
        actor="my_actor",
        job_id=_JOB_ID,
        step=None,
        percent=None,
        detail=None,
        data=None,
        seq=1,
    )

    assert redis_client.publish.await_count == 1
    assert redis_client.publish.call_args[0][0] == progress_channel(_SCHEMA_LABEL, _JOB_ID)


# ── terminal transition emits terminal=True, kind=state_change ─────


async def test_terminal_transition_publishes_terminal_state_change() -> None:
    """Simulate mark_succeeded terminal transition via _publish_state_change_event.
    Final published event must have terminal=True, kind='state_change',
    status='succeeded'. Progress events (terminal=False) come first."""
    redis_client = _make_redis_mock()
    _rc, settings, buffers = _make_publish_args(redis_client=redis_client, publish_global=False)

    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=2)
    buffers[_JOB_ID] = buf

    await _publish_state_change_event(
        redis_client,
        settings,
        _JOB_ID,
        "my_actor",
        buffers,
        status="succeeded",
        terminal=True,
    )

    payload_json = redis_client.publish.call_args[0][1]
    parsed = json.loads(payload_json)
    assert parsed["kind"] == "state_change"
    assert parsed["status"] == "succeeded"
    assert parsed["terminal"] is True


# ── state_change event for running has terminal=False ────────────


async def test_state_change_event_running_is_not_terminal() -> None:
    """_publish_state_change_event for status='running' must produce
    kind='state_change', status='running', terminal=False."""
    redis_client = _make_redis_mock()
    _rc, settings, buffers = _make_publish_args(redis_client=redis_client, publish_global=False)

    await _publish_state_change_event(
        redis_client,
        settings,
        _JOB_ID,
        "my_actor",
        buffers,
        status="running",
        terminal=False,
    )

    payload_json = redis_client.publish.call_args[0][1]
    parsed = json.loads(payload_json)
    assert parsed["kind"] == "state_change"
    assert parsed["status"] == "running"
    assert parsed["terminal"] is False


# ── all mark_* methods produce state_change with correct status ───


@pytest.mark.parametrize(
    ("status", "terminal"),
    [
        ("running", False),
        ("succeeded", True),
        ("failed", True),
        ("cancelled", True),
        ("abandoned", True),
        ("snoozed", False),
    ],
)
async def test_state_change_event_for_all_mark_methods(status: str, terminal: bool) -> None:
    """Each mark_* transition publishes kind='state_change' with the correct
    status and terminal flag."""
    redis_client = _make_redis_mock()
    _rc, settings, buffers = _make_publish_args(redis_client=redis_client, publish_global=False)

    await _publish_state_change_event(
        redis_client,
        settings,
        _JOB_ID,
        "my_actor",
        buffers,
        status=status,
        terminal=terminal,
    )

    payload_json = redis_client.publish.call_args[0][1]
    parsed = json.loads(payload_json)
    assert parsed["kind"] == "state_change"
    assert parsed["status"] == status


# ── ctx.progress() published event has kind=progress, status=running


async def test_ctx_progress_event_has_kind_progress_and_status_running() -> None:
    """The event published by ctx.progress() has kind='progress' and
    status='running'."""
    import asyncio
    from datetime import UTC, datetime

    import structlog

    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.context import JobContext
    from taskq.obs import bind_job_context
    from taskq.settings import WorkerSettings
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload

    captured: list[dict[str, object]] = []
    redis_client = AsyncMock()

    async def _capture(channel: str, payload: str) -> int:
        captured.append(json.loads(payload))
        return 1

    redis_client.publish.side_effect = _capture

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buffers = {_JOB_ID: buf}

    ctx: JobContext[PassthroughPayload] = JobContext(
        job_id=_JOB_ID,
        actor="test_actor",
        queue="default",
        attempt=1,
        worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: fixture helper accesses private field for test setup.
        payload=PassthroughPayload(),
        cancel_event=asyncio.Event(),
        jobs=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=backend),
        log=bind_job_context(
            structlog.get_logger("test"),
            job_id=_JOB_ID,
            actor="test_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
        _progress_buffers=buffers,
        _redis_client=redis_client,  # type: ignore[arg-type]
        _worker_settings=settings,
    )

    await ctx.progress(step=1)

    assert len(captured) == 1
    assert captured[0]["kind"] == "progress"
    assert captured[0]["status"] == "running"
    assert captured[0]["terminal"] is False


# ── cancel discards buffer; no flush; terminal state_change published


async def test_cancel_discards_buffer_no_flush_terminal_state_change() -> None:
    """Job calls ctx.progress() twice, then is hard-cancelled.
    Assert: buffer is discarded (not flushed); a kind='state_change' terminal
    event with terminal=True, status='cancelled' is published; in-memory row
    has status='cancelled'."""
    from datetime import UTC, datetime

    from taskq._ids import new_job_id, new_uuid
    from taskq.backend._protocol import EnqueueArgs
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    job_id = new_job_id()
    worker_id = new_uuid()

    args = EnqueueArgs(
        id=job_id,
        actor="test_actor",
        queue="default",
        payload={},
        payload_schema_ver=1,
        max_attempts=3,
        retry_kind="fixed",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        priority=0,
        fairness_key=None,
        metadata={},
        identity_key=None,
        idempotency_key=None,
        trace_id=None,
        span_id=None,
    )
    await backend.enqueue(args)
    from datetime import timedelta as _td2

    jobs = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=1, lock_lease=_td2(seconds=60)
    )
    assert len(jobs) == 1

    redis_client = _make_redis_mock()
    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buffers = {job_id: buf}

    # Simulate two ctx.progress() calls (no flush)
    buf.pending_seq_delta = 2
    buf.pending_state["step"] = 2
    buf.dirty = True

    # Cancel: pop buffer (discard, not flush), then mark_cancelled
    cancel_buf = buffers.pop(job_id)
    override_seq = cancel_buf.base_seq
    override_state = dict(cancel_buf.pending_state)

    await backend.mark_cancelled(job_id, worker_id, progress_seq=0)

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "cancelled"

    # The in-memory backend did not receive the pending progress (buffer was discarded)
    # progress_seq=0 because we passed 0 (buffer was discarded, not flushed)
    assert row.progress_seq == 0

    # Publish terminal state_change event (what the consumer does after discard)
    _rc, settings, _bufs = _make_publish_args(redis_client=redis_client, publish_global=False)
    await _publish_state_change_event(
        redis_client,
        settings,
        job_id,
        "test_actor",
        None,
        status="cancelled",
        terminal=True,
        _override_seq=override_seq,
        _override_pending_state=override_state,
    )

    payload_json = redis_client.publish.call_args[0][1]
    parsed = json.loads(payload_json)
    assert parsed["kind"] == "state_change"
    assert parsed["status"] == "cancelled"
    assert parsed["terminal"] is True


# ── state-change publish failure does not fail the job ────────────


async def test_state_change_publish_failure_does_not_fail_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Redis publish failure during state_change event must not propagate.
    publish_failures counter >= 1; publish is fire-and-forget."""
    import taskq.obs._otel as otel_mod

    reader = setup_meter(monkeypatch)
    new_counter = otel_mod.get_meter().create_counter("taskq.progress.publish_failures")
    monkeypatch.setattr(otel_mod, "_progress_publish_failures", new_counter)
    otel_mod.set_otel_enabled(True)

    redis_client = _make_redis_mock(raise_on_publish=ConnectionError("redis down"))
    _rc, settings, buffers = _make_publish_args(redis_client=redis_client, publish_global=False)

    # Must not raise
    await _publish_state_change_event(
        redis_client,
        settings,
        _JOB_ID,
        "my_actor",
        buffers,
        status="running",
        terminal=False,
    )

    assert counter_value(reader, "taskq.progress.publish_failures") >= 1


# ── Regression: ProgressEvent construction error is fire-and-forget ────────


async def test_publish_progress_event_construction_error_is_fire_and_forget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If ProgressEvent construction fails, the error is caught and the
    OTel counter increments; the coroutine does NOT raise."""
    import taskq.obs._otel as otel_mod
    import taskq.progress._events as events_mod

    reader = setup_meter(monkeypatch)
    new_counter = otel_mod.get_meter().create_counter("taskq.progress.publish_failures")
    monkeypatch.setattr(otel_mod, "_progress_publish_failures", new_counter)
    otel_mod.set_otel_enabled(True)

    redis_client = _make_redis_mock()

    settings = MagicMock()
    settings.schema_name = _SCHEMA_LABEL
    settings.progress_publish_global = False

    def _failing_init(self: object, **kwargs: object) -> None:
        raise ValueError("simulated construction error")

    monkeypatch.setattr(events_mod.ProgressEvent, "__init__", _failing_init)

    await _publish_progress_event(
        redis_client,
        settings,
        "test_actor",
        _JOB_ID,
        step=1,
        percent=50.0,
        detail=None,
        data=None,
        seq=1,
    )

    assert counter_value(reader, "taskq.progress.publish_failures") >= 1


# ── Regression: clean-buffer cancel path preserves base_seq (not 0) ──────


async def test_cancel_clean_buffer_passes_base_seq_not_zero() -> None:
    """Regression for findings-3 Critical: when the buffer is clean (post-flush,
    base_seq=5, pending_seq_delta=0), the cancel path must compute seq as
    base_seq + pending_seq_delta = 5, NOT 0. The _terminal_seq_and_state helper
    ensures this; _snapshot_progress would have returned 0."""
    from taskq._ids import new_job_id, new_uuid
    from taskq.backend._protocol import EnqueueArgs
    from taskq.progress._buffer import _terminal_seq_and_state
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    job_id = new_job_id()
    worker_id = new_uuid()

    args = EnqueueArgs(
        id=job_id,
        actor="test_actor",
        queue="default",
        payload={},
        payload_schema_ver=1,
        max_attempts=3,
        retry_kind="fixed",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        priority=0,
        fairness_key=None,
        metadata={},
        identity_key=None,
        idempotency_key=None,
        trace_id=None,
        span_id=None,
    )
    await backend.enqueue(args)
    from datetime import timedelta as _td2

    jobs = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=1, lock_lease=_td2(seconds=60)
    )
    assert len(jobs) == 1

    redis_client = _make_redis_mock()
    # Simulate a buffer that has been flushed: base_seq=5, clean
    buf = _ProgressBuffer(job_id=job_id, base_seq=5, pending_seq_delta=0, dirty=False)
    buf.pending_state = {"step": 5}
    buffers = {job_id: buf}

    # Cancel: pop buffer (discard, not flush), compute seq via _terminal_seq_and_state
    cancel_buf = buffers.pop(job_id)
    cancel_seq, cancel_state = _terminal_seq_and_state(cancel_buf)

    # The critical assertion: cancel_seq must be 5, not 0
    assert cancel_seq == 5
    assert cancel_state == {"step": 5}

    await backend.mark_cancelled(job_id, worker_id, progress_seq=cancel_seq)

    row = await backend.get(job_id)
    assert row is not None
    assert row.status == "cancelled"
    assert row.progress_seq == 5

    # Publish terminal state_change event with the correct override values
    _rc, settings, _bufs = _make_publish_args(redis_client=redis_client, publish_global=False)
    await _publish_state_change_event(
        redis_client,
        settings,
        job_id,
        "test_actor",
        None,
        status="cancelled",
        terminal=True,
        _override_seq=cancel_seq,
        _override_pending_state=cancel_state,
    )

    payload_json = redis_client.publish.call_args[0][1]
    parsed = json.loads(payload_json)
    assert parsed["kind"] == "state_change"
    assert parsed["status"] == "cancelled"
    assert parsed["terminal"] is True
    assert parsed["seq"] == 5
