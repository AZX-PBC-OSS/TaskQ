"""Unit tests for _ProgressBuffer state management and ctx.progress() buffer logic."""

import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from taskq.exceptions import ProgressTooLarge
from taskq.progress._buffer import (
    _consume_state_change_seq,
    _ProgressBuffer,
    _seq_and_state_after_flush_attempt,
    _terminal_seq_and_state,
)
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from tests._progress_context import make_progress_context

_JOB_ID = UUID("00000000-0000-0000-0000-000000000001")
_JOB_ID_B = UUID("00000000-0000-0000-0000-000000000002")


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)))


def _make_pool_mock(*, returning_row: dict[str, object] | None = None) -> MagicMock:
    conn = AsyncMock()
    conn.fetchrow.return_value = returning_row
    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire
    return pool


# ── ctx.progress() marks buffer dirty with all four fields ─────────


async def test_ctx_progress_marks_dirty_with_all_fields() -> None:
    """ctx.progress(step, percent, detail, data) marks buffer dirty, delta=1,
    and all four fields appear in pending_state."""
    backend = _make_backend()
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, backend=backend)

    await ctx.progress(step=1, percent=10.0, detail="start", data={"rows": 0})

    assert buf.dirty is True
    assert buf.pending_seq_delta == 1
    assert buf.pending_state["step"] == 1
    assert buf.pending_state["percent"] == 10.0
    assert buf.pending_state["detail"] == "start"
    assert buf.pending_state["data"] == {"rows": 0}


# ── three ctx.progress() calls produce seq 1,2,3; flush drains delta


async def test_ctx_progress_seq_sequence_and_flush_drains_delta() -> None:
    """Three ctx.progress() calls produce published events with seq=1,2,3.
    After simulated flush, pending_seq_delta==0 and base_seq==3."""
    published_seqs: list[int] = []
    redis_client = AsyncMock()

    async def _capture_publish(channel: str, payload: str) -> int:
        published_seqs.append(json.loads(payload)["seq"])
        return 1

    redis_client.publish.side_effect = _capture_publish

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )

    backend = _make_backend()
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(
        buffers, _JOB_ID, backend=backend, settings=settings, redis_client=redis_client
    )

    await ctx.progress(step=1)
    await ctx.progress(step=2)
    await ctx.progress(step=3)

    assert published_seqs == [1, 2, 3]

    # Simulate flush: backend writes back progress_seq=3
    pool = _make_pool_mock(returning_row={"progress_seq": 3})
    from taskq._ids import new_uuid
    from taskq.progress._flush import _flush_buffer

    await _flush_buffer(pool, "taskq_test", _JOB_ID, new_uuid(), buf, buffers)

    assert buf.base_seq == 3
    assert buf.pending_seq_delta == 0
    assert buf.dirty is False


# ── ProgressTooLarge raised; buffer NOT updated ───────────────────


async def test_ctx_progress_too_large_does_not_update_buffer() -> None:
    """ctx.progress(data=oversized) raises ProgressTooLarge and leaves the
    buffer completely unchanged (delta=0, dirty=False)."""
    settings = WorkerSettings.load_from_dict({"TASKQ_PROGRESS_DATA_MAX_BYTES": "16384"})
    backend = _make_backend()
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, backend=backend, settings=settings)

    with pytest.raises(ProgressTooLarge) as exc_info:
        await ctx.progress(data={"x": "a" * 17000})

    assert exc_info.value.limit == 16384
    assert exc_info.value.actual > 16384
    assert buf.pending_seq_delta == 0
    assert buf.dirty is False
    assert buf.pending_state == {}


async def test_ctx_progress_too_large_no_redis_publish() -> None:
    """ProgressTooLarge path: no Redis publish call must occur."""
    redis_client = AsyncMock()
    settings = WorkerSettings.load_from_dict({"TASKQ_PROGRESS_DATA_MAX_BYTES": "16384"})
    backend = _make_backend()
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(
        buffers, _JOB_ID, backend=backend, settings=settings, redis_client=redis_client
    )

    with pytest.raises(ProgressTooLarge):
        await ctx.progress(data={"x": "a" * 17000})

    redis_client.publish.assert_not_called()


# ── coalesced flush merges fields last-writer-wins ─────────────────


async def test_ctx_progress_coalesced_merge_last_writer_wins() -> None:
    """Two ctx.progress() calls coalesce: step/percent reflect second call;
    detail from first call is preserved; data absent since never set."""
    from taskq._ids import new_uuid
    from taskq.progress._flush import _flush_buffer

    pool = _make_pool_mock(returning_row={"progress_seq": 2})
    backend = _make_backend()
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, backend=backend)

    await ctx.progress(step=1, percent=10.0)
    await ctx.progress(step=2, percent=50.0, detail="mid")

    assert buf.pending_state["step"] == 2
    assert buf.pending_state["percent"] == 50.0
    assert buf.pending_state["detail"] == "mid"
    assert "data" not in buf.pending_state

    await _flush_buffer(pool, "taskq_test", _JOB_ID, new_uuid(), buf, buffers)

    assert buf.pending_seq_delta == 0
    assert buf.dirty is False


# ── Snooze preserves progress_seq; redispatch continues from base_seq


async def test_ctx_progress_snooze_preserves_seq_and_redispatch_continues() -> None:
    """Job calls ctx.progress() 3 times (seq 1,2,3), then is snoozed.
    mark_snoozed receives progress_seq=3. On redispatch with base_seq=3,
    the next ctx.progress() emits seq=4."""
    from taskq._ids import new_job_id, new_uuid
    from taskq.backend._protocol import EnqueueArgs

    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement - candidates come FROM the registry).
    backend.register_actor_config(actor="test_actor")
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
    from datetime import timedelta as _td

    jobs = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=1, lock_lease=_td(seconds=60)
    )
    assert len(jobs) == 1

    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {job_id: buf}
    ctx = make_progress_context(buffers, job_id, backend=backend)

    await ctx.progress(step=1)
    await ctx.progress(step=2)
    await ctx.progress(step=3)
    assert buf.pending_seq_delta == 3

    progress_seq = buf.base_seq + buf.pending_seq_delta
    result = await backend.mark_snoozed(
        job_id,
        worker_id,
        delay=timedelta(seconds=60),
        progress_seq=progress_seq,
        progress_state=dict(buf.pending_state),
        attempt=1,
        claim_epoch=1,
    )
    assert result == "scheduled"

    row = await backend.get(job_id)
    assert row is not None
    assert row.progress_seq == 3

    # Redispatch: promote scheduled→pending, then dispatch
    clock.advance(timedelta(seconds=120))
    await backend.scheduled_to_pending()
    jobs2 = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert len(jobs2) == 1

    published_seqs: list[int] = []
    redis_client = AsyncMock()

    async def _capture(channel: str, payload: str) -> int:
        published_seqs.append(json.loads(payload)["seq"])
        return 1

    redis_client.publish.side_effect = _capture
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )

    buf2 = _ProgressBuffer(job_id=job_id, base_seq=row.progress_seq)
    buffers2: dict[UUID, _ProgressBuffer] = {job_id: buf2}
    ctx2 = make_progress_context(
        buffers2, job_id, backend=backend, settings=settings, redis_client=redis_client
    )
    await ctx2.progress(step=4)
    assert published_seqs == [4]


# ── seq is isolated per job ID ───────────────────────────────────


async def test_ctx_progress_seq_isolated_per_job_id() -> None:
    """Two independent job IDs each have their own buffer. Each job's first
    ctx.progress() emits seq=1; there is no cross-job seq contamination."""
    published: dict[str, list[int]] = {"a": [], "b": []}
    redis_client = AsyncMock()

    async def _capture(channel: str, payload: str) -> int:
        parsed = json.loads(payload)
        key = "a" if parsed["job_id"] == str(_JOB_ID) else "b"
        published[key].append(parsed["seq"])
        return 1

    redis_client.publish.side_effect = _capture

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )

    backend = _make_backend()
    buf_a = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buf_b = _ProgressBuffer(job_id=_JOB_ID_B, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf_a, _JOB_ID_B: buf_b}

    ctx_a = make_progress_context(
        buffers, _JOB_ID, backend=backend, settings=settings, redis_client=redis_client
    )
    ctx_b = make_progress_context(
        buffers, _JOB_ID_B, backend=backend, settings=settings, redis_client=redis_client
    )

    await ctx_a.progress(step=1)
    await ctx_b.progress(step=1)

    assert published["a"] == [1]
    assert published["b"] == [1]
    assert buf_a.pending_seq_delta == 1
    assert buf_b.pending_seq_delta == 1


# ── _seq_and_state_after_flush_attempt: flush-failure fallback regression ──


async def test_seq_and_state_after_flush_attempt_returns_base_seq_when_clean() -> None:
    """When the buffer is clean (flush succeeded), _seq_and_state_after_flush_attempt
    reads the retired head from base_seq and CONSUMES one past it: the terminal
    event's seq must strictly follow the last event the buffer carried, so a
    seq-cursor consumer never dedupes the terminal event into the progress
    event before it."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5, pending_seq_delta=0, dirty=False)
    buf.pending_state = {"step": 3}

    seq, state = _seq_and_state_after_flush_attempt(buf)
    assert seq == 6  # retired head 5, terminal consumes 6
    assert state == {"step": 3}


async def test_seq_and_state_after_flush_attempt_falls_back_when_dirty() -> None:
    """When the buffer is still dirty (flush failed silently), _seq_and_state_after_flush_attempt
    keeps base_seq + pending_seq_delta as the head so the pending
    delta is NOT lost in the terminal write, and consumes one past it for
    the terminal event's own seq."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5, pending_seq_delta=3, dirty=True)
    buf.pending_state = {"step": 8}

    seq, state = _seq_and_state_after_flush_attempt(buf)
    assert seq == 9  # head 5 + 3, terminal consumes 9
    assert state == {"step": 8}


async def test_seq_and_state_after_flush_attempt_returns_zero_when_none() -> None:
    """When the buffer is None (no buffer registered), returns (0, None)."""
    seq, state = _seq_and_state_after_flush_attempt(None)
    assert seq == 0
    assert state is None


async def test_seq_and_state_after_flush_attempt_dirty_zero_delta() -> None:
    """When the buffer is dirty but pending_seq_delta is 0, the write still
    reads the head from base_seq (not losing state), which is correct for the
    edge case where only pending_state was updated without a delta increment,
    and consumes one past it for the terminal event's seq."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=10, pending_seq_delta=0, dirty=True)
    buf.pending_state = {"percent": 50.0}

    seq, state = _seq_and_state_after_flush_attempt(buf)
    assert seq == 11  # head 10, terminal consumes 11
    assert state == {"percent": 50.0}


# ── _terminal_seq_and_state: consumes one past the head ───────────────────


async def test_terminal_seq_and_state_clean_buffer_returns_base_seq() -> None:
    """When the buffer is clean (post-flush, base_seq=5, pending_seq_delta=0),
    _terminal_seq_and_state returns (6, state) - NOT (0, {}).
    This is the exact window where _snapshot_progress would incorrectly
    return 0, clobbering the previously-flushed sequence; the retired head
    is 5 and the state-change write consumes 6."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5, pending_seq_delta=0, dirty=False)
    buf.pending_state = {"step": 3}

    seq, state = _terminal_seq_and_state(buf)
    assert seq == 6
    assert state == {"step": 3}


async def test_terminal_seq_and_state_dirty_buffer_returns_sum() -> None:
    """When the buffer is dirty (pre-flush), _terminal_seq_and_state reads
    the head at base_seq + pending_seq_delta (the pending delta must not be
    lost) and consumes one past it for the state-change event's seq."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5, pending_seq_delta=3, dirty=True)
    buf.pending_state = {"step": 8}

    seq, state = _terminal_seq_and_state(buf)
    assert seq == 9
    assert state == {"step": 8}


async def test_terminal_seq_and_state_none_buffer_returns_zero() -> None:
    """When the buffer is None, returns (0, {})."""
    seq, state = _terminal_seq_and_state(None)
    assert seq == 0
    assert state == {}


async def test_terminal_seq_and_state_clean_zero_delta() -> None:
    """When base_seq > 0 and pending_seq_delta == 0 and buffer is clean,
    _terminal_seq_and_state consumes one past base_seq (the authoritative
    flushed value): the state-change event strictly follows the flushed
    progress event instead of repeating its seq."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=10, pending_seq_delta=0, dirty=False)
    buf.pending_state = {"step": 10}

    seq, state = _terminal_seq_and_state(buf)
    assert seq == 11
    assert state == {"step": 10}


async def test_consume_state_change_seq_stacks_on_the_head() -> None:
    """A running transition's consumption (see _consume_state_change_seq)
    sits on a CLEAN buffer as pending_seq_delta=1: the running event carries
    base + 1, and the terminal write stacks one past it, never repeating
    the running event's seq."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5, pending_seq_delta=0, dirty=False)
    running_seq = _consume_state_change_seq(buf)
    assert running_seq == 6  # head 5, the running event consumes 6
    assert buf.dirty is False  # a consumption alone never dirties the buffer
    assert buf.pending_seq_delta == 1
    seq, _state = _terminal_seq_and_state(buf)
    assert seq == 7  # the terminal consumes one past the running event
    assert buf.pending_seq_delta == 1  # the projection consumes; it never mutates


async def test_consume_state_change_seq_none_buffer_consumes_nothing() -> None:
    assert _consume_state_change_seq(None) == 0
