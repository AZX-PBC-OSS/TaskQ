"""Unit and Hypothesis tests for _flush_buffer, progress_flush_loop, and edge cases."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st

from taskq.progress._buffer import _progress_after_flush, _ProgressBuffer, _snapshot_progress
from taskq.progress._flush import _flush_buffer, _flush_buffer_immediate, progress_flush_loop

_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000001")
_WORKER_ID = UUID("11111111-2222-3333-4444-555555555555")


def _make_pool_mock(*, returning_row: dict[str, object] | None = None) -> MagicMock:
    """Return a pool mock whose acquire() acts as an async context manager."""
    pool, _conn = _make_pool_with_conn(returning_row=returning_row)
    return pool


def _make_pool_with_conn(
    *, returning_row: dict[str, object] | None = None
) -> tuple[MagicMock, AsyncMock]:
    """Return (pool, conn) mocks whose acquire() is an async context manager."""
    conn = AsyncMock()
    conn.fetchrow.return_value = returning_row

    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire
    return pool, conn


def _make_dirty_buffer(*, base_seq: int = 0, delta: int = 2) -> _ProgressBuffer:
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=base_seq)
    buf.pending_seq_delta = delta
    buf.pending_state["step"] = 1
    buf.dirty = True
    return buf


# ── base_seq + pending_seq_delta is strictly non-decreasing ─────────


@given(
    base_seq=st.integers(min_value=0, max_value=1_000_000),
    delta=st.integers(min_value=1, max_value=1_000),
)
@hyp_settings(max_examples=200)
async def test_flush_preserves_monotone_seq(base_seq: int, delta: int) -> None:
    """After a successful flush, base_seq == returned_seq and delta == 0.
    seq never decrements."""
    returned_seq = base_seq + delta
    pool = _make_pool_mock(returning_row={"progress_seq": returned_seq})

    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=base_seq)
    buf.pending_seq_delta = delta
    buf.pending_state["step"] = 1
    buf.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}

    await _flush_buffer(pool, "taskq_test", _JOB_ID, _WORKER_ID, buf, buffers)

    assert buf.base_seq == returned_seq
    assert buf.pending_seq_delta == 0
    assert buf.dirty is False


# ── data exactly max+1 bytes raises ProgressTooLarge(16384, 16385) ──


async def test_progress_too_large_at_exactly_max_plus_one_byte() -> None:
    """ctx.progress(data=...) where serialised data is exactly 16385 bytes
    raises ProgressTooLarge(limit=16384, actual=16385)."""
    import asyncio
    from datetime import UTC, datetime

    import structlog

    from taskq._json import dumps
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.context import JobContext
    from taskq.exceptions import ProgressTooLarge
    from taskq.obs import bind_job_context
    from taskq.settings import WorkerSettings
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload

    settings = WorkerSettings.load_from_dict({"TASKQ_PROGRESS_DATA_MAX_BYTES": "16384"})
    backend = InMemoryBackend(clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)))
    job_id = UUID("00000000-0000-0000-0000-aabbccddeeff")
    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {job_id: buf}

    # Build data whose JSON serialisation is exactly 16385 bytes.
    # {"x": "..."} with padding adjusted to hit exactly 16385.
    overhead = len(dumps({"x": ""}))  # {"x":""} baseline
    target = 16385
    value = "a" * (target - overhead)
    data: dict[str, object] = {"x": value}
    # Verify we hit the target
    actual_len = len(dumps(data))
    # Adjust if off-by-one due to encoding overhead
    if actual_len < target:
        data = {"x": value + "a" * (target - actual_len)}
    elif actual_len > target:
        data = {"x": value[: -(actual_len - target)]}
    assert len(dumps(data)) == 16385

    ctx: JobContext[PassthroughPayload] = JobContext(
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: fixture helper accesses private field for test setup.
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

    with pytest.raises(ProgressTooLarge) as exc_info:
        await ctx.progress(data=data)

    assert exc_info.value.limit == 16384
    assert exc_info.value.actual == 16385


# ── percent=150.0 is NOT rejected ─────────────────────────────────


async def test_ctx_progress_out_of_range_percent_not_rejected() -> None:
    """ctx.progress(percent=150.0) succeeds — no range validation on percent.
    pending_state["percent"] == 150.0."""
    import asyncio
    from datetime import UTC, datetime

    import structlog

    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.context import JobContext
    from taskq.obs import bind_job_context
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload

    backend = InMemoryBackend(clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)))
    job_id = UUID("00000000-0000-0000-0000-aabbccddee01")
    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {job_id: buf}

    ctx: JobContext[PassthroughPayload] = JobContext(
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: fixture helper accesses private field for test setup.
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
    )

    await ctx.progress(percent=150.0)

    assert buf.pending_state["percent"] == 150.0
    assert buf.dirty is True


# ── unserializable data raises TypeError before ProgressTooLarge ────


async def test_ctx_progress_unserializable_data_raises_type_error() -> None:
    """ctx.progress(data={"bad": object()}) raises TypeError (from JSON
    serialisation) before the ProgressTooLarge check. The buffer is not
    updated."""
    import asyncio
    from datetime import UTC, datetime

    import structlog

    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.context import JobContext
    from taskq.obs import bind_job_context
    from taskq.settings import WorkerSettings
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload

    settings = WorkerSettings.load_from_dict({"TASKQ_PROGRESS_DATA_MAX_BYTES": "16384"})
    backend = InMemoryBackend(clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)))
    job_id = UUID("00000000-0000-0000-0000-aabbccddee02")
    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {job_id: buf}

    ctx: JobContext[PassthroughPayload] = JobContext(
        job_id=job_id,
        actor="test_actor",
        queue="default",
        attempt=1,
        worker_id=backend._worker_id,  # type: ignore[reportPrivateUsage] # Why: fixture helper accesses private field for test setup.
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

    with pytest.raises(TypeError):
        await ctx.progress(data={"bad": object()})

    # Buffer must NOT have been mutated
    assert buf.pending_seq_delta == 0
    assert buf.dirty is False


# ── Flush loop regression tests ────────────────────────────────────────────


async def test_flush_immediate_noop_on_clean_buffer() -> None:
    pool = _make_pool_mock()
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}

    await _flush_buffer_immediate(pool, "taskq_test", _JOB_ID, _WORKER_ID, buffers)

    assert buf.base_seq == 5
    assert buf.dirty is False


async def test_flush_immediate_noop_when_buffer_absent() -> None:
    pool = _make_pool_mock()
    buffers: dict[UUID, _ProgressBuffer] = {}
    await _flush_buffer_immediate(pool, "taskq_test", _JOB_ID, _WORKER_ID, buffers)


async def test_flush_immediate_flushes_dirty_buffer() -> None:
    returned_seq = 5
    pool = _make_pool_mock(returning_row={"progress_seq": returned_seq})

    buf = _make_dirty_buffer(base_seq=3, delta=2)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}

    await _flush_buffer_immediate(pool, "taskq_test", _JOB_ID, _WORKER_ID, buffers)

    assert buf.dirty is False
    assert buf.base_seq == returned_seq
    assert buf.pending_seq_delta == 0


async def test_flush_loop_resolves_pool_via_getter_each_tick() -> None:
    """The loop resolves the pool fresh on every flush — after a credential
    hot-reload swaps the worker pool, flushes must target the new pool,
    not the (drained/closed) startup pool."""
    pool_a, conn_a = _make_pool_with_conn(returning_row={"progress_seq": 2})
    pool_b, conn_b = _make_pool_with_conn(returning_row={"progress_seq": 2})
    current = {"pool": pool_a}

    buffers: dict[UUID, _ProgressBuffer] = {}
    shutdown = asyncio.Event()
    task = asyncio.create_task(
        progress_flush_loop(
            lambda: current["pool"],
            "taskq_test",
            _WORKER_ID,
            buffers,
            0.01,
            shutdown,  # type: ignore[arg-type]
        )
    )
    try:
        buffers[_JOB_ID] = _make_dirty_buffer()
        await asyncio.sleep(0.05)
        current["pool"] = pool_b  # simulate credential hot-reload swap
        buffers[_JOB_ID] = _make_dirty_buffer()
        await asyncio.sleep(0.05)
    finally:
        shutdown.set()
        await task

    assert conn_a.fetchrow.await_count >= 1
    assert conn_b.fetchrow.await_count >= 1  # post-swap flush hit the NEW pool


async def test_flush_loop_raises_on_invalid_schema() -> None:
    pool = _make_pool_mock()
    buffers: dict[UUID, _ProgressBuffer] = {}
    shutdown = asyncio.Event()
    shutdown.set()

    with pytest.raises(ValueError, match="invalid schema identifier"):
        await progress_flush_loop(lambda: pool, "bad schema!", _WORKER_ID, buffers, 0.1, shutdown)


async def test_flush_loop_exits_when_shutdown_set() -> None:
    pool = _make_pool_mock()
    buffers: dict[UUID, _ProgressBuffer] = {}
    shutdown = asyncio.Event()
    shutdown.set()

    await progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown)


async def test_flush_loop_flushes_dirty_buffer_on_tick() -> None:
    returned_seq = 3
    pool = _make_pool_mock(returning_row={"progress_seq": returned_seq})

    buf = _make_dirty_buffer(base_seq=1, delta=2)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    shutdown = asyncio.Event()

    async def _set_shutdown_after_flush() -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(
        progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown),
        _set_shutdown_after_flush(),
    )

    assert buf.dirty is False
    assert buf.base_seq == returned_seq


async def test_flush_loop_skips_clean_buffers() -> None:
    pool = _make_pool_mock()
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=7)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    shutdown = asyncio.Event()

    async def _stop() -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(
        progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown),
        _stop(),
    )

    assert buf.base_seq == 7
    assert buf.dirty is False


async def test_flush_loop_removes_buffer_when_row_gone() -> None:
    pool = _make_pool_mock(returning_row=None)

    buf = _make_dirty_buffer(base_seq=0, delta=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    shutdown = asyncio.Event()

    async def _stop() -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(
        progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown),
        _stop(),
    )

    assert _JOB_ID not in buffers


async def test_flush_loop_continues_after_per_job_exception() -> None:
    """If one buffer raises an unexpected exception, the loop continues for others."""
    import asyncpg

    bad_id = UUID("bad00000-0000-0000-0000-000000000000")
    good_id = UUID("600d0000-0000-0000-0000-000000000000")

    bad_buf = _make_dirty_buffer(base_seq=0, delta=1)
    bad_buf.job_id = bad_id
    good_buf = _ProgressBuffer(job_id=good_id, base_seq=0)
    good_buf.dirty = True
    good_buf.pending_seq_delta = 1

    returned_seq = 5
    conn = AsyncMock()
    call_count = 0

    async def _fetchrow(*args: object) -> dict[str, object] | None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise asyncpg.PostgresError("simulated pg error")
        return {"progress_seq": returned_seq}

    conn.fetchrow.side_effect = _fetchrow

    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire

    buffers: dict[UUID, _ProgressBuffer] = {bad_id: bad_buf, good_id: good_buf}
    shutdown = asyncio.Event()

    async def _stop() -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(
        progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown),
        _stop(),
    )

    assert good_buf.dirty is False
    assert good_buf.base_seq == returned_seq


# ── _snapshot_progress regression tests ───────────────────────────────────────


async def test_snapshot_progress_returns_zero_empty_for_none_buffer() -> None:
    seq, state = _snapshot_progress(None)
    assert seq == 0
    assert state == {}


async def test_snapshot_progress_returns_zero_empty_for_clean_buffer() -> None:
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5)
    seq, state = _snapshot_progress(buf)
    assert seq == 0
    assert state == {}


async def test_snapshot_progress_returns_accumulated_for_dirty_buffer() -> None:
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=3)
    buf.pending_seq_delta = 2
    buf.pending_state["step"] = 7
    buf.pending_state["percent"] = 42.0
    buf.dirty = True
    seq, state = _snapshot_progress(buf)
    assert seq == 5
    assert state == {"step": 7, "percent": 42.0}


async def test_snapshot_progress_returns_copy_of_pending_state() -> None:
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0)
    buf.pending_seq_delta = 1
    buf.pending_state["step"] = 1
    buf.dirty = True
    _, state = _snapshot_progress(buf)
    state["extra"] = True
    assert "extra" not in buf.pending_state


# ── _progress_after_flush tests ──────────────────────────────────────────


async def test_progress_after_flush_returns_base_seq_for_clean_buffer() -> None:
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5)
    buf.pending_state["step"] = 3
    seq, state = _progress_after_flush(buf)
    assert seq == 5
    assert state == {"step": 3}


async def test_progress_after_flush_returns_base_seq_for_dirty_buffer() -> None:
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5)
    buf.pending_seq_delta = 3
    buf.pending_state["step"] = 3
    buf.dirty = True
    seq, state = _progress_after_flush(buf)
    assert seq == 5
    assert state == {"step": 3}


async def test_progress_after_flush_returns_zero_empty_for_none() -> None:
    seq, state = _progress_after_flush(None)
    assert seq == 0
    assert state == {}


async def test_progress_after_flush_returns_copy_of_pending_state() -> None:
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=10)
    buf.pending_state["step"] = 1
    _, state = _progress_after_flush(buf)
    state["extra"] = True
    assert "extra" not in buf.pending_state
