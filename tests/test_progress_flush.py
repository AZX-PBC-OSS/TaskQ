"""Unit and Hypothesis tests for _flush_buffer, progress_flush_loop, and edge cases.

The tick-level pins hold the loop to its single-statement contract: one
batched multi-row UPDATE per tick carries every dirty buffer's row, with
the fencing gate applied per-row over the unnest arrays.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import orjson
import pytest
import structlog
from hypothesis import given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st

from taskq._ids import new_uuid
from taskq._json import dumps, dumps_jsonb_str, embed_encoded
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.progress._buffer import (
    _progress_after_flush,
    _ProgressBuffer,
    _seq_and_state_after_flush_attempt,
    _snapshot_progress,
)
from taskq.progress._flush import (
    _FLUSH_BATCH_ROWS,  # pyright: ignore[reportPrivateUsage]  # Why: the pin asserts the batch bound itself - the doctrine constant is the contract under test.
    _FLUSH_MAX_BATCHES_PER_TICK,  # pyright: ignore[reportPrivateUsage]  # Why: the pin asserts the tick cap itself - the doctrine constant is the contract under test.
    _flush_buffer,
    _flush_buffer_immediate,
    _flush_dirty_set,
    progress_flush_loop,
)
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload
from tests._progress_context import make_progress_context

_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000001")
_JOB_ID_B = UUID("aaaaaaaa-bbbb-cccc-dddd-000000000002")
_WORKER_ID = UUID("11111111-2222-3333-4444-555555555555")
_POOL_SIZE = 4
"""Reported by every pool double so the double mirrors a real asyncpg
pool's surface; the tick's batched flush statement reads no pool size."""


def _make_pool_mock(*, returning_row: dict[str, object] | None = None) -> MagicMock:
    """Return a pool mock whose acquire() acts as an async context manager."""
    pool, _conn = _make_pool_with_conn(returning_row=returning_row)
    return pool


def _make_pool_with_conn(
    *, returning_row: dict[str, object] | None = None
) -> tuple[MagicMock, AsyncMock]:
    """Return (pool, conn) doubles for both flush statement surfaces.

    ``conn.fetchrow`` answers the single-row statement (the immediate and
    crash-flush paths); ``conn.fetch`` answers the tick's batched
    statement, echoing one ``{"id", "progress_seq"}`` RETURNING row per
    job id found in the call's unnest id array - ``returning_row=None``
    fences every row out (empty RETURNING).
    """
    conn = AsyncMock()
    conn.fetchrow.return_value = returning_row

    async def _fetch(*args: object) -> list[dict[str, object]]:
        # The tick's batched UPDATE binds (sql, job_ids, deltas, states, attempts, worker).
        job_ids = args[1] if len(args) > 1 else None
        if returning_row is None or not isinstance(job_ids, list):
            return []
        returned_seq = returning_row.get("progress_seq", 0)
        typed_ids = cast("list[UUID]", job_ids)  # pyright: ignore[reportUnknownVariableType]  # Why: the fake conn's *args are object; the pins only ever bind UUID lists.
        return [{"id": job_id, "progress_seq": returned_seq} for job_id in typed_ids]

    conn.fetch.side_effect = _fetch

    pool = MagicMock()
    pool.get_size.return_value = _POOL_SIZE

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

    import structlog

    from taskq._json import dumps
    from taskq.exceptions import ProgressTooLarge
    from taskq.settings import WorkerSettings

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
    """ctx.progress(percent=150.0) succeeds - no range validation on percent.
    pending_state["percent"] == 150.0."""
    import asyncio

    import structlog

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

    import structlog

    from taskq.settings import WorkerSettings

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


async def test_flush_buffer_rejects_nul_before_touching_connection() -> None:
    """A NUL byte in progress state - as arbitrary ``ctx.progress(**kwargs)``
    calls can produce - must be rejected by the jsonb NUL guard
    (``dumps_jsonb_str``) before the connection is ever touched. Postgres
    ``jsonb_in`` rejects a stored NUL with ``UntranslatableCharacterError``
    (asyncpg.PostgresError), which is exactly the exception class the
    terminal-write path treats as retryable infra failure - so the guard
    must fire first, deterministically, without reaching the DB at all."""
    pool, conn = _make_pool_with_conn()
    buf = _make_dirty_buffer()
    buf.pending_state["detail"] = "bad\x00value"
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}

    await _flush_buffer(pool, "taskq_test", _JOB_ID, _WORKER_ID, buf, buffers)

    conn.fetchrow.assert_not_awaited()
    # The buffer is left dirty (not falsely marked flushed) so the state is
    # never silently discarded.
    assert buf.dirty is True


# ── The flush binds the data bytes ctx.progress already encoded ────


def _encodes_of(data: dict[str, object], encoded: list[object]) -> int:
    """How many orjson encodes walked *data*, at top level or as a value."""
    return sum(
        1
        for value in encoded
        if value is data or (isinstance(value, dict) and any(v is data for v in value.values()))  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]  # Why: the recorded encoder arguments are untyped; only identity is inspected.
    )


async def _flush_one(
    surface: str,
    pool: MagicMock,
    conn: AsyncMock,
    job_id: UUID,
    buffers: dict[UUID, _ProgressBuffer],
) -> str:
    """Flush *job_id* through one of the two statement surfaces; return the bound state document."""
    buffer = buffers[job_id]
    if surface == "single_row":
        await _flush_buffer(pool, "taskq_test", job_id, _WORKER_ID, buffer, buffers)
        bound = conn.fetchrow.await_args
    else:
        await _flush_dirty_set(pool, "taskq_test", _WORKER_ID, buffers, [(job_id, buffer)])
        bound = conn.fetch.await_args
    assert bound is not None
    state_docs = cast("list[str]", bound.args[3])
    return state_docs[0]


@pytest.mark.parametrize("surface", ["single_row", "tick_batch"])
async def test_flush_binds_the_data_bytes_ctx_progress_already_encoded(
    monkeypatch: pytest.MonkeyPatch, surface: str
) -> None:
    """``ctx.progress`` encodes ``data`` once to enforce the size cap; the
    flush binds those bytes rather than walking the dict a second time,
    and the document it binds is byte-identical to a fresh encode of the
    snapshot - key order, nesting and escaping included."""
    encoded: list[object] = []
    real_dumps = orjson.dumps

    def recording_dumps(value: object, *args: object, **kwargs: object) -> bytes:
        encoded.append(value)
        return real_dumps(value, *args, **kwargs)  # type: ignore[arg-type]  # Why: pass-through of orjson's own keyword options.

    monkeypatch.setattr(orjson, "dumps", recording_dumps)

    job_id = UUID("00000000-0000-0000-0000-aabbccddee02")
    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {job_id: buf}
    ctx = make_progress_context(buffers, job_id, settings=WorkerSettings.load_from_dict({}))
    data: dict[str, object] = {
        "rows": [{"id": i, "name": f"item-{i}", "u": "é\n"} for i in range(50)]
    }

    await ctx.progress(step=1, detail="halfway", data=data)
    pool, conn = _make_pool_with_conn(returning_row={"progress_seq": 1})
    bound_doc = await _flush_one(surface, pool, conn, job_id, buffers)
    data_encodes = _encodes_of(data, encoded)

    assert bound_doc == dumps_jsonb_str({"step": 1, "detail": "halfway", "data": data})
    assert buf.dirty is False
    assert data_encodes == 1, "data was encoded again for the flush"


_json_ints = st.integers(min_value=-(2**63), max_value=2**64 - 1)
_json_scalars = st.none() | st.booleans() | _json_ints | st.floats(allow_nan=False) | st.text()
_json_values = st.recursive(
    _json_scalars,
    lambda children: (
        st.lists(children, max_size=4) | st.dictionaries(st.text(), children, max_size=4)
    ),
    max_leaves=12,
)


@given(data=st.dictionaries(st.text(), _json_values, max_size=4), step=_json_ints)
@hyp_settings(max_examples=200)
def test_embedded_data_bytes_render_the_same_document(data: dict[str, object], step: int) -> None:
    """Embedding ``dumps(data)`` in the state document is byte-identical to
    encoding ``data`` in place, for any JSON-shaped ``data`` - a NUL
    included, so the jsonb guard's byte scan sees the same bytes."""
    in_place = dumps({"step": step, "data": data})
    embedded = dumps({"step": step, "data": embed_encoded(dumps(data))})
    assert embedded == in_place


@pytest.mark.parametrize("surface", ["single_row", "tick_batch"])
async def test_flush_rejects_a_nul_inside_pre_encoded_data(surface: str) -> None:
    """The jsonb NUL guard fires on the bytes the flush binds, whether it
    encoded them itself or reused ``ctx.progress``'s: the statement never
    reaches the connection and the buffer stays dirty."""
    job_id = UUID("00000000-0000-0000-0000-aabbccddee03")
    buf = _ProgressBuffer(job_id=job_id, base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {job_id: buf}
    ctx = make_progress_context(buffers, job_id, settings=WorkerSettings.load_from_dict({}))

    await ctx.progress(data={"path": "bad\x00value"})
    pool, conn = _make_pool_with_conn(returning_row={"progress_seq": 1})
    if surface == "single_row":
        await _flush_buffer(pool, "taskq_test", job_id, _WORKER_ID, buf, buffers)
    else:
        await _flush_dirty_set(pool, "taskq_test", _WORKER_ID, buffers, [(job_id, buf)])

    conn.fetchrow.assert_not_awaited()
    conn.fetch.assert_not_awaited()
    assert buf.dirty is True


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
    """The loop resolves the pool fresh on every flush - after a credential
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

    assert conn_a.fetch.await_count >= 1
    assert conn_b.fetch.await_count >= 1  # post-swap flush hit the NEW pool


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


async def test_flush_loop_fenced_out_row_dropped_while_sibling_flushes() -> None:
    """A row whose gate does not match (reclaimed, terminal, or owned by a
    later attempt epoch) is simply absent from the batch's RETURNING: it
    does not update, its buffer is dropped, and - because there is no
    per-buffer statement - its fence cannot fail the statement carrying
    the sibling row.
    """
    reclaimed_id = UUID("bad00000-0000-0000-0000-000000000000")
    live_id = UUID("600d0000-0000-0000-0000-000000000000")

    conn = AsyncMock()

    async def _fetch(*args: object) -> list[dict[str, object]]:
        # Only the live job's row clears the gate and RETURNS.
        job_ids = args[1] if len(args) > 1 else None
        if not isinstance(job_ids, list) or live_id not in job_ids:
            return []
        return [{"id": live_id, "progress_seq": 5}]

    conn.fetch.side_effect = _fetch

    pool = MagicMock()
    pool.get_size.return_value = _POOL_SIZE

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire

    reclaimed_buf = _ProgressBuffer(job_id=reclaimed_id, base_seq=0)
    reclaimed_buf.pending_seq_delta = 1
    reclaimed_buf.pending_state["step"] = 1
    reclaimed_buf.dirty = True
    live_buf = _ProgressBuffer(job_id=live_id, base_seq=0)
    live_buf.pending_seq_delta = 1
    live_buf.pending_state["step"] = 1
    live_buf.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {reclaimed_id: reclaimed_buf, live_id: live_buf}
    shutdown = asyncio.Event()

    async def _stop() -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(
        progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown),
        _stop(),
    )

    assert reclaimed_id not in buffers, (
        "the fenced-out row's buffer must be dropped - the gate no-op'd for that job"
    )
    assert live_buf.dirty is False
    assert live_buf.base_seq == 5


# ── Single-statement batched flush per tick ────────────────────────────


async def test_flush_tick_batches_all_dirty_buffers_into_one_statement() -> None:
    """One flush tick reaches the backend exactly once regardless of dirty
    count: every dirty buffer's row rides ONE batched multi-row UPDATE -
    columnar arrays (unnest-array form) carry both job ids with their
    per-row deltas and attempt epochs. The recorded statement's unnest
    arrays keep the fencing gate per-row over the unnest rows, and both
    rows update.
    """
    conn = AsyncMock()

    async def _fetch(*args: object) -> list[dict[str, object]]:
        job_ids = args[1] if len(args) > 1 else None
        if not isinstance(job_ids, list):
            return []
        typed_ids = cast("list[UUID]", job_ids)  # pyright: ignore[reportUnknownVariableType]  # Why: the fake conn's *args are object; the pins only ever bind UUID lists.
        return [{"id": job_id, "progress_seq": 9} for job_id in typed_ids]

    conn.fetch.side_effect = _fetch

    pool = MagicMock()
    pool.get_size.return_value = _POOL_SIZE

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire

    buf_a = _make_dirty_buffer()
    buf_a.attempt = 3
    buf_b = _ProgressBuffer(job_id=_JOB_ID_B, base_seq=0)
    buf_b.pending_seq_delta = 2
    buf_b.pending_state["step"] = 1
    buf_b.attempt = 5
    buf_b.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf_a, _JOB_ID_B: buf_b}

    shutdown = asyncio.Event()

    async def _stop() -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(
        progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown),
        _stop(),
    )

    assert conn.fetch.await_count == 1, (
        f"the tick issued {conn.fetch.await_count} statements for 2 dirty buffers - "
        "the contract is one batched multi-row statement per tick"
    )
    conn.fetchrow.assert_not_awaited()  # the tick path has no per-buffer statement

    call_args = conn.fetch.await_args_list[0].args
    sql, job_ids, seq_deltas, _state_docs, attempts, worker_arg = (
        call_args[0],
        call_args[1],
        call_args[2],
        call_args[3],
        call_args[4],
        call_args[5],
    )
    assert isinstance(sql, str)
    assert "unnest(" in sql, "the statement must carry its rows columnar-style in unnest arrays"
    assert "j.status = 'running'" in sql, "the per-row running gate must stay in the statement"
    assert "j.locked_by_worker = $5::uuid" in sql, (
        "the per-row worker gate must stay in the statement"
    )
    assert "j.attempt = f.attempt" in sql, "the per-row attempt-epoch gate conjunct is missing"
    assert "RETURNING j.id, j.progress_seq" in sql, (
        "the retire protocol keys on which job ids came back - RETURNING must carry them"
    )
    assert set(job_ids) == {_JOB_ID, _JOB_ID_B}, (
        f"one statement must carry both dirty buffers' rows; got {job_ids!r}"
    )
    assert dict(zip(job_ids, seq_deltas, strict=True)) == {_JOB_ID: 2, _JOB_ID_B: 2}, (
        f"the id→delta columnar pairing is broken; got deltas {seq_deltas!r}"
    )
    assert dict(zip(job_ids, attempts, strict=True)) == {_JOB_ID: 3, _JOB_ID_B: 5}, (
        f"each row must carry its buffer's attempt epoch; got attempts {attempts!r}"
    )
    assert worker_arg == _WORKER_ID

    # Both rows updated: both buffers adopted the returned seq and retired.
    assert buf_a.dirty is False
    assert buf_a.base_seq == 9
    assert buf_b.dirty is False
    assert buf_b.base_seq == 9


async def test_flush_tick_drains_in_bounded_batches_with_a_tick_cap() -> None:
    """The bounded-batch doctrine: no flush statement ever carries more than
    ``_FLUSH_BATCH_ROWS`` rows, and no tick issues more than
    ``_FLUSH_MAX_BATCHES_PER_TICK`` batches - an all-in-one statement
    over the whole dirty set is the long-running-statement trap (it
    times out as a whole and stalls the tick). 70 dirty buffers drain
    as exactly ceil(70/32) = 3 bounded statements; 300 drain as the
    tick-capped 8 statements, leaving the remainder dirty for the next
    tick - the incremental-commit discipline the leader sweeps apply."""
    conn = AsyncMock()

    async def _fetch(*args: object) -> list[dict[str, object]]:
        job_ids = args[1] if len(args) > 1 else None
        if not isinstance(job_ids, list):
            return []
        typed_ids = cast("list[UUID]", job_ids)  # pyright: ignore[reportUnknownVariableType]  # Why: the fake conn's *args are object; the pins only ever bind UUID lists.
        return [{"id": job_id, "progress_seq": 9} for job_id in typed_ids]

    conn.fetch.side_effect = _fetch

    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire

    def _dirty_buffers(n: int) -> dict[UUID, _ProgressBuffer]:
        out: dict[UUID, _ProgressBuffer] = {}
        for _ in range(n):
            job_id = new_uuid()
            buf = _ProgressBuffer(job_id=job_id, base_seq=0)
            buf.pending_seq_delta = 1
            buf.pending_state["step"] = 1
            buf.attempt = 2
            buf.dirty = True
            out[job_id] = buf
        return out

    buffers = _dirty_buffers(70)
    shutdown = asyncio.Event()

    async def _stop() -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(
        progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown),
        _stop(),
    )

    assert conn.fetch.await_count == 3, (
        f"70 dirty buffers must drain as ceil(70/{_FLUSH_BATCH_ROWS}) = 3 bounded "
        f"statements; got {conn.fetch.await_count}"
    )
    for call in conn.fetch.await_args_list:
        assert len(call.args[1]) <= _FLUSH_BATCH_ROWS, (
            f"a flush statement carried {len(call.args[1])} rows - the batch bound "
            f"({_FLUSH_BATCH_ROWS}) is the doctrine's guarantee that no "
            "statement runs long"
        )
    assert all(buf.dirty is False for buf in buffers.values())

    # 300 dirty buffers: the tick cap holds - 8 batches, 256 rows, the
    # remainder stays dirty for the next tick.
    conn2 = AsyncMock()
    conn2.fetch.side_effect = _fetch
    pool2 = MagicMock()

    @asynccontextmanager
    async def _acquire2() -> AsyncGenerator[AsyncMock, None]:
        yield conn2

    pool2.acquire = _acquire2

    buffers2 = _dirty_buffers(300)
    shutdown2 = asyncio.Event()

    async def _stop2() -> None:
        await asyncio.sleep(0.05)
        shutdown2.set()

    await asyncio.gather(
        progress_flush_loop(lambda: pool2, "taskq_test", _WORKER_ID, buffers2, 0.15, shutdown2),
        _stop2(),
    )

    assert conn2.fetch.await_count == _FLUSH_MAX_BATCHES_PER_TICK, (
        f"300 dirty buffers must issue at most {_FLUSH_MAX_BATCHES_PER_TICK} batches "
        f"in one tick; got {conn2.fetch.await_count} - the tick must not be "
        "monopolised by a huge dirty set"
    )
    still_dirty = [buf for buf in buffers2.values() if buf.dirty]
    assert len(still_dirty) == 300 - _FLUSH_MAX_BATCHES_PER_TICK * _FLUSH_BATCH_ROWS, (
        f"the remainder ({300 - _FLUSH_MAX_BATCHES_PER_TICK * _FLUSH_BATCH_ROWS} "
        f"buffers) must stay dirty for the next tick; got {len(still_dirty)} dirty"
    )


async def test_flush_tick_cost_is_flat_up_to_the_batch_bound() -> None:
    """A tick's database cost does not grow with the number of dirty
    buffers, up to the batch bound: one statement and one connection
    checkout, whether one buffer is dirty or the bound's worth are.

    Why it matters operationally: progress reporting is the surface a
    chatty actor drives hardest, and a tick whose cost tracked the dirty
    count would turn a busy worker's own progress calls into the thing
    that stalls its progress loop - a feedback loop that gets worse
    exactly when an operator is watching progress because jobs are slow.
    The flat shape is what makes the coalescing interval, not the
    concurrency, the thing that sizes the flush load.
    """
    per_size_costs: dict[int, tuple[int, int]] = {}

    for dirty_count in (1, 2, _FLUSH_BATCH_ROWS // 2, _FLUSH_BATCH_ROWS):
        conn = AsyncMock()

        async def _fetch(*args: object) -> list[dict[str, object]]:
            job_ids = args[1] if len(args) > 1 else None
            if not isinstance(job_ids, list):
                return []
            typed_ids = cast("list[UUID]", job_ids)  # pyright: ignore[reportUnknownVariableType]  # Why: the fake conn's *args are object; the pins only ever bind UUID lists.
            return [{"id": job_id, "progress_seq": 9} for job_id in typed_ids]

        conn.fetch.side_effect = _fetch

        acquires = 0
        pool = MagicMock()
        pool.get_size.return_value = _POOL_SIZE

        @asynccontextmanager
        async def _acquire(_conn: AsyncMock = conn) -> AsyncGenerator[AsyncMock, None]:
            nonlocal acquires
            acquires += 1
            yield _conn

        pool.acquire = _acquire

        buffers: dict[UUID, _ProgressBuffer] = {}
        for _ in range(dirty_count):
            job_id = new_uuid()
            buf = _ProgressBuffer(job_id=job_id, base_seq=0)
            buf.pending_seq_delta = 1
            buf.pending_state["step"] = 1
            buf.attempt = 2
            buf.dirty = True
            buffers[job_id] = buf

        shutdown = asyncio.Event()

        async def _stop(_event: asyncio.Event = shutdown) -> None:
            await asyncio.sleep(0.05)
            _event.set()

        await asyncio.gather(
            progress_flush_loop(
                lambda _pool=pool: _pool,  # pyright: ignore[reportUnknownLambdaType, reportUnknownArgumentType]  # Why: default-bound so each loop iteration's pool double is captured, not the last one.
                "taskq_test",
                _WORKER_ID,
                buffers,
                0.15,
                shutdown,
            ),
            _stop(),
        )

        assert all(buf.dirty is False for buf in buffers.values()), (
            f"{dirty_count} dirty buffers did not all drain in one tick"
        )
        per_size_costs[dirty_count] = (conn.fetch.await_count, acquires)

    assert set(per_size_costs.values()) == {(1, 1)}, (
        "a tick's cost must be one statement on one connection checkout for any "
        f"dirty count up to the batch bound ({_FLUSH_BATCH_ROWS}); measured "
        f"(statements, acquires) per dirty count: {per_size_costs} - a cost that "
        "scales with the dirty count makes a busy worker's own progress calls "
        "stall its flush loop"
    )


async def test_flush_failing_batch_leaves_only_its_own_buffers_dirty() -> None:
    """Per-batch failure isolation - the doctrine's other half: a
    failing batch is that batch's failure alone. 70 dirty buffers in 3
    bounded batches; the middle batch's statement fails - its 32
    buffers stay dirty with deltas intact while the first and third
    batches' buffers flush in the same tick. The single-statement shape
    made every dirty job lose its flush together; the bounded shape
    keeps the blast radius at one batch."""
    conn = AsyncMock()
    fetch_calls: list[list[object]] = []

    async def _fetch(*args: object) -> list[dict[str, object]]:
        job_ids = args[1] if len(args) > 1 else None
        if not isinstance(job_ids, list):
            return []
        typed_ids = cast("list[UUID]", job_ids)  # pyright: ignore[reportUnknownVariableType]  # Why: the fake conn's *args are object; the pins only ever bind UUID lists.
        fetch_calls.append(list(typed_ids))
        if len(fetch_calls) == 2:
            raise RuntimeError("middle batch infra failure")
        return [{"id": job_id, "progress_seq": 9} for job_id in typed_ids]

    conn.fetch.side_effect = _fetch

    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire

    buffers: dict[UUID, _ProgressBuffer] = {}
    for _ in range(70):
        job_id = new_uuid()
        buf = _ProgressBuffer(job_id=job_id, base_seq=0)
        buf.pending_seq_delta = 1
        buf.pending_state["step"] = 1
        buf.attempt = 2
        buf.dirty = True
        buffers[job_id] = buf

    shutdown = asyncio.Event()

    async def _stop() -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    await asyncio.gather(
        progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.15, shutdown),
        _stop(),
    )

    assert len(fetch_calls) == 3, (
        "all three bounded batches must run despite the middle one failing"
    )
    failed_ids = {job_id for job_id in fetch_calls[1] if isinstance(job_id, UUID)}
    for job_id, buf in buffers.items():
        if job_id in failed_ids:
            assert buf.dirty is True, (
                "the failed batch's buffers must stay dirty with deltas intact"
            )
            assert buf.pending_seq_delta == 1
        else:
            assert buf.dirty is False, "the other batches' buffers must have flushed"


async def test_flush_loop_failing_statement_leaves_both_buffers_dirty_and_survives() -> None:
    """The tick's single batched statement is one failure surface: when it
    fails, BOTH dirty buffers stay dirty with their deltas intact for the
    next tick, and the loop survives to re-issue the batch - which then
    flushes both rows.
    """
    import asyncpg

    bad_id = UUID("bad00000-0000-0000-0000-000000000000")
    good_id = UUID("600d0000-0000-0000-0000-000000000000")

    statement_calls = 0
    first_failure_seen = asyncio.Event()
    conn = AsyncMock()

    async def _fetch(*args: object) -> list[dict[str, object]]:
        nonlocal statement_calls
        statement_calls += 1
        if statement_calls == 1:
            first_failure_seen.set()
            raise asyncpg.PostgresError("simulated batched flush UPDATE failure")
        job_ids = args[1] if len(args) > 1 else None
        if not isinstance(job_ids, list):
            return []
        typed_ids = cast("list[UUID]", job_ids)  # pyright: ignore[reportUnknownVariableType]  # Why: the fake conn's *args are object; the pins only ever bind UUID lists.
        return [{"id": job_id, "progress_seq": 4} for job_id in typed_ids]

    conn.fetch.side_effect = _fetch

    pool = MagicMock()
    pool.get_size.return_value = _POOL_SIZE

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire

    bad_buf = _ProgressBuffer(job_id=bad_id, base_seq=0)
    bad_buf.pending_seq_delta = 1
    bad_buf.pending_state["step"] = 1
    bad_buf.dirty = True
    good_buf = _ProgressBuffer(job_id=good_id, base_seq=0)
    good_buf.pending_seq_delta = 1
    good_buf.pending_state["step"] = 1
    good_buf.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {bad_id: bad_buf, good_id: good_buf}

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown)
    )
    try:
        try:
            async with asyncio.timeout(2.0):
                await first_failure_seen.wait()
        except TimeoutError:
            pytest.fail("the tick never issued its batched statement (fetch was not called)")
        # Let the failure handler finish; the buffers must be untouched by it.
        for _ in range(4):
            await asyncio.sleep(0)
        assert bad_buf.dirty is True, "a failed batch must leave every dirty buffer dirty"
        assert bad_buf.pending_seq_delta == 1, "the failed batch must not retire any delta"
        assert bad_buf.pending_state == {"step": 1}
        assert good_buf.dirty is True
        assert good_buf.pending_seq_delta == 1
        assert good_buf.pending_state == {"step": 1}
        async with asyncio.timeout(2.0):
            while bad_buf.dirty or good_buf.dirty:  # noqa: ASYNC110  # Why: polling observable mock-DB state (buffer.dirty) that carries no event to await; bounded by the surrounding asyncio.timeout.
                await asyncio.sleep(0.005)
    finally:
        shutdown.set()
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
        except (TimeoutError, asyncio.CancelledError):
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    assert statement_calls == 2, "the loop must re-issue the batch on the next tick"
    assert bad_buf.dirty is False
    assert bad_buf.base_seq == 4
    assert good_buf.dirty is False
    assert good_buf.base_seq == 4


async def test_flush_loop_pool_acquire_failure_keeps_buffers_dirty_with_pool_kind() -> None:
    """A pool-stage failure (acquire raises or exhausts) loses every dirty
    job's flush that tick: both buffers stay dirty with deltas intact for
    the next tick, and the failure is labeled with the pool event/kind -
    the same taxonomy the loop's pool_getter handler uses.
    """
    import asyncpg

    bad_id = UUID("bad00000-0000-0000-0000-000000000000")
    good_id = UUID("600d0000-0000-0000-0000-000000000000")

    pool = MagicMock()
    pool.get_size.return_value = _POOL_SIZE

    def _acquire() -> object:
        raise asyncpg.PostgresConnectionError("pool acquire failed")

    pool.acquire = _acquire

    bad_buf = _ProgressBuffer(job_id=bad_id, base_seq=0)
    bad_buf.pending_seq_delta = 1
    bad_buf.pending_state["step"] = 1
    bad_buf.dirty = True
    good_buf = _ProgressBuffer(job_id=good_id, base_seq=0)
    good_buf.pending_seq_delta = 1
    good_buf.pending_state["step"] = 1
    good_buf.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {bad_id: bad_buf, good_id: good_buf}
    shutdown = asyncio.Event()

    async def _stop() -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    with structlog.testing.capture_logs() as logs:
        await asyncio.gather(
            progress_flush_loop(lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown),
            _stop(),
        )

    kinds = {e.get("kind") for e in logs if e.get("kind")}
    assert kinds == {"progress_flush_pool_error"}, (
        f"an acquire failure is a pool-wide outage and must log the pool kind: {logs}"
    )
    assert bad_buf.dirty is True, "the failed tick must leave every dirty buffer dirty"
    assert bad_buf.pending_seq_delta == 1
    assert good_buf.dirty is True
    assert good_buf.pending_seq_delta == 1
    assert set(buffers) == {bad_id, good_id}, "no buffer may be dropped on a pool failure"


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


# ── The immediate flush and the tick flush never double-apply one delta ──


async def test_immediate_flush_skips_a_tick_flush_in_flight() -> None:
    """A pre-terminal flush arriving while the tick's batched statement
    holds an unretired snapshot of the same buffer must not issue a second
    statement.

    Both surfaces apply the same ``progress_seq = row + delta`` merge:
    with base 10 and delta 5, the tick lands the row at 15, a racing
    immediate statement lands it at 20, and the terminal write then SETs
    the retired base 15 absolutely, a regression of the monotone-seq
    invariant. With the per-buffer gate the immediate path returns
    without flushing, the buffer keeps its unflushed delta, and the
    terminal write's absolute SET carries it: the row's seq is monotone
    across the interleaving and lands on the correct final value.
    """
    row_seq = 10
    seq_history = [row_seq]
    statement_gate = asyncio.Event()

    async def _merge_fetch(*args: object) -> list[dict[str, object]]:
        # The tick statement's merge shape: progress_seq = row + delta,
        # one RETURNING row per bound job id. Suspended mid-flight until
        # the gate opens, so the consumer's immediate flush genuinely
        # overlaps it.
        nonlocal row_seq
        await statement_gate.wait()
        job_ids = args[1]
        deltas = args[2]
        assert isinstance(job_ids, list) and isinstance(deltas, list)
        for _merged_id, delta in zip(job_ids, deltas, strict=True):
            row_seq += delta
        seq_history.append(row_seq)
        typed_ids = cast("list[UUID]", job_ids)  # pyright: ignore[reportUnknownVariableType]  # Why: the fake conn's *args are object; the pin only ever binds UUID lists.
        return [{"id": job_id, "progress_seq": row_seq} for job_id in typed_ids]

    async def _merge_fetchrow(*args: object) -> dict[str, object] | None:
        # The immediate statement's single-row merge shape; it must never
        # run while the tick's statement is in flight.
        nonlocal row_seq
        deltas = args[2]
        assert isinstance(deltas, list)
        row_seq += deltas[0]
        seq_history.append(row_seq)
        return {"id": _JOB_ID, "progress_seq": row_seq}

    conn = AsyncMock()
    conn.fetch.side_effect = _merge_fetch
    conn.fetchrow.side_effect = _merge_fetchrow
    pool = MagicMock()
    pool.get_size.return_value = _POOL_SIZE

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[AsyncMock, None]:
        yield conn

    pool.acquire = _acquire

    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=10)
    buf.pending_seq_delta = 5
    buf.pending_state["step"] = 1
    buf.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}

    tick = asyncio.create_task(
        _flush_dirty_set(pool, "taskq_test", _WORKER_ID, buffers, [(_JOB_ID, buf)])
    )
    for _ in range(10):
        await asyncio.sleep(0)
        if buf.flush_in_flight:
            break
    assert buf.flush_in_flight is True, "the tick must latch the gate before its statement"

    # The consumer's pre-terminal flush overlaps the tick's in-flight
    # statement: it must skip, not double-apply.
    await _flush_buffer_immediate(pool, "taskq_test", _JOB_ID, _WORKER_ID, buffers)
    assert conn.fetchrow.await_count == 0, (
        "a second statement here applies the tick's unretired delta twice"
    )
    assert conn.fetch.await_count == 1
    # The skipped flush leaves the buffer's delta intact for the terminal SET.
    assert buf.dirty is True
    assert buf.base_seq == 10
    assert buf.pending_seq_delta == 5

    statement_gate.set()
    await tick

    # The tick's retire adopted the authoritative seq and reopened the gate.
    assert buf.base_seq == 15
    assert buf.pending_seq_delta == 0
    assert buf.flush_in_flight is False

    # The terminal write reads base + pending from the buffer and SETs it
    # absolutely; the row's seq must never regress across the interleaving.
    terminal_seq, _terminal_state = _seq_and_state_after_flush_attempt(buf)
    seq_history.append(terminal_seq)

    assert seq_history == sorted(seq_history), (
        f"progress_seq regressed across the interleaving: {seq_history}"
    )
    assert row_seq == 15
    assert terminal_seq == 15
