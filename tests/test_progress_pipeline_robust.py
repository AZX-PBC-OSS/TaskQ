"""Red-team pins for the progress pipeline: buffer, flush surfaces, publish gate.

Every test here attacks one hypothesis about the coalesced-flush machinery
and pins the invariant that survived (or the defect that did not):

1. concurrent flush triggers (tick timer + pre-terminal immediate + the
   late ctx.progress) — the delta must be applied exactly once, never
   double-applied, never driven negative by a racing retire;
2. flush failure paths — a failed statement or pool acquire must leave the
   buffer whole (delta and state intact, gate reopened) so the NEXT flush
   or the terminal write's absolute SET carries everything;
3. seq monotonicity — the wire contract underpinning taskq.web.progress's
   ``seq <= last_emitted_seq`` dedup: no two events may repeat a seq, and
   the terminal event's seq must strictly exceed every progress seq the
   buffer published (the SSE generator discards stale seqs BEFORE the
   terminal check, so a stale terminal seq would hang the stream open);
4. the jsonb NUL guard on the flush path — a poisoned buffer must not
   poison its batch siblings nor wedge the flush_in_flight gate;
5. the enqueue-side backpressure contract — a rejected progress call
   (non-finite percent, oversized data, NUL data, wrong types) must
   neither mutate the buffer nor consume a seq.

All interleavings are armed with observed-state events (a suspended
statement or publish the test resolves) — no sleeps-as-sync.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import cast
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.exceptions import ProgressTooLarge
from taskq.progress._buffer import (
    _consume_state_change_seq,
    _ProgressBuffer,
    _seq_and_state_after_flush_attempt,
)
from taskq.progress._flush import _flush_buffer_immediate, _flush_dirty_set
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from tests._progress_context import make_progress_context

_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-00000000c001")
_WORKER_ID = UUID("11111111-2222-3333-4444-555555555555")


@pytest.fixture
def schema_name() -> str:
    """The schema identifier the flush statements render against - test-local.

    This tier never touches real PG (the pool is an ``AsyncMock``/simulator),
    so the value is an arbitrary marker string; it lives in a per-test fixture
    rather than a module-level constant so the flush pins get it from one
    place without re-introducing the shared/stale-constant anti-pattern the
    suite-hygiene pin bans.
    """
    return "taskq_test"


def _settings(schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )


def _progress_context(
    buffers: dict[UUID, _ProgressBuffer],
    job_id: UUID,
    *,
    schema: str,
    redis_client: object | None = None,
) -> object:
    """A progress-wired context at the job's live attempt epoch."""
    backend = InMemoryBackend(clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)))
    return make_progress_context(
        buffers,
        job_id,
        attempt=1,
        backend=backend,
        settings=_settings(schema),
        redis_client=redis_client,  # type: ignore[arg-type]  # Why: the AsyncMock double below is duck-identical to redis.asyncio.Redis for the two calls the publish path makes.
        pending_publish_tasks=set(),
    )


class _SuspendingFetchPool:
    """A pool double whose batched-statement ``fetch`` suspends on an Event.

    The double is an honest DB simulator: every statement's unnest deltas
    are APPLIED to a running ``row_seq`` and the RETURNING rows echo the
    merged absolute value, exactly what the real ``progress_seq =
    j.progress_seq + f.seq_delta ... RETURNING j.progress_seq`` merge
    produces. Recorded statements let the pins assert exactly-once delta
    application. The first ``fetch`` parks on ``release`` (after setting
    ``started``) so the test can run interleavings against the suspended
    statement — the observed-state arming pattern, no sleeps.
    """

    def __init__(self, *, row_seq: int = 0, suspend_first: bool = True) -> None:
        self.row_seqs: dict[UUID, int] = {}
        self.initial_row_seq = row_seq
        self.suspend_first = suspend_first
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.statements: list[dict[str, object]] = []
        self._suspensions = 0

    @property
    def row_seq(self) -> int:
        return sum(self.row_seqs.values()) if self.row_seqs else self.initial_row_seq

    async def _fetch(self, *args: object) -> list[dict[str, object]]:
        job_ids = cast("list[UUID]", args[1])
        deltas = cast("list[int]", args[2])
        if self.suspend_first and self._suspensions == 0:
            self._suspensions += 1
            self.started.set()
            await self.release.wait()
        self.statements.append({"job_ids": list(job_ids), "deltas": list(deltas)})
        rows = []
        for job_id, delta in zip(job_ids, deltas, strict=True):
            seq = self.row_seqs.get(job_id, self.initial_row_seq) + delta
            self.row_seqs[job_id] = seq
            rows.append({"id": job_id, "progress_seq": seq})
        return rows

    async def _fetchrow(self, *args: object) -> dict[str, object] | None:
        rows = await self._fetch(*args)
        return rows[0] if rows else None

    @property
    def conn(self) -> MagicMock:
        conn = MagicMock()
        conn.fetch.side_effect = self._fetch
        conn.fetchrow.side_effect = self._fetchrow
        return conn

    def pool(self) -> MagicMock:
        conn = self.conn
        pool = MagicMock()

        @asynccontextmanager
        async def _acquire() -> AsyncGenerator[MagicMock, None]:
            yield conn

        pool.acquire = _acquire
        return pool


def _recording_redis() -> tuple[AsyncMock, list[int]]:
    """An AsyncMock redis whose ``publish`` records each event's seq.

    The recording is synchronous inside the awaited call, so a drained
    task set leaves ``published`` settled — deterministic observation.
    """
    published: list[int] = []
    redis = AsyncMock()

    async def _capture_publish(channel: str, payload: str) -> int:
        published.append(json.loads(payload)["seq"])
        return 1

    redis.publish.side_effect = _capture_publish
    return redis, published


async def _drain_publish_tasks(tasks: set[asyncio.Task[None]]) -> None:
    pending = {t for t in tasks if not t.done()}
    if pending:
        await asyncio.gather(*pending)


# ── H1: concurrent flush triggers — exactly-once delta application ────


async def test_tick_suspension_immediate_flush_skips_and_late_call_survives(
    schema_name: str,
) -> None:
    """Regression: a pre-terminal immediate flush issued while the tick's
    batched statement holds an unretired snapshot of the same buffer used
    to double-apply the delta (the second statement re-merged the same
    seq_delta, and the later retire drove pending_seq_delta negative, so
    the terminal write's absolute SET regressed the durable row).

    The pin: with the tick's statement suspended mid-flight, the immediate
    path must SKIP (flush_in_flight gate), a ctx.progress landing during
    the suspension must survive the retire on top of the new base, and the
    post-retire flush must apply only the late call's delta — the tick's
    delta exactly once, total.
    """
    sim = _SuspendingFetchPool(row_seq=5)
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5, attempt=1)
    buf.pending_seq_delta = 3
    buf.pending_state["step"] = 1
    buf.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    pool = sim.pool()

    tick = asyncio.create_task(
        _flush_dirty_set(pool, schema_name, _WORKER_ID, buffers, [(_JOB_ID, buf)])
    )
    # Observed-state arming: the tick's statement is suspended holding its
    # snapshot (delta=3). Everything below races it on the same loop.
    await sim.started.wait()

    # The immediate (pre-terminal) flush must skip, not issue a statement.
    await _flush_buffer_immediate(pool, schema_name, _JOB_ID, _WORKER_ID, buffers)
    assert len(sim.statements) == 0, "immediate flush double-applied the latched delta"

    # A progress call landing during the suspension mutates the buffer in
    # place: head moves 5+3=8 to 5+4=9.
    ctx = _progress_context(buffers, _JOB_ID, schema=schema_name)
    await ctx.progress(step=2)  # type: ignore[attr-defined]
    assert buf.pending_seq_delta == 4
    assert buf.base_seq + buf.pending_seq_delta == 9

    sim.release.set()
    await tick

    # The tick applied its snapshot delta=3 exactly once; the retire adopted
    # the row's merged seq (8) and kept ONLY the late call's delta standing.
    assert sim.statements == [{"job_ids": [_JOB_ID], "deltas": [3]}]
    assert buf.base_seq == 8
    assert buf.pending_seq_delta == 1
    assert buf.dirty is True

    # The next flush carries only the late delta, landing the row at the
    # buffer's head (9): no loss, no repeat.
    await _flush_buffer_immediate(pool, schema_name, _JOB_ID, _WORKER_ID, buffers)
    assert sim.statements[-1] == {"job_ids": [_JOB_ID], "deltas": [1]}
    assert sim.row_seq == 9
    assert buf.base_seq == 9 and buf.pending_seq_delta == 0 and buf.dirty is False

    # The terminal projection consumes one past the authoritative head.
    seq, _state = _seq_and_state_after_flush_attempt(buf)
    assert seq == 10


async def test_immediate_skip_under_tick_gate_keeps_delta_for_terminal_write(
    schema_name: str,
) -> None:
    """Regression: when the immediate (pre-terminal) flush skips because the
    tick holds the gate, the skipped delta must stay ON the buffer so the
    terminal write's absolute SET carries it — a flush that "succeeds" by
    skipping while dropping the delta would silently rewind the durable
    progress_seq the terminal write lands.
    """
    sim = _SuspendingFetchPool(row_seq=5, suspend_first=True)
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5, attempt=1)
    buf.pending_seq_delta = 3
    buf.pending_state["step"] = 1
    buf.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    pool = sim.pool()

    tick = asyncio.create_task(
        _flush_dirty_set(pool, schema_name, _WORKER_ID, buffers, [(_JOB_ID, buf)])
    )
    await sim.started.wait()

    await _flush_buffer_immediate(pool, schema_name, _JOB_ID, _WORKER_ID, buffers)

    # The buffer the caller hands the terminal projection still carries the
    # full unflushed head, and the projection consumes one past it.
    seq, state = _seq_and_state_after_flush_attempt(buf)
    assert seq == 5 + 3 + 1
    assert state == {"step": 1}

    sim.release.set()
    await tick
    assert buf.base_seq == 8  # the tick's own flush retired normally


# ── H2: flush failure paths — buffer whole, gate reopened, no loss ────


async def test_failed_statement_leaves_buffer_whole_and_next_flush_applies_full_delta(
    schema_name: str,
) -> None:
    """Regression: a flush statement failing mid-buffer (connection reset,
    statement timeout) must not consume, corrupt, or drop the buffer's
    delta — the failure is the tick's alone, the buffer stays dirty with
    its delta and pending state intact, and the NEXT successful flush
    applies the FULL accumulated delta in one statement. A flush that
    partially retired on failure would strand seqs the terminal write
    never carries."""
    sim = _SuspendingFetchPool(row_seq=0, suspend_first=False)
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buf.pending_seq_delta = 3
    buf.pending_state["step"] = 1
    buf.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}

    # Statement failure: the fetch raises this tick.
    conn = sim.conn
    failing = asyncio.Event()

    async def _boom(*args: object) -> list[dict[str, object]]:
        failing.set()
        raise RuntimeError("connection reset mid-flush")

    conn.fetch.side_effect = _boom
    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[MagicMock, None]:
        yield conn

    pool.acquire = _acquire

    await _flush_dirty_set(pool, schema_name, _WORKER_ID, buffers, [(_JOB_ID, buf)])

    assert failing.is_set()
    assert buf.dirty is True
    assert buf.pending_seq_delta == 3
    assert buf.pending_state == {"step": 1}
    assert buf.flush_in_flight is False, "a failed statement wedged the flush gate"

    # Recovery: the next flush applies the FULL delta, once.
    conn.fetch.side_effect = sim._fetch
    await _flush_dirty_set(pool, schema_name, _WORKER_ID, buffers, [(_JOB_ID, buf)])
    assert sim.statements == [{"job_ids": [_JOB_ID], "deltas": [3]}]
    assert buf.base_seq == 3 and buf.pending_seq_delta == 0 and buf.dirty is False


async def test_pool_acquire_failure_leaves_buffer_whole_and_gate_reopened(schema_name: str) -> None:
    """Regression: a pool-level acquire failure (exhaustion, wedged pool)
    must lose the tick, not the buffer — the buffer stays dirty with its
    delta intact and the immediate path's gate reopened, so a pre-terminal
    flush right after still issues its statement and the terminal write
    still carries the head."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buf.pending_seq_delta = 2
    buf.pending_state["step"] = 1
    buf.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}

    pool = MagicMock()

    @asynccontextmanager
    async def _dead_acquire() -> AsyncGenerator[MagicMock, None]:
        raise RuntimeError("pool exhausted")
        yield  # pragma: no cover

    pool.acquire = _dead_acquire
    await _flush_buffer_immediate(pool, schema_name, _JOB_ID, _WORKER_ID, buffers)

    assert buf.dirty is True
    assert buf.pending_seq_delta == 2
    assert buf.flush_in_flight is False, "a pool failure wedged the flush gate"

    # The same buffer still flushes through a healthy pool afterwards.
    sim = _SuspendingFetchPool(row_seq=0, suspend_first=False)
    healthy = MagicMock()

    @asynccontextmanager
    async def _live_acquire() -> AsyncGenerator[MagicMock, None]:
        yield sim.conn

    healthy.acquire = _live_acquire
    await _flush_buffer_immediate(healthy, schema_name, _JOB_ID, _WORKER_ID, buffers)
    assert sim.statements == [{"job_ids": [_JOB_ID], "deltas": [2]}]
    assert buf.base_seq == 2 and buf.dirty is False


# ── H3: seq monotonicity — the SSE dedup contract's foundation ────────


async def test_published_wire_seqs_strictly_increasing_and_unique_through_gate(
    schema_name: str,
) -> None:
    """Regression: the coalesced publish gate (in-flight task + latch) used
    to be the candidate for a repeated or decreasing wire seq — a latched
    event re-published after a newer one, or a direct publish racing the
    drain. The pin drives the REAL gate (ctx.progress + pending-publish
    task set) through direct calls, a latched call, and a state-change
    consumption, and requires every seq reaching the channel to be
    strictly increasing and unique: taskq.web.progress dedups by
    ``seq <= last_emitted_seq`` alone, so one repeat merges two distinct
    events and one rewind discards a live one."""
    redis, published = _recording_redis()
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = _progress_context(buffers, _JOB_ID, schema=schema_name, redis_client=redis)

    await ctx.progress(step=1)  # type: ignore[attr-defined]
    await ctx.progress(step=2)  # type: ignore[attr-defined]
    await _drain_publish_tasks(cast("set[asyncio.Task[None]]", ctx._pending_publish_tasks))  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]  # Why: the test-wired task set IS the drain handle.
    assert published == [1, 2]

    # A state-change consumption stacks on the head; the next progress call
    # must publish strictly above it.
    consumed = _consume_state_change_seq(buf)
    assert consumed == 3
    await ctx.progress(step=3)  # type: ignore[attr-defined]
    await _drain_publish_tasks(cast("set[asyncio.Task[None]]", ctx._pending_publish_tasks))  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]
    assert published == [1, 2, 4]
    assert published == sorted(set(published))


async def test_latched_trailing_call_publishes_after_in_flight_round_trip(schema_name: str) -> None:
    """Regression: a progress call landing while a publish round trip is in
    flight latches on the buffer; the in-flight task must drain the latch
    AFTER its own event (strictly increasing wire seqs, no lost trailing
    call) and release the gate exactly once — a double release or a lost
    latch here is the lost-final-event defect the gate exists to prevent."""
    redis, published = _recording_redis()
    round_trip = asyncio.Event()
    raw_publish = redis.publish.side_effect

    async def _gated_publish(channel: str, payload: str) -> int:
        await round_trip.wait()
        return await raw_publish(channel, payload)  # type: ignore[misc]  # Why: the recording side_effect captured above.

    redis.publish.side_effect = _gated_publish

    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = _progress_context(buffers, _JOB_ID, schema=schema_name, redis_client=redis)
    tasks = cast("set[asyncio.Task[None]]", ctx._pending_publish_tasks)  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]

    await ctx.progress(step=1)  # type: ignore[attr-defined]  # in flight, suspended in the round trip
    await ctx.progress(step=2)  # type: ignore[attr-defined]  # latches, no second task
    assert len(tasks) == 1
    assert published == []
    assert buf.publish_in_flight is True

    round_trip.set()
    await _drain_publish_tasks(tasks)

    assert published == [1, 2], "the latched trailing call was lost or reordered"
    assert buf.publish_in_flight is False
    assert buf.pending_publish is None, "the drain left a stale latch on the buffer"


async def test_terminal_override_seq_exceeds_every_published_progress_seq(schema_name: str) -> None:
    """Regression: taskq.web.progress's SSE generator discards events with
    ``seq <= last_emitted_seq`` BEFORE it checks ``terminal`` — a terminal
    state-change event carrying a seq at or below any already-emitted
    progress seq would be silently dropped and the stream would never
    emit ``done``. The pin: through a suspended tick flush, a late
    progress call, and the coalesced publish gate, the terminal override
    projection (``_seq_and_state_after_flush_attempt``) must be strictly
    greater than every seq the buffer put on the wire."""
    sim = _SuspendingFetchPool(row_seq=5)
    redis, published = _recording_redis()
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=5, attempt=1)
    buf.pending_seq_delta = 3
    buf.pending_state["step"] = 1
    buf.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    pool = sim.pool()

    ctx = _progress_context(buffers, _JOB_ID, schema=schema_name, redis_client=redis)
    tasks = cast("set[asyncio.Task[None]]", ctx._pending_publish_tasks)  # type: ignore[attr-defined]  # pyright: ignore[reportAttributeAccessIssue]

    tick = asyncio.create_task(
        _flush_dirty_set(pool, schema_name, _WORKER_ID, buffers, [(_JOB_ID, buf)])
    )
    await sim.started.wait()

    await ctx.progress(step=2)  # type: ignore[attr-defined]  # head 5+4=9, wire seq 9 (direct or latched)
    sim.release.set()
    await tick
    await _drain_publish_tasks(tasks)

    seq, _state = _seq_and_state_after_flush_attempt(buf)
    assert published, "the interleave published nothing"
    assert seq > max(published), (
        f"terminal seq {seq} does not exceed every published progress seq {published}: "
        "the SSE stream would drop the terminal event and never close"
    )


async def test_fenced_out_flush_drops_only_the_stale_buffer_never_the_new_epoch(
    schema_name: str,
) -> None:
    """Regression: a flush whose row the fencing gate rejects (the job was
    re-claimed and redispatched to a later attempt on this worker) must
    drop ONLY the stale epoch's buffer — an identity-blind pop took the
    live attempt's buffer with it, deseeding the live attempt's progress
    and rewinding its seq allocation to a stale base."""
    stale = _ProgressBuffer(job_id=_JOB_ID, base_seq=5, attempt=1)
    stale.pending_seq_delta = 3
    stale.dirty = True
    live = _ProgressBuffer(job_id=_JOB_ID, base_seq=100, attempt=2)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: live}

    conn = MagicMock()
    conn.fetchrow.return_value = None  # the fence: no row RETURNed
    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[MagicMock, None]:
        yield conn

    pool.acquire = _acquire
    await _flush_buffer_immediate(pool, schema_name, _JOB_ID, new_uuid(), buffers)

    assert buffers.get(_JOB_ID) is live, "the fenced pop evicted the live attempt's buffer"
    assert live.base_seq == 100 and live.pending_seq_delta == 0
    # The live epoch's seq allocation is untouched by the stale epoch's flush.
    assert _consume_state_change_seq(live) == 101


# ── H4: the NUL guard on the flush path — isolation, not poisoning ────


async def test_nul_poisoned_buffer_skipped_without_poisoning_its_batch(schema_name: str) -> None:
    """Regression: a buffer poisoned past the ctx.progress door (a direct
    writer put a NUL into pending_state — the flush guard is the
    defense-in-depth for exactly this shape) must be skipped alone: the
    snapshot phase's ValueError skip must not take its batch siblings'
    rows with it, and the poisoned buffer must stay dirty for the next
    tick rather than being silently dropped with its delta."""
    poisoned = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    poisoned.pending_seq_delta = 1
    poisoned.pending_state["detail"] = "bad\x00value"  # direct writer, past the ctx door
    poisoned.dirty = True
    sibling_id = UUID("aaaaaaaa-bbbb-cccc-dddd-00000000c002")
    healthy = _ProgressBuffer(job_id=sibling_id, base_seq=10, attempt=1)
    healthy.pending_seq_delta = 2
    healthy.pending_state["step"] = 3
    healthy.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: poisoned, sibling_id: healthy}

    sim = _SuspendingFetchPool(row_seq=0, suspend_first=False)
    sim.row_seqs[sibling_id] = 10  # the healthy sibling's durable row sits at its buffer's base
    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[MagicMock, None]:
        yield sim.conn

    pool.acquire = _acquire
    await _flush_dirty_set(
        pool, schema_name, _WORKER_ID, buffers, [(_JOB_ID, poisoned), (sibling_id, healthy)]
    )

    # The healthy sibling flushed through the same tick.
    assert sim.statements == [{"job_ids": [sibling_id], "deltas": [2]}]
    assert healthy.base_seq == 12 and healthy.dirty is False
    # The poisoned row was skipped, not flushed, not dropped: it stays
    # dirty (the next tick retries and re-logs it) with its delta intact.
    assert poisoned.dirty is True
    assert poisoned.pending_seq_delta == 1
    assert buffers.get(_JOB_ID) is poisoned
    assert poisoned.flush_in_flight is False, "the skip wedged the poisoned buffer's gate"


async def test_nul_poisoned_immediate_flush_contained_and_gate_reopened(schema_name: str) -> None:
    """Regression: the single-row (immediate/crash-flush) path hitting the
    NUL guard while rendering the state document must contain the
    ValueError to that one buffer — logged, buffer left dirty — and must
    NOT leave the flush_in_flight gate shut (a wedged gate would make
    every later immediate flush silently skip the buffer for the rest of
    its lifetime, stranding its delta)."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buf.pending_seq_delta = 1
    buf.pending_state["detail"] = "bad\x00value"
    buf.dirty = True
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}

    sim = _SuspendingFetchPool(row_seq=0, suspend_first=False)
    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[MagicMock, None]:
        yield sim.conn

    pool.acquire = _acquire
    await _flush_buffer_immediate(pool, schema_name, _JOB_ID, _WORKER_ID, buffers)

    assert sim.statements == [], "the poisoned state document reached the wire"
    assert buf.dirty is True and buf.pending_seq_delta == 1
    assert buf.flush_in_flight is False


# ── H5: enqueue-side backpressure — rejected calls never consume ──────


@pytest.mark.parametrize(
    ("kwargs", "error_type"),
    [
        pytest.param({"percent": float("nan")}, ValueError, id="nan-percent"),
        pytest.param({"percent": float("inf")}, ValueError, id="inf-percent"),
        pytest.param({"percent": "half"}, TypeError, id="str-percent"),
        pytest.param({"step": "1"}, TypeError, id="str-step"),
        pytest.param({"detail": 7}, TypeError, id="non-str-detail"),
        pytest.param({"data": {"k": "x" * 20000}}, ProgressTooLarge, id="oversized-data"),
        pytest.param({"data": {"k": "a\x00b"}}, ValueError, id="nul-data"),
        pytest.param({"data": {1: "v"}}, (TypeError, ValueError), id="non-str-key-data"),
        pytest.param({"detail": "d\x00e"}, ValueError, id="nul-detail"),
        pytest.param({"detail": "x" * 20000}, ProgressTooLarge, id="oversized-detail"),
    ],
)
async def test_rejected_progress_call_never_consumes_seq_nor_mutates_buffer(
    kwargs: dict[str, object],
    error_type: type[BaseException] | tuple[type[BaseException], ...],
    schema_name: str,
) -> None:
    """Regression: every rejection gate (finiteness, types, size caps, NUL)
    must fire BEFORE the buffer mutation — a gate that consumed the seq
    first would punch a permanent hole in the wire order (or, worse, a
    partially-mutated pending_state would flush a value the actor was
    told was refused). After any rejection, the buffer's delta, state,
    dirty flag, and encoded bytes are untouched, and the next ACCEPTED
    call's seq is exactly previous+1: no hole, no reuse."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = _progress_context(buffers, _JOB_ID, schema=schema_name)
    # An accepted call first, so the pin proves the rejection leaves the
    # buffer exactly where the accepted call left it.
    await ctx.progress(step=1)  # type: ignore[attr-defined]
    before = (buf.pending_seq_delta, dict(buf.pending_state), buf.dirty)

    with pytest.raises(error_type):
        await ctx.progress(**kwargs)  # type: ignore[arg-type]

    assert (buf.pending_seq_delta, dict(buf.pending_state), buf.dirty) == before

    # The next accepted call stacks directly on the accepted head.
    await ctx.progress(step=2)  # type: ignore[attr-defined]
    assert buf.base_seq + buf.pending_seq_delta == 2


async def test_flush_failure_backlog_never_corrupts_the_accumulated_state(schema_name: str) -> None:
    """Regression (backpressure shape): a persistently failing flush (an
    outage) with an actor still calling progress must accumulate cleanly —
    the delta grows, the fixed-key state doc merges last-writer-wins, and
    ONE successful flush at the end of the outage applies the ENTIRE
    accumulated delta in a single statement with the latest value of
    every field. No partial state, no lost seq, no unbounded per-tick
    statement churn."""
    sim = _SuspendingFetchPool(row_seq=0, suspend_first=False)
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    conn = (
        sim.conn
    )  # one connection instance: the failure then the recovery swap land on the same double
    pool = MagicMock()

    @asynccontextmanager
    async def _acquire() -> AsyncGenerator[MagicMock, None]:
        yield conn

    pool.acquire = _acquire

    # The outage: every flush fails while the actor keeps reporting.
    conn.fetchrow.side_effect = RuntimeError("db down")

    ctx = _progress_context(buffers, _JOB_ID, schema=schema_name)
    for step in range(1, 6):
        await ctx.progress(step=step)  # type: ignore[attr-defined]
        await _flush_buffer_immediate(pool, schema_name, _JOB_ID, _WORKER_ID, buffers)

    assert buf.pending_seq_delta == 5
    assert buf.pending_state == {"step": 5}
    assert buf.dirty is True

    # Recovery: one flush carries everything.
    conn.fetchrow.side_effect = sim._fetchrow
    await _flush_buffer_immediate(pool, schema_name, _JOB_ID, _WORKER_ID, buffers)
    assert sim.statements == [{"job_ids": [_JOB_ID], "deltas": [5]}]
    assert buf.base_seq == 5 and buf.pending_seq_delta == 0 and buf.dirty is False
