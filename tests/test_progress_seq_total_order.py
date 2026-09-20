"""Pins for the seq total-order contract over a job's whole event stream.

The job row's ``progress_seq`` and the wire ``seq`` on every published
event are a STRICT TOTAL ORDER over the job's whole event stream:
progress events and state-change events (running, retried, snoozed,
succeeded, failed, cancelled, interrupted) each consume the next seq
value, so every event on the wire carries a seq strictly greater than
every event before it. A consumer deduping or resuming by seq alone
(EventSource ``Last-Event-ID`` discipline) can therefore never mistake
a state-change event for a duplicate of the progress event before it.
"""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobRow
from taskq.context import BaseModel, CancelOrigin, JobContext
from taskq.exceptions import Snooze
from taskq.progress._buffer import (
    _consume_state_change_seq,
    _ProgressBuffer,
    _seq_and_state_after_flush_attempt,
)
from taskq.progress._flush import _flush_buffer
from taskq.progress._publish import _publish_state_change_event
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import EmptyPayload, StubActorConfig, default_actor_config
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = new_uuid()
_JOB_ID = UUID("00000000-0000-0000-0000-000000000001")


class _RecordingRedis:
    """Records every published payload in completion order."""

    def __init__(self) -> None:
        self.published: list[dict[str, object]] = []

    async def publish(self, channel: str, payload: str) -> int:
        self.published.append(json.loads(payload))
        return 1


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_SCHEMA_NAME": "taskq_test",
            "TASKQ_PROGRESS_PUBLISH_GLOBAL": "false",
        }
    )


def _deps(
    buffers: dict[UUID, _ProgressBuffer],
    redis_client: _RecordingRedis,
) -> MagicMock:
    deps = MagicMock(spec=WorkerDeps)
    deps.progress_buffers = buffers
    deps.worker_pool = None
    deps.settings = _settings()
    deps.redis_client = redis_client
    deps.disowned_jobs = set()
    # None sends every ctx.progress publish down the inline-await path,
    # so capture order is publication order and the seq assertions are
    # deterministic.
    deps.pending_publish_tasks = None
    return deps


def _wire_events(redis_client: _RecordingRedis) -> list[dict[str, object]]:
    return [e for e in redis_client.published if e.get("kind") in ("progress", "state_change")]


def _seqs(events: list[dict[str, object]]) -> list[int]:
    return [e["seq"] for e in events]  # type: ignore[return-value]  # Why: the wire always carries int seqs.


async def _consume(
    backend: InMemoryBackend,
    job: JobRow,
    actor: Any,
    deps: MagicMock,
    cfg: Any = None,
    *,
    active_jobs: ActiveJobRegistry | None = None,
) -> Any:
    return await consume_one_job(
        backend,
        job,
        _WORKER_ID,
        run_actor=actor,
        actor_config=cfg if cfg is not None else default_actor_config(),
        payload_type=EmptyPayload,
        clock=FakeClock(_NOW),
        deps=deps,
        active_jobs=active_jobs,
    )


async def _dispatch_claim(backend: InMemoryBackend) -> JobRow:
    jobs = await backend.dispatch_batch(
        worker_id=_WORKER_ID, queues=["default"], limit=1, lock_lease=timedelta(seconds=60)
    )
    assert len(jobs) == 1, "the claim must admit exactly the one enqueued job"
    return jobs[0]


# ── Pin 1: strict total order across progress and state-change events ──


@pytest.mark.asyncio
async def test_seq_strictly_increases_across_progress_and_state_changes() -> None:
    """running, progress, retried, running again, progress, succeeded: every
    seq on the wire strictly increases, across the retry-attempt boundary
    included, and the durable row ends exactly at the terminal event's seq."""
    clock = FakeClock(_NOW)
    backend = InMemoryBackend(clock=clock)
    backend.register_actor_config(actor="order_actor")
    await backend.enqueue(make_enqueue_args(actor="order_actor", scheduled_at=_NOW))

    redis_client = _RecordingRedis()
    buffers: dict[UUID, _ProgressBuffer] = {}
    cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0))

    attempt = 0

    async def failing_once(job: JobRow, ctx: JobContext[BaseModel]) -> None:
        nonlocal attempt
        attempt += 1
        await ctx.progress(step=attempt, percent=10.0 * attempt)
        if attempt == 1:
            raise RuntimeError("transient boom")

    # Attempt 1: claim, run, fail into a retry.
    job = await _dispatch_claim(backend)
    await _consume(backend, job, failing_once, _deps(buffers, redis_client), cfg)

    row = await backend.get(job.id)
    assert row is not None and row.status == "scheduled"
    # The retry write consumed the seq: running(1), progress(2), retry(3).
    assert row.progress_seq == 3

    # Attempt 2: the real loop's promotion sweep re-pends the deferral once
    # it has elapsed, then the claim runs (attempt + 1) and the new buffer
    # seeds from the row's consumed seq.
    clock.advance(timedelta(seconds=10))
    assert await backend.scheduled_to_pending() == 1
    redispatched = await _dispatch_claim(backend)
    assert redispatched.attempt == 2
    assert redispatched.progress_seq == 3
    await _consume(backend, redispatched, failing_once, _deps(buffers, redis_client), cfg)

    row = await backend.get(job.id)
    assert row is not None and row.status == "succeeded"
    assert row.progress_seq == 6

    events = _wire_events(redis_client)
    seqs = _seqs(events)
    assert seqs == sorted(seqs), f"wire seq regressed: {seqs}"
    assert len(seqs) == len(set(seqs)), f"duplicate seq on the wire: {seqs}"
    kinds = [(e["kind"], e.get("status")) for e in events]
    # The retry arm announces the handler's outcome vocabulary on the wire
    # (the generic-exception handler publishes status="failed" for the
    # attempt, while the row itself goes back to scheduled); the pin is the
    # SEQ arithmetic, not that vocabulary.
    assert kinds == [
        ("state_change", "running"),
        ("progress", "running"),
        ("state_change", "failed"),
        ("state_change", "running"),
        ("progress", "running"),
        ("state_change", "succeeded"),
    ]
    # Every event strictly follows the one before it.
    for before, after in pairwise(seqs):
        assert after == before + 1, f"seq gap or repeat at {before} -> {after}"
    # The terminal event's seq is exactly last_progress_seq + 1.
    assert events[-1]["terminal"] is True
    assert seqs[-1] == seqs[-2] + 1
    # The durable row carries the terminal event's consumed seq.
    assert row.progress_seq == seqs[-1]


# ── Pin 2: the exit event consumes one past the last progress event ──


async def _progress_then(
    outcome: str,
) -> tuple[_RecordingRedis, InMemoryBackend, UUID]:
    """Drive one job that reports progress twice, then exits as *outcome*."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    backend.register_actor_config(actor="order_actor")
    await backend.enqueue(make_enqueue_args(actor="order_actor", scheduled_at=_NOW))
    job = await _dispatch_claim(backend)

    redis_client = _RecordingRedis()
    buffers: dict[UUID, _ProgressBuffer] = {}

    async def actor(running: JobRow, ctx: JobContext[BaseModel]) -> None:
        await ctx.progress(step=1, percent=50.0)
        await ctx.progress(step=2, percent=100.0)
        if outcome == "failed":
            raise RuntimeError("boom")
        if outcome == "cancelled":
            raise asyncio.CancelledError
        if outcome == "snoozed":
            raise Snooze(timedelta(seconds=5))

    cfg: Any = default_actor_config()
    if outcome == "failed":
        cfg = StubActorConfig(retry=RetryPolicy(kind="transient", max_attempts=1, jitter=0.0))

    if outcome == "interrupted":
        registry = ActiveJobRegistry()

        async def interrupted(running: JobRow, ctx: JobContext[BaseModel]) -> None:
            task = asyncio.current_task()
            assert task is not None
            await registry.register(running.id, task, ctx)
            entry = registry.get(running.id)
            assert entry is not None
            entry.cancel_origin = CancelOrigin.SHUTDOWN
            await ctx.progress(step=1, percent=50.0)
            await ctx.progress(step=2, percent=100.0)
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await _consume(
                backend,
                job,
                interrupted,
                _deps(buffers, redis_client),
                active_jobs=registry,
            )
        return redis_client, backend, job.id

    if outcome == "cancelled":
        with pytest.raises(asyncio.CancelledError):
            await _consume(backend, job, actor, _deps(buffers, redis_client), cfg)
        return redis_client, backend, job.id

    await _consume(backend, job, actor, _deps(buffers, redis_client), cfg)
    return redis_client, backend, job.id


@pytest.mark.parametrize("outcome", ["succeeded", "failed", "cancelled", "snoozed", "interrupted"])
@pytest.mark.asyncio
async def test_exit_event_seq_is_last_progress_seq_plus_one(outcome: str) -> None:
    """Under every exit path the state-change event's seq is exactly one
    past the last progress event's seq, and the durable row carries that
    same consumed seq, never the head the progress event already held."""
    redis_client, backend, job_id = await _progress_then(outcome)

    events = _wire_events(redis_client)
    kinds = [(e["kind"], e.get("status"), e.get("terminal")) for e in events]
    progress_seqs = [e["seq"] for e in events if e["kind"] == "progress"]
    state_seqs = [e["seq"] for e in events if e["kind"] == "state_change"]
    assert len(progress_seqs) == 2, f"expected two progress events, got {kinds}"
    assert len(state_seqs) == 2, f"expected the running and the exit events, got {kinds}"
    seqs = _seqs(events)
    assert seqs == sorted(seqs), f"wire seq regressed: {seqs}"
    last_progress = _seqs([e for e in events if e["kind"] == "progress"])[-1]
    exit_seq = _seqs([e for e in events if e["kind"] == "state_change"])[-1]
    assert state_seqs[0] == 1, "the running transition consumes seq 1"
    assert exit_seq == last_progress + 1, (
        f"the exit event must carry last_progress_seq + 1 ({last_progress + 1}), got {exit_seq}"
    )

    row = await backend.get(job_id)
    assert row is not None
    assert row.progress_seq == exit_seq, (
        f"the durable row must carry the exit event's consumed seq "
        f"({exit_seq}), got {row.progress_seq}"
    )


# ── Pin 3: a flush across the state-change boundary neither double-
#    consumes nor regresses ─────────────────────────────────────────────


class _MergeConn:
    """fetchrow double applying the real ``row + delta`` merge shape."""

    def __init__(self, row_seq: list[int]) -> None:
        self._row_seq = row_seq

    async def fetchrow(self, _sql: str, *args: object) -> dict[str, object]:
        deltas = args[1]
        assert isinstance(deltas, list)
        self._row_seq[0] += sum(deltas)
        return {"id": _JOB_ID, "progress_seq": self._row_seq[0]}


class _AcquiredConn:
    def __init__(self, conn: "_MergeConn") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_MergeConn":
        return self._conn

    async def __aexit__(self, *_exc: object) -> None:
        return None


class _MergePool:
    def __init__(self, row_seq: list[int]) -> None:
        self._row_seq = row_seq

    def acquire(self) -> "_AcquiredConn":
        return _AcquiredConn(_MergeConn(self._row_seq))


@pytest.mark.asyncio
async def test_flush_across_state_change_boundary_neither_double_consumes_nor_regresses() -> None:
    """The running transition's consumption rides the flush delta with the
    progress deltas (row 10 -> 16: one consumption, five progress events),
    the retire adopts the flushed head, and the terminal write consumes
    exactly one past it (17). The consumption lands once, on the row and
    on the terminal event's seq, never twice, never a regression."""
    row_seq = [10]
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=10, attempt=1)
    consumed = _consume_state_change_seq(buf)  # the running transition
    assert consumed == 11
    buf.pending_state["step"] = 1
    buf.dirty = True
    for step in range(2, 7):
        # Five progress events stacked after the consumption.
        buf.pending_seq_delta += 1
        buf.pending_state["step"] = step
    assert buf.pending_seq_delta == 6

    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    await _flush_buffer(_MergePool(row_seq), "taskq_test", _JOB_ID, _WORKER_ID, buf, buffers)

    assert row_seq[0] == 16, "the flush applies the consumption and the progress exactly once"
    assert buf.base_seq == 16
    assert buf.pending_seq_delta == 0
    assert buf.dirty is False

    terminal_seq, _state = _seq_and_state_after_flush_attempt(buf)
    assert terminal_seq == 17, (
        f"the terminal event must consume exactly one past the flushed head "
        f"(17), got {terminal_seq}: either it repeated the head (a seq-cursor "
        f"consumer drops it) or the consumption was applied twice"
    )


# ── Pin 3b: a state-change publish after a flush retire carries the
#    consumed seq, never the retired head ──────────────────────────────


@pytest.mark.asyncio
async def test_state_change_publish_after_flush_retire_carries_the_consumed_seq() -> None:
    """A successful pre-terminal flush retires the buffer: the delta lands
    in ``base_seq`` and the flushed keys are deleted from
    ``pending_state``, so ``_seq_and_state_after_flush_attempt`` returns
    ``(consumed, None)``. The state-change publish must carry that
    consumed seq — exactly the value the ``mark_*`` write SETs durably —
    and must NOT fall back to reading the buffer: the retired head is the
    LAST event's seq, so a fallback publishes a duplicate of the progress
    event before the state change and drops the consumed seq the row now
    holds (a seq-cursor consumer discards the state-change event as a
    duplicate and the stream never closes)."""
    row_seq = [10]
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=10, attempt=1)
    buf.pending_seq_delta += 1  # the running transition's consumption
    buf.pending_seq_delta += 1  # one progress event
    buf.pending_state["step"] = 1
    buf.dirty = True
    await _flush_buffer(_MergePool(row_seq), "taskq_test", _JOB_ID, _WORKER_ID, buf, {})

    # The retire: flushed head adopted, delta cleared, keys deleted.
    assert buf.base_seq == 12
    assert buf.pending_seq_delta == 0
    assert buf.pending_state == {}

    consumed, state = _seq_and_state_after_flush_attempt(buf)
    assert consumed == 13
    assert state is None  # no state delta left to write: the flush took it

    # Publish exactly as the consumer does after the mark_* write.
    redis_client = _RecordingRedis()
    await _publish_state_change_event(
        redis_client,
        _settings(),
        _JOB_ID,
        "order_actor",
        {_JOB_ID: buf},
        status="succeeded",
        terminal=True,
        _override_seq=consumed,
        _override_pending_state=state,
    )

    events = _wire_events(redis_client)
    assert len(events) == 1, f"expected exactly the state-change event, got {events}"
    assert events[0]["seq"] == consumed, (
        f"the state-change event must carry the consumed seq ({consumed}), "
        f"got {events[0]['seq']}: a retired-head fallback duplicates the "
        f"progress event before it on the wire"
    )


# ── Pin 4: the SSE replay sees the terminal strictly after the last
#    progress event ─────────────────────────────────────────────────────


class _StubPubSub:
    """Minimal redis PubSub duck-type; messages replay then exhaust."""

    def __init__(self, messages: list[Any]) -> None:
        self._messages = list(messages)
        self._pos = 0

    async def subscribe(self, channel: str) -> None:
        return None

    async def get_message(
        self,
        *,
        ignore_subscribe_messages: bool = True,
        timeout: float = 0,  # noqa: ASYNC109  # Why: mirrors redis-py PubSub.get_message's signature; not a missing asyncio.timeout pattern.
    ) -> Any:
        if self._pos < len(self._messages):
            item = self._messages[self._pos]
            self._pos += 1
            return item
        return None

    async def unsubscribe(self, channel: str) -> None:
        return None

    async def aclose(self) -> None:
        return None


def _event_json(*, seq: int, kind: str, status: str, terminal: bool) -> dict[str, Any]:
    from taskq.progress._events import ProgressEvent

    event = ProgressEvent(
        kind=kind,  # type: ignore[arg-type]  # Why: the literal is pinned by the pydantic model at runtime.
        job_id=_JOB_ID,
        actor="order_actor",
        ts=datetime(2026, 1, 1, tzinfo=UTC),
        seq=seq,
        status=status,
        terminal=terminal,
    )
    return {
        "type": "message",
        "data": event.model_dump_json(exclude_none=True).encode("utf-8"),
    }


async def _drive_sse(
    *,
    pg_seq: int,
    is_terminal: bool,
    last_event_id: int | None,
    messages: list[Any],
) -> list[Any]:
    # Both imports below need the fastapi extra (taskq.web.progress imports
    # fastapi; sse-starlette ships inside the same extra). Skip before
    # either import runs, so an env with one but not both skips instead of
    # erroring.
    pytest.importorskip("fastapi")
    pytest.importorskip("sse_starlette")
    from sse_starlette.event import ServerSentEvent

    from taskq.web.progress import _event_generator, _resolve_last_event_id

    request = MagicMock()
    request.headers.get.return_value = str(last_event_id) if last_event_id is not None else None
    resolved = _resolve_last_event_id(request, None)
    assert resolved == last_event_id
    gen = _event_generator(
        pubsub=_StubPubSub(messages),
        channel=f"taskq:taskq_test:progress:{_JOB_ID}",
        job_id=_JOB_ID,
        is_terminal=is_terminal,
        progress_seq=pg_seq,
        progress_data="{}",
        resolved_last_event_id=resolved,
        heartbeat_secs=0.01,
    )
    results: list[ServerSentEvent] = []
    try:
        async for sse_event in gen:
            results.append(sse_event)
            if len(results) >= 6:
                break
    finally:
        await gen.aclose()
    return results


@pytest.mark.asyncio
async def test_sse_reconnect_sees_the_terminal_after_the_last_progress_event() -> None:
    """A consumer that saw the last progress event (seq 3) reconnects: the
    row's progress_seq is 4 (the terminal write consumed it), so the
    catch-up snapshot fires (4 > 3) and the stream closes with the
    terminal. Under the old progress-only contract the row still held 3,
    the catch-up never fired, and the stream hung forever."""
    results = await _drive_sse(pg_seq=4, is_terminal=True, last_event_id=3, messages=[])
    encoded = [sse.encode().decode("utf-8") for sse in results]
    terminal = [e for e in encoded if "event: terminal" in e]
    assert len(terminal) == 1, f"expected the catch-up terminal, got {encoded}"
    assert "id: 4" in terminal[0], f"the terminal must carry the consumed seq 4: {terminal[0]}"
    assert any("event: done" in e for e in encoded), "the stream must close"


@pytest.mark.asyncio
async def test_sse_live_stream_orders_the_terminal_after_the_last_progress_event() -> None:
    """On the live stream the terminal event (seq 4) strictly follows the
    last progress event (seq 3) and closes the stream: no dedupe guard can
    drop it, the exact failure mode a seq-sharing terminal produced."""
    messages = [
        _event_json(seq=3, kind="progress", status="running", terminal=False),
        _event_json(seq=4, kind="state_change", status="succeeded", terminal=True),
    ]
    results = await _drive_sse(pg_seq=3, is_terminal=False, last_event_id=None, messages=messages)
    encoded = [sse.encode().decode("utf-8") for sse in results]
    ids = [int(e.split("id: ")[1].split("\n")[0]) for e in encoded if "id: " in e]
    assert ids == [3, 4], f"the terminal must follow the last progress event: {encoded}"
    assert any("event: terminal" in e for e in encoded)
    assert any("event: done" in e for e in encoded), "the stream must close"
