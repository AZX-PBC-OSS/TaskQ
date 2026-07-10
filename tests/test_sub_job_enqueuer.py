"""Unit tests for SubJobEnqueuer and EnqueueItem (through).

Pure-Python tests against the in-memory backend. No PG required.
"""

from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
from pydantic import BaseModel, TypeAdapter

from taskq.actor import ActorRef
from taskq.batch import EnqueueItem
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.exceptions import PartialBatchError, SubEnqueueError
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

_SENTINEL_POOL = object()


class _Payload(BaseModel):
    value: str = "test"


class _Result(BaseModel):
    ok: bool = True


def _make_actor_ref(
    *,
    name: str = "child_actor",
    queue: str = "default",
    singleton: bool = False,
    unique_for: timedelta | None = None,
    max_pending: int | None = None,
) -> ActorRef[_Payload, _Result]:
    async def _handler(payload: _Payload) -> _Result:
        return _Result()

    return ActorRef(
        name=name,
        queue=queue,
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_Payload,
        result_adapter=TypeAdapter(_Result),
        retry=__import__("taskq.retry", fromlist=["RetryPolicy"]).RetryPolicy(),
        result_ttl=None,
        singleton=singleton,
        unique_for=unique_for,
        max_pending=max_pending,
    )


def _make_backend() -> InMemoryBackend:
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    return InMemoryBackend(clock=clock)


def _make_enqueuer(
    *,
    loop_scope_resolved: dict[type, object] | None = None,
    worker_pool: object | None = None,
    backend: InMemoryBackend | None = None,
    clock: FakeClock | None = None,
) -> SubJobEnqueuer:
    if backend is None:
        backend = _make_backend()
    if clock is None:
        clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    return SubJobEnqueuer(
        loop_scope_resolved=loop_scope_resolved,
        worker_pool=worker_pool,  # type: ignore[arg-type] # Why: tests pass a plain object sentinel for worker_pool to satisfy the "pool is not None" guard; the autonomous path never calls pool.acquire()
        backend=backend,
        clock=clock,
    )


class _StubConn:
    """Minimal asyncpg.Connection stand-in for LOOP-scope resolved dict."""

    pass


# ── Sub-enqueue uses LOOP-scope conn when available ──────────────


async def test_tu1_loop_scope_conn_buffers_in_memory() -> None:
    """With a LOOP-scope conn on an in-memory backend, enqueue buffers."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    handle = await enqueuer.enqueue(ref, _Payload())

    assert enqueuer.pending_count == 1
    assert handle._row.id == enqueuer.pending_items[0].id
    assert handle._row.id not in backend._jobs


async def test_tu1_buffer_empty_until_flush() -> None:
    """Backend _jobs is empty until flush_buffer is called."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    await enqueuer.enqueue(ref, _Payload())
    assert len(backend._jobs) == 0

    await enqueuer.flush_buffer()
    assert len(backend._jobs) == 1


# ── Sub-enqueue falls back to worker pool ────────────────────────


async def test_tu2_no_loop_scope_uses_autonomous_path() -> None:
    """Without LOOP-scope conn, enqueue goes autonomous (no buffer)."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    enqueuer = _make_enqueuer(
        worker_pool=_SENTINEL_POOL,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    handle = await enqueuer.enqueue(ref, _Payload())

    assert len(backend._jobs) == 1
    assert enqueuer.pending_count == 0
    assert backend._jobs[handle._row.id] is not None


# ── Explicit connection= override ────────────────────────────────


async def test_tu3_explicit_connection_overrides() -> None:
    """Explicit connection= bypasses buffer and inserts directly."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    explicit_conn = _StubConn()
    handle = await enqueuer.enqueue(ref, _Payload(), connection=explicit_conn)

    assert len(backend._jobs) == 1
    assert enqueuer.pending_count == 0
    assert backend._jobs[handle._row.id] is not None


# ── Per-100 periodic re-warn ─────────────────────────────────────


async def test_tu4_periodic_rewarn_at_100() -> None:
    """100 autonomous enqueues all land in the backend."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    enqueuer = _make_enqueuer(
        worker_pool=_SENTINEL_POOL,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    for _ in range(100):
        await enqueuer.enqueue(ref, _Payload())

    assert len(backend._jobs) == 100
    assert enqueuer._autonomous_enqueue_count == 100


# ── Parent succeeds → buffer flushed; rows visible ──────────────


async def test_tu5_flush_makes_rows_visible() -> None:
    """Two enqueues via buffer; flush makes both rows visible."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    await enqueuer.enqueue(ref, _Payload(value="a"))
    await enqueuer.enqueue(ref, _Payload(value="b"))

    assert len(backend._jobs) == 0

    await enqueuer.flush_buffer()

    assert len(backend._jobs) == 2


# ── Parent raises Transient → discard_buffer ─────────────────────


async def test_tu6_discard_buffer_transient() -> None:
    """discard_buffer on transient failure; rows NOT in _jobs."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    await enqueuer.enqueue(ref, _Payload(value="a"))
    await enqueuer.enqueue(ref, _Payload(value="b"))

    enqueuer.discard_buffer()

    assert len(backend._jobs) == 0
    assert enqueuer.pending_count == 0


# ── Parent raises Snooze → discard_buffer ────────────────────────


async def test_tu7_discard_buffer_snooze() -> None:
    """discard_buffer on Snooze; rows NOT in _jobs."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    await enqueuer.enqueue(ref, _Payload(value="a"))
    enqueuer.discard_buffer()

    assert len(backend._jobs) == 0


# ── Parent raises RetryAfter → discard_buffer ────────────────────


async def test_tu8_discard_buffer_retry_after() -> None:
    """discard_buffer on RetryAfter; rows NOT in _jobs."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    await enqueuer.enqueue(ref, _Payload(value="a"))
    enqueuer.discard_buffer()

    assert len(backend._jobs) == 0


# ── Idempotency_key dedup respected during flush ─────────────────


async def test_tu9_idempotency_key_dedup_during_flush() -> None:
    """Two items with the same idempotency_key; flush yields one row."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    await enqueuer.enqueue(ref, _Payload(value="a"), idempotency_key="key1")
    await enqueuer.enqueue(ref, _Payload(value="b"), idempotency_key="key1")

    await enqueuer.flush_buffer()

    assert len(backend._jobs) == 1


# ── enqueue() outside actor body raises RuntimeError ─────────────


async def test_tn1_outside_actor_body_raises() -> None:
    """enqueue with worker_pool=None and no LOOP-scope conn raises RuntimeError."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    enqueuer = _make_enqueuer(
        worker_pool=None,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    with pytest.raises(RuntimeError, match=r"ctx\.jobs is only available inside an actor body"):
        await enqueuer.enqueue(ref, _Payload())


# ── Buffer handle id stability ──────────────────────────────────────────


async def test_buffer_handle_id_stable_after_flush() -> None:
    """Buffer-path handle id matches flushed row id."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    handle = await enqueuer.enqueue(ref, _Payload())
    buffered_id = handle._row.id

    await enqueuer.flush_buffer()

    assert buffered_id in backend._jobs
    flushed_row = backend._jobs[buffered_id]
    assert flushed_row.id == buffered_id


# ── flush_buffer logs per-item errors and continues ──────────────────────


async def test_flush_buffer_raises_sub_enqueue_error_on_failure() -> None:
    """Per-item flush failures are collected and re-raised as SubEnqueueError."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    await enqueuer.enqueue(ref, _Payload(value="a"))
    await enqueuer.enqueue(ref, _Payload(value="b"))

    original_enqueue = backend.enqueue
    call_count = 0

    async def _failing_enqueue(args: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("synthetic flush failure")
        return await original_enqueue(args)

    backend.enqueue = _failing_enqueue  # type: ignore[assignment] # Why: test injects a failing enqueue to exercise the SubEnqueueError path in flush_buffer

    with pytest.raises(SubEnqueueError) as exc_info:
        await enqueuer.flush_buffer()

    assert len(exc_info.value.failed_items) == 1
    assert len(backend._jobs) == 1
    assert enqueuer.pending_count == 0


# ── EnqueueItem dataclass ────────────────────────────────────────────────


def test_enqueue_item_frozen() -> None:
    """EnqueueItem is frozen."""
    from pydantic import ValidationError

    ref = _make_actor_ref()
    item = EnqueueItem(actor_ref=ref, payload=_Payload())
    with pytest.raises(ValidationError):
        item.priority = 99  # type: ignore[misc] # Why: assigning to frozen model to assert ValidationError


def test_enqueue_item_defaults() -> None:
    """EnqueueItem defaults match the spec."""
    ref = _make_actor_ref()
    item = EnqueueItem(actor_ref=ref, payload=_Payload())
    assert item.scheduled_at is None
    assert item.priority is None
    assert item.fairness_key is None
    assert item.metadata == {}


# ── enqueue_batch ─────────────────────────────────────────────────────────


async def test_enqueue_batch_returns_handles() -> None:
    """enqueue_batch calls enqueue per item and returns handles."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    enqueuer = _make_enqueuer(
        worker_pool=_SENTINEL_POOL,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    items = [
        EnqueueItem(actor_ref=ref, payload=_Payload(value="a")),
        EnqueueItem(actor_ref=ref, payload=_Payload(value="b")),
    ]
    handles = await enqueuer.enqueue_batch(items)

    assert len(handles) == 2
    assert len(backend._jobs) == 2


async def test_enqueue_batch_with_connection() -> None:
    """enqueue_batch threads connection= through to each item."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    explicit_conn = _StubConn()
    items = [
        EnqueueItem(actor_ref=ref, payload=_Payload(value="a")),
        EnqueueItem(actor_ref=ref, payload=_Payload(value="b")),
    ]
    handles = await enqueuer.enqueue_batch(items, connection=explicit_conn)

    assert len(handles) == 2
    assert len(backend._jobs) == 2
    assert enqueuer.pending_count == 0


# ── Autonomous counter increments correctly ──────────────────────────────


async def test_autonomous_count_increments() -> None:
    """Autonomous enqueue increments _autonomous_enqueue_count."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    enqueuer = _make_enqueuer(
        worker_pool=_SENTINEL_POOL,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    assert enqueuer._autonomous_enqueue_count == 0
    await enqueuer.enqueue(ref, _Payload())
    assert enqueuer._autonomous_enqueue_count == 1
    await enqueuer.enqueue(ref, _Payload())
    assert enqueuer._autonomous_enqueue_count == 2


# ── Connection resolution priority ────────────────────────────────────────


async def test_explicit_connection_overrides_loop_scope() -> None:
    """Explicit connection= takes priority over LOOP-scope conn."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    explicit_conn = _StubConn()
    await enqueuer.enqueue(ref, _Payload(), connection=explicit_conn)

    assert enqueuer.pending_count == 0
    assert len(backend._jobs) == 1


async def test_loop_scope_overrides_autonomous() -> None:
    """LOOP-scope conn takes priority over autonomous fallback."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        worker_pool=_SENTINEL_POOL,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    await enqueuer.enqueue(ref, _Payload())

    assert enqueuer.pending_count == 1
    assert len(backend._jobs) == 0


async def test_no_loop_scope_no_explicit_uses_autonomous() -> None:
    """No LOOP-scope conn and no explicit connection= falls back to autonomous."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    enqueuer = _make_enqueuer(
        worker_pool=_SENTINEL_POOL,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    await enqueuer.enqueue(ref, _Payload())

    assert enqueuer.pending_count == 0
    assert len(backend._jobs) == 1


# ── M3: enqueue_batch atomicity ───────────────────────────────────────────


async def test_enqueue_batch_loop_scope_buffers_all_items() -> None:
    """enqueue_batch with LOOP-scope conn on in-memory backend buffers all items atomically."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    items = [
        EnqueueItem(actor_ref=ref, payload=_Payload(value="a")),
        EnqueueItem(actor_ref=ref, payload=_Payload(value="b")),
        EnqueueItem(actor_ref=ref, payload=_Payload(value="c")),
    ]
    handles = await enqueuer.enqueue_batch(items)

    assert len(handles) == 3
    assert enqueuer.pending_count == 3
    assert len(backend._jobs) == 0

    await enqueuer.flush_buffer()

    assert len(backend._jobs) == 3
    assert enqueuer.pending_count == 0


async def test_enqueue_batch_autonomous_partial_failure_raises() -> None:
    """enqueue_batch autonomous path raises PartialBatchError listing failed items."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    enqueuer = _make_enqueuer(
        worker_pool=_SENTINEL_POOL,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    original_enqueue = backend.enqueue
    call_count = 0

    async def _failing_enqueue(args: object) -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise RuntimeError("synthetic item failure")
        return await original_enqueue(args)

    backend.enqueue = _failing_enqueue  # type: ignore[assignment] # Why: test injects a failing enqueue to exercise PartialBatchError

    items = [
        EnqueueItem(actor_ref=ref, payload=_Payload(value="a")),
        EnqueueItem(actor_ref=ref, payload=_Payload(value="b")),
        EnqueueItem(actor_ref=ref, payload=_Payload(value="c")),
    ]

    with pytest.raises(PartialBatchError) as exc_info:
        await enqueuer.enqueue_batch(items)

    err = exc_info.value
    assert err.succeeded_count == 2
    assert err.total == 3
    assert len(err.failed_items) == 1
    failed_indices = [i for i, _ in err.failed_items]
    assert failed_indices == [1]


async def test_enqueue_batch_no_pool_raises_runtime_error() -> None:
    """enqueue_batch with no pool and no LOOP-scope conn raises RuntimeError."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    enqueuer = _make_enqueuer(
        worker_pool=None,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    items = [EnqueueItem(actor_ref=ref, payload=_Payload())]

    with pytest.raises(RuntimeError, match=r"ctx\.jobs is only available inside an actor body"):
        await enqueuer.enqueue_batch(items)


# ── M4: enqueue_batch batch_id injection ──────────────────────────────────


async def test_enqueue_batch_injects_batch_id_autonomous() -> None:
    """enqueue_batch autonomous path injects batch_id into each item's metadata."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    enqueuer = _make_enqueuer(
        worker_pool=_SENTINEL_POOL,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    items = [
        EnqueueItem(actor_ref=ref, payload=_Payload(value="a")),
        EnqueueItem(actor_ref=ref, payload=_Payload(value="b")),
    ]
    handles = await enqueuer.enqueue_batch(items)

    batch_ids: set[str] = set()
    for handle in handles:
        meta = handle._row.metadata
        assert "batch_id" in meta
        batch_ids.add(str(meta["batch_id"]))

    assert len(batch_ids) == 1


async def test_enqueue_batch_injects_batch_id_connected() -> None:
    """enqueue_batch with explicit connection injects batch_id into each item's metadata."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    explicit_conn = _StubConn()
    items = [
        EnqueueItem(actor_ref=ref, payload=_Payload(value="a")),
        EnqueueItem(actor_ref=ref, payload=_Payload(value="b")),
    ]
    handles = await enqueuer.enqueue_batch(items, connection=explicit_conn)

    batch_ids: set[str] = set()
    for handle in handles:
        meta = handle._row.metadata
        assert "batch_id" in meta
        batch_ids.add(str(meta["batch_id"]))

    assert len(batch_ids) == 1


async def test_enqueue_batch_injects_batch_id_buffered() -> None:
    """enqueue_batch LOOP-scope buffer path injects batch_id into flushed rows."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    stub_conn = _StubConn()
    enqueuer = _make_enqueuer(
        loop_scope_resolved={asyncpg.Connection: stub_conn},
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    items = [
        EnqueueItem(actor_ref=ref, payload=_Payload(value="a")),
        EnqueueItem(actor_ref=ref, payload=_Payload(value="b")),
    ]
    handles = await enqueuer.enqueue_batch(items)

    for handle in handles:
        assert "batch_id" in handle._row.metadata

    await enqueuer.flush_buffer()

    for _job_id, row in backend._jobs.items():
        assert "batch_id" in row.metadata

    flushed_batch_ids: set[str] = set()
    for row in backend._jobs.values():
        flushed_batch_ids.add(str(row.metadata["batch_id"]))
    assert len(flushed_batch_ids) == 1


# ── M4: enqueue_batch forwards idempotency_key / identity_key ───────────


async def test_enqueue_batch_forwards_idempotency_key() -> None:
    """enqueue_batch autonomous path forwards idempotency_key to each item."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    enqueuer = _make_enqueuer(
        worker_pool=_SENTINEL_POOL,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    items = [
        EnqueueItem(actor_ref=ref, payload=_Payload(value="a"), idempotency_key="key-a"),
        EnqueueItem(actor_ref=ref, payload=_Payload(value="b"), idempotency_key="key-b"),
    ]
    handles = await enqueuer.enqueue_batch(items)

    assert handles[0]._row.idempotency_key is not None
    assert handles[1]._row.idempotency_key is not None


async def test_enqueue_batch_forwards_identity_key() -> None:
    """enqueue_batch autonomous path forwards identity_key to each item."""
    backend = _make_backend()
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    enqueuer = _make_enqueuer(
        worker_pool=_SENTINEL_POOL,
        backend=backend,
        clock=clock,
    )
    ref = _make_actor_ref()

    items = [
        EnqueueItem(actor_ref=ref, payload=_Payload(value="a"), identity_key="id-a"),
        EnqueueItem(actor_ref=ref, payload=_Payload(value="b"), identity_key="id-b"),
    ]
    handles = await enqueuer.enqueue_batch(items)

    assert handles[0]._row.identity_key is not None
    assert handles[1]._row.identity_key is not None


def test_enqueue_item_has_idempotency_key_and_identity_key() -> None:
    """EnqueueItem dataclass exposes idempotency_key and identity_key fields."""
    ref = _make_actor_ref()
    item = EnqueueItem(
        actor_ref=ref,
        payload=_Payload(),
        idempotency_key="test-key",
        identity_key="test-identity",
    )
    assert item.idempotency_key == "test-key"
    assert item.identity_key == "test-identity"

    item_default = EnqueueItem(actor_ref=ref, payload=_Payload())
    assert item_default.idempotency_key is None
    assert item_default.identity_key is None
