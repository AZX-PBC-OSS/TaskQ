"""Unit tests for ConcurrencyReservation and _InMemorySlotTable.

Tests through plus sync_slots
unit tests. These validate the ConcurrencyReservation class and
_InMemorySlotTable using the in-memory backend with FakeClock — no real PG
instance required.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from taskq._ids import new_uuid
from taskq.exceptions import ReservationUnavailable
from taskq.ratelimit.reservation import ConcurrencyReservation, _InMemorySlotTable
from taskq.testing.clock import FakeClock

_START = datetime(2025, 1, 1, tzinfo=UTC)
_LEASE = timedelta(seconds=10)


def _reservation(
    name: str = "test",
    slots: int = 4,
    lease: timedelta | float = _LEASE,
    clock: FakeClock | None = None,
) -> ConcurrencyReservation:
    if clock is None:
        clock = FakeClock(_START)
    return ConcurrencyReservation(name=name, slots=slots, lease=lease, clock=clock)


# ── Slot pre-allocation ─────────────────────────────────────────


async def test_ensure_slots_creates_rows() -> None:
    """Slot pre-allocation."""
    clock = FakeClock(_START)
    res = _reservation(name="gpu", slots=4, clock=clock)
    table = res.table
    table.ensure_slots("gpu", 4)

    assert table.get_slot_count("gpu") == 4
    for i in range(4):
        slot = table.get_slot("gpu", i)
        assert slot is not None
        assert slot.job_id is None
        assert slot.worker_id is None
        assert slot.acquired_at is None
        assert slot.lease_expires_at is None


# ── Idempotent pre-allocation ───────────────────────────────────


async def test_ensure_slots_idempotent() -> None:
    """Idempotent pre-allocation."""
    clock = FakeClock(_START)
    res = _reservation(name="gpu", slots=4, clock=clock)
    table = res.table
    table.ensure_slots("gpu", 4)
    table.ensure_slots("gpu", 4)

    assert table.get_slot_count("gpu") == 4


# ── Acquire a free slot ─────────────────────────────────────────


async def test_acquire_free_slot() -> None:
    """Acquire a free slot."""
    clock = FakeClock(_START)
    res = _reservation(name="gpu", slots=4, clock=clock)
    table = res.table
    table.ensure_slots("gpu", 4)

    job_id = new_uuid()
    worker_id = new_uuid()
    idx = table.acquire("gpu", job_id, worker_id, _LEASE)

    assert idx == 0
    slot = table.get_slot("gpu", 0)
    assert slot is not None
    assert slot.job_id == job_id
    assert slot.worker_id == worker_id
    assert slot.acquired_at == _START
    assert slot.lease_expires_at == _START + _LEASE


# ── Acquire all N slots; (N+1)th raises ReservationUnavailable ─


async def test_acquire_all_then_denied() -> None:
    """Acquire all N slots; (N+1)th raises ReservationUnavailable."""
    clock = FakeClock(_START)
    res = _reservation(name="gpu", slots=8, clock=clock)
    table = res.table
    table.ensure_slots("gpu", 8)

    for i in range(8):
        idx = table.acquire("gpu", new_uuid(), new_uuid(), _LEASE)
        assert idx == i

    with pytest.raises(ReservationUnavailable):
        table.acquire("gpu", new_uuid(), new_uuid(), _LEASE)


# ── Release a slot ──────────────────────────────────────────────


async def test_release_then_reacquire() -> None:
    """Release a slot."""
    clock = FakeClock(_START)
    res = _reservation(name="gpu", slots=4, clock=clock)
    table = res.table
    table.ensure_slots("gpu", 4)

    job_id = new_uuid()
    worker_id = new_uuid()
    idx = table.acquire("gpu", job_id, worker_id, _LEASE)
    assert idx == 0

    released = table.release("gpu", 0, worker_id)
    assert released is True

    slot = table.get_slot("gpu", 0)
    assert slot is not None
    assert slot.job_id is None

    idx2 = table.acquire("gpu", new_uuid(), new_uuid(), _LEASE)
    assert idx2 == 0


# ── ReservationUnavailable fields ───────────────────────────────


async def test_reservation_unavailable_fields() -> None:
    """ReservationUnavailable fields."""
    clock = FakeClock(_START)
    res = _reservation(name="gpu", slots=8, clock=clock)
    table = res.table
    table.ensure_slots("gpu", 8)

    for _ in range(8):
        table.acquire("gpu", new_uuid(), new_uuid(), _LEASE)

    with pytest.raises(ReservationUnavailable) as exc_info:
        table.acquire("gpu", new_uuid(), new_uuid(), _LEASE)

    e = exc_info.value
    assert e.bucket_name == "gpu"
    assert e.retry_after > timedelta(0)


# ── Lease expiry (FakeClock) ────────────────────────────────────


async def test_lease_expiry_reclaim() -> None:
    """Lease expiry (FakeClock)."""
    clock = FakeClock(_START)
    res = _reservation(name="gpu", slots=4, clock=clock)
    table = res.table
    table.ensure_slots("gpu", 4)

    job_id = new_uuid()
    worker_id = new_uuid()
    table.acquire("gpu", job_id, worker_id, _LEASE)

    clock.advance(_LEASE + timedelta(seconds=1))

    new_job_id = new_uuid()
    new_worker_id = new_uuid()
    idx = table.acquire("gpu", new_job_id, new_worker_id, _LEASE)
    assert idx == 0

    slot = table.get_slot("gpu", 0)
    assert slot is not None
    assert slot.job_id == new_job_id
    assert slot.worker_id == new_worker_id


# ── slots=0 raises ValueError ───────────────────────────────────


def test_slots_zero_raises() -> None:
    """slots=0 raises ValueError at construction."""
    with pytest.raises(ValueError, match="slots must be >= 1"):
        ConcurrencyReservation(name="gpu", slots=0, lease=_LEASE, clock=FakeClock(_START))


# ── lease=0 raises ValueError ────────────────────────────────────


def test_lease_zero_raises() -> None:
    """lease=0 raises ValueError at construction."""
    with pytest.raises(ValueError, match="lease must be > 0"):
        ConcurrencyReservation(name="gpu", slots=4, lease=0, clock=FakeClock(_START))


# ── sync_slots unit tests ──────────────────────────────────────────────


async def test_sync_slots_insertion() -> None:
    """sync_slots: insertion — 4 slots registered, 0 in table → 4 inserted."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)

    result = table.sync_slots("gpu", 4)

    assert len(result.inserted) == 4
    assert result.deleted == []
    assert result.skipped_held == []
    assert [(bn, i) for bn, i in result.inserted] == [
        ("gpu", 0),
        ("gpu", 1),
        ("gpu", 2),
        ("gpu", 3),
    ]
    assert table.get_slot_count("gpu") == 4


async def test_sync_slots_deletion() -> None:
    """sync_slots: deletion — 2 registered, 4 in table → 2 excess free deleted."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)
    table.ensure_slots("gpu", 4)

    result = table.sync_slots("gpu", 2)

    assert result.inserted == []
    assert len(result.deleted) == 2
    assert result.skipped_held == []
    assert sorted([i for _, i in result.deleted]) == [2, 3]
    assert table.get_slot_count("gpu") == 2


async def test_sync_slots_skips_held() -> None:
    """sync_slots: skips held — 2 registered, 4 in table, 1 excess held → 1 deleted, 1 skipped_held."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)
    table.ensure_slots("gpu", 4)

    held_worker = new_uuid()
    table.acquire("gpu", new_uuid(), held_worker, _LEASE)
    table.acquire("gpu", new_uuid(), held_worker, _LEASE)
    table.acquire("gpu", new_uuid(), held_worker, _LEASE)

    result = table.sync_slots("gpu", 2)

    assert result.inserted == []
    deleted_indices = [i for _, i in result.deleted]
    held_indices = [i for _, i in result.skipped_held]
    assert len(deleted_indices) == 1
    assert len(held_indices) == 1
    assert 2 in held_indices or 3 in held_indices
    for i in held_indices:
        slot = table.get_slot("gpu", i)
        assert slot is not None
        assert slot.job_id is not None


# ── _InMemorySlotTable: missing-bucket branches ─────────────────


async def test_acquire_missing_bucket_raises_unavailable() -> None:
    """acquire() against a bucket that was never ensure_slots'd raises."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)

    with pytest.raises(ReservationUnavailable):
        table.acquire("nope", new_uuid(), new_uuid(), _LEASE)


async def test_release_missing_bucket_returns_false() -> None:
    """release() against a bucket that doesn't exist returns False."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)

    assert table.release("nope", 0, new_uuid()) is False


async def test_release_worker_id_mismatch_returns_false() -> None:
    """release() with a worker_id that doesn't hold the slot returns False
    and leaves the slot untouched."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)
    table.ensure_slots("gpu", 2)
    holder = new_uuid()
    slot_index = table.acquire("gpu", new_uuid(), holder, _LEASE)

    assert table.release("gpu", slot_index, new_uuid()) is False
    slot = table.get_slot("gpu", slot_index)
    assert slot is not None
    assert slot.worker_id == holder


async def test_get_slot_missing_bucket_returns_none() -> None:
    """get_slot() against a missing bucket returns None."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)

    assert table.get_slot("nope", 0) is None


async def test_get_slot_count_missing_bucket_returns_zero() -> None:
    """get_slot_count() against a missing bucket returns 0."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)

    assert table.get_slot_count("nope") == 0


async def test_get_slot_indices_missing_bucket_returns_empty() -> None:
    """get_slot_indices() against a missing bucket returns []."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)

    assert table.get_slot_indices("nope") == []


# ── _InMemorySlotTable.extend_leases ────────────────────────────


async def test_extend_leases_only_extends_matching_worker_held_slots() -> None:
    """extend_leases() bumps lease_expires_at only for slots held by
    worker_id, leaves other workers' slots and free slots untouched."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)
    table.ensure_slots("gpu", 4)

    worker_a = new_uuid()
    worker_b = new_uuid()
    job_a1 = new_uuid()
    job_a2 = new_uuid()
    job_b = new_uuid()

    idx_a1 = table.acquire("gpu", job_a1, worker_a, _LEASE)
    idx_a2 = table.acquire("gpu", job_a2, worker_a, _LEASE)
    idx_b = table.acquire("gpu", job_b, worker_b, _LEASE)
    # slot 3 stays free

    new_lock_lease = timedelta(seconds=99)
    count = table.extend_leases(worker_a, new_lock_lease)

    assert count == 2
    for idx in (idx_a1, idx_a2):
        slot = table.get_slot("gpu", idx)
        assert slot is not None
        assert slot.lease_expires_at == _START + new_lock_lease

    slot_b = table.get_slot("gpu", idx_b)
    assert slot_b is not None
    assert slot_b.lease_expires_at == _START + _LEASE

    slot_free = table.get_slot("gpu", 3)
    assert slot_free is not None
    assert slot_free.job_id is None


async def test_extend_leases_no_held_slots_returns_zero() -> None:
    """extend_leases() with no matching held slots returns 0."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)
    table.ensure_slots("gpu", 4)

    assert table.extend_leases(new_uuid(), timedelta(seconds=30)) == 0


# ── _InMemorySlotTable.peek_slots ───────────────────────────────


async def test_peek_slots_missing_bucket_returns_zero_zero() -> None:
    """peek_slots() against a missing bucket returns (0, 0)."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)

    assert table.peek_slots("nope") == (0, 0)


async def test_peek_slots_counts_free_held_and_expired_as_free() -> None:
    """peek_slots() counts free + expired-lease slots as free, others held."""
    clock = FakeClock(_START)
    table = _InMemorySlotTable(clock)
    table.ensure_slots("gpu", 4)

    table.acquire("gpu", new_uuid(), new_uuid(), _LEASE)
    table.acquire("gpu", new_uuid(), new_uuid(), _LEASE)
    # slots 2, 3 remain free

    free, held = table.peek_slots("gpu")
    assert (free, held) == (2, 2)

    clock.advance(_LEASE + timedelta(seconds=1))
    free_after_expiry, held_after_expiry = table.peek_slots("gpu")
    assert (free_after_expiry, held_after_expiry) == (4, 0)


# ── ConcurrencyReservation: construction validation ─────────────


def test_lease_as_positive_float_converts_to_timedelta() -> None:
    """lease passed as a positive float converts to a timedelta and all
    basic properties are exposed."""
    res = ConcurrencyReservation(name="gpu", slots=3, lease=5.0, clock=FakeClock(_START))

    assert res.lease == timedelta(seconds=5.0)
    assert res.slots == 3
    assert res.name == "gpu"
    assert res.bucket_name == "gpu"


def test_lease_timedelta_zero_raises() -> None:
    """lease passed as a non-positive timedelta raises ValueError."""
    with pytest.raises(ValueError, match="lease must be > 0"):
        ConcurrencyReservation(name="gpu", slots=4, lease=timedelta(0), clock=FakeClock(_START))


def test_lease_timedelta_negative_raises() -> None:
    """lease passed as a negative timedelta raises ValueError."""
    with pytest.raises(ValueError, match="lease must be > 0"):
        ConcurrencyReservation(
            name="gpu", slots=4, lease=timedelta(seconds=-1), clock=FakeClock(_START)
        )


def test_invalid_schema_raises_value_error() -> None:
    """An invalid schema identifier raises ValueError at construction."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        ConcurrencyReservation(
            name="gpu",
            slots=4,
            lease=_LEASE,
            clock=FakeClock(_START),
            schema="bad-schema;drop table",
        )


def test_lock_lease_longer_than_lease_logs_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """lock_lease > lease logs a warning but does not raise."""
    res = ConcurrencyReservation(
        name="gpu",
        slots=4,
        lease=timedelta(seconds=5),
        lock_lease=timedelta(seconds=10),
        clock=FakeClock(_START),
    )
    assert res.name == "gpu"
    assert res.bucket_name == "gpu"


# ── ConcurrencyReservation: properties/errors without a clock ───


def test_table_property_without_clock_raises_runtime_error() -> None:
    """Accessing .table without clock= at construction raises RuntimeError."""
    res = ConcurrencyReservation(name="gpu", slots=4, lease=_LEASE)

    with pytest.raises(RuntimeError, match="in-memory table not available"):
        _ = res.table


async def test_acquire_without_pool_or_table_raises_runtime_error() -> None:
    """acquire(pool=None) without an in-memory table raises RuntimeError."""
    res = ConcurrencyReservation(name="gpu", slots=4, lease=_LEASE)

    with pytest.raises(RuntimeError, match="no in-memory table"):
        await res.acquire(new_uuid(), new_uuid(), pool=None)


async def test_release_without_pool_or_table_raises_runtime_error() -> None:
    """release(pool=None) without an in-memory table raises RuntimeError."""
    res = ConcurrencyReservation(name="gpu", slots=4, lease=_LEASE)

    with pytest.raises(RuntimeError, match="no in-memory table"):
        await res.release(0, new_uuid(), pool=None)


async def test_peek_without_pool_or_table_raises_runtime_error() -> None:
    """peek(pool=None) without an in-memory table raises RuntimeError."""
    res = ConcurrencyReservation(name="gpu", slots=4, lease=_LEASE)

    with pytest.raises(RuntimeError, match="no in-memory table"):
        await res.peek(pool=None)


async def test_peek_in_memory_reports_free_total_held() -> None:
    """peek(pool=None) with an in-memory table reports free/total/held counts."""
    clock = FakeClock(_START)
    res = _reservation(name="gpu", slots=4, clock=clock)

    await res.acquire(new_uuid(), new_uuid(), pool=None)
    await res.acquire(new_uuid(), new_uuid(), pool=None)

    result = await res.peek(pool=None)
    assert result == {"free_count": 2, "total_slots": 4, "held_count": 2}


# ── ConcurrencyReservation.peek: defensive PG-only branches ──────


class _FakeConn:
    def __init__(self, row: dict[str, int] | None) -> None:
        self._row = row

    async def fetchrow(self, _sql: str, _bucket_name: str) -> dict[str, int] | None:
        return self._row


class _FakeAcquireCtx:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class _FakePool:
    def __init__(self, row: dict[str, int] | None) -> None:
        self._row = row

    def acquire(self) -> _FakeAcquireCtx:
        return _FakeAcquireCtx(_FakeConn(self._row))


async def test_peek_pg_row_none_falls_back_to_defaults() -> None:
    """peek() with a PG pool whose query returns no row falls back to
    ``free_count == total_slots`` and ``held_count == 0`` defaults."""
    res = ConcurrencyReservation(name="gpu", slots=6, lease=_LEASE, schema="taskq")
    fake_pool: Any = _FakePool(row=None)

    result = await res.peek(pool=fake_pool)
    assert result == {"free_count": 6, "total_slots": 6, "held_count": 0}


async def test_peek_pg_row_present_returns_counts() -> None:
    """peek() with a PG pool returning a row surfaces the row's counts."""
    res = ConcurrencyReservation(name="gpu", slots=6, lease=_LEASE, schema="taskq")
    fake_pool: Any = _FakePool(row={"free_count": 2, "total_slots": 6, "held_count": 4})

    result = await res.peek(pool=fake_pool)
    assert result == {"free_count": 2, "total_slots": 6, "held_count": 4}


async def test_peek_invalid_mutated_schema_raises_value_error() -> None:
    """Defensive re-validation: if ``_schema`` is mutated post-construction
    to an invalid identifier, peek() raises ValueError before touching the
    pool (the pool is never awaited)."""
    res = ConcurrencyReservation(name="gpu", slots=4, lease=_LEASE, schema="taskq")
    res._schema = "bad;schema"  # type: ignore[misc] # Why: simulate corrupted post-construction state to exercise the defensive re-check

    with pytest.raises(ValueError, match="invalid schema identifier"):
        await res.peek(pool=object())  # type: ignore[arg-type] # Why: pool is never touched — the schema check raises first
