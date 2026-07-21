"""Concurrency reservation primitive using pre-allocated slot rows.

PG-only — no Redis fast path. Slot rows live in ``taskq.reservation_slots``;
acquisition uses ``FOR UPDATE SKIP LOCKED`` in a CTE (verbatim from ).
The heartbeat loop (``src/taskq/worker/heartbeat.py``) already extends
``reservation_slots.lease_expires_at`` in the same transaction as job locks;
this module does not modify the heartbeat.

The in-memory backend (``_InMemorySlotTable``) is the unit-test substitute for
PG and mirrors the slot-row model as a ``dict[str, dict[int, _SlotState]]``
where each ``(bucket_name, slot_index)`` pair is a persistent key, matching
the PG primary key. Thread-safe via ``threading.Lock`` with ``Clock``
injection for deterministic lease expiry.
"""

import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from taskq.backend.clock import Clock
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_RESERVATION_BACKOFF,
)
from taskq.exceptions import ReservationUnavailable

if TYPE_CHECKING:
    import asyncpg

logger = structlog.get_logger("taskq.ratelimit.reservation")

_ENSURE_SLOTS_SQL_TEMPLATE = """\
INSERT INTO "{schema}".reservation_slots (bucket_name, slot_index)
SELECT $1, generate_series(0, $2 - 1)
ON CONFLICT (bucket_name, slot_index) DO NOTHING"""

_ACQUIRE_SQL_TEMPLATE = """\
WITH free_slot AS (
    SELECT slot_index FROM "{schema}".reservation_slots
    WHERE bucket_name = $1
      AND (job_id IS NULL OR lease_expires_at < now())
    ORDER BY slot_index
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE "{schema}".reservation_slots
SET job_id            = $2,
    held_by_worker_id = $3,
    acquired_at       = now(),
    lease_expires_at  = now() + $4 * INTERVAL '1 second'
WHERE (bucket_name, slot_index) IN (SELECT $1, slot_index FROM free_slot)
RETURNING slot_index"""

_RELEASE_SQL_TEMPLATE = """\
UPDATE "{schema}".reservation_slots
SET job_id            = NULL,
    held_by_worker_id = NULL,
    acquired_at       = NULL,
    lease_expires_at  = NULL
WHERE bucket_name       = $1
  AND slot_index        = $2
  AND held_by_worker_id = $3"""

_SYNC_EXISTING_SQL_TEMPLATE = """\
SELECT slot_index FROM "{schema}".reservation_slots
WHERE bucket_name = $1
ORDER BY slot_index"""

_SYNC_INSERT_SQL_TEMPLATE = """\
INSERT INTO "{schema}".reservation_slots (bucket_name, slot_index)
SELECT $1, unnest($2::int[])
ON CONFLICT (bucket_name, slot_index) DO NOTHING
RETURNING slot_index"""

_SYNC_DELETE_SQL_TEMPLATE = """\
DELETE FROM "{schema}".reservation_slots
WHERE bucket_name = $1
  AND slot_index = ANY($2)
  AND job_id IS NULL
RETURNING slot_index"""

_SYNC_HELD_SQL_TEMPLATE = """\
SELECT slot_index FROM "{schema}".reservation_slots
WHERE bucket_name = $1
  AND slot_index = ANY($2)
  AND job_id IS NOT NULL
ORDER BY slot_index"""


@dataclass(frozen=True, slots=True)
class SyncResult:
    """Result of a ``sync_slots`` call."""

    inserted: list[tuple[str, int]]
    deleted: list[tuple[str, int]]
    skipped_held: list[tuple[str, int]]


@dataclass(frozen=True, slots=True)
class _SlotState:
    """Single slot row in the in-memory table."""

    job_id: UUID | None = None
    worker_id: UUID | None = None
    acquired_at: datetime | None = None
    lease_expires_at: datetime | None = None


class _InMemorySlotTable:
    """In-memory slot table for unit tests.

    ``dict[str, dict[int, _SlotState]]`` keyed by ``bucket_name``. Each bucket
    is a ``dict[int, _SlotState]`` mapping persistent ``slot_index`` → slot
    state, mirroring the PG model where ``(bucket_name, slot_index)`` is a
    primary key. Thread-safe via ``threading.Lock``. Uses ``Clock`` injection
    for deterministic lease-expiry so ``FakeClock`` tests don't require real
    time passage.
    """

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._lock = threading.Lock()
        self._buckets: dict[str, dict[int, _SlotState]] = {}

    def ensure_slots(self, bucket_name: str, slots: int) -> None:
        with self._lock:
            bucket = self._buckets.setdefault(bucket_name, {})
            for i in range(slots):
                if i not in bucket:
                    bucket[i] = _SlotState()

    def acquire(
        self,
        bucket_name: str,
        job_id: UUID,
        worker_id: UUID,
        lease: timedelta,
    ) -> int:
        now = self._clock.now()
        with self._lock:
            bucket = self._buckets.get(bucket_name)
            if bucket is None:
                raise ReservationUnavailable(bucket_name, DEFAULT_RESERVATION_BACKOFF)

            for i in sorted(bucket):
                slot = bucket[i]
                free = slot.job_id is None
                expired = slot.lease_expires_at is not None and slot.lease_expires_at < now
                if free or expired:
                    bucket[i] = _SlotState(
                        job_id=job_id,
                        worker_id=worker_id,
                        acquired_at=now,
                        lease_expires_at=now + lease,
                    )
                    return i

            raise ReservationUnavailable(bucket_name, DEFAULT_RESERVATION_BACKOFF)

    def release(
        self,
        bucket_name: str,
        slot_index: int,
        worker_id: UUID,
    ) -> bool:
        with self._lock:
            bucket = self._buckets.get(bucket_name)
            if bucket is None:
                return False
            slot = bucket.get(slot_index)
            if slot is None or slot.worker_id != worker_id:
                return False
            bucket[slot_index] = _SlotState()
            return True

    def extend_leases(self, worker_id: UUID, lock_lease: timedelta) -> int:
        now = self._clock.now()
        count = 0
        with self._lock:
            for bucket in self._buckets.values():
                for i, slot in bucket.items():
                    if slot.worker_id == worker_id and slot.job_id is not None:
                        bucket[i] = _SlotState(
                            job_id=slot.job_id,
                            worker_id=slot.worker_id,
                            acquired_at=slot.acquired_at,
                            lease_expires_at=now + lock_lease,
                        )
                        count += 1
        return count

    def get_slot(self, bucket_name: str, slot_index: int) -> _SlotState | None:
        with self._lock:
            bucket = self._buckets.get(bucket_name)
            if bucket is None:
                return None
            return bucket.get(slot_index)

    def get_slot_count(self, bucket_name: str) -> int:
        with self._lock:
            bucket = self._buckets.get(bucket_name)
            if bucket is None:
                return 0
            return len(bucket)

    def get_slot_indices(self, bucket_name: str) -> list[int]:
        with self._lock:
            bucket = self._buckets.get(bucket_name)
            if bucket is None:
                return []
            return sorted(bucket)

    def peek_slots(self, bucket_name: str) -> tuple[int, int]:
        """Return ``(free_count, held_count)`` for *bucket_name*."""
        now = self._clock.now()
        with self._lock:
            bucket = self._buckets.get(bucket_name)
            if bucket is None:
                return (0, 0)
            free = 0
            held = 0
            for slot in bucket.values():
                expired = slot.lease_expires_at is not None and slot.lease_expires_at < now
                if slot.job_id is None or expired:
                    free += 1
                else:
                    held += 1
            return (free, held)

    def sync_slots(
        self,
        bucket_name: str,
        desired_slots: int,
    ) -> SyncResult:
        """In-memory equivalent of the PG ``sync_slots`` function.

        Computes diff between *desired_slots* and the current bucket state:
        inserts missing slots (fills gaps from prior held-slot-preserving
        shrinks), deletes excess free slots, and reports held slots that
        cannot be deleted. Slot indices are persistent keys and never shift
        — matching the PG model.
        """
        inserted: list[tuple[str, int]] = []
        deleted: list[tuple[str, int]] = []
        skipped_held: list[tuple[str, int]] = []

        with self._lock:
            bucket = self._buckets.get(bucket_name)
            if bucket is None:
                bucket = {}
                self._buckets[bucket_name] = bucket

            for i in range(desired_slots):
                if i not in bucket:
                    bucket[i] = _SlotState()
                    inserted.append((bucket_name, i))

            for i in sorted(bucket):
                if i < desired_slots:
                    continue
                slot = bucket.get(i)
                if slot is not None and slot.job_id is not None:
                    skipped_held.append((bucket_name, i))
                else:
                    del bucket[i]
                    deleted.append((bucket_name, i))

        return SyncResult(
            inserted=inserted,
            deleted=deleted,
            skipped_held=skipped_held,
        )


def _validate_schema(schema: str) -> None:
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")


class ConcurrencyReservation:
    """Concurrency reservation using pre-allocated slot rows.

    Raises :class:`ValueError` if ``slots < 1`` or ``lease <= 0``.
    Raises :class:`ReservationUnavailable` when no slot is available.
    """

    __slots__ = (
        "_acquire_sql",
        "_ensure_sql",
        "_lease",
        "_lock_lease",
        "_name",
        "_release_sql",
        "_schema",
        "_slots",
        "_table",
    )

    def __init__(
        self,
        name: str,
        slots: int,
        lease: timedelta | float,
        lock_lease: timedelta | None = None,
        *,
        clock: Clock | None = None,
        schema: str = "taskq",
    ) -> None:
        if slots < 1:
            raise ValueError(f"slots must be >= 1, got {slots}")

        if isinstance(lease, timedelta):
            if lease <= timedelta(0):
                raise ValueError(f"lease must be > 0, got {lease!r}")
            lease_td = lease
        else:
            if lease <= 0:
                raise ValueError(f"lease must be > 0, got {lease}")
            lease_td = timedelta(seconds=lease)

        self._name = name
        self._slots = slots
        self._lease = lease_td
        self._lock_lease = lock_lease
        self._schema = schema

        if lock_lease is not None and lease_td < lock_lease:
            logger.warning(
                "reservation-lease-shorter-than-lock-lease",
                bucket_name=name,
                lease=lease_td,
                lock_lease=lock_lease,
            )

        _validate_schema(schema)
        self._ensure_sql = _ENSURE_SLOTS_SQL_TEMPLATE.format(schema=schema)
        self._acquire_sql = _ACQUIRE_SQL_TEMPLATE.format(schema=schema)
        self._release_sql = _RELEASE_SQL_TEMPLATE.format(schema=schema)

        if clock is not None:
            self._table: _InMemorySlotTable | None = _InMemorySlotTable(clock)
        else:
            self._table = None

    @property
    def schema(self) -> str:
        """The PG schema this reservation's slot table lives in.

        Workers filter registry-global reservations by their own schema at
        startup (a process-global registry may carry reservations declared
        for other schemas/databases — touching those would write into the
        wrong schema or fail noisily).
        """
        return self._schema

    @property
    def name(self) -> str:
        return self._name

    @property
    def slots(self) -> int:
        return self._slots

    @property
    def lease(self) -> timedelta:
        return self._lease

    @property
    def bucket_name(self) -> str:
        return self._name

    @property
    def table(self) -> _InMemorySlotTable:
        """The in-memory slot table (requires ``clock`` at construction)."""
        if self._table is None:
            raise RuntimeError("in-memory table not available — pass clock= at construction")
        return self._table

    async def ensure_slots(self, pool: "asyncpg.Pool") -> None:
        """Idempotent pre-allocation of slot rows."""
        async with pool.acquire() as conn:
            await conn.execute(self._ensure_sql, self._name, self._slots)

    async def acquire(
        self,
        job_id: UUID,
        worker_id: UUID,
        pool: "asyncpg.Pool | None" = None,
    ) -> int:
        """Acquire a slot. Returns ``slot_index``.

        When *pool* is ``None``, the in-memory table (``clock=`` at
        construction) is used.  Raises :class:`ReservationUnavailable` when
        no slot is available.
        """
        if pool is None:
            if self._table is None:
                raise RuntimeError(
                    "pool=None but no in-memory table — pass clock= at "
                    "construction for in-memory acquire, or supply a PG pool"
                )
            self._table.ensure_slots(self._name, self._slots)
            slot_index = self._table.acquire(
                self._name,
                job_id,
                worker_id,
                self._lease,
            )
            logger.debug(
                "reservation-acquired",
                bucket_name=self._name,
                slot_index=slot_index,
                job_id=job_id,
                worker_id=worker_id,
                backend="memory",
            )
            return slot_index

        async with pool.acquire() as conn, conn.transaction():
            row = await conn.fetchrow(
                self._acquire_sql,
                self._name,
                job_id,
                worker_id,
                self._lease.total_seconds(),
            )

        if row is None:
            logger.info(
                "reservation-unavailable",
                bucket_name=self._name,
            )
            raise ReservationUnavailable(self._name, DEFAULT_RESERVATION_BACKOFF)

        slot_index: int = row["slot_index"]
        logger.debug(
            "reservation-acquired",
            bucket_name=self._name,
            slot_index=slot_index,
            job_id=job_id,
            worker_id=worker_id,
        )
        return slot_index

    async def release(
        self,
        slot_index: int,
        worker_id: UUID,
        pool: "asyncpg.Pool | None" = None,
    ) -> None:
        """Release slot. No-op if ``worker_id`` mismatch.

        When *pool* is ``None``, the in-memory table is used.
        """
        if pool is None:
            if self._table is None:
                raise RuntimeError(
                    "pool=None but no in-memory table — pass clock= at "
                    "construction for in-memory release, or supply a PG pool"
                )
            self._table.release(self._name, slot_index, worker_id)
            logger.debug(
                "reservation-released",
                bucket_name=self._name,
                slot_index=slot_index,
                worker_id=worker_id,
                backend="memory",
            )
            return

        async with pool.acquire() as conn:
            await conn.execute(
                self._release_sql,
                self._name,
                slot_index,
                worker_id,
            )
        logger.debug(
            "reservation-released",
            bucket_name=self._name,
            slot_index=slot_index,
            worker_id=worker_id,
        )

    async def peek(self, pool: "asyncpg.Pool | None" = None) -> dict[str, object]:
        """Return ``{"free_count": int, "total_slots": int}`` for the bucket.

        When *pool* is ``None``, the in-memory table is used.
        """
        if pool is None:
            if self._table is None:
                raise RuntimeError(
                    "pool=None but no in-memory table — pass clock= at "
                    "construction for in-memory peek, or supply a PG pool"
                )
            free, held = self._table.peek_slots(self._name)
            return {"free_count": free, "total_slots": self._slots, "held_count": held}

        if not _IDENT_RE.match(self._schema):
            raise ValueError(f"invalid schema identifier: {self._schema!r}")
        schema = self._schema

        # Schema-name interpolation ; schema_name is
        # pre-validated against _IDENT_RE at WorkerSettings load time.
        peek_sql = (
            f"SELECT count(*) FILTER (WHERE job_id IS NULL OR lease_expires_at < now()) AS free_count, "  # noqa: S608
            f"count(*) AS total_slots, "
            f"count(*) FILTER (WHERE job_id IS NOT NULL AND lease_expires_at >= now()) AS held_count "
            f'FROM "{schema}".reservation_slots WHERE bucket_name = $1'
        )
        async with pool.acquire() as conn:
            row = await conn.fetchrow(peek_sql, self._name)

        if row is None:
            return {"free_count": self._slots, "total_slots": self._slots, "held_count": 0}

        return {
            "free_count": int(row["free_count"]),
            "total_slots": int(row["total_slots"]),
            "held_count": int(row["held_count"]),
        }


async def sync_slots(
    reservations: list[ConcurrencyReservation],
    pool: "asyncpg.Pool",
    *,
    schema: str = "taskq",
) -> SyncResult:
    """Synchronise slot rows to match the registered reservation config.

    For each reservation: insert missing slots (filling gaps from prior
    held-slot-preserving shrinks), delete excess free slots, and report
    held slots that could not be deleted.
    """
    _validate_schema(schema)

    all_inserted: list[tuple[str, int]] = []
    all_deleted: list[tuple[str, int]] = []
    all_skipped: list[tuple[str, int]] = []

    for res in reservations:
        n_inserted = 0
        n_deleted = 0
        n_skipped = 0

        async with pool.acquire() as conn, conn.transaction():
            existing_sql = _SYNC_EXISTING_SQL_TEMPLATE.format(schema=schema)
            existing_rows = await conn.fetch(existing_sql, res.name)
            existing_indices: set[int] = {row["slot_index"] for row in existing_rows}

            desired_set = set(range(res.slots))
            missing_indices = sorted(desired_set - existing_indices)
            excess_indices = sorted(existing_indices - desired_set)

            if missing_indices:
                insert_sql = _SYNC_INSERT_SQL_TEMPLATE.format(schema=schema)
                rows = await conn.fetch(
                    insert_sql,
                    res.name,
                    missing_indices,
                )
                for row in rows:
                    all_inserted.append((res.name, row["slot_index"]))
                n_inserted = len(rows)

            if excess_indices:
                held_sql = _SYNC_HELD_SQL_TEMPLATE.format(schema=schema)
                held_rows = await conn.fetch(
                    held_sql,
                    res.name,
                    excess_indices,
                )
                for row in held_rows:
                    all_skipped.append((res.name, row["slot_index"]))
                n_skipped = len(held_rows)

                delete_sql = _SYNC_DELETE_SQL_TEMPLATE.format(schema=schema)
                deleted_rows = await conn.fetch(
                    delete_sql,
                    res.name,
                    excess_indices,
                )
                for row in deleted_rows:
                    all_deleted.append((res.name, row["slot_index"]))
                n_deleted = len(deleted_rows)

        logger.debug(
            "reservation-sync-slots",
            bucket_name=res.name,
            inserted=n_inserted,
            deleted=n_deleted,
            skipped=n_skipped,
        )

    return SyncResult(
        inserted=all_inserted,
        deleted=all_deleted,
        skipped_held=all_skipped,
    )
