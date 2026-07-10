"""In-memory reservation slot table for InMemoryBackend.

``_SlotTable`` mirrors the PG ``reservation_slots`` model: a
``dict[str, dict[int, _SlotState]]`` keyed by bucket name, where each
bucket maps a persistent ``slot_index`` to slot state. ``(bucket_name,
slot_index)`` is the primary key. Thread-safe via ``threading.Lock`` so
the slot table can be consulted from the heartbeat path.
"""

import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID

__all__ = ["_SlotState", "_SlotTable"]


@dataclass(frozen=True, slots=True)
class _SlotState:
    """Single reservation slot row in the in-memory table."""

    job_id: UUID | None = None
    worker_id: UUID | None = None
    acquired_at: datetime | None = None
    lease_expires_at: datetime | None = None


class _SlotTable:
    """In-memory reservation slot storage used by InMemoryBackend.

    Used by ``extend_reservation_leases`` to extend lease durations for
    slots held by a given worker.
    """

    def __init__(self) -> None:
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
        now: datetime,
    ) -> int:
        with self._lock:
            bucket = self._buckets.get(bucket_name)
            if bucket is None:
                return -1

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

            return -1

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

    def extend_leases_for_job(
        self,
        job_id: UUID,
        now: datetime,
        lock_lease: timedelta,
    ) -> int:
        with self._lock:
            count = 0
            for bucket in self._buckets.values():
                for i, slot in bucket.items():
                    if slot.job_id == job_id:
                        bucket[i] = _SlotState(
                            job_id=slot.job_id,
                            worker_id=slot.worker_id,
                            acquired_at=slot.acquired_at,
                            lease_expires_at=now + lock_lease,
                        )
                        count += 1
        return count
