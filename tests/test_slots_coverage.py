"""Coverage for ``taskq.testing._slots._SlotTable``.

Exercises the branches and methods not covered by the existing
``test_in_memory_backend.py`` tests:

- ``ensure_slots`` re-invocation on an existing bucket (branch at line 43)
- ``acquire`` on a missing bucket (returns -1, line 57)
- ``acquire`` when all slots are occupied (returns -1, line 72)
- ``release`` missing bucket / wrong worker / success (lines 80-88)
- ``extend_leases_for_job`` with no matching slots and across buckets
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.testing._slots import _SlotState, _SlotTable

_NOW = datetime(2025, 1, 1, tzinfo=UTC)
_LEASE = timedelta(seconds=30)


class TestEnsureSlots:
    def test_creates_empty_slots(self) -> None:
        table = _SlotTable()
        table.ensure_slots("bucket", 3)
        # Three empty slots were created
        acquired = table.acquire("bucket", new_uuid(), new_uuid(), _LEASE, _NOW)
        assert acquired == 0

    def test_re_invoke_on_existing_bucket_skips_present_slots(self) -> None:
        """Re-calling ensure_slots on an existing bucket does not reset
        already-allocated slots (exercises the ``i in bucket`` branch)."""
        table = _SlotTable()
        table.ensure_slots("bucket", 2)

        job = new_uuid()
        worker = new_uuid()
        slot = table.acquire("bucket", job, worker, _LEASE, _NOW)
        assert slot == 0

        # Re-invoke ensure_slots with the same size — slot 0 must remain held.
        table.ensure_slots("bucket", 2)
        # Slot 0 is still occupied by ``job``; the next acquire takes slot 1.
        slot2 = table.acquire("bucket", new_uuid(), new_uuid(), _LEASE, _NOW)
        assert slot2 == 1

    def test_grow_existing_bucket(self) -> None:
        """Growing a bucket adds only the new empty slots."""
        table = _SlotTable()
        table.ensure_slots("bucket", 1)
        table.ensure_slots("bucket", 3)
        # Acquire three slots — all should be available.
        results = [table.acquire("bucket", new_uuid(), new_uuid(), _LEASE, _NOW) for _ in range(3)]
        assert results == [0, 1, 2]


class TestAcquireMissingBucket:
    def test_acquire_on_missing_bucket_returns_minus_one(self) -> None:
        table = _SlotTable()
        result = table.acquire("never_created", new_uuid(), new_uuid(), _LEASE, _NOW)
        assert result == -1


class TestAcquireAllFull:
    def test_acquire_when_all_slots_full_returns_minus_one(self) -> None:
        table = _SlotTable()
        table.ensure_slots("bucket", 1)
        # Fill the only slot
        first = table.acquire("bucket", new_uuid(), new_uuid(), _LEASE, _NOW)
        assert first == 0
        # No free slot available (none expired yet)
        second = table.acquire("bucket", new_uuid(), new_uuid(), _LEASE, _NOW)
        assert second == -1

    def test_acquire_reclaims_expired_slot(self) -> None:
        """A slot whose lease has expired is reclaimed by a new acquire."""
        table = _SlotTable()
        table.ensure_slots("bucket", 1)
        worker = new_uuid()
        table.acquire("bucket", new_uuid(), worker, _LEASE, _NOW)
        # Advance past the lease expiry
        later = _NOW + _LEASE + timedelta(seconds=1)
        reclaimed = table.acquire("bucket", new_uuid(), new_uuid(), _LEASE, later)
        assert reclaimed == 0


class TestRelease:
    def test_release_missing_bucket_returns_false(self) -> None:
        table = _SlotTable()
        assert table.release("never_created", 0, new_uuid()) is False

    def test_release_missing_slot_returns_false(self) -> None:
        table = _SlotTable()
        table.ensure_slots("bucket", 2)
        assert table.release("bucket", 99, new_uuid()) is False

    def test_release_wrong_worker_returns_false(self) -> None:
        table = _SlotTable()
        table.ensure_slots("bucket", 1)
        owner = new_uuid()
        slot = table.acquire("bucket", new_uuid(), owner, _LEASE, _NOW)
        assert slot == 0
        wrong = new_uuid()
        assert table.release("bucket", slot, wrong) is False

    def test_release_by_owner_succeeds_and_frees_slot(self) -> None:
        table = _SlotTable()
        table.ensure_slots("bucket", 1)
        owner = new_uuid()
        job = new_uuid()
        slot = table.acquire("bucket", job, owner, _LEASE, _NOW)
        assert slot == 0
        assert table.release("bucket", slot, owner) is True
        # Slot is now free for re-acquire by a different job
        slot2 = table.acquire("bucket", new_uuid(), new_uuid(), _LEASE, _NOW)
        assert slot2 == 0


class TestExtendLeasesForJob:
    def test_no_matching_slots_returns_zero(self) -> None:
        table = _SlotTable()
        table.ensure_slots("bucket", 2)
        count = table.extend_leases_for_job(new_uuid(), _NOW, _LEASE)
        assert count == 0

    def test_extends_matching_slot_lease(self) -> None:
        table = _SlotTable()
        table.ensure_slots("bucket", 2)
        job = new_uuid()
        worker = new_uuid()
        table.acquire("bucket", job, worker, _LEASE, _NOW)

        later = _NOW + timedelta(seconds=10)
        new_lease = timedelta(seconds=120)
        count = table.extend_leases_for_job(job, later, new_lease)
        assert count == 1

        # The new lease (later + new_lease = _NOW + 130 s) is not expired at
        # _NOW + 31 s even though the OLD lease (_NOW + 30 s) is.  Acquiring
        # at _NOW + 31 s must NOT reclaim slot 0 — it takes slot 1 instead.
        reclaimed = table.acquire(
            "bucket", new_uuid(), new_uuid(), _LEASE, _NOW + timedelta(seconds=31)
        )
        assert reclaimed == 1

    def test_extend_across_multiple_buckets(self) -> None:
        """A job holding slots in two buckets gets both extended."""
        table = _SlotTable()
        table.ensure_slots("bucket_a", 2)
        table.ensure_slots("bucket_b", 2)
        job = new_uuid()
        worker = new_uuid()
        table.acquire("bucket_a", job, worker, _LEASE, _NOW)
        table.acquire("bucket_b", job, worker, _LEASE, _NOW)

        count = table.extend_leases_for_job(job, _NOW + timedelta(seconds=1), timedelta(seconds=60))
        assert count == 2

    def test_extend_preserves_worker_and_acquired_at(self) -> None:
        """extend_leases_for_job keeps job_id/worker_id/acquired_at intact."""
        table = _SlotTable()
        table.ensure_slots("bucket", 1)
        job = new_uuid()
        worker = new_uuid()
        table.acquire("bucket", job, worker, _LEASE, _NOW)

        table.extend_leases_for_job(job, _NOW + timedelta(seconds=1), timedelta(seconds=60))
        # Releasing by the original worker must still work after extension.
        assert table.release("bucket", 0, worker) is True


class TestSlotStateDataclass:
    def test_default_slot_state_is_empty(self) -> None:
        state = _SlotState()
        assert state.job_id is None
        assert state.worker_id is None
        assert state.acquired_at is None
        assert state.lease_expires_at is None

    def test_slot_state_is_frozen(self) -> None:
        state = _SlotState(job_id=UUID(int=1))
        with pytest.raises(AttributeError):
            state.job_id = UUID(int=2)  # type: ignore[misc]
