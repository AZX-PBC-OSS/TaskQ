"""Unit tests for InMemoryBackend.cancel_where (bulk cancel)."""

from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

from taskq.backend._protocol import CancelPhase, JobFilter
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args
from taskq.types import BulkCancelResult

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


async def test_cancel_where_pending_jobs() -> None:
    """cancel_where moves pending jobs straight to 'cancelled'."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    for _ in range(3):
        await backend.enqueue(make_enqueue_args(tags=("tenant-acme", "run-001"), scheduled_at=_NOW))
    for _ in range(2):
        await backend.enqueue(make_enqueue_args(tags=("tenant-other",), scheduled_at=_NOW))

    result = await backend.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="offboard",
    )

    assert result.cancelled_directly == 3
    assert result.cancel_requested == 0
    assert result.total_affected == 3
    assert len(result.cancelled_ids) == 3

    remaining = await backend.list_jobs(JobFilter(tags=("tenant-other",)))
    assert len(remaining) == 2
    assert all(r.status == "pending" for r in remaining)


async def test_cancel_where_running_jobs_cooperative() -> None:
    """cancel_where sets cancel_phase=1 for running jobs (cooperative)."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
    row = await backend.enqueue(args)
    backend._jobs[row.id] = replace(
        backend._jobs[row.id], status="running", locked_by_worker=uuid4()
    )

    result = await backend.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="offboard",
    )

    assert result.cancelled_directly == 0
    assert result.cancel_requested == 1
    assert len(result.cancel_requested_ids) == 1

    updated = await backend.get(row.id)
    assert updated is not None
    assert updated.status == "running"
    assert updated.cancel_phase == CancelPhase.COOPERATIVE


async def test_cancel_where_mixed_statuses() -> None:
    """cancel_where handles both pending and running in one call."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    for _ in range(2):
        await backend.enqueue(make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW))

    args3 = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
    row3 = await backend.enqueue(args3)
    backend._jobs[row3.id] = replace(
        backend._jobs[row3.id], status="running", locked_by_worker=uuid4()
    )

    result = await backend.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="offboard",
    )

    assert result.cancelled_directly == 2
    assert result.cancel_requested == 1
    assert result.total_affected == 3


async def test_cancel_where_no_matches_returns_zero() -> None:
    """cancel_where with a filter matching nothing returns zero counts."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    await backend.enqueue(make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW))

    result = await backend.cancel_where(
        JobFilter(tags=("nonexistent",)),
        reason="offboard",
    )

    assert result.cancelled_directly == 0
    assert result.cancel_requested == 0
    assert result.total_affected == 0


async def test_cancel_where_already_cancelled_not_affected() -> None:
    """Already-terminal jobs are not re-cancelled."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
    row = await backend.enqueue(args)
    backend._jobs[row.id] = replace(backend._jobs[row.id], status="cancelled", finished_at=_NOW)

    result = await backend.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="offboard",
    )

    assert result.total_affected == 0


async def test_cancel_where_filter_by_batch_id() -> None:
    """cancel_where works with batch_id filter."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    bid = uuid4()
    for _ in range(3):
        args = make_enqueue_args(
            tags=("tenant-acme",),
            scheduled_at=_NOW,
            metadata={"batch_id": str(bid)},
        )
        await backend.enqueue(args)
    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW, metadata={"batch_id": str(uuid4())}))

    result = await backend.cancel_where(
        JobFilter(batch_id=bid),
        reason="batch abort",
    )

    assert result.cancelled_directly == 3


async def test_cancel_where_filter_by_queue_and_actor() -> None:
    """cancel_where works with queue and actor filters."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    await backend.enqueue(make_enqueue_args(queue="default", actor="worker-a", scheduled_at=_NOW))
    await backend.enqueue(make_enqueue_args(queue="default", actor="worker-b", scheduled_at=_NOW))
    await backend.enqueue(make_enqueue_args(queue="priority", actor="worker-a", scheduled_at=_NOW))

    result = await backend.cancel_where(
        JobFilter(queue="default", actor="worker-a"),
        reason="abort",
    )

    assert result.cancelled_directly == 1


async def test_cancel_where_active_filter() -> None:
    """cancel_where with active=True targets only non-terminal jobs."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW))
    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW))
    args3 = make_enqueue_args(scheduled_at=_NOW)
    row3 = await backend.enqueue(args3)
    backend._jobs[row3.id] = replace(backend._jobs[row3.id], status="succeeded", finished_at=_NOW)

    result = await backend.cancel_where(
        JobFilter(active=True),
        reason="drain",
    )

    assert result.cancelled_directly == 2


async def test_cancel_where_ignores_filter_limit() -> None:
    """cancel_where cancels ALL matching jobs even when filter.limit is small.

    Guards against the H3 bug: _list_jobs applies filters.limit (default
    100). If _cancel_where reuses _list_jobs without sanitizing the filter,
    a caller passing JobFilter(limit=5, tags=...) would cancel only 5 jobs.
    """
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    for _ in range(11):
        await backend.enqueue(make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW))

    result = await backend.cancel_where(
        JobFilter(tags=("tenant-acme",), limit=5),
        reason="offboard",
    )

    assert result.cancelled_directly == 11
    assert result.total_affected == 11


async def test_cancel_where_already_cooperative_cancel_not_recounted() -> None:
    """A running job already in cooperative cancel (cancel_phase=1) is
    NOT double-counted by a subsequent cancel_where call."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
    row = await backend.enqueue(args)
    worker_id = uuid4()
    backend._jobs[row.id] = replace(
        backend._jobs[row.id],
        status="running",
        locked_by_worker=worker_id,
        cancel_phase=CancelPhase.COOPERATIVE,
        cancel_requested_at=_NOW,
    )

    result = await backend.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="offboard",
    )

    assert result.cancel_requested == 0
    assert result.total_affected == 0


async def test_cancel_where_events_inserted() -> None:
    """cancel_where inserts state_change + cancel_request events for
    pending jobs, matching single-job write_cancel_request semantics."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
    row = await backend.enqueue(args)

    await backend.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="test",
    )

    events = await backend.get_events(row.id)
    kinds = [e.kind for e in events]
    assert "state_change" in kinds
    assert "cancel_request" in kinds
    sc = [e for e in events if e.kind == "state_change"]
    assert sc[0].detail["from_state"] in ("pending", "scheduled")
    assert sc[0].detail["to_state"] == "cancelled"


async def test_cancel_where_running_event_only_cancel_request() -> None:
    """Running jobs get only cancel_request (no state_change)."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
    row = await backend.enqueue(args)
    backend._jobs[row.id] = replace(
        backend._jobs[row.id], status="running", locked_by_worker=uuid4()
    )

    await backend.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="test",
    )

    events = await backend.get_events(row.id)
    kinds = [e.kind for e in events]
    assert "cancel_request" in kinds
    assert "state_change" not in kinds


async def test_cancel_where_wakes_cancel_subscribers() -> None:
    """cancel_where sets cancel wake events for running jobs."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    args = make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW)
    row = await backend.enqueue(args)
    backend._jobs[row.id] = replace(
        backend._jobs[row.id], status="running", locked_by_worker=uuid4()
    )

    async with backend.subscribe_cancel_wake() as event:
        assert not event.is_set()
        await backend.cancel_where(
            JobFilter(tags=("tenant-acme",)),
            reason="test",
        )
        assert event.is_set()


async def test_cancel_where_returns_bulk_cancel_result_type() -> None:
    """cancel_where returns a BulkCancelResult instance."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    await backend.enqueue(make_enqueue_args(tags=("x",), scheduled_at=_NOW))

    result = await backend.cancel_where(JobFilter(tags=("x",)), reason=None)

    assert isinstance(result, BulkCancelResult)
