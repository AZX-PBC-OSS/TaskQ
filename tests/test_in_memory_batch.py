"""Unit tests for InMemoryBackend batch protocol methods.

Covers all 10 batch operations defined in the Backend protocol:
create_batch, get_batch, increment_batch_failures, reset_batch_failures,
abort_batch, complete_batch, count_batch_non_terminal, list_batches,
enqueue_batch_atomic, and prune_old_batches.
"""

from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import (
    BatchFilter,
    BatchRow,
    EnqueueArgs,
    JobRow,
)
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args, make_job_row

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend(clock: FakeClock | None = None) -> InMemoryBackend:
    return InMemoryBackend(clock=clock or FakeClock(_START))


def _make_batch_row(
    *,
    id: UUID | None = None,
    queue: str = "default",
    status: str = "active",
    expected_size: int = 10,
    consecutive_failures: int = 0,
    failure_threshold: int | None = None,
    finalizer_job_id: UUID | None = None,
    originating_actor: str | None = None,
    created_at: datetime | None = None,
    completed_at: datetime | None = None,
    metadata: dict[str, object] | None = None,
) -> BatchRow:
    return BatchRow(
        id=id or new_uuid(),
        queue=queue,
        status=status,  # type: ignore[arg-type]  # Why: str is a valid Literal at runtime
        expected_size=expected_size,
        consecutive_failures=consecutive_failures,
        failure_threshold=failure_threshold,
        finalizer_job_id=finalizer_job_id,
        originating_actor=originating_actor,
        created_at=created_at or _START,
        completed_at=completed_at,
        metadata=metadata or {},
    )


def _make_batch_job(
    backend: InMemoryBackend,
    *,
    batch_id: UUID,
    status: str = "pending",
    queue: str = "default",
) -> JobRow:
    row = make_job_row(status=status, queue=queue)  # type: ignore[arg-type]  # Why: str status is valid JobStatus at runtime
    row = replace(row, metadata={**row.metadata, "batch_id": str(batch_id)})
    backend._jobs[row.id] = row
    return row


# ── TestInMemoryCreateBatch ─────────────────────────────────────────────


class TestInMemoryCreateBatch:
    async def test_create_batch_stores_row(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=5,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor="my_actor",
        )

        stored = backend._batches.get(bid)
        assert stored is not None
        assert stored.id == bid
        assert stored.queue == "default"
        assert stored.status == "active"
        assert stored.expected_size == 5
        assert stored.failure_threshold == 3
        assert stored.originating_actor == "my_actor"
        assert stored.consecutive_failures == 0
        assert stored.completed_at is None

    async def test_get_batch_returns_stored_row(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=5,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor=None,
        )

        result = await backend.get_batch(bid)
        assert result is not None
        assert result.id == bid
        assert result.queue == "default"

    async def test_get_batch_not_found_returns_none(self) -> None:
        backend = _make_backend()
        result = await backend.get_batch(new_uuid())
        assert result is None


# ── TestInMemoryIncrementBatchFailures ──────────────────────────────────


class TestInMemoryIncrementBatchFailures:
    async def test_increment_returns_count_threshold_remaining(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=5,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor=None,
        )
        _make_batch_job(backend, batch_id=bid, status="pending")
        _make_batch_job(backend, batch_id=bid, status="scheduled")

        count, threshold, remaining = await backend.increment_batch_failures(bid)

        assert count == 1
        assert threshold == 3
        assert remaining == 2

    async def test_increment_no_batch_row_returns_zeros(self) -> None:
        backend = _make_backend()
        count, threshold, remaining = await backend.increment_batch_failures(new_uuid())
        assert count == 0
        assert threshold is None
        assert remaining == 0

    async def test_increment_twice_counts_correctly(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=5,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor=None,
        )

        c1, _, _ = await backend.increment_batch_failures(bid)
        c2, _, _ = await backend.increment_batch_failures(bid)

        assert c1 == 1
        assert c2 == 2
        stored = backend._batches[bid]
        assert stored.consecutive_failures == 2

    async def test_increment_with_no_non_terminal_jobs(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=1,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor=None,
        )
        _make_batch_job(backend, batch_id=bid, status="succeeded")

        count, threshold, remaining = await backend.increment_batch_failures(bid)

        assert count == 1
        assert threshold == 3
        assert remaining == 0


# ── TestInMemoryResetBatchFailures ──────────────────────────────────────


class TestInMemoryResetBatchFailures:
    async def test_reset_returns_remaining_count(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=5,
            failure_threshold=3,
            finalizer_job_id=None,
            originating_actor=None,
        )
        _make_batch_job(backend, batch_id=bid, status="pending")
        _make_batch_job(backend, batch_id=bid, status="succeeded")

        await backend.increment_batch_failures(bid)
        remaining = await backend.reset_batch_failures(bid)

        assert remaining == 1
        stored = backend._batches[bid]
        assert stored.consecutive_failures == 0

    async def test_reset_no_batch_returns_zero(self) -> None:
        backend = _make_backend()
        remaining = await backend.reset_batch_failures(new_uuid())
        assert remaining == 0


# ── TestInMemoryAbortBatch ──────────────────────────────────────────────


class TestInMemoryAbortBatch:
    async def test_abort_cancels_pending_jobs(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        j1 = _make_batch_job(backend, batch_id=bid, status="pending")
        j2 = _make_batch_job(backend, batch_id=bid, status="scheduled")
        j3 = _make_batch_job(backend, batch_id=bid, status="running")

        cancelled = await backend.abort_batch(bid)

        assert cancelled == 2
        assert backend._jobs[j1.id].status == "cancelled"
        assert backend._jobs[j2.id].status == "cancelled"
        assert backend._jobs[j3.id].status == "running"
        assert backend._jobs[j1.id].error_class == "BatchAbortedError"
        assert backend._jobs[j1.id].finished_at is not None

    async def test_abort_with_no_batch_row_still_cancels_jobs(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        j1 = _make_batch_job(backend, batch_id=bid, status="pending")
        j2 = _make_batch_job(backend, batch_id=bid, status="scheduled")

        cancelled = await backend.abort_batch(bid)

        assert cancelled == 2
        assert backend._jobs[j1.id].status == "cancelled"
        assert backend._jobs[j2.id].status == "cancelled"
        assert bid not in backend._batches

    async def test_abort_sets_batch_status_to_aborted(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        _make_batch_job(backend, batch_id=bid, status="pending")

        await backend.abort_batch(bid)

        stored = backend._batches[bid]
        assert stored.status == "aborted"
        assert stored.completed_at is not None


# ── TestInMemoryCompleteBatch ───────────────────────────────────────────


class TestInMemoryCompleteBatch:
    async def test_complete_sets_status_and_completed_at(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )

        await backend.complete_batch(bid)

        stored = backend._batches[bid]
        assert stored.status == "complete"
        assert stored.completed_at is not None

    async def test_complete_noop_if_already_terminal(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.abort_batch(bid)
        aborted_row = backend._batches[bid]

        await backend.complete_batch(bid)

        stored = backend._batches[bid]
        assert stored.status == "aborted"
        assert stored.completed_at == aborted_row.completed_at

    async def test_complete_noop_if_no_row(self) -> None:
        backend = _make_backend()
        await backend.complete_batch(new_uuid())
        assert len(backend._batches) == 0


# ── TestInMemoryCountBatchNonTerminal ───────────────────────────────────


class TestInMemoryCountBatchNonTerminal:
    async def test_counts_non_terminal_jobs(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        _make_batch_job(backend, batch_id=bid, status="pending")
        _make_batch_job(backend, batch_id=bid, status="scheduled")
        _make_batch_job(backend, batch_id=bid, status="running")
        _make_batch_job(backend, batch_id=bid, status="succeeded")
        _make_batch_job(backend, batch_id=bid, status="cancelled")

        count = await backend.count_batch_non_terminal(bid)
        assert count == 3

    async def test_returns_zero_when_all_terminal(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        _make_batch_job(backend, batch_id=bid, status="succeeded")
        _make_batch_job(backend, batch_id=bid, status="failed")

        count = await backend.count_batch_non_terminal(bid)
        assert count == 0

    async def test_returns_zero_when_no_jobs(self) -> None:
        backend = _make_backend()
        count = await backend.count_batch_non_terminal(new_uuid())
        assert count == 0

    async def test_does_not_count_other_batch_jobs(self) -> None:
        backend = _make_backend()
        bid1 = new_uuid()
        bid2 = new_uuid()

        _make_batch_job(backend, batch_id=bid1, status="pending")
        _make_batch_job(backend, batch_id=bid2, status="pending")

        count = await backend.count_batch_non_terminal(bid1)
        assert count == 1


# ── TestInMemoryListBatches ─────────────────────────────────────────────


class TestInMemoryListBatches:
    async def test_list_active_batches(self) -> None:
        backend = _make_backend()
        bid1 = new_uuid()
        bid2 = new_uuid()

        await backend.create_batch(
            bid1,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.create_batch(
            bid2,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.complete_batch(bid2)

        results = await backend.list_batches(BatchFilter(active=True))

        assert len(results) == 1
        row, _counts = results[0]
        assert row.id == bid1
        assert row.status == "active"

    async def test_list_empty(self) -> None:
        backend = _make_backend()
        results = await backend.list_batches(BatchFilter())
        assert results == []

    async def test_filter_by_queue(self) -> None:
        backend = _make_backend()
        bid1 = new_uuid()
        bid2 = new_uuid()

        await backend.create_batch(
            bid1,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.create_batch(
            bid2,
            queue="high_priority",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )

        results = await backend.list_batches(BatchFilter(queue="high_priority"))

        assert len(results) == 1
        assert results[0][0].id == bid2

    async def test_filter_by_batch_id(self) -> None:
        backend = _make_backend()
        bid1 = new_uuid()
        bid2 = new_uuid()

        await backend.create_batch(
            bid1,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.create_batch(
            bid2,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )

        results = await backend.list_batches(BatchFilter(batch_id=bid1))

        assert len(results) == 1
        assert results[0][0].id == bid1

    async def test_list_returns_counts(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=5,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        _make_batch_job(backend, batch_id=bid, status="pending")
        _make_batch_job(backend, batch_id=bid, status="succeeded")
        _make_batch_job(backend, batch_id=bid, status="failed")

        results = await backend.list_batches(BatchFilter(batch_id=bid))

        assert len(results) == 1
        _, counts = results[0]
        assert counts.total == 3
        assert counts.pending == 1
        assert counts.succeeded == 1
        assert counts.failed == 1
        assert counts.cancelled == 0

    async def test_list_ordered_by_created_at_desc(self) -> None:
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        bid1 = new_uuid()
        bid2 = new_uuid()

        await backend.create_batch(
            bid1,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        clock.advance(timedelta(seconds=10))
        await backend.create_batch(
            bid2,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )

        results = await backend.list_batches(BatchFilter())

        assert len(results) == 2
        assert results[0][0].id == bid2
        assert results[1][0].id == bid1

    async def test_list_limit(self) -> None:
        clock = FakeClock(_START)
        backend = _make_backend(clock)

        for _ in range(5):
            bid = new_uuid()
            await backend.create_batch(
                bid,
                queue="default",
                expected_size=3,
                failure_threshold=None,
                finalizer_job_id=None,
                originating_actor=None,
            )
            clock.advance(timedelta(seconds=1))

        results = await backend.list_batches(BatchFilter(limit=2))
        assert len(results) == 2

    async def test_list_terminal_batches(self) -> None:
        backend = _make_backend()
        bid1 = new_uuid()
        bid2 = new_uuid()

        await backend.create_batch(
            bid1,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.create_batch(
            bid2,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.complete_batch(bid2)

        results = await backend.list_batches(BatchFilter(active=False))

        assert len(results) == 1
        assert results[0][0].id == bid2
        assert results[0][0].status == "complete"

    async def test_list_batches_queue_and_active_combined(self) -> None:
        backend = _make_backend()

        # ingest/active
        bid_ia = new_uuid()
        # ingest/complete
        bid_ic = new_uuid()
        # export/active
        bid_ea = new_uuid()

        await backend.create_batch(
            bid_ia,
            queue="ingest",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.create_batch(
            bid_ic,
            queue="ingest",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.create_batch(
            bid_ea,
            queue="export",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.complete_batch(bid_ic)

        results = await backend.list_batches(BatchFilter(queue="ingest", active=True))

        assert len(results) == 1
        assert results[0][0].id == bid_ia
        assert results[0][0].queue == "ingest"
        assert results[0][0].status == "active"

    async def test_list_batches_batch_id_and_queue_combined(self) -> None:
        backend = _make_backend()

        bid1 = new_uuid()
        bid2 = new_uuid()

        await backend.create_batch(
            bid1,
            queue="ingest",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.create_batch(
            bid2,
            queue="ingest",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        await backend.create_batch(
            new_uuid(),
            queue="export",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )

        results = await backend.list_batches(BatchFilter(queue="ingest", batch_id=bid2))

        assert len(results) == 1
        assert results[0][0].id == bid2
        assert results[0][0].queue == "ingest"


# ── TestInMemoryEnqueueBatchAtomic ──────────────────────────────────────


class TestInMemoryEnqueueBatchAtomic:
    async def test_inserts_all_items(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        items: list[EnqueueArgs] = [
            make_enqueue_args(actor="a1", queue="default"),
            make_enqueue_args(actor="a2", queue="default"),
            make_enqueue_args(actor="a3", queue="default"),
        ]

        rows = await backend.enqueue_batch_atomic(
            items,
            batch_id=bid,
            queue="default",
            batch_row=None,
            finalizer_args=None,
        )

        assert len(rows) == 3
        assert all(isinstance(r, JobRow) for r in rows)
        assert all(r.metadata.get("batch_id") is not None for r in rows)

    async def test_inserts_batch_row_when_provided(self) -> None:
        backend = _make_backend()
        bid = new_uuid()
        batch_row = _make_batch_row(id=bid, queue="default", expected_size=3)

        items: list[EnqueueArgs] = [
            make_enqueue_args(actor="a1", queue="default", metadata={"batch_id": str(bid)}),
            make_enqueue_args(actor="a2", queue="default", metadata={"batch_id": str(bid)}),
        ]

        await backend.enqueue_batch_atomic(
            items,
            batch_id=bid,
            queue="default",
            batch_row=batch_row,
            finalizer_args=None,
        )

        stored = backend._batches.get(bid)
        assert stored is not None
        assert stored.id == bid
        assert stored.expected_size == 3

    async def test_inserts_finalizer_last(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        items: list[EnqueueArgs] = [
            make_enqueue_args(actor="a1", queue="default"),
            make_enqueue_args(actor="a2", queue="default"),
        ]
        finalizer = make_enqueue_args(actor="finalizer_actor", queue="default")

        rows = await backend.enqueue_batch_atomic(
            items,
            batch_id=bid,
            queue="default",
            batch_row=None,
            finalizer_args=finalizer,
        )

        assert len(rows) == 3
        assert rows[-1].actor == "finalizer_actor"

    async def test_consumes_generator_fully(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        def gen() -> Iterable[EnqueueArgs]:
            yield make_enqueue_args(actor="a1", queue="default")
            yield make_enqueue_args(actor="a2", queue="default")
            yield make_enqueue_args(actor="a3", queue="default")

        rows = await backend.enqueue_batch_atomic(
            gen(),
            batch_id=bid,
            queue="default",
            batch_row=None,
            finalizer_args=None,
        )

        assert len(rows) == 3

    async def test_enqueue_batch_atomic_rolls_back_on_generator_failure(self) -> None:
        backend = _make_backend()
        bid = new_uuid()
        batch_row = _make_batch_row(id=bid, queue="default", expected_size=5)

        def gen() -> Iterable[EnqueueArgs]:
            yield make_enqueue_args(actor="a1", queue="default")
            yield make_enqueue_args(actor="a2", queue="default")
            yield make_enqueue_args(actor="a3", queue="default")
            raise ValueError("generator exploded")

        with pytest.raises(ValueError, match="generator exploded"):
            await backend.enqueue_batch_atomic(
                gen(),
                batch_id=bid,
                queue="default",
                batch_row=batch_row,
                finalizer_args=None,
            )

        # No jobs should remain after rollback.
        batch_jobs = [r for r in backend._jobs.values() if r.metadata.get("batch_id") == str(bid)]
        assert len(batch_jobs) == 0
        # No batch row should remain after rollback.
        assert bid not in backend._batches


# ── TestInMemoryPruneOldBatches ─────────────────────────────────────────


class TestInMemoryPruneOldBatches:
    async def test_completed_batch_pruned_after_cutoff(self) -> None:
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        clock.advance(timedelta(hours=1))
        await backend.complete_batch(bid)

        cutoff = _START + timedelta(hours=2)
        pruned = await backend.prune_old_batches(cutoff)

        assert pruned == 1
        assert bid not in backend._batches

    async def test_active_batch_never_pruned(self) -> None:
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )

        cutoff = _START + timedelta(hours=100)
        pruned = await backend.prune_old_batches(cutoff)

        assert pruned == 0
        assert bid in backend._batches

    async def test_completed_batch_not_pruned_before_cutoff(self) -> None:
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        clock.advance(timedelta(hours=1))
        await backend.complete_batch(bid)

        cutoff = _START + timedelta(minutes=30)
        pruned = await backend.prune_old_batches(cutoff)

        assert pruned == 0
        assert bid in backend._batches

    async def test_prune_skips_batches_with_live_jobs(self) -> None:
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        bid = new_uuid()

        await backend.create_batch(
            bid,
            queue="default",
            expected_size=3,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )
        _make_batch_job(backend, batch_id=bid, status="succeeded")
        clock.advance(timedelta(hours=1))
        await backend.complete_batch(bid)

        cutoff = _START + timedelta(hours=2)
        pruned = await backend.prune_old_batches(cutoff)

        assert pruned == 0
        assert bid in backend._batches
