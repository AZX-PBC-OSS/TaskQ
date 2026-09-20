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
from taskq.exceptions import IdempotencyKeyActorMismatchError, SingletonCollisionError
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


async def _create_test_batch(
    backend: InMemoryBackend,
    *,
    batch_id: UUID | None = None,
    queue: str = "default",
    expected_size: int = 3,
    failure_threshold: int | None = None,
    finalizer_job_id: UUID | None = None,
    originating_actor: str | None = None,
) -> UUID:
    """Create a batch with sensible defaults, returning the batch ID."""
    bid = batch_id or new_uuid()
    await backend.create_batch(
        bid,
        queue=queue,
        expected_size=expected_size,
        failure_threshold=failure_threshold,
        finalizer_job_id=finalizer_job_id,
        originating_actor=originating_actor,
    )
    return bid


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

        await _create_test_batch(
            backend,
            batch_id=bid,
            expected_size=5,
            failure_threshold=3,
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

        await _create_test_batch(backend, batch_id=bid, expected_size=5, failure_threshold=3)

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

        await _create_test_batch(backend, batch_id=bid, expected_size=5, failure_threshold=3)
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

        await _create_test_batch(backend, batch_id=bid, expected_size=5, failure_threshold=3)

        c1, _, _ = await backend.increment_batch_failures(bid)
        c2, _, _ = await backend.increment_batch_failures(bid)

        assert c1 == 1
        assert c2 == 2
        stored = backend._batches[bid]
        assert stored.consecutive_failures == 2

    async def test_increment_with_no_non_terminal_jobs(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await _create_test_batch(backend, batch_id=bid, expected_size=1, failure_threshold=3)
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

        await _create_test_batch(backend, batch_id=bid, expected_size=5, failure_threshold=3)
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

        await _create_test_batch(backend, batch_id=bid, expected_size=3)
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

        await _create_test_batch(backend, batch_id=bid, expected_size=3)
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

        await _create_test_batch(backend, batch_id=bid, expected_size=3)

        await backend.complete_batch(bid)

        stored = backend._batches[bid]
        assert stored.status == "complete"
        assert stored.completed_at is not None

    async def test_complete_noop_if_already_terminal(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await _create_test_batch(backend, batch_id=bid, expected_size=3)
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

        await _create_test_batch(backend, batch_id=bid1)
        await _create_test_batch(backend, batch_id=bid2)
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

        await _create_test_batch(backend, batch_id=bid1)
        await _create_test_batch(backend, batch_id=bid2, queue="high_priority")

        results = await backend.list_batches(BatchFilter(queue="high_priority"))

        assert len(results) == 1
        assert results[0][0].id == bid2

    async def test_filter_by_batch_id(self) -> None:
        backend = _make_backend()
        bid1 = new_uuid()
        bid2 = new_uuid()

        await _create_test_batch(backend, batch_id=bid1)
        await _create_test_batch(backend, batch_id=bid2)

        results = await backend.list_batches(BatchFilter(batch_id=bid1))

        assert len(results) == 1
        assert results[0][0].id == bid1

    async def test_list_returns_counts(self) -> None:
        backend = _make_backend()
        bid = new_uuid()

        await _create_test_batch(backend, batch_id=bid, expected_size=5)
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

        await _create_test_batch(backend, batch_id=bid1)
        clock.advance(timedelta(seconds=10))
        await _create_test_batch(backend, batch_id=bid2)

        results = await backend.list_batches(BatchFilter())

        assert len(results) == 2
        assert results[0][0].id == bid2
        assert results[1][0].id == bid1

    async def test_list_limit(self) -> None:
        clock = FakeClock(_START)
        backend = _make_backend(clock)

        for _ in range(5):
            await _create_test_batch(backend)
            clock.advance(timedelta(seconds=1))

        results = await backend.list_batches(BatchFilter(limit=2))
        assert len(results) == 2

    async def test_list_terminal_batches(self) -> None:
        backend = _make_backend()
        bid1 = new_uuid()
        bid2 = new_uuid()

        await _create_test_batch(backend, batch_id=bid1)
        await _create_test_batch(backend, batch_id=bid2)
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

        await _create_test_batch(backend, batch_id=bid_ia, queue="ingest")
        await _create_test_batch(backend, batch_id=bid_ic, queue="ingest")
        await _create_test_batch(backend, batch_id=bid_ea, queue="export")
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

        await _create_test_batch(backend, batch_id=bid1, queue="ingest")
        await _create_test_batch(backend, batch_id=bid2, queue="ingest")
        await _create_test_batch(backend, queue="export")

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

    async def test_enqueue_batch_atomic_cross_actor_in_batch_duplicate_rolls_back(self) -> None:
        """An in-batch cross-actor idempotency duplicate refuses the atomic
        arm too: PG's arm runs every chunk through ONE transaction, so the
        refusal withdraws the member chunks already inserted, and the
        mirror's compensating rollback must leave the identical empty
        stored-row state - no member rows, no batch row."""
        backend = _make_backend()
        bid = new_uuid()
        batch_row = _make_batch_row(id=bid, queue="default", expected_size=2)

        item_a = make_enqueue_args(actor="actor-a", queue="default", idempotency_key="k1")
        item_b = make_enqueue_args(actor="actor-b", queue="default", idempotency_key="k1")

        with pytest.raises(IdempotencyKeyActorMismatchError):
            await backend.enqueue_batch_atomic(
                [item_a, item_b],
                batch_id=bid,
                queue="default",
                batch_row=batch_row,
                finalizer_args=None,
            )

        assert await backend.get(item_a.id) is None, "the refused batch must leave no rows behind"
        assert await backend.get(item_b.id) is None
        assert bid not in backend._batches


class TestEnqueueBatchAtomicRollbackSparesDedupHits:
    """The compensating rollback must withdraw only rows THIS call
    inserted, never rows a dedup hit returned.

    Postgres' atomic arm runs every chunk in ONE transaction: a failure
    partway through rolls the TRANSACTION back, so a row that already
    existed (a dedup hit returning the holder row) is untouched and stays
    stored. A compensating rollback that pops every returned row id
    withdraws the pre-existing holder too, certifying code that loses
    live jobs on a failed batch.
    """

    async def _failed_batch_after_dedup_hit(self, backend: InMemoryBackend) -> JobRow:
        """Seed a stored keyed row, fail an atomic batch whose FIRST item
        dedups onto it, and return the holder row."""
        holder = await backend.enqueue(make_enqueue_args(idempotency_key="k1", scheduled_at=_START))
        bid = new_uuid()
        hit = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)

        def gen() -> Iterable[EnqueueArgs]:
            yield hit
            raise ValueError("generator exploded")

        # chunk_size=1 lands the failure AFTER the hit: chunk one resolves
        # the hit (returning the holder row), chunk two raises mid-stream.
        with pytest.raises(ValueError, match="generator exploded"):
            await backend.enqueue_batch_atomic(
                gen(),
                batch_id=bid,
                queue="default",
                batch_row=None,
                finalizer_args=None,
                chunk_size=1,
            )
        return holder

    async def test_rollback_leaves_dedup_hit_holder_row_stored(self) -> None:
        """A pre-existing row a dedup hit returned must survive the failed
        batch: PG's transaction rollback leaves it intact."""
        backend = _make_backend()
        holder = await self._failed_batch_after_dedup_hit(backend)

        assert await backend.get(holder.id) is not None, (
            "the compensating rollback must not withdraw a row this call never inserted"
        )

    async def test_dedup_hit_id_resolves_after_failed_batch(self) -> None:
        """After the failed batch the holder row is still resolvable: a
        fresh enqueue with the same pair dedups onto it, not a new row."""
        backend = _make_backend()
        holder = await self._failed_batch_after_dedup_hit(backend)

        again = await backend.enqueue(make_enqueue_args(idempotency_key="k1", scheduled_at=_START))

        assert again.id == holder.id

    async def test_successful_batch_after_failed_batch_sees_preexisting_row(self) -> None:
        """A successful batch after the failed one still sees the
        pre-existing row: the keyed item dedups onto the holder and only
        the clean item is stored."""
        backend = _make_backend()
        holder = await self._failed_batch_after_dedup_hit(backend)

        bid = new_uuid()
        keyed = make_enqueue_args(idempotency_key="k1", scheduled_at=_START)
        clean = make_enqueue_args(scheduled_at=_START)
        rows = await backend.enqueue_batch_atomic(
            [keyed, clean],
            batch_id=bid,
            queue="default",
            batch_row=None,
            finalizer_args=None,
        )

        assert [r.id for r in rows] == [holder.id, clean.id]
        assert await backend.get(holder.id) is not None

    async def test_in_batch_dedup_holder_still_withdrawn_on_rollback(self) -> None:
        """The carve-out is only for rows this call NEVER inserted: an
        in-batch dedup holder (the first item's row) rides the rollback
        with the rest of the transaction's inserts, exactly PG's."""
        backend = _make_backend()
        bid = new_uuid()
        first = make_enqueue_args(actor="actor-a", idempotency_key="k1", scheduled_at=_START)
        repeat = make_enqueue_args(actor="actor-a", idempotency_key="k1", scheduled_at=_START)

        def gen() -> Iterable[EnqueueArgs]:
            yield first
            yield repeat
            raise ValueError("generator exploded")

        with pytest.raises(ValueError, match="generator exploded"):
            await backend.enqueue_batch_atomic(
                gen(),
                batch_id=bid,
                queue="default",
                batch_row=None,
                finalizer_args=None,
                chunk_size=1,
            )

        assert await backend.get(first.id) is None, (
            "an in-batch holder was inserted by this call, the rollback withdraws it"
        )
        assert bid not in backend._batches


# ── TestInMemoryPruneOldBatches ─────────────────────────────────────────


class TestInMemoryPruneOldBatches:
    async def test_completed_batch_pruned_after_cutoff(self) -> None:
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        bid = new_uuid()

        await _create_test_batch(backend, batch_id=bid)
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

        await _create_test_batch(backend, batch_id=bid)

        cutoff = _START + timedelta(hours=100)
        pruned = await backend.prune_old_batches(cutoff)

        assert pruned == 0
        assert bid in backend._batches

    async def test_completed_batch_not_pruned_before_cutoff(self) -> None:
        clock = FakeClock(_START)
        backend = _make_backend(clock)
        bid = new_uuid()

        await _create_test_batch(backend, batch_id=bid)
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

        await _create_test_batch(backend, batch_id=bid)
        _make_batch_job(backend, batch_id=bid, status="succeeded")
        clock.advance(timedelta(hours=1))
        await backend.complete_batch(bid)

        cutoff = _START + timedelta(hours=2)
        pruned = await backend.prune_old_batches(cutoff)

        assert pruned == 0
        assert bid in backend._batches


# ── TestInMemoryEnqueueBatchSingletonAtomicity ──────────────────────────


def _singleton_args(actor: str) -> EnqueueArgs:
    return make_enqueue_args(actor=actor, queue="default", metadata={"singleton": True})


class TestInMemoryEnqueueBatchSingletonAtomicity:
    """``enqueue_batch`` is whole-call atomic on a singleton collision.

    The Postgres bulk tier runs one ``unnest`` INSERT inside one
    transaction, so a constraint violation anywhere in the batch aborts
    the entire statement and commits nothing. The in-memory mirror must
    admit nothing on the same collision. A mirror that stores the items
    preceding the colliding one certifies application code that on
    Postgres leaves no such rows behind, so behavior validated against
    the mirror diverges the moment it reaches production.
    """

    async def test_two_singleton_items_for_same_actor_admit_nothing(self) -> None:
        """Two same-actor singleton items in one batch collide with each
        other. Neither may be stored, including the first one, which the
        per-item loop reaches before the second item's preflight raises.
        """
        backend = _make_backend()
        args_list = [_singleton_args("atomicity-actor"), _singleton_args("atomicity-actor")]

        with pytest.raises(SingletonCollisionError):
            await backend.enqueue_batch(args_list)

        stored = [row for row in backend._jobs.values() if row.actor == "atomicity-actor"]
        assert stored == [], (
            "batch admitted a partial prefix "
            f"({len(stored)} row(s) stored) after a mid-batch SingletonCollisionError; "
            "the single-statement batch INSERT aborts with nothing written"
        )

    async def test_collision_against_live_singleton_admits_no_other_item(self) -> None:
        """The collision can equally be against a job committed by an
        earlier call. A batch carrying one singleton item for an actor
        that already holds a live singleton job must also admit none of
        its other, unrelated items.
        """
        backend = _make_backend()
        await backend.enqueue(_singleton_args("held-actor"))

        bystander_args = make_enqueue_args(actor="bystander-actor", queue="default")
        args_list = [bystander_args, _singleton_args("held-actor")]

        with pytest.raises(SingletonCollisionError):
            await backend.enqueue_batch(args_list)

        bystander_rows = [row for row in backend._jobs.values() if row.actor == "bystander-actor"]
        assert bystander_rows == [], (
            "batch admitted an unrelated actor's item before the colliding item's "
            "SingletonCollisionError aborted the call; the single-statement "
            "batch INSERT would have rolled back this row too"
        )

    async def test_aborted_batch_leaves_no_idempotency_index_entry(self) -> None:
        """The rollback must also unwind the idempotency index, not just
        the job rows. An entry surviving an aborted batch permanently
        dedups every later enqueue carrying that key against a job that
        was never committed, so the work silently never runs.
        """
        backend = _make_backend()
        await backend.enqueue(_singleton_args("held-actor-2"))

        keyed_args = make_enqueue_args(
            actor="keyed-actor",
            queue="default",
            idempotency_key="batch-key",
        )
        args_list = [keyed_args, _singleton_args("held-actor-2")]

        with pytest.raises(SingletonCollisionError):
            await backend.enqueue_batch(args_list)

        assert backend._idempotency_index.get(("", "batch-key")) is None, (
            "aborted batch left an idempotency index entry pointing at a row "
            "the call never committed; a later enqueue with the same key would "
            "dedup against a job that does not exist"
        )

    async def test_truthy_non_true_singleton_metadata_does_not_block(self) -> None:
        """A stored row whose metadata carries a truthy-but-not-``True``
        ``singleton`` value (``1``) is not a live singleton blocker.

        The singleton stamp is a JSON boolean: Postgres' partial unique
        index matches ``metadata @> '{"singleton": true}'`` exactly, and
        the single-enqueue preflights on both backends test ``is True``.
        A batch preflight that matched on truthiness alone would refuse a
        batch Postgres admits.
        """
        backend = _make_backend()
        await backend.enqueue(
            make_enqueue_args(actor="lenient-actor", queue="default", metadata={"singleton": 1})
        )

        rows = await backend.enqueue_batch([_singleton_args("lenient-actor")])

        assert len(rows) == 1, (
            "a stored metadata singleton value of 1 (truthy, but not the JSON "
            "boolean true) blocked a batch singleton item - Postgres' partial "
            "index matches only jsonb true, so the mirror must not refuse here"
        )
