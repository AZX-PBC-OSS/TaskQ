"""Integration tests for PostgresBackend batch operations against real PG.

Covers create_batch, get_batch, increment_batch_failures,
reset_batch_failures, abort_batch, complete_batch,
count_batch_non_terminal, list_batches, and prune_old_batches.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import asyncpg
import pytest

from taskq.backend._protocol import BatchFilter
from taskq.testing.fixtures import JobsApp
from taskq.testing.jobs import make_enqueue_args

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm]  # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive import

pytestmark = pytest.mark.integration


async def _insert_test_job(
    conn: _Conn,
    schema: str,
    batch_id: UUID,
    *,
    status: str = "pending",
) -> UUID:
    """Insert a job row directly via asyncpg with batch_id metadata."""
    job_id = uuid4()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: test helper — schema is a validated constant from settings, not user input
        "(id, queue, actor, payload, max_attempts, retry_kind, metadata, status) "
        "VALUES ($1, 'default', 'test_actor', '{}'::jsonb, "
        "1, 'non_retryable', $2::jsonb, $3)",
        job_id,
        json.dumps({"batch_id": str(batch_id)}),
        status,
    )
    return job_id


# ── create_batch + get_batch ─────────────────────────────────────


class TestPostgresCreateBatch:
    async def test_create_and_get_batch(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        bid = uuid4()
        fin_id = uuid4()

        await backend.create_batch(
            bid,
            "default",
            expected_size=10,
            failure_threshold=5,
            finalizer_job_id=fin_id,
            originating_actor="test_actor",
        )

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.id == bid
        assert row.queue == "default"
        assert row.status == "active"
        assert row.expected_size == 10
        assert row.consecutive_failures == 0
        assert row.failure_threshold == 5
        assert row.finalizer_job_id == fin_id
        assert row.originating_actor == "test_actor"
        assert row.created_at is not None
        assert row.completed_at is None
        assert row.metadata == {}

    async def test_create_batch_with_nulls(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        bid = uuid4()

        await backend.create_batch(
            bid,
            "default",
            expected_size=0,
            failure_threshold=None,
            finalizer_job_id=None,
            originating_actor=None,
        )

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.failure_threshold is None
        assert row.finalizer_job_id is None
        assert row.originating_actor is None

    async def test_get_batch_nonexistent(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        row = await backend.get_batch(uuid4())
        assert row is None

    async def test_create_batch_with_connection(self, jobs_app: JobsApp) -> None:
        deps = jobs_app.deps
        backend = jobs_app.backend
        bid = uuid4()

        async with deps.worker_pool.acquire() as conn:
            await backend.create_batch(
                bid,
                "default",
                expected_size=3,
                failure_threshold=2,
                finalizer_job_id=None,
                originating_actor=None,
                connection=conn,
            )

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.id == bid


# ── increment_batch_failures ─────────────────────────────────────


class TestPostgresIncrementBatchFailures:
    async def test_increment_returns_count_threshold_remaining(self, jobs_app: JobsApp) -> None:
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        bid = uuid4()

        await backend.create_batch(bid, "default", 5, 3, None, None)

        async with deps.worker_pool.acquire() as conn:
            await _insert_test_job(conn, schema, bid, status="pending")
            await _insert_test_job(conn, schema, bid, status="pending")
            await _insert_test_job(conn, schema, bid, status="succeeded")

        count, threshold, remaining = await backend.increment_batch_failures(bid)
        assert count == 1
        assert threshold == 3
        assert remaining == 2

        count, threshold, remaining = await backend.increment_batch_failures(bid)
        assert count == 2
        assert threshold == 3
        assert remaining == 2

    async def test_increment_no_batch_returns_zeros(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        count, threshold, remaining = await backend.increment_batch_failures(uuid4())
        assert count == 0
        assert threshold is None
        assert remaining == 0

    async def test_increment_with_null_threshold(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        bid = uuid4()

        await backend.create_batch(bid, "default", 5, None, None, None)

        count, threshold, _remaining = await backend.increment_batch_failures(bid)
        assert count == 1
        assert threshold is None


# ── abort_batch ──────────────────────────────────────────────────


class TestPostgresAbortBatch:
    async def test_abort_cancels_pending_jobs(self, jobs_app: JobsApp) -> None:
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        bid = uuid4()

        await backend.create_batch(bid, "default", 5, 3, None, None)

        async with deps.worker_pool.acquire() as conn:
            j1 = await _insert_test_job(conn, schema, bid, status="pending")
            j2 = await _insert_test_job(conn, schema, bid, status="scheduled")
            await _insert_test_job(conn, schema, bid, status="running")
            await _insert_test_job(conn, schema, bid, status="succeeded")

        cancelled_count = await backend.abort_batch(bid)
        assert cancelled_count == 2

        async with deps.worker_pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT id, status, error_class, error_message, "  # noqa: S608  # Why: test helper — schema is a validated constant from settings, not user input
                f"cancel_requested_at, cancel_phase "
                f'FROM "{schema}".jobs '
                f"WHERE id = ANY($1::uuid[])",
                [j1, j2],
            )
            for r in rows:
                assert r["status"] == "cancelled"
                assert r["error_class"] == "BatchAbortedError"
                assert r["error_message"] == "Batch aborted due to consecutive failures"
                assert r["cancel_requested_at"] is not None
                assert r["cancel_phase"] == 2

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.status == "aborted"
        assert row.completed_at is not None

    async def test_abort_no_batch_returns_zero(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        count = await backend.abort_batch(uuid4())
        assert count == 0


# ── reset_batch_failures ─────────────────────────────────────────


class TestPostgresResetBatchFailures:
    async def test_reset_returns_remaining(self, jobs_app: JobsApp) -> None:
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        bid = uuid4()

        await backend.create_batch(bid, "default", 5, 3, None, None)

        async with deps.worker_pool.acquire() as conn:
            await _insert_test_job(conn, schema, bid, status="pending")
            await _insert_test_job(conn, schema, bid, status="pending")
            await _insert_test_job(conn, schema, bid, status="succeeded")

        await backend.increment_batch_failures(bid)
        await backend.increment_batch_failures(bid)

        remaining = await backend.reset_batch_failures(bid)
        assert remaining == 2

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.consecutive_failures == 0

    async def test_reset_no_batch_returns_zero(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        remaining = await backend.reset_batch_failures(uuid4())
        assert remaining == 0


# ── complete_batch ───────────────────────────────────────────────


class TestPostgresCompleteBatch:
    async def test_complete_sets_status_and_completed_at(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        bid = uuid4()

        await backend.create_batch(bid, "default", 5, 3, None, None)

        await backend.complete_batch(bid)

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.status == "complete"
        assert row.completed_at is not None

    async def test_complete_noop_if_already_terminal(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        bid = uuid4()

        await backend.create_batch(bid, "default", 5, 3, None, None)
        await backend.abort_batch(bid)

        row_before = await backend.get_batch(bid)
        assert row_before is not None
        assert row_before.status == "aborted"
        completed_before = row_before.completed_at

        await backend.complete_batch(bid)

        row_after = await backend.get_batch(bid)
        assert row_after is not None
        assert row_after.status == "aborted"
        assert row_after.completed_at == completed_before


# ── count_batch_non_terminal ─────────────────────────────────────


class TestPostgresCountBatchNonTerminal:
    async def test_counts_non_terminal_jobs(self, jobs_app: JobsApp) -> None:
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        bid = uuid4()

        await backend.create_batch(bid, "default", 10, 5, None, None)

        async with deps.worker_pool.acquire() as conn:
            await _insert_test_job(conn, schema, bid, status="pending")
            await _insert_test_job(conn, schema, bid, status="scheduled")
            await _insert_test_job(conn, schema, bid, status="running")
            await _insert_test_job(conn, schema, bid, status="succeeded")
            await _insert_test_job(conn, schema, bid, status="failed")
            await _insert_test_job(conn, schema, bid, status="cancelled")

        count = await backend.count_batch_non_terminal(bid)
        assert count == 3

    async def test_count_zero_when_no_jobs(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        bid = uuid4()

        await backend.create_batch(bid, "default", 10, 5, None, None)

        count = await backend.count_batch_non_terminal(bid)
        assert count == 0


# ── list_batches ─────────────────────────────────────────────────


class TestPostgresListBatches:
    async def test_list_active_batches(self, jobs_app: JobsApp) -> None:
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        bid1 = uuid4()
        bid2 = uuid4()

        await backend.create_batch(bid1, "default", 10, 5, None, None)
        await backend.create_batch(bid2, "other_queue", 5, 3, None, None)

        async with deps.worker_pool.acquire() as conn:
            await _insert_test_job(conn, schema, bid1, status="pending")
            await _insert_test_job(conn, schema, bid1, status="succeeded")
            await _insert_test_job(conn, schema, bid2, status="pending")

        results = await backend.list_batches(BatchFilter(active=True))
        assert len(results) == 2
        ids = {r[0].id for r in results}
        assert bid1 in ids
        assert bid2 in ids

        _row1, counts1 = next(r for r in results if r[0].id == bid1)
        assert counts1.total == 2
        assert counts1.pending == 1
        assert counts1.succeeded == 1

    async def test_list_filter_by_queue(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        bid1 = uuid4()
        bid2 = uuid4()

        await backend.create_batch(bid1, "default", 10, 5, None, None)
        await backend.create_batch(bid2, "other_queue", 5, 3, None, None)

        results = await backend.list_batches(BatchFilter(queue="other_queue"))
        assert len(results) == 1
        assert results[0][0].id == bid2
        assert results[0][0].queue == "other_queue"

    async def test_list_filter_active_false(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        bid1 = uuid4()
        bid2 = uuid4()

        await backend.create_batch(bid1, "default", 10, 5, None, None)
        await backend.create_batch(bid2, "default", 5, 3, None, None)
        await backend.complete_batch(bid1)
        await backend.abort_batch(bid2)

        results = await backend.list_batches(BatchFilter(active=False))
        assert len(results) == 2
        statuses = {r[0].status for r in results}
        assert statuses == {"complete", "aborted"}

    async def test_list_filter_by_batch_id(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        bid1 = uuid4()
        bid2 = uuid4()

        await backend.create_batch(bid1, "default", 10, 5, None, None)
        await backend.create_batch(bid2, "default", 5, 3, None, None)

        results = await backend.list_batches(BatchFilter(batch_id=bid1))
        assert len(results) == 1
        assert results[0][0].id == bid1

    async def test_list_limit(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        for _ in range(5):
            await backend.create_batch(uuid4(), "default", 1, 1, None, None)

        results = await backend.list_batches(BatchFilter(limit=3))
        assert len(results) == 3


# ── prune_old_batches ────────────────────────────────────────────


class TestPostgresPruneOldBatches:
    async def test_prune_completed_batch_after_cutoff(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        bid = uuid4()

        await backend.create_batch(bid, "default", 5, 3, None, None)
        await backend.complete_batch(bid)

        cutoff = datetime.now(UTC) + timedelta(seconds=1)
        pruned = await backend.prune_old_batches(cutoff)
        assert pruned == 1

        row = await backend.get_batch(bid)
        assert row is None

    async def test_prune_does_not_remove_active_batch(self, jobs_app: JobsApp) -> None:
        backend = jobs_app.backend
        bid = uuid4()

        await backend.create_batch(bid, "default", 5, 3, None, None)

        cutoff = datetime.now(UTC) + timedelta(hours=1)
        pruned = await backend.prune_old_batches(cutoff)
        assert pruned == 0

        row = await backend.get_batch(bid)
        assert row is not None
        assert row.status == "active"

    async def test_prune_does_not_remove_batch_with_jobs(self, jobs_app: JobsApp) -> None:
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        bid = uuid4()

        await backend.create_batch(bid, "default", 5, 3, None, None)
        await backend.complete_batch(bid)

        async with deps.worker_pool.acquire() as conn:
            await _insert_test_job(conn, schema, bid, status="succeeded")

        cutoff = datetime.now(UTC) + timedelta(hours=1)
        pruned = await backend.prune_old_batches(cutoff)
        assert pruned == 0

        row = await backend.get_batch(bid)
        assert row is not None


# ── enqueue_batch_atomic rollback ────────────────────────────────


class TestPostgresEnqueueBatchAtomicRollback:
    async def test_enqueue_batch_atomic_rolls_back_on_generator_failure(
        self, jobs_app: JobsApp
    ) -> None:
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        bid = uuid4()

        def gen():
            yield make_enqueue_args(actor="a1", queue="default")
            yield make_enqueue_args(actor="a2", queue="default")
            raise ValueError("generator exploded")

        with pytest.raises(ValueError, match="generator exploded"):
            await backend.enqueue_batch_atomic(
                gen(),
                batch_id=bid,
                queue="default",
                batch_row=None,
                finalizer_args=None,
            )

        # No jobs with this batch_id should exist (transaction rolled back).
        async with deps.worker_pool.acquire() as conn:
            count = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '  # noqa: S608  # Why: test helper — schema is a validated constant from settings, not user input
                "WHERE metadata @> $1::jsonb",
                json.dumps({"batch_id": str(bid)}),
            )
        assert count == 0

        # No batch row should exist.
        row = await backend.get_batch(bid)
        assert row is None


# ── abort_batch no-row contract ──────────────────────────────────


class TestPostgresAbortBatchNoRow:
    async def test_abort_cancels_jobs_without_batch_row(self, jobs_app: JobsApp) -> None:
        deps = jobs_app.deps
        backend = jobs_app.backend
        schema = deps.settings.schema_name
        bid = uuid4()

        # Insert jobs with batch_id metadata but do NOT create a batches row.
        async with deps.worker_pool.acquire() as conn:
            j1 = await _insert_test_job(conn, schema, bid, status="pending")
            j2 = await _insert_test_job(conn, schema, bid, status="scheduled")
            await _insert_test_job(conn, schema, bid, status="succeeded")

        cancelled = await backend.abort_batch(bid)
        assert cancelled == 2

        # Verify jobs were cancelled.
        async with deps.worker_pool.acquire() as conn:
            rows = await conn.fetch(
                f'SELECT id, status FROM "{schema}".jobs '  # noqa: S608  # Why: test helper — schema is a validated constant from settings, not user input
                "WHERE id = ANY($1::uuid[])",
                [j1, j2],
            )
            for r in rows:
                assert r["status"] == "cancelled"

        # No batches row should have been created.
        row = await backend.get_batch(bid)
        assert row is None
