"""Tests for batch prune and stale-batch completion sweep integration.

Unit tests cover ``InMemoryBackend.prune_old_batches`` (no PG required).
Integration test covers the module-level ``complete_stale_batches`` sweep
function against real PostgreSQL.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_job_row
from taskq.worker._leader_shared import complete_stale_batches

_START = datetime(2025, 1, 1, tzinfo=UTC)


def _make_backend(clock: FakeClock | None = None) -> InMemoryBackend:
    return InMemoryBackend(clock=clock or FakeClock(_START))


# ── Unit tests: InMemoryBackend.prune_old_batches ───────────────────


class TestInMemoryPruneOldBatches:
    async def test_completed_batch_pruned_after_retention(self) -> None:
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

    async def test_prune_skips_batches_with_live_jobs(self) -> None:
        from dataclasses import replace

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
        row = make_job_row(status="pending", queue="default")  # type: ignore[arg-type]  # Why: str status is valid JobStatus at runtime
        row = replace(row, metadata={**row.metadata, "batch_id": str(bid)})
        backend._jobs[row.id] = row

        clock.advance(timedelta(hours=1))
        await backend.complete_batch(bid)

        cutoff = _START + timedelta(hours=2)
        pruned = await backend.prune_old_batches(cutoff)

        assert pruned == 0
        assert bid in backend._batches


# ── Integration test: complete_stale_batches sweep ───────────────────


@pytest.mark.integration
async def test_sweep_completes_stale_batch(pg_dsn: str) -> None:
    schema = f"tq_batch_prune_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        from taskq import migrate as migrate_mod

        await migrate_mod.apply_pending(conn, schema=schema)

        bid = new_uuid()
        await conn.execute(
            f'INSERT INTO "{schema}".batches (id, queue, expected_size) '  # noqa: S608  # Why: test helper — schema is a unique test-generated constant, not user input
            "VALUES ($1, 'default', 1)",
            bid,
        )
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: test helper — schema is a unique test-generated constant
            "(id, queue, actor, payload, max_attempts, retry_kind, metadata, status, "
            "priority, attempt, cancel_phase, progress_seq, payload_schema_ver) "
            "VALUES ($1, 'default', 'test_actor', '{}'::jsonb, "
            "1, 'non_retryable', $2::jsonb, 'succeeded', "
            "0, 1, 0, 0, 1)",
            new_uuid(),
            json.dumps({"batch_id": str(bid)}),
        )

        count = await complete_stale_batches(conn, schema=schema)
        assert count == 1

        status = await conn.fetchval(
            f'SELECT status FROM "{schema}".batches WHERE id = $1',  # noqa: S608  # Why: test helper
            bid,
        )
        assert status == "complete"
        completed_at = await conn.fetchval(
            f'SELECT completed_at FROM "{schema}".batches WHERE id = $1',  # noqa: S608  # Why: test helper
            bid,
        )
        assert completed_at is not None

        count2 = await complete_stale_batches(conn, schema=schema)
        assert count2 == 0
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
