"""Tests for batch prune and stale-batch completion sweep integration.

Unit tests cover ``InMemoryBackend.prune_old_batches`` (no PG required)
and the Postgres ``prune_old_batches`` drain loop (a recording ConnLike
stand-in). Integration test covers the module-level
``complete_stale_batches`` sweep function against real PostgreSQL.
"""

from __future__ import annotations

import inspect
import json
from datetime import UTC, datetime, timedelta
from typing import cast

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._batch_sql import (
    _PRUNE_OLD_BATCHES_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: pin the production statement, not a copy - a copy drifts from the SQL that runs.
    prune_old_batches,
    render_batch_sql,
)
from taskq.backend._protocol import ConnLike
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


# ── Unit tests: Postgres prune_old_batches drain loop ────────────────


class _FetchvalScriptConn:
    """ConnLike stand-in answering ``prune_old_batches``' fetchval with a
    scripted per-call deletion count, recording every statement."""

    def __init__(self, counts: list[int]) -> None:
        self._counts = list(counts)
        self._index = 0
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        count = self._counts[self._index] if self._index < len(self._counts) else 0
        self._index += 1
        return count


async def _prune_old_batches_pg(conn: _FetchvalScriptConn, *, cutoff: datetime) -> int:
    """Invoke the module-level Postgres helper with a rendered BatchSql."""
    return await prune_old_batches(
        cast(ConnLike, conn),  # pyright: ignore[reportArgumentType]  # Why: the stand-in satisfies the fetchval surface the helper uses.
        render_batch_sql("taskq"),
        cutoff,
        batch_size=1000,
    )


def test_prune_old_batches_signature_carries_batch_size() -> None:
    """The bound must be part of the callable's contract, not a caller's
    hope: the Postgres ``prune_old_batches`` exposes a keyword-only
    ``batch_size`` (the ``complete_stale_batches`` precedent)."""
    params = inspect.signature(prune_old_batches).parameters
    assert "batch_size" in params, (
        "prune_old_batches has no batch_size parameter - one call is an "
        f"unbounded DELETE again; signature is {inspect.signature(prune_old_batches)}"
    )


def test_prune_old_batches_sql_windows_candidates_with_limit() -> None:
    """The statement windows candidates in a MATERIALIZED CTE with a
    parameterized LIMIT and reports its count without materialising ids
    (a COUNT over the DELETE's RETURNING set)."""
    sql = _PRUNE_OLD_BATCHES_SQL.format(schema="taskq")
    assert "AS MATERIALIZED" in sql, (
        "the candidate window is unfenced - the planner may inline the "
        f"LIMIT-ed CTE into the DELETE and remove more rows than the LIMIT; got: {sql!r}"
    )
    assert "LIMIT $2" in sql, (
        "the statement carries no parameterized LIMIT - one call is an "
        f"unbounded DELETE again; got: {sql!r}"
    )
    assert "count(*)::int" in sql, (
        "the statement must count without materialising ids - rows are "
        f"fetched only to be counted; got: {sql!r}"
    )


async def test_prune_old_batches_drains_in_bounded_batches() -> None:
    """A 2 500-row eligible set at ``batch_size=1 000`` drains in three
    bounded calls (full, full, short) and reports the total - windowed
    deletes with one committed statement per batch."""
    conn = _FetchvalScriptConn(counts=[1000, 1000, 500])
    total = await _prune_old_batches_pg(conn, cutoff=_START)
    assert total == 2500
    assert len(conn.fetchval_calls) == 3
    for _sql, args in conn.fetchval_calls:
        assert args == (_START, 1000), (
            f"each bounded call must bind (cutoff, batch_size); got {args!r}"
        )


async def test_prune_old_batches_stops_on_empty_window() -> None:
    """An empty first window is one call and zero - no extra round trips."""
    conn = _FetchvalScriptConn(counts=[0])
    total = await _prune_old_batches_pg(conn, cutoff=_START)
    assert total == 0
    assert len(conn.fetchval_calls) == 1


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
            f'INSERT INTO "{schema}".batches (id, queue, expected_size) '  # noqa: S608  # Why: test helper - schema is a unique test-generated constant, not user input
            "VALUES ($1, 'default', 1)",
            bid,
        )
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '  # noqa: S608  # Why: test helper - schema is a unique test-generated constant
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
