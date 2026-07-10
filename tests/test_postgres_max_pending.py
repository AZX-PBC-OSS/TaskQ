"""Integration tests for max_pending backpressure against real PG.

Covers max_pending backpressure and index design.
Runs against the session-scoped ``pg_container`` via the ``clean_jobs_app`` fixture.

Scenarios:
- max_pending enforcement
- count-query performance
- concurrent enqueue over-count bound
- caller transaction safety
- bulk import pattern at the max_pending limit
"""

import asyncio
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.client import JobsClient
from taskq.exceptions import MaxPendingExceededError

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.deps import WorkerDeps
else:
    WorkerDeps = PostgresBackend = object

pytestmark = pytest.mark.integration


class _Payload(BaseModel):
    value: int = 1


@actor(name="_max_pending_10_actor", max_pending=10)
async def _actor_10(payload: _Payload) -> None:
    pass


@actor(name="_max_pending_9_actor", max_pending=9)
async def _actor_9(payload: _Payload) -> None:
    pass


@actor(name="_max_pending_5_actor", max_pending=5)
async def _actor_5(payload: _Payload) -> None:
    pass


@actor(name="_max_pending_1000_actor", max_pending=1000)
async def _actor_1000(payload: _Payload) -> None:
    pass


# ── DoD test (max_pending enforcement) ─────────────────────────────


async def test_max_pending_eleventh_enqueue_raises(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """max_pending=10: enqueue 10, 11th raises, transition one to
    running, 12th succeeds — literal DoD requirement."""
    deps, pg_backend = clean_jobs_app
    schema = deps.settings.schema_name
    client = JobsClient(pg_backend)

    for _ in range(10):
        handle = await client.enqueue(_actor_10, _Payload())
        assert handle.was_existing is False

    with pytest.raises(MaxPendingExceededError) as exc_info:
        await client.enqueue(_actor_10, _Payload())

    assert exc_info.value.actor == "_max_pending_10_actor"
    assert exc_info.value.current_count == 10
    assert exc_info.value.max_pending == 10

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT id FROM \"{schema}\".jobs WHERE actor = $1 AND status = 'pending' LIMIT 1",
            "_max_pending_10_actor",
        )
        assert row is not None
        job_id = row["id"]
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET status = 'running', started_at = now(), "
            f"locked_by_worker = $1, lock_expires_at = now() + interval '30 seconds' "
            f"WHERE id = $2",
            new_uuid(),
            job_id,
        )

    handle_12 = await client.enqueue(_actor_10, _Payload())
    assert handle_12.was_existing is False


# ── count-query performance ───────────────────────


@pytest.mark.slow
async def test_count_query_uses_partial_index_under_one_ms(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Bulk-seed 100k pending jobs, VACUUM ANALYZE, EXPLAIN ANALYZE
    shows Index Scan or Index Only Scan on jobs_actor_pending_idx, < 1ms."""
    deps, _ = clean_jobs_app
    schema = deps.settings.schema_name

    columns = [
        "id",
        "actor",
        "queue",
        "identity_key",
        "fairness_key",
        "payload",
        "payload_schema_ver",
        "status",
        "priority",
        "attempt",
        "max_attempts",
        "retry_kind",
        "created_at",
        "scheduled_at",
        "metadata",
        "progress_state",
        "progress_seq",
    ]

    target_actor = "_max_pending_10_actor"

    async with deps.worker_pool.acquire() as conn:
        async with conn.transaction():
            batch_size = 5000
            for batch_start in range(0, 100_000, batch_size):
                batch_end = min(batch_start + batch_size, 100_000)
                records: list[tuple[object, ...]] = []
                for i in range(batch_start, batch_end):
                    records.append(
                        (
                            new_uuid(),
                            f"seed_actor_{i % 1000}",
                            "default",
                            f"seed_ident_{i}",
                            None,
                            '{"value": 1}',
                            1,
                            "pending",
                            0,
                            0,
                            3,
                            "transient",
                            datetime.now(UTC),
                            datetime.now(UTC),
                            "{}",
                            "{}",
                            0,
                        )
                    )
                await conn.copy_records_to_table(
                    "jobs",
                    schema_name=schema,
                    records=records,
                    columns=columns,
                )

            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                f"(id, actor, queue, identity_key, payload, max_attempts, retry_kind, "
                f"scheduled_at, status, metadata) "
                f"VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10::jsonb)",
                new_uuid(),
                target_actor,
                "default",
                "perf-test-identity",
                '{"value": 1}',
                3,
                "transient",
                datetime.now(UTC),
                "pending",
                "{}",
            )

        await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs')

    async with deps.worker_pool.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL enable_seqscan = off")
        rec = await conn.fetchrow(
            f"EXPLAIN (ANALYZE, FORMAT JSON) "
            f'SELECT count(*) FROM "{schema}".jobs '
            f"WHERE actor = $1 AND status IN ('pending', 'scheduled')",
            target_actor,
        )
        assert rec is not None, "EXPLAIN ANALYZE returned no rows"
        plan_text: str = rec[0]  # type: ignore[reportOptionalSubscript] # Why: guarded by assert above

    plan = json.loads(plan_text)[0]
    plan_json = json.dumps(plan, default=str)

    assert "Index Scan" in plan_json or "Index Only Scan" in plan_json, (
        f"Expected index scan, got plan: {plan_json[:500]}"
    )
    assert "jobs_actor_pending_idx" in plan_json, (
        f"Expected jobs_actor_pending_idx in plan, got: {plan_json[:500]}"
    )

    total_runtime = plan.get("Execution Time", 999999)
    # < 1ms. If flaky in CI, loosen to < 5ms with a comment
    # explaining the bound is OS / IO-scheduler driven, not algorithmic.
    assert total_runtime < 1, f"Total runtime {total_runtime}ms >= 1ms"


# ── concurrent enqueue over-count bound ─────────────────────────


async def test_concurrent_enqueue_over_count_bounded_by_one(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """max_pending=9, seed 8, two barrier-coordinated enqueues.
    Final count ≤ 10 (max_pending + num_producers - 1). The barrier
    synchronises entry into enqueue; depending on connection-acquire
    timing one or both may succeed, one may observe count=9 and raise.
    The loose oracle accepts any count ≤ max_pending + num_producers - 1."""
    deps, pg_backend = clean_jobs_app
    schema = deps.settings.schema_name
    client = JobsClient(pg_backend)

    for _ in range(8):
        await client.enqueue(_actor_9, _Payload())

    barrier = asyncio.Barrier(2)

    async def _enq() -> None:
        await barrier.wait()
        await client.enqueue(_actor_9, _Payload())

    results = await asyncio.gather(_enq(), _enq(), return_exceptions=True)
    exceptions = [r for r in results if isinstance(r, BaseException)]
    assert all(isinstance(e, MaxPendingExceededError) for e in exceptions), (
        f"Unexpected exception type: {[type(e).__name__ for e in exceptions]}"
    )

    async with deps.worker_pool.acquire() as conn:
        pending_count = await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = $1 AND status IN ('pending', 'scheduled')",
            "_max_pending_9_actor",
        )
    current_count: int = int(pending_count)

    assert current_count <= 10, (
        f"Expected pending count ≤ 10 (max_pending + num_producers - 1), got {current_count}"
    )


# ── caller transaction safety ───────────────────────────────────


async def test_max_pending_raises_before_insert_no_partial_state(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """max_pending=5. Enqueue 5, then 6th raises before INSERT;
    count stays 5 — no row was inserted, no partial state leaks."""
    deps, pg_backend = clean_jobs_app
    schema = deps.settings.schema_name
    client = JobsClient(pg_backend)

    for _ in range(5):
        await client.enqueue(_actor_5, _Payload())

    async with deps.worker_pool.acquire() as conn:
        pre_count = await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = $1 AND status IN ('pending', 'scheduled')",
            "_max_pending_5_actor",
        )
    assert int(pre_count) == 5

    with pytest.raises(MaxPendingExceededError):
        await client.enqueue(_actor_5, _Payload())

    async with deps.worker_pool.acquire() as conn:
        post_count = await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = $1 AND status IN ('pending', 'scheduled')",
            "_max_pending_5_actor",
        )
    assert int(post_count) == 5, (
        f"Expected 5 pending jobs after MaxPendingExceededError, got {post_count}"
    )


# ── bulk import pattern at the max_pending limit ────────────────────────


@pytest.mark.slow
async def test_bulk_import_pattern_at_max_pending_limit(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Bulk-import shape: max_pending=1000; enqueue 1000; 1001st raises
    MaxPendingExceededError with current_count=1000, max_pending=1000."""
    _, pg_backend = clean_jobs_app
    client = JobsClient(pg_backend)

    for _ in range(1000):
        await client.enqueue(_actor_1000, _Payload())

    with pytest.raises(MaxPendingExceededError) as exc_info:
        await client.enqueue(_actor_1000, _Payload())

    assert exc_info.value.current_count == 1000
    assert exc_info.value.max_pending == 1000
    assert exc_info.value.actor == "_max_pending_1000_actor"
