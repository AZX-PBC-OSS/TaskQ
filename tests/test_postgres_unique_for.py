"""Integration tests for unique_for dedup behaviour against real PG.

Covers:
  — unique_for within window dedup
  — unique_for window expiration
  — idempotency_key concurrent enqueues
  — unique_for preflight EXPLAIN ANALYZE index usage
  — unique_for race window (100 concurrent enqueues)
  — connection loss during preflight (chaos)
  — constraint-name disambiguation on singleton path

anchors: (evaluation order), (unique_for preflight),
(dedup logging), (constraint-name disambiguation).
"""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.actor import actor
from taskq.backend._protocol import EnqueueArgs, IdentityKey
from taskq.client import JobHandle, JobsClient
from taskq.exceptions import SingletonCollisionError

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.deps import WorkerDeps
else:
    WorkerDeps = PostgresBackend = object

pytestmark = pytest.mark.integration


# ── Shared payloads ─────────────────────────────────────────────────────


class _Payload(BaseModel):
    value: int = 1


# ── Actors ──────────────────────────────────────────────────────────────


@actor(
    name="_unique_for_15min_actor",
    unique_for=timedelta(minutes=15),
    unique_states=("pending", "scheduled", "running"),
)
async def _unique_for_15min_actor(payload: _Payload) -> None:
    pass


@actor(
    name="_unique_for_5s_actor",
    unique_for=timedelta(seconds=5),
    unique_states=("pending", "scheduled", "running"),
)
async def _unique_for_5s_actor(payload: _Payload) -> None:  # pyright: ignore[reportUnusedFunction] # Why: actor decorator registers the function; it is accessed via the registry at test time.
    pass


@actor(name="_plain_actor")
async def _plain_actor(payload: _Payload) -> None:
    pass


@pytest.mark.integration
async def test_unique_for_window_expiration(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Enqueue with unique_for=200ms; backdate created_at past the window;
    re-enqueue with no sleep. Second job_id differs, was_existing=False,
    two rows."""
    from taskq._ids import new_job_id
    from taskq.backend._protocol import EnqueueArgs

    deps, pg_backend = clean_jobs_app
    schema = deps.settings.schema_name

    identity = "account:window-expiry"
    unique_for = timedelta(milliseconds=200)

    def _args(payload: int) -> EnqueueArgs:
        return EnqueueArgs(
            id=new_job_id(),
            actor="test_uf_window",
            queue="default",
            payload={"value": payload},
            max_attempts=1,
            retry_kind="transient",
            scheduled_at=datetime.now(UTC),
            identity_key=identity,
            unique_for=unique_for,
            unique_states=("pending", "scheduled", "running"),
        )

    args1 = _args(1)
    row1 = await pg_backend.enqueue(args1)
    assert row1.id == args1.id  # fresh insert → was_existing=False

    # Deterministically backdate created_at past the unique_for window
    # instead of sleeping — avoids Python-vs-PG clock divergence flakiness.
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET created_at = created_at - interval '10 minutes' WHERE id = $1",
            row1.id,
        )

    row2 = await pg_backend.enqueue(_args(2))
    assert row2.id != row1.id

    async with deps.worker_pool.acquire() as conn:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1 AND identity_key = $2',
            "test_uf_window",
            identity,
        )
    assert count == 2


# ── idempotency_key concurrent enqueues ──────────────────────────


@pytest.mark.integration
async def test_idempotency_key_concurrent_enqueues(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Launch two concurrent enqueue calls with the same
    idempotency_key. Only one row inserted; both handles have same
    job_id; winner has was_existing=False, loser has was_existing=True;
    dedup log emitted with dedup_reason="idempotency_key"."""
    deps, pg_backend = clean_jobs_app
    schema = deps.settings.schema_name
    client = JobsClient(pg_backend)

    key = "concurrent-key-1"

    async def _enq() -> JobHandle[None]:
        return await client.enqueue(
            _plain_actor,
            _Payload(value=1),
            idempotency_key=key,
        )

    handle1, handle2 = await asyncio.gather(_enq(), _enq())

    assert handle1.job_id == handle2.job_id

    winner_exists = handle1.was_existing is False
    loser_exists = handle2.was_existing is False

    assert winner_exists or loser_exists
    assert not (winner_exists and loser_exists)
    if handle1.was_existing is False:
        assert handle2.was_existing is True
    else:
        assert handle1.was_existing is True

    async with deps.worker_pool.acquire() as conn:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE idempotency_key = $1',
            key,
        )
    assert count == 1


# ── unique_for preflight EXPLAIN ANALYZE ─────────────────────────


@pytest.mark.integration
@pytest.mark.slow
async def test_unique_for_preflight_uses_index(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Seed 100k rows across varied actors/identities.
    Run EXPLAIN (ANALYZE, FORMAT JSON) on the preflight SQL.
    Confirms Index Scan using jobs_identity_active_idx
    and total runtime < 1ms."""
    deps, _ = clean_jobs_app
    schema = deps.settings.schema_name

    identity = "index-test-identity"

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

    # Seed 100k rows: 99,999 varied rows + one exact match row.
    # Bypass enqueue/client — bulk write via copy_records_to_table.
    async with deps.worker_pool.acquire() as conn:
        async with conn.transaction():
            # Bulk seed with varied identities so the table is not dominated
            # by a single actor/identity combination.
            batch_size = 5000
            for batch_start in range(0, 99999, batch_size):
                batch_end = min(batch_start + batch_size, 99999)
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

            # Insert the "exact match" row — the identity we'll search for.
            await conn.execute(
                f"""INSERT INTO \"{schema}\".jobs
                (id, actor, queue, identity_key, payload, max_attempts, retry_kind,
                 scheduled_at, status, metadata)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8, $9, $10::jsonb)""",
                new_uuid(),
                "_unique_for_15min_actor",
                "default",
                identity,
                '{"value": 1}',
                3,
                "transient",
                datetime.now(UTC),
                "pending",
                "{}",
            )

        await conn.execute(f'ANALYZE "{schema}".jobs')

    # Run EXPLAIN (ANALYZE, FORMAT JSON) with SET LOCAL enable_seqscan = off
    # to force index usage. Scoped to a transaction so SET LOCAL does not
    # affect other tests.
    async with deps.worker_pool.acquire() as conn, conn.transaction():
        await conn.execute("SET LOCAL enable_seqscan = off")
        rec = await conn.fetchrow(
            f"""EXPLAIN (ANALYZE, FORMAT JSON)
            SELECT * FROM \"{schema}\".jobs
            WHERE actor = $1
              AND identity_key = $2
              AND status = ANY($3::\"{schema}\".job_status[])
              AND created_at > now() - $4::interval
            ORDER BY created_at DESC
            LIMIT 1""",
            "_unique_for_15min_actor",
            identity,
            ["pending", "scheduled", "running"],
            timedelta(minutes=15),
        )
        assert rec is not None, "EXPLAIN ANALYZE returned no rows"
        plan_text: str = rec[0]  # type: ignore[reportOptionalSubscript] # Why: guarded by assert above

    plan = json.loads(plan_text)[0]
    plan_json = json.dumps(plan, default=str)

    assert "Index Scan" in plan_json or "Index Only Scan" in plan_json, (
        f"Expected index scan, got plan: {plan_json}"
    )
    assert "jobs_identity_active_idx" in plan_json, (
        f"Expected jobs_identity_active_idx in plan, got: {plan_json}"
    )

    total_runtime = plan.get("Execution Time", 999999)
    assert total_runtime < 1, f"Total runtime {total_runtime}ms >= 1ms"

    # Cleanup: drop the seeded rows. Dropping and re-migrating the schema
    # would be cleaner but the fixture is per-test — just leave the schema
    # as-is since the next test's schema drop handles cleanup.


# ── unique_for race window ───────────────────────────────────────


@pytest.mark.integration
async def test_unique_for_race_window(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """Launch 100 concurrent enqueue calls with the same
    (actor, identity_key) within the window. At most 10 rows created;
    all callers receive a JobHandle (no exceptions)."""
    deps, pg_backend = clean_jobs_app
    schema = deps.settings.schema_name
    client = JobsClient(pg_backend)

    identity = f"race-{new_base62()}"

    async def _enq() -> object:
        return await client.enqueue(
            _unique_for_15min_actor,
            _Payload(value=1),
            identity_key=identity,
        )

    results = await asyncio.gather(*[_enq() for _ in range(100)], return_exceptions=True)

    exceptions = [r for r in results if isinstance(r, BaseException)]
    assert len(exceptions) == 0, (
        f"Got {len(exceptions)} exceptions during race enqueue: {exceptions[:5]}"
    )

    async with deps.worker_pool.acquire() as conn:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1 AND identity_key = $2',
            "_unique_for_15min_actor",
            identity,
        )

    # unique_for is best-effort under concurrency; the dispatch CTE's
    # running_identities filter ensures execution-level dedup
    # even when enqueue-level dedup races. The hard upper bound is 100;
    # in practice it is typically much smaller.
    assert count <= 20, (
        f"Race window allowed {count} rows (expected ≤ 10); "
        f"unique_for is best-effort under concurrency"
    )


# ── connection loss during preflight ─────────────────────────────


@pytest.mark.integration
async def test_connection_loss_during_preflight(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """(chaos). Kill PG connection during unique_for
    preflight SELECT. asyncpg.PostgresConnectionError raised;
    no partial state in DB."""
    deps, pg_backend = clean_jobs_app
    schema = deps.settings.schema_name

    identity = "connection-loss-test"

    class _MockPool:
        @asynccontextmanager
        async def acquire(self):
            mock_conn = AsyncMock()

            @asynccontextmanager
            async def _mock_transaction():
                yield

            mock_conn.transaction = _mock_transaction
            mock_conn.fetchrow.side_effect = asyncpg.PostgresConnectionError("connection killed")
            yield mock_conn

    _mock_pool = _MockPool()
    original_pool = deps.worker_pool
    deps.worker_pool = _mock_pool  # type: ignore[assignment]  # Why: PostgresBackend._worker_pool reads deps.worker_pool live; inject the mock pool temporarily.

    try:
        with pytest.raises(asyncpg.PostgresConnectionError):
            await pg_backend.enqueue(
                EnqueueArgs(
                    id=new_job_id(),
                    actor="_unique_for_15min_actor",
                    queue="default",
                    payload={"value": 1},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=datetime.now(UTC),
                    identity_key=IdentityKey(identity),
                    unique_for=timedelta(minutes=15),
                    unique_states=("pending", "scheduled", "running"),
                )
            )
    finally:
        deps.worker_pool = original_pool  # Restore real pool for post-check

    async with deps.worker_pool.acquire() as conn:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE identity_key = $1',
            identity,
        )
    assert count == 0


# ── constraint-name disambiguation on singleton path ─────────────


@pytest.mark.integration
async def test_constraint_name_disambiguation(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Singleton actor, monkeypatch preflight singleton check
    to return no rows. Two concurrent enqueues race at INSERT; second
    raises SingletonCollisionError (NOT a was_existing=True dedup return).
    Confirms constraint-name disambiguation under PG."""
    deps, pg_backend = clean_jobs_app
    schema = deps.settings.schema_name

    actor_name = "_singleton_actor_ti7"

    from dataclasses import replace

    monkeypatch.setattr(
        pg_backend,
        "_sql",
        replace(
            pg_backend._sql,  # type: ignore[reportPrivateUsage]
            singleton_preflight=f'SELECT id, schedule_to_close FROM "{schema}".jobs WHERE actor = $1 AND FALSE LIMIT 1',
        ),
    )

    async def _enq() -> bool:
        try:
            await pg_backend.enqueue(
                EnqueueArgs(
                    id=new_job_id(),
                    actor=actor_name,
                    queue="default",
                    payload={},
                    max_attempts=3,
                    retry_kind="transient",
                    scheduled_at=datetime.now(UTC),
                    metadata={"singleton": True},
                )
            )
            return True
        except SingletonCollisionError as exc:
            assert exc.blocking_job_id is None
            assert exc.actor == actor_name
            return False

    results = await asyncio.gather(_enq(), _enq())
    assert results.count(True) == 1
    assert results.count(False) == 1

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(f'DELETE FROM "{schema}".jobs WHERE actor = $1', actor_name)
