"""Integration tests for the dispatch CTE against real PG (testcontainers).

Covers the full CTE round-trip, bounded over-count under
concurrent producers, identity serialization,
sync ordering, and the lock-expiry chaos test.

All tests use ``pytest.mark.integration``, the ``jobs_app`` fixture
or ``_open_pg_backend`` helper, and assert via direct SQL — these tests
are about PG behaviour.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import JobsApp, _open_pg_backend
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.pg import create_worker
from taskq.worker.actor_config import ActorConfig
from taskq.worker.run import register_worker
from taskq.worker.startup import sync_actor_config

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    type _PGConn = asyncpg.Connection[asyncpg.Record] | PoolConnectionProxy[asyncpg.Record]
else:
    type _PGConn = object  # pyright: ignore[reportInvalidTypeForm] # Why: asyncpg classes are not subscriptable at runtime

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_CANCEL_GRACE = timedelta(seconds=30)
_CLEANUP_GRACE = timedelta(seconds=30)


# ── Helpers ────────────────────────────────────────────────────────────


async def _count_running(conn: "_PGConn", schema: str, actor: str) -> int:
    row = await conn.fetchrow(
        f"SELECT count(*) AS cnt FROM \"{schema}\".jobs WHERE status = 'running' AND actor = $1",
        actor,
    )
    assert row is not None
    return row["cnt"]


async def _count_running_identity_violations(conn: "_PGConn", schema: str) -> int:
    rows = await conn.fetch(
        f'SELECT actor, identity_key, count(*) AS cnt FROM "{schema}".jobs '
        "WHERE status = 'running' AND identity_key IS NOT NULL "
        "GROUP BY (actor, identity_key) HAVING count(*) > 1"
    )
    return len(rows)


# ── Full CTE round-trip ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_full_cte_round_trip(jobs_app: JobsApp) -> None:
    """Enqueue one job, dispatch, verify every field."""
    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name

    worker_id = new_uuid()

    # Insert actor_config row — required by per_actor_capacity CTE
    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING',
            "A",
            "default",
        )

    args = make_enqueue_args(actor="A")
    await backend.enqueue(args)

    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=10,
        lock_lease=_LEASE,
    )
    assert len(dispatched) == 1
    row = dispatched[0]

    assert row.status == "running"  # type: ignore[comparison-overlap] # Why: JobStatus is Literal[...]; pyright narrows too conservatively across frozen dataclass fields
    assert row.attempt == 1
    assert row.locked_by_worker == worker_id

    assert row.started_at is not None
    assert row.last_heartbeat_at is not None
    assert row.lock_expires_at is not None

    async with deps.worker_pool.acquire() as conn:
        pg_now_row = await conn.fetchrow(f'SELECT now() AS pg_now FROM "{schema}".jobs LIMIT 1')
    assert pg_now_row is not None
    pg_now: datetime = pg_now_row["pg_now"]

    tolerance = timedelta(seconds=1)
    assert abs(row.started_at - pg_now) < tolerance
    assert abs(row.last_heartbeat_at - pg_now) < tolerance
    expected_lock = pg_now + _LEASE
    assert abs(row.lock_expires_at - expected_lock) < tolerance


# ── Concurrent producers, bounded over-count ────────────────────


@pytest.mark.asyncio
async def test_concurrent_producers_bounded_overcount(pg_dsn: str) -> None:
    """Two concurrent producers dispatching against actor_config cap.

    Pre-populates actor_config with max_concurrent=4 for actor "X",
    enqueues 20 pending jobs, then runs two concurrent dispatch calls
    synchronised via ``asyncio.Barrier(2)``. Each dispatch uses
    limit_n=2.

    Oracle (formula): at most
    ``max_concurrent + (num_producers - 1) * min(limit_n, max_concurrent)``
    = ``4 + (2 - 1) * min(2, 4) = 6`` running jobs after both commit.

    Runs 5 times with schema drops between iterations — flake suppression.
    """
    actor = "X"
    num_iterations = 5
    schema_name = f"tdp_overcount_{new_base62()}".lower()

    for _ in range(num_iterations):
        stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema_name)
        try:
            schema = deps.settings.schema_name
            worker_id = new_uuid()

            async with deps.worker_pool.acquire() as conn:
                await conn.execute(
                    f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '
                    "VALUES ($1, $2, $3, $4::jsonb) "
                    "ON CONFLICT (actor) DO UPDATE SET max_concurrent = $2, queue = $3",
                    actor,
                    4,
                    "default",
                    "{}",
                )

            for _i in range(20):
                await backend.enqueue(make_enqueue_args(actor=actor))

            barrier = asyncio.Barrier(2)

            async def _producer(
                barrier: asyncio.Barrier = barrier,
                backend: PostgresBackend = backend,
                worker_id: UUID = worker_id,
            ) -> list[object]:
                await barrier.wait()
                dispatched_list = await backend.dispatch_batch(
                    worker_id=worker_id,
                    queues=["default"],
                    limit=2,
                    lock_lease=_LEASE,
                )
                return dispatched_list  # type: ignore[return-value] # Why: list[JobRow] is covariant-compatible with list[object] at runtime

            results = await asyncio.gather(_producer(), _producer())

            async with deps.worker_pool.acquire() as conn:
                running_count = await _count_running(conn, schema, actor)
                identity_violations = await _count_running_identity_violations(conn, schema)

            assert running_count <= 6, f"running_count={running_count} exceeds bound of 6"
            assert identity_violations == 0, (
                f"expected no identity violations, got {identity_violations}"
            )
            total_dispatched = sum(len(r) for r in results)  # type: ignore[arg-type] # Why: results elements are list[object] at type level, list[JobRow] at runtime
            assert total_dispatched >= 1
        finally:
            await stack.aclose()


# ── Identity serialization under concurrency ────────────────────


@pytest.mark.asyncio
async def test_identity_serialization_concurrent(
    jobs_app: JobsApp,
) -> None:
    """At most 1 identity-key instance in ``running`` at a snapshot.

    Enqueues 10 jobs with the same ``(actor, identity_key)`` pair
    (actor="I", identity_key="K"), then runs 3 dispatch rounds
    sequentially with a brief sleep between each to allow PG visibility.
    Asserts at most 1 job is running for that identity after all rounds.
    """
    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    worker_id = new_uuid()

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '
            "VALUES ($1, $2, $3, $4::jsonb)",
            "I",
            10,
            "default",
            "{}",
        )

    for _i in range(10):
        await backend.enqueue(make_enqueue_args(actor="I", identity_key="K"))

    # Sequential dispatch rounds with limit=1 so at most 1 identity per round.
    # First round dispatches 1 (empty running_identities, limited by limit=1).
    # Subsequent rounds see the identity in running_identities, dispatch 0.
    round_1 = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=1,
        lock_lease=_LEASE,
    )
    await asyncio.sleep(0.05)

    round_2 = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=1,
        lock_lease=_LEASE,
    )
    await asyncio.sleep(0.05)

    round_3 = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=1,
        lock_lease=_LEASE,
    )

    total_dispatched = len(round_1) + len(round_2) + len(round_3)

    async with deps.worker_pool.acquire() as conn:
        count = await _count_running(conn, schema, "I")

    assert len(round_1) == 1, f"expected first round to dispatch 1, got {len(round_1)}"
    assert len(round_2) == 0, (
        f"expected second round dispatch 0 (identity running), got {len(round_2)}"
    )
    assert len(round_3) == 0, (
        f"expected third round dispatch 0 (identity running), got {len(round_3)}"
    )
    assert total_dispatched >= 1, "expected at least one job dispatched"
    assert count <= 1, f"expected <= 1 running for identity 'K', got {count}"


# ── Sync ordering (bootstrap → dispatch) ────────────────────────


@pytest.mark.asyncio
async def test_sync_ordering_bootstrap_to_dispatch(
    jobs_app: JobsApp,
) -> None:
    """sync_actor_config write visible to immediate dispatch.

    Calls register_worker → sync_actor_config on dispatcher_pool,
    enqueues 5 pending jobs for "S", dispatches one tick.
    Oracle: exactly 2 transition to ``running`` (max_concurrent=2),
    3 remain ``pending``.
    """
    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name

    await register_worker(deps.dispatcher_pool, deps.settings)

    configs = [ActorConfig(actor="S", max_concurrent=2, queue="default", metadata={})]
    async with deps.dispatcher_pool.acquire() as conn:
        await sync_actor_config(
            conn,  # type: ignore[arg-type] # Why: PoolConnectionProxy is a transparent proxy delegating to the real Connection; asyncpg's public API accepts it interchangeably
            configs,
            force=False,
            schema=schema,
        )

    for _i in range(5):
        await backend.enqueue(make_enqueue_args(actor="S"))

    worker_id = new_uuid()
    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=10,
        lock_lease=_LEASE,
    )

    assert len(dispatched) == 2, (
        f"expected exactly 2 running (max_concurrent=2), got {len(dispatched)}"
    )

    async with deps.worker_pool.acquire() as conn:
        running = await _count_running(conn, schema, "S")
        row = await conn.fetchrow(
            f"SELECT count(*) AS cnt FROM \"{schema}\".jobs WHERE status = 'pending' AND actor = 'S'"
        )
        assert row is not None

    assert running == 2, f"expected 2 running, got {running}"
    assert row["cnt"] == 3, f"expected 3 pending, got {row['cnt']}"


# ── Lock expiry during dispatch (chaos) ─────────────────────────


@pytest.mark.asyncio
async def test_lock_expiry_recovery_sweep(jobs_app: JobsApp) -> None:
    """Lock expiry recovery sweep then re-dispatch.

    Dispatches a job, manually expires its lock, runs
    ``sweep_expired_locks``, asserts it returns to ``pending``,
    then re-dispatches and verifies attempt=2 and new worker_id.
    """
    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name

    worker_id_a = new_uuid()
    args = make_enqueue_args(actor="C")
    job_id: UUID = args.id

    await backend.enqueue(args)

    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id_a)

    dispatched = await backend.dispatch_batch(
        worker_id=worker_id_a,
        queues=["default"],
        limit=10,
        lock_lease=_LEASE,
    )
    assert len(dispatched) == 1
    assert dispatched[0].status == "running"  # type: ignore[comparison-overlap] # Why: JobStatus Literal union narrowed conservatively across frozen dataclass
    assert dispatched[0].attempt == 1
    assert dispatched[0].locked_by_worker == worker_id_a

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f"UPDATE \"{schema}\".jobs SET lock_expires_at = now() - interval '1 minute' WHERE id = $1",
            job_id,
        )

        now = datetime.now(UTC)
        count = await PostgresBackend.sweep_expired_locks(
            conn, now, _CANCEL_GRACE, _CLEANUP_GRACE, schema=schema
        )
        assert count >= 1

        row = await conn.fetchrow(
            f'SELECT status, attempt, locked_by_worker, lock_expires_at FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        assert row is not None
        assert row["status"] == "pending"
        assert row["locked_by_worker"] is None
        assert row["lock_expires_at"] is None

        # Sweep advances scheduled_at by ~5 s (re-queue backoff).
        # Reset to now() so re-dispatch finds the job.
        await conn.execute(
            f'UPDATE "{schema}".jobs SET scheduled_at = now() WHERE id = $1',
            job_id,
        )

    worker_id_b = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id_b)

    re_dispatched = await backend.dispatch_batch(
        worker_id=worker_id_b,
        queues=["default"],
        limit=10,
        lock_lease=_LEASE,
    )

    assert len(re_dispatched) == 1
    rd = re_dispatched[0]
    assert rd.status == "running"  # type: ignore[comparison-overlap] # Why: JobStatus Literal union narrowed conservatively across frozen dataclass
    assert rd.attempt == 2, f"expected attempt=2 after re-dispatch, got {rd.attempt}"
    assert rd.locked_by_worker == worker_id_b
    assert rd.lock_expires_at is not None
