# ruff: noqa: S608

"""Integration tests for round-robin dispatch behaviour against PG.

Tests the ``DISPATCH_ROUND_ROBIN_SQL`` CTE across multiple queues:
fairness, concurrent producers, queue-depth skew, max_concurrent
caps, identity constraints, and bounded over-count under concurrency.

All tests are marked ``integration`` and use the ``module_pg_schema`` /
``clean_jobs_app`` fixtures or ``_open_pg_backend`` for concurrent-
producer scenarios.
"""

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import JobsApp, ModulePgSchema, _open_pg_backend
from taskq.testing.jobs import make_enqueue_args

if TYPE_CHECKING:
    import asyncpg

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)


# ── Helpers ────────────────────────────────────────────────────────────


async def _count_running(conn: "asyncpg.Connection", schema: str, actor: str) -> int:
    row = await conn.fetchrow(
        f'SELECT count(*) AS cnt FROM "{schema}".jobs WHERE status = $1 AND actor = $2',
        "running",
        actor,
    )
    assert row is not None
    return row["cnt"]


async def _count_running_identity_violations(conn: "asyncpg.Connection", schema: str) -> int:
    rows = await conn.fetch(
        f'SELECT actor, identity_key, count(*) AS cnt FROM "{schema}".jobs '
        "WHERE status = 'running' AND identity_key IS NOT NULL "
        "GROUP BY (actor, identity_key) HAVING count(*) > 1"
    )
    return len(rows)


async def _queue_distribution(
    conn: "asyncpg.Connection", schema: str, actor: str
) -> dict[str, int]:
    """Return {queue_name: running_job_count} for the given actor."""
    rows = await conn.fetch(
        f'SELECT queue, count(*) AS cnt FROM "{schema}".jobs '
        "WHERE status = 'running' AND actor = $1 "
        "GROUP BY queue",
        actor,
    )
    return {r["queue"]: r["cnt"] for r in rows}


async def _setup_round_robin_queues(
    conn: "asyncpg.Connection", schema: str, *queue_names: str
) -> None:
    """Insert ``round_robin`` queue rows (safe to call repeatedly)."""
    for q in queue_names:
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, mode) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING',
            q,
            "round_robin",
        )


async def _ensure_actor_config(
    conn: "asyncpg.Connection",
    schema: str,
    actor: str,
    *,
    max_concurrent: int | None = None,
) -> None:
    """Upsert actor_config with optional max_concurrent.

    Uses ``ON CONFLICT … DO UPDATE`` so it works even if ``seed_actors``
    already inserted a default row for the actor.
    """
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '
        "VALUES ($1, $2, $3, $4::jsonb) "
        "ON CONFLICT (actor) DO UPDATE SET max_concurrent = $2, queue = $3, metadata = $4::jsonb",
        actor,
        max_concurrent,
        "default",
        "{}",
    )


# ── Basic round-robin across two queues ───────────────────────


@pytest.mark.asyncio
async def test_round_robin_two_queues_alternate(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """Jobs in two round_robin queues are dispatched alternately.

    Enqueues 5 jobs in q1 and 5 jobs in q2 for the same actor, dispatches
    with both queues, and verifies that jobs from both queues are picked up
    (fairness) rather than one queue being starved.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    actor = "rr1"

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr] # Why: deps is object-typed; WorkerDeps has worker_pool at runtime
        await _setup_round_robin_queues(conn, schema, "q1", "q2")
        await _ensure_actor_config(conn, schema, actor)

    # Enqueue 5 in q1, 5 in q2
    for _i in range(5):
        await backend.enqueue(make_enqueue_args(actor=actor, queue="q1"))
    for _i in range(5):
        await backend.enqueue(make_enqueue_args(actor=actor, queue="q2"))

    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["q1", "q2"],
        limit=10,
        lock_lease=_LEASE,
    )

    assert dispatched, "expected at least one job dispatched"

    # Both queues should contribute — round-robin should not starve either.
    q1_count = sum(1 for r in dispatched if r.queue == "q1")  # type: ignore[comparison-overlap] # Why: Literal str vs str at runtime
    q2_count = sum(1 for r in dispatched if r.queue == "q2")  # type: ignore[comparison-overlap]
    assert q1_count >= 1, f"expected at least 1 job from q1, got {q1_count}"
    assert q2_count >= 1, f"expected at least 1 job from q2, got {q2_count}"


# ── Round-robin fairness under concurrency ────────────────────


@pytest.mark.asyncio
async def test_round_robin_fairness_concurrent(pg_dsn: str) -> None:
    """Two concurrent producers dispatching from 3 round_robin queues.

    Each queue has 5 pending jobs (15 total). Two producers dispatch
    with limit=4 each, synchronised via an ``asyncio.Barrier(2)``.
    Oracle: after both commit, each of the 3 queues should have at least
    one job dispatched — round-robin across queues should prevent any
    single queue from monopolising the dispatch window.
    """
    actor = "X"
    queues_to_test = ["qa", "qb", "qc"]
    _schema = f"rr2_{new_base62()}".lower()

    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=_schema)
    try:
        schema = deps.settings.schema_name
        worker_id = new_uuid()

        async with deps.worker_pool.acquire() as conn:
            await _setup_round_robin_queues(conn, schema, *queues_to_test)
            await _ensure_actor_config(conn, schema, actor)

        # 5 jobs per queue = 15 total
        for qname in queues_to_test:
            for _ in range(5):
                await backend.enqueue(make_enqueue_args(actor=actor, queue=qname))

        barrier = asyncio.Barrier(2)

        async def _producer() -> list[object]:
            await barrier.wait()
            dispatched_list = await backend.dispatch_batch(
                worker_id=worker_id,
                queues=queues_to_test,
                limit=4,
                lock_lease=_LEASE,
            )
            return dispatched_list  # type: ignore[return-value]

        results = await asyncio.gather(_producer(), _producer())

        async with deps.worker_pool.acquire() as conn:
            distribution = await _queue_distribution(conn, schema, actor)
            running_count = await _count_running(conn, schema, actor)

        total_dispatched = sum(len(r) for r in results)  # type: ignore[arg-type]
        assert total_dispatched >= 1, "expected at least one job dispatched"
        assert 1 <= running_count <= 8, f"running_count={running_count} out of expected bounds"

        # Each queue should have at least one running job — round-robin
        # across the 3 queues must not starve any queue.
        for qname in queues_to_test:
            assert distribution.get(qname, 0) >= 1, (
                f"queue {qname} got no dispatched jobs; distribution={distribution}"
            )
    finally:
        await stack.aclose()


# ── Round-robin with queue depth skew ─────────────────────────


@pytest.mark.asyncio
async def test_round_robin_queue_depth_skew(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """One queue has many more pending jobs than another.

    q1 has 20 jobs, q2 has 2 jobs. Dispatch with limit=5 and verify
    that the shallower queue (q2) is not starved — round-robin ignores
    per-queue depth.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    actor = "rr3"

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]
        await _setup_round_robin_queues(conn, schema, "q_deep", "q_shallow")
        await _ensure_actor_config(conn, schema, actor)

    # q_deep: 20 jobs, q_shallow: 2 jobs
    for _ in range(20):
        await backend.enqueue(make_enqueue_args(actor=actor, queue="q_deep"))
    for _ in range(2):
        await backend.enqueue(make_enqueue_args(actor=actor, queue="q_shallow"))

    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["q_deep", "q_shallow"],
        limit=5,
        lock_lease=_LEASE,
    )

    deep_count = sum(1 for r in dispatched if r.queue == "q_deep")  # type: ignore[comparison-overlap]
    shallow_count = sum(1 for r in dispatched if r.queue == "q_shallow")  # type: ignore[comparison-overlap]

    assert dispatched, "expected at least one job dispatched"
    assert shallow_count >= 1, (
        f"shallow queue starved: deep={deep_count} shallow={shallow_count} dispatched"
    )
    assert deep_count >= 1, (
        f"deep queue starved: deep={deep_count} shallow={shallow_count} dispatched"
    )


# ── Round-robin respects max_concurrent ────────────────────────


@pytest.mark.asyncio
async def test_round_robin_respects_max_concurrent(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """Actor config with max_concurrent=2 caps dispatch.

    10 jobs across 2 round_robin queues, dispatch limit=10. Oracle:
    exactly 2 jobs transition to ``running`` regardless of limit.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    actor = "rr4"

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]
        await _setup_round_robin_queues(conn, schema, "q1", "q2")
        await _ensure_actor_config(conn, schema, actor, max_concurrent=2)

    for _i in range(5):
        await backend.enqueue(make_enqueue_args(actor=actor, queue="q1"))
    for _i in range(5):
        await backend.enqueue(make_enqueue_args(actor=actor, queue="q2"))

    dispatched = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["q1", "q2"],
        limit=10,
        lock_lease=_LEASE,
    )

    assert len(dispatched) == 2, (
        f"expected exactly 2 running (max_concurrent=2), got {len(dispatched)}"
    )

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]
        running = await _count_running(conn, schema, actor)

    assert running == 2, f"expected 2 running in PG, got {running}"


# ── Round-robin with identity constraints ──────────────────────


@pytest.mark.asyncio
async def test_round_robin_identity_constraints(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """Same identity_key across different queues serialises to ≤1.

    Enqueues 5 jobs with identity_key="K" in q1 and 5 with the same
    identity_key="K" in q2 (same actor). Dispatches with limit=1 over
    multiple rounds. Oracle: at most 1 job with that identity_key should
    be running at any time.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = module_pg_schema.schema_name
    worker_id = new_uuid()
    actor = "rr5"

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]
        await _setup_round_robin_queues(conn, schema, "q1", "q2")
        await _ensure_actor_config(conn, schema, actor, max_concurrent=10)

    for _i in range(5):
        await backend.enqueue(make_enqueue_args(actor=actor, queue="q1", identity_key="K"))
    for _i in range(5):
        await backend.enqueue(make_enqueue_args(actor=actor, queue="q2", identity_key="K"))

    # Sequential dispatch rounds — at most 1 identity "K" running at a time.
    round_1 = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["q1", "q2"],
        limit=1,
        lock_lease=_LEASE,
    )
    await asyncio.sleep(0.05)

    round_2 = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["q1", "q2"],
        limit=1,
        lock_lease=_LEASE,
    )
    await asyncio.sleep(0.05)

    round_3 = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["q1", "q2"],
        limit=1,
        lock_lease=_LEASE,
    )

    total_dispatched = len(round_1) + len(round_2) + len(round_3)

    async with deps.worker_pool.acquire() as conn:  # type: ignore[union-attr]
        running = await _count_running(conn, schema, actor)
        identity_violations = await _count_running_identity_violations(conn, schema)

    assert len(round_1) == 1, f"expected first round to dispatch 1, got {len(round_1)}"
    assert len(round_2) == 0, (
        f"expected second round dispatch 0 (identity running), got {len(round_2)}"
    )
    assert len(round_3) == 0, (
        f"expected third round dispatch 0 (identity running), got {len(round_3)}"
    )
    assert total_dispatched >= 1, "expected at least one job dispatched"
    assert running <= 1, f"expected ≤ 1 running for actor {actor}, got {running}"
    assert identity_violations == 0, f"expected no identity violations, got {identity_violations}"


# ── Concurrent dispatch bounded over-count ─────────────────────


@pytest.mark.asyncio
async def test_concurrent_producers_bounded_overcount_round_robin(pg_dsn: str) -> None:
    """Two concurrent producers, round_robin mode, bounded over-count.

    Mirrors the standard concurrent-producers bounded over-count scenario
    but with round_robin queues. Actor "X" with
    max_concurrent=4, two round_robin queues (qa, qb), 20 pending jobs
    (10 per queue). Two concurrent dispatch calls with limit=2.

    Oracle (formula): at most
    ``max_concurrent + (num_producers - 1) * min(limit_n, max_concurrent)``
    = ``4 + (2 - 1) * min(2, 4) = 6`` running jobs after both commit.

    Runs 5 iterations with schema drops between iterations.
    """
    actor = "X"
    queues = ["qa", "qb"]
    num_iterations = 5
    _schema = f"rr6_{new_base62()}".lower()

    for _ in range(num_iterations):
        stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=_schema)
        try:
            schema = deps.settings.schema_name
            worker_id = new_uuid()

            async with deps.worker_pool.acquire() as conn:
                await _setup_round_robin_queues(conn, schema, *queues)
                await _ensure_actor_config(conn, schema, actor, max_concurrent=4)

            # 10 jobs per queue = 20 total
            for qname in queues:
                for _i in range(10):
                    await backend.enqueue(make_enqueue_args(actor=actor, queue=qname))

            barrier = asyncio.Barrier(2)

            async def _producer(
                barrier: asyncio.Barrier = barrier,
                backend: PostgresBackend = backend,
                worker_id: UUID = worker_id,
            ) -> list[object]:
                await barrier.wait()
                dispatched_list = await backend.dispatch_batch(
                    worker_id=worker_id,
                    queues=queues,
                    limit=2,
                    lock_lease=_LEASE,
                )
                return dispatched_list  # type: ignore[return-value]

            results = await asyncio.gather(_producer(), _producer())

            async with deps.worker_pool.acquire() as conn:
                running_count = await _count_running(conn, schema, actor)
                identity_violations = await _count_running_identity_violations(conn, schema)
                distribution = await _queue_distribution(conn, schema, actor)

            assert running_count <= 6, f"running_count={running_count} exceeds bound of 6"
            assert identity_violations == 0, (
                f"expected no identity violations, got {identity_violations}"
            )
            total_dispatched = sum(len(r) for r in results)  # type: ignore[arg-type]
            assert total_dispatched >= 1, "expected at least one job dispatched"

            # Both queues should contribute across iterations.
            assert distribution.get("qa", 0) + distribution.get("qb", 0) >= 1
        finally:
            await stack.aclose()
