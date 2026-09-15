"""Integration tests for the dispatch CTE against real PG (testcontainers).

Covers the full CTE round-trip, bounded over-count under
concurrent producers, identity serialization,
sync ordering, and the lock-expiry chaos test.

All tests use ``pytest.mark.integration``, the ``jobs_app`` fixture
or ``_open_pg_backend`` helper, and assert via direct SQL — these tests
are about PG behaviour.
"""

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg as _asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import set_actor_config_capacity
from taskq.backend._protocol import JobRow
from taskq.backend.postgres import PostgresBackend
from taskq.obs import setup_logging
from taskq.testing.assertions import wait_for_condition
from taskq.testing.fixtures import JobsApp, ModulePgSchema, _open_pg_backend
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.otel import collect_metrics, histogram_points, setup_meter, setup_tracer
from taskq.testing.pg import create_worker
from taskq.worker.run import producer_loop, register_worker
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


# ── Live capacity change (no worker restart) ────────────────────


@pytest.mark.asyncio
async def test_live_capacity_change_takes_effect_without_restart(
    jobs_app: JobsApp,
) -> None:
    """A stored max_concurrent change is visible to the very next dispatch
    call — no ``sync_actor_config`` re-run, no worker restart.

    This is the load-bearing claim behind treating max_concurrent as an
    operator-tunable field: the dispatch CTE joins ``actor_config`` fresh
    on every call (``taskq/backend/_dispatch_sql.py``), so an out-of-band
    UPDATE — exactly what ``taskq actor-config set`` issues — is picked
    up immediately.

    Oracle: sync with max_concurrent=2, dispatch once (2 running, cap
    reached). Directly UPDATE the stored row to max_concurrent=4 —
    simulating an operator running `taskq actor-config set` while the
    worker keeps running. Dispatch again with the same worker/backend
    and assert 2 MORE jobs start running (4 total), proving the second
    dispatch call read the new cap.
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

    for _i in range(8):
        await backend.enqueue(make_enqueue_args(actor="S"))

    worker_id = new_uuid()

    first = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=10,
        lock_lease=_LEASE,
    )
    assert len(first) == 2, f"expected 2 running at max_concurrent=2, got {len(first)}"

    # Operator override, out of band — exactly what `taskq actor-config
    # set S --max-concurrent 4` does under the hood. No sync_actor_config
    # call, no worker restart.
    async with deps.dispatcher_pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{schema}".actor_config SET max_concurrent = 4 WHERE actor = $1',
            "S",
        )

    second = await backend.dispatch_batch(
        worker_id=worker_id,
        queues=["default"],
        limit=10,
        lock_lease=_LEASE,
    )
    assert len(second) == 2, (
        f"expected 2 MORE jobs to start after raising max_concurrent to 4 "
        f"without a restart, got {len(second)}"
    )

    async with deps.worker_pool.acquire() as conn:
        running = await _count_running(conn, schema, "S")
    assert running == 4, f"expected 4 running after the live cap change, got {running}"


@pytest.mark.asyncio
async def test_clearing_max_concurrent_uncaps_rather_than_reverting_to_the_literal(
    jobs_app: JobsApp,
) -> None:
    """Clearing a stored ``max_concurrent`` makes the actor UNLIMITED — it
    does **not** revert to the ``@actor(max_concurrent=...)`` literal.

    This asymmetry is the sharp edge of operator-owned capacity, and it is
    documented in three places (``actor_config_ops`` module and function
    docstrings, and the upgrading guide) while nothing proved it. Its
    sibling — clearing ``max_pending`` reverting *to* the literal — is
    pinned by test_actor_capacity_pg.py::test_cleared_override_reverts_to_literal_pg,
    so a change that made the two fields behave alike would pass that test
    and silently uncap production here.

    The cause is structural: the dispatch CTE joins ``actor_config`` and
    can only see the stored column, never the code literal, so NULL can
    only mean "no cap". ``max_pending`` is enforced client-side where the
    literal is still in scope, hence the difference.

    Oracle: seed a literal of 2, dispatch (2 running, capped). Clear the
    override through the real operator API, then dispatch again and
    require every remaining job to start — the literal 2 must NOT come
    back.
    """
    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name

    await register_worker(deps.dispatcher_pool, deps.settings)

    async with deps.dispatcher_pool.acquire() as conn:
        await sync_actor_config(
            conn,  # type: ignore[arg-type] # Why: PoolConnectionProxy is a transparent proxy delegating to the real Connection; asyncpg's public API accepts it interchangeably
            [ActorConfig(actor="U", max_concurrent=2, queue="default", metadata={})],
            force=False,
            schema=schema,
        )

    for _i in range(6):
        await backend.enqueue(make_enqueue_args(actor="U"))

    worker_id = new_uuid()
    first = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=10, lock_lease=_LEASE
    )
    assert len(first) == 2, f"expected the seeded cap of 2 to hold, got {len(first)}"

    # The operator clears the override — `taskq actor-config set U
    # --clear-max-concurrent`, through the same function the CLI calls.
    async with deps.dispatcher_pool.acquire() as conn:
        await set_actor_config_capacity(
            conn,  # type: ignore[arg-type] # Why: see above
            "U",
            max_concurrent=None,
            schema=schema,
        )

    second = await backend.dispatch_batch(
        worker_id=worker_id, queues=["default"], limit=10, lock_lease=_LEASE
    )
    assert len(second) == 4, (
        "clearing max_concurrent must uncap the actor, so all 4 remaining jobs "
        f"start at once; got {len(second)}. If this is 0, the literal 2 was "
        "wrongly treated as still in force after the clear."
    )

    async with deps.worker_pool.acquire() as conn:
        running = await _count_running(conn, schema, "U")
    assert running == 6, f"expected all 6 running once uncapped, got {running}"


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

        count = await PostgresBackend.sweep_expired_locks(
            conn, _CANCEL_GRACE, _CLEANUP_GRACE, schema=schema
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


# ── Two identical dispatchers share one backlog ─────────────────


@pytest.mark.asyncio
async def test_two_identical_dispatchers_never_claim_the_same_job(
    jobs_app: JobsApp,
) -> None:
    """Two workers running the same config against one queue must never both
    claim the same job, over repeated concurrent rounds.

    This is the safety half of running a fleet: a job claimed by two
    dispatchers executes twice, and every side effect it has — a charge, an
    email, an external call — happens twice with nothing in the job's own
    state to show it. The row's ``locked_by_worker`` must agree with exactly
    one dispatcher's returned claim.
    """
    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    actor = "shared_backlog_actor"

    worker_a = new_uuid()
    worker_b = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_a)
        await create_worker(conn, schema, worker_b)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '
            "VALUES ($1, NULL, $2, $3::jsonb) ON CONFLICT (actor) DO NOTHING",
            actor,
            "default",
            "{}",
        )

    backlog = 40
    for _ in range(backlog):
        await backend.enqueue(make_enqueue_args(actor=actor))

    claimed_by: dict[UUID, list[UUID]] = {worker_a: [], worker_b: []}

    async def _round(worker_id: UUID, barrier: asyncio.Barrier) -> None:
        await barrier.wait()
        rows = await backend.dispatch_batch(
            worker_id=worker_id,
            queues=["default"],
            limit=5,
            lock_lease=_LEASE,
        )
        claimed_by[worker_id].extend(row.id for row in rows)

    for _ in range(4):
        barrier = asyncio.Barrier(2)
        await asyncio.gather(_round(worker_a, barrier), _round(worker_b, barrier))

    all_claims = claimed_by[worker_a] + claimed_by[worker_b]
    assert len(all_claims) == len(set(all_claims)), (
        "a job was claimed by more than one dispatcher — duplicate execution"
    )

    async with deps.worker_pool.acquire() as conn:
        rows = await conn.fetch(
            f'SELECT id, locked_by_worker FROM "{schema}".jobs '
            "WHERE status = 'running' AND actor = $1",
            actor,
        )
    assert len(rows) == len(all_claims), (
        "running rows must match exactly the set of dispatch claims"
    )
    assert {row["locked_by_worker"] for row in rows} <= {worker_a, worker_b}, (
        "no job may be locked by a worker that never claimed it"
    )


@pytest.mark.asyncio
async def test_concurrent_dispatchers_both_claim_from_a_backlog_deeper_than_their_windows(
    jobs_app: JobsApp,
) -> None:
    """Adding a dispatcher must add claim throughput.

    With an uncapped actor and a backlog many times deeper than what both
    dispatchers together ask for in one round, there is no capacity reason
    for either to come back empty: there is plenty of unclaimed, unlocked,
    due work for both. A round in which one dispatcher takes its full limit
    and the other takes nothing means the second worker contributed no
    throughput at all — the fleet ran at single-worker speed while an
    operator paid for two. That failure is invisible to every single-worker
    test and, in production, looks like a healthy but permanently idle pod
    next to a saturated one.
    """
    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    actor = "deep_backlog_actor"

    worker_a = new_uuid()
    worker_b = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_a)
        await create_worker(conn, schema, worker_b)
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, max_concurrent, queue, metadata) '
            "VALUES ($1, NULL, $2, $3::jsonb) ON CONFLICT (actor) DO NOTHING",
            actor,
            "default",
            "{}",
        )

    per_round_limit = 5
    # Far deeper than both dispatchers' combined per-round appetite, so
    # neither can be starved by a shortage of eligible work.
    for _ in range(per_round_limit * 8):
        await backend.enqueue(make_enqueue_args(actor=actor))

    counts: dict[UUID, int] = {}

    async def _round(worker_id: UUID, barrier: asyncio.Barrier) -> None:
        await barrier.wait()
        rows = await backend.dispatch_batch(
            worker_id=worker_id,
            queues=["default"],
            limit=per_round_limit,
            lock_lease=_LEASE,
        )
        counts[worker_id] = len(rows)

    barrier = asyncio.Barrier(2)
    await asyncio.gather(_round(worker_a, barrier), _round(worker_b, barrier))

    assert counts[worker_a] > 0 and counts[worker_b] > 0, (
        "both dispatchers must claim from a backlog deeper than their combined "
        f"windows; got A={counts[worker_a]} B={counts[worker_b]} — the empty-handed "
        "dispatcher added no throughput to the fleet"
    )


# ── Dispatch telemetry on the failure path ─────────────────────────────


@pytest.fixture
async def statement_timeout_dispatcher_pool(
    module_pg_schema: ModulePgSchema,
) -> AsyncIterator[_asyncpg.Pool]:
    """A real dispatcher pool whose server aborts every statement.

    ``statement_timeout`` is a Postgres server setting, so this is the
    real production failure — the server cancels the dispatch query and
    the driver raises — reproduced without a double anywhere in the
    dispatch path. It stands in for every way the dispatch query can
    fail against a loaded or degraded database: a statement timeout on a
    slow plan, a lock timeout, a connection reset mid-query.
    """
    pool = await _asyncpg.create_pool(
        module_pg_schema.pg_dsn,
        min_size=1,
        max_size=2,
        server_settings={"statement_timeout": "1ms"},
    )
    assert pool is not None
    try:
        yield pool
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_dispatch_duration_is_recorded_when_the_dispatch_query_fails(
    clean_jobs_app: JobsApp,
    statement_timeout_dispatcher_pool: _asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dispatch round the database aborts must still contribute a
    duration sample.

    The shipped alert on dispatch health is a p99 over
    ``taskq.dispatch.duration``. A round the server cancels is the
    slowest round there is: it burned the full statement budget and
    returned nothing. If only rounds that returned rows are sampled,
    that budget-exhausting round leaves the series untouched — so a
    dispatcher failing every round reads as a dispatcher with a
    perfectly healthy p99, because the only samples left are the fast
    successful ones (here, none at all, which renders as a flat line an
    operator cannot distinguish from an idle queue).
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
    await backend.enqueue(make_enqueue_args(actor="telemetry_actor"))

    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)
    monkeypatch.setattr(deps, "dispatcher_pool", statement_timeout_dispatcher_pool)

    with pytest.raises(_asyncpg.PostgresError):
        await backend.dispatch_batch(
            worker_id=worker_id,
            queues=["default"],
            limit=5,
            lock_lease=_LEASE,
        )

    points = histogram_points(reader, "taskq.dispatch.duration")
    assert points, (
        "a dispatch round the database aborted recorded no duration sample — "
        "the histogram the dispatch-latency alert reads only ever sees rounds "
        "that succeeded, so total dispatch failure and an idle queue emit the "
        "same thing: nothing"
    )
    assert sum(p.count for p in points) >= 1, (
        f"expected at least one duration observation for the failed round; got {points!r}"
    )


@pytest.mark.asyncio
async def test_dispatch_failure_is_visible_in_a_metric_not_only_a_log_line(
    clean_jobs_app: JobsApp,
    statement_timeout_dispatcher_pool: _asyncpg.Pool,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A producer whose every dispatch round fails must move some metric.

    This is the operator's worst case in this area because nothing else
    reports it. The producer catches the dispatch exception, logs it, and
    polls again, so the process stays up and its liveness probe stays
    green; no job changes status, so no job-level counter moves; the
    pending backlog stays pending, which is exactly what an idle fleet
    also looks like. Every other failure-prone subsystem in the worker
    has a failure counter an alert can name — sweep timeouts, election
    failures, refund failures, slot-pool acquire failures, progress
    publish failures — and dispatch, the one loop whose failure stops all
    work, is asserted here to be no exception. A log line is not a
    substitute: it carries no series to alert on, and an operator who is
    not already tailing that worker's logs has no way to learn that the
    queue stopped draining.
    """
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    worker_id = new_uuid()
    async with deps.worker_pool.acquire() as conn:
        await create_worker(conn, schema, worker_id)
    for _ in range(5):
        await backend.enqueue(make_enqueue_args(actor="telemetry_actor"))

    setup_tracer(monkeypatch)
    reader = setup_meter(monkeypatch)
    monkeypatch.setattr(deps, "dispatcher_pool", statement_timeout_dispatcher_pool)
    # Tighten only the loop cadence, so the scenario's several failed
    # rounds happen promptly. The cadence is not what is under test.
    monkeypatch.setattr(deps.settings, "poll_interval", 0.01, raising=False)
    monkeypatch.setattr(deps.settings, "notify_poll_interval", 0.01, raising=False)

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue(maxsize=5)
    shutdown_event = asyncio.Event()
    producer_stop_event = asyncio.Event()

    # Read the failure where an operator reads it. setup_logging is the
    # production configurator and is idempotent, so this pins the routing
    # rather than depending on an earlier test having configured it.
    setup_logging(level="INFO", log_format="json")
    task = asyncio.create_task(
        producer_loop(
            deps,
            local_queue,
            shutdown_event,
            producer_stop_event,
            backend=backend,
            worker_id=worker_id,
        )
    )
    try:
        with caplog.at_level(logging.ERROR):
            # The failed rounds are the observable; waiting on a count of
            # them is what makes this deterministic rather than timed.
            await wait_for_condition(
                lambda: (
                    sum(
                        1
                        for record in caplog.records
                        if "dispatch-batch-error" in record.getMessage()
                    )
                    >= 3
                ),
                description="three dispatch rounds to fail against the degraded database",
            )
    finally:
        producer_stop_event.set()
        shutdown_event.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert local_queue.qsize() == 0, (
        "no job should have been claimed — the premise is that every round failed"
    )
    async with deps.worker_pool.acquire() as conn:
        still_pending = await conn.fetchval(
            f"SELECT count(*) FROM \"{schema}\".jobs WHERE status = 'pending'"
        )
    assert still_pending == 5, (
        f"the backlog must be untouched for this scenario to be the one under test; "
        f"{still_pending} of 5 jobs are still pending"
    )

    emitted = sorted(metric.name for metric in collect_metrics(reader))
    assert emitted, (
        "a producer that failed every dispatch round against a degraded database "
        "emitted no metric at all — the process stays up, its liveness probe stays "
        "green, the backlog stays pending exactly as an idle queue would, and the "
        "only trace of total dispatch failure is an untelemetered log line"
    )


# ── Attempt counter at the column ceiling ───────────────────────

#: The domain ceiling of the ``jobs.attempt`` smallint column. A row
#: parked here has no headroom for dispatch's ``attempt = attempt + 1``.
_ATTEMPT_COLUMN_CEILING = 32767


async def _park_job_at_attempt_ceiling(
    conn: _PGConn,
    schema: str,
    actor: str,
) -> UUID:
    """Seed one claimable pending job whose ``attempt`` is at the ceiling.

    ``retry_kind='indefinite'`` is the kind that reaches this state in
    production: its retry budget is the ``schedule_to_close`` deadline,
    not ``max_attempts``, so dispatch keeps incrementing the counter for
    as long as the job keeps failing.
    """
    job_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, scheduled_at, attempt, "
        " max_attempts, retry_kind) "
        "VALUES ($1, $2, 'default', '{}'::jsonb, 'pending', "
        "        clock_timestamp() - interval '5 minutes', $3, 3, 'indefinite')",
        job_id,
        actor,
        _ATTEMPT_COLUMN_CEILING,
    )
    return job_id


async def test_backlog_still_dispatches_with_a_job_at_the_attempt_ceiling(
    jobs_app: JobsApp,
) -> None:
    """One job whose attempt counter has reached the column ceiling must
    not stop the rest of the backlog from being claimed.

    A dispatch round claims its whole batch in one statement that stamps
    ``attempt = attempt + 1`` on every claimed row. A job parked at the
    smallint ceiling makes that single statement fail, which aborts the
    claim of every healthy job selected alongside it. Because dispatch
    ranks oldest-scheduled first, the offending row is selected on every
    subsequent round too, so the queue stops draining permanently: an
    operator sees a growing backlog, workers reporting healthy and idle,
    and jobs that are never claimed — with no job in a terminal state to
    point at.

    ``retry_kind='indefinite'`` is the kind that reaches the ceiling:
    ``max_attempts`` is documented as ignored for it, so nothing else
    bounds how far the counter climbs.
    """
    deps = jobs_app.deps
    backend = jobs_app.backend
    schema = deps.settings.schema_name
    actor = f"ceiling_{new_base62(6)}"
    worker_id = new_uuid()

    async with deps.worker_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '
            "ON CONFLICT (actor) DO NOTHING",
            actor,
            "default",
        )
        await _park_job_at_attempt_ceiling(conn, schema, actor)
        for _ in range(3):
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, status, scheduled_at, attempt, "
                " max_attempts, retry_kind) "
                "VALUES ($1, $2, 'default', '{}'::jsonb, 'pending', "
                "        clock_timestamp() - interval '1 minute', 0, 3, 'transient')",
                new_uuid(),
                actor,
            )

    try:
        dispatched = await backend.dispatch_batch(
            worker_id=worker_id,
            queues=["default"],
            limit=10,
            lock_lease=_LEASE,
        )
    except Exception as exc:
        # Why catch broadly: the defect is that ANY driver error escapes a
        # dispatch round because one row cannot be incremented. Naming the
        # concrete exception class would pin the driver, not the behaviour.
        pytest.fail(
            f"a dispatch round against a backlog containing one job at the "
            f"attempt column ceiling raised {type(exc).__name__}: {exc}. "
            f"The round claimed nothing, so three healthy backlogged jobs went "
            f"unclaimed; the same row is re-selected every round, so the queue "
            f"never drains again."
        )

    claimed = {row.id for row in dispatched}
    async with deps.worker_pool.acquire() as conn:
        unclaimed = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1 '
            "AND retry_kind = 'transient' AND status = 'pending'",
            actor,
        )
    assert unclaimed == 0, (
        f"{unclaimed} of the 3 healthy backlogged jobs were left pending by a "
        f"dispatch round that claimed {len(claimed)} rows — a single job at the "
        f"attempt ceiling must not cost the round its healthy work"
    )
