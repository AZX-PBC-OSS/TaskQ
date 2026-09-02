"""Integration tests for heartbeat_loop and isolate_self against real PG18.

Each test asserts the behavioral contract — leases are renewed to live
future values, liveness timestamps stay fresh, jobs transition correctly —
using single-domain comparisons: one statement reads the server clock and
the row together, so the assertion cannot be corrupted by divergence
between the application and database clocks (VM pause/resume and NTP
drift are common causes of such divergence in containerized environments).
Cross-moment wall-clock comparisons test the environment's clock
alignment, not the contract.

Each test uses small intervals (heartbeat_interval=0.5s, lock_lease=2.0s)
so the suite completes in seconds rather than minutes.
"""

import asyncio
import time
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import pytest
import structlog

from taskq.backend.postgres import _SWEEP_1_SQL
from taskq.testing.assertions import assert_job_status
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.pg import create_running_job, reset_schema, setup_running_job
from taskq.testing.settings import make_integration_settings
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.heartbeat import heartbeat_loop, isolate_self

pytestmark = pytest.mark.integration

_HEARTBEAT_INTERVAL = 0.5
_LOCK_LEASE = 2.0
# Liveness freshness bound: a healthy heartbeat loop misses at most one
# tick, so a liveness timestamp is never older than 2x heartbeat_interval
# when read alongside the server clock in the same statement. This bound
# is part of the behavioral contract (staleness detection), not a fudge
# factor for clock skew.
_STALENESS_BOUND = timedelta(seconds=2 * _HEARTBEAT_INTERVAL)


async def _setup_fast(
    module_pg_schema: ModulePgSchema,
) -> tuple[AsyncExitStack, WorkerDeps, str]:
    """Create WorkerDeps with fast heartbeat intervals per test.

    Uses the module-scoped PG schema (migrated once per test file) and
    truncates all tables for per-test isolation.

    Returns (stack, deps, schema) — the caller MUST ``await stack.aclose()``.
    """
    import asyncpg

    settings = make_integration_settings(
        module_pg_schema.pg_dsn,
        SCHEMA_NAME=module_pg_schema.schema_name,
        HEARTBEAT_INTERVAL=str(_HEARTBEAT_INTERVAL),
        LOCK_LEASE=str(_LOCK_LEASE),
        CANCELLATION_GRACE_PERIOD="0.0",
        CLEANUP_GRACE_PERIOD="0.0",
    )
    schema = settings.schema_name

    # Per-test isolation: truncate all tables
    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await reset_schema(conn, schema)
    finally:
        await conn.close()

    assert settings.pg_dsn_direct is not None
    assert settings.pg_dsn_pooled is not None

    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    return stack, deps, schema


# ── Real PG last_seen_at increments ──────────────────────────────


async def test_last_seen_at_and_heartbeat_advance(module_pg_schema: ModulePgSchema) -> None:
    """The heartbeat loop keeps worker and job liveness fresh.

    Behavioral contract: while the loop runs, ``workers.last_seen_at`` and
    ``jobs.last_heartbeat_at`` are refreshed at the heartbeat cadence, so
    neither is ever staler than one missed tick (``_STALENESS_BOUND``) when
    read alongside the server clock. Each read is one statement that
    observes the clock and the value atomically: cross-moment clock
    comparisons (pairwise progress, first-vs-last) couple the assertion to
    the application and database clocks staying aligned across the whole
    window — they are separate clocks, and divergence or a step between
    them (VM pause/resume, NTP drift) produces false regressions, which is
    exactly how the previous pairwise form failed. A stalled heartbeat
    shows up here as a stale value against the clock read in the same
    statement — the failure mode the contract actually cares about.

    The freshness loop starts only after the first tick has landed: the
    loop ticks immediately on start, but under a parallel ``-n`` run
    contending on the shared PG container the first tick's pool acquire
    and writes can take seconds, and liveness is NULL until it commits.
    Waiting for the first tick tests the cadence contract from a running
    loop instead of racing its startup.
    """
    stack, deps, schema = await _setup_fast(module_pg_schema)
    try:
        async with deps.heartbeat_pool.acquire() as conn:
            worker_id, _job_id = await setup_running_job(
                conn,
                schema,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )

        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown),
            name="heartbeat-ti1",
        )
        try:
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                async with deps.heartbeat_pool.acquire() as conn:
                    seen = await conn.fetchrow(
                        f'SELECT last_seen_at FROM "{schema}".workers WHERE id = $1',
                        worker_id,
                    )
                assert seen is not None
                if seen["last_seen_at"] is not None:
                    break
                await asyncio.sleep(0.2)
            else:
                pytest.fail("heartbeat loop never landed its first tick")

            for _ in range(3):
                await asyncio.sleep(_HEARTBEAT_INTERVAL + 0.05)
                async with deps.heartbeat_pool.acquire() as conn:
                    # One statement per read: the server clock and the
                    # liveness value are observed atomically, so the
                    # freshness comparison below cannot be skewed by a
                    # wall-clock step between write and read.
                    ws = await conn.fetchrow(
                        f"SELECT now() AS pg_now, last_seen_at "
                        f'FROM "{schema}".workers WHERE id = $1',
                        worker_id,
                    )
                    assert ws is not None
                    assert ws["last_seen_at"] is not None
                    assert ws["pg_now"] - ws["last_seen_at"] <= _STALENESS_BOUND

                    jb = await conn.fetchrow(
                        f"SELECT now() AS pg_now, last_heartbeat_at "
                        f'FROM "{schema}".jobs WHERE locked_by_worker = $1',
                        worker_id,
                    )
                    assert jb is not None
                    assert jb["last_heartbeat_at"] is not None
                    assert jb["pg_now"] - jb["last_heartbeat_at"] <= _STALENESS_BOUND
        finally:
            shutdown.set()
            await task
    finally:
        await stack.aclose()


# ── Multi-job lock_expires_at extension under contention ─────────


async def test_multi_job_lock_extension(module_pg_schema: ModulePgSchema) -> None:
    """Heartbeat ticks renew every running job's lock, tightly clustered.

    Behavioral contract: one tick rewrites all four running jobs' locks to
    live future values (one UPDATE, one server clock read), so each lock
    post-tick is dated in the future of the clock observed alongside it,
    and the four leases stay within 2*heartbeat_interval of each other.

    The renewal is detected by polling until every lease is strictly later
    than every setup-time lease (a tick stamps ``clock_timestamp() +
    lock_lease`` and runs after setup, so a rewrite always lands later than
    the originals) instead of assuming a tick lands within a fixed ~1s
    window — under a parallel ``-n`` run contending on the shared PG
    container the first tick can land seconds late, and reading un-renewed
    setup stamps would test the setup, not the renewal."""
    stack, deps, schema = await _setup_fast(module_pg_schema)
    try:
        async with deps.heartbeat_pool.acquire() as conn:
            worker_id, _ = await setup_running_job(
                conn,
                schema,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )
            for _ in range(3):
                await create_running_job(
                    conn,
                    schema,
                    worker_id,
                    lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
                )

            dispatched_count = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE locked_by_worker = $1',
                worker_id,
            )
            assert dispatched_count == 4

            originals = await conn.fetch(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE locked_by_worker = $1',
                worker_id,
            )
            assert len(originals) == 4
            original_max = max(r["lock_expires_at"] for r in originals)
            assert original_max is not None

        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown),
            name="heartbeat-multi-lock",
        )
        try:
            current_locks: list[asyncpg.Record] = []
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                async with deps.heartbeat_pool.acquire() as conn:
                    # Single statement: every lock is compared against the
                    # server clock observed in the same read, so a
                    # wall-clock step between the tick and this read
                    # cannot corrupt the assertion.
                    current_locks = await conn.fetch(
                        f"SELECT now() AS pg_now, lock_expires_at "
                        f'FROM "{schema}".jobs WHERE locked_by_worker = $1 ORDER BY id',
                        worker_id,
                    )
                assert len(current_locks) == 4
                times = [r["lock_expires_at"] for r in current_locks]
                if all(t is not None and t > original_max for t in times):
                    break
                await asyncio.sleep(0.2)
            else:
                pytest.fail("heartbeat loop never rewrote the four job locks")

            times = [r["lock_expires_at"] for r in current_locks]
            assert all(t is not None for t in times)

            # A just-extended lock is live: dated in the future of the
            # server clock read alongside it (tick clock + lock_lease,
            # read in the same statement that observed the rewrite).
            for r in current_locks:
                assert r["lock_expires_at"] > r["pg_now"]

            # All four jobs were extended by the same tick (one UPDATE,
            # one clock read), so their leases stay tightly clustered.
            min_t = min(times)
            max_t = max(times)
            assert (max_t - min_t) <= timedelta(seconds=2 * _HEARTBEAT_INTERVAL)
        finally:
            shutdown.set()
            await task
    finally:
        await stack.aclose()


# ── Reservation lease extension ──────────────────────────────────


async def test_reservation_lease_extension(module_pg_schema: ModulePgSchema) -> None:
    """Heartbeat ticks renew reservation leases to live future values.

    The reservation is inserted with a 1s lease; once a heartbeat tick has
    run, the lease must be live — dated in the future of the server clock
    observed in the same statement that reads it. An unrenewed lease
    (inserted 1s ahead) is already in the past by read time, so a live
    lease proves a tick renewed it. The read polls until the lease is live
    instead of assuming the first tick lands within a fixed ~1s window:
    under a parallel ``-n`` run contending on the shared PG container, the
    loop's first tick can land seconds late — waiting only strengthens the
    unrenewed-lease-is-past proof, and a renewal still shows up as a live
    lease. Comparing against the initial lease instead (the old form)
    coupled the assertion to the application and database clocks staying
    aligned across the whole test window — they are separate clocks and
    can diverge or step (VM pause/resume, NTP drift).
    """
    stack, deps, schema = await _setup_fast(module_pg_schema)
    try:
        async with deps.heartbeat_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn,
                schema,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )

            await conn.execute(
                f'INSERT INTO "{schema}".reservation_slots '
                "(job_id, bucket_name, slot_index, acquired_at, lease_expires_at) "
                "VALUES ($1, $2, 0, now(), now() + interval '1 second')",
                job_id,
                "default",
            )

            initial = await conn.fetchrow(
                f'SELECT lease_expires_at FROM "{schema}".reservation_slots WHERE job_id = $1',
                job_id,
            )
            assert initial is not None
            assert initial["lease_expires_at"] is not None

        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown),
            name="heartbeat-reservation-lease",
        )
        try:
            row: asyncpg.Record | None = None
            deadline = time.monotonic() + 30.0
            while time.monotonic() < deadline:
                async with deps.heartbeat_pool.acquire() as conn:
                    row = await conn.fetchrow(
                        f"SELECT now() AS pg_now, lease_expires_at "
                        f'FROM "{schema}".reservation_slots WHERE job_id = $1',
                        job_id,
                    )
                assert row is not None
                if row["lease_expires_at"] is not None and row["lease_expires_at"] > row["pg_now"]:
                    break
                await asyncio.sleep(0.2)
            assert row is not None
            assert row["lease_expires_at"] is not None, (
                "reservation lease was never renewed to a live value"
            )
            assert row["lease_expires_at"] > row["pg_now"]
        finally:
            shutdown.set()
            await task
    finally:
        await stack.aclose()


# ── Sweep 1 consistency ──────────────────────────────────────────


async def test_sweep1_consistency(module_pg_schema: ModulePgSchema) -> None:
    """Sweep 1 consistency.
    After heartbeat stops and lock is forced expired,
    Sweep 1 SQL transitions the job to pending or crashed."""
    stack, deps, schema = await _setup_fast(module_pg_schema)
    try:
        async with deps.heartbeat_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn,
                schema,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )

        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown),
            name="heartbeat-ti4",
        )
        try:
            await asyncio.sleep(_HEARTBEAT_INTERVAL + 0.05)
        finally:
            shutdown.set()
            await task

        async with deps.heartbeat_pool.acquire() as conn:
            await conn.execute(
                f"UPDATE \"{schema}\".jobs SET lock_expires_at = now() - interval '60 seconds' WHERE id = $1",
                job_id,
            )

            await conn.execute(
                _SWEEP_1_SQL.format(schema=schema),
                timedelta(seconds=30),
                timedelta(seconds=30),
            )

            row = await conn.fetchrow(
                f'SELECT status FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            assert row["status"] in ("pending", "crashed")
    finally:
        await stack.aclose()


# ── isolate_self transitions jobs with cancel_phase > 0 ──────────


async def test_isolate_self_transitions_cancel_phase_gt_zero(
    module_pg_schema: ModulePgSchema,
) -> None:
    """isolate_self transitions jobs with cancel_phase > 0.
    Job with cancel_phase=1 is transitioned (status no longer 'running'),
    and — mirroring _SWEEP_1_SQL branch-for-branch (see
    tests/test_leader_property.py's isolate≡sweep invariant) — its cancel
    state is RESET: a reclaimed attempt starts with a clean cancellation
    slate, so the next worker's cancel-poll does not immediately re-cancel
    the retried job (the retry loop the sweep's identical reset fixes).
    A caller's cancel therefore does not survive into a retried attempt —
    the same documented tradeoff the crash-reclaim sweep makes."""
    stack, deps, schema = await _setup_fast(module_pg_schema)
    try:
        async with deps.heartbeat_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn,
                schema,
                cancel_phase=1,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )

        shutdown = asyncio.Event()
        await isolate_self(deps, worker_id, shutdown)
        assert shutdown.is_set()

        async with deps.heartbeat_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, cancel_phase FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            assert row["status"] == "pending"
            assert row["cancel_phase"] == 0
    finally:
        await stack.aclose()


async def test_isolate_self_cancel_in_flight_exhausted_lands_cancelled(
    module_pg_schema: ModulePgSchema,
) -> None:
    """Mirror of the sweep's exhausted branch: a job with an in-flight
    cancel request and no retries remaining lands on 'cancelled' — the
    caller's explicit request is the honest terminal label.  Cancel state
    is cleared, the attempt row still records outcome='crashed'/
    error_class='HeartbeatLost' (that IS what happened to the attempt),
    and isolate-self-complete telemetry counts the job as cancelled, not
    crashed."""
    stack, deps, schema = await _setup_fast(module_pg_schema)
    try:
        async with deps.heartbeat_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn,
                schema,
                attempt=1,
                max_attempts=1,
                cancel_phase=1,
                cancel_requested_at=datetime.now(UTC),
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )

        shutdown = asyncio.Event()
        with structlog.testing.capture_logs() as captured:
            await isolate_self(deps, worker_id, shutdown)
        assert shutdown.is_set()

        async with deps.heartbeat_pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT status, cancel_phase, cancel_requested_at, finished_at "
                f'FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            attempt_row = await conn.fetchrow(
                f'SELECT outcome, error_class FROM "{schema}".job_attempts WHERE job_id = $1',
                job_id,
            )

        assert row is not None
        assert row["status"] == "cancelled"
        assert row["cancel_phase"] == 0
        assert row["cancel_requested_at"] is None
        assert row["finished_at"] is not None

        assert attempt_row is not None
        assert attempt_row["outcome"] == "crashed"
        assert attempt_row["error_class"] == "HeartbeatLost"

        complete = [e for e in captured if e["event"] == "isolate-self-complete"]
        assert len(complete) == 1
        assert complete[0]["jobs_cancelled_count"] == 1
        assert complete[0]["jobs_crashed_count"] == 0
    finally:
        await stack.aclose()


# ── isolate_self writes one AttemptRow per transition ────────────


async def test_isolate_self_writes_attempt_rows(module_pg_schema: ModulePgSchema) -> None:
    """isolate_self writes one AttemptRow per transition.
    3 running jobs → 3 rows in job_attempts with outcome='crashed',
    error_class='HeartbeatLost', and attempt matching each job."""
    stack, deps, schema = await _setup_fast(module_pg_schema)
    try:
        job_ids: list[UUID] = []
        async with deps.heartbeat_pool.acquire() as conn:
            worker_id, jid1 = await setup_running_job(
                conn,
                schema,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )
            job_ids.append(jid1)
            for _ in range(2):
                jid = await create_running_job(
                    conn,
                    schema,
                    worker_id,
                    lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
                )
                job_ids.append(jid)

        shutdown = asyncio.Event()
        await isolate_self(deps, worker_id, shutdown)
        assert shutdown.is_set()

        async with deps.heartbeat_pool.acquire() as conn:
            attempts = await conn.fetch(
                f'SELECT job_id, attempt, outcome, error_class FROM "{schema}".job_attempts '
                "WHERE job_id = ANY($1) ORDER BY job_id",
                job_ids,
            )
            assert len(attempts) == 3

            for a in attempts:
                assert a["outcome"] == "crashed"
                assert a["error_class"] == "HeartbeatLost"

            jobs = await conn.fetch(
                f'SELECT id, attempt FROM "{schema}".jobs WHERE id = ANY($1) ORDER BY id',
                job_ids,
            )
            for j, a in zip(jobs, attempts, strict=True):  # pyright: ignore[reportUnknownVariableType] # Why: asyncpg Record typing is incomplete via asyncpg-stubs.
                assert a["job_id"] == j["id"]
                assert a["attempt"] == j["attempt"]
    finally:
        await stack.aclose()


# ── isolate_self non_retryable mirrors Sweep 1 ───────────────────


async def test_isolate_self_non_retryable_mirrors_sweep1(
    module_pg_schema: ModulePgSchema,
) -> None:
    """isolate_self non_retryable + budget-remaining mirrors Sweep 1 exactly.
    For a non_retryable job with attempt < max_attempts: status='crashed',
    finished_at IS NOT NULL, scheduled_at unchanged, AttemptRow written."""
    stack, deps, schema = await _setup_fast(module_pg_schema)
    try:
        async with deps.heartbeat_pool.acquire() as conn:
            worker_id, job_id = await setup_running_job(
                conn,
                schema,
                retry_kind="non_retryable",
                attempt=0,
                max_attempts=3,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )
            orig = await conn.fetchrow(
                f'SELECT scheduled_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert orig is not None
            original_scheduled_at = orig["scheduled_at"]

        shutdown = asyncio.Event()
        await isolate_self(deps, worker_id, shutdown)
        assert shutdown.is_set()

        async with deps.heartbeat_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT status, finished_at, scheduled_at FROM "{schema}".jobs WHERE id = $1',
                job_id,
            )
            assert row is not None
            assert_job_status(row, "crashed", finished=True)
            assert row["scheduled_at"] == original_scheduled_at

            attempts = await conn.fetch(
                f'SELECT outcome, error_class FROM "{schema}".job_attempts WHERE job_id = $1',
                job_id,
            )
            assert len(attempts) == 1
            assert attempts[0]["outcome"] == "crashed"
            assert attempts[0]["error_class"] == "HeartbeatLost"
    finally:
        await stack.aclose()


# ── Acceptance-definition: heartbeat extension over 3 ticks ─────────────


async def test_acceptance_definition_heartbeat_extension(
    module_pg_schema: ModulePgSchema,
) -> None:
    """Acceptance-definition: while the loop runs, every running job's
    lock_expires_at is live (future-dated against the server clock read
    alongside it), and last_heartbeat_at / workers.last_seen_at stay
    fresh (never staler than one missed tick), sampled three times."""
    stack, deps, schema = await _setup_fast(module_pg_schema)
    try:
        async with deps.heartbeat_pool.acquire() as conn:
            worker_id, j1 = await setup_running_job(
                conn,
                schema,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )
            j2 = await create_running_job(
                conn,
                schema,
                worker_id,
                lock_expires_at=datetime.now(UTC) + timedelta(seconds=_LOCK_LEASE),
            )

        shutdown = asyncio.Event()
        task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown),
            name="heartbeat-acceptance",
        )
        try:
            for _ in range(3):
                await asyncio.sleep(_HEARTBEAT_INTERVAL + 0.05)

                async with deps.heartbeat_pool.acquire() as conn:
                    ws = await conn.fetchrow(
                        f"SELECT now() AS pg_now, last_seen_at "
                        f'FROM "{schema}".workers WHERE id = $1',
                        worker_id,
                    )
                    assert ws is not None
                    assert ws["last_seen_at"] is not None
                    assert ws["pg_now"] - ws["last_seen_at"] <= _STALENESS_BOUND

                    for jid in (j1, j2):
                        j = await conn.fetchrow(
                            f"SELECT now() AS pg_now, lock_expires_at, last_heartbeat_at "
                            f'FROM "{schema}".jobs WHERE id = $1',
                            jid,
                        )
                        assert j is not None
                        assert j["lock_expires_at"] is not None
                        assert j["lock_expires_at"] > j["pg_now"]
                        assert j["last_heartbeat_at"] is not None
                        assert j["pg_now"] - j["last_heartbeat_at"] <= _STALENESS_BOUND
        finally:
            shutdown.set()
            await task
    finally:
        await stack.aclose()
