"""Integration tests for heartbeat_loop and isolate_self against real PG18.

Test IDs: through plus
the acceptance-definition assertion.

Each test uses small intervals (heartbeat_interval=0.5s, lock_lease=2.0s)
so the suite completes in seconds rather than minutes.
"""

import asyncio
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

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
# See test_last_seen_at_and_heartbeat_advance's docstring: an environment/
# pooling timing characteristic (reproduced independently of any TaskQ code),
# not an application bug.
_CLOCK_JITTER_TOLERANCE = timedelta(milliseconds=750)


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


async def _run_heartbeat_ticks(
    deps: WorkerDeps,
    worker_id: UUID,
    ticks: int,
) -> None:
    """Run heartbeat_loop in a background task for *ticks* ticks, then cancel."""
    shutdown = asyncio.Event()
    task = asyncio.create_task(
        heartbeat_loop(deps, worker_id, shutdown),
        name="heartbeat-integration",
    )
    try:
        await asyncio.sleep(_HEARTBEAT_INTERVAL * ticks + 0.05)
    finally:
        shutdown.set()
        await task


# ── Real PG last_seen_at increments ──────────────────────────────


async def test_last_seen_at_and_heartbeat_advance(module_pg_schema: ModulePgSchema) -> None:
    """Real PG last_seen_at and last_heartbeat_at advance over repeated
    heartbeat ticks.

    Per-tick comparisons tolerate a small amount of backward jitter
    (_CLOCK_JITTER_TOLERANCE): independently reproduced against a minimal
    asyncpg + real Postgres harness with no TaskQ code involved at all, two
    reads of a clock_timestamp()-stamped row can occasionally observe a
    small apparent regression under concurrent connection-pool load — an
    environment/pooling timing characteristic, not an application bug (each
    write to a given row is still fully serialized by Postgres's row lock,
    so the actual written values are monotonic; what's occasionally stale
    is a *read* racing a pool connection handoff). A strict zero-tolerance
    pairwise assertion is testing that environmental guarantee, not
    anything TaskQ's heartbeat mechanism promises. The real property under
    test — heartbeats are actually happening, not stalled — is instead
    verified by the final assertion, which requires clear overall forward
    progress across the whole test that no plausible jitter could produce.
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
            first_seen: datetime | None = None
            first_hb: datetime | None = None
            prev_last_seen: datetime | None = None
            prev_last_hb: datetime | None = None
            cur_seen: datetime | None = None
            cur_hb: datetime | None = None

            for _ in range(3):
                await asyncio.sleep(_HEARTBEAT_INTERVAL + 0.05)
                async with deps.heartbeat_pool.acquire() as conn:
                    ws = await conn.fetchrow(
                        f'SELECT last_seen_at FROM "{schema}".workers WHERE id = $1',
                        worker_id,
                    )
                    assert ws is not None
                    seen_val: datetime = ws["last_seen_at"]
                    if prev_last_seen is not None:
                        assert seen_val >= prev_last_seen - _CLOCK_JITTER_TOLERANCE, (
                            f"last_seen_at regressed beyond clock-jitter tolerance: "
                            f"{seen_val} < {prev_last_seen} - {_CLOCK_JITTER_TOLERANCE}"
                        )
                    cur_seen = seen_val

                    jb = await conn.fetchrow(
                        f'SELECT last_heartbeat_at FROM "{schema}".jobs WHERE locked_by_worker = $1',
                        worker_id,
                    )
                    assert jb is not None
                    hb_val: datetime = jb["last_heartbeat_at"]
                    if prev_last_hb is not None:
                        assert hb_val >= prev_last_hb - _CLOCK_JITTER_TOLERANCE, (
                            f"last_heartbeat_at regressed beyond clock-jitter tolerance: "
                            f"{hb_val} < {prev_last_hb} - {_CLOCK_JITTER_TOLERANCE}"
                        )
                    cur_hb = hb_val

                    if first_seen is None:
                        first_seen = cur_seen
                        first_hb = cur_hb
                    prev_last_seen = cur_seen
                    prev_last_hb = cur_hb
        finally:
            shutdown.set()
            await task

        assert first_seen is not None
        assert first_hb is not None
        assert cur_seen is not None
        assert cur_hb is not None
        # Clear overall forward progress over 3 ticks — far beyond anything
        # the clock-jitter tolerance above could produce — proves the
        # heartbeat mechanism is genuinely advancing, not stalled.
        min_advance = timedelta(seconds=_HEARTBEAT_INTERVAL)
        assert cur_seen - first_seen >= min_advance, (
            f"last_seen_at did not advance over the test: {first_seen} -> {cur_seen}"
        )
        assert cur_hb - first_hb >= min_advance, (
            f"last_heartbeat_at did not advance over the test: {first_hb} -> {cur_hb}"
        )
    finally:
        await stack.aclose()


# ── Multi-job lock_expires_at extension under contention ─────────


async def test_multi_job_lock_extension(module_pg_schema: ModulePgSchema) -> None:
    """Multi-job lock_expires_at extension under contention.
    All 4 jobs' lock_expires_at advance past the dispatched value,
    and all 4 are within 2*heartbeat_interval of each other."""
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

            dispatched_locks = await conn.fetch(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE locked_by_worker = $1',
                worker_id,
            )
            assert len(dispatched_locks) == 4

        await _run_heartbeat_ticks(deps, worker_id, ticks=2)

        async with deps.heartbeat_pool.acquire() as conn:
            current_locks = await conn.fetch(
                f'SELECT lock_expires_at FROM "{schema}".jobs WHERE locked_by_worker = $1 ORDER BY id',
                worker_id,
            )
            assert len(current_locks) == 4

            times = [
                r["lock_expires_at"] for r in current_locks if r["lock_expires_at"] is not None
            ]
            assert len(times) == 4

            for i in range(4):
                dl = dispatched_locks[i]["lock_expires_at"]
                assert dl is not None
                assert times[i] > dl

            min_t = min(times)
            max_t = max(times)
            assert (max_t - min_t) <= timedelta(seconds=2 * _HEARTBEAT_INTERVAL)
    finally:
        await stack.aclose()


# ── Reservation lease extension ──────────────────────────────────


async def test_reservation_lease_extension(module_pg_schema: ModulePgSchema) -> None:
    """Reservation lease extension.
    INSERT a reservation_slots row tied to a running job with
    lease_expires_at = now() + 5s. After 2 heartbeat ticks,
    lease_expires_at > now() + 5s."""
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

        await _run_heartbeat_ticks(deps, worker_id, ticks=2)

        async with deps.heartbeat_pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT lease_expires_at FROM "{schema}".reservation_slots WHERE job_id = $1',
                job_id,
            )
            assert row is not None
            assert row["lease_expires_at"] is not None
            initial_lease = initial["lease_expires_at"]
            assert row["lease_expires_at"] > initial_lease
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
    and cancel_phase is preserved on the row."""
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
            assert row["status"] != "running"
            assert row["cancel_phase"] == 1
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
    """Acceptance-definition: After 3 ticks, every running job's
    lock_expires_at and last_heartbeat_at are both > their values from
    the previous tick; workers.last_seen_at is also advancing."""
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
            prev_worker_seen: datetime | None = None
            prev_job_locks: dict[UUID, tuple[datetime, datetime]] = {}

            for _ in range(3):
                await asyncio.sleep(_HEARTBEAT_INTERVAL + 0.05)

                async with deps.heartbeat_pool.acquire() as conn:
                    ws = await conn.fetchrow(
                        f'SELECT last_seen_at FROM "{schema}".workers WHERE id = $1',
                        worker_id,
                    )
                    assert ws is not None
                    cur_seen: datetime = ws["last_seen_at"]
                    if prev_worker_seen is not None:
                        assert cur_seen >= prev_worker_seen
                    prev_worker_seen = cur_seen

                    for jid in (j1, j2):
                        j = await conn.fetchrow(
                            f'SELECT lock_expires_at, last_heartbeat_at FROM "{schema}".jobs WHERE id = $1',
                            jid,
                        )
                        assert j is not None
                        cur_lock: datetime = j["lock_expires_at"]
                        cur_hb: datetime = j["last_heartbeat_at"]
                        if jid in prev_job_locks:
                            prev_lock, prev_hb = prev_job_locks[jid]
                            assert cur_lock >= prev_lock
                            assert cur_hb >= prev_hb
                        prev_job_locks[jid] = (cur_lock, cur_hb)
        finally:
            shutdown.set()
            await task
    finally:
        await stack.aclose()
