# ruff: noqa: S608  # Why: schema names are fixed test identifiers, every value is $n-bound.
"""Attack: the lifecycle's ENDS are less proven than its middle.

Four attack families against boot and shutdown, mined from river
#376/#400/#482/#1118/#63, pg-boss #133, graphile #353, dramatiq #441:

1. THE CONCURRENT BOOT. Eight workers barrier-started against (a) a fresh
   empty schema and (b) an established one. The migration advisory lock
   must serialize the appliers (exactly-once ledger, a partition of the
   bundled chain across the winners, no duplicate-key death), a holder
   dying MID-MIGRATION (the river #1118 non-atomic shape: a transaction
   that rolled back whole, no debris) must hand the lock to the rest, and
   a short lock wait must refuse bounded-and-loud (SystemExit) rather
   than hang past the startup probe. The leader election runs 8-way and
   yields exactly one winner; LISTEN arms 8 times with no drift and no
   residue.
2. THE INDEX MIGRATION. The newest plain (non-CONCURRENTLY) CREATE INDEX
   migrations run against a 1M-row pre-existing table in
   CI-representative conditions: the write-blocking lock window is
   measured, the queue-behind-any-reader hazard is proven bounded by
   ``ddl_lock_timeout`` (MigrationLockTimeoutError), and the
   maintenance-window contract the migration files document is pinned.
   The NULL-heavy legacy-data shape (river #63): the assignment_routed
   ADD COLUMN NOT NULL DEFAULT lands metadata-only (no table rewrite) on
   old rows, and the backfill drains every legacy NULL.
3. THE DOUBLE SHUTDOWN. ``orchestrate_shutdown`` called twice
   concurrently: both return 0, the second call sends nothing on the
   first call's closed leader conn (the river #400 use-after-close
   shape), and a third sequential call still returns 0.
4. THE ORPHANED SHAPE (dramatiq #441). A boot that fails AFTER the
   workers row is registered (the signal-mid-init window's orderly
   sibling: any startup raise between ``register_worker`` and the
   TaskGroup's deregister finally) must not leave the row behind fresh
   -heartbeated with no process behind it; and the raw-crash residue (a
   row whose process died with no cleanup at all) is reaped by the
   staleness sweep inside the documented bound.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import Counter
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_base62, new_uuid
from taskq.actor import actor
from taskq.exceptions import ActorConfigDriftList
from taskq.migrate import (
    MigrationLockTimeoutError,
    apply_pending,
    apply_pending_locked,
    discover,
    migration_lock_name,
    split_statements,
)
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.health import unique_health_sock_path
from taskq.worker.leader import build_leader_lease_sql, cleanup_stale_workers
from taskq.worker.notify import notify_listener_loop
from taskq.worker.run import register_worker
from taskq.worker.shutdown import orchestrate_shutdown

pytestmark = [pytest.mark.integration]

#: The fleet size every boot race runs at.
FLEET = 8


# ── helpers ──────────────────────────────────────────────────────────────


async def _fresh_schema(pg_dsn: str) -> str:
    """A unique schema label for one test."""
    return f"tle_{new_base62()}".lower()


async def _drop_schema(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


async def _apply_chain(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()


class _ApplierOutcome:
    """One boot-race applier's result, SystemExit recorded as its own kind.

    ``apply_pending_locked`` refuses contention with ``SystemExit`` (a
    BaseException): the race harness must record it, not crash the gather.
    """

    def __init__(self) -> None:
        self.applied: list[str] = []
        self.system_exit: str | None = None
        self.error: BaseException | None = None

    @property
    def kind(self) -> str:
        if self.system_exit is not None:
            return "system_exit"
        if self.error is not None:
            return "error"
        return "ok"


async def _run_applier(
    pg_dsn: str,
    schema: str,
    barrier: asyncio.Barrier,
    *,
    lock_timeout: float | None = None,
) -> _ApplierOutcome:
    outcome = _ApplierOutcome()
    try:
        conn = await asyncpg.connect(pg_dsn)
    except BaseException as exc:  # pragma: no cover - connect failures only
        outcome.error = exc
        return outcome
    try:
        await barrier.wait()
        kwargs: dict[str, Any] = {}
        if lock_timeout is not None:
            kwargs["lock_timeout"] = lock_timeout
        applied = await apply_pending_locked(
            conn=conn,
            schema=schema,
            phase=None,
            **kwargs,
        )
        outcome.applied = [m.key for m in applied]
    except SystemExit as exc:
        outcome.system_exit = str(exc)
    except BaseException as exc:  # Why: the harness records, never propagates.
        outcome.error = exc
    finally:
        with contextlib.suppress(Exception):
            await conn.close()
    return outcome


async def _boot_race(
    pg_dsn: str,
    schema: str,
    *,
    lock_timeout: float | None = None,
) -> list[_ApplierOutcome]:
    """Eight barrier-started appliers, each on its own connection."""
    barrier = asyncio.Barrier(FLEET)
    outcomes = await asyncio.gather(
        *(_run_applier(pg_dsn, schema, barrier, lock_timeout=lock_timeout) for _ in range(FLEET))
    )
    return list(outcomes)


def _assert_exactly_once(outcomes: list[_ApplierOutcome]) -> list[str]:
    """Every bundled migration applied exactly once across the winners."""
    ok = [o for o in outcomes if o.kind == "ok"]
    tally: Counter[str] = Counter()
    for outcome in ok:
        tally.update(outcome.applied)
    expected = [m.key for m in discover()]
    assert tally == Counter(expected), (
        f"the ledger partition is broken: {sorted(tally.items())} vs {len(expected)} bundled keys"
    )
    return expected


# ── attack 1a: the concurrent boot on a FRESH schema ────────────────────


async def test_boot_race_8_appliers_fresh_schema(pg_dsn: str) -> None:
    """Eight barrier-started appliers on a fresh empty schema.

    The advisory lock serializes them: no duplicate-key death, no
    double-apply, the union of the winners' returns is exactly the bundled
    chain, and the final ledger lists every key with the bundled checksum.
    """
    schema = await _fresh_schema(pg_dsn)
    try:
        outcomes = await _boot_race(pg_dsn, schema)
        kinds = Counter(o.kind for o in outcomes)
        assert kinds == {"ok": FLEET}, (
            f"a barrier-started boot race on a fresh schema must lose nobody: {kinds}; "
            f"first error: {outcomes[0].error or outcomes[0].system_exit}"
        )
        _assert_exactly_once(outcomes)

        conn = await asyncpg.connect(pg_dsn)
        try:
            rows = await conn.fetch(f'SELECT version, checksum FROM "{schema}".schema_migrations')
        finally:
            await conn.close()
        bundled = {m.key: m.checksum(schema) for m in discover()}
        assert {r["version"]: r["checksum"] for r in rows} == bundled
    finally:
        await _drop_schema(pg_dsn, schema)


async def test_boot_race_8_appliers_established_schema(pg_dsn: str) -> None:
    """Eight barrier-started appliers against an ESTABLISHED schema.

    The common rolling-deploy boot: every applier acquires the lock in
    turn, finds nothing pending, and applies nothing. No applier may
    re-run a recorded migration (a duplicate-key death here is the race
    made real).
    """
    schema = await _fresh_schema(pg_dsn)
    try:
        await _apply_chain(pg_dsn, schema)
        outcomes = await _boot_race(pg_dsn, schema)
        kinds = Counter(o.kind for o in outcomes)
        assert kinds == {"ok": FLEET}, f"an established-schema boot race must lose nobody: {kinds}"
        assert all(o.applied == [] for o in outcomes), (
            "an established schema owes every applier zero migrations"
        )
    finally:
        await _drop_schema(pg_dsn, schema)


async def test_boot_race_holder_dies_mid_migration_rest_proceed(pg_dsn: str) -> None:
    """The river #1118 non-atomic shape: one applier dies MID-MIGRATION.

    A holder takes the migration advisory lock, opens a transaction, and
    gets through part of the first migration's SQL; its backend is
    terminated. The transaction rolls back WHOLE (no half-applied schema
    objects, no ledger trace), the advisory lock dies with the session,
    and the eight waiting appliers proceed: exactly one winner applies the
    full chain, the rest find nothing pending. Nothing partial survives.
    """
    schema = await _fresh_schema(pg_dsn)
    try:
        conn = await asyncpg.connect(pg_dsn)
        first = discover()[0]
        statements = split_statements(first.render(schema))

        lock_name = migration_lock_name(schema)
        await conn.execute("SET lock_timeout = '5s'")
        await conn.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", lock_name)
        # The holder is mid-FILE: some statements applied inside its
        # transaction, the rest never reached.
        await conn.execute("BEGIN")
        for statement in statements[:2]:
            await conn.execute(statement)

        barrier = asyncio.Barrier(FLEET)
        tasks = [asyncio.create_task(_run_applier(pg_dsn, schema, barrier)) for _ in range(FLEET)]
        await asyncio.sleep(1.0)  # the fleet is queued on the advisory lock

        pid = await conn.fetchval("SELECT pg_backend_pid()")
        # A separate executioner: pg_terminate_backend on your own session
        # kills the connection mid-command, and the kill must be clean.
        executioner = await asyncpg.connect(pg_dsn)
        try:
            terminated = await executioner.fetchval("SELECT pg_terminate_backend($1)", pid)
            assert terminated is True
        finally:
            await executioner.close()
        with contextlib.suppress(Exception):
            await conn.close()

        outcomes = await asyncio.gather(*tasks)
        kinds = Counter(o.kind for o in outcomes)
        assert kinds == {"ok": FLEET}, (
            f"every waiting applier must proceed once the holder dies: {kinds}; "
            f"first failure: {outcomes[0].error or outcomes[0].system_exit}"
        )
        _assert_exactly_once(outcomes)

        # No debris from the killed holder's half-applied file: the ledger
        # records the FULL chain and nothing before it exists half-built.
        fresh = await asyncpg.connect(pg_dsn)
        try:
            rows = await fresh.fetch(
                f'SELECT version FROM "{schema}".schema_migrations ORDER BY version'
            )
            invalid = await fresh.fetch(
                """
                SELECT c.relname FROM pg_class c
                JOIN pg_index i ON i.indexrelid = c.oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND NOT i.indisvalid
                """,
                schema,
            )
        finally:
            await fresh.close()
        assert {r["version"] for r in rows} == {m.key for m in discover()}
        assert invalid == [], f"an interrupted migration left INVALID index debris: {invalid}"
    finally:
        await _drop_schema(pg_dsn, schema)


async def test_boot_race_short_lock_wait_refuses_bounded(pg_dsn: str) -> None:
    """A bounded lock wait refuses LOUD instead of hanging.

    With a holder parked on the migration lock, every applier given a
    short ``lock_timeout`` must exit with SystemExit naming the
    contention within the bound (the pre-deploy-job contract: migrations
    run once, from one surface, not from every replica), and the fleet
    must boot cleanly once the holder lets go.
    """
    schema = await _fresh_schema(pg_dsn)
    try:
        conn = await asyncpg.connect(pg_dsn)
        lock_name = migration_lock_name(schema)
        await conn.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", lock_name)

        started = time.monotonic()
        outcomes = await _boot_race(pg_dsn, schema, lock_timeout=1.0)
        elapsed = time.monotonic() - started
        assert all(o.kind == "system_exit" for o in outcomes), (
            f"a bounded waiter must refuse with SystemExit, got: "
            f"{Counter(o.kind for o in outcomes)}"
        )
        assert elapsed < 30.0, (
            f"the bounded refusal took {elapsed:.1f}s for an 8-deep queue at a 1s "
            "wait: the bound is per-acquire, not per-fleet, but it must not be "
            "unbounded in practice"
        )
        assert all("migration advisory lock" in (o.system_exit or "") for o in outcomes)

        await conn.execute("SELECT pg_advisory_unlock(hashtextextended($1, 0))", lock_name)
        await conn.close()

        # The deploy surface that won the race applies the chain cleanly.
        barrier = asyncio.Barrier(1)
        (solo,) = await asyncio.gather(_run_applier(pg_dsn, schema, barrier, lock_timeout=60.0))
        assert solo.kind == "ok", f"post-contention boot must succeed: {solo.error}"
        assert len(solo.applied) == len(discover())
    finally:
        await _drop_schema(pg_dsn, schema)


# ── attack 1b: the 8-way leader election ─────────────────────────────────


async def test_leader_election_8_way_single_winner(pg_dsn: str) -> None:
    """Eight pods race the elect statement: exactly one winner.

    The row alone decides the election in every state; a held lease means
    the elect's conflict WHERE clause returns zero rows for all seven
    losers, and the table still holds exactly one row naming the winner.
    """
    schema = await _fresh_schema(pg_dsn)
    try:
        await _apply_chain(pg_dsn, schema)
        elect_sql, _renew, _resign = build_leader_lease_sql(schema)
        worker_ids = [new_uuid() for _ in range(FLEET)]

        # The elect statement's FK: every pod's workers row must exist
        # (the real boot registers the worker before the leader runs).
        seed = await asyncpg.connect(pg_dsn)
        try:
            for worker_id in worker_ids:
                await seed.execute(
                    f'INSERT INTO "{schema}".workers'
                    " (id, hostname, pid, queues, last_seen_at)"
                    " VALUES ($1, 'tle-elect', 4242, $2, clock_timestamp())",
                    worker_id,
                    ["default"],
                )
        finally:
            await seed.close()

        barrier = asyncio.Barrier(FLEET)

        async def _racer(worker_id: object) -> object:
            conn = await asyncpg.connect(pg_dsn)
            try:
                await barrier.wait()
                return await conn.fetchval(elect_sql, worker_id, 30.0, 15.0)
            finally:
                await conn.close()

        results = await asyncio.gather(*(_racer(w) for w in worker_ids))
        winners = [w for w, r in zip(worker_ids, results, strict=True) if r is not None]
        assert len(winners) == 1, f"an 8-way election elected {len(winners)} leaders: {winners}"

        conn = await asyncpg.connect(pg_dsn)
        try:
            rows = await conn.fetch(f'SELECT worker_id FROM "{schema}".maintenance_leader')
        finally:
            await conn.close()
        assert len(rows) == 1
        assert rows[0]["worker_id"] == winners[0]
    finally:
        await _drop_schema(pg_dsn, schema)


# ── attack 1c: LISTEN arms 8 times, no drift, no residue ─────────────────


async def test_listen_arms_8_concurrent_no_drift_no_residue(pg_dsn: str) -> None:
    """Eight concurrent LISTEN arming on one schema: every subscriber
    woken exactly once per notify, and zero registry residue after every
    context exits (no drift: a stale registration can only mute or
    duplicate later wakes)."""
    from taskq.testing.fixtures import _open_pg_backend

    schema = f"tle_listen_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        worker_id = new_uuid()
        raw = await asyncpg.connect(pg_dsn)
        try:
            await raw.execute(
                f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, last_seen_at) '
                "VALUES ($1, 'tle-host', 4242, $2, clock_timestamp())",
                worker_id,
                ["default"],
            )
        finally:
            await raw.close()

        shutdown = asyncio.Event()
        listener = asyncio.create_task(
            notify_listener_loop(deps, backend, shutdown, worker_id),
            name="tle.notify",
        )

        barrier = asyncio.Barrier(FLEET)
        all_armed = asyncio.Event()
        release = asyncio.Event()

        async def _arm() -> asyncio.Event:
            async with backend.subscribe_wake(queues={"default"}) as event:
                await barrier.wait()
                all_armed.set()
                # Hold the arm open until the notify has fired and been
                # observed: a context that exits early discards its
                # registration, which is drift this test must not create.
                await release.wait()
                return event

        arm_tasks: list[asyncio.Task[asyncio.Event]] = []
        armed: list[asyncio.Event] = []
        try:
            arm_tasks = [asyncio.create_task(_arm()) for _ in range(FLEET)]

            async def _all_armed() -> bool:
                return len(backend._wake_subscribers) == FLEET  # pyright: ignore[reportPrivateUsage]

            await wait_for_condition(
                _all_armed, description="all 8 listeners armed concurrently", timeout=10.0
            )
            await all_armed.wait()
            # The events the subscribers registered: read from the
            # backend's registry (the tasks are still parked inside their
            # contexts, results are not ready yet).
            armed = list(backend._wake_subscribers)  # pyright: ignore[reportPrivateUsage]

            # One committed enqueue fires the trigger once; every armed
            # LISTEN must deliver the wake to its own subscriber.
            raw = await asyncpg.connect(pg_dsn)
            try:
                await raw.execute(
                    f'INSERT INTO "{schema}".jobs'
                    " (id, actor, queue, payload, max_attempts, retry_kind)"
                    " VALUES ($1, 'test_actor', 'default', '{}', 3, 'transient')",
                    new_uuid(),
                )
            finally:
                await raw.close()

            async def _all_woken() -> bool:
                return all(e.is_set() for e in armed)

            await wait_for_condition(
                _all_woken, description="all 8 armed listeners woken", timeout=10.0
            )
        finally:
            release.set()
            results = await asyncio.gather(*arm_tasks, return_exceptions=True)
            assert not any(isinstance(r, BaseException) for r in results), (
                f"an armer task died holding its registration: {results}"
            )
            shutdown.set()
            listener.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener

        # No drift: every registration left with its context.
        assert len(backend._wake_subscribers) == 0  # pyright: ignore[reportPrivateUsage]  # Why: the residue IS the assertion; the registry is the backend's private arm-state.
        assert len(backend._wake_queues) == 0  # pyright: ignore[reportPrivateUsage]
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)


# ── attack 2: the index migration against 1M rows ────────────────────────

_ROW_TARGET = 1_000_000


async def _bulk_load_jobs(pg_dsn: str, schema: str, rows: int) -> None:
    """Load ``rows`` jobs in one statement: mostly terminal/pending
    history, NULL-heavy finished_at on the legacy share (the river #63
    shape), a small live running population (what the partial
    fence-probe index costs)."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute("SET synchronous_commit = off")
        await conn.execute(
            f"""
            INSERT INTO "{schema}".jobs
                (id, actor, queue, payload, max_attempts, retry_kind, status,
                 started_at, finished_at, locked_by_worker)
            SELECT
                gen_random_uuid(), 'test_actor', 'default', '{{}}'::jsonb, 3, 'transient',
                (CASE WHEN i % 100 = 0 THEN 'running' ELSE 'succeeded' END)
                    ::"{schema}".job_status,
                CASE WHEN i % 100 = 0 THEN NULL ELSE clock_timestamp() - interval '2 days' END,
                CASE WHEN i % 100 = 0 THEN NULL
                     WHEN i % 7 = 0 THEN NULL
                     ELSE clock_timestamp() - interval '1 day' END,
                CASE WHEN i % 100 = 0 THEN gen_random_uuid() ELSE NULL END
            FROM generate_series(1, $1) AS i
            """,
            rows,
        )
    finally:
        await conn.close()


async def test_newest_index_migrations_lock_window_at_1m_rows(pg_dsn: str) -> None:
    """The newest plain CREATE INDEX migrations vs a 1M-row table.

    Measures the write-blocking lock window (the migration files' OPS
    NOTE names it) and pins the bounded-refusal contract: the DDL runs
    transactional, so it queues behind ANY reader of jobs, and
    ``ddl_lock_timeout`` converts an unbounded park into
    LockNotAvailableError (MigrationLockTimeoutError on the wrapper) with
    nothing applied. The maintenance-window contract (run the
    CONCURRENTLY equivalent by hand for large tables) is documented in
    the files; what is pinned here is that the runner's bound is real
    and the build itself completes at 1M rows in bounded time.
    """
    schema = await _fresh_schema(pg_dsn)
    try:
        conn = await asyncpg.connect(pg_dsn)
        try:
            # Stop the chain BEFORE 01.00.17: the migrations under test
            # must be PENDING, not already applied by the full chain.
            await apply_pending(conn, schema=schema, target="01.00.16_01")
            await _bulk_load_jobs(pg_dsn, schema, _ROW_TARGET)

            count = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs')
            assert count == _ROW_TARGET

            # 01.00.17_01 rebuilds jobs_finished_at_idx as the composite
            # (status, finished_at) INCLUDE (id): a plain transactional
            # CREATE INDEX over the whole table.
            t0 = time.monotonic()
            applied = await apply_pending(conn, schema=schema, target="01.00.17_01")
            build_window = time.monotonic() - t0
            assert [m.key for m in applied] == ["01.00.17_01:pre"]
            assert build_window < 300.0, (
                f"the composite index build held its write-blocking lock for "
                f"{build_window:.1f}s at 1M rows - re-pin the documented "
                "maintenance-window contract or move the build to CONCURRENTLY"
            )

            # The bounded-refusal half of the contract: a reader holding
            # ACCESS SHARE parks the next DDL, and ddl_lock_timeout
            # converts the park into a lock-timeout failure with the
            # migration rolled back (nothing applied).
            reader = await asyncpg.connect(pg_dsn)
            try:
                await reader.execute("BEGIN")
                await reader.execute(f'SELECT 1 FROM "{schema}".jobs LIMIT 1')
                t1 = time.monotonic()
                with pytest.raises(MigrationLockTimeoutError):
                    await apply_pending(
                        conn,
                        schema=schema,
                        target="01.00.19_01",
                        ddl_lock_timeout=1.0,
                    )
                waited = time.monotonic() - t1
                assert waited < 10.0, (
                    f"the bounded DDL waited {waited:.1f}s against a 1.0s lock_timeout"
                )
                still_applied = await conn.fetch(
                    f'SELECT version FROM "{schema}".schema_migrations '
                    "WHERE version = '01.00.19_01:pre'"
                )
                assert still_applied == [], "a timed-out DDL must roll back to nothing"
            finally:
                with contextlib.suppress(Exception):
                    await reader.execute("ROLLBACK")
                with contextlib.suppress(Exception):
                    await reader.close()

            # Without the reader, the remaining chain (the partial
            # fence-probe index among them) applies and is VALID. The
            # timed-out run left 01.00.18_01 applied (its DDL matched no
            # locked table) and 01.00.18_02 pending (its ALTER was the
            # one that timed out): the resume applies the difference.
            all_keys = [m.key for m in discover()]
            resume_from = all_keys.index("01.00.18_01:pre") + 1
            t2 = time.monotonic()
            rest = await apply_pending(conn, schema=schema)
            rest_window = time.monotonic() - t2
            assert [m.key for m in rest] == all_keys[resume_from:]
            assert rest_window < 120.0, (
                f"the partial fence-probe build took {rest_window:.1f}s at 1M rows: "
                "its build cost must track the RUNNING population, not the table"
            )
            invalid = await conn.fetch(
                """
                SELECT c.relname FROM pg_class c
                JOIN pg_index i ON i.indexrelid = c.oid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND NOT i.indisvalid
                """,
                schema,
            )
            assert invalid == [], f"a 1M-row build left INVALID indexes: {invalid}"
        finally:
            await conn.close()
    finally:
        await _drop_schema(pg_dsn, schema)


async def test_migration_over_null_heavy_legacy_data_no_rewrite(pg_dsn: str) -> None:
    """The river #63 shape: the new NOT NULL/DEFAULT columns over old rows.

    1M legacy rows predate the assignment_routed column. The ADD COLUMN
    ... NOT NULL DEFAULT must land metadata-only (PG11+: no table
    rewrite, the relation's physical file unchanged), and the backfill
    migration must drain every legacy unrouted row so dispatch routes
    cleanly afterwards.
    """
    schema = await _fresh_schema(pg_dsn)
    try:
        conn = await asyncpg.connect(pg_dsn)
        try:
            # Stop the chain BEFORE the assignment_routed columns exist.
            await apply_pending(conn, schema=schema, target="01.00.12_04")
            await _bulk_load_jobs(pg_dsn, schema, _ROW_TARGET)

            relfilenode_before = await conn.fetchval(
                """
                SELECT relfilenode FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = 'jobs'
                """,
                schema,
            )

            # _05 adds the columns, _06 builds the probe indexes, _07
            # backfills the legacy rows: the river #63 sequence in one run.
            applied = await apply_pending(conn, schema=schema, target="01.00.12_07")
            assert [m.key for m in applied] == [
                "01.00.12_05:pre",
                "01.00.12_06:pre",
                "01.00.12_07:pre",
            ]

            relfilenode_after = await conn.fetchval(
                """
                SELECT relfilenode FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = 'jobs'
                """,
                schema,
            )
            assert relfilenode_after == relfilenode_before, (
                f"the ADD COLUMN rewrote the table (relfilenode {relfilenode_before} -> "
                f"{relfilenode_after} at 1M rows): the metadata-only "
                "contract (PG11+ constant default) is broken and every "
                "legacy deployment will pay a full rewrite under lock"
            )

            # The backfill drained the legacy population: no unrouted
            # legacy row survives (running rows were mid-flight at the
            # upgrade and are the reclaim sweep's business, the backfill's
            # WHERE clause excludes them by started_at).
            stranded = await conn.fetchval(
                f'''
                SELECT count(*) FROM "{schema}".jobs
                WHERE status IN ('pending', 'scheduled')
                  AND started_at IS NOT NULL
                  AND assignment_routed = false
                '''
            )
            assert stranded == 0, f"{stranded} legacy rows never routed after the backfill"
            nulls = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs WHERE assignment_routed IS NULL'
            )
            assert nulls == 0, "NOT NULL over legacy rows left NULLs somehow"
        finally:
            await conn.close()
    finally:
        await _drop_schema(pg_dsn, schema)


# ── attack 3: the double shutdown ────────────────────────────────────────


async def test_orchestrate_shutdown_called_twice_concurrently(pg_dsn: str) -> None:
    """stop() called twice at once (double SIGTERM, drain monitor racing
    the signal): both orchestrations return 0, neither sends on the other's
    closed leader conn, and a THIRD sequential call still returns 0. The
    use-after-close shape (river #400) dies here or nowhere."""
    from taskq.testing.fixtures import _open_pg_backend

    schema = f"tle_dbl_{new_base62()}".lower()
    stack, deps, backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        worker_id = new_uuid()
        shutdown_event = asyncio.Event()
        escalate_event = asyncio.Event()

        results = await asyncio.gather(
            orchestrate_shutdown(
                deps, deps.settings, worker_id, shutdown_event, escalate_event, backend=backend
            ),
            orchestrate_shutdown(
                deps, deps.settings, worker_id, shutdown_event, escalate_event, backend=backend
            ),
            return_exceptions=True,
        )
        for i, result in enumerate(results):
            assert not isinstance(result, BaseException), (
                f"orchestration #{i} raised under double call: {result!r}"
            )
        assert results == [0, 0]
        assert deps.leader_conn is None, (
            "the double call must still end with the owned leader conn closed and nulled"
        )

        # A third call after everything is closed: the guarded no-op path.
        third = await orchestrate_shutdown(
            deps, deps.settings, worker_id, shutdown_event, escalate_event, backend=backend
        )
        assert third == 0
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)


# ── attack 4: the orphaned shape (dramatiq #441) ─────────────────────────


class _DriftPayload(BaseModel):
    x: int = 0


async def _boot_and_fail(settings: WorkerSettings, ref: object) -> int:
    """Run _main far enough to hit the drift refusal."""
    from taskq.worker._bootstrap import _main

    return await _main(
        settings,
        actor_registry={"orphan_actor": ref},  # type: ignore[dict-item]
    )


async def test_boot_failure_after_registration_leaves_no_orphaned_worker_row(
    pg_dsn: str,
) -> None:
    """A boot that fails AFTER ``register_worker`` must not strand the row.

    The signal-mid-init window's orderly sibling: any raise between the
    workers INSERT and the TaskGroup's deregister finally (here, an actor
    -config drift refusal) leaves the row fresh-heartbeated with no
    process behind it until the staleness sweep reaps it a whole grace
    period later. The boot-failure backstop (deps._exit_stack teardown)
    deletes it while the dispatcher pool is still open.
    """
    schema = f"tle_orphan_{new_base62()}".lower()
    try:
        await _apply_chain(pg_dsn, schema)
        # Seed the operator-owned row a boot will then disagree with.
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(
                f'INSERT INTO "{schema}".actor_config'
                " (actor, max_concurrent, max_pending, queue, metadata)"
                " VALUES ('orphan_actor', 1, NULL, 'default',"
                ' \'{"seeded": "operator"}\')',
            )
        finally:
            await conn.close()

        @actor(name="orphan_actor", metadata={"declared": "code"})  # type: ignore[call-overload]
        async def orphan_actor(payload: _DriftPayload) -> None: ...

        settings = WorkerSettings.load_from_dict(
            {
                "pg_dsn": pg_dsn,
                "schema_name": schema,
                "health_socket_path": unique_health_sock_path("tle_orphan"),
            }
        )

        with pytest.raises(ActorConfigDriftList):
            await _boot_and_fail(settings, orphan_actor)

        conn = await asyncpg.connect(pg_dsn)
        try:
            rows = await conn.fetch(f'SELECT id FROM "{schema}".workers')
        finally:
            await conn.close()
        assert rows == [], (
            f"the failed boot stranded {len(rows)} worker row(s): a boot that dies "
            "after registering must deregister on the way out, not wait for the "
            "staleness sweep"
        )
    finally:
        await _drop_schema(pg_dsn, schema)


async def test_crash_orphan_worker_row_reaped_by_staleness_sweep(pg_dsn: str) -> None:
    """The raw-crash residue (SIGKILL mid-anything): a dead worker's row
    is reaped by the stale-worker sweep within the documented bound, on
    identity (the caller's own row is never deleted)."""
    from taskq.testing.fixtures import _open_pg_backend

    schema = f"tle_reap_{new_base62()}".lower()
    stack, deps, _backend = await _open_pg_backend(pg_dsn, schema_name=schema)
    try:
        # register_worker through the real boot path: the row the crash
        # leaves behind looks exactly like this.
        orphan_id = await register_worker(deps.dispatcher_pool, deps.settings)

        conn = await asyncpg.connect(pg_dsn)
        try:
            # Backdate the heartbeat: the process died at t0.
            await conn.execute(
                f'UPDATE "{schema}".workers '
                "SET last_seen_at = clock_timestamp() - interval '1 hour' "
                "WHERE id = $1",
                orphan_id,
            )
            deleted = await cleanup_stale_workers(
                conn,
                worker_id=new_uuid(),  # the sweeping leader is a different pod
                staleness=timedelta(
                    seconds=deps.settings.heartbeat_interval
                    * (deps.settings.max_heartbeat_failures + 3)
                ),
                schema=schema,
            )
            remaining = await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".workers WHERE id = $1', orphan_id
            )
        finally:
            await conn.close()
        assert deleted == 1
        assert remaining == 0, "the crash-orphaned row outlived the staleness sweep"
    finally:
        await stack.aclose()
        await _drop_schema(pg_dsn, schema)
