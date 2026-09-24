"""A live migration under a running fleet must degrade, never crash-loop.

The zero-downtime deploy story: ops runs ``taskq migrate up`` (or any DDL)
against the tables WHILE workers and admins hold pools of cached prepared
statements against them. asyncpg caches prepared statements per connection;
a migration that changes what those statements depend on makes the server
reject the next execution of the cached plan. What happens next is the
whole question: a graceful degradation the pool heals by itself, or every
pooled connection failing until each is recycled.

The shapes were measured against PostgreSQL 18.6 with a warm pool using
TaskQ's shipped statement-cache tuning (``statement_cache_size=512``,
``max_cached_statement_lifetime=3600``):

* **ADD COLUMN (the safe migration) rides through.** Cached plans over the
  old column set stay valid (fast defaults, no plan invalidation), and an
  old-shape INSERT that omits the new NOT NULL DEFAULT column reads the
  default back invisibly. No error of any class reaches the worker.
* **A result-type change (``ALTER COLUMN TYPE``, a ``DROP COLUMN`` under
  ``SELECT *``) surfaces as ``InvalidCachedStatementError`` (SQLSTATE
  0A000) from statements running INSIDE a transaction**, where asyncpg
  cannot retry invisibly. Outside a transaction the driver retries once
  and clears the pool-wide statement cache, so autocommit statements never
  see it. The next tick re-prepares against the new schema and succeeds
  with no pool recycle: this family is infrastructure, retryable.
* **A DROP/RENAME of a column old code still names re-prepares into
  ``UndefinedColumnError`` (42703) on EVERY subsequent execution, forever.**
  The SQL text itself is now wrong, not the cache. That is the
  expansion-then-contract contract being violated, and it must stay loud
  and fatal, never classified transient (a transient classification would
  livelock a fleet whose SQL can never succeed again).
* **The migration's own lock queue freezes the fleet.** An ``ALTER TABLE``
  queues ACCESS EXCLUSIVE behind any open job transaction, and once
  queued, EVERY later statement on that table queues behind the DDL - the
  whole fleet freezes behind one migration waiting on one job. TaskQ's
  runner bounds the wait (``DEFAULT_MIGRATION_DDL_LOCK_TIMEOUT``, 30s) so
  the freeze window is bounded and the DDL gives up (55P03) instead of
  parking the fleet for the length of the longest job.

These tests pin those shapes against real Postgres. What each protects:
the classifier must keep the 0A000 family transient (loop liveness) while
42703 stays fatal (the contract's loudness), the pool must heal without a
recycle, in-flight jobs must conserve across the DDL, and the freeze
window must be bounded by the DDL's lock_timeout and not by the longest
job on the fleet.
"""

from __future__ import annotations

import asyncio
import time

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.migrate import apply_pending
from taskq.testing.pg import create_worker
from taskq.worker._transient import is_transient_pg_error
from tests._fleet import FleetPayload, fleet_actor_config, open_fleet

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_ACTOR = "live_migration_actor"
_QUEUE = "live_migration_q"
_ACTOR_CONFIG = fleet_actor_config()


def _worker_pool() -> dict[str, object]:
    """create_pool kwargs carrying TaskQ's shipped statement-cache tuning."""
    return {
        "min_size": 1,
        "max_size": 4,
        "statement_cache_size": 512,
        "max_cached_statement_lifetime": 3600,
    }


def _sqlstate(exc: BaseException) -> str | None:
    return getattr(exc, "sqlstate", None)


async def _migrated_schema(pg_dsn: str, prefix: str) -> str:
    """A fresh schema with the full bundled migration set applied."""
    schema = f"{prefix}_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    return schema


async def _drop_schema(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


# ── attack 2 + 3: ADD COLUMN, the safe migration ────────────────────────


async def test_add_column_not_null_default_rides_through_running_pool(
    pg_dsn: str,
) -> None:
    """PIN: an ADD COLUMN ... NOT NULL DEFAULT, applied while a pool with
    fully cached statements keeps running, is invisible to every old
    statement: cached SELECTs, in-transaction statements, and old-shape
    INSERTs that omit the new column (PG's fast default fills it, no
    rewrite, no plan invalidation, no 0A000). This is the migration
    shape the pre-deploy job actually runs, and the rolling-deploy pins
    claim it is safe: here is that claim under a live pool."""
    schema = await _migrated_schema(pg_dsn, "atk_add_col")
    pool = await asyncpg.create_pool(pg_dsn, **_worker_pool())  # pyright: ignore[reportCallIssue]
    ddl = await asyncpg.connect(pg_dsn)
    try:
        jobs = f'"{schema}".jobs'

        # Warm the pool the way a running worker is warm: a dispatch-shaped
        # SELECT (cached), and one old-shape INSERT whose column list
        # predates the new column.
        await pool.execute(f"SELECT id, status FROM {jobs} WHERE queue = $1", _QUEUE)
        await pool.execute(
            f"""INSERT INTO {jobs}
                (id, actor, queue, payload, max_attempts, retry_kind, scheduled_at)
                VALUES ($1, $2, $3, '{{}}'::jsonb, 1, 'transient', now())""",
            "11111111-1111-1111-1111-111111111111",
            _ACTOR,
            _QUEUE,
        )

        # The migration lands, from a second connection, under the warm pool.
        await ddl.execute(
            f"ALTER TABLE {jobs} ADD COLUMN attack_probe text NOT NULL DEFAULT 'x'"
        )

        # The old cached SELECT rides through on its stale plan.
        await pool.execute(f"SELECT id, status FROM {jobs} WHERE queue = $1", _QUEUE)

        # The old-shape INSERT rides through and the fast default is
        # INVISIBLE to it: the row reads back carrying the column's
        # default without the INSERT ever naming the column.
        row = await pool.fetchrow(
            f"""INSERT INTO {jobs}
                (id, actor, queue, payload, max_attempts, retry_kind, scheduled_at)
                VALUES ($1, $2, $3, '{{}}'::jsonb, 1, 'transient', now())
                RETURNING attack_probe""",
            "22222222-2222-2222-2222-222222222222",
            _ACTOR,
            _QUEUE,
        )
        assert row is not None and row["attack_probe"] == "x", (
            f"the NOT NULL DEFAULT was not filled behind the old-shape INSERT: "
            f"got {row and row['attack_probe']!r}"
        )

        # A statement that starts and runs INSIDE a transaction, the shape
        # the per-slot pool and the terminal write use, rides through too.
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(f"SELECT count(*) FROM {jobs}")
                await conn.execute(f"SELECT id FROM {jobs} WHERE queue = $1", _QUEUE)
    finally:
        await pool.close()
        await ddl.close()
        await _drop_schema(pg_dsn, schema)


# ── attack 1a: the 0A000 cached-plan family is infra-retryable ──────────


async def test_result_type_change_in_txn_surfaces_transient_then_heals(
    pg_dsn: str,
) -> None:
    """PIN: when a live DDL changes a cached statement's result type, the
    in-transaction execution raises ``InvalidCachedStatementError``
    (0A000) - and TaskQ must classify it as TRANSIENT infrastructure,
    because the pool heals by itself on the next tick with no recycle:
    asyncpg has already cleared the pool-wide statement cache by the time
    the error propagates. A regression to "unexpected error" here feeds
    the loop budget and kills the worker mid-migration: the crash-loop
    disposition the rolling-deploy story cannot afford."""
    schema = await _migrated_schema(pg_dsn, "atk_0a000")
    pool = await asyncpg.create_pool(pg_dsn, **_worker_pool())  # pyright: ignore[reportCallIssue]
    ddl = await asyncpg.connect(pg_dsn)
    try:
        jobs = f'"{schema}".jobs'

        # Cache a statement whose result row type the DDL will change.
        await pool.execute(f"SELECT * FROM {jobs} WHERE queue = $1", _QUEUE)

        # Result-type change, twice over: a type change and a column drop.
        await ddl.execute(f"ALTER TABLE {jobs} ALTER COLUMN priority TYPE integer")
        await ddl.execute(f"ALTER TABLE {jobs} DROP COLUMN tags")

        # The in-transaction shape (no invisible asyncpg retry available):
        # the cached plan is rejected with 0A000 and the transaction aborts.
        raised: BaseException | None = None
        async with pool.acquire() as conn:
            async with conn.transaction():
                try:
                    await conn.execute(f"SELECT * FROM {jobs} WHERE queue = $1", _QUEUE)
                except asyncpg.PostgresError as exc:
                    raised = exc
        assert raised is not None, (
            "the in-transaction execution of a cached statement whose result "
            "type was just altered by live DDL did not fail: the attack shape "
            "is gone, this test pins nothing"
        )
        assert isinstance(raised, asyncpg.InvalidCachedStatementError), (
            f"expected the cached-plan rejection (0A000), got "
            f"{type(raised).__name__} (sqlstate={_sqlstate(raised)}): the driver's "
            "recovery contract moved, re-derive the classification before touching it"
        )
        assert _sqlstate(raised) == "0A000"
        assert is_transient_pg_error(raised, pooled=False), (
            "InvalidCachedStatementError is classified non-transient: a live "
            "migration under a running fleet would feed the unexpected-error "
            "budget and crash-loop every worker instead of retrying next tick"
        )

        # The heal: the NEXT tick, same pool, no recycle. asyncpg cleared
        # the pool-wide statement cache when the error surfaced; a fresh
        # prepare against the new schema just works.
        await pool.execute(f"SELECT * FROM {jobs} WHERE queue = $1", _QUEUE)
        await pool.execute(f"SELECT count(*) FROM {jobs}")
    finally:
        await pool.close()
        await ddl.close()
        await _drop_schema(pg_dsn, schema)


# ── attack 1b: the contract violation stays loud ────────────────────────


async def test_drop_referenced_column_is_loud_and_never_transient(
    pg_dsn: str,
) -> None:
    """PIN: a migration that drops (or renames) a column the RUNNING code
    still references makes every execution of those statements fail with
    ``UndefinedColumnError`` (42703) - permanently, on every connection,
    because the SQL text itself no longer parses against the schema. This
    is the expansion-then-contract contract being violated (the post
    phase applied while old pods still serve), and it must stay LOUD:
    42703 is never transient, it feeds the unexpected-error budget and
    kills the worker deliberately. What must NOT break: unrelated
    statements on the same pool keep working, the pool is not poisoned."""
    schema = await _migrated_schema(pg_dsn, "atk_drop")
    pool = await asyncpg.create_pool(pg_dsn, **_worker_pool())  # pyright: ignore[reportCallIssue]
    ddl = await asyncpg.connect(pg_dsn)
    try:
        jobs = f'"{schema}".jobs'

        await ddl.execute(f"ALTER TABLE {jobs} ADD COLUMN attack_probe text")
        # Cache a statement that references the column, old-code style.
        await pool.execute(f"SELECT attack_probe FROM {jobs} LIMIT 1")

        await ddl.execute(f"ALTER TABLE {jobs} DROP COLUMN attack_probe")

        # Every execution of the broken text fails, repeatedly: not a
        # stale cache, a contract violation.
        for attempt in (1, 2, 3):
            with pytest.raises(asyncpg.UndefinedColumnError) as exc_info:
                await pool.execute(f"SELECT attack_probe FROM {jobs} LIMIT 1")
            assert _sqlstate(exc_info.value) == "42703", (
                f"attempt {attempt}: expected 42703, got {_sqlstate(exc_info.value)}"
            )
        assert not is_transient_pg_error(
            asyncpg.UndefinedColumnError("x"), pooled=False
        ), (
            "UndefinedColumnError is classified transient: a migration that "
            "dropped a column old code still names would livelock the fleet "
            "retrying statements that can never succeed again"
        )

        # The pool is not poisoned: unrelated statements keep working.
        await pool.execute(f"SELECT id FROM {jobs} LIMIT 1")
        await pool.execute(f"SELECT count(*) FROM {jobs}")
    finally:
        await pool.close()
        await ddl.close()
        await _drop_schema(pg_dsn, schema)


# ── the fleet disposition: work conserves across a live DDL ─────────────


async def test_fleet_works_through_a_live_schema_change(pg_dsn: str) -> None:
    """PIN: a pod dispatching in a loop when the DDL lands keeps claiming
    and completing work: in-flight and following jobs conserve, none
    strand, the pod needs no restart. The 0A000 blip (if any statement
    hits it in-transaction) costs a tick, not the fleet."""
    schema = await _migrated_schema(pg_dsn, "atk_fleet")
    conn = await asyncpg.connect(pg_dsn)
    try:
        await create_worker(conn, schema, new_uuid())
    finally:
        await conn.close()

    try:
        async with open_fleet(
            pg_dsn,
            schema=schema,
            pods=("mid-migration",),
            actors=((_ACTOR, _QUEUE),),
            migrate=False,
        ) as fleet:
            job_ids = await fleet.enqueue(6, actor=_ACTOR, queue=_QUEUE)

            async def _work(_payload: FleetPayload, _ctx: object) -> str:
                # Long enough that the DDL lands while a job is in flight.
                await asyncio.sleep(0.3)
                return "done"

            async def _dispatch_loop() -> int:
                completed = 0
                remaining = set(job_ids)
                for _round in range(400):
                    claimed = await fleet.pod("mid-migration").claim([_QUEUE], 2)
                    for job in claimed:
                        await fleet.pod("mid-migration").run(
                            job, _work, actor_config=_ACTOR_CONFIG
                        )
                        completed += 1
                        remaining.discard(job.id)
                    if not remaining:
                        return completed
                    await asyncio.sleep(0.05)
                return completed

            # The DDL lands a beat after the loop starts claiming.
            async def _migrate() -> None:
                await asyncio.sleep(0.4)
                ddl = await asyncpg.connect(pg_dsn)
                try:
                    await ddl.execute(
                        f'ALTER TABLE "{schema}".jobs '
                        "ADD COLUMN attack_probe text NOT NULL DEFAULT 'x'"
                    )
                    await ddl.execute(
                        f'ALTER TABLE "{schema}".jobs DROP COLUMN attack_probe'
                    )
                finally:
                    await ddl.close()

            loop_task = asyncio.create_task(_dispatch_loop())
            migrate_task = asyncio.create_task(_migrate())
            await migrate_task
            completed = await asyncio.wait_for(loop_task, timeout=60)

            assert completed == len(job_ids), (
                f"{completed} of {len(job_ids)} jobs completed across a live ADD "
                "COLUMN + DROP COLUMN: a schema change under a running fleet "
                "stranded work"
            )
            statuses = [
                await fleet.fetch(f'SELECT status FROM "{schema}".jobs WHERE id = $1', jid)
                for jid in job_ids
            ]
            flat = [str(r[0]["status"]) for r in statuses if r]
            assert all(s == "succeeded" for s in flat), (
                f"job statuses after the live DDL: {flat}"
            )
    finally:
        await _drop_schema(pg_dsn, schema)


# ── attack 4: the migration's own lock queue ────────────────────────────


async def test_migration_lock_queue_freeze_is_bounded_by_lock_timeout(
    pg_dsn: str,
) -> None:
    """PIN: an ``ALTER TABLE`` queued behind a long-running job transaction
    parks every later statement on the table behind itself (Postgres'
    lock-queue fairness): the whole fleet freezes behind one migration
    waiting on one job. The runner's ``lock_timeout`` discipline is the
    only defense: with a bound, the DDL gives up (55P03) at the bound and
    the freeze ends with it; unbounded, the freeze window is the length
    of the longest open job transaction. Measured here at realistic
    settings: a 4s job hold, a 1.5s DDL bound, hot traffic throughout."""
    schema = await _migrated_schema(pg_dsn, "atk_freeze")
    pool = await asyncpg.create_pool(pg_dsn, **_worker_pool())  # pyright: ignore[reportCallIssue]
    ddl = await asyncpg.connect(pg_dsn)
    jobs = f'"{schema}".jobs'
    try:
        await pool.execute(f"SELECT 1 FROM {jobs} LIMIT 1")

        stats = {"n": 0, "frozen": 0, "worst": 0.0}
        stop = asyncio.Event()

        async def _hot_traffic() -> None:
            while not stop.is_set():
                t0 = time.monotonic()
                await pool.execute(f"SELECT 1 FROM {jobs} LIMIT 1")
                dt = time.monotonic() - t0
                stats["n"] += 1
                if dt > 0.5:
                    stats["frozen"] += 1
                    stats["worst"] = max(stats["worst"], dt)
                await asyncio.sleep(0.05)

        async def _job_transaction(hold: float) -> None:
            """A job attempt mid-flight: an open transaction holding row
            locks (ROW EXCLUSIVE on the table) for *hold* seconds."""
            txn = await asyncpg.connect(pg_dsn)
            try:
                async with txn.transaction():
                    await txn.execute(
                        f"UPDATE {jobs} SET status = 'running' WHERE id = $1",
                        "11111111-1111-1111-1111-111111111111",
                    )
                    await txn.execute("SELECT pg_sleep($1)", hold)
            finally:
                await txn.close()

        hot = asyncio.create_task(_hot_traffic())
        try:
            # Unbounded DDL: the freeze window IS the job's lock hold. The
            # queued ACCESS EXCLUSIVE parks every later SELECT behind it.
            job = asyncio.create_task(_job_transaction(4.0))
            await asyncio.sleep(0.5)
            t0 = time.monotonic()
            await ddl.execute(
                f"ALTER TABLE {jobs} ADD COLUMN attack_probe text NOT NULL DEFAULT 'x'"
            )
            unbounded_wait = time.monotonic() - t0
            await job
            # The hot query blocked inside the freeze window records its
            # elapsed time only when the loop resumes it; yield a beat so
            # the measurement lands before it is asserted on.
            await asyncio.sleep(0.2)
            assert unbounded_wait >= 1.0, (
                f"the unbounded ALTER waited only {unbounded_wait:.2f}s behind a job "
                "transaction that held the table for 4s: the lock-queue shape this "
                "test pins did not occur (the job txn took no table lock?)"
            )
            assert stats["n"] > 10, (
                f"the hot-traffic task only ran {stats['n']} queries: it is not "
                "exercising the table while the DDL queues"
            )
            assert stats["worst"] >= 1.0, (
                f"hot traffic never froze (worst {stats['worst']:.2f}s over "
                f"{stats['n']} queries) while the unbounded ALTER was queued behind "
                "the job transaction: the fleet-freeze shape did not occur"
            )

            await ddl.execute(f"ALTER TABLE {jobs} DROP COLUMN attack_probe")
            stats["worst"] = 0.0

            # The runner's discipline: a bounded wait. The DDL gives up at
            # the bound and the freeze ends with it, instead of the fleet
            # freezing for the length of the longest job.
            job = asyncio.create_task(_job_transaction(4.0))
            await asyncio.sleep(0.5)
            t0 = time.monotonic()
            with pytest.raises(asyncpg.LockNotAvailableError) as exc_info:
                async with ddl.transaction():
                    await ddl.execute("SET LOCAL lock_timeout = 1500")
                    await ddl.execute(
                        f"ALTER TABLE {jobs} ADD COLUMN attack_probe text NOT NULL DEFAULT 'x'"
                    )
            bounded_elapsed = time.monotonic() - t0
            assert _sqlstate(exc_info.value) == "55P03"
            assert bounded_elapsed < 2.5, (
                f"the bounded DDL took {bounded_elapsed:.2f}s to give up; the "
                "lock_timeout bound did not govern the wait"
            )
            await job
            await asyncio.sleep(0.2)
            assert stats["worst"] < unbounded_wait, (
                f"hot traffic froze {stats['worst']:.2f}s under the bounded DDL, at "
                f"least as long as the unbounded freeze ({unbounded_wait:.2f}s): the "
                "lock_timeout bound did not bound the fleet's freeze window"
            )
        finally:
            stop.set()
            await hot
    finally:
        await pool.close()
        await ddl.close()
        await _drop_schema(pg_dsn, schema)
