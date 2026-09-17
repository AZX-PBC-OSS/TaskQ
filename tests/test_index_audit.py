"""Index-audit pins for the bounded maintenance/cancel paths (01.00.06_01).

Three pin families, the first two grounded in the SQL/index audit that
produced migration 01.00.06_01_pre_cancel_and_cascade_indexes.sql and
the statement_timestamp()/ORDER BY sweep-bound rewrite in
taskq.backend._sweeps / taskq.worker.cron_loop:

1. Migration pins — the three audit indexes exist, are VALID, the
   ledger records the file as transactional (plain CREATE INDEX by
   deliberate choice: the CONCURRENTLY form deadlocks with the
   migration advisory lock — see the migration header), and a re-run
   is a clean no-op.
2. Plan pins — bulk-seeded (the same discipline as
   test_postgres_max_pending.py: plans are only stable at realistic
   volumes), each audited statement's EXPLAIN names the index it is
   supposed to be served by, and the sweep bounds appear as Index
   Conds (the property that makes them terminate at the range boundary
   instead of filtering a whole-table walk). The seeds hold the
   steady-state shape — nothing eligible yet, realistic populations in
   every partial index — because that is the every-second/every-30s
   tick the audit found to be the costly one.
3. Prune/archive selection-bound pins — the same STABLE-bound property
   for the once-a-day prune/archive/expiry candidate CTEs in
   taskq.worker._leader_shared (their VOLATILE clock_timestamp()
   selection bounds survived the first audit pass as a documented
   follow-up): the drained steady state AND the eligible backlog, the
   ad-hoc statement AND the server-prepared form a long-lived
   connection runs after five same-statement executions (PREPARE x6 —
   the plancache's generic-plan consideration threshold), plus a
   source-shape drift-guard for the two-clock split (selection bound
   STABLE, write stamps VOLATILE) that no plan or behavior pin can
   observe at microsecond granularity.
4. Move-queue drain pin -- the same Index Cond property for
   ``move_actor_queue``'s per-batch backlog drain, whose WHERE fixes
   both actor and queue. Leaving either predicate as a post-scan Filter
   makes every batch re-walk the rows earlier batches already moved, so
   drain time grows quadratically in backlog size.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only this module's own throwaway schema identifier (built from new_base62, validated by the migration runner's _IDENT_RE) or renders a module SQL constant; all values are $n-bound.

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_uuid
from taskq.actor_config_ops import (
    _MOVE_BACKLOG_BATCH_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: pinning the exact production drain statement, not a copy that could drift from it.
)
from taskq.backend._sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: pinning the exact production statements is the point of these tests; redefining them here would let the pins drift from the SQL that actually runs.
    _SWEEP_1_SQL,
    _SWEEP_2_SQL,
    _SWEEP_3_SQL,
    _SWEEP_4_SQL,
    _SWEEP_EVENT_TTL_SQL,
    _SWEEP_IDLE_KEYED_BUCKETS_SQL,
    _SWEEP_IDLE_KEYED_SLOTS_SQL,
    _SWEEP_RESULT_TTL_SQL,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_MAX_RETRY_BACKOFF,
    DEFAULT_PRUNE_BATCH_SIZE,  # pyright: ignore[reportPrivateUsage]  # Why: the production batch/retention the daily prune runs with; the corpus is seeded around them.
    DEFAULT_PRUNE_RETENTION,  # pyright: ignore[reportPrivateUsage]  # Why: same.
    RECLAIM_OUTBOX_RETENTION_MULTIPLIER,
)
from taskq.worker._leader_shared import (
    _ARCHIVE_CTE_ACTOR_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: same as above — pin the production statement, not a copy.
    _ARCHIVE_CTE_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: same.
    _CLEANUP_STALE_WORKERS_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: same.
    _EXPIRY_CTE_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: same.
    _QUERY_QUEUE_DEPTH_SQL_TEMPLATE,  # pyright: ignore[reportPrivateUsage]  # Why: same — the queue-depth gauge's exact statement.
)
from taskq.worker._leader_sweeps import (
    _QUERY_ACTOR_BACKLOG_SQL_TEMPLATE,  # pyright: ignore[reportPrivateUsage]  # Why: same as above — pin the production statement, not a copy.
    _QUERY_OLDEST_DUE_AGE_SQL_TEMPLATE,  # pyright: ignore[reportPrivateUsage]  # Why: same as above — pin the production statement, not a copy.
    _QUERY_RUNNING_LEASE_EXPIRED_SQL_TEMPLATE,  # pyright: ignore[reportPrivateUsage]  # Why: same — the zombie-running gauge's exact statement.
)
from taskq.worker.cron_loop import cron_due_sql

pytestmark = pytest.mark.integration

_AUDIT_INDEXES = (
    "jobs_queue_active_idx",
    "jobs_actor_active_id_idx",
    "job_attempts_worker_id_idx",
)

# cancel_where's pending/scheduled driving statement, rebuilt exactly as
# src/taskq/backend/_cancel_bulk.py assembles it (same builder, same
# template, same aggregate tail including matched_count) so the pin
# follows the production statement shape.
_CANCEL_PS_CTE_TEMPLATE = """
WITH matching AS MATERIALIZED (
    SELECT id, status
    FROM "{schema}".jobs
    WHERE {conditions}
      AND status IN ('pending', 'scheduled')
      AND id > ${cursor_ph}::uuid
    ORDER BY id
    LIMIT ${limit_ph}
),
batch_ids AS MATERIALIZED (
    SELECT array_agg(id ORDER BY id) AS ids,
           (array_agg(id ORDER BY id))[count(*)] AS last_id
    FROM matching
),
cancelled AS (
    UPDATE "{schema}".jobs AS j
    SET status = 'cancelled', finished_at = clock_timestamp()
    WHERE j.id = ANY ((SELECT ids FROM batch_ids)::uuid[])
      AND j.status IN ('pending', 'scheduled')
    RETURNING j.id
),
cancelled_prev AS (
    SELECT c.id, m.status AS prev_status
    FROM cancelled AS c
    JOIN matching AS m ON m.id = c.id
)
SELECT
    (SELECT count(*)::int FROM matching) AS matched_count,
    (SELECT last_id FROM batch_ids) AS last_id,
    (SELECT count(*)::int FROM cancelled_prev) AS cancelled_directly,
    (SELECT array_agg(id ORDER BY id) FROM cancelled_prev) AS cancelled_ids,
    (SELECT array_agg(prev_status ORDER BY id) FROM cancelled_prev) AS cancelled_prev_statuses
"""


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def _explain(conn: asyncpg.Connection, sql: str, *params: object) -> str:
    """EXPLAIN (no ANALYZE: nothing executes, so UPDATE/DELETE statements
    are pinned without a rollback wrapper) as joined plan text."""
    rows = await conn.fetch(f"EXPLAIN (BUFFERS) {sql}", *params)
    return "\n".join(r["QUERY PLAN"] for r in rows)


def _assert_index_cond(plan: str, index: str, cond_substring: str) -> None:
    """The plan must scan *index*, and the range bound must appear in an
    Index Cond line — the property the audit found load-bearing: a bound
    that degrades to a post-scan Filter walks the table (or a whole
    partial index's population) per tick instead of stopping at the
    boundary. The cond may be combined with other conds in one line
    (e.g. sweep 2's ``(stc IS NOT NULL) AND (stc < bound)``), so this
    accepts any Index Cond line containing the bound."""
    assert index in plan, f"expected {index} in plan:\n{plan}"
    cond_lines = [line for line in plan.splitlines() if "Index Cond:" in line]
    assert cond_lines and any(cond_substring in line for line in cond_lines), (
        f"expected an Index Cond containing {cond_substring!r} on {index}; a "
        f"bound that only appears as a Filter is not index-served:\n{plan}"
    )


# ── 1. migration pins ─────────────────────────────────────────────────


async def test_audit_indexes_exist_and_valid(pg_dsn: str) -> None:
    schema = f"idx_audit_exist_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        rows = await conn.fetch(
            """
            SELECT i.relname AS indexname, ix.indisvalid AS valid
            FROM pg_class i
            JOIN pg_index ix ON ix.indexrelid = i.oid
            JOIN pg_namespace n ON n.oid = i.relnamespace
            WHERE n.nspname = $1 AND i.relname = ANY($2::text[])
            """,
            schema,
            list(_AUDIT_INDEXES),
        )
        found = {r["indexname"]: r["valid"] for r in rows}
        for name in _AUDIT_INDEXES:
            assert name in found, f"{name} missing after apply_pending"
            assert found[name], f"{name} exists but is INVALID — apply_pending left broken debris"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_audit_migration_is_transactional_and_reapplies_cleanly(pg_dsn: str) -> None:
    schema = f"idx_audit_ledger_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        # The ledger must record the transactional apply: this file
        # deliberately uses plain CREATE INDEX inside the per-file
        # transaction (the no-transaction CONCURRENTLY form deadlocks
        # with apply_pending_locked's advisory-lock serialization —
        # see the migration header), and tests/test_migrations_unit.py
        # pins that every bundled migration stays transactional.
        use_txn = await conn.fetchval(
            f'SELECT use_transaction FROM "{schema}".schema_migrations '
            "WHERE version = '01.00.06_01:pre'"
        )
        assert use_txn is True, (
            "01.00.06_01 must be recorded with use_transaction=true; a "
            "no-transaction record means the CONCURRENTLY form came back "
            f"(use_transaction={use_txn!r})"
        )

        # Re-run: nothing pending, every index still present and valid.
        applied = await migrate_mod.apply_pending(conn, schema=schema)
        assert applied == [], f"re-run should apply nothing, applied {[m.key for m in applied]}"
        valid = await conn.fetchval(
            """
            SELECT bool_and(ix.indisvalid)
            FROM pg_class i
            JOIN pg_index ix ON ix.indexrelid = i.oid
            JOIN pg_namespace n ON n.oid = i.relnamespace
            WHERE n.nspname = $1 AND i.relname = ANY($2::text[])
            """,
            schema,
            list(_AUDIT_INDEXES),
        )
        assert valid is True, "re-run left an INVALID index behind"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


# ── 2. plan pins (bulk-seeded steady state) ──────────────────────────


@pytest.fixture(scope="module")
async def audit_schema(pg_dsn: str) -> Any:
    """Throwaway schema, all migrations applied, bulk-seeded into the
    steady-state shape: sizeable terminal history, live populations in
    every partial index, and NOTHING eligible for any sweep yet (every
    lease/stc/scheduled_at/result expiry in the future) — the shape the
    every-second/every-30s ticks see ~always, and the one the audit
    found seq-scanning before the bound rewrite."""
    schema = f"idx_audit_plan_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        # The shared test cluster runs with synchronous_commit=off (a
        # throwaway container — see taskq.testing._shared_containers),
        # and an async commit's records are not WAL-flushed when the
        # client is acknowledged. The visibility-map set is gated on the
        # page's WAL being durable, so a VACUUM after async-committed
        # COPY loads cannot mark those pages all-visible — the map
        # stalls part-covered and every candidate plan is priced as a
        # heap-fetching scan. Seeding this fixture's loads with
        # synchronous_commit=on makes every COPY commit durable by the
        # time the VACUUM below runs, so the map completes and the
        # planner can cost index-only scans — the representative steady
        # state of a production table, whose commits are durable and
        # whose autovacuum completes the map. Session-scoped: only this
        # one seeding connection pays the flushes.
        await conn.execute("SET synchronous_commit = on")

        now = datetime.now(UTC)

        def future(secs: float) -> datetime:
            return now + timedelta(seconds=secs)

        # 40k terminal history: makes whole-table walks expensive enough
        # that the planner's index choices match production-scale plans.
        terminal = [
            (
                new_uuid(),
                "hist.actor",
                "default",
                '{"v": 1}',
                "succeeded",
                now - timedelta(days=d % 30),
                3,
                "transient",
            )
            for d in range(40_000)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "finished_at",
                "max_attempts",
                "retry_kind",
            ],
            records=terminal,
        )
        # Steady-state live populations (nothing eligible):
        # 5k running w/ future leases, 2k scheduled future (plus 3k more
        # below via queue seeds), 2k pending w/ future stc, 3k results
        # not yet expired, 5k pending in queue 'orders' (cancel-by-queue
        # target), 2k pending for one actor (cancel/deregister target).
        running = [
            (
                new_uuid(),
                "live.actor",
                "default",
                '{"v": 1}',
                "running",
                future(600 + i),
                3,
                "transient",
            )
            for i in range(5_000)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "lock_expires_at",
                "max_attempts",
                "retry_kind",
            ],
            records=running,
        )
        # Heartbeat-configured running slice (nothing eligible — every
        # heartbeat fresh, every lease future): the steady-state
        # population of the heartbeat arm's partial index
        # (jobs_running_heartbeat_deadline_idx), whose every-tick cost
        # the arm's plan pin asserts stays index-bounded. A distinct
        # actor names the slice so the single UPDATE below can set
        # heartbeat_timeout on exactly these rows.
        heartbeat_running = [
            (
                new_uuid(),
                "live.heartbeat",
                "default",
                '{"v": 1}',
                "running",
                future(600 + i),
                now - timedelta(seconds=i % 30),
                3,
                "transient",
            )
            for i in range(500)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "lock_expires_at",
                "last_heartbeat_at",
                "max_attempts",
                "retry_kind",
            ],
            records=heartbeat_running,
        )
        await conn.execute(
            f'UPDATE "{schema}".jobs '  # Why: fixed actor literal — no user input, nothing to $-bind.
            "SET heartbeat_timeout = interval '30 seconds' "
            "WHERE actor = 'live.heartbeat' AND status = 'running'"
        )
        scheduled = [
            (
                new_uuid(),
                "live.actor",
                "default",
                '{"v": 1}',
                "scheduled",
                future(3600 + i),
                3,
                "transient",
            )
            for i in range(2_000)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "scheduled_at",
                "max_attempts",
                "retry_kind",
            ],
            records=scheduled,
        )
        stc = [
            (
                new_uuid(),
                "live.actor",
                "default",
                '{"v": 1}',
                "pending",
                future(3600 + i),
                3,
                "transient",
            )
            for i in range(2_000)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "schedule_to_close",
                "max_attempts",
                "retry_kind",
            ],
            records=stc,
        )
        results = [
            (
                new_uuid(),
                "hist.actor",
                "default",
                '{"v": 1}',
                "succeeded",
                '{"r": "x"}',
                future(3600 + i),
                3,
                "transient",
            )
            for i in range(3_000)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "result",
                "result_expires_at",
                "max_attempts",
                "retry_kind",
            ],
            records=results,
        )
        orders_queue = [
            (
                new_uuid(),
                "orders.process",
                "orders",
                '{"v": 1}',
                "pending",
                now + timedelta(seconds=i),
                3,
                "transient",
            )
            for i in range(5_000)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "scheduled_at",
                "max_attempts",
                "retry_kind",
            ],
            records=orders_queue,
        )
        actor_pending = [
            (
                new_uuid(),
                "sync.inventory",
                "sync",
                '{"v": 1}',
                "pending",
                now + timedelta(seconds=i),
                3,
                "transient",
            )
            for i in range(2_000)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "scheduled_at",
                "max_attempts",
                "retry_kind",
            ],
            records=actor_pending,
        )
        # 10k enabled cron schedules, all future (the volume at which the
        # due-tick's index-ordered path measurably beats a seq walk).
        cron = [
            (new_uuid(), f"cron.actor{i % 50}", f"n{i}", "*/5 * * * *", True, future(60 + i))
            for i in range(10_000)
        ]
        await conn.copy_records_to_table(
            "cron_schedules",
            schema_name=schema,
            columns=["id", "actor", "name", "cron_expr", "enabled", "next_fire_at"],
            records=cron,
        )
        # A stale worker with attempt history: the RI probe pin target.
        # The NULL-worker attempt bulk exists so the probe's index path
        # beats a seq scan at plan time (a 200-row table would seq-scan
        # and the pin would assert nothing about the index).
        stale_worker = new_uuid()
        await conn.execute(
            f'INSERT INTO "{schema}".workers (id, hostname, pid, queues) '
            "VALUES ($1, 'stale.example', 1, '{default}')",
            stale_worker,
        )
        referencing = [
            (
                terminal[d][0],
                1,
                now - timedelta(hours=2),
                now - timedelta(hours=2),
                "succeeded",
                stale_worker,
            )
            for d in range(200)
        ]
        unreferenceable = [
            (
                terminal[200 + d][0],
                1,
                now - timedelta(hours=3),
                now - timedelta(hours=3),
                "succeeded",
                None,
            )
            for d in range(15_000)
        ]
        await conn.copy_records_to_table(
            "job_attempts",
            schema_name=schema,
            columns=["job_id", "attempt", "started_at", "finished_at", "outcome", "worker_id"],
            records=referencing + unreferenceable,
        )
        # jobs gets VACUUM (ANALYZE), not bare ANALYZE: a production jobs
        # table is constantly vacuumed (autovacuum trails every bulk
        # write), so its live pages are all-visible and the planner can
        # cost index-only scans over them — the cost model the pins must
        # be evaluated at. Bare ANALYZE leaves the COPY-loaded pages'
        # visibility bits unset (ANALYZE never touches the map), which
        # forces every candidate plan into heap-fetching scans and
        # misprices the queue-depth gauge away from its (queue, id)
        # partial index. The sibling tables keep bare ANALYZE: their
        # pins assert Index Cond seeks whose chosen plans do not depend
        # on the visibility map.
        await conn.execute(f'VACUUM (ANALYZE) "{schema}".jobs')
        for table in ("cron_schedules", "job_attempts", "workers"):
            await conn.execute(f'ANALYZE "{schema}".{table}')
        yield schema, stale_worker
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_sweep_1_snap_is_index_bounded(audit_schema: Any, pg_dsn: str) -> None:
    """Sweep 1's snap must seek jobs_running_lock_expires_idx with the
    expiry range as an Index Cond (statement_timestamp() is STABLE; the
    old clock_timestamp() bound filtered a whole-table seq walk)."""
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(
            conn,
            _SWEEP_1_SQL.format(schema=schema),
            timedelta(seconds=30),
            timedelta(seconds=10),
            100,
            DEFAULT_MAX_RETRY_BACKOFF.total_seconds(),
        )
        _assert_index_cond(
            plan,
            "jobs_running_lock_expires_idx",
            "(lock_expires_at < statement_timestamp())",
        )
    finally:
        await conn.close()


async def test_sweep_1_heartbeat_arm_is_index_bounded(audit_schema: Any, pg_dsn: str) -> None:
    """Sweep 1's heartbeat arm (the per-job ``heartbeat_timeout``
    disjunct) must seek jobs_running_heartbeat_deadline_idx — partial on
    ``status='running' AND heartbeat_timeout IS NOT NULL`` — with
    ``last_heartbeat_at < statement_timestamp()`` as an Index Cond.

    The row-exact deadline (``last_heartbeat_at + heartbeat_timeout``)
    cannot be an index condition (the bound is row-dependent, and
    timestamptz+interval is STABLE so no expression index exists), so
    the arm's necessary condition — a heartbeat at all in the past — is
    stated explicitly to give the partial index its range bound, and the
    ORDER BY last_heartbeat_at pins the scan to that index's key order
    (the ORDER-BY-pins-the-scan rule the sibling sweeps follow). Without
    the index the OR's second arm degrades every sweep tick to a filter
    over the whole running set — the exact whole-table-walk class this
    audit family exists to prevent."""
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(
            conn,
            _SWEEP_1_SQL.format(schema=schema),
            timedelta(seconds=30),
            timedelta(seconds=10),
            100,
            DEFAULT_MAX_RETRY_BACKOFF.total_seconds(),
        )
        _assert_index_cond(
            plan,
            "jobs_running_heartbeat_deadline_idx",
            "(last_heartbeat_at < statement_timestamp())",
        )
    finally:
        await conn.close()


async def test_sweep_2_snap_is_index_bounded(audit_schema: Any, pg_dsn: str) -> None:
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(conn, _SWEEP_2_SQL.format(schema=schema), 100)
        _assert_index_cond(
            plan,
            "jobs_schedule_to_close_idx",
            "(schedule_to_close < statement_timestamp())",
        )
    finally:
        await conn.close()


async def test_sweep_3_snap_is_index_bounded(audit_schema: Any, pg_dsn: str) -> None:
    """The every-second scheduled-wake snap: ORDER BY scheduled_at pins
    it to jobs_scheduled_wake_idx with the due bound as an Index Cond —
    without the ORDER BY the planner can fractional-walk a different
    predicate-implied partial index (measured: a 21,000-entry walk of
    jobs_schedule_to_close_idx)."""
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(conn, _SWEEP_3_SQL.format(schema=schema), 100)
        _assert_index_cond(
            plan,
            "jobs_scheduled_wake_idx",
            "(scheduled_at <= statement_timestamp())",
        )
    finally:
        await conn.close()


async def test_result_ttl_sweep_is_index_bounded(audit_schema: Any, pg_dsn: str) -> None:
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(conn, _SWEEP_RESULT_TTL_SQL.format(schema=schema), 100)
        _assert_index_cond(
            plan,
            "jobs_result_expires_at_idx",
            "(result_expires_at < statement_timestamp())",
        )
    finally:
        await conn.close()


async def test_sweep_4_window_is_index_bounded(audit_schema: Any, pg_dsn: str) -> None:
    """Sweep 4's candidate window must seek
    reservation_slots_lease_expires_idx with the lease-expiry range as an
    Index Cond. The window's bound is statement_timestamp() (STABLE) and
    its ORDER BY pins the scan to the lease-keyed partial index — the
    same two-clock/ORDER-BY doctrine as every sibling sweep; a volatile
    clock_timestamp() bound (this sweep's old form) cannot be a btree
    index condition and degrades to a post-scan Filter over the whole
    held-slot population per tick."""
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        # Held slots with FUTURE leases: the steady-state shape (nothing
        # eligible yet) every sibling plan pin seeds to. 5k rows across
        # buckets so the planner's index choice matches a deployed fleet,
        # not an empty table's seq-scan default.
        now = datetime.now(UTC)
        held = [
            (
                f"bucket.{b}",
                s,
                new_uuid(),
                new_uuid(),
                now,
                now + timedelta(seconds=600 + b * 10 + s),
            )
            for b in range(500)
            for s in range(10)
        ]
        await conn.copy_records_to_table(
            "reservation_slots",
            schema_name=schema,
            columns=[
                "bucket_name",
                "slot_index",
                "job_id",
                "held_by_worker_id",
                "acquired_at",
                "lease_expires_at",
            ],
            records=held,
        )
        await conn.execute(f'ANALYZE "{schema}".reservation_slots')
        plan = await _explain(conn, _SWEEP_4_SQL.format(schema=schema), 100)
        _assert_index_cond(
            plan,
            "reservation_slots_lease_expires_idx",
            "(lease_expires_at < statement_timestamp())",
        )
    finally:
        await conn.close()


async def test_backlog_oldest_due_age_sampler_is_index_bounded(
    audit_schema: Any, pg_dsn: str
) -> None:
    """The backlog detector's oldest-due-age aggregate must seek
    jobs_scheduled_wake_idx with the due bound as an Index Cond
    (statement_timestamp() is STABLE; a volatile clock_timestamp() bound
    degrades to a post-scan Filter walking the partial index's whole
    scheduled population per worker, per interval — during the exact
    promotion-stall incident the gauge exists to expose)."""
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(conn, _QUERY_OLDEST_DUE_AGE_SQL_TEMPLATE.format(schema=schema))
        _assert_index_cond(
            plan,
            "jobs_scheduled_wake_idx",
            "(scheduled_at <= statement_timestamp())",
        )
    finally:
        await conn.close()


async def test_backlog_running_lease_expired_sampler_is_index_bounded(
    audit_schema: Any, pg_dsn: str
) -> None:
    """The zombie-running detector's count must seek
    jobs_running_lock_expires_idx (partial on status='running', keyed on
    lock_expires_at) with the expiry bound as an Index Cond — the same
    two-clock rule as every sibling sampler: a VOLATILE
    clock_timestamp() bound cannot be a btree index condition, so it
    would degrade to a post-scan Filter walking the whole running
    population per worker, per interval."""
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(conn, _QUERY_RUNNING_LEASE_EXPIRED_SQL_TEMPLATE.format(schema=schema))
        _assert_index_cond(
            plan,
            "jobs_running_lock_expires_idx",
            "(lock_expires_at < statement_timestamp())",
        )
    finally:
        await conn.close()


async def test_backlog_depth_gauge_is_attributable_to_actor_and_queue() -> None:
    """Backlog depth must be attributable at actor-and-queue granularity,
    not queue alone. An actor whose queue no worker consumes is the
    condition operators most need to see, and it is invisible in a
    queue-only aggregate: a queue carrying healthy traffic for other
    actors shows an unremarkable total while one actor's jobs pile up
    inside it forever. Because a worker that can do work never refuses to
    start over an unconsumed queue, this gauge is the only place that
    misconfiguration becomes visible, so the emitted series must carry
    both dimensions."""
    # The per-(actor, queue) attribution ships as the actor_backlog /
    # oldest-pending-age series fed by this sampler; the fleet-wide
    # queue-depth gauge (_QUERY_QUEUE_DEPTH_SQL_TEMPLATE) stays
    # queue-scoped by design.
    sql = _QUERY_ACTOR_BACKLOG_SQL_TEMPLATE.format(schema="s")
    grouped = sql.lower().partition("group by")[2]
    assert "actor" in grouped and "queue" in grouped, (
        "the backlog-depth gauge must group by BOTH actor and queue so an "
        "actor accumulating jobs on a queue nobody consumes is "
        "distinguishable from healthy load on that queue; a queue-only "
        f"aggregate hides it entirely: {sql!r}"
    )
    selected = sql.lower().partition("select")[2].partition("from")[0]
    assert "actor" in selected and "queue" in selected, (
        "both the actor and the queue label must be selected so the emitted "
        f"series is attributable to the actor that is backing up: {sql!r}"
    )


async def test_backlog_depth_gauge_is_index_bounded(audit_schema: Any, pg_dsn: str) -> None:
    """The every-queue_depth_interval gauge must be served by an index
    over exactly the pending/scheduled predicate, keyed so the grouping
    columns are read from the index rather than recovered by a walk. The
    sampler runs on every leader tick, so its cost must stay independent
    of terminal history no matter how many dimensions the series
    carries — adding actor attribution must not turn the gauge into a
    sequential scan."""
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(conn, _QUERY_QUEUE_DEPTH_SQL_TEMPLATE.format(schema=schema))
        assert "Seq Scan" not in plan, (
            "the backlog-depth gauge must not fall back to a sequential scan "
            "over jobs — its cost has to stay bounded by the active "
            f"pending/scheduled population, not by terminal history:\n{plan}"
        )
        assert "Index" in plan, (
            "the backlog-depth gauge must be served by an index over the "
            "pending/scheduled predicate carrying its grouping columns "
            f"(actor and queue), so the per-tick sampler stays bounded:\n{plan}"
        )
    finally:
        await conn.close()


async def test_cron_due_tick_is_index_bounded_without_sort(audit_schema: Any, pg_dsn: str) -> None:
    """The every-second tick's due statement (the probe runs first in its
    own statement; see CRON_LOCK_SQL_TEMPLATE for why the read cannot
    share it): cron_schedules_next_fire_idx serves the bound as an Index
    Cond and presorts the first ORDER BY key (next_fire_at). The s.id
    tiebreaker — budget deferrals mint next_fire_at ties, and which tied
    row is funded first must not depend on heap order — may only be paid
    for with an Incremental Sort over the tie prefix the LIMIT actually
    reads; a plain Sort node (sorting the whole scan output) means the
    ordered path regressed."""
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(conn, cron_due_sql(schema), 100)
        _assert_index_cond(
            plan,
            "cron_schedules_next_fire_idx",
            "(next_fire_at <= statement_timestamp())",
        )
        assert "Presorted Key: next_fire_at" in plan, (
            f"the index's key order must still carry the first ORDER BY key:\n{plan}"
        )
        plain_sort = re.search(r"(?m)^\s*(?:->\s*)?Sort(?! Key)", plan)
        assert plain_sort is None, (
            "ORDER BY next_fire_at, id must stay index-presorted with a "
            "bounded Incremental Sort over the tie prefix — a plain Sort "
            "sorts the whole scan output and the every-second tick's cost "
            f"regresses:\n{plan}"
        )
    finally:
        await conn.close()


async def test_cancel_by_queue_cte_is_index_served(audit_schema: Any, pg_dsn: str) -> None:
    """cancel_where(queue=...)'s matching CTE must seek
    jobs_queue_active_idx (queue, id) — pre-index it whole-table-walked
    jobs' PK in id order, ~30-40 ms for the drain's final empty call."""
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        sql = _CANCEL_PS_CTE_TEMPLATE.format(
            schema=schema, conditions="queue = $1", cursor_ph=2, limit_ph=3
        )
        plan = await _explain(conn, sql, "orders", UUID(int=0), 100)
        # The engine folds bound params to literals in EXPLAIN text, so the
        # Index Cond reads `queue = 'orders'::text`, not `queue = $1`.
        _assert_index_cond(plan, "jobs_queue_active_idx", "queue =")
    finally:
        await conn.close()


async def test_cancel_and_deregister_by_actor_is_index_served(
    audit_schema: Any, pg_dsn: str
) -> None:
    """cancel_where(actor=...) and deregister_actor's force-cancel drain
    must be served by an actor-keyed partial index — pre-index the
    planner whole-table-walked jobs' PK in id order (jobs_actor_pending_idx
    cannot provide the CTE's ORDER BY id, so it lost the plan race at
    realistic volume; jobs_actor_active_id_idx (actor, id) exists to win
    that race outright). Which of the two actor indexes the planner
    picks is a cost decision — (actor)+top-N-sort vs (actor,id) ordered
    seek — and both are bounded actor-population scans, so this pin
    asserts the actor-keyed seek and the absence of a whole-table walk
    rather than a specific index name."""
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        cancel_sql = _CANCEL_PS_CTE_TEMPLATE.format(
            schema=schema, conditions="actor = $1", cursor_ph=2, limit_ph=3
        )
        plan = await _explain(conn, cancel_sql, "sync.inventory", UUID(int=0), 100)
        assert "jobs_actor_active_id_idx" in plan or "jobs_actor_pending_idx" in plan, (
            f"cancel(actor) matching CTE must seek an actor-keyed index:\n{plan}"
        )
        assert "Seq Scan on jobs" not in plan, f"whole-table scan regressed:\n{plan}"

        from taskq.actor_config_ops import (
            _DEREGISTER_CANCEL_PENDING_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: pinning the production statement, same rationale as the _sweeps imports.
        )

        dereg_plan = await _explain(
            conn,
            _DEREGISTER_CANCEL_PENDING_SQL.format(schema=schema),
            "sync.inventory",
            100,
        )
        assert "jobs_actor_active_id_idx" in dereg_plan or "jobs_actor_pending_idx" in dereg_plan, (
            f"deregister cancel CTE must seek an actor-keyed index:\n{dereg_plan}"
        )
        assert "Seq Scan on jobs" not in dereg_plan, f"whole-table scan regressed:\n{dereg_plan}"
    finally:
        await conn.close()


async def test_worker_delete_cascade_probe_is_index_served(audit_schema: Any, pg_dsn: str) -> None:
    """The job_attempts_worker_id_fkey ON DELETE SET NULL RI trigger
    probes `worker_id = $1` once per deleted worker: with the partial
    job_attempts_worker_id_idx that probe is an index seek; without it
    (pre-migration) it was a full seq scan of job_attempts per deleted
    worker (measured 688 ms per 100 workers at 123k attempts). The
    probe shape below is the trigger's, parameterized like the cached
    RI plan is."""
    schema, stale_worker = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(
            conn,
            f'SELECT 1 FROM "{schema}".job_attempts WHERE worker_id = $1 FOR KEY SHARE',
            stale_worker,
        )
        assert "job_attempts_worker_id_idx" in plan, (
            f"RI probe must use job_attempts_worker_id_idx:\n{plan}"
        )
    finally:
        await conn.close()


async def test_cleanup_stale_workers_bound_is_index_qualified(
    audit_schema: Any, pg_dsn: str
) -> None:
    """cleanup_stale_workers' staleness bound uses statement_timestamp()
    for the same STABLE-vs-VOLATILE reason as the sweeps (uniform shape;
    the workers table itself is small enough that any plan is cheap)."""
    schema, _ = audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(
            conn,
            _CLEANUP_STALE_WORKERS_SQL.format(schema=schema),
            timedelta(seconds=60),
            new_uuid(),
            100,
        )
        assert "statement_timestamp()" in plan, (
            f"staleness bound should be STABLE statement_timestamp():\n{plan}"
        )
    finally:
        await conn.close()


# ── 3. prune/archive selection-bound plan pins ──────────────────────
#
# The once-a-day prune/archive/expiry candidate CTEs in
# taskq.worker._leader_shared kept VOLATILE clock_timestamp() selection
# bounds after the sweep rewrite — bounded (LIMIT-stopped, ORDER-BY-
# pinned) but paying a post-scan Filter over the partial index's whole
# population per batch, because the planner refuses a VOLATILE
# expression as a btree index condition. Measured on this corpus shape
# (70k terminal rows, PG 18, EXPLAIN ANALYZE, BUFFERS): the archive
# candidate scan Filter-walked all 70,000 jobs_finished_at_idx entries
# (1,757 buffers, ~11 ms) in the drained steady state where the STABLE
# statement_timestamp() bound is an Index Cond that terminates at the
# range boundary (2 buffers, ~0.05 ms); the expiry CTE's steady state
# walked all 30,000 jobs_archive entries (2,039 buffers, ~6.7 ms vs
# 2 buffers / 0.03 ms), and its server-prepared form (a long-lived
# connection past five same-statement executions) flips to a GENERIC
# plan that walks the whole population under a Filter.

_BATCH = DEFAULT_PRUNE_BATCH_SIZE
_RETENTION = DEFAULT_PRUNE_RETENTION
_ARCHIVE_RETENTION = timedelta(days=365)
# Warm-up executions for the PREPARE pins run against the drained
# population so they archive/expire nothing; the EXPLAIN then faces the
# backlog population (a different status) without executing.
_DRAINED_STATUS = "succeeded"
_BACKLOG_STATUS = "failed"
_PREPARE_WARMUPS = 5


def _assert_index_cond_line(plan: str, index: str, *cond_substrings: str) -> None:
    """The plan must carry ONE Index Cond line containing every
    *cond_substrings* — the property that makes a selection bound
    index-served (the scan terminates at the range boundary or the
    LIMIT) instead of a post-scan Filter over the population. Accepts
    the cond on either a plain Index Scan or a Bitmap Index Scan node
    (the bitmap form rechecks the same cond but still never walks the
    whole population)."""
    assert index in plan, f"expected {index} in plan:\n{plan}"
    cond_lines = [line for line in plan.splitlines() if "Index Cond:" in line]
    assert any(all(sub in line for sub in cond_substrings) for line in cond_lines), (
        f"expected an Index Cond on {index} containing {cond_substrings}; a "
        f"bound that only appears as a Filter is not index-served:\n{plan}"
    )


def _assert_bound_is_not_a_post_scan_filter(plan: str, bound_substring: str) -> None:
    """The selection bound must not ALSO (or instead) appear as a
    post-scan Filter: the Filter form is the measured defect — the scan
    visits every row of the table or partial-index population and
    evaluates the comparison per row. Status/actor equality conds stay
    Filters by design (the partial index proves status membership), so
    only the RANGE bound is asserted here."""
    filter_lines = [line for line in plan.splitlines() if "Filter:" in line]
    assert not any(bound_substring in line for line in filter_lines), (
        f"the selection bound {bound_substring!r} degraded to a post-scan "
        f"Filter — the whole-population walk these pins exist to prevent:\n{plan}"
    )


@pytest.fixture(scope="module")
async def prune_audit_schema(pg_dsn: str) -> Any:
    """Throwaway schema, all migrations applied, bulk-seeded into the two
    states the once-a-day prune/archive/expiry ticks actually see:

    * drained steady state — 40k terminal 'succeeded' finished 1-29 days
      ago (nothing eligible at the 30-day default retention) plus 30k
      jobs_archive rows whose expire_at is a year out;
    * backlog — 30k terminal 'failed' finished 31-90 days ago (all
      eligible at the same retention).

    The two states are split by STATUS so both live in one corpus: the
    prune loop runs one statement per terminal status with the same
    retention, so one EXPLAIN per CTE pins both states with
    production-typical parameters. 5k running + 5k scheduled live rows
    keep the jobs table's stats realistic for every partial index, per
    the bulk-seeding discipline of ``audit_schema`` above: plans are
    only stable at realistic volumes."""
    schema = f"prune_audit_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        now = datetime.now(UTC)
        jobs_cols = [
            "id",
            "actor",
            "queue",
            "payload",
            "status",
            "finished_at",
            "max_attempts",
            "retry_kind",
        ]

        def terminal(
            status: str, d_from: int, d_to: int, n: int
        ) -> list[tuple[UUID | str | datetime | int, ...]]:
            return [
                (
                    new_uuid(),
                    "test_actor",
                    "default",
                    '{"v": 1}',
                    status,
                    now - timedelta(days=d_from + (d_to - d_from) * i // max(n - 1, 1)),
                    3,
                    "transient",
                )
                for i in range(n)
            ]

        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=jobs_cols,
            records=terminal(_DRAINED_STATUS, 1, 29, 40_000),
        )
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=jobs_cols,
            records=terminal(_BACKLOG_STATUS, 31, 90, 30_000),
        )
        running = [
            (
                new_uuid(),
                "live.actor",
                "default",
                '{"v": 1}',
                "running",
                now + timedelta(seconds=600 + i),
                3,
                "transient",
            )
            for i in range(5_000)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "lock_expires_at",
                "max_attempts",
                "retry_kind",
            ],
            records=running,
        )
        scheduled = [
            (
                new_uuid(),
                "live.actor",
                "default",
                '{"v": 1}',
                "scheduled",
                now + timedelta(seconds=3600 + i),
                3,
                "transient",
            )
            for i in range(5_000)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=[
                "id",
                "actor",
                "queue",
                "payload",
                "status",
                "scheduled_at",
                "max_attempts",
                "retry_kind",
            ],
            records=scheduled,
        )
        archive_cols = [
            "id",
            "actor",
            "queue",
            "payload",
            "max_attempts",
            "retry_kind",
            "status",
            "priority",
            "scheduled_at",
            "schedule_to_close",
            "finished_at",
            "archived_at",
            "expire_at",
            "metadata",
            "payload_schema_ver",
        ]
        archive = [
            (
                new_uuid(),
                "test_actor",
                "default",
                '{"v": 1}',
                3,
                "transient",
                "succeeded",
                0,
                now,
                now + timedelta(hours=1),
                now - timedelta(days=31),
                now,
                now + timedelta(days=365),
                "{}",
                1,
            )
            for _ in range(30_000)
        ]
        await conn.copy_records_to_table(
            "jobs_archive", schema_name=schema, columns=archive_cols, records=archive
        )
        for table in ("jobs", "jobs_archive"):
            await conn.execute(f'ANALYZE "{schema}".{table}')
        yield schema
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def _explain_prepared(
    conn: asyncpg.Connection, prepare: str, warmup: str, explain: str
) -> str:
    """PREPARE the statement, EXECUTE it five times (the server's
    generic-plan consideration threshold — the form a long-lived
    connection's server-side prepared statement settles into), then
    EXPLAIN the next execution's plan. The statement type list must
    match what production binds: asyncpg resolves ``$1::job_status`` to
    the enum type, so PREPARE must declare the enum (a text-typed
    param would make the cast a runtime IO coercion, unfoldable at plan
    time, and even custom plans would lose the partial-index predicate
    proof)."""
    await conn.execute(prepare)
    for _ in range(_PREPARE_WARMUPS):
        await conn.execute(warmup)
    rows = await conn.fetch(f"EXPLAIN (BUFFERS) {explain}")
    return "\n".join(r["QUERY PLAN"] for r in rows)


async def test_archive_cte_drained_steady_state_is_index_bounded(
    prune_audit_schema: Any, pg_dsn: str
) -> None:
    """The once-a-day prune's archive candidate scan against the drained
    steady state (nothing eligible, 70k-entry terminal population): the
    STABLE bound must be an Index Cond on jobs_finished_at_idx so the
    scan terminates at the range boundary — the VOLATILE form
    Filter-walked all 70,000 entries (measured: 1,757 buffers, ~11 ms
    per call, per status)."""
    schema = prune_audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(
            conn,
            _ARCHIVE_CTE_SQL.format(schema=schema),
            _DRAINED_STATUS,
            _RETENTION,
            _BATCH,
            _ARCHIVE_RETENTION,
        )
        _assert_index_cond_line(
            plan, "jobs_finished_at_idx", "finished_at <", "statement_timestamp"
        )
        _assert_bound_is_not_a_post_scan_filter(plan, "finished_at <")
    finally:
        await conn.close()


async def test_archive_cte_backlog_is_index_bounded(prune_audit_schema: Any, pg_dsn: str) -> None:
    """The same candidate scan against the eligible backlog (30k
    'failed' rows older than retention): the bound must still be an
    Index Cond — in the backlog the VOLATILE form only "works" because
    the oldest entries happen to pass its Filter, and any youngest-first
    population (recent terminal rows ahead of eligible ones in index
    order) pays the Filter over everything ahead of the eligible set."""
    schema = prune_audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(
            conn,
            _ARCHIVE_CTE_SQL.format(schema=schema),
            _BACKLOG_STATUS,
            _RETENTION,
            _BATCH,
            _ARCHIVE_RETENTION,
        )
        _assert_index_cond_line(
            plan, "jobs_finished_at_idx", "finished_at <", "statement_timestamp"
        )
        _assert_bound_is_not_a_post_scan_filter(plan, "finished_at <")
    finally:
        await conn.close()


async def test_archive_actor_cte_is_index_bounded_drained_and_backlog(
    prune_audit_schema: Any, pg_dsn: str
) -> None:
    """The per-actor-retention override CTE has the same selection shape
    plus an actor equality; both states must carry the range as an Index
    Cond (the actor filter rides along as a Filter, like status)."""
    schema = prune_audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        for status in (_DRAINED_STATUS, _BACKLOG_STATUS):
            plan = await _explain(
                conn,
                _ARCHIVE_CTE_ACTOR_SQL.format(schema=schema),
                status,
                _RETENTION,
                _BATCH,
                _ARCHIVE_RETENTION,
                "test_actor",
            )
            _assert_index_cond_line(
                plan, "jobs_finished_at_idx", "finished_at <", "statement_timestamp"
            )
            _assert_bound_is_not_a_post_scan_filter(plan, "finished_at <")
    finally:
        await conn.close()


async def test_archive_cte_prepared_statement_stays_index_bounded(
    prune_audit_schema: Any, pg_dsn: str
) -> None:
    """PREPARE x6 with production parameter typing: after five
    same-statement executions (the plancache's generic-plan threshold —
    a pooled connection running the daily prune), the plan the next
    execution uses must still seek jobs_finished_at_idx with the range
    as an Index Cond. Measured pre-fix: the prepared statement keeps a
    custom plan whose VOLATILE bound is a Filter over the partial-index
    population, so the defect survives on long-lived connections."""
    schema = prune_audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        warm = (
            f"EXECUTE taskq_pin_archive('{_DRAINED_STATUS}', "
            f"interval '{_RETENTION.days} days', {_BATCH}, "
            f"interval '{_ARCHIVE_RETENTION.days} days')"
        )
        backlog = (
            f"EXECUTE taskq_pin_archive('{_BACKLOG_STATUS}', "
            f"interval '{_RETENTION.days} days', {_BATCH}, "
            f"interval '{_ARCHIVE_RETENTION.days} days')"
        )
        plan = await _explain_prepared(
            conn,
            f'PREPARE taskq_pin_archive ("{schema}".job_status, interval, int, '
            f"interval) AS {_ARCHIVE_CTE_SQL.format(schema=schema)}",
            warm,
            backlog,
        )
        _assert_index_cond_line(
            plan, "jobs_finished_at_idx", "finished_at <", "statement_timestamp"
        )
        _assert_bound_is_not_a_post_scan_filter(plan, "finished_at <")
    finally:
        await conn.close()


async def test_archive_actor_cte_prepared_statement_bound_stays_stable(
    prune_audit_schema: Any, pg_dsn: str
) -> None:
    """PREPARE x6 for the per-actor override CTE, pinning what the bound
    must guarantee on EVERY plan form the server can pick for it.

    Measured on this corpus: past the threshold the server flips this
    prepared statement to a GENERIC plan (the actor equality makes the
    generic seq-scan cost-competitive — the custom plan's total is
    dominated by the INSERT/DELETE join sides, not the candidate scan),
    and no index can serve that generic form at all: ``status = $1`` /
    ``actor = $5`` as Params cannot prove the partial predicates of
    jobs_finished_at_idx or the actor partial indexes, so the generic
    plan seq-scans jobs whatever the bound's volatility. That generic
    seq-scan shape is a reported follow-up (it needs a full
    (actor, finished_at)-style index — a DDL change), not something the
    bound can fix; the Index-Cond property itself stays pinned by the
    actor CTE's custom-plan pins above. What the bound MUST guarantee
    here is that neither plan form ever regresses to the VOLATILE
    clock_timestamp() bound, which measurably degrades both (the custom
    form Filter-walks the partial-index population; the generic form's
    Filter walks it too). The write stamps never surface in EXPLAIN
    output, so ``clock_timestamp()`` absent from the plan is exactly
    the selection bound's absence."""
    schema = prune_audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        warm = (
            f"EXECUTE taskq_pin_actor('{_DRAINED_STATUS}', "
            f"interval '{_RETENTION.days} days', {_BATCH}, "
            f"interval '{_ARCHIVE_RETENTION.days} days', 'test_actor')"
        )
        backlog = (
            f"EXECUTE taskq_pin_actor('{_BACKLOG_STATUS}', "
            f"interval '{_RETENTION.days} days', {_BATCH}, "
            f"interval '{_ARCHIVE_RETENTION.days} days', 'test_actor')"
        )
        plan = await _explain_prepared(
            conn,
            f'PREPARE taskq_pin_actor ("{schema}".job_status, interval, int, '
            f"interval, text) AS {_ARCHIVE_CTE_ACTOR_SQL.format(schema=schema)}",
            warm,
            backlog,
        )
        assert "statement_timestamp" in plan, (
            "the prepared actor CTE's selection bound must be STABLE "
            "statement_timestamp() in whichever plan form (custom or generic) "
            f"the server picks:\n{plan}"
        )
        assert "clock_timestamp()" not in plan, (
            "a VOLATILE selection bound regressed into the prepared actor CTE's "
            "plan — it degrades every plan form this statement can run:\n{plan}"
        )
    finally:
        await conn.close()


async def test_archive_expiry_cte_drained_steady_state_is_index_bounded(
    prune_audit_schema: Any, pg_dsn: str
) -> None:
    """The archive-expiry sweep against the drained steady state (30k
    rows, nothing expired yet): the bound must be an Index Cond on
    jobs_archive_expire_at_idx — the VOLATILE form Filter-walked all
    30,000 entries (measured: 2,039 buffers, ~6.7 ms per call)."""
    schema = prune_audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(conn, _EXPIRY_CTE_SQL.format(schema=schema), _BATCH)
        _assert_index_cond_line(
            plan, "jobs_archive_expire_at_idx", "expire_at <", "statement_timestamp"
        )
        _assert_bound_is_not_a_post_scan_filter(plan, "expire_at <")
    finally:
        await conn.close()


async def test_archive_expiry_cte_backlog_is_index_bounded(
    prune_audit_schema: Any, pg_dsn: str
) -> None:
    """The expiry sweep with a real eligible backlog: 5k of 35k rows
    expired, stats refreshed in-transaction, so the pin faces the
    selectivity the production statement plans against. The mutation
    runs inside a transaction the test rolls back, so the shared corpus
    stays pristine regardless of test order. Measured pre-fix with fresh
    backlog stats: the VOLATILE bound seq-scans the whole archive table
    under a Filter; the STABLE bound is a Bitmap Index Scan whose Index
    Cond carries the range (the bitmap pays only a top-N sort of the
    LIMIT-ed batch, never of the backlog)."""
    schema = prune_audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute("BEGIN")
        try:
            await conn.execute(
                f'UPDATE "{schema}".jobs_archive '
                "SET expire_at = clock_timestamp() - interval '1 hour' "
                f'WHERE id IN (SELECT id FROM "{schema}".jobs_archive LIMIT 5000)'
            )
            await conn.execute(f'ANALYZE "{schema}".jobs_archive')
            plan = await _explain(conn, _EXPIRY_CTE_SQL.format(schema=schema), _BATCH)
            _assert_index_cond_line(
                plan, "jobs_archive_expire_at_idx", "expire_at <", "statement_timestamp"
            )
            _assert_bound_is_not_a_post_scan_filter(plan, "expire_at <")
        finally:
            await conn.execute("ROLLBACK")
    finally:
        await conn.close()


async def test_archive_expiry_cte_prepared_statement_stays_index_bounded(
    prune_audit_schema: Any, pg_dsn: str
) -> None:
    """PREPARE x6 for the expiry CTE — the statement whose prepared form
    measurably degrades WORSE than its ad-hoc form: with the VOLATILE
    bound the server flips to a generic plan that walks the whole
    jobs_archive_expire_at_idx population under a Filter on a
    long-lived connection; the STABLE bound keeps the Index Cond in
    both plan forms."""
    schema = prune_audit_schema
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain_prepared(
            conn,
            f"PREPARE taskq_pin_expiry (int) AS {_EXPIRY_CTE_SQL.format(schema=schema)}",
            f"EXECUTE taskq_pin_expiry({_BATCH})",
            f"EXECUTE taskq_pin_expiry({_BATCH})",
        )
        _assert_index_cond_line(
            plan, "jobs_archive_expire_at_idx", "expire_at <", "statement_timestamp"
        )
        _assert_bound_is_not_a_post_scan_filter(plan, "expire_at <")
    finally:
        await conn.close()


def test_prune_archive_two_clock_split_is_pinned() -> None:
    """Drift-guard for the documented two-clock split in the
    prune/archive selection CTEs: the row-SELECTION bounds must be
    STABLE statement_timestamp() (index-servable — the plan pins above
    catch a regression there), and the WRITE stamps (archived_at /
    expire_at) must stay VOLATILE clock_timestamp() (co-monotonic with
    the other clock-domain writes in the same statement — see
    _sweeps.py's module docstring). A stamp regression is invisible to
    every plan and behavior pin at microsecond granularity, so the
    split is pinned at its source here."""
    for cte_sql in (_ARCHIVE_CTE_SQL, _ARCHIVE_CTE_ACTOR_SQL):
        assert "finished_at < statement_timestamp() - $2::interval" in cte_sql, (
            "the archive candidate SELECTION bound must be STABLE "
            "statement_timestamp() so jobs_finished_at_idx serves it as an "
            "Index Cond"
        )
        assert "finished_at < clock_timestamp()" not in cte_sql, (
            "a VOLATILE selection bound degrades to a post-scan Filter over "
            "the partial-index population"
        )
        assert "clock_timestamp(), clock_timestamp() + $4" in cte_sql, (
            "the archived_at/expire_at WRITE stamps must stay clock_timestamp() "
            "(co-monotonic with the same statement's other clock-domain writes)"
        )
    assert "expire_at < statement_timestamp()" in _EXPIRY_CTE_SQL, (
        "the expiry SELECTION bound must be STABLE statement_timestamp() so "
        "jobs_archive_expire_at_idx serves it as an Index Cond"
    )
    assert "expire_at < clock_timestamp()" not in _EXPIRY_CTE_SQL, (
        "a VOLATILE selection bound degrades to a post-scan Filter over the "
        "whole archive population"
    )


# ── 4. move-queue backlog drain plan pin ─────────────────────────────


_MQ_ACTOR = "mq_drain.actor"
_MQ_OLD_QUEUE = "mq_drain_old"
_MQ_NEW_QUEUE = "mq_drain_new"
_MQ_BATCH_SIZE = 100
# Large enough that an O(n) Filter walk over the moved population is
# visible in the plan's row-count estimate, small enough to seed quickly.
_MQ_ALREADY_MOVED = 150_000


@pytest.fixture(scope="module")
async def move_drain_schema(pg_dsn: str) -> Any:
    """A backlog shaped like a move-queue drain three quarters of the way
    through a big actor backlog: most of the actor's pending/scheduled
    rows already carry the NEW queue label (a prior batch moved them), a
    smaller remainder still carries the OLD queue label (what this batch
    must find), plus unrelated neighbor rows on both queues for other
    actors sharing the source queue."""
    schema = f"idx_audit_mq_drain_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)
        await conn.execute("SET synchronous_commit = on")

        # The actor's own already-moved backlog: same status filter and
        # same actor as the drain target, but sitting on the NEW queue,
        # exactly the population a queue-blind actor index walks past.
        moved = [
            (new_uuid(), _MQ_ACTOR, _MQ_NEW_QUEUE, '{"v": 1}', "pending", 3, "transient")
            for _ in range(_MQ_ALREADY_MOVED)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=["id", "actor", "queue", "payload", "status", "max_attempts", "retry_kind"],
            records=moved,
        )
        # The remainder still on the source queue: what a correct batch
        # must find, cheaply, regardless of how large the moved
        # population above is.
        remaining = [
            (new_uuid(), _MQ_ACTOR, _MQ_OLD_QUEUE, '{"v": 1}', "pending", 3, "transient")
            for _ in range(500)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=["id", "actor", "queue", "payload", "status", "max_attempts", "retry_kind"],
            records=remaining,
        )
        # Neighbor actors sharing both queues: must not make the
        # queue-only index look attractive either, and must not be
        # touched by the actor-scoped drain.
        neighbors = [
            (
                new_uuid(),
                f"mq_drain.neighbor{i % 50}",
                _MQ_OLD_QUEUE if i % 2 == 0 else _MQ_NEW_QUEUE,
                '{"v": 1}',
                "pending",
                3,
                "transient",
            )
            for i in range(20_000)
        ]
        await conn.copy_records_to_table(
            "jobs",
            schema_name=schema,
            columns=["id", "actor", "queue", "payload", "status", "max_attempts", "retry_kind"],
            records=neighbors,
        )

        await conn.execute("VACUUM ANALYZE " + f'"{schema}".jobs')
        return schema
    finally:
        await conn.close()


async def test_move_queue_drain_batch_bounds_cost_on_actor_and_queue_together(
    move_drain_schema: str, pg_dsn: str
) -> None:
    """``move_actor_queue`` rewrites an actor's pending/scheduled backlog
    onto the target queue in bounded committed batches, each selecting the
    next ``batch_size`` rows still carrying the source queue. Every batch
    must cost the same regardless of how many of the actor's rows earlier
    batches already moved onto the target queue, so the plan has to fix
    BOTH ``actor`` and ``queue`` in an Index Cond rather than leaving one
    of them as a post-scan Filter.

    With only single-column partial indexes (actor, id) and (queue, id),
    whichever one the planner picks leaves the other predicate as a Filter
    that walks the already-moved population, so each batch pays for every
    row previously moved and total drain time grows quadratically in the
    backlog size. On a large backlog that turns a routine operator action
    into a per-batch statement timeout.
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(
            conn,
            _MOVE_BACKLOG_BATCH_SQL.format(schema=move_drain_schema),
            _MQ_ACTOR,
            _MQ_OLD_QUEUE,
            _MQ_NEW_QUEUE,
            _MQ_BATCH_SIZE,
        )

        index_cond_lines = [line for line in plan.splitlines() if "Index Cond:" in line]
        combined = [line for line in index_cond_lines if "actor" in line and "queue" in line]
        assert combined, (
            "expected an Index Cond that fixes BOTH actor and queue for the "
            "drain batch's matching-rows scan (e.g. a composite index on "
            "(actor, queue, id) or (queue, actor, id) under the pending/"
            "scheduled predicate) so a batch's cost is independent of how "
            "much of the actor's backlog already moved to the target "
            f"queue; got Index Cond lines: {index_cond_lines!r}\nplan:\n{plan}"
        )

        # The Rows Removed by Filter on the matching-rows scan should be
        # small (bounded roughly by the batch/neighbor shape), never on
        # the order of the already-moved population. That gap is exactly
        # the quadratic-drain mechanism.
        filter_removed_lines = [
            line for line in plan.splitlines() if "Rows Removed by Filter" in line
        ]
        for line in filter_removed_lines:
            digits = "".join(ch for ch in line if ch.isdigit())
            removed = int(digits) if digits else 0
            assert removed < _MQ_ALREADY_MOVED // 10, (
                "the matching-rows scan filtered out a large share of the "
                "already-moved backlog instead of having it excluded by an "
                f"Index Cond, which is the quadratic-drain mechanism: {line!r}\n"
                f"plan:\n{plan}"
            )
    finally:
        await conn.close()


# ── 5. job_events retention sweep plan + cost pins ────────────────────
#
# The event-retention sweep ticks on the leader forever, and its ordinary
# arm must never pay for the crash-reclaim outbox slice it deliberately
# exempts. That slice is immortal at ordinary retention age (it drains
# only at RECLAIM_OUTBOX_RETENTION_MULTIPLIER x retention), so in a fleet
# whose reclaim consumer lags it accumulates without bound. If the age
# bound or the carve-out is not index-served, every tick re-walks that
# accumulating population: the drained steady-state tick — the one that
# runs every cycle and deletes nothing — grows more expensive the longer
# the deployment lives, until it trips its statement timeout and event
# retention stops working silently.

_EV_RETENTION = timedelta(days=30)
_EV_BATCH = 100
# Ordinary (deletable-kind) events, all NEWER than the retention age: the
# drained steady-state shape every sibling plan pin seeds to.
_EV_LIVE_ORDINARY = 40_000
# Events spread over many jobs, as a fleet's are.
_EV_JOBS = 2_000


def _event_ttl_sql(schema: str) -> str:
    return _SWEEP_EVENT_TTL_SQL.format(
        schema=schema, outbox_multiplier=RECLAIM_OUTBOX_RETENTION_MULTIPLIER
    )


async def _make_event_schema(conn: asyncpg.Connection, schema: str, *, outbox_rows: int) -> None:
    """Migrated schema holding a realistic event corpus: a live
    ordinary-event population inside the retention window, and
    *outbox_rows* unconsumed crash-reclaim outbox events far older than
    ordinary retention but still inside the outbox age cap — i.e. rows
    the sweep must never delete and must never pay to look at.

    Events are spread across many jobs, as a real fleet's are: piling a
    whole corpus onto one job_id makes the per-job index the cheapest
    path to everything and the plan stops resembling production."""
    await _drop_schema(conn, schema)
    await migrate_mod.apply_pending(conn, schema=schema)

    job_ids = [new_uuid() for _ in range(_EV_JOBS)]
    await conn.copy_records_to_table(
        "jobs",
        schema_name=schema,
        columns=["id", "actor", "queue", "payload", "status", "max_attempts", "retry_kind"],
        records=[
            (job_id, "ev.actor", "default", '{"v": 1}', "running", 3, "transient")
            for job_id in job_ids
        ],
    )

    now = datetime.now(UTC)
    await conn.copy_records_to_table(
        "job_events",
        schema_name=schema,
        columns=["job_id", "occurred_at", "kind", "detail"],
        records=[
            (job_ids[i % _EV_JOBS], now - timedelta(minutes=5), "progress", "{}")
            for i in range(_EV_LIVE_ORDINARY)
        ],
    )
    if outbox_rows:
        await conn.copy_records_to_table(
            "job_events",
            schema_name=schema,
            columns=["job_id", "occurred_at", "kind", "detail"],
            records=[
                (
                    job_ids[i % _EV_JOBS],
                    # Older than ordinary retention, far inside the outbox
                    # cap — exempt at this age, so the sweep deletes none.
                    now - _EV_RETENTION * 2,
                    "state_change",
                    '{"reason": "lock_expired"}',
                )
                for i in range(outbox_rows)
            ],
        )
    await conn.execute(f'VACUUM (ANALYZE) "{schema}".job_events')


@pytest.fixture(scope="module")
async def event_ttl_schema(pg_dsn: str) -> Any:
    schema = f"idx_audit_ev_ttl_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _make_event_schema(conn, schema, outbox_rows=60_000)
        yield schema
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_event_retention_window_is_index_bounded(event_ttl_schema: str, pg_dsn: str) -> None:
    """The retention sweep's ordinary-events window must seek
    ``job_events_occurred_at_idx`` with the age bound as an Index Cond.

    A bound that survives only as a post-scan Filter makes every tick
    walk the whole event table — including the exempt crash-reclaim
    outbox slice, which is immortal at this age and grows for the life of
    the deployment. The tick then gets slower forever and eventually
    times out, at which point event retention stops draining and the
    table grows without bound, with nothing but the sweep's timeout
    counter to say so."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(conn, _event_ttl_sql(event_ttl_schema), _EV_RETENTION, _EV_BATCH)
        _assert_index_cond(
            plan,
            "job_events_occurred_at_idx",
            "occurred_at <",
        )
    finally:
        await conn.close()


async def test_event_retention_outbox_arm_is_index_bounded(
    event_ttl_schema: str, pg_dsn: str
) -> None:
    """The outbox age-cap arm's own age bound must be an Index Cond, not a
    post-scan Filter.

    Confining the arm to the outbox partial index is not enough: that
    index is keyed on ``id`` alone, so an age bound left as a Filter makes
    the arm walk every unconsumed outbox row on every tick and discard
    them all. That population is exempt from ordinary retention by
    design, and in a fleet whose ``watch_reclaims`` consumer lags or is
    absent it grows for the life of the deployment — so the tick that is
    supposed to bound the outbox is itself unbounded in the outbox's
    size, which is the whole-population-walk class this audit family
    exists to prevent."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(conn, _event_ttl_sql(event_ttl_schema), _EV_RETENTION, _EV_BATCH)
        # The outbox arm's bound is retention x the multiplier; the server
        # folds that to a single interval literal, so the arm's Index Cond
        # is distinguished from the ordinary arm's by the folded value.
        outbox_bound = _EV_RETENTION * RECLAIM_OUTBOX_RETENTION_MULTIPLIER
        outbox_cond = [
            line
            for line in plan.splitlines()
            if "Index Cond:" in line
            and "occurred_at <" in line
            and f"{outbox_bound.days} days" in line
        ]
        assert outbox_cond, (
            "the outbox age-cap arm's occurred_at bound must appear as an Index "
            "Cond so the scan stops at the age boundary; as a Filter it walks "
            f"the entire unconsumed outbox population every tick:\n{plan}"
        )
    finally:
        await conn.close()


async def test_event_retention_drained_tick_cost_is_flat_in_exempt_population(
    pg_dsn: str,
) -> None:
    """A drained steady-state tick — nothing eligible, nothing deleted —
    must cost the same whether the fleet holds a handful of unconsumed
    crash-reclaim outbox events or tens of thousands of them.

    This is the operator-visible property: the leader's event-retention
    tick runs forever, and the outbox slice it exempts accumulates
    whenever a fleet's reclaim consumer lags or is absent. If the tick's
    cost tracks that population, a long-lived deployment's maintenance
    loop degrades on its own, with no change in job throughput to explain
    it, until the statement timeout fires and retention silently stops.
    Cost is measured as buffers actually touched by the executed
    statement, which is what turns into I/O on a table too large to
    cache."""
    small_schema = f"idx_audit_ev_flat_s_{new_base62()}".lower()
    large_schema = f"idx_audit_ev_flat_l_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _make_event_schema(conn, small_schema, outbox_rows=200)
        await _make_event_schema(conn, large_schema, outbox_rows=60_000)

        async def _buffers(schema: str) -> int:
            # ANALYZE so the measured cost is what the tick really pays,
            # not an estimate; the sweep deletes nothing here, so the
            # measurement is repeatable and leaves no state behind.
            rows = await conn.fetch(
                f"EXPLAIN (ANALYZE, BUFFERS) {_event_ttl_sql(schema)}",
                _EV_RETENTION,
                _EV_BATCH,
            )
            plan = "\n".join(r["QUERY PLAN"] for r in rows)
            total = 0
            for line in plan.splitlines():
                if "shared hit=" in line or "shared read=" in line:
                    for token in line.replace("shared", "").split():
                        if token.startswith(("hit=", "read=")):
                            total += int(token.split("=")[1])
            assert total > 0, f"no buffer accounting in plan:\n{plan}"
            return total

        small_cost = await _buffers(small_schema)
        large_cost = await _buffers(large_schema)

        # A 300x larger exempt population must not move the tick's cost
        # materially. The allowance covers planner/visibility noise, not
        # a scan whose length tracks the population.
        assert large_cost <= small_cost * 3 + 50, (
            "the drained event-retention tick's cost grows with the exempt "
            f"crash-reclaim outbox population: {small_cost} buffers at 200 "
            f"outbox rows vs {large_cost} buffers at 60,000 — the tick is "
            "walking the rows it is required to skip, so maintenance cost "
            "degrades over the life of the deployment"
        )
    finally:
        await _drop_schema(conn, small_schema)
        await _drop_schema(conn, large_schema)
        await conn.close()


# ── 6. keyed-row fleet reclaim plan pins ──────────────────────────────
#
# The keyed-row sweeps tick on the leader against tables whose population
# is the fleet's live tenant/key cardinality — the one maintenance target
# that grows with customers rather than with backlog. Almost every row is
# fresh (in use), so the every-tick shape is the drained one: nothing
# eligible, nothing deleted. That tick must cost a seek to the age
# boundary, not a walk of every keyed row; a bound left as a post-scan
# Filter means onboarding tenants silently makes the maintenance loop
# slower until it trips its statement timeout and keyed rows stop being
# reclaimed at all.

_KEYED_HORIZON = timedelta(hours=1)
_KEYED_BATCH = 100
# Fresh keyed rows for distinct keys — the steady state a multi-tenant
# fleet sits in, at a volume where the planner's index choice is the one
# a deployment would get rather than a small table's seq-scan default.
_KEYED_LIVE_ROWS = 30_000


@pytest.fixture(scope="module")
async def keyed_reclaim_schema(pg_dsn: str) -> Any:
    schema = f"idx_audit_keyed_{new_base62()}".lower()
    assert _IDENT_RE.match(schema)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)
        now = datetime.now(UTC)
        await conn.copy_records_to_table(
            "rate_limit_buckets",
            schema_name=schema,
            columns=["bucket_name", "kind", "state", "keyed", "last_used_at"],
            records=[
                (f"tenant.{i}", "token_bucket", "{}", True, now - timedelta(seconds=i % 60))
                for i in range(_KEYED_LIVE_ROWS)
            ],
        )
        await conn.copy_records_to_table(
            "reservation_slots",
            schema_name=schema,
            columns=["bucket_name", "slot_index", "keyed", "last_used_at"],
            records=[
                (f"tenant.{i}", 0, True, now - timedelta(seconds=i % 60))
                for i in range(_KEYED_LIVE_ROWS)
            ],
        )
        for table in ("rate_limit_buckets", "reservation_slots"):
            await conn.execute(f'ANALYZE "{schema}".{table}')
        yield schema
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_keyed_bucket_reclaim_window_is_index_bounded(
    keyed_reclaim_schema: str, pg_dsn: str
) -> None:
    """The idle keyed rate-limit-bucket sweep's candidate window must seek
    ``rate_limit_buckets_keyed_last_used_idx`` with the idle horizon as an
    Index Cond.

    Keyed buckets are created per rate-limit key, so their count is the
    fleet's live key cardinality. A horizon that survives only as a Filter
    makes every leader tick re-read every in-use bucket to find the
    handful that went idle — cost that grows with tenant count and shows
    up as a maintenance loop that gets slower as the product succeeds."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(
            conn,
            _SWEEP_IDLE_KEYED_BUCKETS_SQL.format(schema=keyed_reclaim_schema),
            _KEYED_HORIZON,
            _KEYED_BATCH,
        )
        _assert_index_cond(
            plan,
            "rate_limit_buckets_keyed_last_used_idx",
            "last_used_at <",
        )
    finally:
        await conn.close()


async def test_keyed_slot_reclaim_window_is_index_bounded(
    keyed_reclaim_schema: str, pg_dsn: str
) -> None:
    """The idle keyed reservation-slot sweep's candidate window must seek
    ``reservation_slots_keyed_last_used_idx`` with the idle horizon as an
    Index Cond — the same property as its bucket sibling, over a table
    that carries one row per slot per key and so grows faster still."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        plan = await _explain(
            conn,
            _SWEEP_IDLE_KEYED_SLOTS_SQL.format(schema=keyed_reclaim_schema),
            _KEYED_HORIZON,
            _KEYED_BATCH,
        )
        _assert_index_cond(
            plan,
            "reservation_slots_keyed_last_used_idx",
            "last_used_at <",
        )
    finally:
        await conn.close()
