# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team attacks on retry_job's completed-batch membership lie.

``retry_job`` accepts any failed/crashed/cancelled member and resets it to
pending with NO batch awareness (src/taskq/backend/_sql_templates.py:1178-1193).
Every batch-status writer guards on ``b.status = 'active'`` —
``complete_batch`` / ``abort_batch`` (src/taskq/backend/_batch_sql.py:126-147)
and the leader's ``complete_stale_batches``
(src/taskq/worker/_leader_shared.py:674) — so after a batch is driven to
``complete``, a retried member re-enters non-terminal membership under a row
that still claims completion, and no writer reconciles it:
``wait_for_batch`` snoozes forever on the batch (src/taskq/batch.py:571-572).

The contract: a batch's status must not lie about its membership — reconcile
on retry, or refuse retry_job for completed-batch members.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._batch_sql import (
    abort_batch,
    complete_batch,
    create_batch,
    get_batch,
    render_batch_sql,
)
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.batch import wait_for_batch
from taskq.exceptions import Snooze
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.worker._leader_shared import complete_stale_batches

pytestmark = pytest.mark.integration

# No actor_config row is seeded for this actor: the re-pended member can
# never dispatch (the dispatch CTE requires an actor_config row), freezing
# the strand permanently.
_MEMBER_ACTOR = "no_config_batch_actor"


class _StubBackendDeps:
    """Minimal duck-typed BackendDeps: settings + pools only."""

    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.worker_pool: object | None = None
        self.heartbeat_pool: object | None = None
        self.dispatcher_pool: object | None = None


def _pool_backend(schema: str, pool: asyncpg.Pool) -> PostgresBackend:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SCHEMA_NAME": schema,
        },
        validate=False,
    )
    deps = _StubBackendDeps(settings)
    deps.worker_pool = pool
    deps.heartbeat_pool = pool
    deps.dispatcher_pool = pool
    return PostgresBackend(
        deps,  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; only settings + pools are read on the paths under test.
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )


async def _seed_completed_batch_with_failed_member(
    conn: asyncpg.Connection, schema: str
) -> tuple[UUID, UUID, UUID]:
    """A legitimately-completed batch (all members terminal) with one failed
    member; returns (batch_id, succeeded_member_id, failed_member_id)."""
    batch_sql = render_batch_sql(schema)
    bid = new_uuid()
    await create_batch(
        conn,
        batch_sql,
        bid,
        "default",
        expected_size=2,
        failure_threshold=None,
        finalizer_job_id=None,
        originating_actor=None,
    )
    succeeded_id = await _insert_member(conn, schema, bid, status="succeeded")
    failed_id = await _insert_member(conn, schema, bid, status="failed")
    await complete_batch(conn, batch_sql, bid)
    row = await get_batch(conn, batch_sql, bid)
    assert row is not None and row.status == "complete", (
        f"setup pin: batch must be legitimately complete before the attack; got {row!r}"
    )
    return bid, succeeded_id, failed_id


async def _insert_member(conn: asyncpg.Connection, schema: str, bid: UUID, *, status: str) -> UUID:
    jid = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, max_attempts, retry_kind, status, scheduled_at, "
        " finished_at, metadata) "
        "VALUES ($1, $2, 'default', '{}'::jsonb, 3, 'transient', $3, clock_timestamp(), "
        "clock_timestamp(), $4::jsonb)",
        jid,
        _MEMBER_ACTOR,
        status,
        f'{{"batch_id": "{bid}"}}',
    )
    return jid


async def _member_status(conn: asyncpg.Connection, schema: str, jid: UUID) -> str:
    status = await conn.fetchval(f'SELECT status FROM "{schema}".jobs WHERE id = $1', jid)
    assert status is not None
    return str(status)


async def test_retry_job_on_completed_batch_member_leaves_status_lying(pg_dsn: str) -> None:
    """After retry_job re-pends a failed member of a COMPLETED batch, every
    batch writer must reconcile the row (or retry_job must have refused).
    Today the row still says 'complete' while the member is pending again —
    and wait_for_batch snoozes forever on it."""
    schema = f"torp_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        bid, _, failed_id = await _seed_completed_batch_with_failed_member(conn, schema)
        batch_sql = render_batch_sql(schema)

        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
        backend = _pool_backend(schema, pool)
        retried = await backend.retry_job(failed_id)
        assert retried is True, "setup pin: retry_job must accept the failed member today"

        member_after_retry = await _member_status(conn, schema, failed_id)
        row_after_retry = await get_batch(conn, batch_sql, bid)
        assert row_after_retry is not None
        batch_after_retry = row_after_retry.status

        # Every writer the system offers for batch status:
        await complete_batch(conn, batch_sql, bid)  # guard: b.status = 'active'
        stale = await complete_stale_batches(conn, schema=schema)  # guard: b.status = 'active'

        # The strand is permanent (no actor_config → never dispatched), so the
        # finalizer's wait_for_batch can never settle: it snoozes on pending
        # membership under a row that claims completion.
        snoozed = False
        try:
            await wait_for_batch(conn, bid, schema=schema, snooze_interval=timedelta(seconds=1))
        except Snooze:
            snoozed = True

        row_final = await get_batch(conn, batch_sql, bid)
        member_final = await _member_status(conn, schema, failed_id)
        assert row_final is not None
        assert not (row_final.status == "complete" and member_final == "pending"), (
            "Contract: a batch's status must not lie about its membership — reconcile on "
            "retry, or refuse retry_job for completed-batch members. Current behavior "
            f"violates it: retry_job returned {retried} and reset the failed member to "
            f"'{member_after_retry}' while the batch row still said "
            f"{batch_after_retry!r}; after every batch "
            f"writer ran (complete_batch: no-op, its 'active' guard; complete_stale_batches: "
            f"{stale} rows, its 'active' guard at src/taskq/worker/_leader_shared.py:674) "
            f"the row still says {row_final.status!r} with the member {member_final!r} — and "
            f"wait_for_batch snoozed forever on it (snoozed={snoozed}, "
            "src/taskq/batch.py:571-572)."
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_abort_batch_on_the_lying_row_cancels_member_but_row_still_claims_complete(
    pg_dsn: str,
) -> None:
    """Even the operator's abort cannot repair the lie: abort_batch's jobs
    arm cancels the re-pended member (no batch-status guard) but the row
    update guards on ``status = 'active'`` — so a batch that was aborted in
    substance still claims 'complete'."""
    schema = f"torp_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    pool: asyncpg.Pool | None = None
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        bid, _, failed_id = await _seed_completed_batch_with_failed_member(conn, schema)
        batch_sql = render_batch_sql(schema)

        pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
        backend = _pool_backend(schema, pool)
        assert await backend.retry_job(failed_id) is True

        cancelled = await abort_batch(conn, batch_sql, bid)
        row = await get_batch(conn, batch_sql, bid)
        member = await _member_status(conn, schema, failed_id)
        assert row is not None
        assert row.status == "aborted", (
            "Contract: a batch whose members were just aborted must be recorded as aborted — "
            "the row must not claim an outcome it did not have. Current behavior violates "
            f"it: abort_batch cancelled {cancelled} member(s) (member now {member!r}) but the "
            f"batches row still says {row.status!r} — `_ABORT_BATCH_ROW_SQL` guards on "
            "status = 'active' (src/taskq/backend/_batch_sql.py:129), so the abort that "
            "actually happened is unrecorded forever."
        )
    finally:
        if pool is not None:
            await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()
