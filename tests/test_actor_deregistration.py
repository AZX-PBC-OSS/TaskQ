"""Integration tests for ``deregister_actor`` — the transactional
``actor_config`` deletion with safety checks (force=False / force=True,
purge_queue).

These require real Postgres (marked ``integration``) because the function
executes hand-written SQL against the fully migrated schema — a fake
connection would only prove the query string looks right, not that
Postgres executes it correctly with enum casts, transactional rollback,
and the ``NOT EXISTS`` subquery for queue purging.
"""

import asyncio

import asyncpg
import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import DeregisterResult, deregister_actor, get_actor_config
from taskq.exceptions import (
    ActorHasActiveJobsError,
    ActorHasEnabledSchedulesError,
    ActorNotFoundError,
)
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker.startup import sync_actor_config

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


# ── Helpers ──────────────────────────────────────────────────────────────


async def _insert_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    status: str,
    queue: str = "default",
) -> str:
    """Insert a minimal job row with the given status; return its UUID string."""
    job_id = new_job_id()
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
            id, actor, queue, payload, max_attempts, retry_kind, status
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, $6, $7::"{schema}".job_status
        )""",  # noqa: S608  # Why: schema validated by _IDENT_RE in apply_pending; actor/status/queue are test constants, not user input.
        job_id,
        actor,
        queue,
        '{"key": "value"}',
        3,
        "transient",
        status,
    )
    return str(job_id)


async def _insert_schedule(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    enabled: bool = True,
) -> str:
    """Insert a cron schedule row; return its UUID string."""
    schedule_id = new_uuid()
    await conn.execute(
        f"""INSERT INTO "{schema}".cron_schedules (
            id, actor, cron_expr, enabled, next_fire_at
        ) VALUES (
            $1, $2, $3, $4, now()
        )""",  # noqa: S608  # Why: schema validated by _IDENT_RE in apply_pending; actor is a test constant.
        schedule_id,
        actor,
        "*/5 * * * *",
        enabled,
    )
    return str(schedule_id)


async def _insert_queue(
    conn: asyncpg.Connection,
    schema: str,
    name: str,
) -> None:
    """Insert a queue row (the ``queues`` table is not populated by sync_actor_config)."""
    await conn.execute(
        f'INSERT INTO "{schema}".queues (name) VALUES ($1)',  # noqa: S608  # Why: schema validated by _IDENT_RE in apply_pending; name is a test constant.
        name,
    )


async def _queue_exists(conn: asyncpg.Connection, schema: str, name: str) -> bool:
    """Check whether a queue row exists."""
    return bool(
        await conn.fetchval(
            f'SELECT 1 FROM "{schema}".queues WHERE name = $1',  # noqa: S608
            name,
        )
    )


async def _job_status(conn: asyncpg.Connection, schema: str, job_id: str) -> str:
    """Return the current status of a job row."""
    return str(
        await conn.fetchval(
            f'SELECT status::text FROM "{schema}".jobs WHERE id = $1',  # noqa: S608
            job_id,
        )
    )


async def _schedule_state(conn: asyncpg.Connection, schema: str, schedule_id: str) -> str | None:
    """Return 'enabled', 'disabled', or None (deleted)."""
    row = await conn.fetchrow(
        f'SELECT enabled FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608
        schedule_id,
    )
    if row is None:
        return None
    return "enabled" if row["enabled"] else "disabled"


# ── force=False path ────────────────────────────────────────────────────


async def test_deregister_raises_not_found_for_unknown_actor(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name

    with pytest.raises(ActorNotFoundError, match="no stored actor_config row"):
        await deregister_actor(clean_pg_conn, "ghost", schema=schema)


async def test_deregister_succeeds_when_no_jobs_or_schedules(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="clean_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )

    result = await deregister_actor(clean_pg_conn, "clean_actor", schema=schema)

    assert isinstance(result, DeregisterResult)
    assert result.actor == "clean_actor"
    assert result.queue == "default"
    assert result.actor_config_deleted is True
    assert result.schedules_disabled == 0
    assert result.jobs_cancelled == 0
    assert result.terminal_jobs_remaining == 0
    assert result.queue_purged is False

    assert await get_actor_config(clean_pg_conn, "clean_actor", schema=schema) is None


async def test_deregister_refuses_with_pending_jobs(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="busy_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(clean_pg_conn, schema, actor="busy_actor", status="pending")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(clean_pg_conn, "busy_actor", schema=schema)

    assert exc_info.value.active_count == 1
    assert exc_info.value.status_counts == {"pending": 1}

    # Row must still exist — the transaction rolled back.
    assert await get_actor_config(clean_pg_conn, "busy_actor", schema=schema) is not None


async def test_deregister_refuses_with_running_jobs(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="run_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(clean_pg_conn, schema, actor="run_actor", status="running")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(clean_pg_conn, "run_actor", schema=schema)

    assert exc_info.value.active_count == 1
    assert exc_info.value.status_counts == {"running": 1}
    assert await get_actor_config(clean_pg_conn, "run_actor", schema=schema) is not None


async def test_deregister_refuses_with_enabled_schedules(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="sched_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    schedule_id = await _insert_schedule(clean_pg_conn, schema, actor="sched_actor", enabled=True)

    with pytest.raises(ActorHasEnabledSchedulesError) as exc_info:
        await deregister_actor(clean_pg_conn, "sched_actor", schema=schema)

    assert exc_info.value.schedule_ids == [schedule_id]
    assert await get_actor_config(clean_pg_conn, "sched_actor", schema=schema) is not None


async def test_deregister_succeeds_with_disabled_schedules(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="dis_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_schedule(clean_pg_conn, schema, actor="dis_actor", enabled=False)

    result = await deregister_actor(clean_pg_conn, "dis_actor", schema=schema)

    assert result.actor_config_deleted is True
    assert result.schedules_disabled == 0
    assert await get_actor_config(clean_pg_conn, "dis_actor", schema=schema) is None


async def test_deregister_succeeds_with_terminal_jobs(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="term_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(clean_pg_conn, schema, actor="term_actor", status="succeeded")
    await _insert_job(clean_pg_conn, schema, actor="term_actor", status="failed")

    result = await deregister_actor(clean_pg_conn, "term_actor", schema=schema)

    assert result.actor_config_deleted is True
    assert result.terminal_jobs_remaining == 2
    assert await get_actor_config(clean_pg_conn, "term_actor", schema=schema) is None


# ── force=True path ─────────────────────────────────────────────────────


async def test_deregister_force_cancels_pending_and_disables_schedules(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="force_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    pending_id = await _insert_job(clean_pg_conn, schema, actor="force_actor", status="pending")
    scheduled_id = await _insert_job(clean_pg_conn, schema, actor="force_actor", status="scheduled")
    schedule_id = await _insert_schedule(clean_pg_conn, schema, actor="force_actor", enabled=True)

    result = await deregister_actor(clean_pg_conn, "force_actor", force=True, schema=schema)

    assert result.actor_config_deleted is True
    assert result.jobs_cancelled == 2
    assert result.schedules_disabled == 1
    # The 2 cancelled jobs are now terminal — terminal_jobs_remaining
    # counts all non-pending/scheduled/running rows, including the
    # newly-cancelled ones.
    assert result.terminal_jobs_remaining == 2

    # Verify DB state directly.
    assert await _job_status(clean_pg_conn, schema, pending_id) == "cancelled"
    assert await _job_status(clean_pg_conn, schema, scheduled_id) == "cancelled"
    assert await _schedule_state(clean_pg_conn, schema, schedule_id) == "disabled"
    assert await get_actor_config(clean_pg_conn, "force_actor", schema=schema) is None

    # Verify audit trail fields on cancelled jobs (M12).
    job_row = await clean_pg_conn.fetchrow(
        f"SELECT error_class, error_message, finished_at "  # noqa: S608  # Why: schema validated by _IDENT_RE; pending_id is a test-generated UUID.
        f'FROM "{schema}".jobs WHERE id = $1',
        pending_id,
    )
    assert job_row is not None
    assert job_row["error_class"] == "ActorDeregistered"
    assert job_row["error_message"] is not None
    assert "actor deregistration" in job_row["error_message"]
    assert job_row["finished_at"] is not None


async def test_deregister_force_writes_job_events_on_cancel(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """H3: force=True cancel must insert job_events state_change rows —
    every other cancel path does, and audit consumers rely on them."""
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="evt_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    pending_id = await _insert_job(clean_pg_conn, schema, actor="evt_actor", status="pending")
    scheduled_id = await _insert_job(clean_pg_conn, schema, actor="evt_actor", status="scheduled")

    await deregister_actor(clean_pg_conn, "evt_actor", force=True, schema=schema)

    for jid in (pending_id, scheduled_id):
        events = await clean_pg_conn.fetch(
            f"SELECT kind, detail::text AS detail "  # noqa: S608  # Why: schema validated by _IDENT_RE; jid is a test UUID.
            f'  FROM "{schema}".job_events '
            f" WHERE job_id = $1 "
            f" ORDER BY occurred_at",
            jid,
        )
        assert len(events) >= 1, f"no job_events for cancelled job {jid}"
        state_changes = [e for e in events if e["kind"] == "state_change"]
        assert len(state_changes) == 1
        import json

        detail = json.loads(state_changes[0]["detail"])
        assert detail["to_state"] == "cancelled"
        assert detail["reason"] == "actor_deregistered"


async def test_deregister_force_refuses_with_running_jobs(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="frun_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(clean_pg_conn, schema, actor="frun_actor", status="running")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(clean_pg_conn, "frun_actor", force=True, schema=schema)

    assert exc_info.value.active_count == 1
    assert exc_info.value.status_counts == {"running": 1}
    assert await get_actor_config(clean_pg_conn, "frun_actor", schema=schema) is not None


async def test_deregister_force_with_running_and_pending_only_reports_running(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """force=True checks only running jobs — pending jobs are not in the error
    because they would be cancelled, not blocking."""
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="mix_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(clean_pg_conn, schema, actor="mix_actor", status="running")
    await _insert_job(clean_pg_conn, schema, actor="mix_actor", status="pending")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(clean_pg_conn, "mix_actor", force=True, schema=schema)

    assert exc_info.value.active_count == 1
    assert exc_info.value.status_counts == {"running": 1}
    assert "pending" not in exc_info.value.status_counts
    # Row still exists — transaction rolled back.
    assert await get_actor_config(clean_pg_conn, "mix_actor", schema=schema) is not None
    # The pending job must still be pending — the transaction rolled back
    # on the raise, so the cancel UPDATE never committed.
    pending_count = await clean_pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = $1 AND status = 'pending'",  # noqa: S608  # Why: schema validated by _IDENT_RE in apply_pending; actor/status are test constants.
        "mix_actor",
    )
    assert pending_count == 1


async def test_deregister_force_keeps_terminal_history(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Terminal job rows are never modified — only pending/scheduled are cancelled."""
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="hist_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    pending_id = await _insert_job(clean_pg_conn, schema, actor="hist_actor", status="pending")
    succeeded_id = await _insert_job(clean_pg_conn, schema, actor="hist_actor", status="succeeded")
    failed_id = await _insert_job(clean_pg_conn, schema, actor="hist_actor", status="failed")

    result = await deregister_actor(clean_pg_conn, "hist_actor", force=True, schema=schema)

    assert result.jobs_cancelled == 1
    # terminal_jobs_remaining counts all terminal rows including the
    # newly-cancelled pending job: 1 cancelled + 1 succeeded + 1 failed.
    assert result.terminal_jobs_remaining == 3

    assert await _job_status(clean_pg_conn, schema, pending_id) == "cancelled"
    assert await _job_status(clean_pg_conn, schema, succeeded_id) == "succeeded"
    assert await _job_status(clean_pg_conn, schema, failed_id) == "failed"


# ── purge_queue path ─────────────────────────────────────────────────────


async def test_deregister_purge_queue_deletes_orphaned_queue(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    await _insert_queue(clean_pg_conn, schema, "solo_queue")
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="solo_actor", max_concurrent=5, queue="solo_queue")],
        schema=schema,
    )

    result = await deregister_actor(clean_pg_conn, "solo_actor", purge_queue=True, schema=schema)

    assert result.queue == "solo_queue"
    assert result.queue_purged is True
    assert await _queue_exists(clean_pg_conn, schema, "solo_queue") is False


async def test_deregister_purge_queue_keeps_shared_queue(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    await _insert_queue(clean_pg_conn, schema, "shared_queue")
    await sync_actor_config(
        clean_pg_conn,
        [
            ActorConfig(actor="shared_a", max_concurrent=5, queue="shared_queue"),
            ActorConfig(actor="shared_b", max_concurrent=5, queue="shared_queue"),
        ],
        schema=schema,
    )

    result = await deregister_actor(clean_pg_conn, "shared_a", purge_queue=True, schema=schema)

    assert result.queue == "shared_queue"
    assert result.queue_purged is False
    # The queue survives because shared_b still references it.
    assert await _queue_exists(clean_pg_conn, schema, "shared_queue") is True
    # shared_b's row must still exist.
    assert await get_actor_config(clean_pg_conn, "shared_b", schema=schema) is not None


async def test_deregister_without_purge_queue_keeps_queue(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    schema = module_pg_schema.schema_name
    await _insert_queue(clean_pg_conn, schema, "kept_queue")
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="keep_actor", max_concurrent=5, queue="kept_queue")],
        schema=schema,
    )

    result = await deregister_actor(clean_pg_conn, "keep_actor", schema=schema)

    assert result.queue_purged is False
    assert await _queue_exists(clean_pg_conn, schema, "kept_queue") is True


async def test_deregister_purge_keeps_queue_referenced_by_active_jobs_of_other_actors(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """jobs.actor is plain text with no FK, and jobs enqueued to
    unregistered actors are an EXPECTED state (the CLI warns about them).
    The purge guard must therefore also look at the jobs table: dropping
    the queues row while non-terminal jobs still reference the queue
    silently discards their leased-slot cap and dispatch mode (a missing
    row defaults to strict_fifo)."""
    schema = module_pg_schema.schema_name
    await _insert_queue(clean_pg_conn, schema, "busy_queue")
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="owner_actor", max_concurrent=5, queue="busy_queue")],
        schema=schema,
    )
    # An unregistered actor's pending job on the same queue — exactly the
    # state the purge's actor_config-only guard cannot see.
    await _insert_job(
        clean_pg_conn, schema, actor="never_registered", status="pending", queue="busy_queue"
    )

    result = await deregister_actor(clean_pg_conn, "owner_actor", purge_queue=True, schema=schema)

    assert result.queue_purged is False
    assert await _queue_exists(clean_pg_conn, schema, "busy_queue") is True


async def test_deregister_purge_still_deletes_queue_with_only_terminal_job_references(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Terminal history never blocks the purge — those rows are done with
    the queue's cap and mode; only non-terminal jobs still depend on them."""
    schema = module_pg_schema.schema_name
    await _insert_queue(clean_pg_conn, schema, "done_queue")
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="owner_two", max_concurrent=5, queue="done_queue")],
        schema=schema,
    )
    await _insert_job(
        clean_pg_conn, schema, actor="never_registered", status="succeeded", queue="done_queue"
    )

    result = await deregister_actor(clean_pg_conn, "owner_two", purge_queue=True, schema=schema)

    assert result.queue_purged is True
    assert await _queue_exists(clean_pg_conn, schema, "done_queue") is False


# ── idempotency ─────────────────────────────────────────────────────────


async def test_double_deregister_raises_not_found(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A second deregister call on an already-deregistered actor raises ActorNotFoundError.

    This is the primary consumer pattern (cleanup loops using try/except ActorNotFoundError).
    The idempotency guarantee must be tested — an implementation bug that silently returns
    actor_config_deleted=False instead of raising would not be caught otherwise.
    """
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="idem_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )

    result = await deregister_actor(clean_pg_conn, "idem_actor", schema=schema)
    assert result.actor_config_deleted is True

    with pytest.raises(ActorNotFoundError, match="no stored actor_config row"):
        await deregister_actor(clean_pg_conn, "idem_actor", schema=schema)


# ── combined force + purge_queue ────────────────────────────────────────


async def test_deregister_force_with_purge_queue(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """force=True + purge_queue=True simultaneously — the exact pattern downstream
    consumers (aacrtool) use for ephemeral actor cleanup."""
    schema = module_pg_schema.schema_name
    await _insert_queue(clean_pg_conn, schema, "ephemeral_queue")
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="ephemeral_actor", max_concurrent=5, queue="ephemeral_queue")],
        schema=schema,
    )
    await _insert_job(clean_pg_conn, schema, actor="ephemeral_actor", status="pending")
    await _insert_job(clean_pg_conn, schema, actor="ephemeral_actor", status="scheduled")

    result = await deregister_actor(
        clean_pg_conn, "ephemeral_actor", force=True, purge_queue=True, schema=schema
    )

    assert result.actor_config_deleted is True
    assert result.jobs_cancelled == 2
    assert result.queue_purged is True
    assert await get_actor_config(clean_pg_conn, "ephemeral_actor", schema=schema) is None
    assert await _queue_exists(clean_pg_conn, schema, "ephemeral_queue") is False


async def test_deregister_purge_queue_noop_when_queue_row_absent(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """purge_queue=True is a safe no-op when the queues row was never created.

    The queues table is metadata-only and not always populated — operator-managed
    deployments may never create a row. The DELETE returns 0 rows and
    queue_purged is False, which is correct.
    """
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="noqueue_actor", max_concurrent=5, queue="never_created")],
        schema=schema,
    )
    # Deliberately do NOT create a queues row for "never_created".

    result = await deregister_actor(clean_pg_conn, "noqueue_actor", purge_queue=True, schema=schema)

    assert result.actor_config_deleted is True
    assert result.queue_purged is False


# ── concurrent deregistration ────────────────────────────────────────────


async def test_concurrent_force_deregister_one_succeeds_one_raises(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Two concurrent force=True deregister calls: one wins, one raises.

    Both connections target the same actor that has a pending job and an
    enabled schedule. Under READ COMMITTED, row-level locking serializes
    the UPDATEs: the first transaction locks the job rows, the second
    blocks, then sees 0 rows after the first commits. The DELETE returns
    a row for only one transaction; the other gets 0 rows and raises
    ActorNotFoundError.
    """
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="concurrent_force_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    job_id = await _insert_job(
        clean_pg_conn, schema, actor="concurrent_force_actor", status="pending"
    )
    sched_id = await _insert_schedule(
        clean_pg_conn, schema, actor="concurrent_force_actor", enabled=True
    )

    conn2 = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        results: list[BaseException | DeregisterResult] = []

        async def _deregister(conn: asyncpg.Connection) -> None:
            try:
                result = await deregister_actor(
                    conn, "concurrent_force_actor", force=True, schema=schema
                )
                results.append(result)
            except ActorNotFoundError as exc:
                results.append(exc)

        await asyncio.gather(
            _deregister(clean_pg_conn),
            _deregister(conn2),
        )

        successes = [r for r in results if isinstance(r, DeregisterResult)]
        not_found = [r for r in results if isinstance(r, ActorNotFoundError)]
        assert len(successes) == 1
        assert len(not_found) == 1

        # The winner should have cancelled exactly 1 job and disabled 1 schedule
        # — not double-cancelled by both transactions.
        assert successes[0].jobs_cancelled == 1
        assert successes[0].schedules_disabled == 1

        # Verify final DB state — job cancelled, schedule disabled, actor_config gone.
        assert (
            await get_actor_config(clean_pg_conn, "concurrent_force_actor", schema=schema) is None
        )
        assert await _job_status(clean_pg_conn, schema, job_id) == "cancelled"
        assert await _schedule_state(clean_pg_conn, schema, sched_id) == "disabled"
    finally:
        await conn2.close()


# ── refusal gap coverage (L16) ───────────────────────────────────────────


async def test_deregister_refuses_with_scheduled_jobs(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """force=False refuses with scheduled (not just pending) jobs."""
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="sched_job_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(clean_pg_conn, schema, actor="sched_job_actor", status="scheduled")
    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(clean_pg_conn, "sched_job_actor", schema=schema)
    assert exc_info.value.status_counts == {"scheduled": 1}


async def test_deregister_active_jobs_takes_precedence_over_schedules(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """When both active jobs AND enabled schedules exist, the jobs error is raised first."""
    schema = module_pg_schema.schema_name
    await sync_actor_config(
        clean_pg_conn,
        [ActorConfig(actor="precedence_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(clean_pg_conn, schema, actor="precedence_actor", status="pending")
    await _insert_schedule(clean_pg_conn, schema, actor="precedence_actor", enabled=True)
    with pytest.raises(ActorHasActiveJobsError):
        await deregister_actor(clean_pg_conn, "precedence_actor", schema=schema)


async def test_deregister_invalid_schema_raises_value_error(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Invalid schema identifier raises ValueError before any DB access."""
    with pytest.raises(ValueError, match="invalid schema identifier"):
        await deregister_actor(clean_pg_conn, "any_actor", schema="bad; DROP TABLE")
