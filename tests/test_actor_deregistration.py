"""Integration tests for ``deregister_actor`` — the transactional
``actor_config`` deletion with safety checks (force=False / force=True,
purge_queue).

These require real Postgres (marked ``integration``) because the function
executes hand-written SQL against the fully migrated schema — a fake
connection would only prove the query string looks right, not that
Postgres executes it correctly with enum casts, transactional rollback,
and the ``NOT EXISTS`` subquery for queue purging.
"""

from uuid import uuid4

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.actor_config import ActorConfig
from taskq.actor_config_ops import DeregisterResult, deregister_actor, get_actor_config
from taskq.exceptions import (
    ActorHasActiveJobsError,
    ActorHasEnabledSchedulesError,
    ActorNotFoundError,
)
from taskq.worker.startup import sync_actor_config

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


# ── Helpers ──────────────────────────────────────────────────────────────


async def _ensure_schema(conn: asyncpg.Connection, schema: str) -> None:
    """Drop and re-create the full TaskQ schema via ``apply_pending``."""
    from taskq.migrate import apply_pending

    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await apply_pending(conn, schema=schema)


async def _insert_job(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    status: str,
    queue: str = "default",
) -> str:
    """Insert a minimal job row with the given status; return its UUID string."""
    job_id = uuid4()
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
    schedule_id = uuid4()
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


async def _schedule_enabled(conn: asyncpg.Connection, schema: str, schedule_id: str) -> bool:
    """Return the ``enabled`` value of a cron schedule row."""
    return bool(
        await conn.fetchval(
            f'SELECT enabled FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608
            schedule_id,
        )
    )


def _make_schema() -> str:
    return f"tqd_{new_base62()}".lower()


# ── force=False path ────────────────────────────────────────────────────


async def test_deregister_raises_not_found_for_unknown_actor(pg_conn: asyncpg.Connection) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)

    with pytest.raises(ActorNotFoundError, match="no stored actor_config row"):
        await deregister_actor(pg_conn, "ghost", schema=schema)


async def test_deregister_succeeds_when_no_jobs_or_schedules(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="clean_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )

    result = await deregister_actor(pg_conn, "clean_actor", schema=schema)

    assert isinstance(result, DeregisterResult)
    assert result.actor == "clean_actor"
    assert result.queue == "default"
    assert result.actor_config_deleted is True
    assert result.schedules_disabled == 0
    assert result.jobs_cancelled == 0
    assert result.terminal_jobs_remaining == 0
    assert result.queue_purged is False

    assert await get_actor_config(pg_conn, "clean_actor", schema=schema) is None


async def test_deregister_refuses_with_pending_jobs(pg_conn: asyncpg.Connection) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="busy_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, actor="busy_actor", status="pending")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(pg_conn, "busy_actor", schema=schema)

    assert exc_info.value.active_count == 1
    assert exc_info.value.status_counts == {"pending": 1}

    # Row must still exist — the transaction rolled back.
    assert await get_actor_config(pg_conn, "busy_actor", schema=schema) is not None


async def test_deregister_refuses_with_running_jobs(pg_conn: asyncpg.Connection) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="run_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, actor="run_actor", status="running")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(pg_conn, "run_actor", schema=schema)

    assert exc_info.value.active_count == 1
    assert exc_info.value.status_counts == {"running": 1}
    assert await get_actor_config(pg_conn, "run_actor", schema=schema) is not None


async def test_deregister_refuses_with_enabled_schedules(pg_conn: asyncpg.Connection) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="sched_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    schedule_id = await _insert_schedule(pg_conn, schema, actor="sched_actor", enabled=True)

    with pytest.raises(ActorHasEnabledSchedulesError) as exc_info:
        await deregister_actor(pg_conn, "sched_actor", schema=schema)

    assert exc_info.value.schedule_ids == [schedule_id]
    assert await get_actor_config(pg_conn, "sched_actor", schema=schema) is not None


async def test_deregister_succeeds_with_disabled_schedules(pg_conn: asyncpg.Connection) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="dis_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_schedule(pg_conn, schema, actor="dis_actor", enabled=False)

    result = await deregister_actor(pg_conn, "dis_actor", schema=schema)

    assert result.actor_config_deleted is True
    assert result.schedules_disabled == 0
    assert await get_actor_config(pg_conn, "dis_actor", schema=schema) is None


async def test_deregister_succeeds_with_terminal_jobs(pg_conn: asyncpg.Connection) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="term_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, actor="term_actor", status="succeeded")
    await _insert_job(pg_conn, schema, actor="term_actor", status="failed")

    result = await deregister_actor(pg_conn, "term_actor", schema=schema)

    assert result.actor_config_deleted is True
    assert result.terminal_jobs_remaining == 2
    assert await get_actor_config(pg_conn, "term_actor", schema=schema) is None


# ── force=True path ─────────────────────────────────────────────────────


async def test_deregister_force_cancels_pending_and_disables_schedules(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="force_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    pending_id = await _insert_job(pg_conn, schema, actor="force_actor", status="pending")
    scheduled_id = await _insert_job(pg_conn, schema, actor="force_actor", status="scheduled")
    schedule_id = await _insert_schedule(pg_conn, schema, actor="force_actor", enabled=True)

    result = await deregister_actor(pg_conn, "force_actor", force=True, schema=schema)

    assert result.actor_config_deleted is True
    assert result.jobs_cancelled == 2
    assert result.schedules_disabled == 1
    # The 2 cancelled jobs are now terminal — terminal_jobs_remaining
    # counts all non-pending/scheduled/running rows, including the
    # newly-cancelled ones.
    assert result.terminal_jobs_remaining == 2

    # Verify DB state directly.
    assert await _job_status(pg_conn, schema, pending_id) == "cancelled"
    assert await _job_status(pg_conn, schema, scheduled_id) == "cancelled"
    assert await _schedule_enabled(pg_conn, schema, schedule_id) is False
    assert await get_actor_config(pg_conn, "force_actor", schema=schema) is None


async def test_deregister_force_refuses_with_running_jobs(pg_conn: asyncpg.Connection) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="frun_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, actor="frun_actor", status="running")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(pg_conn, "frun_actor", force=True, schema=schema)

    assert exc_info.value.active_count == 1
    assert exc_info.value.status_counts == {"running": 1}
    assert await get_actor_config(pg_conn, "frun_actor", schema=schema) is not None


async def test_deregister_force_with_running_and_pending_only_reports_running(
    pg_conn: asyncpg.Connection,
) -> None:
    """force=True checks only running jobs — pending jobs are not in the error
    because they would be cancelled, not blocking."""
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="mix_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, actor="mix_actor", status="running")
    await _insert_job(pg_conn, schema, actor="mix_actor", status="pending")

    with pytest.raises(ActorHasActiveJobsError) as exc_info:
        await deregister_actor(pg_conn, "mix_actor", force=True, schema=schema)

    assert exc_info.value.active_count == 1
    assert exc_info.value.status_counts == {"running": 1}
    assert "pending" not in exc_info.value.status_counts
    # Row still exists — transaction rolled back.
    assert await get_actor_config(pg_conn, "mix_actor", schema=schema) is not None
    # The pending job must still be pending — the transaction rolled back
    # on the raise, so the cancel UPDATE never committed.
    pending_count = await pg_conn.fetchval(
        f"SELECT count(*) FROM \"{schema}\".jobs WHERE actor = $1 AND status = 'pending'",  # noqa: S608  # Why: schema validated by _IDENT_RE in apply_pending; actor/status are test constants.
        "mix_actor",
    )
    assert pending_count == 1


async def test_deregister_force_keeps_terminal_history(pg_conn: asyncpg.Connection) -> None:
    """Terminal job rows are never modified — only pending/scheduled are cancelled."""
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="hist_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )
    pending_id = await _insert_job(pg_conn, schema, actor="hist_actor", status="pending")
    succeeded_id = await _insert_job(pg_conn, schema, actor="hist_actor", status="succeeded")
    failed_id = await _insert_job(pg_conn, schema, actor="hist_actor", status="failed")

    result = await deregister_actor(pg_conn, "hist_actor", force=True, schema=schema)

    assert result.jobs_cancelled == 1
    # terminal_jobs_remaining counts all terminal rows including the
    # newly-cancelled pending job: 1 cancelled + 1 succeeded + 1 failed.
    assert result.terminal_jobs_remaining == 3

    assert await _job_status(pg_conn, schema, pending_id) == "cancelled"
    assert await _job_status(pg_conn, schema, succeeded_id) == "succeeded"
    assert await _job_status(pg_conn, schema, failed_id) == "failed"


# ── purge_queue path ─────────────────────────────────────────────────────


async def test_deregister_purge_queue_deletes_orphaned_queue(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await _insert_queue(pg_conn, schema, "solo_queue")
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="solo_actor", max_concurrent=5, queue="solo_queue")],
        schema=schema,
    )

    result = await deregister_actor(
        pg_conn, "solo_actor", purge_queue=True, schema=schema
    )

    assert result.queue == "solo_queue"
    assert result.queue_purged is True
    assert await _queue_exists(pg_conn, schema, "solo_queue") is False


async def test_deregister_purge_queue_keeps_shared_queue(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await _insert_queue(pg_conn, schema, "shared_queue")
    await sync_actor_config(
        pg_conn,
        [
            ActorConfig(actor="actor_a", max_concurrent=5, queue="shared_queue"),
            ActorConfig(actor="actor_b", max_concurrent=5, queue="shared_queue"),
        ],
        schema=schema,
    )

    result = await deregister_actor(
        pg_conn, "actor_a", purge_queue=True, schema=schema
    )

    assert result.queue == "shared_queue"
    assert result.queue_purged is False
    # The queue survives because actor_b still references it.
    assert await _queue_exists(pg_conn, schema, "shared_queue") is True
    # actor_b's row must still exist.
    assert await get_actor_config(pg_conn, "actor_b", schema=schema) is not None


async def test_deregister_without_purge_queue_keeps_queue(
    pg_conn: asyncpg.Connection,
) -> None:
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await _insert_queue(pg_conn, schema, "kept_queue")
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="keep_actor", max_concurrent=5, queue="kept_queue")],
        schema=schema,
    )

    result = await deregister_actor(pg_conn, "keep_actor", schema=schema)

    assert result.queue_purged is False
    assert await _queue_exists(pg_conn, schema, "kept_queue") is True


# ── idempotency ─────────────────────────────────────────────────────────


async def test_double_deregister_raises_not_found(pg_conn: asyncpg.Connection) -> None:
    """A second deregister call on an already-deregistered actor raises ActorNotFoundError.

    This is the primary consumer pattern (cleanup loops using try/except ActorNotFoundError).
    The idempotency guarantee must be tested — an implementation bug that silently returns
    actor_config_deleted=False instead of raising would not be caught otherwise.
    """
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="idem_actor", max_concurrent=5, queue="default")],
        schema=schema,
    )

    result = await deregister_actor(pg_conn, "idem_actor", schema=schema)
    assert result.actor_config_deleted is True

    with pytest.raises(ActorNotFoundError, match="no stored actor_config row"):
        await deregister_actor(pg_conn, "idem_actor", schema=schema)


# ── combined force + purge_queue ────────────────────────────────────────


async def test_deregister_force_with_purge_queue(
    pg_conn: asyncpg.Connection,
) -> None:
    """force=True + purge_queue=True simultaneously — the exact pattern downstream
    consumers (aacrtool) use for ephemeral actor cleanup."""
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await _insert_queue(pg_conn, schema, "ephemeral_queue")
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="ephemeral_actor", max_concurrent=5, queue="ephemeral_queue")],
        schema=schema,
    )
    await _insert_job(pg_conn, schema, actor="ephemeral_actor", status="pending")
    await _insert_job(pg_conn, schema, actor="ephemeral_actor", status="scheduled")

    result = await deregister_actor(
        pg_conn, "ephemeral_actor", force=True, purge_queue=True, schema=schema
    )

    assert result.actor_config_deleted is True
    assert result.jobs_cancelled == 2
    assert result.queue_purged is True
    assert await get_actor_config(pg_conn, "ephemeral_actor", schema=schema) is None
    assert await _queue_exists(pg_conn, schema, "ephemeral_queue") is False


async def test_deregister_purge_queue_noop_when_queue_row_absent(
    pg_conn: asyncpg.Connection,
) -> None:
    """purge_queue=True is a safe no-op when the queues row was never created.

    The queues table is metadata-only and not always populated — operator-managed
    deployments may never create a row. The DELETE returns 0 rows and
    queue_purged is False, which is correct.
    """
    schema = _make_schema()
    await _ensure_schema(pg_conn, schema)
    await sync_actor_config(
        pg_conn,
        [ActorConfig(actor="noqueue_actor", max_concurrent=5, queue="never_created")],
        schema=schema,
    )
    # Deliberately do NOT create a queues row for "never_created".

    result = await deregister_actor(
        pg_conn, "noqueue_actor", purge_queue=True, schema=schema
    )

    assert result.actor_config_deleted is True
    assert result.queue_purged is False
