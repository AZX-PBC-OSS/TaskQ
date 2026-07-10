"""End-to-end check that bundled migrations apply cleanly to a real PG18.

If these pass, the schema in :mod:`taskq.migrations` is loadable, the
runner records every file, and a second ``apply_pending`` call is a
no-op (idempotency).
"""

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq.settings import TaskQSettings

pytestmark = pytest.mark.integration


async def test_discover_finds_initial_migration() -> None:
    migrations = migrate_mod.discover()
    assert migrations, "expected at least one bundled migration"
    first = migrations[0]
    assert first.version == "01.00.00_01"
    assert first.phase == "pre"
    assert first.description == "initial"


async def test_apply_pending_creates_schema(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    applied = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    assert len(applied) == len(migrate_mod.discover())

    rows = await pg_conn.fetch(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = $1
        ORDER BY table_name
        """,
        settings.schema_name,
    )
    table_names = {r["table_name"] for r in rows}
    assert {
        "actor_config",
        "cron_schedules",
        "job_attempts",
        "job_attempts_archive",
        "job_events",
        "jobs",
        "jobs_archive",
        "maintenance_leader",
        "rate_limit_buckets",
        "reservation_slots",
        "schema_migrations",
        "workers",
    } <= table_names


async def test_apply_pending_is_idempotent(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    first = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    assert first, "expected initial run to apply migrations"

    second = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    assert second == [], "second apply should be a no-op"


async def test_dispatch_index_exists(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    """Spot-check the most performance-critical index from"""
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
    row = await pg_conn.fetchrow(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = $1 AND indexname = 'jobs_dispatch_idx'
        """,
        settings.schema_name,
    )
    assert row is not None
    assert "queue" in row["indexdef"]
    assert "priority" in row["indexdef"]
    assert "scheduled_at" in row["indexdef"]
    assert "status = 'pending'" in row["indexdef"]


# ── Archive tables () ──────────────────────────────────


async def test_jobs_archive_columns_match_jobs_plus_archive_fields(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    jobs_cols = await pg_conn.fetch(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'jobs'
        ORDER BY ordinal_position
        """,
        settings.schema_name,
    )
    archive_cols = await pg_conn.fetch(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'jobs_archive'
        ORDER BY ordinal_position
        """,
        settings.schema_name,
    )

    jobs_col_map = {r["column_name"]: r for r in jobs_cols}
    archive_col_map = {r["column_name"]: r for r in archive_cols}

    for name in jobs_col_map:
        assert name in archive_col_map, f"jobs column {name!r} missing from jobs_archive"

    assert "archived_at" in archive_col_map
    assert archive_col_map["archived_at"]["is_nullable"] == "NO"
    assert "expire_at" in archive_col_map
    assert archive_col_map["expire_at"]["is_nullable"] == "NO"


async def test_job_attempts_archive_columns_match_job_attempts(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    attempts_cols = await pg_conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'job_attempts'
        ORDER BY ordinal_position
        """,
        settings.schema_name,
    )
    archive_cols = await pg_conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'job_attempts_archive'
        ORDER BY ordinal_position
        """,
        settings.schema_name,
    )

    attempts_names = {r["column_name"] for r in attempts_cols}
    archive_names = {r["column_name"] for r in archive_cols}
    assert attempts_names == archive_names, (
        f"job_attempts columns {attempts_names - archive_names} missing from "
        f"job_attempts_archive; extra: {archive_names - attempts_names}"
    )

    fk_rows = await pg_conn.fetch(
        """
        SELECT tc.constraint_name, kcu.column_name,
               ccu.table_name AS ref_table
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON tc.constraint_name = ccu.constraint_name
            AND tc.table_schema = ccu.table_schema
        WHERE tc.table_schema = $1
            AND tc.table_name = 'job_attempts_archive'
            AND tc.constraint_type = 'FOREIGN KEY'
        """,
        settings.schema_name,
    )
    assert any(
        r["column_name"] == "job_id" and r["ref_table"] == "jobs_archive" for r in fk_rows
    ), "job_attempts_archive.job_id must reference jobs_archive"

    fk_delete_rows = await pg_conn.fetch(
        """
        SELECT rc.delete_rule
        FROM information_schema.referential_constraints rc
        JOIN information_schema.table_constraints tc
            ON rc.constraint_name = tc.constraint_name
            AND rc.constraint_schema = tc.constraint_schema
        WHERE tc.table_schema = $1
            AND tc.table_name = 'job_attempts_archive'
            AND tc.constraint_type = 'FOREIGN KEY'
        """,
        settings.schema_name,
    )
    assert any(r["delete_rule"] == "CASCADE" for r in fk_delete_rows), (
        "job_attempts_archive FK must use ON DELETE CASCADE"
    )


async def test_archive_indexes_exist(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    index_names = await pg_conn.fetch(
        """
        SELECT indexname FROM pg_indexes
        WHERE schemaname = $1
            AND indexname IN (
                'jobs_archive_expire_at_idx',
                'jobs_archive_finished_at_idx',
                'job_attempts_archive_job_id_idx'
            )
        """,
        settings.schema_name,
    )
    found = {r["indexname"] for r in index_names}
    assert {
        "jobs_archive_expire_at_idx",
        "jobs_archive_finished_at_idx",
        "job_attempts_archive_job_id_idx",
    } <= found


async def test_archive_table_comments_exist(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    for table in ("jobs_archive", "job_attempts_archive"):
        row = await pg_conn.fetchrow(
            """
            SELECT obj_description(
                ($1 || '.' || $2)::regclass, 'pg_class'
            ) AS comment
            """,
            settings.schema_name,
            table,
        )
        assert row is not None and row["comment"] is not None, f"{table} missing table comment"


async def test_cron_schedules_has_consecutive_failures_column(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    rows = await pg_conn.fetch(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = $1
            AND table_name = 'cron_schedules'
            AND column_name = 'consecutive_failures'
        """,
        settings.schema_name,
    )
    assert len(rows) == 1, "consecutive_failures column missing from cron_schedules"
    col = rows[0]
    assert col["data_type"] == "integer"
    assert col["is_nullable"] == "NO"
    assert col["column_default"] == "0"
