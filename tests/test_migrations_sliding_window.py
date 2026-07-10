"""Integration tests for the sliding-window migration.

Asserts that ``rate_limit_window_entries`` table exists with the expected
columns, primary key, and lookup index after applying pending migrations.
"""

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq.settings import TaskQSettings

pytestmark = pytest.mark.integration


async def test_rate_limit_window_entries_table_exists(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    rows = await pg_conn.fetch(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'rate_limit_window_entries'
        ORDER BY ordinal_position
        """,
        settings.schema_name,
    )
    col_map = {r["column_name"]: r for r in rows}
    assert "bucket_name" in col_map
    assert col_map["bucket_name"]["data_type"] == "text"
    assert "ts" in col_map
    assert col_map["ts"]["data_type"] == "timestamp with time zone"
    assert "request_id" in col_map
    assert col_map["request_id"]["data_type"] == "uuid"


async def test_rate_limit_window_entries_primary_key(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    rows = await pg_conn.fetch(
        """
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
          AND c.relname = 'rate_limit_window_entries'
          AND i.indisprimary
        ORDER BY a.attname
        """,
        settings.schema_name,
    )
    pk_cols = {r["attname"] for r in rows}
    assert pk_cols == {"bucket_name", "ts", "request_id"}


async def test_rate_limit_window_entries_lookup_index(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

    row = await pg_conn.fetchrow(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = $1 AND indexname = 'rate_limit_window_entries_lookup'
        """,
        settings.schema_name,
    )
    assert row is not None
    idx_def: str = row["indexdef"]
    assert "bucket_name" in idx_def
    assert "ts" in idx_def
