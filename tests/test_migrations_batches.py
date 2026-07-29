"""Integration test for the batches table migration (01.00.05_01_pre_batches).

Verifies the ``batches`` table, its columns, defaults, and indexes exist
after applying all pending migrations to a fresh schema.
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62

pytestmark = pytest.mark.integration

_REQUIRED_COLUMNS = {
    "id",
    "queue",
    "status",
    "expected_size",
    "consecutive_failures",
    "failure_threshold",
    "finalizer_job_id",
    "originating_actor",
    "created_at",
    "completed_at",
    "metadata",
}


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_batches_table_exists(pg_dsn: str) -> None:
    schema = f"mig_batches_tbl_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        exists = await conn.fetchval(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = 'batches'
            )
            """,
            schema,
        )
        assert exists, "batches table should exist after migration"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_batches_columns(pg_dsn: str) -> None:
    schema = f"mig_batches_cols_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        rows = await conn.fetch(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = 'batches'
            """,
            schema,
        )
        col_map = {r["column_name"]: r for r in rows}
        missing = _REQUIRED_COLUMNS - col_map.keys()
        assert not missing, f"missing columns: {missing}"

        assert col_map["id"]["data_type"] == "uuid"
        assert col_map["queue"]["data_type"] == "text"
        assert col_map["queue"]["is_nullable"] == "NO"

        assert col_map["status"]["data_type"] == "text"
        assert col_map["status"]["is_nullable"] == "NO"
        assert col_map["status"]["column_default"] is not None
        assert "'active'" in col_map["status"]["column_default"]

        assert col_map["expected_size"]["data_type"] == "integer"
        assert col_map["expected_size"]["is_nullable"] == "NO"
        assert col_map["expected_size"]["column_default"] is not None
        assert "0" in col_map["expected_size"]["column_default"]

        assert col_map["consecutive_failures"]["data_type"] == "integer"
        assert col_map["consecutive_failures"]["is_nullable"] == "NO"
        assert col_map["consecutive_failures"]["column_default"] is not None
        assert "0" in col_map["consecutive_failures"]["column_default"]

        assert col_map["failure_threshold"]["data_type"] == "integer"
        assert col_map["failure_threshold"]["is_nullable"] == "YES"

        assert col_map["finalizer_job_id"]["data_type"] == "uuid"
        assert col_map["finalizer_job_id"]["is_nullable"] == "YES"

        assert col_map["originating_actor"]["data_type"] == "text"
        assert col_map["originating_actor"]["is_nullable"] == "YES"

        assert col_map["created_at"]["data_type"] == "timestamp with time zone"
        assert col_map["created_at"]["is_nullable"] == "NO"

        assert col_map["completed_at"]["data_type"] == "timestamp with time zone"
        assert col_map["completed_at"]["is_nullable"] == "YES"

        assert col_map["metadata"]["data_type"] == "jsonb"
        assert col_map["metadata"]["is_nullable"] == "NO"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_batches_status_default_is_active(pg_dsn: str) -> None:
    schema = f"mig_batches_dflt_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        row = await conn.fetchrow(
            """
            SELECT column_default
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = 'batches'
              AND column_name = 'status'
            """,
            schema,
        )
        assert row is not None
        default: str = row["column_default"]
        assert "'active'" in default, f"status default should be 'active', got {default!r}"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_batches_primary_key(pg_dsn: str) -> None:
    schema = f"mig_batches_pk_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        rows = await conn.fetch(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1
              AND c.relname = 'batches'
              AND i.indisprimary
            """,
            schema,
        )
        pk_cols = {r["attname"] for r in rows}
        assert pk_cols == {"id"}, f"batches PK should be (id), got {pk_cols}"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_batches_queue_status_index(pg_dsn: str) -> None:
    schema = f"mig_batches_qsi_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        row = await conn.fetchrow(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'batches_queue_status_idx'
            """,
            schema,
        )
        assert row is not None, "batches_queue_status_idx should exist"
        idx_def: str = row["indexdef"]
        assert "queue" in idx_def
        assert "status" in idx_def
        assert "active" in idx_def
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_batches_finalizer_index(pg_dsn: str) -> None:
    schema = f"mig_batches_fi_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        row = await conn.fetchrow(
            """
            SELECT indexdef
            FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'batches_finalizer_idx'
            """,
            schema,
        )
        assert row is not None, "batches_finalizer_idx should exist"
        idx_def: str = row["indexdef"]
        assert "finalizer_job_id" in idx_def
        assert "IS NOT NULL" in idx_def
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_batches_status_check_constraint(pg_dsn: str) -> None:
    schema = f"mig_batches_chk_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        rows = await conn.fetch(
            """
            SELECT pg_get_constraintdef(oid) AS def
            FROM pg_constraint
            WHERE connamespace = (
                SELECT oid FROM pg_namespace WHERE nspname = $1
            ) AND conrelid = (
                SELECT oid FROM pg_class WHERE relname = 'batches'
                AND relnamespace = connamespace
            ) AND contype = 'c'
            """,
            schema,
        )
        defs = [r["def"] for r in rows]
        assert any("active" in d and "complete" in d and "aborted" in d for d in defs), (
            f"status CHECK constraint should include active, complete, aborted; got {defs}"
        )
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_batches_table_comment(pg_dsn: str) -> None:
    schema = f"mig_batches_cmt_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _drop_schema(conn, schema)
        await migrate_mod.apply_pending(conn, schema=schema)

        comment = await conn.fetchval(
            """
            SELECT obj_description(
                ($1 || '.batches')::regclass, 'pg_class'
            )
            """,
            schema,
        )
        assert comment is not None, "batches table should have a comment"
    finally:
        await _drop_schema(conn, schema)
        await conn.close()
