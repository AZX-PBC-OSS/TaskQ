"""Integration tests for the idempotency_scope migration (01.00.03_01).

Covers:
- Migration correctness: after applying all migrations against a fresh DB,
  idempotency_scope column exists NOT NULL DEFAULT '', the old single-column
  unique index is gone, the new composite unique index exists, and two rows
  with the default scope ('') and the same idempotency_key raise
  UniqueViolationError — proving the sentinel-not-NULL composite index
  enforces global dedupe for unscoped keys at the DB level.
- Migration upgrade path: apply migrations up through 01.00.01, insert a
  job row with idempotency_key (no idempotency_scope column yet), then
  apply 01.00.03 and assert the existing row has idempotency_scope = ''
  and a second insert with the same key still conflicts.
"""

from datetime import UTC, datetime

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_uuid
from taskq.settings import TaskQSettings

pytestmark = pytest.mark.integration


async def _insert_job_raw(
    conn: asyncpg.Connection,
    schema: str,
    *,
    idempotency_key: str | None = None,
    idempotency_scope: str | None = None,
) -> None:
    """Insert a minimal job row directly via asyncpg.

    If *idempotency_scope* is None, the column is omitted so PG applies
    the column DEFAULT ('').  If the idempotency_scope column does not
    exist yet (pre-migration), it is always omitted.
    """
    if idempotency_scope is not None:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            f"(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, "
            f"idempotency_scope, idempotency_key) "
            f"VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8, $9)",
            new_uuid(),
            "direct_actor",
            "default",
            "{}",
            3,
            "transient",
            datetime.now(UTC),
            idempotency_scope,
            idempotency_key,
        )
    else:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            f"(id, actor, queue, payload, max_attempts, retry_kind, scheduled_at, "
            f"idempotency_key) "
            f"VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8)",
            new_uuid(),
            "direct_actor",
            "default",
            "{}",
            3,
            "transient",
            datetime.now(UTC),
            idempotency_key,
        )


# ── Migration correctness: fresh schema ────────────────────────


class TestMigrationCorrectness:
    """After applying all migrations, the schema has the correct
    idempotency_scope column and composite unique index."""

    async def test_idempotency_scope_column_exists_not_null_default_empty(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        rows = await pg_conn.fetch(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = $1
                AND table_name = 'jobs'
                AND column_name = 'idempotency_scope'
            """,
            settings.schema_name,
        )
        assert len(rows) == 1, "idempotency_scope column missing from jobs"
        col = rows[0]
        assert col["data_type"] == "text"
        assert col["is_nullable"] == "NO"
        assert col["column_default"] == "''::text"

    async def test_old_single_column_index_gone(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        row = await pg_conn.fetchrow(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_key_uniq'
            """,
            settings.schema_name,
        )
        assert row is None, "old single-column unique index should have been dropped"

    async def test_new_composite_index_exists(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        row = await pg_conn.fetchrow(
            """
            SELECT indexdef FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_scope_key_uniq'
            """,
            settings.schema_name,
        )
        assert row is not None, "composite unique index should exist"
        definition: str = row["indexdef"]
        assert "idempotency_scope" in definition
        assert "idempotency_key" in definition
        assert "UNIQUE" in definition
        assert "idempotency_key IS NOT NULL" in definition

    async def test_default_scope_same_key_raises_unique_violation(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Two rows inserted with the DEFAULT scope ('') and the same
        idempotency_key must raise UniqueViolationError — the critical
        regression check for the NULL-distinctness trap."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        key = "migration-correctness-default-scope"
        await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=key)

        with pytest.raises(asyncpg.UniqueViolationError):
            await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=key)

    async def test_different_scopes_same_key_allowed(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """Two rows with different idempotency_scope values and the same
        idempotency_key should both succeed."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        key = "migration-correctness-diff-scopes"
        await _insert_job_raw(
            pg_conn, settings.schema_name, idempotency_key=key, idempotency_scope="run-A"
        )
        await _insert_job_raw(
            pg_conn, settings.schema_name, idempotency_key=key, idempotency_scope="run-B"
        )

    async def test_null_idempotency_key_allows_duplicates(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        """NULL idempotency_key rows should not conflict regardless of scope."""
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=None)
        await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=None)


# ── Migration upgrade path: pre-existing data ──────────────────


class TestMigrationUpgradePath:
    """Apply migrations up through 01.00.01 (before idempotency_scope),
    insert a job row with idempotency_key, then apply 01.00.03 and verify
    the existing row is backfilled with idempotency_scope = '' and the
    composite unique index still enforces dedupe for the pre-existing row.
    """

    async def test_pre_existing_row_gets_default_scope(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        # 1. Apply migrations up through 01.00.01_01 (before idempotency_scope)
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, target="01.00.01_01")

        # 2. Insert a job row with idempotency_key set (no idempotency_scope column yet)
        key = "upgrade-path-pre-existing"
        await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=key)

        # 3. Apply the remaining migration (01.00.03_01)
        applied = await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)
        assert len(applied) >= 1
        assert applied[0].version == "01.00.03_01"

        # 4. Assert the existing row now has idempotency_scope = ''
        row = await pg_conn.fetchrow(
            f'SELECT idempotency_scope FROM "{settings.schema_name}".jobs '
            f"WHERE idempotency_key = $1",
            key,
        )
        assert row is not None
        assert row["idempotency_scope"] == ""

    async def test_pre_existing_row_still_dedupes_after_upgrade(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        # 1. Apply up through 01.00.01_01
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, target="01.00.01_01")

        # 2. Insert a pre-existing row with idempotency_key
        key = "upgrade-path-dedup"
        await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=key)

        # 3. Apply the new migration
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        # 4. Inserting a second row with the same key and no explicit scope
        #    (defaults to '') should raise UniqueViolationError
        with pytest.raises(asyncpg.UniqueViolationError):
            await _insert_job_raw(pg_conn, settings.schema_name, idempotency_key=key)

    async def test_old_index_gone_after_upgrade(
        self, pg_conn: asyncpg.Connection, settings: TaskQSettings
    ) -> None:
        # 1. Apply up through 01.00.01_01
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name, target="01.00.01_01")

        # 2. Verify old index exists before the new migration
        row = await pg_conn.fetchrow(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_key_uniq'
            """,
            settings.schema_name,
        )
        assert row is not None, "old index should exist before 01.00.03"

        # 3. Apply the new migration
        await migrate_mod.apply_pending(pg_conn, schema=settings.schema_name)

        # 4. Verify old index is gone
        row = await pg_conn.fetchrow(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_key_uniq'
            """,
            settings.schema_name,
        )
        assert row is None, "old index should be gone after 01.00.03"

        # 5. Verify new composite index exists
        row = await pg_conn.fetchrow(
            """
            SELECT indexname FROM pg_indexes
            WHERE schemaname = $1 AND indexname = 'jobs_idempotency_scope_key_uniq'
            """,
            settings.schema_name,
        )
        assert row is not None, "composite index should exist after 01.00.03"
