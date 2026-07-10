"""Integration tests for PostgresBackend.sweep_expired_results() static method.

Covers TTL-based result expiry (sweep 5): clears ``result``,
``result_size_bytes``, and ``result_expires_at`` from terminated jobs
whose ``result_expires_at`` has passed.
"""

# ruff: noqa: S608

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend.postgres import PostgresBackend
from taskq.testing.fixtures import ModulePgSchema

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

    type _Conn = asyncpg.Connection | PoolConnectionProxy
else:
    type _Conn = object  # pyright: ignore[reportInvalidTypeForm] # Why: runtime fallback — asyncpg is TYPE_CHECKING-only to avoid transitive import

pytestmark = pytest.mark.integration


# ── Helpers ────────────────────────────────────────────────────────────


async def _insert_job(
    conn: _Conn,
    schema: str,
    *,
    job_id: UUID,
    status: str,
    result: str | None,
    result_expires_at: datetime | None,
) -> None:
    """Insert a minimal job row with the given result-related columns."""
    await conn.execute(
        f"""INSERT INTO "{schema}".jobs (
            id, actor, queue, payload, max_attempts, retry_kind,
            status, priority, scheduled_at, result, result_expires_at
        ) VALUES (
            $1, $2, $3, $4::jsonb, $5, $6,
            $7, 0, now(), $8::jsonb, $9
        )""",
        job_id,
        "test_actor",
        "default",
        '{"k":"v"}',
        3,
        "transient",
        status,
        result,
        result_expires_at,
    )


# ── Expired result is cleared ──────────────────────────────────────────


class TestSweepExpiredResults:
    """sweep_expired_results (Sweep 5, result TTL)."""

    async def test_expired_result_is_cleared(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
    ) -> None:
        """Succeeded job with result_expires_at in the past and non-null
        result → sweep clears result, result_size_bytes, result_expires_at."""
        schema = module_pg_schema.schema_name
        job_id = new_uuid()
        past_time = datetime.now(UTC) - timedelta(hours=1)

        await _insert_job(
            clean_pg_conn,
            schema,
            job_id=job_id,
            status="succeeded",
            result='{"r":"v"}',
            result_expires_at=past_time,
        )

        count = await PostgresBackend.sweep_expired_results(
            clean_pg_conn,
            datetime.now(UTC),
            schema=schema,
        )

        assert count == 1

        row = await clean_pg_conn.fetchrow(
            f'SELECT result, result_size_bytes, result_expires_at FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        assert row is not None
        assert row["result"] is None
        assert row["result_size_bytes"] is None
        assert row["result_expires_at"] is None

    async def test_non_expired_result_not_cleared(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
    ) -> None:
        """Succeeded job with result_expires_at in the future → result
        is NOT cleared by sweep."""
        schema = module_pg_schema.schema_name
        job_id = new_uuid()
        future_time = datetime.now(UTC) + timedelta(hours=1)

        await _insert_job(
            clean_pg_conn,
            schema,
            job_id=job_id,
            status="succeeded",
            result='{"r":"v"}',
            result_expires_at=future_time,
        )

        count = await PostgresBackend.sweep_expired_results(
            clean_pg_conn,
            datetime.now(UTC),
            schema=schema,
        )

        assert count == 0

        row = await clean_pg_conn.fetchrow(
            f'SELECT result FROM "{schema}".jobs WHERE id = $1',
            job_id,
        )
        assert row is not None
        assert row["result"] is not None

    async def test_job_with_no_result_unaffected(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
    ) -> None:
        """Succeeded job with result=NULL and result_expires_at in the
        past → WHERE ``result IS NOT NULL`` excludes it, count=0."""
        schema = module_pg_schema.schema_name
        job_id = new_uuid()
        past_time = datetime.now(UTC) - timedelta(hours=1)

        await _insert_job(
            clean_pg_conn,
            schema,
            job_id=job_id,
            status="succeeded",
            result=None,
            result_expires_at=past_time,
        )

        count = await PostgresBackend.sweep_expired_results(
            clean_pg_conn,
            datetime.now(UTC),
            schema=schema,
        )

        assert count == 0

    async def test_non_terminal_job_unaffected(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
    ) -> None:
        """Running job with result=NULL (no result_expires_at either) →
        WHERE ``result IS NOT NULL`` excludes it, sweep returns 0."""
        schema = module_pg_schema.schema_name
        job_id = new_uuid()

        await _insert_job(
            clean_pg_conn,
            schema,
            job_id=job_id,
            status="running",
            result=None,
            result_expires_at=None,
        )

        count = await PostgresBackend.sweep_expired_results(
            clean_pg_conn,
            datetime.now(UTC),
            schema=schema,
        )

        assert count == 0

    async def test_multiple_mixed_jobs(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
    ) -> None:
        """One expired, one non-expired, one with no result → only the
        expired one is cleared, count=1."""
        schema = module_pg_schema.schema_name
        expired_id = new_uuid()
        future_id = new_uuid()
        no_result_id = new_uuid()
        past_time = datetime.now(UTC) - timedelta(hours=1)
        future_time = datetime.now(UTC) + timedelta(hours=1)

        # Expired: should be cleared
        await _insert_job(
            clean_pg_conn,
            schema,
            job_id=expired_id,
            status="succeeded",
            result='{"r":"v"}',
            result_expires_at=past_time,
        )

        # Non-expired: should NOT be cleared
        await _insert_job(
            clean_pg_conn,
            schema,
            job_id=future_id,
            status="succeeded",
            result='{"r":"v"}',
            result_expires_at=future_time,
        )

        # No result: should NOT be affected
        await _insert_job(
            clean_pg_conn,
            schema,
            job_id=no_result_id,
            status="succeeded",
            result=None,
            result_expires_at=past_time,
        )

        count = await PostgresBackend.sweep_expired_results(
            clean_pg_conn,
            datetime.now(UTC),
            schema=schema,
        )

        assert count == 1

        # Expired job: result cleared
        expired_row = await clean_pg_conn.fetchrow(
            f'SELECT result, result_size_bytes, result_expires_at FROM "{schema}".jobs WHERE id = $1',
            expired_id,
        )
        assert expired_row is not None
        assert expired_row["result"] is None
        assert expired_row["result_size_bytes"] is None
        assert expired_row["result_expires_at"] is None

        # Future job: result still present
        future_row = await clean_pg_conn.fetchrow(
            f'SELECT result FROM "{schema}".jobs WHERE id = $1',
            future_id,
        )
        assert future_row is not None
        assert future_row["result"] is not None

        # No-result job: still NULL
        no_result_row = await clean_pg_conn.fetchrow(
            f'SELECT result FROM "{schema}".jobs WHERE id = $1',
            no_result_id,
        )
        assert no_result_row is not None
        assert no_result_row["result"] is None

    async def test_no_rows_affected_returns_zero(
        self,
        module_pg_schema: ModulePgSchema,
        clean_pg_conn: asyncpg.Connection,
    ) -> None:
        """Empty table → sweep returns 0."""
        schema = module_pg_schema.schema_name

        count = await PostgresBackend.sweep_expired_results(
            clean_pg_conn,
            datetime.now(UTC),
            schema=schema,
        )

        assert count == 0
