# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team: 01.00.08's NOT VALID + VALIDATE CHECK constraint under deploy.

The denial-counters migration adds ``jobs_max_attempts_check`` in two phases
(NOT VALID, then VALIDATE) so the validation scan runs under the weaker
SHARE UPDATE EXCLUSIVE lock instead of stalling the fleet. Two contracts:

* the constraint must END validated (``convalidated``) and bind new writes
  immediately (NOT VALID semantics: every new write is checked from the
  moment the constraint exists);
* a pre-existing violating row must fail the apply CLEANLY: the whole
  migration transaction rolls back (columns AND constraint AND version row
  together), the ledger does not advance, and a re-run re-attempts
  identically - never the "version advanced + DDL rolled back = permanently
  skipped" corruption shape.
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id
from taskq.migrate import apply_pending, list_applied

pytestmark = pytest.mark.integration

_DENIAL_COUNTERS_KEY = "01.00.08_01:pre"
_PRE_DENIAL_TARGET = "01.00.07_01"

_CONSTRAINT_STATE_SQL = (
    "SELECT c.convalidated FROM pg_constraint c "
    "JOIN pg_class cl ON cl.oid = c.conrelid "
    "JOIN pg_namespace n ON n.oid = cl.relnamespace "
    "WHERE c.conname = 'jobs_max_attempts_check' "
    "AND n.nspname = $1 AND cl.relname = 'jobs'"
)
_COUNTER_COLUMN_EXISTS_SQL = (
    "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
    "WHERE table_schema = $1 AND table_name = 'jobs' AND column_name = 'snooze_count')"
)


async def _prepare(
    conn: asyncpg.Connection, schema: str, *, full: bool, max_attempts: int, job_id: object
) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    if full:
        await apply_pending(conn, schema=schema)
    else:
        await apply_pending(conn, schema=schema, target=_PRE_DENIAL_TARGET)
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, max_attempts, retry_kind, finished_at) "
            "VALUES ($1, 'rt_migrate_ddl', 'q', '{}'::jsonb, 'succeeded', $2, 'transient', "
            "clock_timestamp() - interval '40 days')",
            job_id,
            max_attempts,
        )


async def _drop(pg_dsn: str, schema: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


async def test_max_attempts_check_ends_validated_and_binds_new_writes(pg_dsn: str) -> None:
    """The NOT VALID + VALIDATE pattern must land a VALIDATED constraint that
    also rejects new violating writes immediately."""
    schema = f"tmg_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _prepare(conn, schema, full=False, max_attempts=3, job_id=new_job_id())
        applied = await apply_pending(conn, schema=schema)
        assert _DENIAL_COUNTERS_KEY in {m.key for m in applied}, (
            "precondition: 01.00.08 must have applied over a valid pre-existing row"
        )
        convalidated = await conn.fetchval(_CONSTRAINT_STATE_SQL, schema)
        assert convalidated is True, (
            "contract: the VALIDATE phase must leave jobs_max_attempts_check "
            "validated - an unvalidated constraint would let pre-existing "
            "violators persist silently while only new writes are checked"
        )
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await conn.execute(
                f'INSERT INTO "{schema}".jobs '
                "(id, actor, queue, payload, status, max_attempts, retry_kind) "
                "VALUES ($1, 'rt_migrate_ddl', 'q', '{}'::jsonb, 'pending', 0, 'transient')",
                new_job_id(),
            )
        assert (
            await conn.fetchval(f'SELECT max_attempts FROM "{schema}".jobs WHERE max_attempts = 3')
            == 3
        ), "the valid pre-existing row must be untouched by validation"
    finally:
        await conn.close()
        await _drop(pg_dsn, schema)


async def test_validate_failure_rolls_back_whole_migration_and_stays_rerunnable(
    pg_dsn: str,
) -> None:
    """A pre-existing violating row (hand-rolled write, legal before
    01.00.08) must fail the apply with NO partial state: the version row is
    transactional with the DDL, so nothing is applied, nothing is recorded,
    and a re-run re-attempts identically instead of silently skipping."""
    schema = f"tmg_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await _prepare(conn, schema, full=False, max_attempts=0, job_id=new_job_id())
        with pytest.raises(asyncpg.exceptions.CheckViolationError) as excinfo:
            await apply_pending(conn, schema=schema)
        assert "jobs_max_attempts_check" in str(excinfo.value), (
            "the failure must name the constraint being validated"
        )
        assert _DENIAL_COUNTERS_KEY not in await list_applied(conn, schema), (
            "anti-corruption contract: the version row must be transactional with "
            "the DDL - version advanced while the DDL rolled back would permanently "
            "skip the migration"
        )
        counter_column = await conn.fetchval(_COUNTER_COLUMN_EXISTS_SQL, schema)
        assert counter_column is not True, (
            "the whole migration file must roll back - no partially applied columns"
        )
        constraint_row = await conn.fetchval(_CONSTRAINT_STATE_SQL, schema)
        assert constraint_row is None, "the NOT VALID constraint must roll back with the file"
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await apply_pending(conn, schema=schema)
        assert _DENIAL_COUNTERS_KEY not in await list_applied(conn, schema), (
            "the re-run must re-attempt (loud, deterministic) - not silently skip"
        )
        surviving = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE max_attempts = 0'
        )
        assert surviving == 1, "the failing apply must not touch existing data"
    finally:
        await conn.close()
        await _drop(pg_dsn, schema)
