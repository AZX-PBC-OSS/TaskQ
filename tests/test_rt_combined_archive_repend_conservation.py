# ruff: noqa: S608  # Why: schema is fixture-derived (module_pg_schema), not user input; every value is $-bound.

"""Cross-PR pin: the admin retry's re-pend vs the prune/archive write.

#422's archive machinery moves terminal rows to ``jobs_archive`` in a
two-statement batch (candidate window, then the lock-bearing archive
CTE); #418-family reconciles and the admin retry (#429's surface,
``Backend.retry_job``) re-pends terminal rows. The interleaving no
single PR's tests pin: a retry that commits BETWEEN the archive batch's
candidate read and its write statement. The archive CTE re-verifies the
terminal status at lock time (EvalPlanQual through the ``locked``
materialization), so the retried row must drop out of the batch and
stay LIVE - no lost job, no half-moved row (attempts in the archive,
job in the live tables), no double presence across the two tables.

The second pin holds the reverse order: archive commits first, the
retry must answer "nothing to retry" (the row is gone from ``jobs``),
and no resurrection re-pends the archived copy.

Both tests drive the EXACT statements the sweep machinery renders
(``_ARCHIVE_CANDIDATE_SQL`` / ``_ARCHIVE_CTE_SQL``) and the exact
template ``Backend.retry_job`` runs, so drift in any of the three fails
here first.
"""

from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq.backend._sql_templates import render
from taskq.migrate import apply_pending
from taskq.settings import TaskQSettings
from taskq.testing._shared_containers import skip_test_without_docker
from taskq.worker._leader_shared import (  # pyright: ignore[reportPrivateUsage]  # Why: the pin binds the exact statements the sweep renders; a drift fails here first.
    _ARCHIVE_CANDIDATE_SQL,
    _ARCHIVE_CTE_SQL,
)
from tests.test_leader_prune import _seed_job_attempt, _seed_terminal_job

pytestmark = pytest.mark.integration

_RETENTION = timedelta(hours=6)
_ARCHIVE_RETENTION = timedelta(days=2)
_OLD = datetime.now(UTC) - timedelta(days=60)


@pytest.fixture()
async def _migrated(pg_conn: asyncpg.Connection, settings: TaskQSettings) -> asyncpg.Connection:
    await apply_pending(pg_conn, schema=settings.schema_name)
    return pg_conn


async def test_retry_between_candidate_and_write_keeps_the_job_live(
    _migrated: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """A retry committing inside the archive batch's window drops the row
    out of the write: the job survives live, archived tables untouched,
    the job exists exactly once."""
    skip_test_without_docker()
    schema = settings.schema_name
    jid = await _seed_terminal_job(_migrated, schema, status="succeeded", finished_at=_OLD)
    await _seed_job_attempt(_migrated, jid, schema=schema)

    sql = render(schema)
    candidate_sql = _ARCHIVE_CANDIDATE_SQL.format(schema=schema)
    write_sql = _ARCHIVE_CTE_SQL.format(schema=schema)

    # Connection A: the archive batch's transaction, candidate window read.
    conn_a = await asyncpg.connect(str(settings.pg_dsn))
    conn_b = await asyncpg.connect(str(settings.pg_dsn))
    try:
        tx = conn_a.transaction()
        await tx.start()
        candidates = await conn_a.fetch(candidate_sql, "succeeded", _RETENTION, 100)
        assert [row["id"] for row in candidates] == [jid]

        # Connection B: the admin retry commits INSIDE the window - the
        # row re-pends before the batch's write statement runs.
        retried = await conn_b.fetchval(sql.retry_job, jid)
        assert retried is not None

        # The write runs on the stale candidate list.
        moved = await conn_a.fetch(write_sql, "succeeded", _ARCHIVE_RETENTION, [jid], _RETENTION)
        assert moved == [], "a row a concurrent retry made live must drop out at lock time"
        await tx.commit()
    finally:
        await conn_a.close()
        await conn_b.close()

    # Conservation: the job exists exactly once, LIVE, re-pended.
    live = await _migrated.fetchval(f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1', jid)
    assert live == 1
    status = await _migrated.fetchval(
        f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', jid
    )
    assert status == "pending"
    archived = await _migrated.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs_archive WHERE id = $1', jid
    )
    assert archived == 0
    # No half-moved row: the attempt stayed in the LIVE attempts table.
    live_attempts = await _migrated.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1', jid
    )
    assert live_attempts == 1
    archived_attempts = await _migrated.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts_archive WHERE job_id = $1', jid
    )
    assert archived_attempts == 0


async def test_retry_after_archive_answers_nothing_and_never_resurrects(
    _migrated: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """The archive commits first: the retry's own guard refuses (the row
    is gone from ``jobs``), the archived copy is exactly one, and no
    re-pend resurrects it into the live tables."""
    skip_test_without_docker()
    schema = settings.schema_name
    jid = await _seed_terminal_job(_migrated, schema, status="succeeded", finished_at=_OLD)
    await _seed_job_attempt(_migrated, jid, schema=schema)

    candidate_sql = _ARCHIVE_CANDIDATE_SQL.format(schema=schema)
    write_sql = _ARCHIVE_CTE_SQL.format(schema=schema)

    candidates = await _migrated.fetch(candidate_sql, "succeeded", _RETENTION, 100)
    assert [row["id"] for row in candidates] == [jid]
    moved = await _migrated.fetch(write_sql, "succeeded", _ARCHIVE_RETENTION, [jid], _RETENTION)
    assert len(moved) == 1

    sql = render(schema)
    retried = await _migrated.fetchval(sql.retry_job, jid)
    assert retried is None, "a retried-archived id must answer nothing to retry"

    # Exactly-once across the two tables, and the archive row carries the
    # TERMINAL status it was archived under, not a re-pended one.
    live = await _migrated.fetchval(f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1', jid)
    assert live == 0
    archived_rows = await _migrated.fetch(
        f'SELECT status::text FROM "{schema}".jobs_archive WHERE id = $1', jid
    )
    assert len(archived_rows) == 1
    assert archived_rows[0]["status"] == "succeeded"
    archived_attempts = await _migrated.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts_archive WHERE job_id = $1', jid
    )
    assert archived_attempts == 1
