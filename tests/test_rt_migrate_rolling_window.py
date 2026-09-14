# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team: rolling-deploy windows around migration 01.00.08 (denial counters).

Old-code/new-schema: the previous release's prune CTE names its columns
explicitly and predates ``snooze_count`` / ``rate_limit_blocked_count``. The
verbatim prior-release shape is cited from git history —
``git show b23c921^:src/taskq/worker/_leader_shared.py`` (b23c921 is the
commit that shipped 01.00.08) and
``git show b23c921^:src/taskq/backend/_sql_templates.py`` for its 38-column
COPY_FROM_COLUMNS list. Against a fully-migrated HEAD schema both counters
are trailing additive columns with defaults, so the old INSERT's explicit
column list must keep working — the additive discipline the 01.00.03 header
documents ("Forward-only ADD-only contract").

New-code/old-schema: a leader upgraded before ``migrate up`` runs the current
CTE, whose COPY_FROM_COLUMNS includes the counters, against a schema that
lacks them — the sweep must fail LOUDLY and non-destructively (the whole
batch transaction rolls back; the terminal row stays in jobs), never
silently skip archiving.
"""

from __future__ import annotations

import contextlib
from datetime import timedelta
from typing import Final

import asyncpg
import pytest

from taskq._ids import new_base62, new_job_id
from taskq.migrate import apply_pending
from taskq.worker._leader_shared import prune_terminal_jobs

pytestmark = pytest.mark.integration

_PRIOR_RELEASE_JOBS_COLUMNS: Final[tuple[str, ...]] = (
    "id",
    "actor",
    "queue",
    "identity_key",
    "fairness_key",
    "payload",
    "payload_schema_ver",
    "status",
    "priority",
    "attempt",
    "max_attempts",
    "retry_kind",
    "schedule_to_close",
    "start_to_close",
    "heartbeat_timeout",
    "created_at",
    "scheduled_at",
    "started_at",
    "finished_at",
    "last_heartbeat_at",
    "locked_by_worker",
    "lock_expires_at",
    "cancel_requested_at",
    "cancel_phase",
    "error_class",
    "error_message",
    "error_traceback",
    "progress_state",
    "progress_seq",
    "result",
    "result_size_bytes",
    "result_expires_at",
    "idempotency_scope",
    "idempotency_key",
    "trace_id",
    "span_id",
    "metadata",
    "tags",
)
_PRIOR_CSV: Final[str] = ", ".join(_PRIOR_RELEASE_JOBS_COLUMNS)
_PRIOR_QUALIFIED_CSV: Final[str] = ", ".join(f"j.{c}" for c in _PRIOR_RELEASE_JOBS_COLUMNS)
_PRIOR_ATTEMPTS_CSV: Final[str] = (
    "job_id, attempt, started_at, finished_at, outcome, error_class, "
    "error_message, error_traceback, duration_ms, worker_id, metadata"
)
_PRIOR_ATTEMPTS_QUALIFIED_CSV: Final[str] = (
    "ja.job_id, ja.attempt, ja.started_at, ja.finished_at, ja.outcome, "
    "ja.error_class, ja.error_message, ja.error_traceback, ja.duration_ms, "
    "ja.worker_id, ja.metadata"
)

# Verbatim prior-release archive CTE (git show b23c921^:.../_leader_shared.py):
# no MATERIALIZED fence, 38-column list, identical CTE arms.
_PRIOR_RELEASE_ARCHIVE_CTE: Final[str] = (
    "WITH candidate_ids AS ("
    '  SELECT id FROM "{schema}".jobs'
    '  WHERE status = $1::"{schema}".job_status'
    "    AND finished_at < statement_timestamp() - $2::interval"
    "  ORDER BY finished_at"
    "  LIMIT $3"
    "), moved AS ("
    f'  INSERT INTO "{{schema}}".jobs_archive ({_PRIOR_CSV}, archived_at, expire_at)'
    f"  SELECT {_PRIOR_QUALIFIED_CSV}, clock_timestamp(), clock_timestamp() + $4"
    '  FROM "{schema}".jobs j'
    "  JOIN candidate_ids c ON j.id = c.id"
    "  RETURNING id, actor, status"
    "), moved_attempts AS ("
    f'  INSERT INTO "{{schema}}".job_attempts_archive ({_PRIOR_ATTEMPTS_CSV})'
    f"  SELECT {_PRIOR_ATTEMPTS_QUALIFIED_CSV}"
    '  FROM "{schema}".job_attempts ja'
    "  JOIN moved m ON ja.job_id = m.id"
    "), deleted AS ("
    '  DELETE FROM "{schema}".jobs'
    "  WHERE id IN (SELECT id FROM moved)"
    "  RETURNING id, actor, status"
    ") SELECT actor, status, count(*) AS cnt"
    "  FROM deleted GROUP BY actor, status"
)

_INSERT_TERMINAL_JOB_SQL: Final[str] = (
    'INSERT INTO "{schema}".jobs '
    "(id, actor, queue, payload, status, max_attempts, retry_kind, finished_at) "
    "VALUES ($1, 'rt_migrate_roll', 'q', '{{}}'::jsonb, 'succeeded', 3, 'transient', "
    "clock_timestamp() - interval '40 days')"
)


async def _drop(conn: asyncpg.Connection, schema: str) -> None:
    with contextlib.suppress(Exception):
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def test_prior_release_prune_cte_runs_clean_on_current_schema(pg_dsn: str) -> None:
    """Old pod, new schema: the prior release's archive CTE (verbatim from
    git b23c921^) must move a terminal job on a fully-migrated schema with
    the new counter columns filling from their defaults — the additive
    discipline that makes the rolling-deploy overlap safe for the OLD fleet.
    """
    schema = f"tmg_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        job_id = new_job_id()
        await conn.execute(_INSERT_TERMINAL_JOB_SQL.format(schema=schema), job_id)

        rows = await conn.fetch(
            _PRIOR_RELEASE_ARCHIVE_CTE.format(schema=schema),
            "succeeded",
            timedelta(days=30),
            100,
            timedelta(days=365),
        )
        assert rows and rows[0]["cnt"] == 1, (
            "contract: the prior release's prune CTE must move exactly the one "
            f"terminal job on a current schema — got {rows!r}"
        )
        still_in_jobs = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert still_in_jobs == 0, "the job must be deleted from jobs after the move"
        archived = await conn.fetchrow(
            f"SELECT snooze_count, rate_limit_blocked_count, idempotency_scope, actor "
            f'FROM "{schema}".jobs_archive WHERE id = $1',
            job_id,
        )
        assert archived is not None, "the job must exist in jobs_archive after the move"
        assert archived["snooze_count"] == 0 and archived["rate_limit_blocked_count"] == 0, (
            "the additive counter columns must fill from their DDL defaults for a "
            "row archived by old-code SQL"
        )
        assert archived["idempotency_scope"] == "", (
            "the pre-existing scope column must round-trip through the old CTE"
        )
    finally:
        await _drop(conn, schema)
        await conn.close()


async def test_prune_fails_loud_and_non_destructive_on_pre_counters_schema(
    pg_dsn: str,
) -> None:
    """New leader, old schema: the current prune sweep against a schema that
    predates 01.00.08 must fail LOUDLY (an exception, never a silent skip that
    strands terminal rows forever) and non-destructively (the batch
    transaction rolls back; nothing is partially archived or lost)."""
    schema = f"tmg_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, target="01.00.07_01")
        job_id = new_job_id()
        await conn.execute(_INSERT_TERMINAL_JOB_SQL.format(schema=schema), job_id)

        with pytest.raises(asyncpg.exceptions.UndefinedColumnError) as excinfo:
            await prune_terminal_jobs(
                conn,
                retention_per_status={"succeeded": timedelta(days=30)},
                archive_retention=timedelta(days=365),
                schema=schema,
            )
        assert "snooze_count" in str(excinfo.value), (
            "the failure must name the missing column so an operator recognizes "
            "schema staleness, not data corruption"
        )
        remaining = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE id = $1', job_id
        )
        assert remaining == 1, (
            "non-destructive contract: a failed prune batch must leave the terminal "
            "row in jobs for the next (post-migrate) sweep — nothing partially moved"
        )
        archived_count = await conn.fetchval(f'SELECT count(*) FROM "{schema}".jobs_archive')
        assert archived_count == 0, "a failed prune batch must archive nothing"
    finally:
        await _drop(conn, schema)
        await conn.close()
