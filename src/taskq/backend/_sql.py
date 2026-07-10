"""Shared SQL helpers for the taskq backend package.

Internal module — the leading underscore on the module name itself signals
"private to taskq.backend."  Module-level constants and functions here are
the explicit public surface of this module within the backend package.
"""

from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)

__all__ = [
    "CANCEL_ESCALATION_SQL",
    "INSERT_ATTEMPT_SQL",
    "INSERT_EVENT_SQL",
    "POLL_CANCEL_FLAGS_SQL",
    "UPDATE_JOBS_LOCK_SQL_TEMPLATE",
    "UPDATE_LEADER_PING_SQL_TEMPLATE",
    "UPDATE_RESERVATION_LEASES_SQL_TEMPLATE",
    "UPDATE_WORKER_LIVENESS_SQL_TEMPLATE",
    "build_heartbeat_sql",
    "parse_rowcount",
]

INSERT_ATTEMPT_SQL = """\
INSERT INTO "{schema}".job_attempts
(job_id, attempt, started_at, finished_at, outcome,
 error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
VALUES ($1, $2, $3, clock_timestamp(), $4, $5, $6, $7, $8, $9, $10::jsonb)"""
# Note: finished_at uses server-side clock_timestamp() — this template is used
# only by PostgresBackend._insert_attempt where the write happens inside the
# same transaction as the UPDATE, so clock_timestamp() is the actual wall-clock
# time of execution (not transaction start time like now()).
# PostgresBackend._sql_insert_attempt_explicit (in postgres.py) takes $4 for
# finished_at when the caller supplies an explicit value (e.g. write_attempt).

INSERT_EVENT_SQL = """\
INSERT INTO "{schema}".job_events
(job_id, occurred_at, kind, detail)
VALUES ($1, clock_timestamp(), $2, $3::jsonb)"""

POLL_CANCEL_FLAGS_SQL = """\
SELECT id, cancel_phase
FROM "{schema}".jobs
WHERE locked_by_worker = $1
  AND cancel_requested_at IS NOT NULL
  AND status = 'running'"""

CANCEL_ESCALATION_SQL = """\
UPDATE "{schema}".jobs
SET cancel_phase = 2
WHERE id = $1 AND status = 'running' AND locked_by_worker = $2 AND cancel_phase = 1"""
# Shared between PostgresBackend and the cancel-poll hook factory
# (taskq.worker.cancel) — the hook uses a bare conn.execute on the heartbeat
# connection that already holds an open transaction.  Keeping the SQL in a
# single module-level constant prevents drift between the two call sites (DRY).


def parse_rowcount(tag: str) -> int:
    """Parse asyncpg's ``Connection.execute()`` command tag and return the
    trailing integer.  asyncpg lacks a ``.rowcount`` attribute, so the
    command tag (e.g. ``'UPDATE 1'``, ``'INSERT 0 1'``) is the only way
    to determine affected rows from ``execute()``.
    """
    return int(tag.rsplit(" ", 1)[-1])


# ── Heartbeat SQL templates ──────────────────────────

UPDATE_WORKER_LIVENESS_SQL_TEMPLATE = (
    'UPDATE "{schema}".workers SET last_seen_at = clock_timestamp() WHERE id = $1'
)
UPDATE_JOBS_LOCK_SQL_TEMPLATE = (
    'UPDATE "{schema}".jobs '
    "SET last_heartbeat_at = clock_timestamp(), lock_expires_at = clock_timestamp() + $2 "
    "WHERE locked_by_worker = $1 AND status = 'running'"
)
UPDATE_RESERVATION_LEASES_SQL_TEMPLATE = (
    'UPDATE "{schema}".reservation_slots '
    "SET lease_expires_at = clock_timestamp() + $2 "
    "WHERE job_id IN ("
    "SELECT id FROM \"{schema}\".jobs WHERE locked_by_worker = $1 AND status = 'running'"
    ")"
)
UPDATE_LEADER_PING_SQL_TEMPLATE = (
    'UPDATE "{schema}".maintenance_leader SET last_seen_at = clock_timestamp() WHERE worker_id = $1'
)


def build_heartbeat_sql(schema: str) -> tuple[str, str, str, str]:
    """Render the four heartbeat SQL templates for *schema*.

    Validates *schema* against the canonical identifier regex before
    formatting.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return (
        UPDATE_WORKER_LIVENESS_SQL_TEMPLATE.format(schema=schema),
        UPDATE_JOBS_LOCK_SQL_TEMPLATE.format(schema=schema),
        UPDATE_RESERVATION_LEASES_SQL_TEMPLATE.format(schema=schema),
        UPDATE_LEADER_PING_SQL_TEMPLATE.format(schema=schema),
    )
