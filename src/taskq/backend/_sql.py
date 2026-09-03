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

# job_attempts.worker_id FK-references workers(id) ON DELETE SET NULL. That
# action only protects rows that already exist when the parent is deleted; an
# INSERT carrying the id of an already-deleted worker still violates the FK.
# A live worker's row CAN be gone before it writes a terminal attempt:
# cleanup_stale_workers (another worker's leader sweep) deletes rows whose
# heartbeat is stale heartbeat_interval * (max_heartbeat_failures + 3) — a
# blocked-but-alive loop (watchdog off, or a lag budget above that threshold,
# or isolate_self after enough heartbeat failures) is exactly such a row.
# The holder CTE resolves the id at insert time under FOR KEY SHARE — the
# same row lock the FK check itself takes — so one statement cannot be split
# by a concurrent delete: a present parent records the id, a deleted (or
# NULL) one records NULL, mirroring the column's ON DELETE SET NULL
# semantics. Constraint violations are deliberately non-transient (see
# taskq.worker._transient), so an unmitigated FK hit would tear down the
# writing loop; every job_attempts INSERT that can carry a worker id uses
# this idiom (_sql_templates.insert_attempt_explicit, _sweeps.py's
# _SWEEP_1_ATTEMPT_SQL). _SWEEP_2_ATTEMPT_SQL stays plain: its worker_id is
# NULL by construction (never dispatched).
INSERT_ATTEMPT_SQL = """\
WITH holder AS (
    SELECT id FROM "{schema}".workers WHERE id = $9 FOR KEY SHARE
)
INSERT INTO "{schema}".job_attempts
(job_id, attempt, started_at, finished_at, outcome,
 error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
VALUES ($1, $2, $3, clock_timestamp(), $4, $5, $6, $7, $8,
        (SELECT id FROM holder), $10::jsonb)"""
# Note: finished_at uses server-side clock_timestamp() — this template (the
# sql.insert_attempt field) is consumed only by _terminal.py's _insert_attempt,
# where the write runs inside the caller's existing transaction on the same
# connection as the status UPDATE, so clock_timestamp() is the actual
# wall-clock time of execution (not transaction start time like now()).
# The explicit-finished_at variant is _sql_templates.insert_attempt_explicit,
# bound by _terminal.py's _write_attempt (the write_attempt path): it takes
# $4 for finished_at from the caller instead of stamping clock_timestamp().

INSERT_EVENT_SQL = """\
INSERT INTO "{schema}".job_events
(job_id, occurred_at, kind, detail)
VALUES ($1, clock_timestamp(), $2, $3::jsonb)"""

# Multi-row form of INSERT_EVENT_SQL for the dispatch path.
#
# Dispatch previously issued one awaited round trip per dispatched job, inside
# the transaction still holding the FOR UPDATE SKIP LOCKED row locks. At a batch
# of 50 against a managed Postgres (~1-3ms RTT) that is 50-150ms of extra
# transaction hold per dispatch cycle, per worker -- lock hold time that
# directly narrows the window other dispatchers can work in.
#
# Every row in one dispatch batch shares `kind` and `detail` (same from_state,
# to_state and worker_id), so only the job ids vary and a single unnest over a
# uuid[] suffices. `clock_timestamp()` is still evaluated per row, matching the
# per-statement behaviour it replaces.
INSERT_EVENTS_BATCH_SQL = """\
INSERT INTO "{schema}".job_events
(job_id, occurred_at, kind, detail)
SELECT t.id, clock_timestamp(), $2, $3::jsonb
FROM unnest($1::uuid[]) AS t(id)"""

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
