"""Shared SQL helpers for the taskq backend package.

Internal module, the leading underscore on the module name itself signals
"private to taskq.backend."  Module-level constants and functions here are
the explicit public surface of this module within the backend package.
"""

from datetime import timedelta

from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)

__all__ = [
    "CANCEL_ESCALATION_SQL",
    "INSERT_ATTEMPT_SQL",
    "INSERT_EVENT_SQL",
    "POLL_CANCEL_FLAGS_SQL",
    "UPDATE_JOBS_LOCK_RENEWAL_SQL_TEMPLATE",
    "UPDATE_JOBS_LOCK_SQL_TEMPLATE",
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
# heartbeat is stale heartbeat_interval * (max_heartbeat_failures + 3), a
# blocked-but-alive loop (watchdog off, or a lag budget above that threshold,
# or isolate_self after enough heartbeat failures) is exactly such a row.
# The holder CTE resolves the id at insert time under FOR KEY SHARE, the
# same row lock the FK check itself takes, so one statement cannot be split
# by a concurrent delete: a present parent records the id, a deleted (or
# NULL) one records NULL, mirroring the column's ON DELETE SET NULL
# semantics. Constraint violations are deliberately non-transient (see
# taskq.worker._transient), so an unmitigated FK hit would tear down the
# writing loop; every job_attempts INSERT that can carry a worker id uses
# this idiom (_sql_templates.insert_attempt_explicit, _sweeps.py's
# _SWEEP_1_ATTEMPTS_BATCH_SQL). _SWEEP_2_ATTEMPTS_BATCH_SQL stays plain:
# its worker_id is NULL by construction (never dispatched).
INSERT_ATTEMPT_SQL = """\
WITH holder AS (
    SELECT id FROM "{schema}".workers WHERE id = $9 FOR KEY SHARE
)
INSERT INTO "{schema}".job_attempts
(job_id, attempt, started_at, finished_at, outcome,
 error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
VALUES ($1, $2, $3, clock_timestamp(), $4, $5, $6, $7, $8,
        (SELECT id FROM holder), $10::jsonb)
-- A claim-clamped attempt number repeats at the smallint ceiling (the
-- dispatch claim saturates its increment there): keep the first record
-- of the number, never raise a PK collision on the hot path (the
-- deadline sweep's insert carries the same doctrine).
ON CONFLICT (job_id, attempt) DO NOTHING"""
# Note: finished_at uses server-side clock_timestamp(), this template
# (INSERT_ATTEMPT_SQL, formatted by worker/heartbeat.py for its
# isolate-self attempt write) runs on a caller's existing transaction or
# dedicated connection, where clock_timestamp() is the actual wall-clock
# time of execution (not transaction start time like now()).
# The explicit-finished_at variant is _sql_templates.insert_attempt_explicit,
# bound by _terminal.py's _write_attempt (the write_attempt path): it takes
# $4 for finished_at from the caller instead of stamping clock_timestamp().
# The mark_* terminal writes no longer consume this template: they fuse the
# jobs UPDATE with their attempt/event INSERTs into one statement (see
# _terminal.py's module docstring), carrying the same holder-CTE idiom
# inline.

INSERT_EVENT_SQL = """\
INSERT INTO "{schema}".job_events
(job_id, occurred_at, kind, detail)
VALUES ($1, clock_timestamp(), $2, $3::jsonb)"""

# Batched event INSERT for writers whose rows carry DISTINCT per-row detail.
#
# One statement per batch, not one per row: each round trip here is time the
# batch transaction stays open against RECLAIM_EVENT_VISIBILITY_DELAY (see
# taskq.constants), and only the (job_id, detail) pairs vary, kind is shared
# by every row a sweep or bulk write produces.
#
# occurred_at carries a microsecond ladder on the row ordinal rather than a
# bare clock_timestamp(). The watermark contract needs occurred_at
# non-decreasing in job_events.id order, and the audit contract pins it
# DISTINCT per row. A bare volatile clock_timestamp() is evaluated per row but
# cannot deliver the second property inside one statement: the OS clock that
# backs it resolves to microseconds while per-row evaluation is far cheaper ,
# measured on Postgres 18, ~26 rows evaluate within the same microsecond, so
# tens of rows collapse onto one stamp and the ordering information the
# watermark reads is destroyed. The ladder restores it by construction: each
# row's stamp is its own evaluation instant plus (ordinal - 1) microseconds,
# strictly increasing, at most (batch_size - 1) microseconds ahead of real
# time, four-plus orders of magnitude inside the 2 s visibility margin, far
# below the commit-order skew the margin already absorbs, so it cannot mask a
# real inversion. Statements remain separated by whole round trips, so
# different statements' stamps stay disjoint.
INSERT_EVENTS_DETAIL_BATCH_SQL = """\
INSERT INTO "{schema}".job_events
(job_id, occurred_at, kind, detail)
SELECT e.job_id,
       clock_timestamp() + (e.ord - 1) * interval '1 microsecond',
       $3, e.detail
FROM unnest($1::uuid[], $2::jsonb[]) WITH ORDINALITY AS e(job_id, detail, ord)"""

# The one wake statement: $1 is the channel (taskq.constants.wake_channel),
# the payload is empty by contract (listeners never parse it). Rendered
# templates expose it as SqlTemplates.wake_notify; the sweeps and the
# leader, which carry no rendered templates, execute it directly.
WAKE_NOTIFY_SQL = "SELECT pg_notify($1, '')"

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
# (taskq.worker.cancel), the hook uses a bare conn.execute on the heartbeat
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
    'UPDATE "{schema}".workers '
    "SET last_seen_at = clock_timestamp(), metadata = metadata || $2::jsonb "
    "WHERE id = $1"
)
# The WHERE core shared by both jobs-lock statements (the unconditional
# renewal below and the threshold-gated renewal after it): the disowned
# exclusion must never drift between the two. $3 is the worker's
# disowned set (WorkerDeps.disowned_jobs): rows this worker holds but
# could not record an outcome for, whose leases must lapse so the
# reclaim sweep can hand them back. The exclusion lives in this shared
# core so every renewal, the heartbeat loop's gated statement and the
# backend's own heartbeat_jobs, carries it; a renewal without it would
# keep a disowned row's lease alive for as long as the process lived.
# An empty array excludes nothing.
_UPDATE_JOBS_LOCK_WHERE_CORE = (
    "WHERE locked_by_worker = $1 AND status = 'running' AND NOT (id = ANY($3::uuid[]))"
)
UPDATE_JOBS_LOCK_SQL_TEMPLATE = (
    'UPDATE "{schema}".jobs '
    "SET last_heartbeat_at = clock_timestamp(), lock_expires_at = clock_timestamp() + $2 "
    + _UPDATE_JOBS_LOCK_WHERE_CORE
)
# The heartbeat loop's renewal: the same write, threshold-gated by the
# caller.
#
# lock_expires_at is the key of jobs_running_lock_expires_idx, so every
# renewal is a non-HOT update that inserts new entries into every index
# a running row satisfies (PK, actor_running, locked_by_worker_running,
# identity_active, lock_expires, the heartbeat_deadline partial, GIN
# tags/metadata), per running row, per beat, fleet-wide. The gate renews
# only rows whose lease is at or under the threshold ($4, computed by
# the caller: see _lease_renewal_threshold in taskq.worker.heartbeat for
# the sizing derivation). At the default settings the threshold (56s)
# sits under one beat's decay of the 60s lease, so a healthy worker
# rewrites its leases every beat, byte-identical to the unconditional
# renewal. The gate defers nothing at the default lease; it only starts
# spacing the rewrites out once lock_lease exceeds that floor, and the
# savings begin at leases of about 70s (every second beat, 2x fewer
# rewrites) and grow with the lease from there.
#
# The three OR arms, each essential:
# * heartbeat_timeout IS NOT NULL, the per-job heartbeat promise: the
#   reclaim sweep's heartbeat arm reclaims such a row when
#   last_heartbeat_at + heartbeat_timeout < now while the lease is STILL
#   valid, so its beats must stay per-tick fresh. Skipping these rows to
#   save the write would falsely crash-reclaim healthy jobs, the exact
#   regression a naive "skip while more than half the lease remains"
#   (the candidate) produces. Their last_heartbeat_at update is
#   non-HOT anyway (jobs_running_heartbeat_deadline_idx is partial on
#   heartbeat_timeout IS NOT NULL), so folding the lease extension into
#   the same statement costs nothing extra for them.
# * lock_expires_at IS NULL, direct-SQL-reachable shapes; the
#   unconditional statement always renewed them, and the threshold
#   comparison alone never would (NULL <= x is NULL).
# * lock_expires_at <= clock_timestamp() + $4, the renewal threshold
#   itself. Deliberately compared SERVER-side with the same clock that
#   stamped lock_expires_at: a worker-clock skew cannot make a fresh
#   lease look expired (or an expiring one look fresh) to this
#   comparison, the way a client-side remaining-lease computation would.
UPDATE_JOBS_LOCK_RENEWAL_SQL_TEMPLATE = (
    'UPDATE "{schema}".jobs '
    "SET last_heartbeat_at = clock_timestamp(), lock_expires_at = clock_timestamp() + $2 "
    + _UPDATE_JOBS_LOCK_WHERE_CORE
    + " AND (heartbeat_timeout IS NOT NULL"
    " OR lock_expires_at IS NULL"
    " OR lock_expires_at <= clock_timestamp() + $4::interval)"
)
UPDATE_RESERVATION_LEASES_SQL_TEMPLATE = (
    'UPDATE "{schema}".reservation_slots '
    "SET lease_expires_at = clock_timestamp() + $2 "
    "WHERE job_id IN ("
    "SELECT id FROM \"{schema}\".jobs WHERE locked_by_worker = $1 AND status = 'running'"
    ")"
    " AND NOT (job_id = ANY($3::uuid[]))"
)


def build_heartbeat_sql(
    schema: str,
    *,
    renewal_threshold: timedelta | None = None,
) -> tuple[str, str, str]:
    """Render the three heartbeat SQL templates for *schema*.

    Validates *schema* against the canonical identifier regex before
    formatting. The two renewal statements bind ``(worker_id, lease,
    disowned_ids)``, plus, when *renewal_threshold* is given, the
    threshold interval as their jobs-lock statement's ``$4``: the loop's
    lease-renewal then only renews rows whose remaining lease is at or
    under the threshold (or that carry a per-job ``heartbeat_timeout``,
    whose beats must stay fresh for the reclaim sweep's heartbeat arm ,
    see UPDATE_JOBS_LOCK_RENEWAL_SQL_TEMPLATE). ``None``, the default ,
    keeps the unconditional renewal every existing caller and test of
    this helper binds, renewing every held row on every call.

    The liveness statement's ``$2`` merge is deliberately a jsonb
    concat (``metadata || $2``) rather than a metadata overwrite: the
    heartbeat writes only the keys it owns this tick (the loop-stall
    attribution tally), and the registration row's other keys
    (``max_concurrency``, ``notify_enabled``) must survive the merge
    untouched. An empty merge object is a no-op.

    ``maintenance_leader`` is deliberately absent: that row carries the
    maintenance lease and is written only by the election loop, under the
    fence of the term it holds. An unfenced refresh from this loop would
    keep a lapsed holder's row un-takeable.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    jobs_template = (
        UPDATE_JOBS_LOCK_RENEWAL_SQL_TEMPLATE
        if renewal_threshold is not None
        else UPDATE_JOBS_LOCK_SQL_TEMPLATE
    )
    return (
        UPDATE_WORKER_LIVENESS_SQL_TEMPLATE.format(schema=schema),
        jobs_template.format(schema=schema),
        UPDATE_RESERVATION_LEASES_SQL_TEMPLATE.format(schema=schema),
    )
