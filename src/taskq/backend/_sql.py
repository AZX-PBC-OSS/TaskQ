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
 error_class, error_message, error_traceback, duration_ms, worker_id, metadata, due_at)
VALUES ($1, $2, $3, clock_timestamp(), $4, $5, $6, $7, $8,
        (SELECT id FROM holder), $10::jsonb, $11::timestamptz)
-- A claim-clamped attempt number repeats at the smallint ceiling (the
-- dispatch claim saturates its increment there): keep the first record
-- of the number, never raise a PK collision on the hot path (the
-- deadline sweep's insert carries the same doctrine).
ON CONFLICT (job_id, attempt) DO NOTHING"""
# Note: finished_at uses server-side clock_timestamp(), this template
# (INSERT_ATTEMPT_SQL, formatted by worker/heartbeat.py for its
# isolate-self attempt write) runs on a caller's existing transaction or
# dedicated connection, where clock_timestamp() is the actual wall-clock
# time of execution (not transaction start time like now()). $11 is the
# attempt's due time (the job row's claim-time scheduled_at, read by the
# caller's snapshot SELECT - see 01.00.20_04_pre_attempt_due_at.sql).
# The explicit-finished_at variant is _sql_templates.insert_attempt_explicit,
# bound by _terminal.py's _write_attempt (the write_attempt path): it takes
# $4 for finished_at from the caller instead of stamping clock_timestamp().
# The mark_* terminal writes do not consume this template: they fuse the
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
# the sizing derivation). At the default settings the threshold (58s)
# sits under one beat's decay of the 60s lease, so a healthy worker
# rewrites its leases every beat, byte-identical to the unconditional
# renewal. The gate defers nothing at the default lease; it only starts
# spacing the rewrites out once lock_lease exceeds that floor, and the
# savings begin at leases of about 70s (every second beat, 2x fewer
# rewrites) and grow with the lease from there.
#
# The gate is a row-shortlist CTE (``due``) joined back by id, not an
# OR'd WHERE predicate. The original spelling ANDed the three arms into
# the searched set as one OR group beside ``locked_by_worker = $1``; an
# OR group cannot be an index condition, and beside it the holder
# conjunct made ``jobs_locked_by_worker_running_idx`` the only viable
# plan - so the statement visited the worker's whole running fleet per
# beat to decide that most rows need nothing: O(held rows) even when
# O(1) rows renew. At 10k held rows the visit measured a Seq Scan over
# the whole jobs table, linear in the fleet (and it rides the tick
# path). The UNION arms instead give the planner one indexable shape
# per arm:
#
# * the expiry arm (the threshold itself) and the NULL-lease arm carry
#   NO ``locked_by_worker`` conjunct on purpose: with none, the ONLY
#   index either arm can use is ``jobs_running_lock_expires_idx``
#   (partial on status='running', keyed on lock_expires_at) - the range
#   bound and the IS NULL cond are its Index Conds, so the scan visits
#   the due set FLEET-WIDE (the due rows that exist, whichever worker
#   holds them), then the outer UPDATE's WHERE core re-applies the
#   holder and disowned predicates. In the deferred regime that set is
#   the handful the beat must actually renew, independent of how many
#   rows the worker holds; at the defaults it is the whole fleet, but
#   there every row renews anyway and the non-HOT writes dominate - the
#   read is no longer the marginal cost.
# * the expiry bound is ``statement_timestamp()``, not
#   ``clock_timestamp()``: clock_timestamp() is VOLATILE and cannot be
#   an index condition at all (measured, see backend/_sweeps.py's
#   module docstring - the reclaim sweep made the same
#   statement_timestamp() trade for the same reason), while
#   statement_timestamp() is STABLE within the statement. Both are the
#   server clock that stamped lock_expires_at, so worker-clock skew
#   still cannot move the gate; the bound reads earlier than a
#   clock_timestamp() one by the tick's in-transaction elapsed time
#   (bounded by the tick's command budget, milliseconds at the
#   defaults), which makes the gate RENEW slightly EARLIER - strictly
#   the safe direction of that error (see _lease_renewal_threshold: a
#   renewal that lands early only widens the margin the floor sizes).
# * the heartbeat arm KEEPS ``locked_by_worker = $1``: these rows renew
#   on every beat regardless of lease - the reclaim sweep's heartbeat
#   arm reclaims them on a stale last_heartbeat_at while the lease is
#   STILL valid, so their beats must stay per-tick fresh - and the
#   holder conjunct lets the planner take whichever of
#   jobs_locked_by_worker_running_idx (the worker's own set) or
#   jobs_running_heartbeat_deadline_idx (the heartbeat-configured set,
#   its predicate verbatim) is smaller, so the arm never amplifies
#   ACROSS workers: without the conjunct, every worker's beat would
#   visit every worker's heartbeat set. Visiting this set per beat is
#   required work, not waste - each row in it is written by this same
#   statement anyway.
# * the NULL-lease arm covers the direct-SQL-reachable shapes the
#   unconditional statement always renewed and the range bound never
#   selects (NULL <= x is NULL, and the partial index cannot serve a
#   range that includes NULLs for a bound that excludes them).
#
# The arms are UNIONed (deduplicated): a row matching two arms (a
# heartbeat_timeout row whose lease is also due) must appear once, and
# the outer statement re-checks the full core either way, so arm
# membership is a pure over-approximation of the old OR - the renewed
# row set is provably identical. The join is ``UPDATE ... FROM`` with
# ``jobs.id = due.id`` so the planner drives it from ``due`` (the
# shortlist) into PK lookups; an ``id IN (SELECT ...)`` semi-join could
# instead probe from the worker's fleet side, which reintroduces the
# O(held rows) visit the CTE exists to remove.
UPDATE_JOBS_LOCK_RENEWAL_SQL_TEMPLATE = """\
WITH due AS (
    SELECT id FROM "{schema}".jobs
    WHERE status = 'running' AND locked_by_worker = $1
      AND NOT (id = ANY($3::uuid[]))
      AND heartbeat_timeout IS NOT NULL
    UNION
    SELECT id FROM "{schema}".jobs
    WHERE status = 'running'
      AND NOT (id = ANY($3::uuid[]))
      AND lock_expires_at IS NULL
    UNION
    SELECT id FROM "{schema}".jobs
    WHERE status = 'running'
      AND NOT (id = ANY($3::uuid[]))
      AND lock_expires_at <= statement_timestamp() + $4::interval
)
UPDATE "{schema}".jobs
SET last_heartbeat_at = clock_timestamp(), lock_expires_at = clock_timestamp() + $2
FROM due
WHERE locked_by_worker = $1 AND status = 'running'
  AND NOT (jobs.id = ANY($3::uuid[]))
  AND jobs.id = due.id"""
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
