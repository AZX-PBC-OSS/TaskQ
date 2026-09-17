"""Leader/worker maintenance sweeps for PostgresBackend.

The five sweep operations are stateless (they take a connection and
schema, hold no instance state), so they live here as module-level
functions.  :class:`~taskq.backend.postgres.PostgresBackend` exposes
thin ``@staticmethod`` wrappers that delegate here, preserving the
existing ``PostgresBackend.sweep_*`` call surface.

Sweeps 1-3 are bounded batch writers: ONE call transitions at most
``batch_size`` rows, using a constant number of statements (a LIMIT-ed
driving UPDATE, one batched ``job_attempts`` INSERT where applicable,
one batched ``job_events`` INSERT, one ``pg_notify`` where applicable)
inside one short transaction that also applies a server-side
``statement_timeout``.  The timeout is bound via
``set_config('statement_timeout', ..., true)`` — ``SET LOCAL``
semantics with a bindable value — with the previous value captured
first and restored on the success path, because ``SET LOCAL``'s scope
is the whole transaction: a sweep nested in a caller's open transaction
(asyncpg runs its block as a savepoint) would otherwise leak its bound
past the savepoint RELEASE into the caller's subsequent statements.  On
the error path no restore is needed — the savepoint ROLLBACK restores
the GUC via PostgreSQL's subtransaction stack.  Repeated calls drain
the remainder, one committed batch at a time.  The bound exists because
:data:`~taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY` conditions the
``poll_reclaim_events`` trailing-watermark guarantee on no
``job_events`` writer holding its transaction open longer than the
margin between INSERT and COMMIT — an unbounded sweep is exactly the
"abnormally large batch inserted in one transaction" that docstring
names as a violation.

Every finished-at / terminal timestamp written in these sweeps uses
``clock_timestamp()``, not ``now()``: ``now()`` is fixed at transaction
*start*, so within a long-held sweep transaction it can disagree both
with other ``clock_timestamp()``-derived values in the same row and with
``job_events.occurred_at`` (also ``clock_timestamp()`` — see
``taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY`` for why that column's
co-monotonicity with ``job_events.id`` matters). ``duration_ms`` is
likewise computed against a ``clock_timestamp()`` returned by the sweep
statement itself — never against this process's clock, which would
offset it by the app/database skew even though both ``started_at`` and
the attempt row's ``finished_at`` are database-written.

The RANGE predicates (the ``<``/``<=`` comparisons against "now" that
select which rows a snap considers eligible) use
``statement_timestamp()``, not ``clock_timestamp()`` — a deliberate
two-clock split, and the difference is load-bearing for index use:

* ``clock_timestamp()`` is VOLATILE. PostgreSQL's planner refuses to
  use a volatile expression as a btree index condition (it cannot
  promise the bound holds for every row the scan would visit), so every
  ``col < clock_timestamp()`` degrades to a post-scan Filter: the scan
  must visit every row of the table (or of the partial index's whole
  population) and evaluate the comparison per row. Measured on a
  93k-row ``jobs`` table (PostgreSQL 18, EXPLAIN ANALYZE, BUFFERS): the
  sweep-3 snap seq-scanned all 93,000 rows (7.3 ms, 3,264 buffers) on
  the every-second leader tick in the empty steady state; with a stable
  bound the same snap is an Index Scan with an ``Index Cond`` that
  terminates at the boundary (2 buffers, ~0.02 ms).
* ``statement_timestamp()`` is STABLE and is the wall clock at the
  start of the *statement* — for a snap predicate it is semantically
  ``clock_timestamp()`` evaluated once at statement start (they differ
  only by the statement's own execution time, microseconds here: the
  snaps are LIMIT-ed and statement_timeout-bounded). It is therefore
  eligible as an index bound, and as a bonus both references to it in
  one WHERE clause agree exactly, where two ``clock_timestamp()``
  evaluations could drift by microseconds.

Written values (``finished_at``, ``now_ts``, ``occurred_at``, the
retry-backoff ``scheduled_at``) stay ``clock_timestamp()`` per the
co-monotonicity rationale above; only the row-selection bounds moved to
``statement_timestamp()``.

Each snap also carries an ``ORDER BY`` on the column its partial index
is keyed on. This is load-bearing for the same reason: with only a
LIMIT, the planner picks between partial-index population walks on
fractional-cost guesses (it assumes the first N entries of ANY
predicate-implied partial index pass the filter), and a stats skew can
make it walk the whole population of the WRONG partial index — measured:
the sweep-3 snap chose a 21,000-entry walk of
``jobs_schedule_to_close_idx`` (whose partial predicate
``status IN ('pending','scheduled')`` the snap implies, but whose key
column is a different timestamp) over the 2-buffer bounded scan of
``jobs_scheduled_wake_idx``. The ORDER BY pins the snap to the index
keyed on the predicate's own column — the same window-then-update
pattern the prune/archive sweep already uses with
``jobs_finished_at_idx`` (ORDER BY finished_at LIMIT), pinned by
``test_leader_prune.py``. The order is free at scan time (the index
provides it for the index-scan plans; where the planner prefers a
bitmap it pays only a top-N sort of the LIMIT-ed batch, never of the
backlog), makes the drain deterministic (oldest-eligible
first), and does NOT scan the whole backlog: the index-ordered scan
stops at the LIMIT in the backlog case and at the range boundary in the
empty case.

Batched timestamp columns (``job_events.occurred_at``, the attempt
rows' ``finished_at``/``started_at`` fallback) carry a microsecond
ladder on the row ordinal rather than a bare volatile
``clock_timestamp()``; see the long comment in
:mod:`taskq.backend._sql` above ``INSERT_EVENTS_DETAIL_BATCH_SQL`` for
why the ladder is load-bearing.
"""

import time
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import NamedTuple
from uuid import UUID

import structlog

from taskq.backend._protocol import ConnLike, JobId
from taskq.backend._records import compute_duration_ms, jsonb_param, parse_rowcount
from taskq.backend._sql import INSERT_EVENTS_DETAIL_BATCH_SQL, WAKE_NOTIFY_SQL
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_EVENT_RETENTION_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    DEFAULT_KEYED_ROW_RECLAIM_BATCH_SIZE,
    DEFAULT_MAX_RETRY_BACKOFF,
    RECLAIM_OUTBOX_RETENTION_MULTIPLIER,
    wake_channel,
)
from taskq.obs import get_logger, log_state_change, record_deadline_exceeded_swept

__all__ = [
    "_SWEEP_1_SQL",
    "_SWEEP_2_SQL",
    "_SWEEP_3_SQL",
    "_SWEEP_4_SQL",
    "_SWEEP_EVENT_TTL_SQL",
    "_SWEEP_IDLE_KEYED_BUCKETS_SQL",
    "_SWEEP_IDLE_KEYED_SLOTS_SQL",
    "_SWEEP_RESULT_TTL_SQL",
    "SweepBatchSizer",
    "prune_job_events",
    "sweep_deadline_exceeded",
    "sweep_expired_events",
    "sweep_expired_locks",
    "sweep_expired_results",
    "sweep_idle_keyed_rows",
    "sweep_leaked_reservation_slots",
    "sweep_scheduled_to_pending",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

# Schema identifier is interpolated at call time after validation against
# _IDENT_RE.  Prepared-statement cache is not preserved across calls, but
# sweep frequency is low (every 5 s on the leader).

# Recovery sweep transitions: running->scheduled when retries remain;
# running->crashed when exhausted.  The SQL serialises the read+write
# atomically via WHERE status='running', which is the single-source guard
# that the transition is valid.

_RECLAIM_HAS_BUDGET_SQL = "(j.retry_kind = 'indefinite' OR (j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable'))"
"""Whether a reclaimed job still has an attempt to give.

The same budget dimensions every other retry arbiter reads (see the
``mark_snoozed`` / ``mark_failed_or_retry`` templates): ``indefinite`` has
no attempt ceiling — its ``schedule_to_close`` deadline is its budget, and
the deadline sweep is what ends it — while every other kind is bounded by
``max_attempts``, and ``non_retryable`` has no second attempt at all.

Held as one fragment because the reclaim statement asks the same question
three times (which status to write, whether to reschedule, whether to
stamp a finish), and three hand-maintained copies is how the
``indefinite`` kind came to match none of them."""

# The exponent clamp mirrors taskq.retry's _MAX_BACKOFF_EXPONENT exactly
# (that module's comment carries the full derivation).  In short: 67 is
# the smallest bound that cannot change any timedelta-representable curve
# — the smallest positive base is 1e-6 s (timedelta resolution) and the
# largest representable cap is ~8.6e13 s (999999999 days), so every
# representable (base, cap) pair saturates by exponent 66 at the latest
# (1e-6 * 2**67 >= 1.4e14 >= 8.6e13), and the outer LEAST against the
# effective cap decides identically with or without the clamp — AND that
# cannot overflow: Postgres RAISES "value out of range: overflow"
# (SQLSTATE 22003) when the float8 multiply ``base * power(2.0, e)``
# exceeds ~1.8e308 — it does NOT saturate to Infinity the way Python's
# float arithmetic does — so the historical clamp at power()'s own domain
# ceiling (1023) still let the multiply raise for any base > ~2 once the
# attempt passed ~1021: a non-transient data error escaping into the
# leader sweep's failure path, on a corner the Python twin answered with
# the cap.  base * 2**67 <= ~1.3e34 for any timedelta-representable base,
# ~274 orders of magnitude below the float8 ceiling.
_RECLAIM_BACKOFF_MAX_EXPONENT_SQL = "67"

# The effective ceiling every clamp in the curve reads: the lesser of the
# row's stamped cap and the operator's global ``max_retry_backoff`` — the
# same ``min(policy.cap, max_retry_backoff)`` compute_backoff applies on
# the failure path. Held as one fragment so the three clamp sites (the
# exponential and linear arms' raw clamps, and the final post-jitter
# clamp) cannot drift on what "the cap" is. ``{max_backoff_seconds}`` is
# a named placeholder, not a ``$N``: the two statements embedding it (the
# reclaim sweep and the heartbeat isolate) carry different parameter
# layouts, so each binds the index its own layout assigns at the same
# place it binds the other shared fragments.
_RECLAIM_EFFECTIVE_CAP_SQL = "LEAST(j.retry_cap_seconds, {max_backoff_seconds}::float8)"

# raw_seconds: the unjittered curve value for this row's (base, cap,
# backoff, attempt) — byte-for-byte the same three-way branch and
# LEAST(effective-cap, ...) clamp compute_backoff applies for
# attempt=j.attempt (a reclaimed row's attempt is the CURRENT,
# not-yet-refunded claim dispatch stamped, 1-indexed the same as
# compute_backoff's parameter — see mark_failed_or_retry's call site).
# Only the exponential arm's EXPONENT is attempt-1 (compute_backoff:
# ``2.0 ** min(attempt - 1, _MAX_BACKOFF_EXPONENT)``); the linear arm
# multiplies by attempt itself (``base_s * attempt``), not attempt-1 —
# the GREATEST(j.attempt - 1, 0) clamp below applies ONLY to the
# exponent, matching that asymmetry exactly.
_RECLAIM_RAW_BACKOFF_SQL = (
    "(CASE j.retry_backoff "
    f"WHEN 'exponential' THEN LEAST({_RECLAIM_EFFECTIVE_CAP_SQL}, "
    "j.retry_base_seconds * power(2.0, LEAST(GREATEST(j.attempt - 1, 0), "
    f"{_RECLAIM_BACKOFF_MAX_EXPONENT_SQL}))) "
    f"WHEN 'linear' THEN LEAST({_RECLAIM_EFFECTIVE_CAP_SQL}, "
    "j.retry_base_seconds * j.attempt) "
    "ELSE j.retry_base_seconds "
    "END)"
)
"""``compute_backoff``'s raw (pre-jitter) curve value, evaluated in SQL
from the row's own stamped policy columns rather than a live
``RetryPolicy`` object — see the migration's comment for why the columns
exist."""

# Why a hash, not random(): one row's reclaim delay is computed by more
# than one statement — the leader's sweep below and a partitioned worker's
# isolate_self (worker/heartbeat.py's _ISOLATE_JOB_SQL_TEMPLATE embeds the
# same {reclaim_delay} fragment) can each transition the same row within
# one outage window — and by the in-memory twin a third time.  A
# per-statement random() makes those paths disagree about the same row's
# hand-back instant (the test_property_sweep_equivalence flake class: two
# draws, one fixed comparison window) and makes a replayed sweep stamp a
# different instant than the first pass.  Deriving the draw from the row's
# own (id, attempt) makes every path stamp the SAME delay, so a reclaim
# replay is idempotent per row — strictly more robust than a fresh draw.
# The fleet spread random() bought is preserved: distinct ids hash to
# distinct fractions, so a mass-expired cohort still arrives spread across
# the jitter band instead of at one synchronised point.  (A per-evaluation
# random draw needs exactly one process ever to compute a given row's
# retry delay; the per-row hash here is the
# dual-statement, dual-implementation parity requirement, not a different
# spreading goal.)
#
# The formula is byte-identical to taskq.retry._reclaim_jitter_fraction:
# md5 of '<job_id>:<attempt>' as ASCII text (j.id::text is the lowercase
# dashed uuid form Python's str() produces, j.attempt::text the plain
# decimal — both pure ASCII, so the database encoding cannot change the
# hashed bytes), the first 8 hex digits read as a uint32 (the 'x'-prefixed
# bit(32) cast keeps the value non-negative through ::bigint), divided by
# 2**32 in float8.  Both sides run the same correctly-rounded IEEE-754
# operations, so the fractions — and the delays built on them below —
# agree bit for bit (pinned by tests/test_reclaim_backoff_policy_parity.py).
_RECLAIM_JITTER_FRACTION_SQL = (
    "(('x' || substr(md5(j.id::text || ':' || j.attempt::text), 1, 8))"
    "::bit(32)::bigint::float8 / 4294967296.0::float8)"
)
"""The reclaim jitter fraction in [0, 1): deterministic per (job, attempt),
identical in this database and in ``taskq.retry._reclaim_jitter_fraction``."""

# The jitter band fitted under the effective cap — the twin of
# taskq.retry._capped_jitter_band: the raw curve value clamped to the cap,
# the band's lower edge raw·(1-j), its upper edge raw·(1+j) clamped to the
# cap. The cap bounds the BAND, not the drawn value: clamping the drawn
# value collapsed the upper half of a saturated row's band onto the cap,
# so half of any cohort reclaimed at the ceiling came due at one instant.
_RECLAIM_CAPPED_RAW_SQL = f"LEAST({_RECLAIM_EFFECTIVE_CAP_SQL}, {_RECLAIM_RAW_BACKOFF_SQL})"
_RECLAIM_BAND_LOWER_SQL = f"({_RECLAIM_CAPPED_RAW_SQL} * (1.0 - j.retry_jitter))"
_RECLAIM_BAND_UPPER_SQL = (
    f"LEAST({_RECLAIM_CAPPED_RAW_SQL} * (1.0 + j.retry_jitter), {_RECLAIM_EFFECTIVE_CAP_SQL})"
)

_RECLAIM_DELAY_SQL = (
    # lower + (upper - lower) · fraction, then a closing LEAST that only
    # absorbs float rounding at the top edge (the band already lies inside
    # [0, cap]) — operand for operand taskq.retry._draw_in_band.
    f"(LEAST({_RECLAIM_EFFECTIVE_CAP_SQL}, {_RECLAIM_BAND_LOWER_SQL} "
    f"+ ({_RECLAIM_BAND_UPPER_SQL} - {_RECLAIM_BAND_LOWER_SQL}) "
    f"* {_RECLAIM_JITTER_FRACTION_SQL})) * interval '1 second'"
)
"""How far out a reclaimed job is rescheduled — the job's own
``RetryPolicy`` curve (base, cap, backoff kind, jitter), stamped on the
row at enqueue time, evaluated exactly as the reclaim curve twin
:func:`taskq.retry._compute_reclaim_backoff` would for this attempt,
including its ceiling: the lesser of the row's stamped cap and the
operator's ``max_retry_backoff``, bound per statement through the
``{max_backoff_seconds}`` placeholder, and including the band fitted
under that ceiling so a cohort at the cap spreads over ``[cap·(1-j), cap]``.

A fleet-wide event — a node drain, a zone loss, an OOM sweep across a
deployment — expires many leases at once, and one sweep hands the whole
cohort back.  A flat delay would stamp every row with the same instant,
so the entire backlog would become due together and land on the
still-recovering fleet as one synchronised wave.  The multiplicative-
symmetric jitter here is the same spreading mechanism the failure
backoff applies, evaluated per row from the row's own deterministic
fraction (see ``_RECLAIM_JITTER_FRACTION_SQL``: same row, same delay on
every path — sweep, isolate, replay, mirror), so the cohort arrives
spread across a band instead of at a point."""


#: The per-arm attempt-row error messages, keyed by the sweep's own
#: reclaim_reason literals ('lock_expired' / 'heartbeat_timeout' — the
#: same values the arms' reason columns and the events' cause key carry).
#: One map, imported by the PG caller (which feeds it to the batched
#: INSERT as the $6 text array), the in-memory twin (which builds its
#: AttemptRow from it), and _SWEEP_1_SQL's own crashed-arm row SET (via
#: the {lock_expired_message} / {heartbeat_timeout_message} fragments), so
#: the row, the attempt, and the twin's copies cannot drift.
_ATTEMPT_MESSAGES: dict[str, str] = {
    "lock_expired": "lock expired before worker reported terminal state",
    "heartbeat_timeout": "heartbeat timeout passed before worker reported terminal state",
}


_SWEEP_1_BODY = """\
-- Leader-only reclaim sweep (per architecture §Leader Election).  FOR
-- UPDATE SKIP LOCKED is kept so the SQL is safe if the sweep is ever run
-- concurrently; the production leader loop serializes it.
--
-- Two disjoint eligibility arms, one UPDATE:
--
-- * lease arm — the row's lock_expires_at has passed: the holder's
--   global lease (TASKQ_LOCK_LEASE) expired.
-- * heartbeat arm — the row carries a per-job heartbeat_timeout and its
--   holder has been silent past it (last_heartbeat_at + heartbeat_timeout
--   < now) while the lease is STILL valid. The lease is the per-worker
--   global; heartbeat_timeout is the per-job promise, and the shorter of
--   the two deadlines governs (the parameter was plumbed end to end
--   and read by nothing — a safety knob that silently no-ops). The
--   heartbeat loop refreshes last_heartbeat_at for every running job it
--   holds, so job-level heartbeat silence IS holder silence; a live
--   holder's fresh beats keep the arm quiet. NULL last_heartbeat_at
--   (direct-SQL-reachable only — dispatch always stamps it) is never
--   eligible: NULL + interval is NULL, so the row waits for its lease.
--   The knob test is `heartbeat_timeout > interval '0'`, not merely NOT
--   NULL: enqueue validation refuses a non-positive value but it is the
--   only gate, and the column carries no CHECK, so any direct write —
--   a row stored before the knob was enforced, a manual UPDATE — can
--   leave a zero or negative interval behind. With one, `last_beat +
--   timeout < now` is true from the instant the row is written, and a
--   maximally healthy holder beating right now with an hour of lease
--   left is reclaimed as a false crash on the very first sweep. A
--   degenerate value is inert: the lease governs, as it does with no
--   knob at all. NULL > interval '0' is NULL, so the NOT-NULL case is
--   still excluded, and the comparison stays compatible with the
--   partial index's own IS NOT NULL predicate.
--
-- The arms are DISJOINT by construction — the heartbeat arm requires
-- lock_expires_at >= statement_timestamp(), so a row that is both
-- lease-expired and heartbeat-stale is owned by the lease arm alone.
-- That is what makes UNION ALL safe here: a row satisfying both arms
-- would reach the UPDATE twice, and the batched job_attempts INSERT
-- would then violate job_attempts' PRIMARY KEY (job_id, attempt) — a
-- non-transient error that escapes the sweep loop and tears down the
-- leader worker, leaving the orphan unreclaimed with no live worker to
-- reclaim it.
--
-- The cancel_phase != 0 carve-out below adds a flat extra 60 seconds on
-- top of cancel_grace + cleanup_grace before a job with an in-flight
-- cancel request becomes eligible for crash-reclaim on EITHER arm.
-- This is a fixed safety margin, not derived from any other setting: it
-- gives the cooperative-cancel/escalation protocol (see the
-- cancellation-protocol section of docs/architecture.md) extra headroom
-- to complete on its own before the crash-recovery path pre-empts it,
-- so a merely-slow (not actually crashed) cancellation isn't mistaken
-- for a crash. The heartbeat arm's carve-out uses the same grace ladder
-- applied to its own deadline expression.
--
-- Cancel-state handling on reclaim (operator intent outranks retry
-- budget, the ordering #238 pinned, supersedes the old reset-on-
-- re-pend tradeoff):
-- * Cancel branch (cancel_phase != 0, ANY budget): the row is
--   terminalised 'cancelled', NOT re-pended: the lock-holding
--   worker is the only writer that could honour the request
--   cooperatively, and the carve-out above already gave that
--   worker cancel_grace + cleanup_grace + 60s to finish; past it
--   the row is provably abandoned mid-protocol, and the caller's
--   explicit request is the honest terminal label: anyone
--   reconciling terminal states sees the cancel was honored. The
--   row KEEPS its cancel_phase/cancel_requested_at as the audit
--   trail of the honoured request, exactly as mark_cancelled does.
-- * Retry branch (cancel_phase = 0, budget remains): 'pending' on
--   the reclaim delay. The row's cancel columns were already
--   clean (the CASE fell through the cancel arm), so the re-pend
--   cannot hand a next claimant an inherited phase: the re-cancel
--   loop the old reset-on-re-pend spelling existed to prevent is
--   now excluded structurally: no arm this statement owns can
--   produce a re-pended row carrying cancel columns.
-- * Crashed branch (cancel_phase = 0, no budget): 'crashed' as
--   before, error fields self-describing on the row.
--   The job_attempts row records outcome='crashed' on every
--   branch: that IS what happened to the attempt.
--
-- THE SHAPE (vendor/river's JobCancel + JobRescuer): operator intent
-- outranking reclaim-driven retry is river's pattern too. JobCancel
-- (vendor/river/riverdriver/riverpgxv5/internal/dbsqlc/
-- river_job.sql:40-77) leaves a running row running, the cooperative
-- protocol belongs to the live holder, and stamps
-- metadata.cancel_attempted_at "so that the rescuer knows not to
-- rescue it, even if it gets stuck in the running state"; the rescuer
-- (vendor/river/internal/maintenance/job_rescuer.go:195-259) checks
-- that stamp and routes a stamped stuck job straight to 'cancelled',
-- never into the retry decision. Two deliberate divergences here:
-- the marker rides first-class columns (cancel_phase /
-- cancel_requested_at, readable as an audit trail and PRESERVED on
-- the terminal row) rather than a metadata JSONB stamp, and the
-- terminalisation happens in THIS statement, under the grace ladder
-- the carve-out above already waited out, rather than in a separate
-- rescuer on its own stuck horizon (river's default is an hour) during
-- which a cancel-addressed row whose holder is dead simply sits
-- running.
--
-- locked_by_worker is snapshotted raw (the last-known holder id, even when
-- that worker's workers row was already removed by cleanup_stale_workers on
-- an earlier tick — possible whenever the stale-worker window,
-- heartbeat_interval * (max_heartbeat_failures + 3), is shorter than the
-- lease). The job_attempts INSERT resolves it through the holder-CTE idiom
-- (see _sql.py's INSERT_ATTEMPT_SQL): a present parent records the id, a
-- deleted one records NULL (mirroring the column's ON DELETE SET NULL), so
-- the INSERT cannot FK-violate on the dangling id — which would escape the
-- sweep loop (a constraint violation is deliberately non-transient) and
-- tear down the leader worker, leaving the orphan unreclaimed with no live
-- worker to reclaim it. Keeping the join OUT of this statement also keeps
-- the workers probe out of the hot jobs-table scan: it happens once per
-- RECLAIMED batch in the attempt INSERT, not per candidate row.
--
-- Bounded batch: each arm's snap carries its own LIMIT $3, so one call
-- transitions at most 2 x batch_size rows (a constant bound — the bounded-
-- writes doctrine's requirement is that no statement is unbounded, not
-- that every bound be the same number) and the transaction (and the FOR
-- UPDATE row locks it holds) spans a constant number of statements for
-- a constant number of rows; the caller's loop drains the remainder one
-- committed batch at a time, and the disjoint arms mean no row is
-- transitioned twice across those batches.
--
-- MATERIALIZED is load-bearing.  Without it the planner may inline the
-- LIMIT-ed CTE into the UPDATE as a nested loop over the target table
-- and update more rows than the LIMIT (the LIMIT then bounds only the
-- CTE's inlined appearances, not the joined result).  A CTE containing
-- FOR UPDATE is not inlinable today; the keyword pins that fence so a
-- future planner change cannot silently unbound the sweep.
--
-- ORDER BY + the statement_timestamp() bounds are load-bearing TOGETHER
-- (see the module docstring for the full derivation): the planner will
-- not use a VOLATILE clock_timestamp() comparison as a btree index
-- condition, so the bound must be STABLE (statement_timestamp() — the
-- wall clock at this statement's start, semantically clock_timestamp()
-- evaluated once) to become an Index Cond, and each arm's ORDER BY pins
-- its scan to its own partial index (the ORDER-BY-pins-the-scan rule)
-- so the planner cannot instead fractional-walk some other
-- predicate-implied partial index. The lease arm seeks
-- jobs_running_lock_expires_idx (partial on status='running', keyed on
-- lock_expires_at) — measured on a 93k-row jobs table (PG 18, EXPLAIN
-- ANALYZE, BUFFERS): clock_timestamp() bound + no ORDER BY seq-scanned
-- 93,000 rows (7.3 ms) / population-walked 10,000 index entries (5,143
-- buffers) per empty-state tick; the ordered STABLE-bound form is an
-- Index Scan with Index Cond that stops at the range boundary (2
-- buffers, ~0.02 ms empty; ~1 buffer per reclaimed row in backlog,
-- oldest-expired first). The heartbeat arm seeks
-- jobs_running_heartbeat_deadline_idx (partial on status='running' AND
-- heartbeat_timeout IS NOT NULL, keyed on last_heartbeat_at — migration
-- 01.00.10_01). Its row-exact deadline (last_heartbeat_at +
-- heartbeat_timeout < statement_timestamp()) CANNOT be an index
-- condition — the bound is row-dependent, and timestamptz + interval is
-- STABLE, so no expression index may even exist on it — so the arm
-- states the necessary condition (last_heartbeat_at <
-- statement_timestamp()) explicitly to give the partial index a
-- range bound, and the row-exact deadline rides as a filter over the
-- tiny partial index (only heartbeat-configured running rows ever
-- enter it). Both index scans stop at their LIMIT in a backlog and at
-- their age boundary in the empty steady state.
--
-- No keyset cursor either: every row the snap returns is transitioned by
-- this same statement, so the eligible set shrinks monotonically per
-- committed batch — there is no "later page" to resume into, the next
-- call simply sees the remainder.  SKIP LOCKED steps over contended rows
-- rather than blocking, so no front-of-order row can starve the rest.
--
-- Each arm carries a reason literal out through the RETURNING: the
-- crash-reclaim outbox channel is keyed on detail reason='lock_expired'
-- (the slice poll_reclaim_events tails under the trailing-watermark
-- protocol and the event-retention carve-out keeps — see
-- _SWEEP_EVENT_TTL_SQL), so BOTH arms ride that channel, and the
-- consumer-visible distinction (which deadline fired) is carried as the
-- batched event's separate detail cause key.
--
-- Shape note: the arms are locked CTEs unioned by a plain snap CTE
-- rather than one UNION statement, because Postgres forbids FOR UPDATE
-- in the arms of a set operation ("FOR UPDATE is not allowed with
-- UNION/INTERSECT/EXCEPT") — each CTE body is a plain SELECT, where the
-- locking clause is legal, and the disjoint arms make the lock
-- semantics uninteresting: no row can be visited by both arms.
WITH lease_arm AS MATERIALIZED (
    SELECT id, locked_by_worker, 'lock_expired'::text AS reason
    FROM "{schema}".jobs
    WHERE status = 'running'
      AND lock_expires_at < statement_timestamp()
      AND (cancel_phase = 0
           OR lock_expires_at < statement_timestamp() - $1::interval - $2::interval - interval '60 seconds')
    ORDER BY lock_expires_at
    LIMIT $3
    FOR UPDATE SKIP LOCKED
),
heartbeat_arm AS MATERIALIZED (
    SELECT id, locked_by_worker, 'heartbeat_timeout'::text AS reason
    FROM "{schema}".jobs
    WHERE status = 'running'
      AND heartbeat_timeout > interval '0'
      AND lock_expires_at >= statement_timestamp()
      AND last_heartbeat_at < statement_timestamp()
      AND last_heartbeat_at + heartbeat_timeout < statement_timestamp()
      AND (cancel_phase = 0
           OR last_heartbeat_at + heartbeat_timeout
                  < statement_timestamp() - $1::interval - $2::interval - interval '60 seconds')
    ORDER BY last_heartbeat_at
    LIMIT $3
    FOR UPDATE SKIP LOCKED
),
snap AS (
    SELECT * FROM lease_arm
    UNION ALL
    SELECT * FROM heartbeat_arm
)
UPDATE "{schema}".jobs j
SET status = CASE
        -- Operator intent outranks retry budget. The cancel arm is
        -- evaluated FIRST: a row carrying cancel_phase != 0 is
        -- terminalised 'cancelled' whether or not retries remain.
        -- The pre-reorder shape (budget first) re-pended such a row
        -- and wiped its cancel columns: the operator's request
        -- silently lost against a dead worker that could never
        -- honour it. Ordering cancel first cannot resurrect the
        -- re-cancel loop the reset was introduced to prevent: that
        -- loop required a RE-PENDED row still carrying cancel
        -- columns (each new claimant's cancel-poll re-raises the
        -- phase, and a reclaim that leaves the columns set re-pends
        -- it again). The cancel arm never re-pends, it
        -- terminalises, so no row it touches can re-enter the
        -- claim/reclaim cycle, and the re-pend arm below now reads
        -- cancel_phase = 0 by construction (the CASE fell through
        -- the cancel arm), so the columns it resets were already
        -- clean. The exhausted-with-cancel shape this arm always
        -- owned keeps its exact old outcome.
        WHEN j.cancel_phase != 0
            THEN 'cancelled'::"{schema}".job_status
        WHEN {has_budget}
            THEN 'pending'::"{schema}".job_status
        ELSE 'crashed'::"{schema}".job_status
    END,
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    -- The cancel columns survive exactly the arm that honoured
    -- them. A terminal 'cancelled' row keeps phase and
    -- cancel_requested_at as its audit trail: the same doctrine
    -- every other terminal cancel path carries (mark_cancelled
    -- deliberately sets neither column, and mark_abandoned's
    -- cancel_phase = 2 guard reads them back). The re-pend and
    -- crashed arms read 0/NULL by construction after the CASE
    -- reorder (the re-pend arm fell through `cancel_phase != 0`;
    -- no writer stamps cancel_requested_at without phase 1), so
    -- the CASE arms are no-ops there, kept as defence-in-depth
    -- against direct-SQL shapes.
    cancel_phase = CASE WHEN j.cancel_phase != 0 THEN j.cancel_phase ELSE 0 END,
    cancel_requested_at = CASE
        WHEN j.cancel_phase != 0 THEN j.cancel_requested_at
        ELSE NULL END,
    -- A reclaim hands the row back to the fleet, so it routes by the
    -- actor's current assignment from here on (the routing contract in
    -- taskq/backend/_dispatch_sql.py). Set on every arm: a row that
    -- terminalises instead is not dispatchable, and the flag is inert.
    assignment_routed = true,
    -- The re-pend arm alone reschedules: its membership after the
    -- reorder is exactly `cancel_phase = 0 AND {has_budget}`: the
    -- cancelled and crashed arms keep the row's own scheduled_at.
    scheduled_at = CASE
        WHEN j.cancel_phase = 0 AND {has_budget}
            THEN clock_timestamp() + {reclaim_delay}
        ELSE j.scheduled_at
    END,
    -- finished_at is stamped on every terminal arm: cancelled (any
    -- budget) and crashed alike. The pre-reorder spelling keyed on
    -- the budget alone, which after the reorder would leave a
    -- budget-carrying cancelled row with a NULL finished_at.
    finished_at = CASE
        WHEN j.cancel_phase != 0 OR NOT ({has_budget})
            THEN clock_timestamp()
        ELSE j.finished_at
    END,
    -- The crashed arm self-describes on the row: every other terminal
    -- failure path stamps error_class (DeadlineExceeded on the deadline
    -- sweep, the cancel-origin markers on the cancel paths), so a
    -- crashed row carrying NULL forced an operator to join job_attempts
    -- to learn why. The message names the deadline THAT fired, read off
    -- snap.reason — never the sibling arm's, the same honesty standard
    -- the attempt rows carry; row and attempt draw from the one
    -- _ATTEMPT_MESSAGES map so the two audit surfaces cannot drift.
    -- The re-pend and cancelled arms keep their error fields untouched:
    -- a re-pended row has no failure to describe yet, and a
    -- cancel-honouring row's record is the in-flight request — no
    -- cancel-origin marker describes a worker that died mid-protocol,
    -- so the arm stamps nothing and the attempt row's WorkerCrashed
    -- plus the event's cause carry the explanation.
    error_class = CASE
        WHEN NOT ({has_budget}) AND j.cancel_phase = 0
            THEN 'WorkerCrashed'
        ELSE j.error_class
    END,
    error_message = CASE
        WHEN NOT ({has_budget}) AND j.cancel_phase = 0
            THEN CASE snap.reason
                     WHEN 'lock_expired' THEN '{lock_expired_message}'
                     ELSE '{heartbeat_timeout_message}'
                 END
        ELSE j.error_message
    END
FROM snap
WHERE j.id = snap.id
RETURNING j.id, j.status, j.attempt, j.started_at, snap.locked_by_worker,
          snap.reason AS reclaim_reason, clock_timestamp() AS now_ts"""

#: The sweep with its shared fragments bound, still carrying ``{schema}``
#: for the caller. The fragments are substituted by name rather than
#: through ``format`` so this stays a one-placeholder template — callers
#: and tests render it with ``.format(schema=...)`` and nothing else.
#: ``$4`` is the effective-cap ceiling (max_retry_backoff, seconds), the
#: statement's fourth parameter after the two grace intervals and the
#: batch LIMIT.
_SWEEP_1_SQL = (
    _SWEEP_1_BODY.replace("{has_budget}", _RECLAIM_HAS_BUDGET_SQL)
    .replace("{reclaim_delay}", _RECLAIM_DELAY_SQL)
    .replace("{max_backoff_seconds}", "$4")
    .replace("{lock_expired_message}", _ATTEMPT_MESSAGES["lock_expired"])
    .replace("{heartbeat_timeout_message}", _ATTEMPT_MESSAGES["heartbeat_timeout"])
)

_SWEEP_2_SQL = """\
-- Bounded batch + MATERIALIZED, same rationale as _SWEEP_1_SQL's comment
-- block: LIMIT caps one call's lock-hold and write set; MATERIALIZED
-- stops the planner from inlining the LIMIT-ed CTE into the UPDATE in a
-- way that could update more rows than the LIMIT (a CTE containing FOR
-- UPDATE is not inlinable today, the keyword pins that fence); ORDER BY
-- on the snap's partial-index key column plus the STABLE
-- statement_timestamp() bound make the snap an Index Scan whose Index
-- Cond terminates at the range boundary (a VOLATILE clock_timestamp()
-- bound cannot be an index condition — see _SWEEP_1_SQL's comment and
-- the module docstring for the measured plans); no keyset cursor
-- because every snapped row is transitioned by this same statement, so
-- the eligible set shrinks monotonically per committed batch and SKIP
-- LOCKED steps over contention instead of blocking on a front-of-order
-- row.
WITH snap AS MATERIALIZED (
    SELECT id, status AS prev_status
    FROM "{schema}".jobs
    WHERE status IN ('pending', 'scheduled')
      AND schedule_to_close IS NOT NULL
      AND schedule_to_close < statement_timestamp()
    ORDER BY schedule_to_close
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE "{schema}".jobs j
SET status = 'failed'::"{schema}".job_status,
    finished_at = clock_timestamp(),
    error_class = 'DeadlineExceeded',
    error_message = 'schedule_to_close reached before next dispatch'
FROM snap
WHERE j.id = snap.id
RETURNING j.id, snap.prev_status, j.attempt, j.started_at, j.actor,
          clock_timestamp() AS now_ts"""

_SWEEP_3_SQL = """\
-- Bounded batch + MATERIALIZED, same rationale as _SWEEP_1_SQL's comment
-- block: LIMIT caps one call's lock-hold and write set; MATERIALIZED
-- stops the planner from inlining the LIMIT-ed CTE into the UPDATE in a
-- way that could update more rows than the LIMIT (a CTE containing FOR
-- UPDATE is not inlinable today, the keyword pins that fence); ORDER BY
-- on the snap's partial-index key column plus the STABLE
-- statement_timestamp() bound make the snap an Index Scan whose Index
-- Cond terminates at the range boundary (a VOLATILE clock_timestamp()
-- bound cannot be an index condition — see _SWEEP_1_SQL's comment and
-- the module docstring for the measured plans); no keyset cursor
-- because every snapped row is transitioned by this same statement, so
-- the eligible set shrinks monotonically per committed batch and SKIP
-- LOCKED steps over contention instead of blocking on a front-of-order
-- row.
WITH snap AS MATERIALIZED (
    SELECT id, status AS prev_status
    FROM "{schema}".jobs
    WHERE status = 'scheduled'
      AND scheduled_at <= statement_timestamp()
    ORDER BY scheduled_at
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
UPDATE "{schema}".jobs j
SET status = 'pending'::"{schema}".job_status
FROM snap
WHERE j.id = snap.id
RETURNING j.id, snap.prev_status"""

_SWEEP_4_SQL = """\
-- Bounded batch + MATERIALIZED, same rationale as the sibling sweeps
-- above: LIMIT $1 caps one call's write set, so a whole-fleet crash
-- (every slot's lease expiring at once) drains in committed batches
-- instead of one unbounded UPDATE; MATERIALIZED stops the planner from
-- inlining the LIMIT-ed CTE into the UPDATE in a way that could release
-- more slots than the LIMIT; ORDER BY lease_expires_at plus the STABLE
-- statement_timestamp() bound make the window an Index Scan whose Index
-- Cond terminates at the range boundary on
-- reservation_slots_lease_expires_idx (partial on job_id IS NOT NULL,
-- keyed on lease_expires_at) — a VOLATILE clock_timestamp() bound cannot
-- be a btree index condition (see the module docstring's two-clock
-- doctrine), and without the ORDER BY the planner may fractional-walk
-- some other predicate-implied partial index (the ORDER-BY-pins-the-scan
-- rule above); no keyset cursor because every windowed row is nulled by
-- this same statement, so the eligible set shrinks monotonically per
-- committed batch.
--
-- The outer re-check of job_id IS NOT NULL keeps a concurrent duplicate
-- sweep (possible during a rolling deploy before the leader lock names
-- converge) a no-op rather than a count-inflating rewrite: a row another
-- leader nulled between window and UPDATE falls out here, so the
-- affected-row count stays the number of rows this call actually
-- released — same shape as _SWEEP_RESULT_TTL_SQL.
WITH expired AS MATERIALIZED (
    SELECT bucket_name, slot_index
    FROM "{schema}".reservation_slots
    WHERE lease_expires_at < statement_timestamp()
      AND job_id IS NOT NULL
    ORDER BY lease_expires_at
    LIMIT $1
)
UPDATE "{schema}".reservation_slots r
SET job_id            = NULL,
    held_by_worker_id = NULL,
    acquired_at       = NULL,
    lease_expires_at  = NULL
FROM expired
WHERE (r.bucket_name, r.slot_index) = (expired.bucket_name, expired.slot_index)
  AND r.job_id IS NOT NULL"""

_SWEEP_RESULT_TTL_SQL = """\
-- Bounded batch + MATERIALIZED, same rationale as the sweep comments
-- above: LIMIT $1 caps one call's write set; MATERIALIZED stops the
-- planner from inlining the LIMIT-ed CTE into the UPDATE in a way that
-- could rewrite more rows than the LIMIT; no ORDER BY because no other
-- predicate-implied partial index competes for this snap (the only
-- partial index on result IS NOT NULL is the keyed one, so the STABLE
-- statement_timestamp() bound alone reliably plans as an Index Cond —
-- measured: 2 buffers in the empty steady state); no keyset cursor
-- because every windowed row is nulled by this same statement, so the
-- eligible set shrinks monotonically per committed batch.
--
-- statement_timestamp() (STABLE) instead of clock_timestamp() (VOLATILE)
-- is what lets the planner use jobs_result_expires_at_idx as a range
-- bound rather than a post-scan filter — same derivation as _SWEEP_1_SQL.
--
-- The outer re-check of the eligibility predicate keeps a concurrent
-- duplicate sweep (possible during a rolling deploy before the leader
-- lock names converge) a no-op rather than a count-inflating rewrite:
-- the window is a snapshot of ids, and a row another leader nulled
-- between window and UPDATE falls out here, so the returned count stays
-- the number of rows this call actually expired.
WITH expired AS MATERIALIZED (
    SELECT id
    FROM "{schema}".jobs
    WHERE result_expires_at < statement_timestamp()
      AND result IS NOT NULL
    LIMIT $1
)
UPDATE "{schema}".jobs j
SET result = NULL,
    result_size_bytes = NULL,
    result_expires_at = NULL
FROM expired
WHERE j.id = expired.id
  AND j.result IS NOT NULL"""

_SWEEP_EVENT_TTL_SQL = """\
-- Bounded batch + MATERIALIZED, same rationale as the sweep comments
-- above: LIMIT $2 caps one call's DELETE; MATERIALIZED stops the planner
-- from inlining the LIMIT-ed CTE into the DELETE in a way that could
-- remove more rows than the LIMIT; ORDER BY (occurred_at, id) pins the
-- window to the retention partial index keyed on exactly those columns
-- (the ORDER-BY-pins-the-scan rule above), so the drain is deterministic
-- (oldest-first, stable under tied occurred_at) and the ordered scan
-- stops at the LIMIT in the backlog case and at the age boundary in the
-- empty case; no keyset cursor because every windowed row is deleted by
-- this same statement, so the eligible set shrinks monotonically per
-- committed batch.
--
-- statement_timestamp() (STABLE) instead of clock_timestamp() (VOLATILE)
-- is what lets the planner use job_events_occurred_at_idx as a range
-- bound rather than a post-scan filter — same derivation as _SWEEP_1_SQL
-- and the module docstring's two-clock doctrine.
--
-- The carve-out is load-bearing and sits INSIDE the windowing CTE: the
-- kind='state_change' AND COALESCE(detail->>'reason','')='lock_expired'
-- slice is the crash-reclaim outbox poll_reclaim_events tails under a
-- trailing-watermark protocol (see poll_reclaim_events in
-- _sql_templates.py) — deleting an unconsumed outbox row too early
-- silently loses a crashed worker's reclaim, so the sweep keeps the slice
-- past the ordinary retention age (the expired_outbox arm below deletes
-- it only at RECLAIM_OUTBOX_RETENTION_MULTIPLIER x that age — the
-- derivation of the multiplier is in constants.py). Inside the CTE the
-- LIMIT applies to the already-filtered deletable-only set; hoisted into
-- the outer DELETE it would let an outbox-dominated prefix of the oldest
-- rows fill the window batch after batch while the DELETE matched
-- nothing — a drain that scans LIMIT rows every call yet never deletes,
-- i.e. under-deletion caused by the carve-out's own placement.
--
-- Why COALESCE and not a bare (detail->>'reason') = 'lock_expired':
-- detail->>'reason' is NULL for every event whose detail carries no
-- reason key, so the naive NOT (kind = 'state_change' AND
-- (detail->>'reason') = 'lock_expired') evaluates to NULL — not TRUE —
-- for an ordinary state_change row, and WHERE drops it: state_change is
-- the most common event kind, so the naive form silently exempts nearly
-- the whole table from retention. COALESCE makes a missing reason
-- simply not-'lock_expired', which is the deletable set the sweep owes.
--
-- The carve-out predicate must stay VERBATIM-identical to the partial
-- index's WHERE clause in migration 01.00.07_01: partial-index predicate
-- matching requires the query to repeat the index's predicate, and the
-- verbatim repeat (same literals, same parentheses) is the proof the
-- planner matches.
--
-- Measured plan shape (PG 18, EXPLAIN (ANALYZE, BUFFERS), retention
-- literal '30 days', LIMIT 100): the windowing CTE is an Index Only Scan
-- on job_events_occurred_at_idx with the occurred_at bound as an Index
-- Cond — 5 buffers per 100-row batch at a 355k-row table, one index
-- search, stops at the LIMIT, oldest-first. The partial-index predicate
-- proof eliminates the carve-out from execution entirely: no Filter
-- line, the predicate never re-evaluated, outbox rows never visited. The
-- outer DELETE joins by PK probe per windowed row at volume (Nested
-- Loop, 100 job_events_pkey Index Scans, 400 buffers); a 6.5k-row table
-- plans that same join as an 88-buffer heap seq scan — a small-table
-- cost artifact on the DELETE side, not the windowing scan. After
-- draining every deletable row the same EXPLAIN still plans the Index
-- Only Scan (0 rows, 21 buffers): the empty steady-state tick stays
-- index-served. Heap Fetches on the index-only scan track visibility-map
-- freshness (freshly inserted rows are not yet all-visible); steady-state
-- autovacuum keeps them near zero.
WITH expired AS MATERIALIZED (
    SELECT id
    FROM "{schema}".job_events
    WHERE occurred_at < statement_timestamp() - $1::interval
      AND NOT (kind = 'state_change' AND COALESCE(detail->>'reason', '') = 'lock_expired')
    ORDER BY occurred_at, id
    LIMIT $2
),
expired_outbox AS MATERIALIZED (
    -- The outbox age-cap arm: the carve-out above keeps unconsumed
    -- lock_expired rows past the ordinary retention age so a lagging
    -- consumer's watermark can still reach them, but NOT at every age --
    -- a fleet with NO watch_reclaims consumer would retain every
    -- lock_expired event forever, and a row committed below a cursor
    -- that already passed it is unreachable to the poll (id > $1 cannot
    -- go back), so without this arm such a row is both undeliverable and
    -- undeletable. The cap is RECLAIM_OUTBOX_RETENTION_MULTIPLIER x the
    -- same retention argument (constants.py derives the value against
    -- both pinned ages).
    --
    -- Predicate form: the bare (detail->>'reason') = 'lock_expired'
    -- (no COALESCE) is the VERBATIM WHERE clause of the
    -- job_events_reclaim_idx partial index (01.00.02_01), so the planner
    -- proves the index predicate and the scan is confined to the outbox
    -- population instead of the whole table. The positive form needs no
    -- COALESCE anyway: it selects outbox rows, and a NULL reason simply
    -- makes the predicate NULL-false — an ordinary row this arm must not
    -- touch, correctly left to the main window above.
    --
    -- ORDER BY (occurred_at, id) pins the scan to job_events_reclaim_age_idx,
    -- which is keyed on exactly those columns under this arm's verbatim
    -- partial predicate: the age bound becomes an Index Cond that stops at
    -- the boundary instead of a post-scan Filter over the whole unconsumed
    -- outbox population, which is what makes a drained tick's cost flat in
    -- that population rather than proportional to it. id and occurred_at
    -- are co-monotonic by the visibility-delay doctrine (constants.py
    -- RECLAIM_EVENT_VISIBILITY_DELAY), so this is the same oldest-first
    -- drain order the id-keyed scan gave. The sibling
    -- job_events_reclaim_idx stays keyed on id alone for the poll's
    -- `id > cursor` tail. The scan cost is bounded by the outbox
    -- population itself, and this arm is what keeps that population
    -- bounded — a self-draining set.
    SELECT id
    FROM "{schema}".job_events
    WHERE kind = 'state_change' AND (detail->>'reason') = 'lock_expired'
      AND occurred_at < statement_timestamp() - $1::interval * {outbox_multiplier}
    ORDER BY occurred_at, id
    LIMIT $2
),
to_delete AS (
    SELECT id FROM expired
    UNION ALL
    SELECT id FROM expired_outbox
)
DELETE FROM "{schema}".job_events e
USING to_delete
WHERE e.id = to_delete.id
RETURNING e.id"""

# Per-sweep batched attempt INSERT templates (schema baked in via .format
# at call time after _IDENT_RE validation).  Kept as constants so the SQL
# surface stays grep-able and free of f-string S608 noise.
#
# Sweep 1's template resolves worker ids through the holder-CTE idiom from
# _sql.py's INSERT_ATTEMPT_SQL (probe workers under FOR KEY SHARE → NULL
# when the row is gone): the reclaim's crash victim can have its workers
# row already deleted by an earlier cleanup_stale_workers tick, and the
# raw snap id would FK-violate here.  The holder CTE probes the batch's
# candidate ids with a single ``id = ANY($4)`` scan — NOT a join against
# ``unnest($4)``: the same worker holds every lock in a fleet-wide crash,
# so its id repeats across the batch, and a join against the repeated
# array element fans each unnested attempt row out into one row per
# occurrence (a PK-violating duplicate burst).  The ANY-scan matches each
# workers row at most once; the LEFT JOIN against it then records a
# present parent's id and a deleted one's NULL (mirroring the column's ON
# DELETE SET NULL).  Sweep 2's template stays plain: its worker_id is NULL
# by construction (the job was never dispatched, so there is no
# lock-holder to reference).
#
# finished_at carries the WITH ORDINALITY microsecond ladder for the same
# reason job_events.occurred_at does in INSERT_EVENTS_DETAIL_BATCH_SQL
# (see _sql.py's comment there): a bare volatile clock_timestamp()
# collapses tens of rows onto one microsecond inside a single statement,
# destroying the per-row distinctness the audit trail pins.  The jsonb
# metadata literal carries doubled braces because the template is
# rendered through str.format.
#
# error_message rides per-row ($6, mapped from the sweep's own
# reclaim_reason literals through _ATTEMPT_MESSAGES below) because the
# two arms fire on different deadlines and an attempt row asserting the
# OTHER arm's deadline is a lie an auditor reconciling job_attempts
# against jobs would trip over: the heartbeat arm selects rows precisely
# because lock_expires_at >= statement_timestamp(), so its attempt rows
# must name the heartbeat deadline, never a lock expiry — the same
# honesty standard the event's separate cause key already carries.  The
# lease arm's text is pinned verbatim by
# tests/test_sweep_expired_locks_bounded.py::test_job_attempts_row_shape.
_SWEEP_1_ATTEMPTS_BATCH_SQL = """\
WITH holder AS (
    SELECT id
    FROM "{schema}".workers
    WHERE id = ANY($4::uuid[])
    FOR KEY SHARE
)
INSERT INTO "{schema}".job_attempts
(job_id, attempt, started_at, finished_at, outcome,
 error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
SELECT a.job_id, a.attempt,
       -- NULL started_at (direct-SQL-reachable only; dispatch always stamps it) falls back to the per-row clock — the in-memory twin's COALESCE-to-now contract.
       COALESCE(a.started_at, clock_timestamp() + (a.ord - 1) * interval '1 microsecond'),
       clock_timestamp() + (a.ord - 1) * interval '1 microsecond',
       'crashed', 'WorkerCrashed',
       a.error_message, NULL,
       a.duration_ms, holder.id, '{{}}'::jsonb
FROM unnest($1::uuid[], $2::smallint[], $3::timestamptz[], $4::uuid[], $5::int[], $6::text[])
    WITH ORDINALITY AS a(job_id, attempt, started_at, worker_id, duration_ms, error_message, ord)
LEFT JOIN holder ON holder.id = a.worker_id
-- Same doctrine as sweep 2's deadline insert below: an attempt number
-- can already have its row (a claim-clamped repeat at the smallint
-- ceiling; a spent attempt left behind by a re-pend), and the truthful
-- first record yields to nothing — the synthetic crash row must skip
-- rather than roll back every sibling swept in the same statement.
ON CONFLICT (job_id, attempt) DO NOTHING"""

_SWEEP_2_ATTEMPTS_BATCH_SQL = """\
INSERT INTO "{schema}".job_attempts
(job_id, attempt, started_at, finished_at, outcome, error_class, error_message,
 error_traceback, duration_ms, worker_id, metadata)
SELECT a.job_id, a.attempt,
       COALESCE(a.started_at, clock_timestamp() + (a.ord - 1) * interval '1 microsecond'),
       clock_timestamp() + (a.ord - 1) * interval '1 microsecond',
       'failed', 'DeadlineExceeded', 'schedule_to_close reached before next dispatch',
       NULL, a.duration_ms, NULL, '{{}}'::jsonb
FROM unnest($1::uuid[], $2::smallint[], $3::timestamptz[], $4::int[])
    WITH ORDINALITY AS a(job_id, attempt, started_at, duration_ms, ord)
-- A pending/scheduled row can legitimately sit at an attempt number that
-- already ran: the transient-retry arm, a crash reclaim, and an operator
-- retry all leave the spent attempt's row behind and keep the counter
-- where it is. Dispatch excludes rows past schedule_to_close, so that
-- counter can never climb again and this sweep is the only thing that can
-- resolve the job. The existing row is the truthful record of what the
-- actor actually did; the deadline lapsing afterwards is not a second
-- execution, so the synthetic row yields to it rather than colliding and
-- rolling back every sibling swept in the same statement.
ON CONFLICT (job_id, attempt) DO NOTHING"""


class _ReclaimedRow(NamedTuple):
    """One row transitioned by sweep 1, carried out for post-commit logging."""

    job_id: JobId
    attempt: int
    new_status: str
    reclaim_reason: str
    """Which eligibility arm reclaimed the row ('lock_expired' or
    'heartbeat_timeout') — the deadline that fired, logged as the
    state-change cause."""


class _DeadlineRow(NamedTuple):
    """One row transitioned by sweep 2, carried out for post-commit logging."""

    job_id: JobId
    from_state: str
    actor: str


class _PromotedRow(NamedTuple):
    """One row transitioned by sweep 3, carried out for post-commit logging."""

    job_id: JobId
    from_state: str


class SweepBatchSizer:
    """Two-tier batch size for one sweep, latching to the reduced tier.

    ``effective_size()`` answers the default tier until the breaker
    latches, then ``max(1, default_size // divisor)`` for the rest of the
    object's lifetime.  ``on_timeout()`` counts a failure and latches once
    ``failure_threshold`` consecutive failures land inside a rolling
    ``window_secs``; ``on_success()`` resets the consecutive-failure count
    but never unlatches.

    Why the latch is one-way: a database that needed smaller bites once
    will need them again, and the control signal (a cancelled sweep
    batch) is contaminated by transient hiccups — a timeout caused by a
    brief lock pile-up or a checkpoint says nothing about whether the
    next full-size batch will fit.  Erring toward staying degraded
    trades a little throughput for stability: each oscillation back to
    the full tier is a trial that can roll back a transaction holding
    row locks, and flapping between tiers under sustained pressure would
    make batch duration bimodal and unpredictable.  The reduced tier is
    a ceiling, not a floor — the sweep still drains, in smaller
    committed batches.

    Deliberately not a dataclass: equality on a mutable breaker would
    compare two sizers with different latch states as equal, which is
    exactly the state that matters.

    ``now`` is injectable so tests can drive the rolling window
    deterministically; it defaults to ``time.monotonic`` because the
    window compares orderings on a single clock, never wall time.
    """

    def __init__(
        self,
        default_size: int,
        divisor: int,
        failure_threshold: int,
        window_secs: float,
        *,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        # Degenerate knobs are configuration bugs, not batch-time events:
        # default_size < 1 puts LIMIT 0 (a legal, rowless query) into every
        # unlatched call; divisor < 2 never reduces — 1 makes the latched
        # tier equal the normal tier, so the breaker engages silently
        # without ever degrading, and 0 or below divides by zero or
        # inflates; failure_threshold < 1 latches on accounting alone;
        # window_secs <= 0 expires every failure instantly, making the
        # latch unreachable.  All four fail here, at the constructor
        # boundary, instead of inside a sweep loop.
        if default_size < 1:
            raise ValueError(f"default_size must be >= 1, got {default_size}")
        if divisor < 2:
            raise ValueError(f"divisor must be >= 2, got {divisor}")
        if failure_threshold < 1:
            raise ValueError(f"failure_threshold must be >= 1, got {failure_threshold}")
        if window_secs <= 0:
            raise ValueError(f"window_secs must be > 0, got {window_secs}")
        self.default_size = default_size
        self.divisor = divisor
        self.failure_threshold = failure_threshold
        self.window_secs = window_secs
        self.now = now
        self._latched: bool = False
        self._recent_failures: list[float] = []

    def effective_size(self) -> int:
        """The batch size a sweep call uses right now."""
        if self._latched:
            return max(1, self.default_size // self.divisor)
        return self.default_size

    def on_success(self) -> None:
        """Reset the consecutive-failure count; never unlatch."""
        self._recent_failures.clear()

    def on_timeout(self) -> None:
        """Count one aborted batch, latching at the failure threshold."""
        now = self.now()
        self._recent_failures = [
            stamp for stamp in self._recent_failures if now - stamp <= self.window_secs
        ]
        self._recent_failures.append(now)
        if len(self._recent_failures) >= self.failure_threshold:
            self._latched = True


def _validate_positive(name: str, value: int) -> None:
    """Reject a non-positive sweep bound at the typed boundary, pre-SQL.

    ``LIMIT 0`` is a legal rowless query — a silent drain stall that
    looks like "nothing to do" to every caller and metric — and a
    negative LIMIT *parameter* is a server-side data error (SQLSTATE
    2201W), which the leader's transient-error classification
    deliberately excludes, so it burns the unexpected-error budget
    instead of being caught where it belongs.  ``statement_timeout = 0``
    disables the batch's safety net outright.  All three are caller
    configuration bugs and get a loud ValueError before any SQL runs.
    """
    if value < 1:
        raise ValueError(f"{name} must be >= 1, got {value}")


async def _apply_batch_statement_timeout(conn: ConnLike, timeout_ms: int) -> str:
    """Bind the batch's ``statement_timeout``; return the value to restore.

    ``set_config(..., true)`` is ``SET LOCAL`` semantics with a bindable
    value (``SET`` itself cannot take parameters).  The previous value is
    captured first because ``SET LOCAL``'s scope is the *transaction*,
    not this function's savepoint: when a caller already has a
    transaction open, asyncpg nests the sweep's block as a savepoint and
    a RELEASE keeps the setting — without the restore the sweep's bound
    would apply to the caller's subsequent statements in the same
    transaction (pinned by ``tests/test_rt_sweeps_timeout_leak.py``).
    On the sweep's error path no restore is needed: the savepoint
    ROLLBACK restores the GUC via PostgreSQL's subtransaction stack.
    """
    # fetch (not fetchval): fetch/execute/transaction is the complete
    # duck-typing surface every ConnLike wrapper in the suite proxies —
    # fetchval is not part of it.
    prev_rows = await conn.fetch("SELECT current_setting('statement_timeout')")
    if not prev_rows:
        # statement_timeout is a registered GUC with a value in every
        # session; no row here means the server answered something the
        # sweep cannot restore, so failing loudly beats guessing.
        raise RuntimeError("current_setting('statement_timeout') returned no value")
    prev: str = prev_rows[0]["current_setting"]
    await conn.execute(
        "SELECT set_config('statement_timeout', $1, true)",
        str(timeout_ms),
    )
    return prev


async def _restore_statement_timeout(conn: ConnLike, prev: str) -> None:
    """Restore the ``statement_timeout`` captured by
    :func:`_apply_batch_statement_timeout`.

    Runs only on the batch's success path, inside the still-open
    transaction: on the error path the savepoint rollback has already
    restored it, and a restore attempted on an aborted transaction would
    fail and mask the original error.
    """
    await conn.execute(
        "SELECT set_config('statement_timeout', $1, true)",
        prev,
    )


async def sweep_expired_locks(
    conn: ConnLike,
    cancel_grace: timedelta,
    cleanup_grace: timedelta,
    *,
    schema: str,
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
    statement_timeout_ms: int = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    max_retry_backoff: timedelta = DEFAULT_MAX_RETRY_BACKOFF,
) -> int:
    """Sweep 1: reclaim running jobs whose holder broke its liveness
    promise, one bounded batch per eligibility arm per call.

    Two disjoint arms (see ``_SWEEP_1_SQL``'s comment block): a row whose
    ``lock_expires_at`` has passed (the per-worker global lease), and a
    row whose holder has been silent past its per-job
    ``heartbeat_timeout`` while the lease is still valid (the per-job
    promise — the shorter of the two deadlines governs). One call
    transitions at most ``2 x batch_size`` rows (each arm's snap carries
    its own LIMIT) in one short transaction (server-side
    ``statement_timeout`` included); repeated calls drain the eligible
    backlog a batch at a time.  For each reclaimed job:

    - If a cancel request was in-flight (``cancel_phase != 0``):
      transition to ``'cancelled`` — the caller's explicit request is
      the honest terminal label, whatever the retry budget; the row
      keeps its cancel columns as the audit trail of the honoured
      request.
    - Else, if attempts remain and retry is allowed: transition to
      ``'pending'`` with ``scheduled_at = clock_timestamp() + <delay>``,
      the delay derived from the row's own stamped retry curve (see
      ``_RECLAIM_DELAY_SQL``) and clamped at the lesser of the row's cap
      and *max_retry_backoff* — the operator's global ceiling, the same
      effective cap the failure path's ``compute_backoff`` applies.
    - Otherwise: transition to ``'crashed'``.

    Both terminal branches set ``finished_at = clock_timestamp()``; the
    re-pend branch leaves it NULL. The cancel columns are reset only on
    the branches whose rows read ``cancel_phase = 0`` by construction
    (re-pend and crashed) — a no-op kept as defence-in-depth — and are
    preserved on the cancel branch; see the ``_SWEEP_1_SQL`` comment for
    the operator-intent-first ordering and why it cannot resurrect the
    re-cancel loop the old reset-on-re-pend spelling prevented.

    All branches write a ``job_attempts`` row (outcome ``'crashed'``,
    error_class ``'WorkerCrashed'`` — that IS what happened to the
    attempt, regardless of the job's terminal label — with an
    ``error_message`` naming the deadline that fired: the lease arm's
    rows say the lock expired, the heartbeat arm's say the heartbeat
    timeout passed, so an auditor reconciling ``job_attempts`` against
    ``jobs`` never reads a heartbeat reclaim asserting a lock expiry the
    sweep's own selection disproved) and a
    ``job_events`` row (kind ``'state_change'``, reason
    ``'lock_expired'`` — the crash-reclaim outbox channel BOTH arms
    ride, so ``poll_reclaim_events`` consumers and the retention
    carve-out see heartbeat reclaims exactly like lease reclaims — with
    a ``cause`` key naming which deadline fired); both writes are
    batched into one statement each
    over the batch's rows. A running job with NULL ``started_at``
    (reachable only via direct SQL — dispatch always stamps it) lands
    the per-row clock fallback for the attempt's ``started_at``,
    matching the in-memory twin's COALESCE-to-now contract (pinned by
    ``tests/test_rt_sweeps_started_at_fallback.py``); without it the
    batched INSERT would violate ``job_attempts.started_at NOT NULL``
    and abort the sweep's transaction on a non-transient error.

    PG uses server-side ``statement_timestamp()`` for the WHERE range
    bound (STABLE, so the partial index serves it as an Index Cond —
    see the module docstring) and ``clock_timestamp()`` for finished-at
    timestamps (not ``now()``, which is transaction-start time — see
    the module docstring's note on why a long-held sweep transaction
    must not mix the two for timestamps that need to agree with each
    other or with ``job_events.occurred_at``); this function takes no
    ``now`` argument.

    A CTE snapshots ``locked_by_worker`` before the UPDATE clears it, so
    the ``job_attempts.worker_id`` is populated correctly. The snapshot
    keeps the raw last-known holder id; the attempt INSERT resolves it
    through a ``FOR KEY SHARE`` holder CTE (see ``_sql.py``'s
    INSERT_ATTEMPT_SQL), so when the crashed worker's row was already
    removed by an earlier ``cleanup_stale_workers`` tick (possible
    whenever the stale-worker window, ``heartbeat_interval *
    (max_heartbeat_failures + 3)``, is shorter than the lease) the attempt
    records a ``NULL`` worker_id — mirroring the column's ``ON DELETE SET
    NULL`` semantics — instead of FK-violating on the dangling id, while
    the job_events detail still carries the last-known holder for audit.

    One ``pg_notify`` is fired per sweep call that reclaims at least one
    row (not one per row) so that fleet-wide consumers using
    ``watch_reclaims`` get a low-latency wakeup on both branches.

    .. note:: This is a **channel-semantics change**, not purely a
       bugfix: ``wake_channel`` previously meant "new dispatchable work"
       (enqueue, scheduled-to-pending promotion); it now *also* means
       "something changed on job_events."  Every crash-reclaim therefore
       wakes every subscriber — including pure-dispatch workers with no
       interest in reclaim events.  Crashes are rare so the cost is low,
       but the wake channel is no longer exclusively a dispatch signal.

    Returns the count of rows reclaimed by this call.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_positive("batch_size", batch_size)
    _validate_positive("statement_timeout_ms", statement_timeout_ms)

    sql = _SWEEP_1_SQL.format(schema=schema)
    attempt_sql = _SWEEP_1_ATTEMPTS_BATCH_SQL.format(schema=schema)
    event_sql = INSERT_EVENTS_DETAIL_BATCH_SQL.format(schema=schema)

    reclaimed: list[_ReclaimedRow] = []

    async with conn.transaction():
        prev_timeout = await _apply_batch_statement_timeout(conn, statement_timeout_ms)
        rows = await conn.fetch(
            sql, cancel_grace, cleanup_grace, batch_size, max_retry_backoff.total_seconds()
        )

        if rows:
            job_ids: list[JobId] = []
            attempts: list[int] = []
            started_ats: list[datetime | None] = []
            worker_ids: list[UUID | None] = []
            duration_mss: list[int | None] = []
            error_messages: list[str] = []
            details: list[str | None] = []

            for rec in rows:
                job_id: JobId = JobId(rec["id"])
                new_status: str = rec["status"]
                attempt: int = rec["attempt"]
                started_at: datetime | None = rec["started_at"]
                original_worker: UUID | None = rec["locked_by_worker"]
                reclaim_reason: str = rec["reclaim_reason"]

                # started_at is database-written and the attempt row's
                # finished_at is stamped clock_timestamp(); the elapsed span
                # between them must be measured in that same domain, so "now"
                # comes back on the sweep's own RETURNING rather than from this
                # process's clock, which would skew (or negate) the stored
                # duration_ms.
                duration_ms = compute_duration_ms(started_at, rec["now_ts"])

                # Both arms ride the crash-reclaim outbox channel
                # (reason='lock_expired' — the slice poll_reclaim_events
                # tails and the retention carve-out keeps); cause names
                # which deadline fired. See _SWEEP_1_SQL's comment block.
                detail: dict[str, object] = {
                    "from_state": "running",
                    "to_state": new_status,
                    "reason": "lock_expired",
                    "cause": reclaim_reason,
                }
                if original_worker is not None:
                    detail["worker_id"] = str(original_worker)

                job_ids.append(job_id)
                attempts.append(attempt)
                started_ats.append(started_at)
                worker_ids.append(original_worker)
                duration_mss.append(duration_ms)
                # The attempt row names the deadline that fired, not the
                # sibling arm's — see _SWEEP_1_ATTEMPTS_BATCH_SQL's comment.
                error_messages.append(_ATTEMPT_MESSAGES[reclaim_reason])
                details.append(jsonb_param(detail))
                reclaimed.append(_ReclaimedRow(job_id, attempt, new_status, reclaim_reason))

            await conn.execute(
                attempt_sql,
                job_ids,
                attempts,
                started_ats,
                worker_ids,
                duration_mss,
                error_messages,
            )
            await conn.execute(event_sql, job_ids, details, "state_change")
            await conn.execute(
                WAKE_NOTIFY_SQL,
                wake_channel(schema),
            )
        # Success path only: restore the caller's timeout inside the
        # still-open transaction (a savepoint RELEASE would otherwise
        # keep the sweep's bound); on error the savepoint rollback has
        # already restored it.
        await _restore_statement_timeout(conn, prev_timeout)

    for row in reclaimed:
        log_state_change(
            logger,
            from_state="running",
            to_state=row.new_status,
            job_id=str(row.job_id),
            attempt=row.attempt,
            reason="lock_expired",
            cause=row.reclaim_reason,
        )
    if reclaimed:
        logger.error(
            "recovery_reclaim",
            kind="recovery_reclaim",
            count=len(reclaimed),
            schema=schema,
        )

    return len(rows)


async def sweep_deadline_exceeded(
    conn: ConnLike,
    *,
    schema: str,
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
    statement_timeout_ms: int = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
) -> int:
    """Sweep 2: fail overdue ``schedule_to_close`` jobs, one bounded batch per call.

    One call transitions at most ``batch_size`` rows in one short
    transaction (server-side ``statement_timeout`` included); repeated
    calls drain the eligible backlog a batch at a time.

    Transitions to ``'failed'`` with ``error_class = 'DeadlineExceeded'``.
    Writes one ``job_attempts`` row and one ``job_events`` row per swept
    job — batched into one statement each — in the same transaction as
    the parent UPDATE.

    ``started_at`` for never-dispatched jobs is NULL; the attempt INSERT
    uses ``COALESCE(started_at, clock_timestamp())`` to satisfy the
    ``job_attempts.started_at NOT NULL`` constraint.

    PG uses server-side ``statement_timestamp()`` for the deadline range
    bound (STABLE, so the partial index serves it as an Index Cond —
    see the module docstring) and ``clock_timestamp()`` for the
    finished-at timestamp (not ``now()``, which is fixed at transaction
    start — see the module docstring); this function takes no ``now``
    argument.

    Returns the count of rows swept by this call.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_positive("batch_size", batch_size)
    _validate_positive("statement_timeout_ms", statement_timeout_ms)

    sql = _SWEEP_2_SQL.format(schema=schema)
    attempt_sql = _SWEEP_2_ATTEMPTS_BATCH_SQL.format(schema=schema)
    event_sql = INSERT_EVENTS_DETAIL_BATCH_SQL.format(schema=schema)

    swept: list[_DeadlineRow] = []
    actor_counts: Counter[str] = Counter()

    async with conn.transaction():
        prev_timeout = await _apply_batch_statement_timeout(conn, statement_timeout_ms)
        rows = await conn.fetch(sql, batch_size)

        if rows:
            job_ids: list[JobId] = []
            attempts: list[int] = []
            started_ats: list[datetime | None] = []
            duration_mss: list[int | None] = []
            details: list[str | None] = []

            for rec in rows:
                job_id: JobId = JobId(rec["id"])
                prev_status: str = rec["prev_status"]
                attempt: int = rec["attempt"]
                started_at: datetime | None = rec["started_at"]
                actor: str = rec["actor"]

                # started_at is database-written and the attempt row's
                # finished_at is stamped clock_timestamp(); the elapsed span
                # between them must be measured in that same domain, so "now"
                # comes back on the sweep's own RETURNING rather than from this
                # process's clock, which would skew (or negate) the stored
                # duration_ms.
                duration_ms = compute_duration_ms(started_at, rec["now_ts"])

                detail: dict[str, object] = {
                    "from_state": prev_status,
                    "to_state": "failed",
                    "error_class": "DeadlineExceeded",
                }

                job_ids.append(job_id)
                attempts.append(attempt)
                started_ats.append(started_at)
                duration_mss.append(duration_ms)
                details.append(jsonb_param(detail))
                swept.append(_DeadlineRow(job_id, prev_status, actor))
                actor_counts[actor] += 1

            await conn.execute(attempt_sql, job_ids, attempts, started_ats, duration_mss)
            await conn.execute(event_sql, job_ids, details, "state_change")
        # Success path only: restore the caller's timeout inside the
        # still-open transaction (a savepoint RELEASE would otherwise
        # keep the sweep's bound); on error the savepoint rollback has
        # already restored it.
        await _restore_statement_timeout(conn, prev_timeout)

    for row in swept:
        log_state_change(
            logger,
            from_state=row.from_state,
            to_state="failed",
            job_id=str(row.job_id),
            error_class="DeadlineExceeded",
        )
    # Aggregated per actor AFTER the transaction: the metric emission is
    # per-row work, and per-row work on the DB-hold path is what the
    # batching exists to remove.
    for actor, count in actor_counts.items():
        record_deadline_exceeded_swept(actor=actor, count=count)
    if swept:
        logger.debug(
            "sweep_deadline_exceeded",
            kind="sweep_deadline_exceeded",
            count=len(swept),
            schema=schema,
        )

    return len(rows)


async def sweep_scheduled_to_pending(
    conn: ConnLike,
    *,
    schema: str,
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
    statement_timeout_ms: int = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
) -> int:
    """Sweep 3: promote due scheduled jobs, one bounded batch per call.

    One call transitions at most ``batch_size`` rows with
    ``status='scheduled'`` and ``scheduled_at <= clock_timestamp()`` to
    ``status='pending'``, in one short transaction (server-side
    ``statement_timeout`` included); repeated calls drain the eligible
    backlog a batch at a time.

    A promotion writes NO ``job_events`` row. scheduled→pending is the
    scheduler's bookkeeping, not an outcome transition, and it is one of
    the two acts every admission-denial cycle repeats (claim + promote):
    under the 429 denial contract a denied job cycles until capacity
    frees or its deadline expires, so a row per promotion is precisely
    the unbounded-growth vector the aggregated denial counters on the job
    row replaced. The transitions of record are the terminal writes and
    the sweep/cancel audit entries; the promotion itself writes no row,
    so the table cannot grow per promotion. The sweep's own observability is the
    per-call count log below and the per-row ``state_change`` log lines
    (logs, not durable rows).

    PG uses server-side ``statement_timestamp()`` for the due range bound
    (STABLE, so ``jobs_scheduled_wake_idx`` serves it as an Index Cond —
    see the module docstring; this snap runs every second on the leader)
    and ``clock_timestamp()`` for written timestamps (not ``now()``,
    which is fixed at transaction start — see the module docstring);
    this function takes no ``now`` argument.

    Returns the count of rows promoted by this call.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_positive("batch_size", batch_size)
    _validate_positive("statement_timeout_ms", statement_timeout_ms)

    sql = _SWEEP_3_SQL.format(schema=schema)

    promoted: list[_PromotedRow] = []

    async with conn.transaction():
        prev_timeout = await _apply_batch_statement_timeout(conn, statement_timeout_ms)
        rows = await conn.fetch(sql, batch_size)

        if rows:
            for rec in rows:
                promoted.append(_PromotedRow(JobId(rec["id"]), rec["prev_status"]))

            await conn.execute(
                WAKE_NOTIFY_SQL,
                wake_channel(schema),
            )
        # Success path only: restore the caller's timeout inside the
        # still-open transaction (a savepoint RELEASE would otherwise
        # keep the sweep's bound); on error the savepoint rollback has
        # already restored it.
        await _restore_statement_timeout(conn, prev_timeout)

    for row in promoted:
        log_state_change(
            logger,
            from_state=row.from_state,
            to_state="pending",
            job_id=str(row.job_id),
        )
    if promoted:
        logger.debug(
            "sweep_scheduled_to_pending",
            kind="sweep_scheduled_to_pending",
            count=len(promoted),
            schema=schema,
        )

    return len(rows)


async def sweep_leaked_reservation_slots(
    conn: ConnLike,
    *,
    schema: str,
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
) -> int:
    """Sweep 4: release reservation slots whose lease has expired, one
    bounded batch per call.

    One call clears ``job_id``, ``held_by_worker_id``, ``acquired_at``,
    and ``lease_expires_at`` on at most ``batch_size`` slots in one short
    statement; repeated calls drain the eligible backlog a committed
    batch at a time. No ``job_attempts`` or ``job_events`` writes —
    reservation slots are not job-state transitions, so the
    trailing-watermark visibility margin does not bind the batch size;
    the bound exists because the write set scales with the expired-lease
    population, and one constant-size statement per call keeps each
    tick's cost independent of that backlog.

    PG uses server-side ``statement_timestamp()`` for the lease range
    bound (STABLE, so ``reservation_slots_lease_expires_idx`` serves it
    as an Index Cond — see the module docstring's two-clock doctrine; a
    ``clock_timestamp()`` bound would degrade the window to a post-scan
    Filter over the whole held-slot population); this function takes no
    ``now`` argument.

    Returns the count of released slots.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_positive("batch_size", batch_size)

    sql = _SWEEP_4_SQL.format(schema=schema)
    tag = await conn.execute(sql, batch_size)
    count = parse_rowcount(tag)
    if count > 0:
        logger.debug(
            "sweep_leaked_reservation_slots",
            kind="sweep_leaked_reservation_slots",
            count=count,
            schema=schema,
        )
    return count


async def sweep_expired_results(
    conn: ConnLike,
    *,
    schema: str,
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
) -> int:
    """Expire result rows whose ``result_expires_at`` has passed, one
    bounded batch per call.

    One call nulls ``result``, ``result_size_bytes`` and
    ``result_expires_at`` on at most ``batch_size`` rows in one short
    transaction; repeated calls drain the eligible backlog a batch at a
    time. The statement writes no ``job_events`` rows, so the
    trailing-watermark visibility margin does not bind it — the bound
    exists because the write set scales with the stored result bytes: a
    backlog of expired results (every result in the fleet expiring at
    once after a retention change) is a single transaction whose
    duration, lock-hold, and WAL volume grow with that backlog, and the
    window caps each of them per call.

    PG uses server-side ``statement_timestamp()`` for the comparison
    (STABLE, so ``jobs_result_expires_at_idx`` serves it as an Index
    Cond — see the module docstring); this function takes no ``now``
    argument.

    Returns the count of results expired by this call.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_positive("batch_size", batch_size)

    sql = _SWEEP_RESULT_TTL_SQL.format(schema=schema)
    tag = await conn.execute(sql, batch_size)
    count = parse_rowcount(tag)
    if count > 0:
        logger.debug(
            "sweep_expired_results",
            kind="sweep_expired_results",
            count=count,
            schema=schema,
        )
    return count


async def sweep_expired_events(
    conn: ConnLike,
    *,
    schema: str,
    retention: timedelta,
    batch_size: int = DEFAULT_EVENT_RETENTION_BATCH_SIZE,
) -> int:
    """Delete ``job_events`` rows older than *retention*, one bounded batch
    per call, regardless of parent-job status.

    One call deletes at most ``batch_size`` rows in one short statement;
    repeated calls drain the eligible backlog a committed batch at a time.
    Events are narration: the durable forensic record for a job is
    jobs/jobs_archive plus job_attempts/job_attempts_archive, kept for the
    full prune and archive windows — so this sweep bounds event volume
    independently of job retention, and reaches the rows the
    terminality-keyed prune never can (a job that never reaches a terminal
    status holds its events forever under the cascade-only regime).

    The crash-reclaim outbox slice (``kind='state_change'`` and
    ``detail->>'reason'='lock_expired'``) is kept past the ordinary
    retention age — deleting an unconsumed outbox row while a lagging
    consumer's watermark can still reach it silently loses a crashed
    worker's reclaim — but NOT at every age: the same statement's outbox
    arm deletes the slice at
    ``RECLAIM_OUTBOX_RETENTION_MULTIPLIER`` times *retention*, so a fleet
    with no ``watch_reclaims`` consumer cannot retain the slice forever
    and a row committed below a watermark cursor that already passed it
    (unreachable to the poll by construction) is still bounded. See
    ``_SWEEP_EVENT_TTL_SQL``'s comment and
    ``taskq.constants.RECLAIM_OUTBOX_RETENTION_MULTIPLIER`` for the
    derivations.

    *retention* must be positive: ``timedelta(0)`` is the SETTING's
    disable sentinel (``WorkerSettings.event_retention_period``), never a
    sweep argument — at the function boundary zero would read as "delete
    every event older than now", the dangerous misreading, so it is
    rejected here as a caller wiring bug.

    PG uses server-side ``statement_timestamp()`` for the age bound
    (STABLE, so the retention partial index serves it as an Index Cond —
    see the module docstring); this function takes no ``now`` argument.

    Returns the count of events deleted by this call (both arms).
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_positive("batch_size", batch_size)
    if retention <= timedelta(0):
        raise ValueError(
            f"retention must be positive, got {retention!r}; timedelta(0) is the "
            "settings-level disable sentinel, not a sweep argument"
        )

    sql = _SWEEP_EVENT_TTL_SQL.format(
        schema=schema, outbox_multiplier=RECLAIM_OUTBOX_RETENTION_MULTIPLIER
    )
    tag = await conn.execute(sql, retention, batch_size)
    count = parse_rowcount(tag)
    if count > 0:
        logger.debug(
            "sweep_expired_events",
            kind="sweep_expired_events",
            count=count,
            schema=schema,
        )
    return count


_SWEEP_IDLE_KEYED_BUCKETS_BODY = """\
-- Bounded batch + MATERIALIZED, same rationale as the sibling sweeps
-- above: LIMIT $2 caps one call's DELETE at $2 rows (one row per bucket
-- for this table); MATERIALIZED stops the planner from inlining the
-- LIMIT-ed CTE into the DELETE in a way that could remove more rows
-- than the LIMIT. ORDER BY age, LIMIT per statement, loop to drain —
-- each tick deletes the oldest eligible rows and stops at the LIMIT in
-- the backlog case and at the age boundary in the empty case. ORDER BY
-- (last_used_at, bucket_name) pins the window to the keyed partial
-- index keyed on exactly (last_used_at) (the ORDER-BY-pins-the-scan
-- rule above), so the drain is deterministic (oldest-first, stable
-- under tied stamps) and the ordered scan stops at the LIMIT in the
-- backlog case and at the age boundary in the empty case; no keyset
-- cursor because every windowed row is deleted by this same statement,
-- so the eligible set shrinks monotonically per committed batch.
--
-- statement_timestamp() (STABLE) instead of clock_timestamp() (VOLATILE)
-- is what lets the planner use rate_limit_buckets_keyed_last_used_idx
-- as a range bound rather than a post-scan filter — same derivation as
-- _SWEEP_1_SQL and the module docstring's two-clock doctrine (the WRITE
-- stamps that refresh last_used_at are clock_timestamp() on the acquire
-- path; only this selection bound is STABLE).
--
-- The keyed flag is the whole eligibility story for this table: static
-- buckets are born false and never flip (nothing in the package passes
-- a keyed mark for them), and redis-backend keyed buckets are
-- deliberately published unmarked — their healthy acquire path never
-- touches PG, so this row's stamp cannot speak for Redis-side use. The
-- outer re-check of the eligibility predicate keeps a concurrent
-- duplicate sweep (or an acquire flipping the mark between window and
-- DELETE) a no-op rather than a count-inflating rewrite — same shape
-- as _SWEEP_EVENT_TTL_SQL.
-- The consumed-fixed-quota veto is this table's analogue of the
-- reservation_slots arm's whole-bucket vetoes: idleness alone is not
-- evidence that a row is safe to delete. A fixed quota (refill 0) is
-- drained forever by design, so a row that has spent part of one carries
-- live state no elapsed time recovers. Deleting it lets the next
-- acquire re-preseed at full capacity, over-admitting against a budget
-- that was already spent, and a fixed-quota bucket has no lease or hold
-- concept for any other guard to catch this. The bucket's own writes
-- carry capacity and refill into state for exactly this decision.
--
-- The veto is fail-CLOSED on missing keys. A row is deleted only when
-- the sweep can PROVE the delete is safe, and the proofs are exactly:
--   (state->>'tokens') IS NULL — the row carries no token count at all
--     (the keyed publish path writes an empty state document until the
--     first acquire), so deleting it loses nothing;
--   refill provably nonzero — a refilling bucket's state converges back
--     to full on its own, so eviction forfeits at most a refill window;
--   refill provably zero AND tokens provably at capacity — a fixed quota
--     nothing was ever spent from, so the re-preseed reproduces the
--     stored state instead of resetting it.
-- Every other shape is kept: a row written before the capacity/refill
-- keys existed carries a token count with nothing to evaluate it against
-- (a partly spent fixed quota and a refilling bucket look identical), and
-- a row the sweep cannot prove safe to delete is one it keeps. NULL
-- three-valued logic does the keeping — every probe on a missing key
-- reads NULL, and NULL satisfies no proof. The kept legacy population is
-- bounded: the first acquire after the keys existed rewrites the row
-- with the full document, so only rows nothing has touched since stay
-- unprovable. The predicate itself is shared verbatim with the
-- per-worker eviction drain via _no_consumed_quota_sql, so the two
-- delete paths cannot disagree about which rows are safe.
WITH expired AS MATERIALIZED (
    SELECT bucket_name
    FROM "{schema}".rate_limit_buckets
    WHERE keyed
      AND last_used_at < statement_timestamp() - $1::interval
      AND {no_consumed_quota_unqualified}
    ORDER BY last_used_at, bucket_name
    LIMIT $2
)
DELETE FROM "{schema}".rate_limit_buckets b
USING expired
WHERE b.bucket_name = expired.bucket_name
  AND b.keyed
  AND b.last_used_at < statement_timestamp() - $1::interval
  AND {no_consumed_quota_b}
RETURNING b.bucket_name"""


def _no_consumed_quota_sql(alias: str = "") -> str:
    """Fail-closed proof that a ``rate_limit_buckets`` row is safe to delete.

    The one place this question is asked, shared by the fleet sweep and
    the per-worker eviction drain so the two cannot disagree about which
    rows are safe to delete.  *alias* qualifies the ``state`` column for
    a statement that has one.

    Fail-CLOSED on missing keys (see the sweep body's comment for the
    full proof table): a delete fires only when the row provably carries
    no token count, or provably refills, or provably holds a fixed quota
    nothing was ever spent from.  Every probe on a missing key reads
    NULL, NULL satisfies no proof, and a row the predicate cannot prove
    safe is kept.
    """
    state = f"{alias}.state" if alias else "state"
    return (
        f"(({state}->>'tokens') IS NULL "
        f"OR ({state}->>'refill')::float8 <> 0 "
        f"OR (({state}->>'refill')::float8 = 0 "
        f"AND ({state}->>'tokens')::float8 >= ({state}->>'capacity')::float8))"
    )


#: The bucket sweep with its quota predicate bound, still carrying
#: ``{schema}`` for the caller — one placeholder, as before.
_SWEEP_IDLE_KEYED_BUCKETS_SQL = _SWEEP_IDLE_KEYED_BUCKETS_BODY.replace(
    "{no_consumed_quota_unqualified}", _no_consumed_quota_sql()
).replace("{no_consumed_quota_b}", _no_consumed_quota_sql("b"))


_SWEEP_IDLE_KEYED_SLOTS_SQL = """\
-- Bounded batch + MATERIALIZED, same rationale as the sibling sweeps
-- above: the window CTE's LIMIT $2 caps one call at $2 BUCKETS (each
-- bucket's full slot-row set deletes together — see the reclaimable
-- CTE), so one tick's write set is bounded by the batch x the
-- configured per-bucket slot count, a constant against any dead-worker
-- backlog. MATERIALIZED stops the planner from inlining either LIMIT-ed
-- CTE into the DELETE. ORDER BY min(last_used_at) makes the drain
-- oldest-bucket-first and deterministic (bucket_name tiebreak).
--
-- The freshness veto is an anti-join INSIDE the window, not only a
-- verdict after it, because a bucket configured for more concurrency
-- than it ever uses keeps permanently idle high-index slot rows. Such a
-- bucket sorts to the front on its per-row minimum however live it
-- actually is, consumes one of the LIMIT's slots, and is then correctly
-- spared by the whole-bucket verdict below. With enough of them a
-- genuine dead-worker orphan never enters the window at all and the tick
-- nets zero deletions, tick after tick — their minimum never advances,
-- because the rows supplying it are never touched — so unreclaimed rows
-- accumulate without bound. Excluding a bucket with any fresh row up
-- front makes the window's membership agree with the verdict: a bucket
-- that cannot be reclaimed does not occupy a slot in the batch. The
-- anti-join keeps the outer scan's index range bound intact (the
-- horizon is still a per-row predicate on the keyed partial index), so
-- the probe costs one bucket_name-keyed lookup per candidate rather
-- than a scan of the table.
--
-- Three CTEs, one whole-bucket contract: lock, decide, delete.
-- `stale` names candidate buckets off the keyed partial index (keyed
-- rows past the horizon, with no fresh sibling). `locked` then
-- takes FOR UPDATE over
-- EVERY row of each candidate bucket — a locking read that re-fetches
-- each row at its LATEST committed version under READ COMMITTED
-- isolation, blocking concurrent writers and ensuring the decision sees
-- the post-write state before deleting. `reclaimable` re-verifies the
-- whole-bucket vetoes over `locked`'s output — the latest versions, not
-- the statement snapshot — and the DELETE removes exactly the rows both
-- later CTEs approved, by ctid.
-- A bucket must be reclaimed WHOLE or not at all: a partial delete
-- would silently shrink the bucket's configured capacity, and the
-- acquire-path heal only fires at ZERO rows (a partially-deleted bucket
-- denies with a smaller slot count forever, no code path ever names it
-- again). Three whole-bucket vetoes, all evaluated over the full row
-- set:
--   max(last_used_at) past the horizon — a fresh sibling (a recent
--     acquire or release touched ONE slot row; the bucket is
--     mid-workflow);
--   bool_and(keyed) — a keyed=false sibling means a static declaration
--     (or a former keyed life re-ensured by a static bootstrap) owns
--     part of the name's rows; fail safe, never sweep;
--   bool_and(job_id IS NULL OR lease_expires_at < now) — a live-held
--     slot vetoes the whole bucket, holder row and free siblings
--     alike, for as long as its lease stands (the lease-heartbeat
--     doctrine: expiry is the abandonment signal — the protection an
--     old-generation acquire's unstamped row relies on under
--     rolling-deploy skew, where the lease and not the stamp is the
--     only liveness signal).
--
-- Why the lock round trip instead of a plain guarded DELETE: EvalPlanQual
-- re-checks a DELETE's per-row guard against only the row it re-locks.
-- A write that lands between the window and the DELETE (an acquire
-- stamping the row it takes, a release stamping the row it frees)
-- refreshes ONE row of the bucket; the per-row guard on that row
-- spares it while its untouched stale free siblings still pass, and the
-- bucket leaves the statement partially deleted — a live bucket
-- permanently running at reduced capacity, since a bucket in continuous
-- use never re-enters eligibility and the heal never fires above zero
-- rows. Deciding over the locked latest versions makes the verdict
-- whole-bucket before any row is removed, and deleting by the locked
-- rows' ctids makes the write set exactly the rows the verdict approved
-- (rows a concurrent writer moved are keyed to the moved-to ctid the
-- statement snapshot cannot see, so they fail the join and stay — their
-- bucket was vetoed anyway, because every writer on these rows either
-- stamps last_used_at fresh or holds a live lease).
WITH stale AS MATERIALIZED (
    -- The window must admit only buckets that can actually pass the
    -- whole-bucket verdict below. A per-row minimum alone admits a live
    -- bucket whose idle high-index slots are old while its working rows
    -- are fresh; every such bucket is correctly vetoed, but it occupies a
    -- window slot and its minimum never advances, so the same live
    -- buckets refill the window tick after tick and a genuine dead-worker
    -- orphan behind them is never reached — the sweep deletes nothing
    -- while an orphan older than the horizon exists. The anti-join drops
    -- those buckets before the LIMIT, so a bounded batch always carries
    -- real candidates and the liveness vetoes below stay a concurrency
    -- re-check rather than the primary filter.
    --
    -- Why an anti-join and not HAVING max(last_used_at): a HAVING over the
    -- grouped set has to aggregate EVERY keyed row before it can filter,
    -- so the window's cost becomes the fleet's whole live key cardinality
    -- on every tick. The ordered min() scan still seeks
    -- reservation_slots_keyed_last_used_idx and stops at the age
    -- boundary, and the NOT EXISTS probe is an index lookup per candidate
    -- group — bounded by the window, not by the table.
    SELECT bucket_name
    FROM "{schema}".reservation_slots s
    WHERE keyed
      AND last_used_at < statement_timestamp() - $1::interval
      AND NOT EXISTS (
        SELECT 1
        FROM "{schema}".reservation_slots f
        WHERE f.bucket_name = s.bucket_name
          AND f.last_used_at >= statement_timestamp() - $1::interval
      )
    GROUP BY bucket_name
    ORDER BY min(last_used_at), bucket_name
    LIMIT $2
),
locked AS MATERIALIZED (
    SELECT s.ctid, s.bucket_name, s.job_id, s.lease_expires_at, s.keyed, s.last_used_at
    FROM "{schema}".reservation_slots s
    JOIN stale k ON s.bucket_name = k.bucket_name
    FOR UPDATE
),
reclaimable AS MATERIALIZED (
    SELECT l.bucket_name
    FROM locked l
    GROUP BY l.bucket_name
    HAVING max(l.last_used_at) < statement_timestamp() - $1::interval
       AND bool_and(l.keyed)
       AND bool_and(l.job_id IS NULL OR l.lease_expires_at < statement_timestamp())
)
DELETE FROM "{schema}".reservation_slots r
USING reclaimable c, locked l
WHERE r.ctid = l.ctid
  AND l.bucket_name = c.bucket_name
RETURNING r.bucket_name"""


async def sweep_idle_keyed_rows(
    conn: ConnLike,
    *,
    schema: str,
    horizon: timedelta,
    batch_size: int = DEFAULT_KEYED_ROW_RECLAIM_BATCH_SIZE,
) -> int:
    """Delete fleet-reclaimable keyed rows unused past *horizon*, one
    bounded, committed batch per table per call.

    Two statements, each its own committed transaction on *conn* (asyncpg
    auto-commits per statement): the ``rate_limit_buckets`` arm deletes at
    most *batch_size* keyed rows (one row per bucket), the
    ``reservation_slots`` arm deletes at most *batch_size* keyed BUCKETS
    whole (see ``_SWEEP_IDLE_KEYED_SLOTS_SQL``'s whole-bucket contract).
    Repeated calls drain the eligible backlog a committed batch at a time
    — the maintenance leader drives one call per tick, the
    slow-and-constant discipline the event-retention block settled (a
    stopped drain is a pause, not a rollback).

    This is the fleet-wide half of keyed row reclamation: the in-process
    half (registry idle eviction + the pending-reclaim drain) can only
    name rows its OWN process materialised, so keyed rows orphan when
    the worker that created them dies. The rows carry their own
    staleness instead — the ``keyed`` mark (migration 01.00.10_02) plus
    ``last_used_at``, refreshed by the acquire/release/upsert statements
    that already touch them — and this sweep is the deletion path that
    needs no registry at all. Static buckets are never marked and never
    deleted; redis-backend keyed rows are never marked and never
    deleted (see the buckets template's comment).

    *horizon* must be positive: ``timedelta(0)`` is the SETTING's
    disable sentinel (``WorkerSettings.keyed_row_reclaim_period``),
    never a sweep argument — at the function boundary zero would read
    as "delete every keyed row older than now", the dangerous
    misreading, so it is rejected here as a caller wiring bug (the same
    contract ``sweep_expired_events`` enforces).

    PG uses server-side ``statement_timestamp()`` for the age bound
    (STABLE, so each table's keyed partial index serves it as an Index
    Cond — see the module docstring); this function takes no ``now``
    argument.

    Returns the count of rows deleted by this call (both arms).
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    _validate_positive("batch_size", batch_size)
    if horizon <= timedelta(0):
        raise ValueError(
            f"horizon must be positive, got {horizon!r}; timedelta(0) is the "
            "settings-level disable sentinel, not a sweep argument"
        )

    buckets_tag = await conn.execute(
        _SWEEP_IDLE_KEYED_BUCKETS_SQL.format(schema=schema), horizon, batch_size
    )
    slots_tag = await conn.execute(
        _SWEEP_IDLE_KEYED_SLOTS_SQL.format(schema=schema), horizon, batch_size
    )
    count = parse_rowcount(buckets_tag) + parse_rowcount(slots_tag)
    if count > 0:
        logger.debug(
            "sweep_idle_keyed_rows",
            kind="sweep_idle_keyed_rows",
            count=count,
            schema=schema,
        )
    return count


# One sweep, two discovery names: the sweep-family naming
# (sweep_expired_events) and the leader-shared prune-family naming both
# reach this function, so the retention seam is discoverable under either.
prune_job_events = sweep_expired_events
