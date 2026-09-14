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
from taskq.backend._sql import INSERT_EVENTS_DETAIL_BATCH_SQL
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    wake_channel,
)
from taskq.obs import get_logger, log_state_change, record_deadline_exceeded_swept

__all__ = [
    "_SWEEP_1_SQL",
    "_SWEEP_2_SQL",
    "_SWEEP_3_SQL",
    "_SWEEP_4_SQL",
    "_SWEEP_RESULT_TTL_SQL",
    "SweepBatchSizer",
    "sweep_deadline_exceeded",
    "sweep_expired_locks",
    "sweep_expired_results",
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

_SWEEP_1_SQL = """\
-- Leader-only reclaim sweep (per architecture §Leader Election).  FOR
-- UPDATE SKIP LOCKED is kept so the SQL is safe if the sweep is ever run
-- concurrently; the production leader loop serializes it.
--
-- The cancel_phase != 0 carve-out below adds a flat extra 60 seconds on
-- top of cancel_grace + cleanup_grace before a job with an in-flight
-- cancel request becomes eligible for crash-reclaim.  This is a fixed
-- safety margin, not derived from any other setting: it gives the
-- cooperative-cancel/escalation protocol (see the cancellation-protocol
-- section of docs/architecture.md) extra headroom to complete on its own
-- before the crash-recovery path pre-empts it, so a merely-slow (not
-- actually crashed) cancellation isn't mistaken for a crash.
--
-- Cancel-state handling on reclaim (a deliberate, documented tradeoff):
-- * Retry branch ('pending'): cancel_phase/cancel_requested_at are
--   RESET, so the next dispatch doesn't immediately re-cancel the
--   retried job — crash-reclaim starts the new attempt with a clean
--   cancellation slate.  A caller's cancel therefore does not survive
--   into a retried attempt; phase-2 escalation only ever runs on the
--   (dead) lock-holding worker, so there is no other path that could
--   honor it there.
-- * Exhausted branch: a job whose cancel was still in-flight lands on
--   'cancelled', NOT 'crashed' — the caller's explicit request is the
--   honest terminal label: anyone reconciling terminal states sees the
--   cancel was honored.  Jobs with no cancel in-flight still land on
--   'crashed' as before.  The job_attempts row records outcome='crashed'
--   either way: that IS what happened to the attempt.
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
-- Bounded batch: LIMIT $3 caps the snap at one batch of rows, so the
-- transaction (and the FOR UPDATE row locks it holds) spans a constant
-- number of statements for a constant number of rows; the caller's loop
-- drains the remainder one committed batch at a time.
--
-- MATERIALIZED is load-bearing.  Without it the planner may inline the
-- LIMIT-ed CTE into the UPDATE as a nested loop over the target table
-- and update more rows than the LIMIT (the LIMIT then bounds only the
-- CTE's inlined appearances, not the joined result).  A CTE containing
-- FOR UPDATE is not inlinable today; the keyword pins that fence so a
-- future planner change cannot silently unbound the sweep.
--
-- ORDER BY lock_expires_at + the statement_timestamp() bounds are
-- load-bearing TOGETHER (see the module docstring for the full
-- derivation): the planner will not use a VOLATILE clock_timestamp()
-- comparison as a btree index condition, so the bound must be STABLE
-- (statement_timestamp() — the wall clock at this statement's start,
-- semantically clock_timestamp() evaluated once) to become an Index
-- Cond on jobs_running_lock_expires_idx, and the ORDER BY pins the scan
-- to that index (partial on status='running', keyed on
-- lock_expires_at) so the planner cannot instead fractional-walk some
-- other predicate-implied partial index. Measured on a 93k-row jobs
-- table (PG 18, EXPLAIN ANALYZE, BUFFERS): clock_timestamp() bound +
-- no ORDER BY seq-scanned 93,000 rows (7.3 ms) / population-walked
-- 10,000 index entries (5,143 buffers) per empty-state tick; this form
-- is an Index Scan with Index Cond that stops at the range boundary
-- (2 buffers, ~0.02 ms empty; ~1 buffer per reclaimed row in backlog,
-- oldest-expired first). The index provides the order for index-scan
-- plans (a bitmap plan pays only a top-N sort of the LIMIT-ed batch),
-- so ordering never scans the whole eligible backlog.
--
-- No keyset cursor either: every row the snap returns is transitioned by
-- this same statement, so the eligible set shrinks monotonically per
-- committed batch — there is no "later page" to resume into, the next
-- call simply sees the remainder.  SKIP LOCKED steps over contended rows
-- rather than blocking, so no front-of-order row can starve the rest.
WITH snap AS MATERIALIZED (
    SELECT id, locked_by_worker
    FROM "{schema}".jobs
    WHERE status = 'running'
      AND lock_expires_at < statement_timestamp()
      AND (cancel_phase = 0
           OR lock_expires_at < statement_timestamp() - $1::interval - $2::interval - interval '60 seconds')
    ORDER BY lock_expires_at
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
UPDATE "{schema}".jobs j
SET status = CASE
        WHEN j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable'
            THEN 'pending'::"{schema}".job_status
        WHEN j.cancel_phase != 0
            THEN 'cancelled'::"{schema}".job_status
        ELSE 'crashed'::"{schema}".job_status
    END,
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    cancel_phase = 0,
    cancel_requested_at = NULL,
    scheduled_at = CASE
        WHEN j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable'
            THEN clock_timestamp() + interval '5 seconds'
        ELSE j.scheduled_at
    END,
    finished_at = CASE
        WHEN NOT (j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable')
            THEN clock_timestamp()
        ELSE j.finished_at
    END
FROM snap
WHERE j.id = snap.id
RETURNING j.id, j.status, j.attempt, j.started_at, snap.locked_by_worker,
          clock_timestamp() AS now_ts"""

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
UPDATE "{schema}".reservation_slots
SET job_id            = NULL,
    held_by_worker_id = NULL,
    acquired_at       = NULL,
    lease_expires_at  = NULL
WHERE lease_expires_at < clock_timestamp()
  AND job_id IS NOT NULL"""

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
       'lock expired before worker reported terminal state', NULL,
       a.duration_ms, holder.id, '{{}}'::jsonb
FROM unnest($1::uuid[], $2::smallint[], $3::timestamptz[], $4::uuid[], $5::int[])
    WITH ORDINALITY AS a(job_id, attempt, started_at, worker_id, duration_ms, ord)
LEFT JOIN holder ON holder.id = a.worker_id"""

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
    WITH ORDINALITY AS a(job_id, attempt, started_at, duration_ms, ord)"""


class _ReclaimedRow(NamedTuple):
    """One row transitioned by sweep 1, carried out for post-commit logging."""

    job_id: JobId
    attempt: int
    new_status: str


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
) -> int:
    """Sweep 1: reclaim expired-lock running jobs, one bounded batch per call.

    One call transitions at most ``batch_size`` rows in one short
    transaction (server-side ``statement_timeout`` included); repeated
    calls drain the eligible backlog a batch at a time.  For each
    reclaimed job:

    - If attempts remain and retry is allowed: transition to
      ``'pending'`` with ``scheduled_at = clock_timestamp() + 5s`` backoff.
    - Otherwise, if a cancel request was still in-flight
      (``cancel_phase != 0``): transition to ``'cancelled'`` — the
      caller's explicit request is the honest terminal label.
    - Otherwise: transition to ``'crashed'``.

    Both terminal branches set ``finished_at = clock_timestamp()``, and
    all branches reset ``cancel_phase``/``cancel_requested_at`` — see the
    ``_SWEEP_1_SQL`` comment for the deliberate tradeoff this makes on
    the retry branch.

    All branches write a ``job_attempts`` row (outcome ``'crashed'``,
    error_class ``'WorkerCrashed'`` — that IS what happened to the
    attempt, regardless of the job's terminal label) and a
    ``job_events`` row (kind ``'state_change'``, reason
    ``'lock_expired'``); both writes are batched into one statement each
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
        rows = await conn.fetch(sql, cancel_grace, cleanup_grace, batch_size)

        if rows:
            job_ids: list[JobId] = []
            attempts: list[int] = []
            started_ats: list[datetime | None] = []
            worker_ids: list[UUID | None] = []
            duration_mss: list[int | None] = []
            details: list[str | None] = []

            for rec in rows:
                job_id: JobId = JobId(rec["id"])
                new_status: str = rec["status"]
                attempt: int = rec["attempt"]
                started_at: datetime | None = rec["started_at"]
                original_worker: UUID | None = rec["locked_by_worker"]

                # started_at is database-written and the attempt row's
                # finished_at is stamped clock_timestamp(); the elapsed span
                # between them must be measured in that same domain, so "now"
                # comes back on the sweep's own RETURNING rather than from this
                # process's clock, which would skew (or negate) the stored
                # duration_ms.
                duration_ms = compute_duration_ms(started_at, rec["now_ts"])

                detail: dict[str, object] = {
                    "from_state": "running",
                    "to_state": new_status,
                    "reason": "lock_expired",
                }
                if original_worker is not None:
                    detail["worker_id"] = str(original_worker)

                job_ids.append(job_id)
                attempts.append(attempt)
                started_ats.append(started_at)
                worker_ids.append(original_worker)
                duration_mss.append(duration_ms)
                details.append(jsonb_param(detail))
                reclaimed.append(_ReclaimedRow(job_id, attempt, new_status))

            await conn.execute(
                attempt_sql, job_ids, attempts, started_ats, worker_ids, duration_mss
            )
            await conn.execute(event_sql, job_ids, details, "state_change")
            await conn.execute(
                "SELECT pg_notify($1, '')",
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
    backlog a batch at a time.  Writes one ``job_events`` row per
    promoted job — batched into one statement over the whole batch —
    with ``kind='state_change'``, ``detail`` carrying
    ``from_state='scheduled'`` and ``to_state='pending'`` (identical per
    row, but carried through the same detail-batch template as every
    other event writer so the one-template rule holds).

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
    event_sql = INSERT_EVENTS_DETAIL_BATCH_SQL.format(schema=schema)

    promoted: list[_PromotedRow] = []

    async with conn.transaction():
        prev_timeout = await _apply_batch_statement_timeout(conn, statement_timeout_ms)
        rows = await conn.fetch(sql, batch_size)

        if rows:
            job_ids: list[JobId] = []
            details: list[str | None] = []
            for rec in rows:
                job_id: JobId = JobId(rec["id"])
                prev_status: str = rec["prev_status"]

                detail: dict[str, object] = {
                    "from_state": prev_status,
                    "to_state": "pending",
                }
                job_ids.append(job_id)
                details.append(jsonb_param(detail))
                promoted.append(_PromotedRow(job_id, prev_status))

            await conn.execute(event_sql, job_ids, details, "state_change")
            await conn.execute(
                "SELECT pg_notify($1, '')",
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
) -> int:
    """Sweep 4: release reservation slots whose lease has expired.

    Clears ``job_id``, ``held_by_worker_id``, ``acquired_at``, and
    ``lease_expires_at`` on matching rows.  No ``job_attempts`` or
    ``job_events`` writes — reservation slots are not job-state
    transitions.

    PG uses server-side ``clock_timestamp()`` (not ``now()``, which is
    fixed at transaction start — see the module docstring); this
    function takes no ``now`` argument.

    Returns the count of released slots.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    sql = _SWEEP_4_SQL.format(schema=schema)
    tag = await conn.execute(sql)
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
