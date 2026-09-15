"""Pre-rendered SQL template bundle for PostgresBackend.

Schema identifier is baked into pre-rendered SQL strings at render time.
All user-supplied values use asyncpg ``$N`` positional parameter binding —
no f-string interpolation of user data.

The schema identifier is validated against ``_IDENT_RE`` before formatting
(asyncpg cannot bind identifiers, so the schema is interpolated as a
validated string constant).
"""

from dataclasses import dataclass
from typing import Final

from taskq.backend._dispatch_sql import (
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_SQL,
)
from taskq.backend._sql import (
    CANCEL_ESCALATION_SQL,
    INSERT_EVENT_SQL,
    POLL_CANCEL_FLAGS_SQL,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    MIN_DEFERRAL_INTERVAL,
)

__all__ = ["SqlTemplates", "render"]

# COPY FROM column list — schema-independent, constant across all backends.
COPY_FROM_COLUMNS: Final[tuple[str, ...]] = (
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
    "snooze_count",
    "rate_limit_blocked_count",
)

# Column list for the enqueue COPY path only.  Every omitted column is
# either stamped/decided by the post-COPY fixup UPDATE
# (enqueue_batch_fast_fixup) from the server clock — never the caller's
# Python clock — or carries a DDL default the COPY lets apply (status
# 'pending', created_at/scheduled_at now(), NULL schedule_to_close /
# result_expires_at, and the zero-defaulted denial counters, which an
# enqueued job has no reason to pre-set).  COPY_FROM_COLUMNS stays
# intact: it is shared by the archive CTE column lists in
# worker/_leader_shared.py.
_COPY_ENQUEUE_OMITTED: Final[frozenset[str]] = frozenset(
    {
        "status",
        "created_at",
        "scheduled_at",
        "schedule_to_close",
        "result_expires_at",
        "snooze_count",
        "rate_limit_blocked_count",
    }
)
COPY_ENQUEUE_COLUMNS: Final[tuple[str, ...]] = tuple(
    c for c in COPY_FROM_COLUMNS if c not in _COPY_ENQUEUE_OMITTED
)

# The non-consuming deferral floor, pre-rendered for the two arms that
# carry it (mark_snoozed's snoozed arm and mark_retry_after's
# consume_budget=False snoozed arm). Derived from the constant so the
# SQL and the in-memory twin read one value and cannot drift.
_MIN_DEFERRAL_INTERVAL_SQL: Final[str] = (
    f"interval '{MIN_DEFERRAL_INTERVAL.total_seconds()} seconds'"
)


@dataclass(frozen=True, slots=True)
class SqlTemplates:
    """Pre-rendered SQL strings for PostgresBackend, schema baked in at render time."""

    # ── Terminal-write UPDATE statements ───────────────────────────
    mark_succeeded: str
    mark_failed: str
    mark_retry: str
    mark_cancelled: str
    mark_abandoned: str
    mark_snoozed: str
    mark_retry_after_consume_true: str
    mark_retry_after_consume_false: str

    # ── Shared INSERT templates ────────────────────────────────────
    insert_attempt_explicit: str
    insert_event: str

    # ── Owner check ────────────────────────────────────────────────
    select_owner: str

    # ── Cancel-path UPDATE statements ──────────────────────────────
    cancel_pending_scheduled: str
    cancel_running: str
    cancel_escalation: str

    # ── Enqueue SQL templates ──────────────────────────────────────
    enqueue: str
    enqueue_unique_for_preflight: str
    singleton_preflight: str
    enqueue_max_pending_count: str
    enqueue_select_by_key: str
    enqueue_notify: str
    enqueue_batch: str
    enqueue_batch_fetch_existing: str
    enqueue_batch_fetch_by_ids: str
    enqueue_batch_fast_fixup: str

    # ── Read SQL templates ─────────────────────────────────────────
    get_job: str
    get_attempts: str
    poll_cancel_flags: str

    # ── Dispatch SQL templates ─────────────────────────────────────
    dispatch_strict_fifo: str
    dispatch_round_robin: str

    # ── Static read SQL ────────────────────────────────────────────
    get_events: str
    poll_reclaim_events: str
    check_reclaim_visibility_risk: str
    count_pending_jobs: str
    count_active_jobs: str
    list_actor_max_pending: str

    # ── Admin operations ───────────────────────────────────────────
    retry_job: str

    # ── COPY FROM column lists ─────────────────────────────────────
    copy_from_columns: tuple[str, ...]
    copy_enqueue_columns: tuple[str, ...]


def render(schema: str) -> SqlTemplates:
    """Render all SQL templates for *schema*.

    Validates *schema* against the canonical identifier regex before
    formatting.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    s = schema

    return SqlTemplates(
        # ── Terminal-write statements ──────────────────────────────
        # Every mark_* statement below is ONE self-contained
        # data-modifying-CTE statement: the fenced jobs UPDATE, the
        # job_attempts INSERT, and the job_events INSERT that previously
        # ran as three awaited round trips inside one transaction are
        # fused into a single statement (see _terminal.py's module
        # docstring for the measured rationale and the preserved
        # invariants).  The fencing predicate, the clock_timestamp()
        # time base, the holder-CTE worker_id resolution, the per-arm
        # outcome_branch arbitration, and the $N parameter positions are
        # exactly the contracts the three-statement versions had — only
        # the round trips collapsed.
        #
        # All terminal writes use clock_timestamp() — not now() — for
        # every timestamp computed in the SET clause. now() is frozen at
        # transaction start; clock_timestamp() is the wall-clock time the
        # statement executes. In the LOOP-scope transactional path
        # (mark_succeeded_with_conn, and any future _with_conn variant)
        # now() would be the actor's start, not completion — the same
        # bug class as queue-time expiry pinning. Even terminal writes
        # that currently only run through the pool's own short
        # transaction (where now() ≈ clock_timestamp()) use
        # clock_timestamp() for consistency: a future _with_conn variant
        # would inherit the correct time base without a migration.
        #
        # result_expires_at resolution, first non-NULL wins: the stored
        # (operator-owned) result_ttl applied at completion; then the
        # caller-supplied fallback ($7 — the @actor literal the SQL cannot
        # see, also applied at completion, so a long-queued job does not
        # complete already expired); then the enqueue-time value.
        #
        # ATTEMPT-EPOCH FENCING: every worker-fenced terminal template
        # carries one conjunct deeper than the worker fence —
        # ``attempt = $k`` (here and in mark_failed / mark_cancelled;
        # ``j.attempt = (SELECT attempt FROM params)`` in the multi-arm
        # arbiters). The worker fence alone cannot distinguish attempt
        # N's stale handler from attempt N+1's live one on the SAME
        # worker after a stall → sweep reclaim → same-worker redispatch:
        # the stale handler's write matches (id, running, worker) and
        # falsely terminalises the redispatched attempt. Fence the
        # redispatched attempt with an attempt-identity epoch on every
        # terminal write so a stale attempt's terminal write cannot land
        # on the row it no longer owns. The epoch is the handler's
        # dispatch-time job-row attempt snapshot, threaded from every
        # call site; a mismatched epoch — a stale handler, or a caller
        # that cannot present one ($k IS NULL never satisfies the
        # equality) — makes the UPDATE match no row and the write no-ops
        # through the same machinery as the worker fence (rowcount 0 →
        # False / WorkerOwnershipMismatch / "noop", no publish).
        #
        # duration_ms is computed IN the statement from the same
        # database-written timestamp pair Python used to receive and
        # multiply back — but server-side, with exact numeric arithmetic
        # instead of Python's float path: values can differ from the old
        # Python computation by 1ms on exactly-whole-millisecond
        # boundaries (where the float product drifted just below the
        # integer), and the server-side values are the strictly more
        # accurate ones.  trunc() keeps the same
        # truncation-toward-zero Python's int() had (a bare ::int cast
        # rounds to nearest — a silent off-by-one against every
        # historically stored value), and NULL operands propagate to
        # NULL exactly like compute_duration_ms's None return.
        mark_succeeded=f"""\
WITH upd AS (
    UPDATE "{s}".jobs
    SET status = 'succeeded',
        finished_at = clock_timestamp(),
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        result = $3::jsonb,
        result_size_bytes = $4,
        result_expires_at = COALESCE(
            (SELECT clock_timestamp() + result_ttl * interval '1 second' FROM "{s}".actor_config WHERE actor = "{s}".jobs.actor),
            clock_timestamp() + $7::interval,
            result_expires_at
        ),
        progress_seq = $5,
        progress_state = CASE WHEN $6::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $6::jsonb ELSE progress_state END
    WHERE id = $1 AND status = 'running' AND locked_by_worker = $2 AND attempt = $8
    RETURNING *
), holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
), att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT upd.id, upd.attempt, upd.started_at, clock_timestamp(), 'succeeded',
           NULL, NULL, NULL,
           trunc(EXTRACT(EPOCH FROM (upd.finished_at - upd.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM upd
), evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT upd.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'succeeded',
                              'worker_id', $2::text)
    FROM upd
)
SELECT * FROM upd""",
        mark_failed=f"""\
WITH upd AS (
    UPDATE "{s}".jobs
    SET status = 'failed',
        finished_at = clock_timestamp(),
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        error_class = $3,
        error_message = $4,
        error_traceback = $5,
        progress_seq = $6,
        progress_state = CASE WHEN $7::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $7::jsonb ELSE progress_state END
    WHERE id = $1 AND status = 'running' AND locked_by_worker = $2 AND attempt = $8
    RETURNING *
), holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
), att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT upd.id, upd.attempt, upd.started_at, clock_timestamp(), 'failed',
           $3, $4, $5,
           trunc(EXTRACT(EPOCH FROM (upd.finished_at - upd.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM upd
), evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT upd.id, clock_timestamp(), 'state_change',
           jsonb_strip_nulls(jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                                                'error_class', $3::text,
                                                'worker_id', $2::text))
    FROM upd
)
SELECT * FROM upd""",
        # mark_retry is a two-CTE single-arbiter statement, structurally
        # mirroring mark_snoozed / mark_retry_after: the delay ($3::interval,
        # floored at MIN_DEFERRAL_INTERVAL via the params CTE's GREATEST —
        # see mark_snoozed's params comment for the monopolisation hazard
        # an unfloored requeue arm is: a failure-retry decision must never
        # requeue below the deferral floor, the same bound the deferral
        # arms and the in-memory twin's _mark_failed_or_retry apply) is
        # applied by the SERVER clock (scheduled_at = clock_timestamp() +
        # effective_delay; the status derives from the effective delay
        # alone), and the schedule_to_close deadline is arbitrated in the
        # same statement — clock_timestamp() + effective_delay <=
        # schedule_to_close retries; past it, the deadline_failed CTE
        # lands 'failed' with error_class='DeadlineExceeded'.  The caller
        # never passes a Python-domain timestamp (C1: a skewed caller could
        # otherwise void the backoff or kill a live job).
        #
        # Every arm that hands the job back for another dispatch (this
        # template's `retried` CTE, and the `snoozed` CTEs of mark_snoozed /
        # mark_retry_after_consume_*) also RESETS cancel_phase and
        # cancel_requested_at, byte-for-byte as _sweeps.py's _SWEEP_1_SQL and
        # heartbeat.py's _ISOLATE_JOB_SQL_TEMPLATE do on their retry arm.
        # Retries reuse the SAME job row, so a cancel that escalated to
        # phase 2 in the same instant the actor raised a retryable exception
        # would otherwise be inherited by the next attempt: the cancel
        # controller's PG-observation fast-advance sees db_phase=FORCED,
        # jumps the local phase straight to FORCED without ever calling
        # task.cancel(), and the attempt becomes uncancellable.  The
        # TERMINAL arms deliberately keep the cancel columns — they are the
        # audit trail of why the job ended, and mark_abandoned's
        # `cancel_phase = 2` guard reads them.
        mark_retry=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           -- The failure-retry floor, the same bound the deferral arms
           -- pin (mark_snoozed's params comment): a sub-floor requeue
           -- delay parks the job at the head of the dispatch order and
           -- cycles a worker slot at claim/run/fail/retry round-trip
           -- rate. effective_delay is the arm's SINGLE delay — status,
           -- scheduled_at and every deadline comparison read it, so the
           -- retried and deadline arms partition exactly.
           GREATEST($3::interval, {_MIN_DEFERRAL_INTERVAL_SQL}) AS effective_delay,
           $9::int AS attempt
),
retried AS (
    UPDATE "{s}".jobs j
    SET status = CASE WHEN (SELECT effective_delay FROM params) > interval '0' THEN 'scheduled'::"{s}".job_status
                      ELSE 'pending'::"{s}".job_status END,
        scheduled_at = clock_timestamp() + (SELECT effective_delay FROM params),
        finished_at = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        cancel_phase = 0,
        cancel_requested_at = NULL,
        error_class = $4,
        error_message = $5,
        error_traceback = $6,
        progress_seq = $7,
        progress_state = CASE WHEN $8::jsonb IS NOT NULL
                              THEN COALESCE(j.progress_state, '{{}}'::jsonb) || $8::jsonb
                              ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT effective_delay FROM params) <= j.schedule_to_close)
    RETURNING j.*, 'retried'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = 'DeadlineExceeded',
        error_message = 'schedule_to_close reached before next retry dispatch',
        error_traceback = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = $7,
        progress_state = CASE WHEN $8::jsonb IS NOT NULL
                              THEN COALESCE(j.progress_state, '{{}}'::jsonb) || $8::jsonb
                              ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT effective_delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM retried)
    RETURNING j.*, 'deadline_failed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
),
retried_att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT r.id, r.attempt, r.started_at, clock_timestamp(), 'failed',
           $4, $5, $6,
           -- The retried arm leaves finished_at NULL (the job lives on),
           -- so the attempt's end is the arm's own now_ts — mirroring the
           -- Python that read rec["now_ts"] off the same RETURNING.
           trunc(EXTRACT(EPOCH FROM (r.now_ts - r.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM retried r
),
retried_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT r.id, clock_timestamp(), 'state_change',
           jsonb_strip_nulls(jsonb_build_object('from_state', 'running', 'to_state', 'scheduled',
                                                'error_class', $4::text,
                                                'worker_id', $2::text))
    FROM retried r
),
deadline_att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT d.id, d.attempt, d.started_at, clock_timestamp(), 'failed',
           'DeadlineExceeded', 'schedule_to_close reached before next retry dispatch', NULL,
           -- Terminal arm: duration reads the arm's finished_at, not now_ts.
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_failed d
),
deadline_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                              'error_class', 'DeadlineExceeded',
                              'worker_id', $2::text)
    FROM deadline_failed d
)
SELECT * FROM retried UNION ALL SELECT * FROM deadline_failed""",
        mark_cancelled=f"""\
WITH upd AS (
    UPDATE "{s}".jobs
    SET status = 'cancelled',
        finished_at = clock_timestamp(),
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        progress_seq = $3,
        progress_state = CASE WHEN $4::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $4::jsonb ELSE progress_state END
    WHERE id = $1 AND status = 'running' AND locked_by_worker = $2 AND attempt = $5
    RETURNING *
), holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
), att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT upd.id, upd.attempt, upd.started_at, clock_timestamp(), 'cancelled',
           NULL, NULL, NULL,
           trunc(EXTRACT(EPOCH FROM (upd.finished_at - upd.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM upd
), evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT upd.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'cancelled',
                              'worker_id', $2::text)
    FROM upd
)
SELECT * FROM upd""",
        mark_abandoned=f"""\
WITH upd AS (
    UPDATE "{s}".jobs
    SET status = 'abandoned',
        finished_at = clock_timestamp(),
        progress_seq = $2,
        progress_state = CASE WHEN $3::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $3::jsonb ELSE progress_state END
    -- The NULL-lease arm is defense-in-depth for the no-exit cell
    -- (running x lock_expires_at IS NULL x dead holder): Postgres has
    -- exactly one writer that moves a row to running -- the dispatch CTE
    -- (_dispatch_sql.py stamps lock_expires_at = clock_timestamp() +
    -- lock_lease unconditionally) -- and every other writer that clears
    -- the lease also leaves running (isolate_self, sweep 1, terminal
    -- writes), so a running row with a NULL lease is a direct-SQL-only
    -- corruption shape. Its holder cannot be live, so the escalation
    -- ladder's precondition (a live holder that must be given the chance
    -- to reach phase 2) can never be satisfied: the cancel protocol can
    -- land phase 1 and stall forever (mark_abandoned was phase-2-only,
    -- the reclaim sweep's lock_expires_at < bound is NULL-false at every
    -- age, and every other exit is owner-scoped). The abandon's
    -- precondition -- the holder had its chance -- holds vacuously, so
    -- the arm abandons directly and _mark_abandoned warns on the shape.
    -- The in-memory twin has no mirror arm: its store stamps a lease on
    -- every lease-less running write (testing/in_memory.py _JobStore),
    -- so the cell is unrepresentable there by construction.
    WHERE id = $1 AND status = 'running'
      AND (cancel_phase = 2 OR lock_expires_at IS NULL)
    RETURNING *
), holder AS (
    -- The abandoned job's worker id is the row's own (possibly already
    -- NULL) locked_by_worker, not a parameter: probe THAT id.
    SELECT id FROM "{s}".workers WHERE id = (SELECT locked_by_worker FROM upd) FOR KEY SHARE
), att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT upd.id, upd.attempt, upd.started_at, clock_timestamp(), 'cancelled',
           NULL, NULL, NULL,
           trunc(EXTRACT(EPOCH FROM (upd.finished_at - upd.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM upd
), evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT upd.id, clock_timestamp(), 'state_change',
           jsonb_strip_nulls(jsonb_build_object('from_state', 'running', 'to_state', 'abandoned',
                                                'worker_id', upd.locked_by_worker::text))
    FROM upd
)
SELECT * FROM upd""",
        # A non-consuming deferral never spends retry budget, and the
        # ceiling is never a counter — no arm here raises max_attempts.
        # Every deferral shape REFUNDS the claim's attempt increment:
        # attempt - 1, floored at 0.  Dispatch stamped attempt = attempt
        # + 1 when it claimed the row; no actor ran, so the increment is
        # returned and the gap `max_attempts - attempt` is exactly what
        # it was before the claim.  A job can therefore defer
        # indefinitely: attempt oscillates between N and N+1 and never
        # walks toward the smallint ceiling.  The ceiling itself is
        # immutable here — a deferral restores the attempt it borrowed
        # rather than widening `max_attempts`, so the budget an operator
        # configured is the budget the job gets, and only an explicit
        # admin retry raises the ceiling.
        #
        # This covers the admission denials ($7 = 'reservation_denied' /
        # 'rate_limit_denied') exactly as it covers the actor-requested
        # deferral ($7 = 'snoozed').  A denial carries the semantics of
        # an HTTP 429 with Retry-After: it reports that the fleet had no
        # slot, which is a statement about capacity and never about the
        # work.  So it may neither spend the budget nor decide the
        # outcome.  Charging it would make how many real retries a job
        # gets depend on how saturated a bucket happened to be while the
        # job waited — a load-dependent, unreproducible retry policy —
        # and would let a queue or rate-limit misconfiguration terminate
        # work that never ran.  A denied job reschedules until capacity
        # frees; its ONLY terminal exit is its own schedule_to_close,
        # reached through the ordinary deadline arm below.  Unbounded
        # rescheduling stays affordable precisely because a deferral
        # mints no per-occurrence rows: contention is carried by the
        # aggregated counters on the row (snooze_count /
        # rate_limit_blocked_count, keyed by $7) and by OTEL, so
        # sustained saturation costs one counter bump per cycle instead
        # of a table's worth of history.
        #
        # Two arms, exhaustive and mutually exclusive over every fenced
        # row:
        #   snoozed         — the reschedule point still fits inside
        #                     schedule_to_close, or there is none;
        #   deadline_failed — the reschedule point passes
        #                     schedule_to_close.
        # denial_reason never reaches this statement: 'capacity' (the
        # store answered "full") and 'unavailable' (the store could not
        # answer) are both admission backpressure about a job whose actor
        # never ran, and take the identical non-consuming shape. The
        # reason is validated at the Python boundary for the caller's own
        # observability, not branched on here.
        #
        # The refund revisits attempt numbers, which is collision-safe:
        # a non-terminal snooze/denial writes NO job_attempts/job_events
        # rows — it is admission control, not an execution — so no writer
        # ever lands on a revisited PK (job_id, attempt).  The deadline
        # arm writes its rows uniformly with every other terminal
        # transition, at the attempt number dispatch stamped and no
        # refund has returned.
        mark_snoozed=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           -- A non-consuming deferral reschedules at least
           -- MIN_DEFERRAL_INTERVAL out (the GREATEST below): a zero/now
           -- delay would park the job 'pending' at clock_timestamp() at
           -- the head of the dispatch order (ORDER BY scheduled_at),
           -- instantly re-claimable — one claim/refund round trip per
           -- cycle monopolising a worker slot. A non-future snooze must
           -- be rejected: a zero delay would cause immediate re-claim and
           -- burn worker slots. The consuming arms keep the raw delay: an
           -- immediate consuming retry is a real execution, bounded by the
           -- budget it spends. effective_delay is the arm's SINGLE delay —
           -- status, scheduled_at and every deadline comparison read it,
           -- so the snoozed and deadline arms partition exactly (a row can
           -- never match neither).
            GREATEST($3::interval, {_MIN_DEFERRAL_INTERVAL_SQL}) AS effective_delay,
            $4::jsonb AS metadata_update,
            $5::int AS progress_seq,
            $6::jsonb AS progress_state,
            $8::int AS attempt
),
snoozed AS (
    UPDATE "{s}".jobs j
    SET status = CASE WHEN (SELECT effective_delay FROM params) > interval '0' THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END,
        scheduled_at = clock_timestamp() + (SELECT effective_delay FROM params),
        finished_at = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        cancel_phase = 0,
        cancel_requested_at = NULL,
        attempt = GREATEST(j.attempt - 1, 0),
        snooze_count = CASE WHEN $7::text = 'snoozed' THEN j.snooze_count + 1 ELSE j.snooze_count END,
        rate_limit_blocked_count = CASE WHEN $7::text IN ('reservation_denied', 'rate_limit_denied') THEN j.rate_limit_blocked_count + 1 ELSE j.rate_limit_blocked_count END,
        metadata = j.metadata || COALESCE((SELECT metadata_update FROM params), '{{}}'::jsonb),
        progress_seq = (SELECT progress_seq FROM params),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT effective_delay FROM params) <= j.schedule_to_close)
    RETURNING j.*, 'snoozed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = 'DeadlineExceeded',
        error_message = 'schedule_to_close reached before next dispatch',
        error_traceback = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        -- The denial that ran the job out of road still happened to it,
        -- and with no per-occurrence rows the aggregate is its only
        -- record: counting it here makes the terminal row show that the
        -- deadline was reached WHILE the job was starving for admission,
        -- rather than making that last denial vanish.  An actor-requested
        -- deferral is NOT counted here — snooze_count tallies deferrals
        -- the job actually took, and this one was rejected outright.
        rate_limit_blocked_count = CASE WHEN $7::text IN ('reservation_denied', 'rate_limit_denied') THEN j.rate_limit_blocked_count + 1 ELSE j.rate_limit_blocked_count END,
        progress_seq = (SELECT progress_seq FROM params),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT effective_delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, 'failed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
),
deadline_att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT d.id, d.attempt, d.started_at, clock_timestamp(), 'failed',
           'DeadlineExceeded', 'schedule_to_close reached before next dispatch', NULL,
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_failed d
),
deadline_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                              'error_class', 'DeadlineExceeded',
                              'worker_id', $2::text)
    FROM deadline_failed d
)
SELECT * FROM snoozed
UNION ALL SELECT * FROM deadline_failed""",
        mark_retry_after_consume_true=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           $3::interval AS delay,
           $4::int AS progress_seq,
           $5::jsonb AS progress_state,
           $6::int AS attempt
),
        snoozed AS (
    UPDATE "{s}".jobs j
    SET status = CASE WHEN $3::interval > interval '0' THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END,
        scheduled_at = clock_timestamp() + (SELECT delay FROM params),
        finished_at = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        cancel_phase = 0,
        cancel_requested_at = NULL,
        progress_seq = (SELECT progress_seq FROM params),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT delay FROM params) <= j.schedule_to_close)
      AND (j.retry_kind = 'indefinite'
           OR j.attempt < j.max_attempts)
    RETURNING j.*, j.attempt AS running_attempt, 'snoozed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
max_attempts_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = 'MaxAttemptsExceeded',
        error_message = 'retry budget exhausted',
        error_traceback = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = (SELECT progress_seq FROM params),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      -- Every non-indefinite kind exhausts here: a non_retryable job at
      -- budget under RetryAfter(consume_budget=True) has no other exit —
      -- a 'transient'-only predicate left it matching no arm at all and
      -- the statement fell through to a silent reschedule.
      AND j.retry_kind <> 'indefinite'
      AND j.attempt >= j.max_attempts
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT delay FROM params) <= j.schedule_to_close)
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, j.attempt AS running_attempt, 'max_attempts_failed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = 'DeadlineExceeded',
        error_message = 'schedule_to_close reached before next dispatch',
        error_traceback = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = (SELECT progress_seq FROM params),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM snoozed)
      AND NOT EXISTS (SELECT 1 FROM max_attempts_failed)
    RETURNING j.*, j.attempt AS running_attempt, 'deadline_failed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
),
snoozed_att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT sn.id, sn.attempt, sn.started_at, clock_timestamp(), 'snoozed',
           'RetryAfter', NULL, NULL,
           trunc(EXTRACT(EPOCH FROM (sn.now_ts - sn.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM snoozed sn
),
snoozed_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT sn.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'scheduled',
                              'worker_id', $2::text)
    FROM snoozed sn
),
max_attempts_att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT m.id, m.attempt, m.started_at, clock_timestamp(), 'failed',
           'MaxAttemptsExceeded', 'retry budget exhausted', NULL,
           trunc(EXTRACT(EPOCH FROM (m.finished_at - m.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM max_attempts_failed m
),
max_attempts_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT m.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                              'error_class', 'MaxAttemptsExceeded',
                              'worker_id', $2::text)
    FROM max_attempts_failed m
),
deadline_att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT d.id, d.attempt, d.started_at, clock_timestamp(), 'failed',
           'DeadlineExceeded', 'schedule_to_close reached before next dispatch', NULL,
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_failed d
),
deadline_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                              'error_class', 'DeadlineExceeded',
                              'worker_id', $2::text)
    FROM deadline_failed d
)
SELECT * FROM snoozed
UNION ALL SELECT * FROM max_attempts_failed
 UNION ALL SELECT * FROM deadline_failed""",
        # The RetryAfter(consume_budget=False) arm is an actor-requested
        # deferral — a server Retry-After honoured without spending
        # budget — so it carries the mark_snoozed 'snoozed' contract in
        # full: the claim's attempt increment is REFUNDED (attempt - 1,
        # floored at 0), the ceiling is never touched, the deferral is
        # unbounded (downstream may be unready for hours; attempt
        # oscillates and never walks the smallint column), and the only
        # terminal exit is the schedule_to_close deadline.  See the
        # mark_snoozed comment for the corpus convention.  The outcome
        # here is always a plain snooze, so only snooze_count increments.
        mark_retry_after_consume_false=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           -- effective_delay carries mark_snoozed's deferral floor (see
           -- its params comment for the why): this arm is non-consuming
           -- by construction, and it must never park the job at the
           -- head of the dispatch order either.
            GREATEST($3::interval, {_MIN_DEFERRAL_INTERVAL_SQL}) AS effective_delay,
            $4::int AS progress_seq,
            $5::jsonb AS progress_state,
            $6::int AS attempt
),
snoozed AS (
    UPDATE "{s}".jobs j
    SET status = CASE WHEN (SELECT effective_delay FROM params) > interval '0' THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END,
        scheduled_at = clock_timestamp() + (SELECT effective_delay FROM params),
        finished_at = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        cancel_phase = 0,
        cancel_requested_at = NULL,
        attempt = GREATEST(j.attempt - 1, 0),
        snooze_count = j.snooze_count + 1,
        progress_seq = (SELECT progress_seq FROM params),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT effective_delay FROM params) <= j.schedule_to_close)
    RETURNING j.*, 'snoozed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = 'DeadlineExceeded',
        error_message = 'schedule_to_close reached before next dispatch',
        error_traceback = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = (SELECT progress_seq FROM params),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND j.attempt = (SELECT attempt FROM params)
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT effective_delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, 'deadline_failed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
),
deadline_att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT d.id, d.attempt, d.started_at, clock_timestamp(), 'failed',
           'DeadlineExceeded', 'schedule_to_close reached before next dispatch', NULL,
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_failed d
),
deadline_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                              'error_class', 'DeadlineExceeded',
                              'worker_id', $2::text)
    FROM deadline_failed d
)
SELECT * FROM snoozed
UNION ALL SELECT * FROM deadline_failed""",
        # ── Shared INSERT templates ────────────────────────────────
        # Same holder-CTE idiom as INSERT_ATTEMPT_SQL (see _sql.py for the
        # rationale): resolve worker_id against workers under FOR KEY SHARE
        # so a deleted worker records NULL instead of FK-violating.
        insert_attempt_explicit=f"""\
WITH holder AS (
    SELECT id FROM "{s}".workers WHERE id = $10 FOR KEY SHARE
)
INSERT INTO "{s}".job_attempts
(job_id, attempt, started_at, finished_at, outcome,
 error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
        (SELECT id FROM holder), $11::jsonb)""",
        insert_event=INSERT_EVENT_SQL.format(schema=s),
        # ── Owner check ────────────────────────────────────────────
        select_owner=f"""\
SELECT locked_by_worker FROM "{s}".jobs WHERE id = $1""",
        # ── Cancel-path UPDATE statements ──────────────────────────
        cancel_pending_scheduled=f"""\
WITH prev AS (
    SELECT status AS prev_status FROM "{s}".jobs WHERE id = $1 FOR UPDATE
)
UPDATE "{s}".jobs
SET status = 'cancelled', finished_at = clock_timestamp()
FROM prev
WHERE "{s}".jobs.id = $1 AND "{s}".jobs.status IN ('pending', 'scheduled')
RETURNING prev.prev_status""",
        cancel_running=f"""\
UPDATE "{s}".jobs
SET cancel_requested_at = clock_timestamp(), cancel_phase = 1
WHERE id = $1 AND status = 'running' AND cancel_phase = 0
RETURNING locked_by_worker""",
        cancel_escalation=CANCEL_ESCALATION_SQL.format(schema=s),
        # ── Enqueue SQL templates ──────────────────────────────────
        # schedule_to_close is single-domain server-side on this arm: the
        # interval form anchors to clock_timestamp() (matching the previous
        # enqueue_with_interval behaviour), and a raw absolute datetime (the
        # deprecated caller-domain form) only applies when the interval is
        # NULL — clock_timestamp() + NULL::interval is NULL, so COALESCE
        # falls through to $22.  $22 is a NEW trailing slot (bound after
        # $21::text[]): $12 is start_to_close and must not be displaced.
        enqueue=f"""\
INSERT INTO "{s}".jobs
(id, actor, queue, identity_key, fairness_key,
 payload, payload_schema_ver, status, priority,
 max_attempts, retry_kind,
 schedule_to_close, start_to_close, heartbeat_timeout,
 scheduled_at,
 idempotency_scope, idempotency_key, trace_id, span_id, metadata, result_expires_at, tags)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, CASE WHEN COALESCE($14, clock_timestamp()) > clock_timestamp() THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END, $8, $9, $10, COALESCE(clock_timestamp() + $11::interval, $22), $12, $13, COALESCE($14, clock_timestamp()), $15, $16, $17, $18, $19::jsonb, clock_timestamp() + $20::interval, $21::text[])
ON CONFLICT (idempotency_scope, idempotency_key) WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING *""",
        enqueue_unique_for_preflight=f"""\
SELECT * FROM "{s}".jobs
WHERE actor = $1
  AND identity_key = $2
  AND status = ANY($3::"{s}".job_status[])
  AND created_at > clock_timestamp() - $4::interval
ORDER BY created_at DESC
LIMIT 1""",
        singleton_preflight=f"""\
SELECT id, schedule_to_close FROM "{s}".jobs
WHERE actor = $1 AND status IN ('pending', 'scheduled', 'running')
AND metadata @> '{{"singleton": true}}'::jsonb
LIMIT 1""",
        enqueue_max_pending_count=f"""\
SELECT count(*) FROM "{s}".jobs
WHERE actor = $1 AND status IN ('pending', 'scheduled')""",
        enqueue_select_by_key=f"""\
SELECT * FROM "{s}".jobs WHERE idempotency_scope = $1 AND idempotency_key = $2""",
        enqueue_notify="SELECT pg_notify($1, '')",
        enqueue_batch=f"""\
INSERT INTO "{s}".jobs (
    id, actor, queue, identity_key, fairness_key,
    payload, payload_schema_ver,
    status, priority, attempt, max_attempts, retry_kind,
    schedule_to_close, start_to_close, heartbeat_timeout,
    scheduled_at, metadata, idempotency_scope, idempotency_key, trace_id, span_id,
    result_expires_at, tags
)
SELECT
    t.id,
    t.actor,
    t.queue,
    t.identity_key,
    t.fairness_key,
    t.payload,
    t.payload_schema_ver,
    CASE WHEN COALESCE(t.scheduled_at, clock_timestamp()) > clock_timestamp() THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END,
    t.priority,
    0,
    t.max_attempts,
    t.retry_kind,
    -- Same single-domain shape as the single-row enqueue template: the
    -- interval form anchors to clock_timestamp(); a raw absolute datetime
    -- (deprecated caller-domain form) applies only when the interval is
    -- NULL (clock_timestamp() + NULL::interval is NULL).
    COALESCE(clock_timestamp() + t.stc_interval, t.stc_raw),
    t.start_to_close,
    t.heartbeat_timeout,
    -- Immediate rows are stamped with the STATEMENT-time clock, matching
    -- the single-row enqueue template and the COPY fixup — not now(),
    -- which on the caller-supplied-connection path is the caller's
    -- transaction start.
    COALESCE(t.scheduled_at, clock_timestamp()),
    t.metadata,
    t.idempotency_scope,
    t.idempotency_key,
    t.trace_id,
    t.span_id,
    -- result_expires_at is anchored to the server clock (the TTL sweep
    -- compares clock_timestamp() server-side); NULL ttl → NULL (PG:
    -- clock_timestamp() + NULL::interval is NULL).
    clock_timestamp() + t.result_ttl,
    -- Pg text[][] does not support jagged arrays (empty sub-array () has different
    -- dimensionality from ('a','b')).  We pass tags via jsonb[] transit ($21::jsonb[])
    -- and unpack each element into text[] with jsonb_array_elements_text(…)::text[].
    (SELECT COALESCE(array_agg(elem::text), '{{}}'::text[]) FROM jsonb_array_elements_text(t.tags_jsonb) AS elem)
FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[],
    $6::jsonb[], $7::int[],
    $8::int[], $9::int[], $10::text[],
    $11::interval[], $12::interval[], $13::interval[],
    $14::timestamptz[], $15::jsonb[], $16::text[], $17::text[], $18::text[], $19::text[],
    $20::interval[], $21::jsonb[], $22::timestamptz[]
) AS t(id, actor, queue, identity_key, fairness_key,
    payload, payload_schema_ver,
    priority, max_attempts, retry_kind,
    stc_interval, start_to_close, heartbeat_timeout,
    scheduled_at, metadata, idempotency_scope, idempotency_key, trace_id, span_id,
    result_ttl, tags_jsonb, stc_raw)
ON CONFLICT (idempotency_scope, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id, actor, queue, identity_key, status, idempotency_key, idempotency_scope""",
        enqueue_batch_fetch_existing=f"""\
SELECT j.* FROM "{s}".jobs j
JOIN unnest($1::text[], $2::text[]) AS pairs(scope, key)
  ON j.idempotency_scope = pairs.scope AND j.idempotency_key = pairs.key""",
        enqueue_batch_fetch_by_ids=f"""\
SELECT * FROM "{s}".jobs WHERE id = ANY($1::uuid[])""",
        # Post-COPY corrective UPDATE for enqueue_batch_fast.  COPY cannot
        # compute/decide anything, so it writes only domain-insensitive
        # columns (COPY_ENQUEUE_COLUMNS) and this UPDATE — executed inside
        # the same transaction, before the notify — stamps status,
        # scheduled_at, schedule_to_close and result_expires_at from the
        # server clock.  The status CASE is byte-for-byte the INSERT arms'
        # semantics (enqueue / enqueue_batch above), which is what makes a
        # NULL ("immediate") scheduled_at safe on this path too.
        enqueue_batch_fast_fixup=f"""\
WITH params AS (
    SELECT * FROM unnest(
        $1::uuid[], $2::timestamptz[], $3::interval[], $4::timestamptz[], $5::interval[]
    ) AS t(id, scheduled_at, stc_interval, stc_raw, result_ttl)
)
UPDATE "{s}".jobs j
SET status            = CASE WHEN COALESCE(p.scheduled_at, clock_timestamp()) > clock_timestamp()
                             THEN 'scheduled'::"{s}".job_status
                             ELSE 'pending'::"{s}".job_status END,
    scheduled_at      = COALESCE(p.scheduled_at, clock_timestamp()),
    schedule_to_close = COALESCE(clock_timestamp() + p.stc_interval, p.stc_raw),
    result_expires_at = CASE WHEN p.result_ttl IS NULL THEN NULL
                             ELSE clock_timestamp() + p.result_ttl END
FROM params p
WHERE j.id = p.id""",
        # ── Read SQL templates ─────────────────────────────────────
        get_job=f"""\
SELECT * FROM "{s}".jobs WHERE id = $1""",
        get_attempts=f"""\
SELECT * FROM "{s}".job_attempts WHERE job_id = $1 ORDER BY attempt""",
        poll_cancel_flags=POLL_CANCEL_FLAGS_SQL.format(schema=s),
        # ── Dispatch SQL templates ─────────────────────────────────
        dispatch_strict_fifo=DISPATCH_STRICT_FIFO_SQL.format(schema=s),
        dispatch_round_robin=DISPATCH_ROUND_ROBIN_SQL.format(schema=s),
        # ── Static read SQL ────────────────────────────────────────
        get_events=f"""\
SELECT id AS event_id, job_id, occurred_at, kind, detail
FROM "{s}".job_events
WHERE job_id = $1
ORDER BY occurred_at, event_id""",
        poll_reclaim_events=f"""\
-- Trailing-watermark filter, NOT a snapshot/xact-id predicate.
-- A per-row transaction-id check against pg_snapshot_xmin() is
-- insufficient: an uncommitted sibling row is invisible under MVCC to
-- this SELECT no matter what predicate is used, so no boundary computed
-- only over *visible* rows can ever detect (or bound) it — this was
-- verified to still lose events under an inverted allocation order
-- (a transaction that commits first can hold a lower transaction id but
-- a HIGHER event_id than one still open with a LOWER event_id).
--
-- id (bigserial nextval) and occurred_at (clock_timestamp()) are stamped
-- by the same INSERT statement, so they are co-monotonic: an earlier id
-- has an earlier-or-equal occurred_at — provided the two volatile calls
-- do not interleave across concurrent transactions within a single
-- INSERT (Postgres gives no such atomicity; the window is nanosecond-
-- scale — see taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY).  Only
-- rows older than
-- RECLAIM_EVENT_VISIBILITY_DELAY are returned: by the time a row clears
-- that margin, any transaction that could have inserted a still-lower
-- id has had at least as long to commit, so it must have either
-- committed (and is returned, correctly ordered, in this or an earlier
-- poll) or aborted (permanently gone).  See
-- taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY for the bound this
-- assumes on writer transaction duration.
SELECT id AS event_id, job_id, occurred_at, kind, detail
FROM "{s}".job_events
WHERE kind = 'state_change'
  AND (detail->>'reason') = 'lock_expired'
  AND id > $1
  AND occurred_at < clock_timestamp() - $3::interval
ORDER BY id ASC
LIMIT $2""",
        check_reclaim_visibility_risk=f"""\
-- Diagnostic only (see LongRunningJobEventsWriter): a proxy signal for
-- "poll_reclaim_events' visibility-delay assumption may currently be
-- violated" — any transaction holding a lock on job_events for longer
-- than the margin is a candidate cause (lock contention, an overloaded
-- scan, a stalled/GC-paused worker, an oversized batch). Not proof of an
-- actual miss: this cannot see whether that transaction will insert a
-- job_events row at all, only that it has held the table open unusually
-- long. Excludes this query's own backend.
-- The ::float8 cast matters: EXTRACT returns numeric, which asyncpg
-- hands back as Decimal — but LongRunningJobEventsWriter.xact_age_seconds
-- is a float, and Decimal is not JSON-serializable for the monitoring
-- loop this diagnostic feeds.
SELECT a.pid, a.xact_start,
       EXTRACT(EPOCH FROM (clock_timestamp() - a.xact_start))::float8 AS xact_age_seconds
FROM pg_locks l
JOIN pg_stat_activity a ON a.pid = l.pid
WHERE l.relation = '"{s}".job_events'::regclass
  AND l.locktype = 'relation'
  AND a.pid != pg_backend_pid()
  AND a.xact_start < clock_timestamp() - $1::interval""",
        count_pending_jobs=(
            f'SELECT actor, count(*)::int AS cnt FROM "{s}".jobs '
            f"WHERE actor = ANY($1::text[]) "
            f"AND status IN ('pending', 'scheduled') "
            f"GROUP BY actor"
        ),
        count_active_jobs=(
            f'SELECT count(*)::int FROM "{s}".jobs '
            f"WHERE queue = ANY($1::text[]) "
            f"AND status IN ('pending', 'scheduled', 'running')"
        ),
        # One row per actor — the client-side capacity cache reads the
        # whole table at most once per TTL window per process.
        list_actor_max_pending=f'SELECT actor, max_pending FROM "{s}".actor_config',
        # ── Admin operations ───────────────────────────────────────
        # Monotonic attempt: admin-retry must leave the attempt counter at
        # its spent value and raise the ceiling instead. Resetting to 0
        # made the re-dispatch revisit the spent epoch's numbers (dispatch
        # claims at attempt + 1, so a reset row climbs back through every
        # spent number), and the next attempt-row INSERT collided on
        # job_attempts_pkey (job_id, attempt) — a data defect the
        # consumer's terminal-write infra family misclassified as transient,
        # stranding the row running with every reclaim cycle dying on the
        # same spent key. attempt is therefore NOT assigned here: the re-run
        # climbs to fresh numbers and every attempt-row write lands on a
        # fresh key. The ceiling raise
        # opens the budget gates (the reclaim sweep's re-pend branch,
        # the terminal arms) for at least one fresh execution, while a
        # mid-budget re-run keeps its remaining budget (GREATEST is a
        # no-op there); the LEAST cap is the smallint max_attempts
        # discipline — and when the cap binds (attempt already at
        # 32767) the gate cannot open, so the retry is refused and the
        # row stays terminal instead of re-pending a row whose next
        # claim would overflow the smallint attempt column and poison
        # the whole claim batch.
        #
        # The reopened CTE is the batch-status reconciliation for the
        # completed-batch membership lie: retry_job re-pends a failed/
        # crashed/cancelled member with no batch awareness, and every
        # batch-status writer guards on status = 'active' (complete_batch,
        # abort_batch, the leader's complete_stale_batches), so a
        # terminal batch row sitting over re-pended membership was
        # unreconcilable by any of them -- the row claimed an outcome its
        # membership contradicted, and wait_for_batch snoozed on it
        # forever. The reopen runs in the retry's own transaction: the
        # moment a member becomes non-terminal again, a terminal
        # ('complete'/'aborted') batch row returns to 'active' with its
        # completed_at cleared, so the ordinary guards re-engage (a later
        # complete_batch or the stale-batch sweep re-arbitrates against
        # the live membership, and an operator abort can again be
        # recorded). metadata.batch_id marks membership only -- the
        # finalizer is deliberately NOT stamped (enqueue_batch_atomic's
        # deadlock-prevention doctrine) -- so a finalizer retry reopens
        # nothing. Guarded on the terminal statuses, the reopen is
        # idempotent: a second retry of an already-'active' batch's member
        # is a no-op here.
        retry_job=f"""\
WITH retried AS (
    UPDATE "{s}".jobs
    SET status = 'pending',
        max_attempts = LEAST(GREATEST(max_attempts, attempt + 1), 32767),
        cancel_phase = 0,
        cancel_requested_at = NULL,
        error_class = NULL,
        error_message = NULL,
        error_traceback = NULL,
        scheduled_at = clock_timestamp(),
        finished_at = NULL,
        result = NULL,
        result_size_bytes = NULL,
        result_expires_at = NULL
    WHERE id = $1
      AND status IN ('failed', 'crashed', 'cancelled')
      -- The retry must leave the row budget-eligible: the raised ceiling
      -- has to exceed the spent attempt. It always does except at the
      -- smallint bound (attempt = 32767), where raising is impossible --
      -- refuse there rather than re-pend an unclaimable-without-overflow
      -- row.
      AND LEAST(GREATEST(max_attempts, attempt + 1), 32767) > attempt
    RETURNING id, metadata->>'batch_id' AS batch_id
),
reopened AS (
    UPDATE "{s}".batches
    SET status = 'active', completed_at = NULL
    WHERE id = (SELECT batch_id::uuid FROM retried WHERE batch_id IS NOT NULL)
      AND status IN ('complete', 'aborted')
    RETURNING id
)
SELECT id, batch_id, EXISTS (SELECT 1 FROM reopened) AS reopened_batch FROM retried""",
        # ── COPY FROM column lists ─────────────────────────────────
        copy_from_columns=COPY_FROM_COLUMNS,
        copy_enqueue_columns=COPY_ENQUEUE_COLUMNS,
    )
