"""Pre-rendered SQL template bundle for PostgresBackend.

Schema identifier is baked into pre-rendered SQL strings at render time.
All user-supplied values use asyncpg ``$N`` positional parameter binding ,
no f-string interpolation of user data.

The schema identifier is validated against ``_IDENT_RE`` before formatting
(asyncpg cannot bind identifiers, so the schema is interpolated as a
validated string constant).
"""

from dataclasses import dataclass
from typing import Final

from taskq.backend._dispatch_sql import (
    DISPATCH_CLAIMABLE_PROBE_SQL,
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_SQL,
)
from taskq.backend._sql import (
    CANCEL_ESCALATION_SQL,
    INSERT_EVENT_SQL,
    POLL_CANCEL_FLAGS_SQL,
    WAKE_NOTIFY_SQL,
)
from taskq.backend._sql_fragments import (
    ATTEMPT_REFUND_SQL,
    DEADLINE_EXCEEDED_MESSAGE,
    DEADLINE_RETRY_EXCEEDED_MESSAGE,
    JOB_FENCE_BOUND_SQL,
    JOB_FENCE_SQL,
    MIN_DEFERRAL_INTERVAL_SQL,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    CANCEL_ORIGIN_ABANDONED,
    CANCEL_ORIGIN_COOPERATIVE,
    CANCEL_ORIGIN_FORCED,
    CANCEL_ORIGIN_PENDING,
    CANCEL_ORIGIN_UNREQUESTED,
    ERROR_CLASS_DEADLINE_EXCEEDED,
    ERROR_CLASS_MAX_ATTEMPTS_EXCEEDED,
)

__all__ = ["SqlTemplates", "render"]

# COPY FROM column list, schema-independent, constant across all backends.
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
    "interrupt_count",
    "claim_epoch",
    "retry_base_seconds",
    "retry_cap_seconds",
    "retry_backoff",
    "retry_jitter",
    "assignment_routed",
)

# Column list for the enqueue COPY path only.  Every omitted column is
# either stamped/decided by the post-COPY fixup UPDATE
# (enqueue_batch_fast_fixup) from the server clock, never the caller's
# Python clock, or carries a DDL default the COPY lets apply
# (created_at/scheduled_at now(), NULL schedule_to_close /
# result_expires_at, the zero-defaulted denial counters, and
# claim_epoch, which only the dispatch claim ever assigns, an enqueued
# row is born at epoch 0).  ``status`` is NOT omitted: the
# COPY writes COPY_ENQUEUE_STATUS explicitly so the INSERT trigger never
# fires for a row whose runnability the fixup has not yet decided (see
# enqueue_batch_fast_fixup).  COPY_FROM_COLUMNS stays intact: it is
# shared by the archive CTE column lists in worker/_leader_shared.py.
_COPY_ENQUEUE_OMITTED: Final[frozenset[str]] = frozenset(
    {
        "created_at",
        "scheduled_at",
        "schedule_to_close",
        "result_expires_at",
        "snooze_count",
        "rate_limit_blocked_count",
        "interrupt_count",
        "claim_epoch",
    }
)
COPY_ENQUEUE_COLUMNS: Final[tuple[str, ...]] = tuple(
    c for c in COPY_FROM_COLUMNS if c not in _COPY_ENQUEUE_OMITTED
)

# The status every COPY row lands with. The fixup UPDATE decides each
# row's real status from the server clock afterwards, inside the same
# transaction; landing as 'scheduled' (never dispatchable, never woken)
# keeps tr_notify_job_insert, WHEN (NEW.status = 'pending'), INSERT only
# , from waking the fleet for rows the fixup then defers, the invariant
# migration 01.00.14_01 documents. The wake for the rows the fixup makes
# runnable is the fixup's own, issued once and only when it made any.
COPY_ENQUEUE_STATUS: Final[str] = "scheduled"

# The fragments below (deadline messages, terminal-write fence, attempt
# refund, deferral floor) are single-sourced in taskq/backend/_sql_fragments.py,
# the driver-free module the in-memory twins read; this bundle interpolates
# them by name so the rendered statements and the twins cannot drift.


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
    mark_interrupted: str

    # ── Shared INSERT templates ────────────────────────────────────
    insert_attempt_explicit: str
    insert_event: str

    # ── Owner check ────────────────────────────────────────────────
    select_owner: str

    # ── Cancel-path UPDATE statements ──────────────────────────────
    cancel_pending_scheduled: str
    cancel_running: str
    cancel_request: str
    cancel_escalation: str

    # ── Enqueue SQL templates ──────────────────────────────────────
    enqueue: str
    enqueue_unique_for_preflight: str
    singleton_preflight: str
    enqueue_max_pending_count: str
    enqueue_select_by_key: str
    wake_notify: str
    enqueue_batch: str
    enqueue_batch_fetch_existing: str
    enqueue_batch_fetch_singleton_blockers: str
    enqueue_batch_fast_fixup: str

    # ── Read SQL templates ─────────────────────────────────────────
    get_job: str
    get_archived_job: str
    get_attempts: str
    get_archived_attempts: str
    poll_cancel_flags: str

    # ── Dispatch SQL templates ─────────────────────────────────────
    dispatch_strict_fifo: str
    dispatch_round_robin: str
    dispatch_claimable_probe: str

    # ── Static read SQL ────────────────────────────────────────────
    get_events: str
    poll_reclaim_events: str
    check_reclaim_visibility_risk: str
    event_prune_watermark: str
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
        # exactly the contracts the three-statement versions had, only
        # the round trips collapsed.
        #
        # All terminal writes use clock_timestamp(), not now(), for
        # every timestamp computed in the SET clause. now() is frozen at
        # transaction start; clock_timestamp() is the wall-clock time the
        # statement executes. In the LOOP-scope transactional path
        # (mark_succeeded_with_conn, and any future _with_conn variant)
        # now() would be the actor's start, not completion, the same
        # bug class as queue-time expiry pinning. Even terminal writes
        # that currently only run through the pool's own short
        # transaction (where now() ≈ clock_timestamp()) use
        # clock_timestamp() for consistency: a future _with_conn variant
        # would inherit the correct time base without a migration.
        #
        # result_expires_at resolution, first non-NULL wins: the stored
        # (operator-owned) result_ttl applied at completion; then the
        # caller-supplied fallback ($7, the @actor literal the SQL cannot
        # see, also applied at completion, so a long-queued job does not
        # complete already expired); then the enqueue-time value.
        #
        # ATTEMPT-EPOCH FENCING: every worker-fenced terminal template
        # carries one conjunct deeper than the worker fence ,
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
        # call site; a mismatched epoch, a stale handler, or a caller
        # that cannot present one ($k IS NULL never satisfies the
        # equality), makes the UPDATE match no row and the write no-ops
        # through the same machinery as the worker fence (rowcount 0 →
        # False / WorkerOwnershipMismatch / "noop", no publish).
        #
        # CLAIM-EPOCH FENCING: every fenced template carries one conjunct
        # deeper still, ``claim_epoch = $m``, the writer's own claim view
        # (JobRow.claim_epoch, threaded beside attempt from every call
        # site). Attempt saturates at the smallint ceiling (the claim's
        # LEAST clamp, _dispatch_sql.py); the epoch does not, so a stale
        # handler and the live one can never share a fence. NULL never
        # satisfies the equality, the same cannot-prove-it doctrine the
        # attempt conjunct applies. See 01.00.18_02_pre_claim_epoch.sql
        # for the invariant.
        #
        # duration_ms is computed IN the statement from the same
        # database-written timestamp pair Python used to receive and
        # multiply back, but server-side, with exact numeric arithmetic
        # instead of Python's float path: values can differ from the old
        # Python computation by 1ms on exactly-whole-millisecond
        # boundaries (where the float product drifted just below the
        # integer), and the server-side values are the strictly more
        # accurate ones.  trunc() keeps the same
        # truncation-toward-zero Python's int() had (a bare ::int cast
        # rounds to nearest, a silent off-by-one against every
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
        progress_seq = GREATEST(progress_seq, $5),
        progress_state = CASE WHEN $6::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $6::jsonb ELSE progress_state END
    WHERE {JOB_FENCE_BOUND_SQL.format(attempt_bind=8, epoch_bind=9)}
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
    -- A claim-clamped attempt number repeats at the smallint ceiling
    -- (dispatch saturates its increment there): keep the first record of
    -- the number, never roll the terminal transition back on a PK
    -- collision (the deadline sweep's insert carries the same doctrine).
    ON CONFLICT (job_id, attempt) DO NOTHING
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
        progress_seq = GREATEST(progress_seq, $6),
        progress_state = CASE WHEN $7::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $7::jsonb ELSE progress_state END
    WHERE {JOB_FENCE_BOUND_SQL.format(attempt_bind=8, epoch_bind=9)}
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
    -- A claim-clamped attempt number repeats at the smallint ceiling
    -- (dispatch saturates its increment there): keep the first record of
    -- the number, never roll the terminal transition back on a PK
    -- collision (the deadline sweep's insert carries the same doctrine).
    ON CONFLICT (job_id, attempt) DO NOTHING
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
        # floored at MIN_DEFERRAL_INTERVAL via the params CTE's GREATEST ,
        # see mark_snoozed's params comment for the monopolisation hazard
        # an unfloored requeue arm is: a failure-retry decision must never
        # requeue below the deferral floor, the same bound the deferral
        # arms and the in-memory twin's _mark_failed_or_retry apply) is
        # applied by the SERVER clock (scheduled_at = clock_timestamp() +
        # effective_delay; the status derives from the effective delay
        # alone), and the schedule_to_close deadline is arbitrated in the
        # same statement, clock_timestamp() + effective_delay <=
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
        # TERMINAL arms deliberately keep the cancel columns, they are the
        # audit trail of why the job ended, and mark_abandoned's
        # `cancel_phase = 2` guard reads them.
        #
        # THE CANCEL FENCE (stated once here; the arm comments below only
        # point back): both arms carry `cancel_phase = 0` because BOTH
        # reset or overwrite state a cancel in flight owns - the retried
        # arm resets the cancel columns, the deadline_failed arm stamps
        # the DeadlineExceeded marker whose hooks must not fire on a
        # cancel in flight (the three deferral templates route this exact
        # shape to a deadline_cancelled arm; a retry has no cancelled row
        # of its own to write). A phase-carrying row therefore matches NO
        # arm: the write no-ops, the caller's WorkerOwnershipMismatch
        # reads back as the handler's no-op, and the row stays 'running'
        # carrying its phase for the cancel ladder to terminalise - the
        # same routing every fenced-out deferral takes. On a clean row
        # both conjuncts are trivially true and the retry semantics are
        # unchanged.
        #
        # Two consequences of that no-op routing, stated once:
        # - Evidence: the fenced-out retryable failure is recorded
        #   nowhere on the row - no job_attempts insert, no error_class
        #   stamp - the same gap the sibling deferral fences' noop
        #   carries. While fenced, the failure evidence lives only in
        #   the worker's logs; the cancel ladder's terminal write is the
        #   row's only record of why it ended.
        # - Liveness: a fenced-out row PAST its schedule_to_close
        #   survives 'running' past its deadline. Correctness depends on
        #   the cancel ladder (POLL_CANCEL_FLAGS_SQL selects
        #   status='running' rows by locked_by_worker) firing while the
        #   worker renews the lease; if the worker dies first, the
        #   crash-reclaim sweep's cancel branch terminalises the row
        #   instead.
        mark_retry=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           -- The failure-retry floor, the same bound the deferral arms
           -- pin (mark_snoozed's params comment): a sub-floor requeue
           -- delay parks the job at the head of the dispatch order and
           -- cycles a worker slot at claim/run/fail/retry round-trip
           -- rate. effective_delay is the arm's SINGLE delay, status,
           -- scheduled_at and every deadline comparison read it, so the
           -- retried and deadline arms partition exactly.
           GREATEST($3::interval, {MIN_DEFERRAL_INTERVAL_SQL}) AS effective_delay,
           $9::int AS attempt,
           $10::bigint AS claim_epoch
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
        -- A failure retry returns a claimed row to the pending pool, so
        -- it routes by the actor's current assignment from here on (the
        -- routing contract in taskq/backend/_dispatch_sql.py).
        assignment_routed = true,
        error_class = $4,
        error_message = $5,
        error_traceback = $6,
        progress_seq = GREATEST(j.progress_seq, $7),
        progress_state = CASE WHEN $8::jsonb IS NOT NULL
                              THEN COALESCE(j.progress_state, '{{}}'::jsonb) || $8::jsonb
                              ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
{JOB_FENCE_SQL}
      -- The cancel fence (see the mark_retry header comment above):
      -- without it the cancel_phase / cancel_requested_at resets below
      -- launder an operator cancel in flight and the job runs again.
      AND j.cancel_phase = 0
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT effective_delay FROM params) <= j.schedule_to_close)
    RETURNING j.*, 'retried'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = '{ERROR_CLASS_DEADLINE_EXCEEDED}',
        error_message = '{DEADLINE_RETRY_EXCEEDED_MESSAGE}',
        error_traceback = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = GREATEST(j.progress_seq, $7),
        progress_state = CASE WHEN $8::jsonb IS NOT NULL
                              THEN COALESCE(j.progress_state, '{{}}'::jsonb) || $8::jsonb
                              ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
{JOB_FENCE_SQL}
      -- The cancel fence (see the mark_retry header comment above):
      -- without it the DeadlineExceeded stamp below fires on a cancel
      -- in flight.
      AND j.cancel_phase = 0
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
           -- so the attempt's end is the arm's own now_ts, mirroring the
           -- Python that read rec["now_ts"] off the same RETURNING.
           trunc(EXTRACT(EPOCH FROM (r.now_ts - r.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM retried r
    -- A claim-clamped attempt number repeats at the smallint ceiling:
    -- keep the first record, never roll the transition back (see the
    -- mark_succeeded insert's comment).
    ON CONFLICT (job_id, attempt) DO NOTHING
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
           '{ERROR_CLASS_DEADLINE_EXCEEDED}', '{DEADLINE_RETRY_EXCEEDED_MESSAGE}', NULL,
           -- Terminal arm: duration reads the arm's finished_at, not now_ts.
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_failed d
    -- A claim-clamped attempt number repeats at the smallint ceiling:
    -- keep the first record, never roll the transition back (see the
    -- mark_succeeded insert's comment).
    ON CONFLICT (job_id, attempt) DO NOTHING
),
deadline_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                              'error_class', '{ERROR_CLASS_DEADLINE_EXCEEDED}',
                              'worker_id', $2::text)
    FROM deadline_failed d
)
SELECT * FROM retried UNION ALL SELECT * FROM deadline_failed""",
        # Every terminal cancel path records its origin the way every
        # terminal failure path records its reason: error_class on the
        # row, on the attempt, and in the state_change detail. Three
        # cancelled rows read side by side then say which actor yielded,
        # which had to be taken away, which never ran, and which the
        # worker's runtime cancelled on its own. mark_cancelled splits
        # its marker on the row's evidence: an actor that stopped while
        # still only ASKED (phase 1, a request on the row) stopped
        # cooperatively; one that had to be interrupted at phase 2 was
        # forced; and a row carrying NEITHER evidence (phase 0, no
        # request) was cancelled by the worker's runtime itself, a
        # sibling crash or an actor self cancel: stamping it
        # cooperatively would forge an operator request that never
        # existed. mark_abandoned and cancel_pending_scheduled stamp
        # their own origins. The SET clause owns the choice; the attempt
        # row and event detail read it back off upd so the three writes
        # can never disagree. The phase-0/no-request arm is reachable
        # from two writers, neither of which stamps a request: the
        # consumer's CancelledError handler for a cancellation no
        # controller stamped, and the stub consumer loop's terminal
        # write (worker/run.py's _stub_terminal_write, which cancels
        # without a request by construction). Every request-carrying
        # writer (cancel_running) stamps cancel_requested_at and phase 1
        # together, so ``cancel_requested_at IS NOT NULL`` is exactly the
        # operator-request evidence.
        mark_cancelled=f"""\
WITH upd AS (
    UPDATE "{s}".jobs
    SET status = 'cancelled',
        finished_at = clock_timestamp(),
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        error_class = CASE WHEN cancel_phase = 2 THEN '{CANCEL_ORIGIN_FORCED}'
                           WHEN cancel_requested_at IS NOT NULL THEN '{CANCEL_ORIGIN_COOPERATIVE}'
                           ELSE '{CANCEL_ORIGIN_UNREQUESTED}' END,
        progress_seq = GREATEST(progress_seq, $3),
        progress_state = CASE WHEN $4::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $4::jsonb ELSE progress_state END
    WHERE {JOB_FENCE_BOUND_SQL.format(attempt_bind=5, epoch_bind=6)}
    RETURNING *
), holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
), att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT upd.id, upd.attempt, upd.started_at, clock_timestamp(), 'cancelled',
           upd.error_class, NULL, NULL,
           trunc(EXTRACT(EPOCH FROM (upd.finished_at - upd.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM upd
    -- A claim-clamped attempt number repeats at the smallint ceiling
    -- (dispatch saturates its increment there): keep the first record of
    -- the number, never roll the terminal transition back on a PK
    -- collision (the deadline sweep's insert carries the same doctrine).
    ON CONFLICT (job_id, attempt) DO NOTHING
), evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT upd.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'cancelled',
                              'error_class', upd.error_class,
                              'worker_id', $2::text)
    FROM upd
)
SELECT * FROM upd""",
        mark_abandoned=f"""\
WITH upd AS (
    UPDATE "{s}".jobs
    SET status = 'abandoned',
        finished_at = clock_timestamp(),
        error_class = '{CANCEL_ORIGIN_ABANDONED}',
        progress_seq = GREATEST(progress_seq, $2),
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
           '{CANCEL_ORIGIN_ABANDONED}', NULL, NULL,
           trunc(EXTRACT(EPOCH FROM (upd.finished_at - upd.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM upd
    -- A claim-clamped attempt number repeats at the smallint ceiling
    -- (dispatch saturates its increment there): keep the first record of
    -- the number, never roll the terminal transition back on a PK
    -- collision (the deadline sweep's insert carries the same doctrine).
    ON CONFLICT (job_id, attempt) DO NOTHING
), evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT upd.id, clock_timestamp(), 'state_change',
           jsonb_strip_nulls(jsonb_build_object('from_state', 'running', 'to_state', 'abandoned',
                                                'error_class', '{CANCEL_ORIGIN_ABANDONED}',
                                                'worker_id', upd.locked_by_worker::text))
    FROM upd
)
SELECT * FROM upd""",
        # A non-consuming deferral never spends retry budget, and the
        # ceiling is never a counter, no arm here raises max_attempts.
        # Every deferral shape REFUNDS the claim's attempt increment:
        # attempt - 1, floored at 0.  Dispatch stamped attempt = attempt
        # + 1 when it claimed the row; no actor ran, so the increment is
        # returned and the gap `max_attempts - attempt` is exactly what
        # it was before the claim.  A job can therefore defer
        # indefinitely: attempt oscillates between N and N+1 and never
        # walks toward the smallint ceiling.  The ceiling itself is
        # immutable here, a deferral restores the attempt it borrowed
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
        # job waited, a load-dependent, unreproducible retry policy ,
        # and would let a queue or rate-limit misconfiguration terminate
        # work that never ran.  A denied job reschedules until capacity
        # frees; its ONLY terminal exit is its own schedule_to_close,
        # reached through the ordinary deadline arm below.  Unbounded
        # rescheduling stays affordable precisely because a deferral
        # mints no per-occurrence rows: contention is carried by the
        # aggregated counters on the row (snooze_count /
        # rate_limit_blocked_count, keyed by $7) and by OTEL, so
        # sustained saturation costs one counter bump per cycle instead
        # of a table's worth of history (measured under an earlier
        # per-denial shape: one job denied 1,014 times, 6.5M job_events
        # rows).
        #
        # Two arms, exhaustive and mutually exclusive over every fenced
        # row:
        #   snoozed        , the reschedule point still fits inside
        #                     schedule_to_close, or there is none;
        #   deadline_failed, the reschedule point passes
        #                     schedule_to_close.
        # denial_reason never branches this statement: 'capacity' (the
        # store answered "full") and 'unavailable' (the store could not
        # answer) are both admission backpressure about a job whose actor
        # never ran, and take the identical non-consuming shape. It rides
        # the $9 bind only so the deadline arm's terminal event can name
        # WHICH starvation ended a perpetually-denied job (the params CTE
        # comment); a non-denial deferral's event detail is unchanged.
        #
        # The refund revisits attempt numbers, which is collision-safe:
        # a non-terminal snooze/denial writes NO job_attempts/job_events
        # rows, it is admission control, not an execution, so no writer
        # ever lands on a revisited PK (job_id, attempt).  The deadline
        # arm writes its rows uniformly with every other terminal
        # transition, at the attempt number dispatch stamped and no
        # refund has returned.  There is deliberately no budget arm: the
        # retry ceiling has exactly one enforcement point, a real
        # execution's terminal write, because only an execution spends
        # budget; admission control never does.
        #
        # job_attempts PK hazard for the deadline arm: it inserts at
        # (job_id, attempt) WITHOUT dispatch having advanced attempt, so
        # the insert is safe only if no row at that key can already
        # exist.  The fence (status='running' AND locked_by_worker=$2 AND
        # attempt=$8) guarantees the job has been running-owned by this
        # worker at this attempt epoch since dispatch stamped attempt=N.
        # Every writer at (job, N) ends that running window first: the
        # terminal mark_* writes transition the row out of 'running', and
        # sweep-1's reclaim, the only writer that acts on a running row
        # this worker no longer owns, either terminalises or hands the
        # job back, whose row at N is followed by a dispatch increment to
        # N+1 before any snooze caller can run again.  Two statements
        # racing on the same running row serialise on the row lock and
        # the loser's fence re-check finds status no longer 'running', so
        # exactly one writer per (job, N) can ever commit.  The one
        # cross-epoch revisitor is gone: retry_job keeps attempt
        # monotonic (it raises the max_attempts ceiling instead of
        # resetting the counter, see the retry_job template's comment).
        mark_snoozed=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           -- A non-consuming deferral reschedules at least
           -- MIN_DEFERRAL_INTERVAL out (the GREATEST below): a zero/now
           -- delay would park the job 'pending' at clock_timestamp() at
           -- the head of the dispatch order (ORDER BY scheduled_at),
           -- instantly re-claimable, one claim/refund round trip per
           -- cycle monopolising a worker slot. A non-future snooze must
           -- be rejected: a zero delay would cause immediate re-claim and
           -- burn worker slots. The consuming arms keep the raw delay: an
           -- immediate consuming retry is a real execution, bounded by the
           -- budget it spends. effective_delay is the arm's SINGLE delay ,
           -- status, scheduled_at and every deadline comparison read it,
           -- so the snoozed and deadline arms partition exactly (a row can
           -- never match neither).
            GREATEST($3::interval, {MIN_DEFERRAL_INTERVAL_SQL}) AS effective_delay,
            $4::jsonb AS metadata_update,
            $5::int AS progress_seq,
            $6::jsonb AS progress_state,
            $8::int AS attempt,
            -- The denial class the caller reported for THIS deferral (the
            -- bounded DenialReason set). No arm branches on it, a denial
            -- takes the identical non-consuming path whatever the reason;
            -- the deadline arm's event below reads it so the terminal
            -- record of a starved-out job names WHICH starvation it was
            -- (saturation vs a store outage), the observability the row
            -- counters alone cannot carry.
            $9::text AS denial_reason,
            $10::bigint AS claim_epoch
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
        -- A deferral returns a claimed row to the pending pool, so it
        -- routes by the actor's current assignment from here on (the
        -- routing contract in taskq/backend/_dispatch_sql.py).
        assignment_routed = true,
        -- Every arm of this statement refunds the claim's attempt
        -- increment. An actor-requested deferral did not execute, and
        -- neither did an admission denial: no handler ran, so nothing
        -- may be charged to the budget the operator sized for real
        -- runs. Charging denials would make how many real retries a job
        -- gets depend on how saturated the bucket was while it waited.
        -- The refund expression is the shared ATTEMPT_REFUND_SQL
        -- fragment (module header).
        attempt = {ATTEMPT_REFUND_SQL},
        snooze_count = CASE WHEN $7::text = 'snoozed' THEN j.snooze_count + 1 ELSE j.snooze_count END,
        rate_limit_blocked_count = CASE WHEN $7::text IN ('reservation_denied', 'rate_limit_denied') THEN j.rate_limit_blocked_count + 1 ELSE j.rate_limit_blocked_count END,
        metadata = j.metadata || COALESCE((SELECT metadata_update FROM params), '{{}}'::jsonb),
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      -- The cancel fence (the mark_interrupted release arm's conjunct):
      -- an operator cancel in flight WINS over the deferral. A row
      -- carrying a cancel phase must never match this arm; the
      -- cancel_phase = 0 / cancel_requested_at = NULL resets above would
      -- launder the operator's request mid-flight (the re-pended row
      -- would read as never-cancel-requested, and the bulk-cancel drain
      -- would report the same id from two arms). The fenced-out row stays
      -- 'running' carrying its phase, the caller reads back "noop", and
      -- the worker's cancel ladder terminalises it. On a clean row the
      -- conjunct is trivially true and the deferral semantics are
      -- unchanged.
      AND j.cancel_phase = 0
      -- The reschedule point is the ONLY admission condition: a deferral
      -- that never ran spends nothing, so nothing but the job's own
      -- deadline can refuse it.
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT effective_delay FROM params) <= j.schedule_to_close)
    RETURNING j.*, 'snoozed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_cancelled AS (
    -- Operator intent outranks the deadline, the same cancel-first
    -- arbitration _SWEEP_1_SQL's CASE carries (and the isolate template
    -- mirrors branch-for-branch): a row carrying a cancel phase whose
    -- schedule_to_close lapses at deferral time terminalises 'cancelled'
    -- here, never 'failed:DeadlineExceeded': the pre-fix shape matched
    -- the deadline arm below and failed a row the operator had already
    -- claimed, firing DeadlineExceeded hooks and error reports on a
    -- cancel in flight. This arm runs BEFORE the failure arm; the
    -- failure arm reads cancel_phase = 0 by construction (and re-guards
    -- with NOT EXISTS here, defence-in-depth against direct-SQL shapes).
    -- The SET mirrors mark_cancelled: the cancel-origin marker on the
    -- row and attempt (phase 2 was forced, phase 1 cooperative), no
    -- DeadlineExceeded stamp (the record of what happened to this row
    -- is the in-flight request it preserves), and the cancel columns
    -- survive untouched as the audit trail, the doctrine every
    -- terminal cancel path carries. The lease-clear trio matches the
    -- deadline arm below: the row's claim is released either way.
    UPDATE "{s}".jobs j
    SET status = 'cancelled',
        finished_at = clock_timestamp(),
        error_class = CASE WHEN j.cancel_phase = 2 THEN '{CANCEL_ORIGIN_FORCED}'
                           ELSE '{CANCEL_ORIGIN_COOPERATIVE}' END,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      AND j.cancel_phase != 0
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT effective_delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, 'cancelled'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = '{ERROR_CLASS_DEADLINE_EXCEEDED}',
        error_message = '{DEADLINE_EXCEEDED_MESSAGE}',
        error_traceback = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        -- The denial that ran the job out of road still happened to it,
        -- and with no per-occurrence rows the aggregate is its only
        -- record: counting it here makes the terminal row show that the
        -- deadline was reached WHILE the job was starving for admission,
        -- rather than making that last denial vanish.  An actor-requested
        -- deferral is NOT counted here, snooze_count tallies deferrals
        -- the job actually took, and this one was rejected outright.
        rate_limit_blocked_count = CASE WHEN $7::text IN ('reservation_denied', 'rate_limit_denied') THEN j.rate_limit_blocked_count + 1 ELSE j.rate_limit_blocked_count END,
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      -- The cancelled arm above owns every phase-carrying row (the
      -- cancel-first ordering); this arm is the clean row's exit. Both
      -- conjuncts are no-ops given the arm order, kept as
      -- defence-in-depth against direct-SQL shapes.
      AND j.cancel_phase = 0
      AND NOT EXISTS (SELECT 1 FROM deadline_cancelled)
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT effective_delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, 'failed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
),
cancelled_att AS (
    -- The cancelled arm's attempt row: outcome 'cancelled' with the
    -- cancel-origin marker the UPDATE stamped, mirroring mark_cancelled's
    -- attempt shape, never a 'failed'/DeadlineExceeded record (the
    -- deadline did not fail this job, the operator's cancel decided it).
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT d.id, d.attempt, d.started_at, clock_timestamp(), 'cancelled',
           d.error_class, NULL, NULL,
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_cancelled d
    -- A claim-clamped attempt number repeats at the smallint ceiling:
    -- keep the first record, never roll the transition back (see the
    -- mark_succeeded insert's comment).
    ON CONFLICT (job_id, attempt) DO NOTHING
),
cancelled_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'cancelled',
                              'error_class', d.error_class,
                              'worker_id', $2::text)
    FROM deadline_cancelled d
),
deadline_att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT d.id, d.attempt, d.started_at, clock_timestamp(), 'failed',
           '{ERROR_CLASS_DEADLINE_EXCEEDED}', '{DEADLINE_EXCEEDED_MESSAGE}', NULL,
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_failed d
    -- A claim-clamped attempt number repeats at the smallint ceiling:
    -- keep the first record, never roll the transition back (see the
    -- mark_succeeded insert's comment).
    ON CONFLICT (job_id, attempt) DO NOTHING
),
deadline_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           -- The conditional key is built explicitly, no
           -- jsonb_strip_nulls wrap: a wrap would silently drop any
           -- future legitimately-NULL key this object gains. The
           -- non-denial exit concatenates the empty object, so its
           -- detail shape is unchanged (no denial_reason key); a
           -- denial-keyed row's terminal event names the denial class
           -- the caller reported ($9), so an operator reading the event
           -- sees which starvation killed the job without joining the
           -- counter columns.
           jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                              'error_class', '{ERROR_CLASS_DEADLINE_EXCEEDED}',
                              'worker_id', $2::text)
           || CASE WHEN $7::text IN ('reservation_denied', 'rate_limit_denied')
                   THEN jsonb_build_object('denial_reason',
                                           (SELECT denial_reason FROM params))
                   ELSE '{{}}'::jsonb END
    FROM deadline_failed d
)
SELECT * FROM snoozed
UNION ALL SELECT * FROM deadline_cancelled
 UNION ALL SELECT * FROM deadline_failed""",
        mark_retry_after_consume_true=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           $3::interval AS delay,
           $4::int AS progress_seq,
           $5::jsonb AS progress_state,
           $6::int AS attempt,
           $7::bigint AS claim_epoch
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
        -- A deferral returns a claimed row to the pending pool, so it
        -- routes by the actor's current assignment from here on (the
        -- routing contract in taskq/backend/_dispatch_sql.py).
        assignment_routed = true,
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      -- The cancel fence (the mark_interrupted release arm's conjunct):
      -- an operator cancel in flight WINS over the deferral. A row
      -- carrying a cancel phase must never match this arm; the
      -- cancel_phase = 0 / cancel_requested_at = NULL resets above would
      -- launder the operator's request mid-flight. The fenced-out row
      -- stays 'running' carrying its phase, the caller reads back
      -- "noop", and the worker's cancel ladder terminalises it. On a
      -- clean row the conjunct is trivially true and the budget
      -- semantics below are unchanged.
      AND j.cancel_phase = 0
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT delay FROM params) <= j.schedule_to_close)
      AND (j.retry_kind = 'indefinite'
           OR j.attempt < j.max_attempts)
    RETURNING j.*, j.attempt AS running_attempt, 'snoozed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_cancelled AS (
    -- Operator intent outranks the deadline AND the budget, the same
    -- cancel-first arbitration _SWEEP_1_SQL's CASE carries (and the
    -- isolate template mirrors branch-for-branch): a row carrying a
    -- cancel phase whose schedule_to_close lapses at deferral time
    -- terminalises 'cancelled' here, never
    -- 'failed:DeadlineExceeded'/'failed:MaxAttemptsExceeded': the
    -- pre-fix shape matched the budget/deadline arms below and failed a
    -- row the operator had already claimed, firing DeadlineExceeded and
    -- retry-exhausted hooks on a cancel in flight. This arm runs BEFORE
    -- both; those arms read cancel_phase = 0 by construction (and
    -- re-guard with NOT EXISTS here, defence-in-depth against
    -- direct-SQL shapes). The SET mirrors mark_cancelled: the
    -- cancel-origin marker on the row and attempt, no deadline/budget
    -- stamp (the record of what happened to this row is the in-flight
    -- request it preserves), and the cancel columns survive untouched
    -- as the audit trail. The lease-clear trio matches the sibling
    -- terminal arms: the row's claim is released either way.
    UPDATE "{s}".jobs j
    SET status = 'cancelled',
        finished_at = clock_timestamp(),
        error_class = CASE WHEN j.cancel_phase = 2 THEN '{CANCEL_ORIGIN_FORCED}'
                           ELSE '{CANCEL_ORIGIN_COOPERATIVE}' END,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      AND j.cancel_phase != 0
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, j.attempt AS running_attempt, 'cancelled'::text AS outcome_branch, clock_timestamp() AS now_ts
),
max_attempts_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = '{ERROR_CLASS_MAX_ATTEMPTS_EXCEEDED}',
        error_message = 'retry budget exhausted',
        error_traceback = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      -- Every non-indefinite kind exhausts here: a non_retryable job at
      -- budget under RetryAfter(consume_budget=True) has no other exit ,
      -- a 'transient'-only predicate left it matching no arm at all and
      -- the statement fell through to a silent reschedule.
      AND j.retry_kind <> 'indefinite'
      AND j.attempt >= j.max_attempts
      -- The cancelled arm above owns every phase-carrying row (the
      -- cancel-first ordering); this arm is the clean row's exit. Both
      -- conjuncts are no-ops given the arm order, kept as
      -- defence-in-depth against direct-SQL shapes.
      AND j.cancel_phase = 0
      AND NOT EXISTS (SELECT 1 FROM deadline_cancelled)
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT delay FROM params) <= j.schedule_to_close)
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, j.attempt AS running_attempt, 'max_attempts_failed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = '{ERROR_CLASS_DEADLINE_EXCEEDED}',
        error_message = '{DEADLINE_EXCEEDED_MESSAGE}',
        error_traceback = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT delay FROM params) > j.schedule_to_close
      -- The cancelled arm owns every phase-carrying row (the
      -- cancel-first ordering); this arm is the clean row's exit. Both
      -- conjuncts are no-ops given the arm order, kept as
      -- defence-in-depth against direct-SQL shapes.
      AND j.cancel_phase = 0
      AND NOT EXISTS (SELECT 1 FROM deadline_cancelled)
      AND NOT EXISTS (SELECT 1 FROM snoozed)
      AND NOT EXISTS (SELECT 1 FROM max_attempts_failed)
    RETURNING j.*, j.attempt AS running_attempt, 'deadline_failed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
),
cancelled_att AS (
    -- The cancelled arm's attempt row: outcome 'cancelled' with the
    -- cancel-origin marker the UPDATE stamped, mirroring mark_cancelled's
    -- attempt shape, never a 'failed' record (neither the deadline nor
    -- the budget failed this job, the operator's cancel decided it).
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT d.id, d.attempt, d.started_at, clock_timestamp(), 'cancelled',
           d.error_class, NULL, NULL,
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_cancelled d
    -- A claim-clamped attempt number repeats at the smallint ceiling:
    -- keep the first record, never roll the transition back (see the
    -- mark_succeeded insert's comment).
    ON CONFLICT (job_id, attempt) DO NOTHING
),
cancelled_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'cancelled',
                              'error_class', d.error_class,
                              'worker_id', $2::text)
    FROM deadline_cancelled d
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
    -- A claim-clamped attempt number repeats at the smallint ceiling:
    -- keep the first record, never roll the transition back (see the
    -- mark_succeeded insert's comment).
    ON CONFLICT (job_id, attempt) DO NOTHING
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
           '{ERROR_CLASS_MAX_ATTEMPTS_EXCEEDED}', 'retry budget exhausted', NULL,
           trunc(EXTRACT(EPOCH FROM (m.finished_at - m.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM max_attempts_failed m
    -- A claim-clamped attempt number repeats at the smallint ceiling:
    -- keep the first record, never roll the transition back (see the
    -- mark_succeeded insert's comment).
    ON CONFLICT (job_id, attempt) DO NOTHING
),
max_attempts_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT m.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                              'error_class', '{ERROR_CLASS_MAX_ATTEMPTS_EXCEEDED}',
                              'worker_id', $2::text)
    FROM max_attempts_failed m
),
deadline_att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT d.id, d.attempt, d.started_at, clock_timestamp(), 'failed',
           '{ERROR_CLASS_DEADLINE_EXCEEDED}', '{DEADLINE_EXCEEDED_MESSAGE}', NULL,
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_failed d
    -- A claim-clamped attempt number repeats at the smallint ceiling:
    -- keep the first record, never roll the transition back (see the
    -- mark_succeeded insert's comment).
    ON CONFLICT (job_id, attempt) DO NOTHING
),
deadline_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                              'error_class', '{ERROR_CLASS_DEADLINE_EXCEEDED}',
                              'worker_id', $2::text)
    FROM deadline_failed d
)
SELECT * FROM snoozed
UNION ALL SELECT * FROM deadline_cancelled
 UNION ALL SELECT * FROM max_attempts_failed
 UNION ALL SELECT * FROM deadline_failed""",
        # The RetryAfter(consume_budget=False) arm is an actor-requested
        # deferral, a server Retry-After honoured without spending
        # budget, so it carries the mark_snoozed 'snoozed' contract in
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
            GREATEST($3::interval, {MIN_DEFERRAL_INTERVAL_SQL}) AS effective_delay,
            $4::int AS progress_seq,
            $5::jsonb AS progress_state,
            $6::int AS attempt,
            $7::bigint AS claim_epoch
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
        -- A deferral returns a claimed row to the pending pool, so it
        -- routes by the actor's current assignment from here on (the
        -- routing contract in taskq/backend/_dispatch_sql.py).
        assignment_routed = true,
        attempt = {ATTEMPT_REFUND_SQL},
        snooze_count = j.snooze_count + 1,
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      -- The cancel fence (the mark_interrupted release arm's conjunct):
      -- an operator cancel in flight WINS over the deferral. A row
      -- carrying a cancel phase must never match this arm; the
      -- cancel_phase = 0 / cancel_requested_at = NULL resets above would
      -- launder the operator's request mid-flight. The fenced-out row
      -- stays 'running' carrying its phase, the caller reads back
      -- "noop", and the worker's cancel ladder terminalises it. On a
      -- clean row the conjunct is trivially true and the deferral
      -- semantics are unchanged.
      AND j.cancel_phase = 0
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT effective_delay FROM params) <= j.schedule_to_close)
    RETURNING j.*, 'snoozed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_cancelled AS (
    -- Operator intent outranks the deadline, the same cancel-first
    -- arbitration _SWEEP_1_SQL's CASE carries (and the isolate template
    -- mirrors branch-for-branch): a row carrying a cancel phase whose
    -- schedule_to_close lapses at deferral time terminalises 'cancelled'
    -- here, never 'failed:DeadlineExceeded': the pre-fix shape matched
    -- the deadline arm below and failed a row the operator had already
    -- claimed, firing DeadlineExceeded hooks and error reports on a
    -- cancel in flight. This arm runs BEFORE the failure arm; the
    -- failure arm reads cancel_phase = 0 by construction (and re-guards
    -- with NOT EXISTS here, defence-in-depth against direct-SQL shapes).
    -- The SET mirrors mark_cancelled: the cancel-origin marker on the
    -- row and attempt, no DeadlineExceeded stamp (the record of what
    -- happened to this row is the in-flight request it preserves), and
    -- the cancel columns survive untouched as the audit trail. The
    -- lease-clear trio matches the deadline arm below: the row's claim
    -- is released either way.
    UPDATE "{s}".jobs j
    SET status = 'cancelled',
        finished_at = clock_timestamp(),
        error_class = CASE WHEN j.cancel_phase = 2 THEN '{CANCEL_ORIGIN_FORCED}'
                           ELSE '{CANCEL_ORIGIN_COOPERATIVE}' END,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      AND j.cancel_phase != 0
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT effective_delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, 'cancelled'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = '{ERROR_CLASS_DEADLINE_EXCEEDED}',
        error_message = '{DEADLINE_EXCEEDED_MESSAGE}',
        error_traceback = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT effective_delay FROM params) > j.schedule_to_close
      -- The cancelled arm above owns every phase-carrying row (the
      -- cancel-first ordering); this arm is the clean row's exit. Both
      -- conjuncts are no-ops given the arm order, kept as
      -- defence-in-depth against direct-SQL shapes.
      AND j.cancel_phase = 0
      AND NOT EXISTS (SELECT 1 FROM deadline_cancelled)
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, 'deadline_failed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
),
cancelled_att AS (
    -- The cancelled arm's attempt row: outcome 'cancelled' with the
    -- cancel-origin marker the UPDATE stamped, mirroring mark_cancelled's
    -- attempt shape, never a 'failed'/DeadlineExceeded record (the
    -- deadline did not fail this job, the operator's cancel decided it).
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT d.id, d.attempt, d.started_at, clock_timestamp(), 'cancelled',
           d.error_class, NULL, NULL,
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_cancelled d
    -- A claim-clamped attempt number repeats at the smallint ceiling:
    -- keep the first record, never roll the transition back (see the
    -- mark_succeeded insert's comment).
    ON CONFLICT (job_id, attempt) DO NOTHING
),
cancelled_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'cancelled',
                              'error_class', d.error_class,
                              'worker_id', $2::text)
    FROM deadline_cancelled d
),
deadline_att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT d.id, d.attempt, d.started_at, clock_timestamp(), 'failed',
           '{ERROR_CLASS_DEADLINE_EXCEEDED}', '{DEADLINE_EXCEEDED_MESSAGE}', NULL,
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_failed d
    -- A claim-clamped attempt number repeats at the smallint ceiling:
    -- keep the first record, never roll the transition back (see the
    -- mark_succeeded insert's comment).
    ON CONFLICT (job_id, attempt) DO NOTHING
),
deadline_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                              'error_class', '{ERROR_CLASS_DEADLINE_EXCEEDED}',
                              'worker_id', $2::text)
    FROM deadline_failed d
)
SELECT * FROM snoozed
UNION ALL SELECT * FROM deadline_cancelled
 UNION ALL SELECT * FROM deadline_failed""",
        # mark_interrupted releases a RUNNING attempt the worker cannot
        # finish because the process is going away (graceful shutdown past
        # its graces). It is the non-terminal release of a *started*
        # attempt: the claim's increment is NOT refunded (an interruption
        # charges the attempt it ran; refunding it would re-create the
        # exact epoch the interrupted handler still holds, and its later
        # terminal write would land on the re-dispatched attempt; see the
        # ATTEMPT_REFUND_SQL module header), no job_attempts row is
        # written (an interruption is not an execution outcome; the same
        # reasoning as the snooze/denial arms above), one job_events
        # state_change with reason 'interrupted' records the transition,
        # and interrupt_count bumps on the row (the same row-counter
        # doctrine as snooze_count / rate_limit_blocked_count, 01.00.08_01).
        # The release is a soft stop: the stop is recognised by the
        # context's cancellation cause rather than by the error type, and
        # the interrupted row lands back 'available' with reason
        # 'interrupted' at the attempt it already spent.
        #
        # Two arms, exhaustive over every fenced row:
        #   released       , the release itself. hold > 0 parks the row
        #                     'scheduled' until the releasing process is
        #                     provably gone (a row released while its
        #                     coroutine may still be alive in this process
        #                     must not be claimable elsewhere until then);
        #                     hold = 0 lands 'pending' at the head of the
        #                     order, the row is genuinely free and the
        #                     actor is gone, so the non-consuming deferral
        #                     floor applies only to a real hold, never to
        #                     the zero case (an interrupted job is
        #                     claimable immediately).
        #   deadline_failed, the hold would push the row past its
        #                     schedule_to_close: the job fails on the
        #                     deadline like every deferral arm's deadline
        #                     exit, never parked past it into a state
        #                     nothing terminalises.
        #
        # The fence carries `cancel_phase = 0` beside the ownership and
        # attempt-epoch conjuncts: an operator cancel in flight WINS over
        # the infrastructure interruption (the call returns no row and the
        # caller routes to the cancel ladder). A row carrying
        # cancel_attempted_at terminalises as 'cancelled', never
        # 'available'; the fence is what keeps the infrastructure release
        # from laundering the operator's request. The cancel columns are reset on
        # release exactly as mark_retry's arm does (the next attempt must
        # not inherit a phase); the fence guarantees they were already 0,
        # so an operator's audit columns are never wiped.
        #
        # progress_seq is GREATEST-merged, not assigned: the release can
        # race the still-running actor's last flush, and a caller without
        # the coalesced buffer (the shutdown orchestrator passes 0) must
        # never regress the row's progress epoch.
        mark_interrupted=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           $3::int AS attempt,
           -- A positive hold is floored at the non-consuming deferral
           -- floor (the mark_snoozed params comment names the
           -- slot-monopolisation hazard); a zero hold stays zero so the
           -- release lands pending immediately.
           CASE WHEN $4::interval > interval '0'
                THEN GREATEST($4::interval, {MIN_DEFERRAL_INTERVAL_SQL})
                ELSE interval '0' END AS effective_hold,
           $5::int AS progress_seq,
           $6::jsonb AS progress_state,
           $7::bigint AS claim_epoch
),
released AS (
    UPDATE "{s}".jobs j
    SET status = CASE WHEN (SELECT effective_hold FROM params) > interval '0'
                      THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END,
        scheduled_at = clock_timestamp() + (SELECT effective_hold FROM params),
        finished_at = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        cancel_phase = 0,
        cancel_requested_at = NULL,
        -- An interruption hands the row back to the fleet, so it routes
        -- by the actor's current assignment from here on (the routing
        -- contract in taskq/backend/_dispatch_sql.py), the same
        -- re-pend class as the snooze/refund arms and the sweep.
        assignment_routed = true,
        -- Deliberately NO attempt refund (the snooze arms'
        -- ATTEMPT_REFUND_SQL): the attempt started executing, so its
        -- increment stands; a refund would re-create the epoch the
        -- interrupted handler still holds and let its zombie terminal
 -- write land on the re-dispatched attempt.
        interrupt_count = j.interrupt_count + 1,
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL
                              THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params)
                              ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      AND j.cancel_phase = 0
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT effective_hold FROM params) <= j.schedule_to_close)
    RETURNING j.*, 'released'::text AS outcome_branch, clock_timestamp() AS now_ts
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = clock_timestamp(),
        error_class = '{ERROR_CLASS_DEADLINE_EXCEEDED}',
        error_message = '{DEADLINE_EXCEEDED_MESSAGE}',
        error_traceback = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = GREATEST(j.progress_seq, (SELECT progress_seq FROM params)),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL
                              THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params)
                              ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      {JOB_FENCE_SQL}
      AND j.cancel_phase = 0
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT effective_hold FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM released)
    RETURNING j.*, 'deadline_failed'::text AS outcome_branch, clock_timestamp() AS now_ts
),
holder AS (
    SELECT id FROM "{s}".workers WHERE id = $2 FOR KEY SHARE
),
released_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT r.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', r.status::text,
                              'reason', 'interrupted',
                              'worker_id', $2::text,
                              'hold_seconds', EXTRACT(EPOCH FROM (SELECT effective_hold FROM params)))
    FROM released r
),
deadline_att AS (
    INSERT INTO "{s}".job_attempts
    (job_id, attempt, started_at, finished_at, outcome,
     error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
    SELECT d.id, d.attempt, d.started_at, clock_timestamp(), 'failed',
           '{ERROR_CLASS_DEADLINE_EXCEEDED}', '{DEADLINE_EXCEEDED_MESSAGE}', NULL,
           trunc(EXTRACT(EPOCH FROM (d.finished_at - d.started_at)) * 1000)::int,
           (SELECT id FROM holder), '{{}}'::jsonb
    FROM deadline_failed d
    -- A claim-clamped attempt number repeats at the smallint ceiling:
    -- keep the first record, never roll the transition back (see the
    -- mark_succeeded insert's comment).
    ON CONFLICT (job_id, attempt) DO NOTHING
),
deadline_evt AS (
    INSERT INTO "{s}".job_events
    (job_id, occurred_at, kind, detail)
    SELECT d.id, clock_timestamp(), 'state_change',
           jsonb_build_object('from_state', 'running', 'to_state', 'failed',
                              'error_class', '{ERROR_CLASS_DEADLINE_EXCEEDED}',
                              'worker_id', $2::text)
    FROM deadline_failed d
)
SELECT * FROM released
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
        (SELECT id FROM holder), $11::jsonb)
-- A claim-clamped attempt number repeats at the smallint ceiling: keep
-- the first record, never raise a PK collision (see the mark_succeeded
-- insert's comment).
ON CONFLICT (job_id, attempt) DO NOTHING""",
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
-- The cancel-origin marker goes on the ROW only: the pending→cancelled
-- state_change event detail keeps its {{from_state, to_state}} shape ,
-- a from_state of pending/scheduled already says the job never ran, and
-- the differential suite pins that detail exactly.
SET status = 'cancelled', finished_at = clock_timestamp(),
    error_class = '{CANCEL_ORIGIN_PENDING}'
FROM prev
WHERE "{s}".jobs.id = $1 AND "{s}".jobs.status IN ('pending', 'scheduled')
RETURNING prev.prev_status""",
        cancel_running=f"""\
UPDATE "{s}".jobs
SET cancel_requested_at = clock_timestamp(), cancel_phase = 1
WHERE id = $1 AND status = 'running' AND cancel_phase = 0
RETURNING locked_by_worker""",
        # write_cancel_request's SINGLE arbiter statement, fused for the
        # same reason every mark_* here is one statement. Its two-statement
        # predecessor — probe pending/scheduled, then arm the running
        # stamp — held NO row lock between the probe and the second
        # UPDATE, so a consumer re-queue (a deferral arm's snoozed CTE, a
        # failure retry's retried CTE) committing in that window escaped
        # BOTH arms: the probe read 'running' and matched nothing, the
        # running arm re-read 'scheduled' under READ COMMITTED and matched
        # nothing, and write_cancel_request returned False on a job the
        # operator had asked to cancel — no phase on the row, no
        # cancel_request event, nothing for the cancel ladder to see. The
        # operator's request VANISHED, the mirror image of the launder the
        # deferral fences close (there the same id is reported twice; here
        # it is reported never). The in-memory twin is a single-pass check
        # with no await between the arms and never had the window.
        #
        # The fusion closes it the way the terminal writes close theirs:
        # ONE statement, the row locked by the prev CTE's FOR UPDATE from
        # probe through UPDATE, the arm chosen by CASE on the LOCKED
        # observation. A re-queue either commits entirely before the probe
        # (the pending/scheduled arm sees it and terminalises with the
        # CancelledBeforeStart origin) or blocks on the lock and commits
        # entirely after the stamp (the row carries phase 1; the deferral
        # fence routes it to noop). No third ordering exists.
        #
        # Arms and their exact legacy shapes:
        # - pending/scheduled: terminalise 'cancelled', finished_at stamped,
        #   the cancel-origin marker on the ROW only (the state_change
        #   detail keeps its {from_state, to_state} shape, the
        #   differential corpus pins it), then one cancel_request event;
        # - running at phase 0: stamp cancel_requested_at + phase 1, one
        #   cancel_request event; the NOTIFY still fires from Python, off
        #   the RETURNING holder (pg_notify lives in the caller's
        #   transaction there, unchanged);
        # - terminal rows, abandoned rows, and rows already carrying a
        #   phase match nothing: the False contract is unchanged, and a
        #   False now means ONLY "there was nothing left to cancel".
        cancel_request=f"""\
WITH prev AS (
    SELECT status AS prev_status, cancel_phase
    FROM "{s}".jobs WHERE id = $1 FOR UPDATE
),
upd AS (
    UPDATE "{s}".jobs j
    SET status = CASE WHEN p.prev_status IN ('pending', 'scheduled')
                      THEN 'cancelled'::"{s}".job_status ELSE j.status END,
        finished_at = CASE WHEN p.prev_status IN ('pending', 'scheduled')
                           THEN clock_timestamp() ELSE j.finished_at END,
        error_class = CASE WHEN p.prev_status IN ('pending', 'scheduled')
                           THEN '{CANCEL_ORIGIN_PENDING}' ELSE j.error_class END,
        cancel_requested_at = CASE WHEN p.prev_status = 'running' AND j.cancel_phase = 0
                                   THEN clock_timestamp() ELSE j.cancel_requested_at END,
        cancel_phase = CASE WHEN p.prev_status = 'running' AND j.cancel_phase = 0
                            THEN 1 ELSE j.cancel_phase END
    FROM prev p
    WHERE j.id = $1
      AND (p.prev_status IN ('pending', 'scheduled')
           OR (p.prev_status = 'running' AND p.cancel_phase = 0))
    RETURNING j.*, p.prev_status AS prev_status
),
evt_ts AS (
    -- ONE clock_timestamp() evaluation for both event rows, so the
    -- (occurred_at, event_id) read order (get_events) never splits them.
    SELECT clock_timestamp() AS ts
),
events AS (
    INSERT INTO "{s}".job_events (job_id, occurred_at, kind, detail)
    -- ONE insert, not two data-modifying CTEs: the execution order of
    -- separate WITH DML statements is not specified, and the first board
    -- to run this fused statement watched real PostgreSQL emit the
    -- cancel_request row before the state_change row, a mirror divergence
    -- the two-statement form never produced (its two consecutive Python
    -- statements wrote state_change first, and the differential corpus
    -- pins that order). A single insert produces its rows in written
    -- order, so the bigserial event_id carries the stream order:
    -- state_change first, then cancel_request, the legacy observables
    -- exactly.
    SELECT u.id, ts, 'state_change',
           jsonb_build_object('from_state', u.prev_status, 'to_state', 'cancelled')
    FROM upd u CROSS JOIN evt_ts
    WHERE u.prev_status IN ('pending', 'scheduled')
    UNION ALL
    SELECT u.id, ts, 'cancel_request',
           -- jsonb_strip_nulls: a NULL reason omits the key, exactly the
           -- _insert_cancel_request_event shape the two-statement form
           -- wrote.
           jsonb_strip_nulls(jsonb_build_object('reason', $2::text))
    FROM upd u CROSS JOIN evt_ts
)
SELECT u.prev_status, u.locked_by_worker FROM upd u""",
        cancel_escalation=CANCEL_ESCALATION_SQL.format(schema=s),
        # ── Enqueue SQL templates ──────────────────────────────────
        # schedule_to_close is single-domain server-side on this arm: the
        # interval form anchors to clock_timestamp() (matching the previous
        # enqueue_with_interval behaviour), and a raw absolute datetime (the
        # deprecated caller-domain form) only applies when the interval is
        # NULL, clock_timestamp() + NULL::interval is NULL, so COALESCE
        # falls through to $22.  $22 is a NEW trailing slot (bound after
        # $21::text[]): $12 is start_to_close and must not be displaced.
        enqueue=f"""\
INSERT INTO "{s}".jobs
(id, actor, queue, identity_key, fairness_key,
 payload, payload_schema_ver, status, priority,
 max_attempts, retry_kind,
 schedule_to_close, start_to_close, heartbeat_timeout,
 scheduled_at,
 idempotency_scope, idempotency_key, trace_id, span_id, metadata, result_expires_at, tags,
 retry_base_seconds, retry_cap_seconds, retry_backoff, retry_jitter)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, CASE WHEN COALESCE($14, clock_timestamp()) > clock_timestamp() THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END, $8, $9, $10, COALESCE(clock_timestamp() + $11::interval, $22), $12, $13, COALESCE($14, clock_timestamp()), $15, $16, $17, $18, $19::jsonb, clock_timestamp() + $20::interval, $21::text[], $23, $24, $25, $26)
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
        # The wake for paths that re-pend a row by UPDATE (admin retry):
        # the jobs INSERT trigger covers every insert path, so no enqueue
        # path issues this.
        wake_notify=WAKE_NOTIFY_SQL,
        enqueue_batch=f"""\
INSERT INTO "{s}".jobs (
    id, actor, queue, identity_key, fairness_key,
    payload, payload_schema_ver,
    status, priority, attempt, max_attempts, retry_kind,
    schedule_to_close, start_to_close, heartbeat_timeout,
    scheduled_at, metadata, idempotency_scope, idempotency_key, trace_id, span_id,
    result_expires_at, tags,
    retry_base_seconds, retry_cap_seconds, retry_backoff, retry_jitter
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
    -- the single-row enqueue template and the COPY fixup, not now(),
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
    (SELECT COALESCE(array_agg(elem::text), '{{}}'::text[]) FROM jsonb_array_elements_text(t.tags_jsonb) AS elem),
    t.retry_base,
    t.retry_cap,
    t.retry_backoff,
    t.retry_jitter
FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[],
    $6::jsonb[], $7::int[],
    $8::int[], $9::int[], $10::text[],
    $11::interval[], $12::interval[], $13::interval[],
    $14::timestamptz[], $15::jsonb[], $16::text[], $17::text[], $18::text[], $19::text[],
    $20::interval[], $21::jsonb[], $22::timestamptz[],
    $23::float[], $24::float[], $25::text[], $26::float[]
) AS t(id, actor, queue, identity_key, fairness_key,
    payload, payload_schema_ver,
    priority, max_attempts, retry_kind,
    stc_interval, start_to_close, heartbeat_timeout,
    scheduled_at, metadata, idempotency_scope, idempotency_key, trace_id, span_id,
    result_ttl, tags_jsonb, stc_raw,
    retry_base, retry_cap, retry_backoff, retry_jitter)
ON CONFLICT (idempotency_scope, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING *""",
        enqueue_batch_fetch_existing=f"""\
SELECT j.* FROM "{s}".jobs j
JOIN unnest($1::text[], $2::text[]) AS pairs(scope, key)
  ON j.idempotency_scope = pairs.scope AND j.idempotency_key = pairs.key""",
        # The singleton_preflight predicate over an ANY-array: the post-abort
        # lookup the batch tiers' jobs_singleton_uniq conversion uses to name
        # the colliding actor (see _attribute_singleton_collision).
        enqueue_batch_fetch_singleton_blockers=f"""\
SELECT actor FROM "{s}".jobs
WHERE actor = ANY($1::text[])
  AND status IN ('pending', 'scheduled', 'running')
  AND metadata @> '{{"singleton": true}}'::jsonb""",
        # Post-COPY corrective UPDATE for enqueue_batch_fast.  COPY cannot
        # compute/decide anything, so it writes only domain-insensitive
        # columns (COPY_ENQUEUE_COLUMNS, status landing as
        # COPY_ENQUEUE_STATUS) and this UPDATE, executed inside the same
        # transaction, stamps status, scheduled_at, schedule_to_close and
        # result_expires_at from the server clock.  The status CASE is
        # byte-for-byte the INSERT arms' semantics (enqueue / enqueue_batch
        # above), which is what makes a NULL ("immediate") scheduled_at
        # safe on this path too.  An UPDATE never fires the INSERT trigger,
        # so the wake for the rows this statement makes runnable is its
        # own: one pg_notify on $6 (the wake channel), issued only when at
        # least one row landed 'pending', the notify folded into the write
        # and gated on the row being due.
        enqueue_batch_fast_fixup=f"""\
WITH params AS (
    SELECT * FROM unnest(
        $1::uuid[], $2::timestamptz[], $3::interval[], $4::timestamptz[], $5::interval[]
    ) AS t(id, scheduled_at, stc_interval, stc_raw, result_ttl)
),
fixed AS (
    UPDATE "{s}".jobs j
    SET status            = CASE WHEN COALESCE(p.scheduled_at, clock_timestamp()) > clock_timestamp()
                                 THEN 'scheduled'::"{s}".job_status
                                 ELSE 'pending'::"{s}".job_status END,
        scheduled_at      = COALESCE(p.scheduled_at, clock_timestamp()),
        schedule_to_close = COALESCE(clock_timestamp() + p.stc_interval, p.stc_raw),
        result_expires_at = CASE WHEN p.result_ttl IS NULL THEN NULL
                                 ELSE clock_timestamp() + p.result_ttl END
    FROM params p
    WHERE j.id = p.id
    RETURNING j.status
)
SELECT pg_notify($6, '')
WHERE EXISTS (SELECT 1 FROM fixed WHERE status = 'pending')""",
        # ── Read SQL templates ─────────────────────────────────────
        get_job=f"""\
SELECT * FROM "{s}".jobs WHERE id = $1""",
        # Same shape as get_job against the archive tier: the client
        # read's jobs-then-archive fallback (issue #314) probes this only
        # when the hot read misses, so an archived id answers instead of
        # reading as missing. jobs_archive mirrors every jobs column plus
        # the archive-only archived_at/expire_at stamps (see
        # _JOBS_COLUMNS_CSV in worker/_leader_shared.py), so the row ->
        # JobRow conversion is the hot table's.
        get_archived_job=f"""\
SELECT * FROM "{s}".jobs_archive WHERE id = $1""",
        get_attempts=f"""\
SELECT * FROM "{s}".job_attempts WHERE job_id = $1 ORDER BY attempt""",
        # Same shape as get_attempts against the archive tier: the client
        # read's hot-then-archive fallback (issue #314) probes this only
        # when the hot read comes back empty, so an archived job's moved
        # attempt history answers instead of reading as never-happened.
        # job_attempts_archive mirrors every job_attempts column (the
        # prune CTE's _JOB_ATTEMPTS_COLUMNS_CSV), so the row ->
        # AttemptRow conversion is the hot table's.
        get_archived_attempts=f"""\
SELECT * FROM "{s}".job_attempts_archive WHERE job_id = $1 ORDER BY attempt""",
        poll_cancel_flags=POLL_CANCEL_FLAGS_SQL.format(schema=s),
        # ── Dispatch SQL templates ─────────────────────────────────
        dispatch_strict_fifo=DISPATCH_STRICT_FIFO_SQL.format(schema=s),
        dispatch_round_robin=DISPATCH_ROUND_ROBIN_SQL.format(schema=s),
        dispatch_claimable_probe=DISPATCH_CLAIMABLE_PROBE_SQL.format(schema=s),
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
-- only over *visible* rows can ever detect (or bound) it, this was
-- verified to still lose events under an inverted allocation order
-- (a transaction that commits first can hold a lower transaction id but
-- a HIGHER event_id than one still open with a LOWER event_id).
--
-- id (bigserial nextval) and occurred_at (clock_timestamp()) are stamped
-- by the same INSERT statement, so they are co-monotonic: an earlier id
-- has an earlier-or-equal occurred_at, provided the two volatile calls
-- do not interleave across concurrent transactions within a single
-- INSERT (Postgres gives no such atomicity; the window is nanosecond-
-- scale, see taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY).  Only
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
-- violated", any transaction holding a lock on job_events for longer
-- than the margin is a candidate cause (lock contention, an overloaded
-- scan, a stalled/GC-paused worker, an oversized batch). Not proof of an
-- actual miss: this cannot see whether that transaction will insert a
-- job_events row at all, only that it has held the table open unusually
-- long. Excludes this query's own backend.
-- The ::float8 cast matters: EXTRACT returns numeric, which asyncpg
-- hands back as Decimal, but LongRunningJobEventsWriter.xact_age_seconds
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
        # The event-prune watermark (migration 01.00.20_01): the highest
        # job_events id any retention deleter has committed a delete below-or-at.
        # watch_reclaims' transports compare the consumer's persisted cursor
        # against this on every poll and fail visible (EventRetentionGapError)
        # when the cursor sits strictly behind it, instead of silently skipping
        # to live over events retention deleted before they were delivered. The
        # row ships at 0 from the migration, so a fleet that never pruned reads
        # 0 and no cursor can sit behind it: the signal arms itself with the
        # first delete, never before.
        event_prune_watermark=f"""\
SELECT pruned_through_id FROM "{s}".job_events_prune_state WHERE singleton = true""",
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
        # One row per actor, the client-side capacity cache reads the
        # whole table at most once per TTL window per process.
        list_actor_max_pending=f'SELECT actor, max_pending FROM "{s}".actor_config',
        # ── Admin operations ───────────────────────────────────────
        # Monotonic attempt: admin-retry must leave the attempt counter at
        # its spent value and raise the ceiling instead. Resetting to 0
        # made the re-dispatch revisit the spent epoch's numbers (dispatch
        # claims at attempt + 1, so a reset row climbs back through every
        # spent number), and the next attempt-row INSERT collided on
        # job_attempts_pkey (job_id, attempt), a data defect the
        # consumer's terminal-write infra family misclassified as transient,
        # stranding the row running with every reclaim cycle dying on the
        # same spent key. attempt is therefore NOT assigned here: the re-run
        # climbs to fresh numbers and every attempt-row write lands on a
        # fresh key. The ceiling raise
        # opens the budget gates (the reclaim sweep's re-pend branch,
        # the terminal arms) for at least one fresh execution, while a
        # mid-budget re-run keeps its remaining budget (GREATEST is a
        # no-op there); the LEAST cap is the smallint max_attempts
        # discipline, and when the cap binds (attempt already at
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
        -- An operator hand-back routes by the actor's current
        -- assignment, not by the label the row was first placed under
        -- (the routing contract in taskq/backend/_dispatch_sql.py).
        -- This holds for a row terminalized before it was ever claimed
        -- too: the retry is the deliberate re-pend, so a job cancelled
        -- while pending and retried after its actor moved reaches the
        -- target queue's consumers instead of stranding on the retired
        -- source queue.
        assignment_routed = true,
        cancel_phase = 0,
        cancel_requested_at = NULL,
        error_class = NULL,
        error_message = NULL,
        error_traceback = NULL,
        scheduled_at = clock_timestamp(),
        -- An already-elapsed schedule_to_close is a spent epoch's
        -- artifact, the same class as finished_at/result: dispatch's own
        -- claim predicate refuses any row whose deadline has passed
        -- (_dispatch_sql.py admits only schedule_to_close IS NULL OR
        -- schedule_to_close > now), so re-pending with the stale deadline
        -- intact hands back a row no worker can ever claim, the
        -- operator's Retry reports success and the next deadline-sweep
        -- tick silently re-fails the job. Clear the deadline only when it
        -- has already elapsed; a still-future one survives, preserving
        -- the operator's original budget intent for an in-window retry.
        -- (NULL <= clock_timestamp() is NULL, so an unset deadline falls
        -- through to the ELSE and stays unset.)
        schedule_to_close = CASE
            WHEN schedule_to_close <= clock_timestamp() THEN NULL
            ELSE schedule_to_close
        END,
        finished_at = NULL,
        result = NULL,
        result_size_bytes = NULL,
        result_expires_at = NULL
    WHERE id = $1
      -- An operator re-run is "run this again", so every state a job can
      -- come to rest in is a valid source, including 'succeeded' (the
      -- replay path after a bad deploy: the status records that the actor
      -- returned without raising, never that the result was right) and
      -- 'abandoned' (a deploy interrupted the job; it did not fail, and
      -- it is the state most likely to need a manual re-run).
      --
      -- 'running' is the one exclusion, and it is a correctness
      -- constraint rather than a policy choice: re-pending a row while
      -- an attempt is live races that attempt's terminal write, and the
      -- job can execute twice concurrently. The pending/scheduled states
      -- are excluded because the job is already queued to run, there is
      -- nothing to put back, and re-pending would discard its place in
      -- the dispatch order and its remaining budget.
      AND status NOT IN ('running', 'pending', 'scheduled')
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
