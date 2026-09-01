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
    INSERT_ATTEMPT_SQL,
    INSERT_EVENT_SQL,
    POLL_CANCEL_FLAGS_SQL,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
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
)

# Column list for the enqueue COPY path only.  Every clock-domain-sensitive
# column is OMITTED so COPY writes the DDL default (status 'pending',
# created_at/scheduled_at now()) or NULL (schedule_to_close,
# result_expires_at), and the post-COPY fixup UPDATE
# (enqueue_batch_fast_fixup) stamps/decides them from the server clock —
# never from the caller's Python clock.  COPY_FROM_COLUMNS stays intact: it
# is shared by the archive CTE column lists in worker/_leader_shared.py.
_COPY_ENQUEUE_OMITTED: Final[frozenset[str]] = frozenset(
    {"status", "created_at", "scheduled_at", "schedule_to_close", "result_expires_at"}
)
COPY_ENQUEUE_COLUMNS: Final[tuple[str, ...]] = tuple(
    c for c in COPY_FROM_COLUMNS if c not in _COPY_ENQUEUE_OMITTED
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
    insert_attempt: str
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
        # ── Terminal-write UPDATE statements ───────────────────────
        # result_expires_at resolution, first non-NULL wins: the stored
        # (operator-owned) result_ttl applied at completion; then the
        # caller-supplied fallback ($7 — the @actor literal the SQL cannot
        # see, also applied at completion, so a long-queued job does not
        # complete already expired); then the enqueue-time value.
        # Why clock_timestamp() and not now(): in the LOOP-scope
        # transactional path now() is the TRANSACTION start (≈ actor
        # start), so an actor whose runtime exceeds its TTL would
        # complete already expired — the same bug class as queue-time
        # pinning, one path over. clock_timestamp() is the wall-clock
        # time the write actually executes. finished_at keeps now() —
        # pre-existing semantics, unchanged by this fix.
        mark_succeeded=f"""\
UPDATE "{s}".jobs
SET status = 'succeeded',
    finished_at = now(),
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
WHERE id = $1 AND status = 'running' AND locked_by_worker = $2
RETURNING *""",
        mark_failed=f"""\
UPDATE "{s}".jobs
SET status = 'failed',
    finished_at = now(),
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    error_class = $3,
    error_message = $4,
    error_traceback = $5,
    progress_seq = $6,
    progress_state = CASE WHEN $7::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $7::jsonb ELSE progress_state END
WHERE id = $1 AND status = 'running' AND locked_by_worker = $2
RETURNING *""",
        # mark_retry is a two-CTE single-arbiter statement, structurally
        # mirroring mark_snoozed / mark_retry_after: the delay ($3::interval)
        # is applied by the SERVER clock (scheduled_at = now() + delay; the
        # status derives from the delay alone), and the schedule_to_close
        # deadline is arbitrated in the same statement — clock_timestamp() +
        # delay <= schedule_to_close retries; past it, the deadline_failed
        # CTE lands 'failed' with error_class='DeadlineExceeded'.  The caller
        # never passes a Python-domain timestamp (C1: a skewed caller could
        # otherwise void the backoff or kill a live job).
        mark_retry=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id, $2::uuid AS worker_id, $3::interval AS retry_delay
),
retried AS (
    UPDATE "{s}".jobs j
    SET status = CASE WHEN $3::interval > interval '0' THEN 'scheduled'::"{s}".job_status
                      ELSE 'pending'::"{s}".job_status END,
        scheduled_at = now() + (SELECT retry_delay FROM params),
        finished_at = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
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
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT retry_delay FROM params) <= j.schedule_to_close)
    RETURNING j.*, 'retried'::text AS outcome_branch
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = now(),
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
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT retry_delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM retried)
    RETURNING j.*, 'deadline_failed'::text AS outcome_branch
)
SELECT * FROM retried UNION ALL SELECT * FROM deadline_failed""",
        mark_cancelled=f"""\
UPDATE "{s}".jobs
SET status = 'cancelled',
    finished_at = now(),
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    progress_seq = $3,
    progress_state = CASE WHEN $4::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $4::jsonb ELSE progress_state END
WHERE id = $1 AND status = 'running' AND locked_by_worker = $2
RETURNING *""",
        mark_abandoned=f"""\
UPDATE "{s}".jobs
SET status = 'abandoned',
    finished_at = now(),
    progress_seq = $2,
    progress_state = CASE WHEN $3::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $3::jsonb ELSE progress_state END
WHERE id = $1 AND status = 'running' AND cancel_phase = 2
RETURNING *""",
        # Snooze does not consume retry budget: the UPDATE deliberately
        # leaves j.attempt unchanged.
        mark_snoozed=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           $3::interval AS delay,
           $4::jsonb AS metadata_update,
           $5::int AS progress_seq,
           $6::jsonb AS progress_state
),
snoozed AS (
    UPDATE "{s}".jobs j
    SET status = CASE WHEN $3::interval > interval '0' THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END,
        scheduled_at = now() + (SELECT delay FROM params),
        finished_at = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        max_attempts = j.max_attempts + 1,
        metadata = j.metadata || COALESCE((SELECT metadata_update FROM params), '{{}}'::jsonb),
        progress_seq = (SELECT progress_seq FROM params),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT delay FROM params) <= j.schedule_to_close)
    RETURNING j.*, 'snoozed'::text AS outcome_branch
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = now(),
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
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, 'failed'::text AS outcome_branch
)
SELECT * FROM snoozed UNION ALL SELECT * FROM deadline_failed""",
        mark_retry_after_consume_true=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           $3::interval AS delay,
           $4::int AS progress_seq,
           $5::jsonb AS progress_state
),
        snoozed AS (
    UPDATE "{s}".jobs j
    SET status = CASE WHEN $3::interval > interval '0' THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END,
        scheduled_at = now() + (SELECT delay FROM params),
        finished_at = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        progress_seq = (SELECT progress_seq FROM params),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT delay FROM params) <= j.schedule_to_close)
      AND (j.retry_kind = 'indefinite'
           OR j.attempt < j.max_attempts)
    RETURNING j.*, j.attempt AS running_attempt, 'snoozed'::text AS outcome_branch
),
max_attempts_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = now(),
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
      AND j.retry_kind = 'transient'
      AND j.attempt >= j.max_attempts
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT delay FROM params) <= j.schedule_to_close)
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, j.attempt AS running_attempt, 'max_attempts_failed'::text AS outcome_branch
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = now(),
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
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM snoozed)
      AND NOT EXISTS (SELECT 1 FROM max_attempts_failed)
    RETURNING j.*, j.attempt AS running_attempt, 'deadline_failed'::text AS outcome_branch
)
SELECT * FROM snoozed
UNION ALL SELECT * FROM max_attempts_failed
UNION ALL SELECT * FROM deadline_failed""",
        mark_retry_after_consume_false=f"""\
WITH params AS (
    SELECT $1::uuid AS job_id,
           $2::uuid AS worker_id,
           $3::interval AS delay,
           $4::int AS progress_seq,
           $5::jsonb AS progress_state
),
snoozed AS (
    UPDATE "{s}".jobs j
    SET status = CASE WHEN $3::interval > interval '0' THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END,
        scheduled_at = now() + (SELECT delay FROM params),
        finished_at = NULL,
        locked_by_worker = NULL,
        lock_expires_at = NULL,
        last_heartbeat_at = NULL,
        max_attempts = j.max_attempts + 1,
        progress_seq = (SELECT progress_seq FROM params),
        progress_state = CASE WHEN (SELECT progress_state FROM params) IS NOT NULL THEN COALESCE(j.progress_state, '{{}}'::jsonb) || (SELECT progress_state FROM params) ELSE j.progress_state END
    WHERE j.id = (SELECT job_id FROM params)
      AND j.status = 'running'
      AND j.locked_by_worker = (SELECT worker_id FROM params)
      AND (j.schedule_to_close IS NULL
           OR clock_timestamp() + (SELECT delay FROM params) <= j.schedule_to_close)
    RETURNING j.*, 'snoozed'::text AS outcome_branch
),
deadline_failed AS (
    UPDATE "{s}".jobs j
    SET status = 'failed',
        finished_at = now(),
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
      AND j.schedule_to_close IS NOT NULL
      AND clock_timestamp() + (SELECT delay FROM params) > j.schedule_to_close
      AND NOT EXISTS (SELECT 1 FROM snoozed)
    RETURNING j.*, 'deadline_failed'::text AS outcome_branch
)
SELECT * FROM snoozed UNION ALL SELECT * FROM deadline_failed""",
        # ── Shared INSERT templates ────────────────────────────────
        insert_attempt=INSERT_ATTEMPT_SQL.format(schema=s),
        insert_attempt_explicit=f"""\
INSERT INTO "{s}".job_attempts
(job_id, attempt, started_at, finished_at, outcome,
 error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)""",
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
SET cancel_requested_at = now(), cancel_phase = 1
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
  AND created_at > now() - $4::interval
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
        # One row per actor — the client-side capacity cache reads the
        # whole table at most once per TTL window per process.
        list_actor_max_pending=f'SELECT actor, max_pending FROM "{s}".actor_config',
        # ── Admin operations ───────────────────────────────────────
        retry_job=f"""\
UPDATE "{s}".jobs
SET status = 'pending',
    attempt = 0,
    cancel_phase = 0,
    cancel_requested_at = NULL,
    error_class = NULL,
    error_message = NULL,
    error_traceback = NULL,
    scheduled_at = now(),
    finished_at = NULL,
    result = NULL,
    result_size_bytes = NULL,
    result_expires_at = NULL
WHERE id = $1 AND status IN ('failed', 'crashed', 'cancelled')
RETURNING id""",
        # ── COPY FROM column lists ─────────────────────────────────
        copy_from_columns=COPY_FROM_COLUMNS,
        copy_enqueue_columns=COPY_ENQUEUE_COLUMNS,
    )
