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
    "idempotency_key",
    "trace_id",
    "span_id",
    "metadata",
    "tags",
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
    enqueue_with_interval: str
    enqueue_unique_for_preflight: str
    singleton_preflight: str
    enqueue_max_pending_count: str
    enqueue_select_by_key: str
    enqueue_notify: str
    enqueue_batch: str
    enqueue_batch_fetch_existing: str
    enqueue_batch_fetch_by_ids: str

    # ── Read SQL templates ─────────────────────────────────────────
    get_job: str
    get_attempts: str
    poll_cancel_flags: str

    # ── Dispatch SQL templates ─────────────────────────────────────
    dispatch_strict_fifo: str
    dispatch_round_robin: str

    # ── Static read SQL ────────────────────────────────────────────
    get_events: str
    count_pending_jobs: str

    # ── Admin operations ───────────────────────────────────────────
    retry_job: str

    # ── COPY FROM column list ──────────────────────────────────────
    copy_from_columns: tuple[str, ...]


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
        mark_succeeded=f"""\
UPDATE "{s}".jobs
SET status = 'succeeded',
    finished_at = now(),
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    result = $3::jsonb,
    result_size_bytes = $4,
    result_expires_at = COALESCE(
        (SELECT now() + result_ttl * interval '1 second' FROM "{s}".actor_config WHERE actor = "{s}".jobs.actor),
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
        mark_retry=f"""\
UPDATE "{s}".jobs
SET status = CASE WHEN $3 > clock_timestamp() THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END,
    scheduled_at = $3,
    finished_at = NULL,
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    last_heartbeat_at = NULL,
    error_class = $4,
    error_message = $5,
    error_traceback = $6,
    progress_seq = $7,
    progress_state = CASE WHEN $8::jsonb IS NOT NULL THEN COALESCE(progress_state, '{{}}'::jsonb) || $8::jsonb ELSE progress_state END
WHERE id = $1 AND status = 'running' AND locked_by_worker = $2
RETURNING *""",
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
        enqueue=f"""\
INSERT INTO "{s}".jobs
(id, actor, queue, identity_key, fairness_key,
 payload, payload_schema_ver, status, priority,
 max_attempts, retry_kind,
 schedule_to_close, start_to_close, heartbeat_timeout,
 scheduled_at,
 idempotency_key, trace_id, span_id, metadata, result_expires_at, tags)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, CASE WHEN COALESCE($14, clock_timestamp()) > clock_timestamp() THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END, $8, $9, $10, $11, $12, $13, COALESCE($14, clock_timestamp()), $15, $16, $17, $18::jsonb, $19, $20::text[])
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING *""",
        enqueue_with_interval=f"""\
INSERT INTO "{s}".jobs
(id, actor, queue, identity_key, fairness_key,
 payload, payload_schema_ver, status, priority,
 max_attempts, retry_kind,
 schedule_to_close, start_to_close, heartbeat_timeout,
 scheduled_at,
 idempotency_key, trace_id, span_id, metadata, result_expires_at, tags)
VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, CASE WHEN COALESCE($14, clock_timestamp()) > clock_timestamp() THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END, $8, $9, $10, clock_timestamp() + $11::interval, $12, $13, COALESCE($14, clock_timestamp()), $15, $16, $17, $18::jsonb, $19, $20::text[])
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
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
SELECT * FROM "{s}".jobs WHERE idempotency_key = $1""",
        enqueue_notify="SELECT pg_notify($1, '')",
        enqueue_batch=f"""\
INSERT INTO "{s}".jobs (
    id, actor, queue, identity_key, fairness_key,
    payload, payload_schema_ver,
    status, priority, attempt, max_attempts, retry_kind,
    schedule_to_close, start_to_close, heartbeat_timeout,
    scheduled_at, metadata, idempotency_key, trace_id, span_id,
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
    CASE WHEN COALESCE(t.scheduled_at, now()) > clock_timestamp() THEN 'scheduled'::"{s}".job_status ELSE 'pending'::"{s}".job_status END,
    t.priority,
    0,
    t.max_attempts,
    t.retry_kind,
    t.schedule_to_close,
    t.start_to_close,
    t.heartbeat_timeout,
    COALESCE(t.scheduled_at, now()),
    t.metadata,
    t.idempotency_key,
    t.trace_id,
    t.span_id,
    t.result_expires_at,
    -- Pg text[][] does not support jagged arrays (empty sub-array () has different
    -- dimensionality from ('a','b')).  We pass tags via jsonb[] transit ($20::jsonb[])
    -- and unpack each element into text[] with jsonb_array_elements_text(…)::text[].
    (SELECT COALESCE(array_agg(elem::text), '{{}}'::text[]) FROM jsonb_array_elements_text(t.tags_jsonb) AS elem)
FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[],
    $6::jsonb[], $7::int[],
    $8::int[], $9::int[], $10::text[],
    $11::timestamptz[], $12::interval[], $13::interval[],
    $14::timestamptz[], $15::jsonb[], $16::text[], $17::text[], $18::text[],
    $19::timestamptz[], $20::jsonb[]
) AS t(id, actor, queue, identity_key, fairness_key,
    payload, payload_schema_ver,
    priority, max_attempts, retry_kind,
    schedule_to_close, start_to_close, heartbeat_timeout,
    scheduled_at, metadata, idempotency_key, trace_id, span_id,
    result_expires_at, tags_jsonb)
ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING id, actor, queue, identity_key, status, idempotency_key""",
        enqueue_batch_fetch_existing=f"""\
SELECT * FROM "{s}".jobs WHERE idempotency_key = ANY($1::text[])""",
        enqueue_batch_fetch_by_ids=f"""\
SELECT * FROM "{s}".jobs WHERE id = ANY($1::uuid[])""",
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
        count_pending_jobs=(
            f'SELECT actor, count(*)::int AS cnt FROM "{s}".jobs '
            f"WHERE actor = ANY($1::text[]) "
            f"AND status IN ('pending', 'scheduled') "
            f"GROUP BY actor"
        ),
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
        # ── COPY FROM column list ──────────────────────────────────
        copy_from_columns=COPY_FROM_COLUMNS,
    )
