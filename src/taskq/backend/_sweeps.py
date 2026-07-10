"""Leader/worker maintenance sweeps for PostgresBackend.

The five sweep operations are stateless (they take a connection and
schema, hold no instance state), so they live here as module-level
functions.  :class:`~taskq.backend.postgres.PostgresBackend` exposes
thin ``@staticmethod`` wrappers that delegate here, preserving the
existing ``PostgresBackend.sweep_*`` call surface.
"""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import structlog

from taskq.backend._protocol import ConnLike, JobId
from taskq.backend._records import jsonb_param, parse_rowcount
from taskq.backend._sql import INSERT_EVENT_SQL
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    wake_channel,
)
from taskq.obs import get_logger, log_state_change, record_deadline_exceeded_swept

__all__ = [
    "_SWEEP_1_SQL",
    "_SWEEP_2_SQL",
    "_SWEEP_3_SQL",
    "_SWEEP_4_SQL",
    "_SWEEP_RESULT_TTL_SQL",
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
-- Sweep 1 runs on every worker, not just the leader.  FOR UPDATE SKIP
-- LOCKED lets sibling workers run concurrently without blocking — each
-- worker reclaims a disjoint subset of expired-lock rows.  Vendor
-- parallel: Oban's lifeline plugin runs this on every instance, not just
-- the elected leader.
WITH snap AS (
    SELECT id, locked_by_worker
    FROM "{schema}".jobs
    WHERE status = 'running'
      AND lock_expires_at < clock_timestamp()
      AND (cancel_phase = 0
           OR lock_expires_at < clock_timestamp() - $1::interval - $2::interval - interval '60 seconds')
    FOR UPDATE SKIP LOCKED
)
UPDATE "{schema}".jobs j
SET status = CASE
        WHEN j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable'
            THEN 'pending'::"{schema}".job_status
        ELSE 'crashed'::"{schema}".job_status
    END,
    locked_by_worker = NULL,
    lock_expires_at = NULL,
    scheduled_at = CASE
        WHEN j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable'
            THEN now() + interval '5 seconds'
        ELSE j.scheduled_at
    END,
    finished_at = CASE
        WHEN NOT (j.attempt < j.max_attempts AND j.retry_kind != 'non_retryable')
            THEN now()
        ELSE j.finished_at
    END
FROM snap
WHERE j.id = snap.id
RETURNING j.id, j.status, j.attempt, j.started_at, snap.locked_by_worker"""

_SWEEP_2_SQL = """\
WITH snap AS (
    SELECT id, status AS prev_status
    FROM "{schema}".jobs
    WHERE status IN ('pending', 'scheduled')
      AND schedule_to_close IS NOT NULL
      AND schedule_to_close < clock_timestamp()
    FOR UPDATE SKIP LOCKED
)
UPDATE "{schema}".jobs j
SET status = 'failed'::"{schema}".job_status,
    finished_at = now(),
    error_class = 'DeadlineExceeded',
    error_message = 'schedule_to_close reached before next dispatch'
FROM snap
WHERE j.id = snap.id
RETURNING j.id, snap.prev_status, j.attempt, j.started_at, j.actor"""

_SWEEP_3_SQL = """\
WITH snap AS (
    SELECT id, status AS prev_status
    FROM "{schema}".jobs
    WHERE status = 'scheduled'
      AND scheduled_at <= clock_timestamp()
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
UPDATE "{schema}".jobs
SET result = NULL,
    result_size_bytes = NULL,
    result_expires_at = NULL
WHERE result_expires_at < clock_timestamp()
  AND result IS NOT NULL"""

# Per-sweep attempt INSERT templates (schema baked in via .format at call
# time after _IDENT_RE validation).  Kept as constants so the SQL surface
# stays grep-able and free of f-string S608 noise.
_SWEEP_1_ATTEMPT_SQL = """\
INSERT INTO "{schema}".job_attempts
(job_id, attempt, started_at, finished_at, outcome,
 error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
VALUES ($1, $2, $3, now(), $4, $5, $6, $7, $8, $9, $10::jsonb)"""

_SWEEP_2_ATTEMPT_SQL = """\
INSERT INTO "{schema}".job_attempts
(job_id, attempt, started_at, finished_at, outcome,
 error_class, error_message, error_traceback, duration_ms, worker_id, metadata)
VALUES ($1, $2, COALESCE($3, now()), now(), $4, $5, $6, $7, $8, $9, $10::jsonb)"""


async def sweep_expired_locks(
    conn: ConnLike,
    now: datetime,
    cancel_grace: timedelta,
    cleanup_grace: timedelta,
    *,
    schema: str,
) -> int:
    """Sweep 1: reclaim running jobs whose lock has expired.

    For each reclaimed job:

    - If attempts remain and retry is allowed: transition to
      ``'scheduled'`` with 5-second backoff.
    - Otherwise: transition to ``'crashed'`` with ``finished_at = now()``.

    Both branches write a ``job_attempts`` row (outcome ``'crashed'``,
    error_class ``'WorkerCrashed'``) and a ``job_events`` row (kind
    ``'state_change'``, reason ``'lock_expired'``).

    *now* is accepted for API consistency; PG uses server-side ``now()``.

    A CTE snapshots ``locked_by_worker`` before the UPDATE clears it, so
    the ``job_attempts.worker_id`` is populated correctly.

    Returns the count of affected rows.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    sql = _SWEEP_1_SQL.format(schema=schema)
    attempt_sql = _SWEEP_1_ATTEMPT_SQL.format(schema=schema)
    event_sql = INSERT_EVENT_SQL.format(schema=schema)

    swept_rows: list[dict[str, object]] = []

    async with conn.transaction():
        rows = await conn.fetch(sql, cancel_grace, cleanup_grace)

        for rec in rows:
            job_id: JobId = JobId(rec["id"])
            new_status: str = rec["status"]
            attempt: int = rec["attempt"]
            started_at: datetime | None = rec["started_at"]
            original_worker: UUID | None = rec["locked_by_worker"]

            duration_ms: int | None = None
            if started_at is not None:
                duration_ms = int((datetime.now(UTC) - started_at).total_seconds() * 1000)

            await conn.execute(
                attempt_sql,
                job_id,
                attempt,
                started_at,
                "crashed",
                "WorkerCrashed",
                "lock expired before worker reported terminal state",
                None,  # error_traceback
                duration_ms,
                original_worker,
                "{}",  # metadata
            )

            detail: dict[str, object] = {
                "from_state": "running",
                "to_state": new_status,
                "reason": "lock_expired",
            }
            if original_worker is not None:
                detail["worker_id"] = str(original_worker)
            await conn.execute(
                event_sql,
                job_id,
                "state_change",
                jsonb_param(detail),
            )

            swept_rows.append(
                {
                    "job_id": job_id,
                    "attempt": attempt,
                    "new_status": new_status,
                }
            )

        _pending_count = sum(1 for info in swept_rows if info["new_status"] == "pending")
        if _pending_count > 0:
            await conn.execute(
                "SELECT pg_notify($1, '')",
                wake_channel(schema),
            )

    for info in swept_rows:
        log_state_change(
            logger,
            from_state="running",
            to_state=info["new_status"],  # type: ignore[arg-type]  # Why: swept_rows is dict[str, object]; new_status is always str at runtime
            job_id=str(info["job_id"]),
            attempt=info["attempt"],  # type: ignore[arg-type]  # Why: swept_rows is dict[str, object]; attempt is always int at runtime
            reason="lock_expired",
        )
    if swept_rows:
        logger.error(
            "recovery_reclaim",
            kind="recovery_reclaim",
            count=len(swept_rows),
            schema=schema,
        )

    return len(rows)


async def sweep_deadline_exceeded(
    conn: ConnLike,
    now: datetime,
    *,
    schema: str,
) -> int:
    """Sweep 2: fail pending/scheduled jobs whose ``schedule_to_close``
    deadline has passed.

    Transitions to ``'failed'`` with ``error_class = 'DeadlineExceeded'``.
    Writes one ``job_attempts`` row and one ``job_events`` row per swept
    job, in the same transaction as the parent UPDATE.

    ``started_at`` for never-dispatched jobs is NULL; the attempt INSERT
    uses ``COALESCE(started_at, clock_timestamp())`` to satisfy the
    ``job_attempts.started_at NOT NULL`` constraint.

    *now* is accepted for API consistency; PG uses server-side ``now()``.

    Returns the count of swept rows.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    sql = _SWEEP_2_SQL.format(schema=schema)
    attempt_sql = _SWEEP_2_ATTEMPT_SQL.format(schema=schema)
    event_sql = INSERT_EVENT_SQL.format(schema=schema)

    swept_rows: list[dict[str, object]] = []

    async with conn.transaction():
        rows = await conn.fetch(sql)

        for rec in rows:
            job_id: JobId = JobId(rec["id"])
            prev_status: str = rec["prev_status"]
            attempt: int = rec["attempt"]
            started_at: datetime | None = rec["started_at"]
            actor: str = rec["actor"]

            record_deadline_exceeded_swept(actor=actor)

            duration_ms: int | None = None
            if started_at is not None:
                duration_ms = int((datetime.now(UTC) - started_at).total_seconds() * 1000)
            await conn.execute(
                attempt_sql,
                job_id,
                attempt,
                started_at,
                "failed",
                "DeadlineExceeded",
                "schedule_to_close reached before next dispatch",
                None,  # error_traceback
                duration_ms,
                None,  # worker_id (never dispatched; no locked_by_worker)
                "{}",  # metadata
            )

            detail: dict[str, object] = {
                "from_state": prev_status,
                "to_state": "failed",
                "error_class": "DeadlineExceeded",
            }
            await conn.execute(
                event_sql,
                job_id,
                "state_change",
                jsonb_param(detail),
            )

            swept_rows.append(
                {
                    "job_id": job_id,
                    "from_state": prev_status,
                }
            )

    for info in swept_rows:
        log_state_change(
            logger,
            from_state=str(info["from_state"]),
            to_state="failed",
            job_id=str(info["job_id"]),
            error_class="DeadlineExceeded",
        )
    if swept_rows:
        logger.debug(
            "sweep_deadline_exceeded",
            kind="sweep_deadline_exceeded",
            count=len(swept_rows),
            schema=schema,
        )

    return len(rows)


async def sweep_scheduled_to_pending(
    conn: ConnLike,
    now: datetime,
    *,
    schema: str,
) -> int:
    """Sweep 3: promote scheduled jobs whose ``scheduled_at`` has passed.

    Transitions ``status='scheduled'`` rows with ``scheduled_at <=
    now()`` to ``status='pending'``.  Writes one ``job_events`` row per
    promoted job with ``kind='state_change'``, ``detail`` carrying
    ``from_state='scheduled'`` and ``to_state='pending'``.

    *now* is accepted for API consistency; PG uses server-side ``now()``.

    Returns the count of promoted rows.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    sql = _SWEEP_3_SQL.format(schema=schema)
    event_sql = INSERT_EVENT_SQL.format(schema=schema)

    promoted_rows: list[dict[str, object]] = []

    async with conn.transaction():
        rows = await conn.fetch(sql)

        for rec in rows:
            job_id: JobId = JobId(rec["id"])
            prev_status: str = rec["prev_status"]

            detail: dict[str, object] = {
                "from_state": prev_status,
                "to_state": "pending",
            }
            await conn.execute(
                event_sql,
                job_id,
                "state_change",
                jsonb_param(detail),
            )

            promoted_rows.append(
                {
                    "job_id": job_id,
                    "from_state": prev_status,
                }
            )

        if promoted_rows:
            await conn.execute(
                "SELECT pg_notify($1, '')",
                wake_channel(schema),
            )

    for info in promoted_rows:
        log_state_change(
            logger,
            from_state=str(info["from_state"]),  # type: ignore[arg-type]  # Why: promoted_rows is dict[str, object]; from_state is always str at runtime
            to_state="pending",
            job_id=str(info["job_id"]),
        )
    if promoted_rows:
        logger.debug(
            "sweep_scheduled_to_pending",
            kind="sweep_scheduled_to_pending",
            count=len(promoted_rows),
            schema=schema,
        )

    return len(rows)


async def sweep_leaked_reservation_slots(
    conn: ConnLike,
    now: datetime,
    *,
    schema: str,
) -> int:
    """Sweep 4: release reservation slots whose lease has expired.

    Clears ``job_id``, ``held_by_worker_id``, ``acquired_at``, and
    ``lease_expires_at`` on matching rows.  No ``job_attempts`` or
    ``job_events`` writes — reservation slots are not job-state
    transitions.

    *now* is accepted for API consistency; PG uses server-side ``now()``.

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
    now: datetime,
    *,
    schema: str,
) -> int:
    """Expire result rows whose ``result_expires_at`` has passed."""
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    sql = _SWEEP_RESULT_TTL_SQL.format(schema=schema)
    tag = await conn.execute(sql)
    count = parse_rowcount(tag)
    if count > 0:
        logger.debug(
            "sweep_expired_results",
            kind="sweep_expired_results",
            count=count,
            schema=schema,
        )
    return count
