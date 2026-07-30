"""Batch SQL constants and asyncpg helpers for PostgresBackend.

Canonical home for batch-related SQL so it stays grep-able and
unit-testable independent of the PostgresBackend class.  Following the
companion-module pattern (``_dispatch_sql.py``, ``_enqueue.py``,
``_terminal.py``, ``_schedules.py``).

Schema identifier is baked into pre-rendered SQL strings at render time
via :func:`render_batch_sql`.  All user-supplied values use asyncpg
``$N`` positional parameter binding — no f-string interpolation of user
data.  The schema identifier is validated against ``_IDENT_RE`` before
formatting (defence-in-depth).
"""

from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime
from itertools import islice
from typing import TYPE_CHECKING, cast
from uuid import UUID

from asyncpg.exceptions import UniqueViolationError

from taskq._json import dumps_str
from taskq.backend._enqueue import _enqueue_batch
from taskq.backend._protocol import (
    BatchCounts,
    BatchFilter,
    BatchRow,
    ConnLike,
    EnqueueArgs,
    JobRow,
)
from taskq.backend._records import _batch_row_from_record
from taskq.backend._sql_templates import SqlTemplates
from taskq.backend.clock import Clock
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "BatchSql",
    "abort_batch",
    "complete_batch",
    "count_batch_non_terminal",
    "create_batch",
    "enqueue_batch_atomic",
    "get_batch",
    "increment_batch_failures",
    "list_batches",
    "prune_old_batches",
    "render_batch_sql",
    "reset_batch_failures",
]

# ── SQL template constants ──────────────────────────────────────────
# ``{schema}`` is interpolated via ``.format`` at render time (schema is
# validated against _IDENT_RE by render_batch_sql), keeping the surface
# free of f-string S608 noise.

# Build the terminal-status NOT IN clause from the canonical
# TERMINAL_STATUSES set so the SQL never drifts when a new status is
# added to the state machine.
_TERMINAL_NOT_IN = "NOT IN (" + ",".join(f"'{s}'" for s in TERMINAL_STATUSES) + ")"

_CREATE_BATCH_SQL = """\
INSERT INTO "{schema}".batches
(id, queue, expected_size, failure_threshold, finalizer_job_id, originating_actor)
VALUES ($1, $2, $3, $4, $5, $6)"""

_GET_BATCH_SQL = """\
SELECT id, queue, status, expected_size, consecutive_failures,
       failure_threshold, finalizer_job_id, originating_actor,
       created_at, completed_at, metadata
FROM "{schema}".batches
WHERE id = $1"""

_INCREMENT_BATCH_FAILURES_SQL = """\
WITH updated AS (
    UPDATE "{schema}".batches
    SET consecutive_failures = consecutive_failures + 1
    WHERE id = $1 AND status = 'active'
    RETURNING consecutive_failures, failure_threshold
),
counts AS (
    SELECT count(*)::int AS remaining
    FROM "{schema}".jobs
    WHERE metadata @> $2::jsonb
      AND status {terminal_not_in}
)
SELECT u.consecutive_failures, u.failure_threshold, c.remaining
FROM updated u CROSS JOIN counts c"""

_RESET_BATCH_FAILURES_SQL = """\
WITH updated AS (
    UPDATE "{schema}".batches
    SET consecutive_failures = 0
    WHERE id = $1 AND status = 'active'
    RETURNING 1
),
counts AS (
    SELECT count(*)::int AS remaining
    FROM "{schema}".jobs
    WHERE metadata @> $2::jsonb
      AND status {terminal_not_in}
)
SELECT c.remaining FROM updated u CROSS JOIN counts c"""

_ABORT_BATCH_JOBS_SQL = """\
UPDATE "{schema}".jobs
SET status = 'cancelled',
    finished_at = clock_timestamp(),
    error_class = 'BatchAbortedError',
    error_message = 'Batch aborted due to consecutive failures',
    cancel_requested_at = clock_timestamp(),
    cancel_phase = 2
WHERE metadata @> $1::jsonb
  AND status IN ('pending', 'scheduled')
RETURNING id"""

_ABORT_BATCH_ROW_SQL = """\
UPDATE "{schema}".batches
SET status = 'aborted', completed_at = clock_timestamp()
WHERE id = $1 AND status = 'active'"""

_COMPLETE_BATCH_SQL = """\
UPDATE "{schema}".batches
SET status = 'complete', completed_at = clock_timestamp()
WHERE id = $1 AND status = 'active'"""

_COUNT_BATCH_NON_TERMINAL_SQL = """\
SELECT count(*)::int FROM "{schema}".jobs
WHERE metadata @> $1::jsonb
  AND status {terminal_not_in}"""

_LIST_BATCHES_BASE_SQL = """\
SELECT b.id, b.queue, b.status, b.expected_size, b.consecutive_failures,
       b.failure_threshold, b.finalizer_job_id, b.originating_actor,
       b.created_at, b.completed_at, b.metadata,
       COALESCE(j.total, 0) AS total,
       COALESCE(j.pending, 0) AS pending,
       COALESCE(j.succeeded, 0) AS succeeded,
       COALESCE(j.failed, 0) AS failed,
       COALESCE(j.cancelled, 0) AS cancelled,
       COALESCE(j.crashed, 0) AS crashed,
       COALESCE(j.abandoned, 0) AS abandoned
FROM "{schema}".batches b
LEFT JOIN LATERAL (
    SELECT count(*) AS total,
           count(*) FILTER (WHERE status {terminal_not_in}) AS pending,
           count(*) FILTER (WHERE status = 'succeeded') AS succeeded,
           count(*) FILTER (WHERE status = 'failed') AS failed,
           count(*) FILTER (WHERE status = 'cancelled') AS cancelled,
           count(*) FILTER (WHERE status = 'crashed') AS crashed,
           count(*) FILTER (WHERE status = 'abandoned') AS abandoned
    FROM "{schema}".jobs j
    WHERE j.metadata @> jsonb_build_object('batch_id', b.id::text)
) j ON true
WHERE 1=1"""

_PRUNE_OLD_BATCHES_SQL = """\
DELETE FROM "{schema}".batches
WHERE completed_at IS NOT NULL
  AND completed_at < $1
  AND NOT EXISTS (
    SELECT 1 FROM "{schema}".jobs j
    WHERE j.metadata @> jsonb_build_object('batch_id', batches.id::text)
  )
RETURNING id"""


@dataclass(frozen=True, slots=True)
class BatchSql:
    """Pre-rendered SQL strings for the batches table."""

    create_batch: str
    get_batch: str
    increment_batch_failures: str
    reset_batch_failures: str
    abort_batch_jobs: str
    abort_batch_row: str
    complete_batch: str
    count_batch_non_terminal: str
    list_batches_base: str
    prune_old_batches: str


def render_batch_sql(schema: str) -> BatchSql:
    """Render all batch SQL templates for *schema*.

    Validates *schema* against the canonical identifier regex before
    formatting.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return BatchSql(
        create_batch=_CREATE_BATCH_SQL.format(schema=schema),
        get_batch=_GET_BATCH_SQL.format(schema=schema),
        increment_batch_failures=_INCREMENT_BATCH_FAILURES_SQL.format(
            schema=schema, terminal_not_in=_TERMINAL_NOT_IN
        ),
        reset_batch_failures=_RESET_BATCH_FAILURES_SQL.format(
            schema=schema, terminal_not_in=_TERMINAL_NOT_IN
        ),
        abort_batch_jobs=_ABORT_BATCH_JOBS_SQL.format(schema=schema),
        abort_batch_row=_ABORT_BATCH_ROW_SQL.format(schema=schema),
        complete_batch=_COMPLETE_BATCH_SQL.format(schema=schema),
        count_batch_non_terminal=_COUNT_BATCH_NON_TERMINAL_SQL.format(
            schema=schema, terminal_not_in=_TERMINAL_NOT_IN
        ),
        list_batches_base=_LIST_BATCHES_BASE_SQL.format(
            schema=schema, terminal_not_in=_TERMINAL_NOT_IN
        ),
        prune_old_batches=_PRUNE_OLD_BATCHES_SQL.format(schema=schema),
    )


# ── Record conversion helpers ───────────────────────────────────────


def _batch_counts_from_record(rec: "asyncpg.Record") -> BatchCounts:
    """Convert count fields from a list_batches query result into :class:`BatchCounts`."""
    return BatchCounts(
        total=rec["total"],
        pending=rec["pending"],
        succeeded=rec["succeeded"],
        failed=rec["failed"],
        cancelled=rec["cancelled"],
        crashed=rec["crashed"],
        abandoned=rec["abandoned"],
    )


def _batch_filter_json(batch_id: UUID) -> str:
    """Serialize a batch_id filter for the ``@>`` jsonb operator."""
    return dumps_str({"batch_id": str(batch_id)})


# ── Module-level async functions ────────────────────────────────────


async def create_batch(
    conn: ConnLike,
    sql: BatchSql,
    batch_id: UUID,
    queue: str,
    expected_size: int,
    failure_threshold: int | None,
    finalizer_job_id: UUID | None,
    originating_actor: str | None,
) -> None:
    """Insert a row into ``batches``.

    Raises :class:`ValueError` when *failure_threshold* is not ``None`` and
    is less than 1 (matches the ``CHECK (failure_threshold >= 1)`` constraint
    on the table).

    Raises :class:`~taskq.exceptions.BatchIdExistsError` when *batch_id*
    already exists (M2: typed domain error instead of raw
    ``UniqueViolationError``).
    """
    if failure_threshold is not None and failure_threshold < 1:
        raise ValueError(f"failure_threshold must be >= 1 when set, got {failure_threshold}")
    try:
        await conn.execute(
            sql.create_batch,
            batch_id,
            queue,
            expected_size,
            failure_threshold,
            finalizer_job_id,
            originating_actor,
        )
    except UniqueViolationError as exc:
        from taskq.exceptions import BatchIdExistsError

        raise BatchIdExistsError(batch_id) from exc


async def get_batch(
    conn: ConnLike,
    sql: BatchSql,
    batch_id: UUID,
) -> BatchRow | None:
    """Fetch a single batch row by ID, or ``None`` if not found."""
    rec = await conn.fetchrow(sql.get_batch, batch_id)
    if rec is None:
        return None
    return _batch_row_from_record(rec)


async def increment_batch_failures(
    conn: ConnLike,
    sql: BatchSql,
    batch_id: UUID,
) -> tuple[int, int | None, int]:
    """Atomically increment consecutive_failures and return the new count,
    the batch's failure_threshold, and the number of non-terminal member jobs.

    Returns ``(0, None, 0)`` if the batch row does not exist.
    """
    rec = await conn.fetchrow(
        sql.increment_batch_failures,
        batch_id,
        _batch_filter_json(batch_id),
    )
    if rec is None:
        return (0, None, 0)
    return (rec["consecutive_failures"], rec["failure_threshold"], rec["remaining"])


async def reset_batch_failures(
    conn: ConnLike,
    sql: BatchSql,
    batch_id: UUID,
) -> int:
    """Reset consecutive_failures to 0 and return the number of non-terminal
    member jobs.

    Returns ``0`` if the batch row does not exist.
    """
    rec = await conn.fetchrow(
        sql.reset_batch_failures,
        batch_id,
        _batch_filter_json(batch_id),
    )
    if rec is None:
        return 0
    return rec["remaining"]


async def abort_batch(
    conn: ConnLike,
    sql: BatchSql,
    batch_id: UUID,
) -> int:
    """Cancel all pending/scheduled member jobs and mark the batch as aborted.

    Returns the number of jobs cancelled.

    The two statements (cancel jobs + update batch row) are wrapped in a
    transaction so they commit atomically even when *conn* is a caller-
    supplied loop connection that is not already inside an explicit
    transaction.  asyncpg nested transactions use savepoints, so this is
    safe when the caller already has an outer transaction.
    """
    async with conn.transaction():
        rows = await conn.fetch(sql.abort_batch_jobs, _batch_filter_json(batch_id))
        await conn.execute(sql.abort_batch_row, batch_id)
        return len(rows)


async def complete_batch(
    conn: ConnLike,
    sql: BatchSql,
    batch_id: UUID,
) -> None:
    """Mark a batch as complete.  No-op if the batch is already terminal."""
    await conn.execute(sql.complete_batch, batch_id)


async def count_batch_non_terminal(
    conn: ConnLike,
    sql: BatchSql,
    batch_id: UUID,
) -> int:
    """Count non-terminal member jobs for a batch."""
    return await conn.fetchval(sql.count_batch_non_terminal, _batch_filter_json(batch_id))


async def list_batches(
    conn: ConnLike,
    sql: BatchSql,
    filter: BatchFilter,
) -> list[tuple[BatchRow, BatchCounts]]:
    """List batches with live job-count aggregates, filtered by the given
    :class:`BatchFilter`.
    """
    parts: list[str] = []
    params: list[object] = []
    idx = 1

    if filter.queue is not None:
        parts.append(f"AND b.queue = ${idx}")
        params.append(filter.queue)
        idx += 1

    if filter.active is not None:
        if filter.active:
            parts.append("AND b.status = 'active'")
        else:
            parts.append("AND b.status IN ('complete', 'aborted')")

    if filter.batch_id is not None:
        parts.append(f"AND b.id = ${idx}")
        params.append(filter.batch_id)
        idx += 1

    parts.append(f"ORDER BY b.created_at DESC LIMIT ${idx}")
    params.append(filter.limit)

    full_sql = sql.list_batches_base + " " + " ".join(parts)
    rows = await conn.fetch(full_sql, *params)
    return [(_batch_row_from_record(r), _batch_counts_from_record(r)) for r in rows]


async def prune_old_batches(
    conn: ConnLike,
    sql: BatchSql,
    cutoff: datetime,
) -> int:
    """Delete completed batches older than *cutoff* that have no remaining
    member jobs.  Returns the number of rows deleted.
    """
    rows = await conn.fetch(sql.prune_old_batches, cutoff)
    return len(rows)


async def enqueue_batch_atomic(
    pool: "asyncpg.Pool",
    schema: str,
    sql: SqlTemplates,
    batch_sql: BatchSql,
    clock: Clock,
    items: Iterable[EnqueueArgs],
    *,
    batch_id: UUID,
    queue: str,
    batch_row: BatchRow | None,
    finalizer_args: EnqueueArgs | None,
    chunk_size: int = 1000,
) -> list[JobRow]:
    """Enqueue all items in a single transaction, stamping each with
    ``metadata.batch_id``.  Optionally insert a batch row and enqueue a
    finalizer job as the LAST statements.

    The finalizer is NOT stamped with ``batch_id`` metadata (deadlock
    prevention — see spec §5.4).

    Consumes the iterable lazily in chunks of *chunk_size* — never
    materializes the full list.  On any exception (including generator
    failure mid-stream) the transaction is rolled back and the exception
    re-raised (MEDIUM-4).
    """
    batch_id_str = str(batch_id)
    all_rows: list[JobRow] = []
    item_count = 0

    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            it = iter(items)
            while True:
                chunk_raw = list(islice(it, chunk_size))
                if not chunk_raw:
                    break
                item_count += len(chunk_raw)
                chunk = [
                    replace(
                        args,
                        metadata={**args.metadata, "batch_id": batch_id_str},
                    )
                    for args in chunk_raw
                ]
                rows = await _enqueue_batch(
                    pool,
                    sql,
                    schema,
                    clock,
                    chunk,
                    connection=cast("asyncpg.Connection | None", conn),
                )
                all_rows.extend(rows)

            # Insert finalizer BEFORE creating the batch row so the returned
            # row's id can be used for finalizer_job_id (M4: idempotency
            # collision may return a different id than finalizer_args.id).
            finalizer_row: JobRow | None = None
            if finalizer_args is not None:
                fin_rows = await _enqueue_batch(
                    pool,
                    sql,
                    schema,
                    clock,
                    [finalizer_args],
                    connection=cast("asyncpg.Connection | None", conn),
                )
                all_rows.extend(fin_rows)
                finalizer_row = fin_rows[0]

            if batch_row is not None:
                # H6: when expected_size is 0 (streaming sentinel), use the
                # actual item count consumed from the iterable.
                expected_size = (
                    batch_row.expected_size if batch_row.expected_size > 0 else item_count
                )
                finalizer_job_id = (
                    finalizer_row.id if finalizer_row is not None else batch_row.finalizer_job_id
                )
                try:
                    await conn.execute(
                        batch_sql.create_batch,
                        batch_row.id,
                        batch_row.queue,
                        expected_size,
                        batch_row.failure_threshold,
                        finalizer_job_id,
                        batch_row.originating_actor,
                    )
                except UniqueViolationError as exc:
                    from taskq.exceptions import BatchIdExistsError

                    raise BatchIdExistsError(batch_row.id) from exc

            await tx.commit()
        except BaseException:
            await tx.rollback()
            raise

    return all_rows
