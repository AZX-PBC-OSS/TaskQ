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

import structlog
from asyncpg.exceptions import LockNotAvailableError, UniqueViolationError

from taskq._json import dumps_str
from taskq.backend._cursor import decode_batch_cursor
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
from taskq.backend.statemachine import ACTIVE_STATUSES, TERMINAL_STATUSES
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_CHUNK_SIZE,
)
from taskq.obs import get_logger

if TYPE_CHECKING:
    import asyncpg

logger: structlog.stdlib.BoundLogger = get_logger(__name__)

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

_ACTIVE_IN = "IN (" + ", ".join(f"'{s}'" for s in sorted(ACTIVE_STATUSES)) + ")"


def _open_member_where(batch_id_param: int) -> str:
    """The open-member probe: "a member of the batch bound at ``$N`` (as
    text) that is not terminal".

    Spelled so jobs_batch_open_members_idx (01.00.13_03) serves it: the
    batch id is the index's expression key, and the positive, sorted
    status list is the index predicate's exact text, which is what lets
    the planner prove the partial index applies. Every per-terminal-write
    question about open members goes through this one predicate; the
    ``metadata @>`` containment form stays only for the statements that
    must touch terminal members too (abort's cancel, list counts, prune).
    """
    return f"(metadata->>'batch_id') = ${batch_id_param}\n      AND status {_ACTIVE_IN}"


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

# Both counter writes run on every batched job's terminal write, so the
# member count they return must not walk the batch: the counts CTE is
# the open-member predicate, which jobs_batch_open_members_idx serves as
# one index range over the members still open — never the terminal
# history — so its cost tracks what remains, not what the batch was.
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
    WHERE {open_member}
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
    WHERE {open_member}
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

# The NOT EXISTS guard arbitrates completion server-side, in this
# statement's own snapshot: the increment/reset counts CTE runs in ITS
# statement's READ COMMITTED snapshot, which after a batches-row lock
# wait can predate a concurrent member's terminal write, so a count of
# zero from the caller is never the completion decision. The guard shape is
# the one complete_stale_batches already uses (worker/_leader_shared.py);
# the status = 'active' sibling condition keeps abort-wins-over-complete
# intact. The probe itself is the open-member predicate served by
# jobs_batch_open_members_idx, so the guard costs one index seek per
# terminal write however many members the batch has.
#
# The membership CTE closes the append-race window the guard alone cannot
# see: a READ COMMITTED snapshot cannot see another transaction's
# uncommitted member INSERT (the streaming-append path -- a caller-
# supplied batch_id of an existing batch, members committed chunk by
# chunk, each chunk transaction holding this same batches-row lock from
# before its INSERTs to its commit -- see _enqueue.py's
# _lock_batch_membership), so the guard alone would complete the batch
# and the append would then commit a pending member onto a terminal row.
# FOR UPDATE NOWAIT makes the conflict itself the signal: an in-flight
# append holds the row, this statement raises LockNotAvailableError
# (SQLSTATE 55P03), and complete_batch() treats that as a DELAY -- the
# docstring's own "can delay completion but never complete prematurely"
# contract -- leaving the row 'active' for the append to commit and the
# next hook or the stale-batch sweep to re-arbitrate. NOWAIT, not a
# blocking wait, is load-bearing: the completer runs on the worker's
# terminal connection inside the caller's open transaction, and blocking
# here would park a terminal write behind an appender of unbounded
# duration. The lock is held to this statement's commit, so an appender
# arriving after it serializes behind the completion instead of racing
# it. A batch row that does not exist locks nothing: EXISTS fails and
# the UPDATE no-ops exactly as it did before the CTE.
_COMPLETE_BATCH_SQL = """\
WITH membership AS (
    SELECT id
    FROM "{schema}".batches
    WHERE id = $1
    FOR UPDATE NOWAIT
)
UPDATE "{schema}".batches
SET status = 'complete', completed_at = clock_timestamp()
WHERE id = $1 AND status = 'active'
  AND EXISTS (SELECT 1 FROM membership)
  AND NOT EXISTS (
    SELECT 1 FROM "{schema}".jobs
    WHERE {open_member}
  )"""

_COUNT_BATCH_NON_TERMINAL_SQL = """\
SELECT count(*)::int FROM "{schema}".jobs
WHERE {open_member}"""

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

# Bounded batch + MATERIALIZED, the _COMPLETE_STALE_BATCHES_SQL shape
# (worker/_leader_shared.py — same table family, same correlated NOT EXISTS
# member-probe): LIMIT $2 caps one call's DELETE, and MATERIALIZED stops the
# planner from inlining the LIMIT-ed CTE into the DELETE in a way that could
# remove more rows than the LIMIT. No ORDER BY: batches cardinality grows
# with batch usage, not job volume, so the window's scan is cheap however it
# plans — the same rationale the stale-batch completion sweep documents. The
# count comes from a COUNT over the DELETE's RETURNING set rather than
# materialising ids the caller only counts.
_PRUNE_OLD_BATCHES_SQL = """\
WITH candidate AS MATERIALIZED (
    SELECT id
    FROM "{schema}".batches
    WHERE completed_at IS NOT NULL
      AND completed_at < $1
      AND NOT EXISTS (
        SELECT 1 FROM "{schema}".jobs j
        WHERE j.metadata @> jsonb_build_object('batch_id', batches.id::text)
      )
    LIMIT $2
),
deleted AS (
    DELETE FROM "{schema}".batches
    WHERE id IN (SELECT id FROM candidate)
    RETURNING id
)
SELECT count(*)::int FROM deleted"""


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
            schema=schema, open_member=_open_member_where(batch_id_param=2)
        ),
        reset_batch_failures=_RESET_BATCH_FAILURES_SQL.format(
            schema=schema, open_member=_open_member_where(batch_id_param=2)
        ),
        abort_batch_jobs=_ABORT_BATCH_JOBS_SQL.format(schema=schema),
        abort_batch_row=_ABORT_BATCH_ROW_SQL.format(schema=schema),
        complete_batch=_COMPLETE_BATCH_SQL.format(
            schema=schema, open_member=_open_member_where(batch_id_param=2)
        ),
        count_batch_non_terminal=_COUNT_BATCH_NON_TERMINAL_SQL.format(
            schema=schema, open_member=_open_member_where(batch_id_param=1)
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

    Returns ``(0, None, 0)`` if the batch row does not exist. The member
    count is the same index-served probe as :func:`count_batch_non_terminal`.
    """
    rec = await conn.fetchrow(sql.increment_batch_failures, batch_id, str(batch_id))
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

    Returns ``0`` if the batch row does not exist. The member count is the
    same index-served probe as :func:`count_batch_non_terminal`.
    """
    rec = await conn.fetchrow(sql.reset_batch_failures, batch_id, str(batch_id))
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
    """Mark a batch as complete.  No-op if the batch is already terminal
    or any member job is still non-terminal.

    Completion is arbitrated inside this statement: the ``NOT EXISTS``
    guard counts non-terminal members in the statement's own snapshot,
    so a caller acting on a stale count (two members terminating
    concurrently can each read the other as non-terminal) can delay
    completion but never complete prematurely — an optimistic attempt
    after any terminal member is always safe, and the same attempt that
    was vetoed lands once the last member turns terminal.

    Delay also covers the member-append window: the statement's
    membership CTE takes the batches row ``FOR UPDATE NOWAIT``, and a
    concurrent append transaction holding that lock (the streaming chunk
    path — see ``_COMPLETE_BATCH_SQL``'s comment) makes the statement
    raise :class:`asyncpg.exceptions.LockNotAvailableError`. That is a
    DELAY, not an error: a READ COMMITTED snapshot cannot see the
    appender's uncommitted member INSERT, so completing now would be
    precisely the premature completion the guard exists to prevent. The
    row stays ``'active'``, the append commits, and the next terminal
    hook or the leader's ``complete_stale_batches`` sweep re-arbitrates
    against the now-visible membership.
    """
    try:
        await conn.execute(sql.complete_batch, batch_id, str(batch_id))
    except LockNotAvailableError:
        # Delayed on the membership lock — see the docstring. Debug, not
        # warning: this is the same optimistic-CAS miss class as the
        # guard's own veto (a concurrent writer won the arbitration), an
        # expected outcome under concurrency that reconciliation already
        # covers; the log line exists so a delayed completion is
        # traceable to its cause when someone asks why a batch with all
        # terminal members is still 'active'.
        logger.debug(
            "complete_batch_delayed_membership_lock",
            kind="batch",
            batch_id=str(batch_id),
        )


async def count_batch_non_terminal(
    conn: ConnLike,
    sql: BatchSql,
    batch_id: UUID,
) -> int:
    """Count non-terminal member jobs for a batch."""
    return await conn.fetchval(sql.count_batch_non_terminal, str(batch_id))


async def list_batches(
    conn: ConnLike,
    sql: BatchSql,
    filter: BatchFilter,
) -> list[tuple[BatchRow, BatchCounts]]:
    """List batches with live job-count aggregates, filtered by the given
    :class:`BatchFilter`.

    Keyset-paginated on ``(created_at, id)`` DESC via ``filter.cursor``
    (see :func:`~taskq.backend._cursor.encode_batch_cursor`), mirroring
    ``_list_jobs``. Both columns are ordered in the same direction, so
    the seam is one row-wise comparison and the ordering is total --
    ``created_at`` alone is not, because it defaults to ``now()``, the
    transaction timestamp.
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

    if filter.cursor is not None:
        # Why a row-wise comparison and not `created_at < $n OR (= AND id <
        # $m)`: one tuple compare is what stays correct once `id` is ordered
        # WITH the primary column rather than against it, which is the shape
        # the admin keyset settled on in 2569da5. `id` is UUIDv7 and
        # therefore time-ordered, so `id DESC` agrees with `created_at DESC`
        # instead of fighting it -- the tiebreaker only decides rows written
        # inside one transaction, where `now()` stamps them all identically.
        #
        # decode_batch_cursor returns a datetime and a UUID, never the raw
        # text: asyncpg types each placeholder from its `::` cast and refuses
        # a str for timestamptz/uuid, the DataError that 500'd every admin
        # job-list page turn before that same fix.
        cursor_created_at, cursor_id = decode_batch_cursor(filter.cursor)
        parts.append(f"AND (b.created_at, b.id) < (${idx}::timestamptz, ${idx + 1}::uuid)")
        params.extend([cursor_created_at, cursor_id])
        idx += 2

    parts.append(f"ORDER BY b.created_at DESC, b.id DESC LIMIT ${idx}")
    params.append(filter.limit)

    full_sql = sql.list_batches_base + " " + " ".join(parts)
    rows = await conn.fetch(full_sql, *params)
    return [(_batch_row_from_record(r), _batch_counts_from_record(r)) for r in rows]


async def prune_old_batches(
    conn: ConnLike,
    sql: BatchSql,
    cutoff: datetime,
    *,
    batch_size: int = DEFAULT_CHUNK_SIZE,
) -> int:
    """Delete completed batches older than *cutoff* that have no remaining
    member jobs, one bounded batch at a time, and return the total number
    of rows deleted.

    Each call of the underlying statement deletes at most *batch_size*
    rows (the ``_COMPLETE_STALE_BATCHES_SQL`` windowing shape); this
    function drains the eligible set by repeating it until a window comes
    back short, so one call still reports the whole day's deletion count.
    Every statement is self-committing, so a drain stopped by an error
    keeps its progress and the next call resumes the remainder. The count
    comes from the statement itself (a COUNT over its RETURNING set), not
    from materialising ids only to count them.
    """
    total = 0
    while True:
        count: int = await conn.fetchval(sql.prune_old_batches, cutoff, batch_size)
        if count == 0:
            break
        total += count
        if count < batch_size:
            break
    return total


async def enqueue_batch_atomic(
    pool: "asyncpg.Pool",
    schema: str,
    sql: SqlTemplates,
    batch_sql: BatchSql,
    items: Iterable[EnqueueArgs],
    *,
    batch_id: UUID,
    queue: str,
    batch_row: BatchRow | None,
    finalizer_args: EnqueueArgs | None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
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

    Why *queue* is unused here: this implementation takes each job's queue
    from ``args.queue`` and the batch row's from *batch_row*, so the
    batch-level queue is already carried by both. It is part of the
    ``Backend.enqueue_batch_atomic`` protocol signature and the InMemory
    implementation does read it, so it stays.
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
                # The chunk's base in the CALLER's coordinate space: the
                # consumed prefix BEFORE this chunk. Captured before the
                # count bump so the bulk core's per-item annotations (the
                # jsonb NUL guard) name STREAM-GLOBAL indices — a
                # chunk-local index from inside this loop is unfixable at
                # the client layer, which cannot know the backend's chunk
                # base (the streaming boundary's registry can only shift
                # errors it sees cross its own per-chunk call, and this
                # re-chunk happens entirely below that boundary).
                chunk_base = item_count
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
                    chunk,
                    connection=cast("asyncpg.Connection | None", conn),
                    # Explicit, not defaulted: these chunks share the atomic
                    # transaction, so per-chunk admission accumulates to the
                    # true aggregate — a default flip must not silently
                    # disarm it.
                    enforce_max_pending=True,
                    # Why whole-call refusal: every chunk runs inside ONE
                    # shared transaction, so the partition's
                    # insert-then-raise would insert this chunk's admitted
                    # items only for the wrapper's rollback to discard
                    # them — a misleading non-admission. All-or-nothing is
                    # the atomic path's documented contract; the refusal
                    # raises here as plain MaxPendingExceededError before
                    # any INSERT, and the rollback discards earlier chunks.
                    refuse_whole_batch_on_cap=True,
                    index_base=chunk_base,
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
                    [finalizer_args],
                    connection=cast("asyncpg.Connection | None", conn),
                    # Same all-or-nothing arm as the chunks above: a capped
                    # finalizer actor must abort the whole atomic batch,
                    # not raise a one-item partition refusal.
                    enforce_max_pending=True,
                    refuse_whole_batch_on_cap=True,
                    # The finalizer's caller-global coordinate: one past
                    # the last stream item (it is the (N+1)th enqueue this
                    # call performs). index 0 — the pre-fix annotation —
                    # falsely accused an innocent stream item.
                    index_base=item_count,
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
