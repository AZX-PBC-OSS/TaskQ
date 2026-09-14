"""Bulk cancel SQL implementation for PostgresBackend.

Two-statement pattern — pending/scheduled first, then running — mirroring
the single-job ``write_cancel_request`` path:

1. ``cancel_pending_scheduled`` — UPDATE pending/scheduled rows to
   terminal ``cancelled`` with EPQ-safe predicates on the target table.
2. ``cancel_running`` — UPDATE running rows with ``cancel_phase=0`` to
   ``cancel_phase=1`` (cooperative cancel), using a fresh snapshot that
   catches jobs dispatched between statements.

The two-statement approach eliminates the race where a job transitioning
``pending→running`` mid-statement escapes both CTEs in a single-shot
design: statement 1's EPQ guard rejects the now-running row, and
statement 2's fresh snapshot sees it as running and sets
``cancel_phase=1``.

Each statement still cancels EVERY matching job, and one call still
returns the complete :class:`BulkCancelResult` plus NOTIFY targets — but
the work executes as a sequence of bounded committed batches
(``batch_size`` driving-CTE rows per transaction), not one unbounded
transaction. The drain terminates on the WINDOW count — the
``matched_count`` aggregate each driving statement returns from its own
MATERIALIZED ``matching`` CTE — never on the UPDATE's affected-row
count: under READ COMMITTED a row windowed by the CTE that a dispatcher
claims (``pending→running``) between the statement's snapshot and the
UPDATE's row lock fails the EPQ re-check on the target and drops out of
the affected count, so an affected-count termination would abandon the
tail of the match set while the window was still full. A batch whose
window was full means more matching rows may remain, so the drain keeps
going; the affected count drives only the result totals and the event
writes. A mid-operation failure therefore leaves partial progress
rather than rolling everything back: a re-run continues where it
stopped, because the EPQ predicates skip the rows earlier committed
batches already cancelled. Each batch's ``job_events`` rows are written
by the same bounded transaction as their driving UPDATE, and the batch
carries a server-side ``statement_timeout`` (``SET LOCAL`` semantics,
the same capture/restore discipline the maintenance sweeps use), so the
INSERT-to-COMMIT span is not merely kept inside the margin
:data:`taskq.constants.RECLAIM_EVENT_VISIBILITY_DELAY` documents — the
timeout is the enforcement — with one batched ``unnest`` INSERT per
event kind, never one round trip per row.

NOTIFY is sent by the caller (``PostgresBackend.cancel_where``) after the
drain completes, because the ``taskq.cancel.notify_sent`` counter lives
in ``postgres.py``.
"""

import asyncio
import random
from collections.abc import Awaitable, Callable
from typing import NamedTuple
from uuid import UUID

import asyncpg

from taskq.backend._filter_sql import build_filter_conditions
from taskq.backend._protocol import BulkCancelResult, ConnLike, JobFilter
from taskq.backend._records import jsonb_param
from taskq.backend._sql import INSERT_EVENTS_DETAIL_BATCH_SQL
from taskq.backend._sql_templates import SqlTemplates
from taskq.backend._sweeps import (
    _apply_batch_statement_timeout,  # pyright: ignore[reportPrivateUsage]  # Why: the batch statement_timeout capture/restore is shared verbatim by every event-writer batch path; re-defining it here would let the two disciplines drift.
    _restore_statement_timeout,  # pyright: ignore[reportPrivateUsage]  # Why: same shared-discipline rationale as _apply_batch_statement_timeout.
    _validate_positive,  # pyright: ignore[reportPrivateUsage]  # Why: the canonical pre-SQL bound validation, shared with the sweeps and deregistration.
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
)

__all__ = ["_cancel_where"]


class NotifyTarget(NamedTuple):
    """A running job that needs a post-commit NOTIFY."""

    job_id: UUID
    worker_id: UUID


async def _drain_cancel_batches(
    pool: asyncpg.Pool,
    statement: str,
    params: list[object],
    batch_size: int,
    statement_timeout_ms: int,
    handle_batch: Callable[[ConnLike, asyncpg.Record], Awaitable[None]],
) -> None:
    """Execute *statement* as bounded committed batches until the match set
    is drained.

    Each iteration is one committed transaction: the driving CTE (limited
    to *batch_size* rows) plus whatever event writes *handle_batch* issues
    on the same connection, so a batch's events can never commit without
    the state change they describe. The transaction carries a server-side
    ``statement_timeout`` bound via ``set_config(..., true)`` (``SET
    LOCAL`` semantics with a bindable value) using the same
    capture/restore discipline as the maintenance sweeps: the previous
    value is restored on the success path inside the still-open
    transaction, and the error path needs no restore because the
    rollback discards a ``SET LOCAL``.

    Termination keys on the WINDOW count — the ``matched_count``
    aggregate the statement returns from its own MATERIALIZED
    ``matching`` CTE — never on the UPDATE's affected-row count: under
    READ COMMITTED a row windowed by the CTE that a dispatcher claims
    (``pending→running``) between the statement's snapshot and the
    UPDATE's row lock fails the EPQ re-check and drops out of the
    affected count, so an affected-count termination abandons the tail
    of the match set while the window was still full. A full window
    means more matching rows may remain, so the drain keeps going; the
    affected count drives only the result totals and the event writes.

    Deadlock is retried per batch (3 attempts, exponential backoff with
    jitter). *handle_batch* must append its ids only after its event
    writes succeed, so a deadlocked batch contributes no phantom ids; the
    retry re-runs the CTE, which no longer matches rows an earlier
    committed batch cancelled (EPQ predicates) but does re-match this
    batch's rolled-back rows — progress is never lost and nothing is
    counted twice.
    """
    while True:
        matched_count = 0
        for attempt in range(3):
            try:
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        prev_timeout = await _apply_batch_statement_timeout(
                            conn, statement_timeout_ms
                        )
                        row = await conn.fetchrow(statement, *params, batch_size)
                        if row is not None:
                            matched_count = int(row["matched_count"])
                            await handle_batch(conn, row)
                        # Success path only: restore the caller's timeout
                        # inside the still-open transaction; on error the
                        # rollback has already discarded the SET LOCAL.
                        await _restore_statement_timeout(conn, prev_timeout)
                break
            except asyncpg.DeadlockDetectedError:
                if attempt == 2:
                    raise
                await asyncio.sleep(0.1 * (2**attempt) + random.random() * 0.05)
        if matched_count < batch_size:
            return


async def _cancel_where(
    pool: asyncpg.Pool,
    schema: str,
    sql: SqlTemplates,  # kept: the backend caller holds the pre-rendered bundle and passes it through; the batched event INSERT below renders from the schema directly
    filter: JobFilter,
    reason: str | None,
    batch_size: int = DEFAULT_EVENT_WRITER_BATCH_SIZE,
    statement_timeout_ms: int = DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
) -> tuple[BulkCancelResult, list[NotifyTarget]]:
    # Defence-in-depth: re-validate the schema identifier at the call site
    # (docs/architecture.md §Identifier validation) — construction-time
    # validation alone is single-point.
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    # LIMIT 0 is a legal rowless query that would otherwise stall the
    # drain forever (an empty window never falls below it); a zero
    # statement_timeout disables the batch's safety net outright.
    _validate_positive("batch_size", batch_size)
    _validate_positive("statement_timeout_ms", statement_timeout_ms)

    filter_sql = build_filter_conditions(filter)
    conditions_str = " AND ".join(filter_sql.conditions) if filter_sql.conditions else "TRUE"
    params = list(filter_sql.params)
    # The filter params occupy $1..$n; the batch LIMIT binds as the next
    # positional parameter, appended after them at every execute site.
    limit_ph = len(params) + 1

    # Statement 1: cancel pending/scheduled → terminal 'cancelled'
    # EPQ-safe: predicates on the target table (j.status) are re-evaluated
    # for concurrently-modified rows.
    cancel_ps_sql = f"""
    -- MATERIALIZED is load-bearing: without it the planner may inline the
    -- LIMIT-ed matching CTE into the UPDATE as a nested loop and update
    -- more rows than the LIMIT admits.
    WITH matching AS MATERIALIZED (
        SELECT id, status
        FROM "{schema}".jobs
        WHERE {conditions_str}
          AND status IN ('pending', 'scheduled')
        ORDER BY id
        LIMIT ${limit_ph}
    ),
    cancelled AS (
        UPDATE "{schema}".jobs AS j
        SET status = 'cancelled', finished_at = clock_timestamp()
        FROM (
            SELECT id, status AS prev_status
            FROM matching
        ) AS prev
        WHERE j.id = prev.id
          AND j.status IN ('pending', 'scheduled')
        RETURNING j.id, prev.prev_status
    )
    SELECT
        (SELECT count(*)::int FROM matching) AS matched_count,
        (SELECT count(*)::int FROM cancelled) AS cancelled_directly,
        (SELECT array_agg(id ORDER BY id) FROM cancelled) AS cancelled_ids,
        (SELECT array_agg(prev_status ORDER BY id) FROM cancelled) AS cancelled_prev_statuses
    """

    # Statement 2: cooperative cancel for running jobs with cancel_phase=0
    # Fresh snapshot — catches jobs dispatched between statements 1 and 2.
    cancel_running_sql = f"""
    -- MATERIALIZED is load-bearing: without it the planner may inline the
    -- LIMIT-ed matching CTE into the UPDATE as a nested loop and update
    -- more rows than the LIMIT admits.
    WITH matching AS MATERIALIZED (
        SELECT id, locked_by_worker
        FROM "{schema}".jobs
        WHERE {conditions_str}
          AND status = 'running'
          AND cancel_phase = 0
        ORDER BY id
        LIMIT ${limit_ph}
    ),
    cancel_requested AS (
        UPDATE "{schema}".jobs AS j
        SET cancel_requested_at = clock_timestamp(), cancel_phase = 1
        FROM (
            SELECT id, locked_by_worker
            FROM matching
        ) AS prev
        WHERE j.id = prev.id
          AND j.status = 'running'
          AND j.cancel_phase = 0
        RETURNING j.id, prev.locked_by_worker
    )
    SELECT
        (SELECT count(*)::int FROM matching) AS matched_count,
        (SELECT count(*)::int FROM cancel_requested) AS cancel_requested,
        (SELECT array_agg(id ORDER BY id) FROM cancel_requested) AS cancel_requested_ids,
        (SELECT array_agg(locked_by_worker ORDER BY id) FROM cancel_requested) AS cancel_requested_workers
    """

    event_batch_sql = INSERT_EVENTS_DETAIL_BATCH_SQL.format(schema=schema)
    cr_detail = jsonb_param({"reason": reason} if reason is not None else {})

    cancelled_ids: list[UUID] = []
    cancel_requested_ids: list[UUID] = []
    notify_targets: list[NotifyTarget] = []

    async def _handle_ps_batch(conn: ConnLike, row: asyncpg.Record) -> None:
        count = int(row["cancelled_directly"])
        if count == 0:
            return
        batch_ids: list[UUID] = list(row["cancelled_ids"] or [])
        prev_statuses: dict[UUID, str] = dict(
            zip(batch_ids, list(row["cancelled_prev_statuses"] or []), strict=True)
        )
        # Per-row detail: from_state is each job's own pre-cancel status —
        # sharing one detail across the unnest would stamp a single
        # from_state on every event in a mixed pending/scheduled batch.
        await conn.execute(
            event_batch_sql,
            batch_ids,
            [
                jsonb_param({"from_state": prev_statuses[jid], "to_state": "cancelled"})
                for jid in batch_ids
            ],
            "state_change",
        )
        # The cancel_request detail is uniform across the batch; the same
        # two-column template keeps the write one statement per batch.
        await conn.execute(
            event_batch_sql,
            batch_ids,
            [cr_detail] * len(batch_ids),
            "cancel_request",
        )
        # Appended only now, after both event writes succeeded: a batch
        # that deadlocks mid-write must contribute no phantom ids.
        cancelled_ids.extend(batch_ids)

    async def _handle_running_batch(conn: ConnLike, row: asyncpg.Record) -> None:
        count = int(row["cancel_requested"])
        if count == 0:
            return
        batch_ids: list[UUID] = list(row["cancel_requested_ids"] or [])
        workers: list[UUID | None] = list(row["cancel_requested_workers"] or [])
        await conn.execute(
            event_batch_sql,
            batch_ids,
            [cr_detail] * len(batch_ids),
            "cancel_request",
        )
        # Same no-phantom rule as the pending/scheduled phase.
        cancel_requested_ids.extend(batch_ids)
        notify_targets.extend(
            NotifyTarget(job_id=jid, worker_id=wid)
            for jid, wid in zip(batch_ids, workers, strict=True)
            if wid is not None
        )

    await _drain_cancel_batches(
        pool, cancel_ps_sql, params, batch_size, statement_timeout_ms, _handle_ps_batch
    )
    await _drain_cancel_batches(
        pool, cancel_running_sql, params, batch_size, statement_timeout_ms, _handle_running_batch
    )

    result = BulkCancelResult(
        cancelled_directly=len(cancelled_ids),
        cancel_requested=len(cancel_requested_ids),
        cancelled_ids=tuple(cancelled_ids),
        cancel_requested_ids=tuple(cancel_requested_ids),
    )
    return result, notify_targets
