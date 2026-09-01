"""Bulk cancel SQL implementation for PostgresBackend.

Two-statement pattern within a single transaction, mirroring the
single-job ``write_cancel_request`` path:

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

Events are inserted via ``executemany`` within the same transaction.
NOTIFY is sent by the caller (``PostgresBackend.cancel_where``) after
commit because the ``taskq.cancel.notify_sent`` counter lives in
``postgres.py``.
"""

import asyncio
import random
from typing import NamedTuple
from uuid import UUID

import asyncpg

from taskq.backend._filter_sql import build_filter_conditions
from taskq.backend._protocol import BulkCancelResult, JobFilter
from taskq.backend._records import jsonb_param
from taskq.backend._sql_templates import SqlTemplates
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
)

__all__ = ["_cancel_where"]


class NotifyTarget(NamedTuple):
    """A running job that needs a post-commit NOTIFY."""

    job_id: UUID
    worker_id: UUID


async def _cancel_where(
    pool: asyncpg.Pool,
    schema: str,
    sql: SqlTemplates,
    filter: JobFilter,
    reason: str | None,
) -> tuple[BulkCancelResult, list[NotifyTarget]]:
    # Defence-in-depth: re-validate the schema identifier at the call site
    # (docs/architecture.md §Identifier validation) — construction-time
    # validation alone is single-point.
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    filter_sql = build_filter_conditions(filter)
    conditions_str = " AND ".join(filter_sql.conditions) if filter_sql.conditions else "TRUE"
    params = list(filter_sql.params)

    # Statement 1: cancel pending/scheduled → terminal 'cancelled'
    # EPQ-safe: predicates on the target table (j.status) are re-evaluated
    # for concurrently-modified rows.
    cancel_ps_sql = f"""
    WITH matching AS (
        SELECT id, status
        FROM "{schema}".jobs
        WHERE {conditions_str}
          AND status IN ('pending', 'scheduled')
        ORDER BY id
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
        (SELECT count(*)::int FROM cancelled) AS cancelled_directly,
        (SELECT array_agg(id ORDER BY id) FROM cancelled) AS cancelled_ids,
        (SELECT array_agg(prev_status ORDER BY id) FROM cancelled) AS cancelled_prev_statuses
    """

    # Statement 2: cooperative cancel for running jobs with cancel_phase=0
    # Fresh snapshot — catches jobs dispatched between statements 1 and 2.
    cancel_running_sql = f"""
    WITH matching AS (
        SELECT id, locked_by_worker
        FROM "{schema}".jobs
        WHERE {conditions_str}
          AND status = 'running'
          AND cancel_phase = 0
        ORDER BY id
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
        (SELECT count(*)::int FROM cancel_requested) AS cancel_requested,
        (SELECT array_agg(id ORDER BY id) FROM cancel_requested) AS cancel_requested_ids,
        (SELECT array_agg(locked_by_worker ORDER BY id) FROM cancel_requested) AS cancel_requested_workers
    """

    cr_detail = jsonb_param({"reason": reason} if reason is not None else {})

    cancelled_ids: list[UUID] = []
    cancel_requested_ids: list[UUID] = []
    notify_targets: list[NotifyTarget] = []

    for attempt in range(3):
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    # Statement 1: pending/scheduled → cancelled
                    ps_row = await conn.fetchrow(cancel_ps_sql, *params)
                    if ps_row is not None:
                        cancelled_ids = list(ps_row["cancelled_ids"] or [])
                        prev_statuses: dict[UUID, str] = dict(
                            zip(
                                cancelled_ids,
                                ps_row["cancelled_prev_statuses"] or [],
                                strict=True,
                            )
                        )

                        if cancelled_ids:
                            await conn.executemany(
                                sql.insert_event,
                                [
                                    (
                                        jid,
                                        "state_change",
                                        jsonb_param(
                                            {
                                                "from_state": prev_statuses[jid],
                                                "to_state": "cancelled",
                                            }
                                        ),
                                    )
                                    for jid in cancelled_ids
                                ],
                            )
                            await conn.executemany(
                                sql.insert_event,
                                [(jid, "cancel_request", cr_detail) for jid in cancelled_ids],
                            )

                    # Statement 2: running → cooperative cancel (fresh snapshot)
                    running_row = await conn.fetchrow(cancel_running_sql, *params)
                    if running_row is not None:
                        cancel_requested_ids = list(running_row["cancel_requested_ids"] or [])
                        notify_targets = [
                            NotifyTarget(job_id=jid, worker_id=wid)
                            for jid, wid in zip(
                                cancel_requested_ids,
                                running_row["cancel_requested_workers"] or [],
                                strict=True,
                            )
                            if wid is not None
                        ]

                        if cancel_requested_ids:
                            await conn.executemany(
                                sql.insert_event,
                                [
                                    (jid, "cancel_request", cr_detail)
                                    for jid in cancel_requested_ids
                                ],
                            )
            break
        except asyncpg.DeadlockDetectedError:
            if attempt == 2:
                raise
            await asyncio.sleep(0.1 * (2**attempt) + random.random() * 0.05)

    result = BulkCancelResult(
        cancelled_directly=len(cancelled_ids),
        cancel_requested=len(cancel_requested_ids),
        cancelled_ids=tuple(cancelled_ids),
        cancel_requested_ids=tuple(cancel_requested_ids),
    )
    return result, notify_targets
