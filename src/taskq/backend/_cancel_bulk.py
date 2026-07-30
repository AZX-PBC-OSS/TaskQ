"""Bulk cancel SQL implementation for PostgresBackend.

Module-level function following the same pattern as ``_reads.py``,
``_terminal.py``, etc. The SQL uses two CTEs in a single statement
with EPQ-safe predicates duplicated in each UPDATE's WHERE clause.
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
    filter_sql = build_filter_conditions(filter)
    conditions_str = " AND ".join(filter_sql.conditions) if filter_sql.conditions else "TRUE"
    params = list(filter_sql.params)

    cancel_sql = f"""
    WITH matching AS (
        SELECT id, status, locked_by_worker
        FROM "{schema}".jobs
        WHERE {conditions_str}
        ORDER BY id
    ),
    cancelled AS (
        UPDATE "{schema}".jobs AS j
        SET status = 'cancelled', finished_at = clock_timestamp()
        FROM (
            SELECT id, status AS prev_status
            FROM matching
            WHERE status IN ('pending', 'scheduled')
        ) AS prev
        WHERE j.id = prev.id
          AND j.status IN ('pending', 'scheduled')
        RETURNING j.id, prev.prev_status
    ),
    cancel_requested AS (
        UPDATE "{schema}".jobs AS j
        SET cancel_requested_at = now(), cancel_phase = 1
        WHERE j.id IN (
            SELECT id FROM matching
            WHERE cancel_phase = 0
        )
        AND j.status = 'running' AND j.cancel_phase = 0
        RETURNING j.id, j.locked_by_worker
    )
    SELECT
        (SELECT count(*)::int FROM cancelled) AS cancelled_directly,
        (SELECT count(*)::int FROM cancel_requested) AS cancel_requested,
        (SELECT array_agg(id ORDER BY id) FROM cancelled) AS cancelled_ids,
        (SELECT array_agg(prev_status ORDER BY id) FROM cancelled) AS cancelled_prev_statuses,
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
                    row = await conn.fetchrow(cancel_sql, *params)

                    if row is None:
                        raise RuntimeError("cancel_where: aggregate query returned no rows")
                    cancelled_ids = list(row["cancelled_ids"] or [])
                    cancel_requested_ids = list(row["cancel_requested_ids"] or [])
                    prev_statuses: dict[UUID, str] = dict(
                        zip(cancelled_ids, row["cancelled_prev_statuses"] or [], strict=True)
                    )
                    notify_targets = [
                        NotifyTarget(job_id=jid, worker_id=wid)
                        for jid, wid in zip(
                            cancel_requested_ids,
                            row["cancel_requested_workers"] or [],
                            strict=True,
                        )
                        if wid is not None
                    ]

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
                    if cancel_requested_ids:
                        await conn.executemany(
                            sql.insert_event,
                            [(jid, "cancel_request", cr_detail) for jid in cancel_requested_ids],
                        )
            break
        except asyncpg.DeadlockDetectedError:
            if attempt == 2:
                raise
            await asyncio.sleep(0.1 * (2**attempt) + random.random() * 0.05)

    result = BulkCancelResult(
        cancelled_directly=len(cancelled_ids),
        cancel_requested=len(cancel_requested_ids),
        cancelled_ids=cancelled_ids,
        cancel_requested_ids=cancel_requested_ids,
    )
    return result, notify_targets
