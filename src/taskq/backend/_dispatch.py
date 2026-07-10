"""Dispatch operations for PostgresBackend.

``dispatch_batch`` and its queue-mode resolver live here as module-level
functions.  :class:`~taskq.backend.postgres.PostgresBackend` methods
are thin wrappers that delegate.
"""

from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

from taskq.backend._dispatch_sql import (
    dispatch_batch as dispatch_batch_helper,
)
from taskq.backend._protocol import ConnLike, JobRow
from taskq.backend._records import (
    _job_row_from_record,
    jsonb_param,
)
from taskq.backend._sql_templates import SqlTemplates
from taskq.obs import get_logger

if TYPE_CHECKING:
    import asyncpg

logger = get_logger(__name__)

__all__ = [
    "_dispatch_batch",
    "_resolve_queue_modes",
]


async def _dispatch_batch(
    dispatcher_pool: "asyncpg.Pool",
    sql: SqlTemplates,
    dispatch_oversample: int,
    schema: str,
    worker_id: UUID,
    queues: list[str],
    limit: int,
    lock_lease: timedelta,
) -> list[JobRow]:
    """Dispatch up to *limit* pending jobs from *queues*.

    When *queues* mixes ``strict_fifo`` and ``round_robin`` queues in a
    single call, the round-robin CTE variant is used for the whole batch
    (round-robin is a superset behaviour — strict_fifo queues still dispatch
    in priority/scheduled_at order, just with an extra no-op fairness_rank
    partition). This is silent by design elsewhere, so a debug log is
    emitted here to make the mode selection observable when queues are
    mixed unintentionally.
    """
    event_sql = sql.insert_event
    async with dispatcher_pool.acquire() as conn:
        async with conn.transaction():
            queue_modes = await _resolve_queue_modes(conn, queues, schema)
            if len(queue_modes) > 1:
                logger.debug(
                    "dispatch-mixed-queue-modes",
                    queues=queues,
                    modes=sorted(queue_modes),
                    selected_sql="round_robin",
                )
            sql_stmt = (
                sql.dispatch_round_robin
                if "round_robin" in queue_modes
                else sql.dispatch_strict_fifo
            )
            records = await dispatch_batch_helper(
                conn,
                sql=sql_stmt,
                queues=queues,
                limit_n=limit,
                worker_id=worker_id,
                lock_lease=lock_lease,
                oversample=dispatch_oversample,
            )
            for rec in records:
                await conn.execute(
                    event_sql,
                    rec["id"],
                    "state_change",
                    jsonb_param(
                        {
                            "from_state": "pending",
                            "to_state": "running",
                            "worker_id": str(worker_id),
                        }
                    ),
                )
    return [_job_row_from_record(rec) for rec in records]


async def _resolve_queue_modes(
    conn: ConnLike,
    queues: list[str],
    schema: str,
) -> set[str]:
    """Return the set of distinct modes for *queues* from the queues table.

    Queues not present in the table default to ``strict_fifo``. Returns
    ``{"strict_fifo"}`` when all queues are strict FIFO, ``{"round_robin"}``
    when all are round-robin, or a mixed set. The caller selects the
    round-robin SQL variant when ``"round_robin"`` appears in the set.
    """
    if not queues:
        return {"strict_fifo"}
    rows = await conn.fetch(
        f'SELECT name, mode FROM "{schema}".queues WHERE name = ANY($1)',  # Why: schema validated at construction; asyncpg cannot bind identifiers.
        queues,
    )
    modes_by_queue: dict[str, str] = {r["name"]: r["mode"] for r in rows}
    return {modes_by_queue.get(q, "strict_fifo") for q in queues}
