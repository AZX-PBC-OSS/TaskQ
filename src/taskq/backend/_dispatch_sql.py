"""Dispatch SQL constants and the asyncpg dispatch_batch helper.

Canonical home for the dispatch CTE SQL so it stays grep-able and
unit-testable independent of the PostgresBackend class.  The worker
module imports from here (worker -> backend is the correct layer
direction).

The strict-FIFO and round-robin variants share ~95% of their CTE body;
the only differences are the ``fairness_rank`` production in the
``candidates`` CTE and the ``ORDER BY`` prefixes in ``ranked`` and
``eligible_candidates``.  A single template is rendered into both
constants via :func:`_render_dispatch_sql`.
"""

import time
from collections.abc import Sequence
from datetime import timedelta
from uuid import UUID

import asyncpg
import structlog
from opentelemetry.trace import SpanKind, StatusCode

from taskq.backend._protocol import ConnLike
from taskq.obs import (
    get_logger,
    record_dispatch_duration,
    safe_start_span,
)

__all__ = [
    "DISPATCH_ROUND_ROBIN_SQL",
    "DISPATCH_STRICT_FIFO_SQL",
    "dispatch_batch",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


# Shared dispatch CTE template.  ``{schema}`` is left intact so callers
# (and tests) can ``.format(schema=...)`` at render time; the ``__*__``
# tokens are substituted by _render_dispatch_sql.
_DISPATCH_SQL_TEMPLATE = """\
WITH params AS (
  SELECT
    $1::text[]   AS queues,
    $2::int      AS limit_n,
    $3::uuid     AS worker_id,
    $4::interval AS lock_lease,
    $5::int      AS oversample
),
running_per_actor AS (
  SELECT actor, count(*) AS in_flight
  FROM "{schema}".jobs
  WHERE status = 'running'
  GROUP BY actor
),
-- Best-effort under concurrent dispatchers: this snapshot is read once at
-- the start of the CTE and is not re-checked after `locked` takes its
-- FOR UPDATE SKIP LOCKED row locks, so two dispatchers running this query
-- concurrently can each see the same identity_key as "not yet running" and
-- both admit one job for it (TOCTOU). The bound is ~<= num_concurrent_
-- dispatchers admitted per identity_key per dispatch round, not a hard 1;
-- callers that need a strict single-flight guarantee per identity_key
-- must not rely on this CTE alone.
running_identities AS (
  SELECT actor, identity_key
  FROM "{schema}".jobs
  WHERE status = 'running' AND identity_key IS NOT NULL
),
per_actor_capacity AS (
  SELECT
    ac.actor,
    CASE WHEN ac.max_concurrent IS NULL
         THEN (SELECT limit_n FROM params)
         ELSE GREATEST(ac.max_concurrent - COALESCE(r.in_flight, 0), 0)
    END AS residual
  FROM "{schema}".actor_config ac
  LEFT JOIN running_per_actor r ON r.actor = ac.actor
),
candidates AS (
  SELECT j.id, j.actor, j.identity_key, j.fairness_key,
         __FAIRNESS_RANK_COLUMN__,
         j.priority, j.scheduled_at, pac.residual
  FROM per_actor_capacity pac
  CROSS JOIN LATERAL unnest((SELECT queues FROM params)) AS sq(queue_name)
  CROSS JOIN LATERAL (
__CANDIDATES_LATERAL__
  ) j
  WHERE pac.residual > 0
),
identity_dedup AS (
  (
    SELECT DISTINCT ON (c.actor, c.identity_key)
      c.id, c.actor, c.fairness_key, c.fairness_rank, c.priority, c.scheduled_at, c.residual
    FROM candidates c
    LEFT JOIN running_identities ri ON ri.actor = c.actor AND ri.identity_key = c.identity_key
    WHERE ri.identity_key IS NULL
      AND c.identity_key IS NOT NULL
    ORDER BY c.actor, c.identity_key, c.priority DESC, c.scheduled_at, c.id
  )
  UNION ALL
  (
    SELECT c.id, c.actor, c.fairness_key, c.fairness_rank, c.priority, c.scheduled_at, c.residual
    FROM candidates c
    WHERE c.identity_key IS NULL
  )
),
ranked AS (
  SELECT id.*,
    ROW_NUMBER() OVER (
      PARTITION BY id.actor
      ORDER BY __RANKED_ORDER_BY__
    ) AS pending_rank
  FROM identity_dedup id
),
locked AS (
  SELECT j.id, j.actor, j.identity_key, j.fairness_key, rk.fairness_rank,
         j.priority, j.scheduled_at, rk.pending_rank, rk.residual
  FROM ranked rk
  JOIN "{schema}".jobs j ON j.id = rk.id
  WHERE j.status = 'pending'
  ORDER BY rk.pending_rank, rk.priority DESC, rk.scheduled_at, j.id
  LIMIT (SELECT limit_n FROM params)
  FOR UPDATE OF j SKIP LOCKED
),
eligible_candidates AS (
  SELECT l.*,
    ac.max_concurrent,
    ROW_NUMBER() OVER (
      PARTITION BY l.actor
      ORDER BY __ELIGIBLE_CANDIDATES_ORDER_BY__
    ) AS actor_rank,
    COALESCE(r.in_flight, 0) AS in_flight,
    CASE WHEN ac.max_concurrent IS NOT NULL
         AND COALESCE(r.in_flight, 0) >= ac.max_concurrent
         THEN FALSE ELSE TRUE END AS boolean_gate
  FROM locked l
  LEFT JOIN "{schema}".actor_config ac ON ac.actor = l.actor
  LEFT JOIN running_per_actor r ON r.actor = l.actor
  WHERE ac.max_concurrent IS NULL
     OR COALESCE(r.in_flight, 0) < ac.max_concurrent
),
eligible AS (
  SELECT ec.id
  FROM eligible_candidates ec
  WHERE ec.max_concurrent IS NULL
     OR ec.actor_rank <= ec.max_concurrent - ec.in_flight
  ORDER BY ec.pending_rank, ec.fairness_rank NULLS LAST, ec.priority DESC, ec.scheduled_at
  LIMIT (SELECT limit_n FROM params)
)
UPDATE "{schema}".jobs j
SET status = 'running',
    started_at = clock_timestamp(),
    finished_at = NULL,
    last_heartbeat_at = clock_timestamp(),
    locked_by_worker = (SELECT worker_id FROM params),
    lock_expires_at = clock_timestamp() + (SELECT lock_lease FROM params),
    error_class = NULL,
    error_message = NULL,
    error_traceback = NULL,
    result = NULL,
    result_size_bytes = NULL,
    attempt = j.attempt + 1
FROM eligible
WHERE j.id = eligible.id
  AND j.status = 'pending'
RETURNING j.*;
"""

_STRICT_FIFO_CANDIDATES_LATERAL = """\
    SELECT j2.id, j2.actor, j2.identity_key, j2.fairness_key,
           j2.priority, j2.scheduled_at
    FROM "{schema}".jobs j2
    WHERE j2.actor = pac.actor
      AND j2.queue = sq.queue_name
      AND j2.status = 'pending'
      AND j2.scheduled_at <= clock_timestamp()
      AND (j2.schedule_to_close IS NULL OR j2.schedule_to_close > clock_timestamp())
    ORDER BY j2.priority DESC, j2.scheduled_at, j2.id
    LIMIT pac.residual * (SELECT oversample FROM params)"""

_ROUND_ROBIN_CANDIDATES_LATERAL = """\
    SELECT w2.id, w2.actor, w2.identity_key, w2.fairness_key,
           w2.fairness_rank,
           w2.priority, w2.scheduled_at
    FROM (
      SELECT j2.id, j2.actor, j2.identity_key, j2.fairness_key,
             j2.priority, j2.scheduled_at,
             ROW_NUMBER() OVER (
               PARTITION BY COALESCE(j2.fairness_key, '__null__')
               ORDER BY j2.priority DESC, j2.scheduled_at, j2.id
             ) AS fairness_rank
      FROM "{schema}".jobs j2
      WHERE j2.actor = pac.actor
        AND j2.queue = sq.queue_name
        AND j2.status = 'pending'
        AND j2.scheduled_at <= clock_timestamp()
        AND (j2.schedule_to_close IS NULL OR j2.schedule_to_close > clock_timestamp())
    ) w2
    -- Oversample per fairness_key (not globally): a global LIMIT here would
    -- truncate the candidate list before fairness_rank partitioning, so a
    -- deep cohort (many pending jobs, one fairness_key) could crowd out all
    -- rows of a shallow cohort before the outer query ever sees them,
    -- starving it indefinitely. Filtering by the per-partition row number
    -- instead guarantees every fairness_key contributes candidates up to
    -- the oversample bound.
    WHERE w2.fairness_rank <= pac.residual * (SELECT oversample FROM params)"""


def _render_dispatch_sql(
    template: str,
    *,
    fairness_rank_column: str,
    candidates_lateral: str,
    ranked_order_by: str,
    eligible_candidates_order_by: str,
) -> str:
    """Substitute the per-variant fragments into the shared dispatch template.

    ``{schema}`` placeholders are preserved so the returned constant can be
    rendered with ``.format(schema=...)`` at the call site.
    """
    return (
        template.replace("__FAIRNESS_RANK_COLUMN__", fairness_rank_column)
        .replace("__CANDIDATES_LATERAL__", candidates_lateral)
        .replace("__RANKED_ORDER_BY__", ranked_order_by)
        .replace("__ELIGIBLE_CANDIDATES_ORDER_BY__", eligible_candidates_order_by)
    )


DISPATCH_STRICT_FIFO_SQL: str = _render_dispatch_sql(
    _DISPATCH_SQL_TEMPLATE,
    fairness_rank_column="NULL::bigint AS fairness_rank",
    candidates_lateral=_STRICT_FIFO_CANDIDATES_LATERAL,
    ranked_order_by="id.priority DESC, id.scheduled_at, id.id",
    eligible_candidates_order_by="l.priority DESC, l.scheduled_at",
)

DISPATCH_ROUND_ROBIN_SQL: str = _render_dispatch_sql(
    _DISPATCH_SQL_TEMPLATE,
    fairness_rank_column="j.fairness_rank",
    candidates_lateral=_ROUND_ROBIN_CANDIDATES_LATERAL,
    ranked_order_by="id.fairness_rank, id.priority DESC, id.scheduled_at, id.id",
    eligible_candidates_order_by="l.fairness_rank, l.priority DESC, l.scheduled_at",
)


async def dispatch_batch(
    conn: ConnLike,
    *,
    sql: str,
    queues: Sequence[str],
    limit_n: int,
    worker_id: UUID,
    lock_lease: timedelta,
    oversample: int = 2,
) -> list[asyncpg.Record]:
    """Execute the rendered dispatch CTE on a live asyncpg connection.

    Returns the raw ``asyncpg.Record`` rows.  Decoding to JobRow happens
    in the caller (PostgresBackend) so this helper stays free of
    backend-shaped types and is unit-testable in isolation.
    """
    queue_list = list(queues)
    queue_attr = queue_list[0] if queue_list else ""
    queues_attr = ",".join(queue_list)

    with safe_start_span(
        "dispatch",
        kind=SpanKind.INTERNAL,
        attributes={
            "taskq.queue": queue_attr,
            "taskq.queues": queues_attr,
            "taskq.batch_size": limit_n,
        },
    ) as span:
        try:
            t0 = time.monotonic()
            rows = await conn.fetch(sql, queue_list, limit_n, worker_id, lock_lease, oversample)
            elapsed = time.monotonic() - t0
        except Exception as exc:
            span.set_status(StatusCode.ERROR, str(exc))
            span.record_exception(exc)
            raise
        returned_count = len(rows)
        span.set_status(StatusCode.OK)
        logger.info(
            "dispatch",
            kind="dispatch",
            from_state="pending",
            to_state="running",
            count=returned_count,
            worker_id=str(worker_id),
            queues=queue_list,
            limit_n=limit_n,
        )

    record_dispatch_duration(queue_attr, elapsed)

    return list(rows)
