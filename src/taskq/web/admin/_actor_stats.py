"""Shared per-actor executor statistics over the archive tables.

The ``/api/history/stats`` JSON endpoint and the actors page render the
same aggregate read over ``jobs_archive`` joined to
``job_attempts_archive``; this module owns the SQL so the two surfaces
cannot drift. Read-only, cardinality-bounded at :data:`STATS_LIMIT` rows.
"""

from typing import Any

from taskq.backend._protocol import ConnLike

STATS_LIMIT: int = 200

# Per-actor (optionally per-(actor, queue)) executor totals over the
# archive. {queue_col}/{queue_group} carry the queue column for the
# per-queue contract of the JSON endpoint; the actors page drops them and
# groups by actor alone. ``last_activity_at`` is the freshest finished_at
# the actor has in the archive, so "is this actor still working" reads
# off the row without a second query. The join fans one row per archived
# attempt, so total counts attempts (one per job in the common single-attempt
# case) - the same semantics the JSON endpoint has always returned.
_STATS_SQL_TEMPLATE = f"""\
SELECT
    j.actor,
{{queue_col}}    count(*) AS total,
    count(*) FILTER (WHERE j.status = 'succeeded') AS succeeded,
    count(*) FILTER (WHERE j.status = 'failed') AS failed,
    count(*) FILTER (WHERE j.status = 'cancelled') AS cancelled,
    count(*) FILTER (WHERE j.status = 'crashed') AS crashed,
    count(*) FILTER (WHERE j.status = 'abandoned') AS abandoned,
    round(avg(a.duration_ms))::bigint AS avg_duration_ms,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY a.duration_ms)::bigint AS p50_duration_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY a.duration_ms)::bigint AS p95_duration_ms,
    max(j.finished_at) AS last_activity_at
FROM "{{schema}}".jobs_archive j
LEFT JOIN "{{schema}}".job_attempts_archive a ON a.job_id = j.id
GROUP BY j.actor{{queue_group}}
ORDER BY total DESC
LIMIT {STATS_LIMIT}"""


def _build_stats_sql(schema: str, *, per_queue: bool) -> str:
    """Return the stats SQL grouped per actor, or per (actor, queue)."""
    return _STATS_SQL_TEMPLATE.format(
        schema=schema,
        queue_col="    j.queue,\n" if per_queue else "",
        queue_group=",\n    j.queue" if per_queue else "",
    )


async def fetch_actor_stats(
    conn: ConnLike,
    *,
    schema: str,
    per_queue: bool = False,
) -> list[dict[str, Any]]:
    """Return per-actor executor stats rows, hottest actor first.

    With ``per_queue=True`` each row is one (actor, queue) pair - the
    ``/api/history/stats`` contract. The default groups by actor alone
    for the actors page, where the operator reads one row per actor.
    """
    sql = _build_stats_sql(schema, per_queue=per_queue)
    rows = await conn.fetch(sql)
    return [dict(r) for r in rows]
