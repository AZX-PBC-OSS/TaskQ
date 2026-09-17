"""Shared per-actor executor statistics over the archive UNIONed with the
live terminal population.

The ``/api/history/stats`` JSON endpoint and the actors page render the
same aggregate read; this module owns the SQL so the two surfaces cannot
drift. Read-only, cardinality-bounded at :data:`STATS_LIMIT` rows.

Why the UNION with live ``jobs`` — the History LIST page already reads
``jobs_archive`` UNION ``jobs`` (its rows are the same completed work a
fresh failure lives in), but this aggregate read historically counted the
archive alone. A terminal row stays in ``jobs`` for its whole prune
retention before the prune sweep moves it, so an actor's freshest
failures were invisible to the actors page for exactly as long as they
matter most — the page could show a clean actor for hours after its jobs
started crashing. The live side counts only terminal rows (the same
closed status set the prune sweep archives), so running/pending work
never inflates executor totals, and it is inherently bounded: terminal
live rows exist only between their finish and their prune, so the side's
population is capped by retention regardless of the archive's age.

The optional *window* bounds both sides by ``finished_at``. Its bound is
``statement_timestamp()`` (STABLE), not ``clock_timestamp()`` (VOLATILE):
unlike the History list's LIMIT-ed reads, this is an aggregate with no
LIMIT to hide a post-scan Filter behind, and a VOLATILE bound cannot be
a btree index condition — a STABLE bound is at least index-ELIGIBLE, so
the planner can serve it as an Index Cond. Measured on a 40k-row
``jobs_archive`` plus live terminal rows (PG 18, EXPLAIN ANALYZE,
BUFFERS): the live side's bound IS an Index Cond on the terminal-partial
``jobs_finished_at_idx`` (2 buffers for the whole side); the archive
side's planner choice is selectivity-dependent — a 24h window covering
~3.6% of the table preferred a seq scan (7.7 ms, 1223 buffers) over
~1400 index probes plus heap fetches, which is the planner doing its
job, not the bound failing to be eligible. The all-time read over the
same corpus costs 52 ms — the same cost class the page's archive-side
aggregate always had (dominated by the archive population, which the
windowed variant only slices smaller); the UNION's live side adds two
buffers because terminal live rows are bounded by prune retention.
"""

from datetime import timedelta
from typing import Any

from fastapi import HTTPException

from taskq.backend._protocol import ConnLike
from taskq.web.admin._constants import (
    _TERMINAL_STATUSES,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
)

STATS_LIMIT: int = 200

#: The window selector's closed set: the named ranges the actors page and
#: the stats endpoint accept, as durations. "all" (no bound) is handled
#: by the routes, not the map — it is the documented default, not a range.
STATS_WINDOWS: dict[str, timedelta] = {
    "1h": timedelta(hours=1),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}

# The terminal-status IN list for the live side of the UNION, rendered
# from the admin constants' closed set so a status added there follows
# without a second hand-maintained literal.
_TERMINAL_IN = ", ".join(f"'{s}'" for s in sorted(_TERMINAL_STATUSES))

# Per-actor (optionally per-(actor, queue)) executor totals over the
# archive UNION the live terminal population. Both sides select the same
# six columns so the UNION's shape is one definition; {queue_col} /
# {queue_group} carry the queue column for the per-queue contract of the
# JSON endpoint (the actors page drops them and groups by actor alone).
# The join fans one row per attempt, so total counts attempts (one per
# job in the common single-attempt case) - the same semantics the JSON
# endpoint has always returned. ``last_activity_at`` is the freshest
# finished_at the actor has on either side, so "is this actor still
# working" reads off the row without a second query.
# ``last_error_class`` is the error_class of the actor's most recent
# row that carries one (ordered by finished_at DESC NULLS LAST) - the
# job row's own error_class, which every terminal failure path stamps
# (the classifier's exception name, WorkerCrashed on reclaim,
# DeadlineExceeded on the deadline sweep), so "what is this actor dying
# of" reads without joining attempts.
#
# {window_archive} / {window_live} are empty for the all-time read (the
# default: the statement carries no parameter at all, exactly the shape
# it always had) and the two-sided statement_timestamp() bound when a
# window is requested, bound once as $1 and referenced by both sides.
_STATS_SQL_TEMPLATE = f"""\
SELECT
    u.actor,
{{queue_col}}    count(*) AS total,
    count(*) FILTER (WHERE u.status = 'succeeded') AS succeeded,
    count(*) FILTER (WHERE u.status = 'failed') AS failed,
    count(*) FILTER (WHERE u.status = 'cancelled') AS cancelled,
    count(*) FILTER (WHERE u.status = 'crashed') AS crashed,
    count(*) FILTER (WHERE u.status = 'abandoned') AS abandoned,
    round(avg(u.duration_ms))::bigint AS avg_duration_ms,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY u.duration_ms)::bigint AS p50_duration_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY u.duration_ms)::bigint AS p95_duration_ms,
    max(u.finished_at) AS last_activity_at,
    (array_agg(u.error_class ORDER BY u.finished_at DESC NULLS LAST)
        FILTER (WHERE u.error_class IS NOT NULL))[1] AS last_error_class
FROM (
    SELECT j.actor, j.queue, j.status, j.finished_at, a.duration_ms, j.error_class
    FROM "{{schema}}".jobs_archive j
    LEFT JOIN "{{schema}}".job_attempts_archive a ON a.job_id = j.id
    {{window_archive}}
    UNION ALL
    SELECT j.actor, j.queue, j.status, j.finished_at, a.duration_ms, j.error_class
    FROM "{{schema}}".jobs j
    LEFT JOIN "{{schema}}".job_attempts a ON a.job_id = j.id
    WHERE j.status IN ({_TERMINAL_IN})
    {{window_live}}
) u
GROUP BY u.actor{{queue_group}}
ORDER BY total DESC
LIMIT {STATS_LIMIT}"""

#: The window bound's comparison, shared by both sides of the UNION;
#: each side prefixes it with its own keyword (the archive side's only
#: predicate is ``WHERE``, the live side already filters terminal
#: statuses so it composes with ``AND``). statement_timestamp() (STABLE)
#: keeps the bound an Index Cond on both sides' finished_at indexes —
#: see the module docstring for why an aggregate must not take the
#: VOLATILE form the LIMIT-ed list reads tolerate.
_WINDOW_BOUND = "j.finished_at >= statement_timestamp() - $1::interval"


def _build_stats_sql(schema: str, *, per_queue: bool, window: bool = False) -> str:
    """Return the stats SQL grouped per actor, or per (actor, queue).

    *window* selects the windowed shape (the ``$1`` bound on both sides)
    over the all-time shape, which carries no parameter at all.
    """
    return _STATS_SQL_TEMPLATE.format(
        schema=schema,
        queue_col="    u.queue,\n" if per_queue else "",
        queue_group=",\n    u.queue" if per_queue else "",
        window_archive=f"WHERE {_WINDOW_BOUND}" if window else "",
        window_live=f"AND {_WINDOW_BOUND}" if window else "",
    )


def resolve_stats_window(raw: str | None) -> timedelta | None:
    """Resolve the window selector to a duration; 400s on unknown values.

    ``None`` and ``"all"`` are the documented all-time default. Anything
    else must name a window in :data:`STATS_WINDOWS`: a typo that
    silently fell back to all-time would show an operator the whole
    retained history while the URL claimed a recency view - the
    wrong-but-plausible answer, not a clean input error (the same closed-
    set treatment ``parse_job_statuses`` gives status filters).
    """
    if raw is None or raw == "all":
        return None
    try:
        return STATS_WINDOWS[raw]
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=f"unknown stats window: {raw!r}; allowed: all, {', '.join(STATS_WINDOWS)}",
        ) from None


async def fetch_actor_stats(
    conn: ConnLike,
    *,
    schema: str,
    per_queue: bool = False,
    window: timedelta | None = None,
) -> list[dict[str, Any]]:
    """Return per-actor executor stats rows, hottest actor first.

    With ``per_queue=True`` each row is one (actor, queue) pair - the
    ``/api/history/stats`` contract. The default groups by actor alone
    for the actors page, where the operator reads one row per actor.

    ``window`` bounds both sides of the read by ``finished_at`` (the
    database's clock, server-side); ``None`` is the documented all-time
    default - every completed job the fleet still has a record of, live
    terminal rows included.
    """
    if window is None:
        sql = _build_stats_sql(schema, per_queue=per_queue)
        rows = await conn.fetch(sql)
    else:
        if window <= timedelta(0):
            raise ValueError(f"stats window must be positive, got {window!r}")
        sql = _build_stats_sql(schema, per_queue=per_queue, window=True)
        rows = await conn.fetch(sql, window)
    return [dict(r) for r in rows]
