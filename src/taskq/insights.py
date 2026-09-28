"""The operational-insights SQL layer: pure-read aggregate statements over
the job ledger, answering the operator questions the dispatch and sweep
paths never need to ask — how long work WAITED, whether the fleet is
IMBALANCED, whether it is OVERPROVISIONED, how long a queue needs to
DRAIN, and whether a cron schedule is FANNING OUT faster than it clears.

Import sensitivity first, the same contract
:mod:`taskq.backend._retention_floor` documents: this module is
asyncpg-free at module level.  Every statement is a SQL template the
caller executes on a connection the CALLER owns and hands in
(``ConnLike`` is typed through ``TYPE_CHECKING`` plus the
future-annotations string forms; the runtime imports are stdlib-only,
plus the schema-identifier regex).  Deploy-step code, scripts and
notebooks can import this module outside a PG context and never drag
``asyncpg`` (or FastAPI) into ``sys.modules``.

Redis adds nothing here, by design.  The live-only signals Redis
carries (wake pubs, rate-limit buckets, leader chatter) are ephemeral
state: nothing durable to aggregate, no history to window.  Every
metric in this module is answered by Postgres alone, on BOTH a vanilla
Postgres and a TimescaleDB hypertable deployment — the statements name
no Timescale function, probe no extension catalog, and run the same
SQL on both (the planner's chunk pruning does the hypertable-side work
invisibly).  A redis-configured deployment gains no insight rows and
needs none: this is a PG-only module.

The statements' shared discipline (see :mod:`taskq.web.admin._actor_stats`
for the precedent each rule follows):

* **Every window bound is ``statement_timestamp()``** (STABLE), never
  ``clock_timestamp()`` (VOLATILE): these are aggregates with no LIMIT
  to hide a post-scan Filter behind, and a VOLATILE bound cannot be a
  btree index condition.  A STABLE bound is index-ELIGIBLE on every
  anchor index (``jobs_finished_at_idx`` and its archive twin,
  ``job_attempts``' ``started_at`` index, ``workers_last_seen_idx``).
* **Terminal history lives in ``jobs`` THEN ``jobs_archive``,
  therefore EVERY wait / throughput / ledger aggregate is a two-sided
  UNION.**  A terminal row stays in ``jobs`` for its whole prune
  retention (default 30d) before the prune sweep moves it to the
  archive (default 1y), so any single-table read has a blind spot
  exactly as wide as the retention tier it skipped.  The live side
  counts only terminal rows — the same closed status set the prune
  sweep archives — so running/pending work never inflates an
  aggregate, and the live side's population is bounded by prune
  retention regardless of the archive's age.
* **The analytics window floor is the archive retention.**  On
  hypertables, ``jobs_archive`` is columnstore-chunked on
  ``finished_at``, ``job_events`` is rowstore with 7d retention, and
  ``job_attempts_archive`` is chunked on ``started_at``: a window
  older than the archive's retention floor silently answers over
  whatever chunks survive.  The statements do not re-derive the
  policy's horizon (that probe lives in
  :func:`taskq.backend._retention_floor.retention_policy_floor`);
  operators pairing a long window with a short archive retention get
  the retained population, not an error — the docs' interpretation
  section calls this out per metric.
* **The schema identifier is validated against** ``_IDENT_RE`` before
  interpolation (asyncpg cannot bind identifiers); every caller value
  is ``$N``-bound.  No user data is ever f-string interpolated.

The confounds every consumer must know (each function's docstring and
the docs guide restate its own):

* **Wait per attempt = ``started_at - scheduled_at``.**  The dispatch
  claim stamps ``started_at = clock_timestamp()`` and never touches
  ``scheduled_at``; ``scheduled_at`` is when the attempt became due.
* **Deferrals move ``scheduled_at`` forward** and leave only the
  coalesced ``snooze_count`` / ``rate_limit_blocked_count`` counters:
  for those rows ``started_at - scheduled_at`` measures only the FINAL
  leg, so the wait distribution segments them out (``deferred``) from
  the clean first-delivery subset (``clean``) — the two have different
  SLOs.  ``started_at - created_at`` is NOT wait for them (it would
  fold the operator's own snooze choice into the queue's latency).
* **Retries leave no per-attempt due stamp**: the retry arm re-stamps
  ``scheduled_at`` and the pre-retry wait is unrecoverable from the
  jobs row; a retried job contributes its final attempt's wait to the
  clean subset when its deferral counters are zero.
* **Cron jobs enqueue pending-at-fire** (no pre-arm), so a cron
  schedule contributes no wait signal between fires — except
  future-armed fires (the DST ``allof`` second occurrence and any
  forward-stamped enqueue), which land as ``status = 'scheduled'``
  with a future ``scheduled_at`` and ARE wait-measurable once claimed.
  The DST ``allof`` double-enqueue is by-design fan-out, not a defect;
  the budget-deferred fire enqueues NO row at all (its deferral is
  invisible to this layer — the ``taskq.cron.budget_deferrals``
  counter and the ``cron-fire-budget-deferred`` log event are its
  record).
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING, Any

from taskq.backend.statemachine import (
    TERMINAL_STATUSES,
)  # Why: the state machine's own closed terminal set — the same encoding the prune sweep archives under — is the authoritative source for every terminal-side UNION arm; the admin constants' twin would be a second hand-maintained copy and drags FastAPI in.
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: the canonical identifier regex, single-sourced like _sql_templates.py; the private prefix scopes it to the package that owns it.
)

if TYPE_CHECKING:
    from taskq.backend._protocol import ConnLike

__all__ = [
    "INSIGHTS_CONTRACT_VERSION",
    "INSIGHTS_WINDOWS",
    "fetch_actor_backlog",
    "fetch_cron_ledger",
    "fetch_drain_estimates",
    "fetch_overprovisioning",
    "fetch_queue_imbalance",
    "fetch_wait_distribution",
    "fetch_worker_busy_ratio",
]


#: The version of the SQL contract this module's ``fetch_*`` functions are
#: pinned to: the function set, each function's keyword-only parameter
#: names and defaults, and every statement's return-row column names and
#: Postgres types (``tests/test_insights_contract.py`` asserts all three
#: against the live statements).  A dynamic worker-scaling operator may
#: build its queries directly on this surface, so a signature or row-shape
#: change is a BREAKING change and must bump this version — a bump that
#: the contract test refuses to accept silently (it asserts the constant
#: equals the version the test itself documents).  Additive changes (a new
#: function, a new column appended at the END of a row) do not bump the
#: major version on their own, but a column REMOVED, RENAMED, RETYPED or
#: REORDERED before an operator's read does.
INSIGHTS_CONTRACT_VERSION: int = 1


#: The window selector's closed set — the named ranges every windowed
#: statement in this module accepts, as the interval each resolves to.
#: The caller binds the timedelta as ``$n::interval``.  "All-time" is
#: deliberately absent: on hypertables all-time IS the archive
#: retention, and an operator asking for it should see that floor in
#: the docs, not a silently-shrinking aggregate.
INSIGHTS_WINDOWS: dict[str, timedelta] = {
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
}


def _require_ident(schema: str) -> str:
    """The schema identifier, validated before any interpolation.

    The same guard ``taskq.backend._sql_templates`` applies: asyncpg
    cannot bind identifiers, so the schema is interpolated as a
    validated string constant — never caller data.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    return schema


# The terminal-status IN list for the live side of every UNION, rendered
# from the admin constants' closed set so a status added there follows
# without a second hand-maintained literal.
# The terminal-status IN list for the live side of every UNION, rendered
# from the state machine's closed set so a status added there follows
# without a second hand-maintained literal.  (The JobStatus values'
# str form is the SQL enum's label; sorted() gives the rendered list a
# deterministic order.)
_TERMINAL_IN = ", ".join(f"'{s}'" for s in sorted(TERMINAL_STATUSES))

# The window bound shared by every finished_at-anchored side of every
# UNION; each side composes it with its own keyword (the archive side's
# only predicate is WHERE, the live side already filters terminal
# statuses so it composes with AND).  statement_timestamp() (STABLE)
# keeps the bound an Index Cond on both sides' finished_at indexes, and
# on hypertables prunes the archive side to the window's chunks.
_FINISHED_BOUND = "j.finished_at >= statement_timestamp() - $1::interval"

# The due-now predicate for the dispatch population, composed with the
# dispatch partial index's own WHERE (status = 'pending'): the bound is
# STABLE, so the scheduled_at comparison is index-eligible inside
# jobs_dispatch_idx (queue, priority DESC, scheduled_at).
_DUE_NOW = "j.scheduled_at <= statement_timestamp()"


# ── 1. Wait distributions ───────────────────────────────────────────────


# The inner UNION both outer shapes read: TERMINAL rows only (the same
# closed status set the prune sweep archives — running/pending work has
# no wait observation yet), started rows only (a never-claimed row is
# not a wait observation), bounded by finished_at on both anchor
# indexes.  Both sides select the same four columns so the UNION's
# shape is one definition.  wait_s is seconds (double precision)
# extracted from the interval so percentile_cont orders numerically.
_WAIT_INNER = """\
SELECT j.queue, j.actor,
       CASE
           WHEN j.snooze_count = 0 AND j.rate_limit_blocked_count = 0
               THEN 'clean'
           ELSE 'deferred'
       END AS segment,
       EXTRACT(EPOCH FROM j.started_at - j.scheduled_at)::float8 AS wait_s
FROM "{schema}".{table} j
WHERE j.status IN ({_TERMINAL_IN})
  AND j.started_at IS NOT NULL
  AND {bound}"""

# The outer wait shape over the rendered inner UNION; the two groupings
# (per queue, per (actor, queue)) differ ONLY in the select/group/order
# columns, so the template carries them as placeholders.
_WAIT_SQL_TEMPLATE = """\
SELECT {select_cols}
    count(*) AS count,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY u.wait_s)::float8 AS p50_wait_s,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY u.wait_s)::float8 AS p95_wait_s,
    max(u.wait_s)::float8 AS max_wait_s
FROM (
    {live}
    UNION ALL
    {archive}
) u
GROUP BY {group_cols}
ORDER BY {order_cols}"""


def _build_wait_sql(schema: str, *, per_actor: bool) -> str:
    """Return the wait-distribution SQL grouped per queue — and, with
    ``per_actor``, per (actor, queue) — segmented clean vs deferred.

    The outer shape is the only difference between the two groupings;
    the inner UNION (terminal rows, both retention tiers, the
    finished_at anchor) is one definition either way.

    The schema is validated before any interpolation — the same guard
    the fetcher applies, restated here because the builder is called
    directly (the EXPLAIN pins, scripts): asyncpg cannot bind
    identifiers, so ``schema`` is the one value that reaches the SQL
    text.
    """
    s = _require_ident(schema)
    select_cols = (
        "u.queue,\n    u.actor,\n    u.segment," if per_actor else "u.queue,\n    u.segment,"
    )
    group_cols = "u.queue, u.actor, u.segment" if per_actor else "u.queue, u.segment"
    order_cols = "u.queue, u.actor, u.segment" if per_actor else "u.queue, u.segment"
    rendered = _WAIT_SQL_TEMPLATE.format(
        schema=s,
        select_cols=select_cols,
        group_cols=group_cols,
        order_cols=order_cols,
        live=_WAIT_INNER.format(
            schema=s, table="jobs", _TERMINAL_IN=_TERMINAL_IN, bound=_FINISHED_BOUND
        ),
        archive=_WAIT_INNER.format(
            schema=s, table="jobs_archive", _TERMINAL_IN=_TERMINAL_IN, bound=_FINISHED_BOUND
        ),
    )
    return rendered


async def fetch_wait_distribution(
    conn: ConnLike,
    *,
    schema: str,
    window: timedelta,
    per_actor: bool = False,
) -> list[dict[str, Any]]:
    """Per queue (and, with ``per_actor``, per (actor, queue)) wait
    distribution over the trailing *window*, live + archive UNION.

    Wait per attempt is ``started_at - scheduled_at``: the dispatch
    claim stamps ``started_at`` and never touches ``scheduled_at``, so
    for a first-delivery attempt that difference IS the queue wait.
    Each grouping key returns one row PER populated segment:

    * ``clean`` — ``snooze_count = 0 AND rate_limit_blocked_count = 0``:
      first-delivery attempts.  This is the subset a queue-latency SLO
      is written against.  (A job retried after a transient failure
      stays here when its counters are zero; its wait measures only
      the FINAL attempt's claim latency, because the retry arm
      re-stamped ``scheduled_at`` and the earlier legs left no
      per-attempt due stamp — the confound is in the module docstring.)
    * ``deferred`` — rows with a deferral counter: ``scheduled_at`` was
      moved forward by the snooze / rate-limit arms, so the measured
      wait excludes the deferred time BY CONSTRUCTION.  Read these
      under their own SLO (or exclude them from the clean percentile);
      ``started_at - created_at`` is NOT their wait.

    The *window* bounds ``finished_at`` on both sides (terminal rows
    only — the index-eligible anchor) and must be positive.  On
    hypertables a window older than the archive retention answers over
    the surviving chunks — the retention floor is the analytics floor.
    """
    if window <= timedelta(0):
        raise ValueError(f"wait window must be positive, got {window!r}")
    s = _require_ident(schema)
    sql = _build_wait_sql(s, per_actor=per_actor)
    rows = await conn.fetch(sql, window)  # type: ignore[attr-defined]
    return [dict(r) for r in rows]


# ── 2. Imbalance ratios ─────────────────────────────────────────────────


# Per-queue fleet imbalance, one statement, one keys CTE plus four
# inputs, each shaped to its own index so no arm touches the hot
# table's heap or a terminal population:
#
# * ``due``    — pending-and-due depth plus the OLDEST due stamp, off
#   jobs_dispatch_idx (queue, priority DESC, scheduled_at) WHERE status
#   = 'pending': index-only scan; the due-now predicate filters inside
#   the index.
# * ``armed``  — future-armed depth plus the incoming wave's min/max
#   scheduled_at, off jobs_scheduled_wake_idx (scheduled_at) WHERE
#   status = 'scheduled': index-only scan of exactly the armed
#   population.
# * ``live``   — workers serving each queue, from workers.queues
#   (text[]) unnested, bounded by workers_last_seen_idx with a STABLE
#   statement_timestamp() liveness bound (the admin's own liveness
#   window semantics; the seconds arrive as $1).
# * ``cap``    — per-actor max_concurrent sums routed to the queue from
#   actor_config (an operator-sized config table).  The queue's
#   effective capacity multiplies the sum by the live worker count:
#   each worker can run every actor's jobs, and the cap is enforced
#   per worker.
#
# utilization is depth ÷ effective capacity (NULL when no capacity
# serves the queue — that is the starvation shape, reported as capacity
# zero rather than a division by zero).  oldest_due_age_s is
# statement_timestamp() - min(scheduled_at) over due rows: the age of
# the oldest dispatchable work, in seconds.
_QUEUE_IMBALANCE_SQL = """\
WITH due AS (
    SELECT j.queue,
           count(*) AS depth,
           min(j.scheduled_at) AS oldest_due_at
    FROM "{schema}".jobs j
    WHERE j.status = 'pending'
      AND {_DUE_NOW}
    GROUP BY j.queue
),
armed AS (
    SELECT j.queue,
           count(*) AS scheduled_depth,
           min(j.scheduled_at) AS wave_min_scheduled_at,
           max(j.scheduled_at) AS wave_max_scheduled_at
    FROM "{schema}".jobs j
    WHERE j.status = 'scheduled'
    GROUP BY j.queue
),
live AS (
    SELECT q AS queue, count(*) AS live_workers
    FROM "{schema}".workers w, unnest(w.queues) AS q
    WHERE w.last_seen_at > statement_timestamp() - make_interval(secs => $1)
    GROUP BY q
),
cap AS (
    SELECT ac.queue,
           sum(ac.max_concurrent)::int AS actor_capacity
    FROM "{schema}".actor_config ac
    WHERE ac.max_concurrent IS NOT NULL
    GROUP BY ac.queue
),
queues AS (
    SELECT queue FROM due
    UNION SELECT queue FROM armed
    UNION SELECT queue FROM live
    UNION SELECT queue FROM cap
)
SELECT q.queue,
       coalesce(d.depth, 0)::int AS depth,
       d.oldest_due_at,
       EXTRACT(EPOCH FROM statement_timestamp() - d.oldest_due_at)::float8 AS oldest_due_age_s,
       coalesce(a.scheduled_depth, 0)::int AS scheduled_depth,
       a.wave_min_scheduled_at,
       a.wave_max_scheduled_at,
       coalesce(l.live_workers, 0)::int AS live_workers,
       c.actor_capacity,
       (coalesce(c.actor_capacity, 0) * coalesce(l.live_workers, 0))::int AS effective_capacity,
       CASE WHEN coalesce(c.actor_capacity, 0) * coalesce(l.live_workers, 0) > 0
            THEN coalesce(d.depth, 0)::float8
                 / (c.actor_capacity * l.live_workers)::float8
            END AS utilization
FROM queues q
LEFT JOIN due d ON d.queue = q.queue
LEFT JOIN armed a ON a.queue = q.queue
LEFT JOIN live l ON l.queue = q.queue
LEFT JOIN cap c ON c.queue = q.queue
ORDER BY q.queue"""


async def fetch_queue_imbalance(
    conn: ConnLike,
    *,
    schema: str,
    worker_liveness_seconds: int = 30,
) -> list[dict[str, Any]]:
    """Per-queue fleet imbalance: depth, armed wave, live workers,
    effective capacity, utilization, oldest-due age.

    * ``depth`` — pending rows DUE NOW (``scheduled_at <= now``, the
      dispatch index's own population): the work a worker could claim
      this instant.
    * ``scheduled_depth`` — future-armed rows (status ``'scheduled'``)
      with the incoming wave's ``min``/``max`` ``scheduled_at``: how
      much work is armed and how wide the wave spreads.
    * ``live_workers`` — workers whose ``queues`` array subscribes the
      queue and whose ``last_seen_at`` is inside
      *worker_liveness_seconds* (the admin UI's liveness window
      default; measured against the DATABASE's clock, statement-side).
    * ``effective_capacity`` — ``sum(max_concurrent)`` over the actors
      routed to the queue (``actor_config.queue``) x live workers: the
      queue's in-flight ceiling under the current fleet.
    * ``utilization`` — depth ÷ effective capacity.  ``> 1`` is a
      starved queue (more due work than one wave of capacity); NULL
      means NOTHING can serve the queue — capacity zero or no live
      worker, the operator-visible starvation shape.
    * ``oldest_due_age_s`` — seconds since the oldest due row became
      dispatchable: the fairness alarm (a large depth with a tiny
      oldest age is a burst; a small depth with a large age is a
      strand).

    Per (actor, queue) backlog-vs-capacity is :func:`fetch_actor_backlog`;
    this statement cannot serve it because the actor-level partial
    indexes key on ``actor``, not ``queue``.
    """
    s = _require_ident(schema)
    sql = _QUEUE_IMBALANCE_SQL.format(schema=s, _DUE_NOW=_DUE_NOW)
    rows = await conn.fetch(sql, worker_liveness_seconds)  # type: ignore[attr-defined]
    return [dict(r) for r in rows]


# Per (actor, queue) backlog vs running vs capacity, each arm index-only
# over its own partial index: jobs_actor_pending_idx (actor) WHERE
# status IN ('pending','scheduled') for the backlog,
# jobs_actor_running_idx (actor) WHERE status = 'running' for the
# in-flight count, actor_config for the per-actor ceiling and its
# routed queue.  ``queue`` is the actor's ROUTED queue
# (actor_config.queue): a re-pended row can carry a stale queue label,
# and the routing discriminator — the same one the stranded-jobs
# detector reads — is what capacity is enforced against.
_ACTOR_BACKLOG_SQL = """\
SELECT ac.actor,
       ac.queue,
       coalesce(b.backlog, 0)::int AS backlog,
       coalesce(r.running, 0)::int AS running,
       ac.max_concurrent,
       CASE WHEN ac.max_concurrent > 0
            THEN coalesce(r.running, 0)::float8 / ac.max_concurrent::float8
            END AS saturation,
       coalesce(b.backlog, 0)::int
           - GREATEST(coalesce(ac.max_concurrent, 0) - coalesce(r.running, 0), 0)::int
           AS unservable_backlog
FROM "{schema}".actor_config ac
LEFT JOIN (
    SELECT j.actor, count(*) AS backlog
    FROM "{schema}".jobs j
    WHERE j.status IN ('pending', 'scheduled')
    GROUP BY j.actor
) b ON b.actor = ac.actor
LEFT JOIN (
    SELECT j.actor, count(*) AS running
    FROM "{schema}".jobs j
    WHERE j.status = 'running'
    GROUP BY j.actor
) r ON r.actor = ac.actor
ORDER BY ac.actor"""


async def fetch_actor_backlog(
    conn: ConnLike,
    *,
    schema: str,
) -> list[dict[str, Any]]:
    """Per (actor, queue) backlog vs running vs capacity.

    * ``backlog`` — the actor's pending + scheduled rows (the
      ``max_pending`` backpressure counter's own population).
    * ``running`` — the actor's in-flight rows.
    * ``max_concurrent`` — the actor's cached ceiling.
    * ``saturation`` — running ÷ max_concurrent; NULL when the actor
      has no cap (unbounded).
    * ``unservable_backlog`` — backlog beyond the actor's free
      capacity right now: rows that will still be waiting after the
      next full wave of claims.  Zero or negative means one claim
      wave can absorb the backlog.

    The queue column is the actor's ROUTED queue
    (``actor_config.queue``), not the enqueued row's label: capacity
    and dispatch follow the routing table, so this read and the
    dispatcher agree on where the work will run.
    """
    s = _require_ident(schema)
    sql = _ACTOR_BACKLOG_SQL.format(schema=s)
    rows = await conn.fetch(sql)  # type: ignore[attr-defined]
    return [dict(r) for r in rows]


# ── 3. Overprovisioning ─────────────────────────────────────────────────


# Per-queue overprovisioning: live workers against the work the queue
# actually did over the trailing window.  Inputs:
#
# * live workers (the imbalance statement's live arm, $2 = liveness
#   seconds),
# * due-now depth (the dispatch partial index's own population),
# * terminalisations over the window — live + archive UNION bounded by
#   finished_at ($1) on both anchor indexes, grouped by queue.
#
# overprovisioned is TRUE when the queue has live workers, ZERO due
# depth, and fewer terminalisations across the whole window than it
# has workers (fewer than one completion per worker in the entire
# window): the shape where the fleet's payroll outruns its work.  A
# single sample can lie (a queue between bursts); the operator
# sustains the verdict across windows — the statement returns the raw
# inputs so a dashboard can trend them instead of trusting one boolean.
_QUEUE_OVERPROVISIONING_SQL = """\
WITH due AS (
    SELECT j.queue, count(*) AS depth
    FROM "{schema}".jobs j
    WHERE j.status = 'pending'
      AND {_DUE_NOW}
    GROUP BY j.queue
),
live AS (
    SELECT q AS queue, count(*) AS live_workers
    FROM "{schema}".workers w, unnest(w.queues) AS q
    WHERE w.last_seen_at > statement_timestamp() - make_interval(secs => $2)
    GROUP BY q
),
done AS (
    SELECT u.queue, count(*) AS terminalisations
    FROM (
        SELECT j.queue
        FROM "{schema}".jobs j
        WHERE j.status IN ({_TERMINAL_IN})
          AND {_FINISHED_BOUND}
        UNION ALL
        SELECT j.queue
        FROM "{schema}".jobs_archive j
        WHERE j.status IN ({_TERMINAL_IN})
          AND {_FINISHED_BOUND}
    ) u
    GROUP BY u.queue
),
queues AS (
    SELECT queue FROM due
    UNION SELECT queue FROM live
    UNION SELECT queue FROM done
)
SELECT q.queue,
       coalesce(l.live_workers, 0)::int AS live_workers,
       coalesce(d.depth, 0)::int AS depth,
       coalesce(dn.terminalisations, 0)::int AS terminalisations,
       (coalesce(l.live_workers, 0) > 0
        AND coalesce(d.depth, 0) = 0
        AND coalesce(dn.terminalisations, 0) < coalesce(l.live_workers, 0))::boolean
           AS overprovisioned
FROM queues q
LEFT JOIN live l ON l.queue = q.queue
LEFT JOIN due d ON d.queue = q.queue
LEFT JOIN done dn ON dn.queue = q.queue
ORDER BY q.queue"""


async def fetch_overprovisioning(
    conn: ConnLike,
    *,
    schema: str,
    window: timedelta,
    worker_liveness_seconds: int = 30,
) -> list[dict[str, Any]]:
    """Per-queue overprovisioning verdict over the trailing *window*.

    ``overprovisioned`` is TRUE when the queue holds live workers, zero
    due depth, and fewer terminalisations across the whole window than
    workers — fewer than one completion per worker in the entire
    window.  The three raw inputs ride beside the verdict so a
    dashboard can trend them (a single-window TRUE is a hypothesis;
    three consecutive windows is a fleet to shrink).  The window must
    be positive; on hypertables the archive side prunes to the
    window's chunks.
    """
    if window <= timedelta(0):
        raise ValueError(f"overprovisioning window must be positive, got {window!r}")
    s = _require_ident(schema)
    sql = _QUEUE_OVERPROVISIONING_SQL.format(
        schema=s, _TERMINAL_IN=_TERMINAL_IN, _FINISHED_BOUND=_FINISHED_BOUND, _DUE_NOW=_DUE_NOW
    )
    rows = await conn.fetch(sql, window, worker_liveness_seconds)  # type: ignore[attr-defined]
    return [dict(r) for r in rows]


# Per-worker busy ratio: the worker's summed attempt duration over the
# trailing window against its tenure.  One statement serves the fleet
# read and the per-worker drill-down: $2 is the worker_id (NULL = all
# workers, the fleet pass).  Both arms are bounded by started_at —
# job_attempts' own btree index on the live side; chunk pruning on the
# archive side, whose hypertable chunks by started_at.  Within the
# surviving chunks the worker filter rides as a post-scan Filter: the
# archive's widened PK (job_id, attempt, started_at) segments by
# job_id and cannot serve a worker_id seek — chunk pruning is what
# keeps the read bounded, and that is the honest cost of a per-worker
# drill-down into compressed history.
_WORKER_BUSY_SQL = """\
SELECT w.id AS worker_id,
       w.hostname,
       w.pid,
       w.queues,
       w.started_at,
       w.last_seen_at,
       coalesce(b.busy_ms, 0)::bigint AS busy_ms,
       (LEAST(
            EXTRACT(EPOCH FROM statement_timestamp() - w.started_at),
            EXTRACT(EPOCH FROM $1::interval)
        ) * 1000.0)::float8 AS observed_ms,
       CASE WHEN LEAST(
                    EXTRACT(EPOCH FROM statement_timestamp() - w.started_at),
                    EXTRACT(EPOCH FROM $1::interval)
                ) > 0
            THEN coalesce(b.busy_ms, 0)::float8 / (
                     LEAST(
                         EXTRACT(EPOCH FROM statement_timestamp() - w.started_at),
                         EXTRACT(EPOCH FROM $1::interval)
                     ) * 1000.0)
            END AS busy_ratio
FROM "{schema}".workers w
LEFT JOIN (
    SELECT u.worker_id, sum(u.duration_ms)::bigint AS busy_ms
    FROM (
        SELECT a.worker_id, a.duration_ms
        FROM "{schema}".job_attempts a
        WHERE a.started_at >= statement_timestamp() - $1::interval
          AND ($2::uuid IS NULL OR a.worker_id = $2::uuid)
        UNION ALL
        SELECT a.worker_id, a.duration_ms
        FROM "{schema}".job_attempts_archive a
        WHERE a.started_at >= statement_timestamp() - $1::interval
          AND ($2::uuid IS NULL OR a.worker_id = $2::uuid)
    ) u
    GROUP BY u.worker_id
) b ON b.worker_id = w.id
WHERE ($2::uuid IS NULL OR w.id = $2::uuid)
ORDER BY w.last_seen_at DESC"""


async def fetch_worker_busy_ratio(
    conn: ConnLike,
    *,
    schema: str,
    window: timedelta,
    worker_id: Any = None,
) -> list[dict[str, Any]]:
    """Per-worker busy ratio over the trailing *window* — summed attempt
    ``duration_ms`` (live attempts + archive twin) against the
    worker's tenure, capped at the window.

    * ``busy_ratio`` — busy_ms ÷ observed_ms, where observed_ms is the
      window the worker actually existed for (tenure capped at the
      window).  Near zero across a sustained window on a worker still
      heartbeating is the idle-worker shape; near one is saturation.
    * ``worker_id`` drills ONE worker down (the idle-worker question is
      always asked about one worker); the default fleet read groups
      the whole window in one pass.  Both arms are bounded by
      ``started_at`` — the live side's btree Index Cond, the archive
      side's chunk pruning; within the surviving chunks the worker
      filter is a post-scan Filter (see the SQL's comment for why the
      archive's PK cannot serve a ``worker_id`` seek).

    The window must be positive.  ``busy_ms`` counts attempts the
    worker RAN (duration_ms is the attempt's own stamp); a worker that
    claimed but never ran contributes zero — the same shape a
    no-traffic window produces.
    """
    if window <= timedelta(0):
        raise ValueError(f"busy-ratio window must be positive, got {window!r}")
    s = _require_ident(schema)
    sql = _WORKER_BUSY_SQL.format(schema=s)
    rows = await conn.fetch(sql, window, worker_id)  # type: ignore[attr-defined]
    return [dict(r) for r in rows]


# ── 4. Drain estimation ─────────────────────────────────────────────────


# Per-queue drain estimate: due depth against the queue's own realised
# throughput over the trailing window.  Terminalisations are the SAME
# live + archive UNION the wait distribution and the overprovisioning
# read ($1 = window); completions_per_second normalizes them;
# eta_seconds divides due depth by that rate.  When the window carried
# NO traffic the rate is zero and the estimate is honestly NULL —
# has_traffic = false says so explicitly rather than letting a
# division-by-zero masquerade as "already drained".  The armed wave's
# min/max scheduled_at rides along: the incoming future work the
# estimate does NOT include (the estimate counts the due population
# only; the wave lands after it).
_QUEUE_DRAIN_SQL = """\
WITH due AS (
    SELECT j.queue, count(*) AS depth
    FROM "{schema}".jobs j
    WHERE j.status = 'pending'
      AND {_DUE_NOW}
    GROUP BY j.queue
),
armed AS (
    SELECT j.queue,
           count(*) AS scheduled_depth,
           min(j.scheduled_at) AS wave_min_scheduled_at,
           max(j.scheduled_at) AS wave_max_scheduled_at
    FROM "{schema}".jobs j
    WHERE j.status = 'scheduled'
    GROUP BY j.queue
),
done AS (
    SELECT u.queue, count(*) AS terminalisations
    FROM (
        SELECT j.queue
        FROM "{schema}".jobs j
        WHERE j.status IN ({_TERMINAL_IN})
          AND {_FINISHED_BOUND}
        UNION ALL
        SELECT j.queue
        FROM "{schema}".jobs_archive j
        WHERE j.status IN ({_TERMINAL_IN})
          AND {_FINISHED_BOUND}
    ) u
    GROUP BY u.queue
),
queues AS (
    SELECT queue FROM due
    UNION SELECT queue FROM armed
    UNION SELECT queue FROM done
)
SELECT q.queue,
       coalesce(d.depth, 0)::int AS depth,
       coalesce(dn.terminalisations, 0)::int AS terminalisations,
       (coalesce(dn.terminalisations, 0)::float8
            / EXTRACT(EPOCH FROM $1::interval)::float8)::float8 AS completions_per_second,
       (coalesce(dn.terminalisations, 0) > 0)::boolean AS has_traffic,
       CASE WHEN coalesce(dn.terminalisations, 0) > 0
            THEN coalesce(d.depth, 0)::float8
                 / (dn.terminalisations::float8
                    / EXTRACT(EPOCH FROM $1::interval)::float8)
            END AS eta_seconds,
       coalesce(a.scheduled_depth, 0)::int AS scheduled_depth,
       a.wave_min_scheduled_at,
       a.wave_max_scheduled_at
FROM queues q
LEFT JOIN due d ON d.queue = q.queue
LEFT JOIN armed a ON a.queue = q.queue
LEFT JOIN done dn ON dn.queue = q.queue
ORDER BY q.queue"""


async def fetch_drain_estimates(
    conn: ConnLike,
    *,
    schema: str,
    window: timedelta,
) -> list[dict[str, Any]]:
    """Per-queue seconds-to-drain over the trailing *window*.

    * ``eta_seconds`` — due depth ÷ (terminalisations over the window,
      normalized to completions per second).  This is a THROUGHPUT
      extrapolation, not a promise: it assumes the next window looks
      like the last one, that the queue's workers stay up, and that
      nothing enqueues behind the current depth.
    * ``has_traffic`` — false when the window carried NO
      terminalisations: ``eta_seconds`` is NULL and the honest
      confidence caveat is "no traffic in the window, estimate
      undefined" — never zero, which would read as "already drained".
      Widen the window (up to the archive retention floor) before
      trusting anything else.
    * ``scheduled_depth`` with ``wave_min_scheduled_at`` /
      ``wave_max_scheduled_at`` — the future-armed population the
      estimate does NOT include: the incoming wave's span.  A queue
      can drain its due depth into an immediately re-arming wave; the
      two numbers together are the real picture.

    The window must be positive; on hypertables the archive side
    prunes to the window's chunks, and a window older than the
    archive retention counts only what survived (the retention floor
    is the analytics floor).
    """
    if window <= timedelta(0):
        raise ValueError(f"drain window must be positive, got {window!r}")
    s = _require_ident(schema)
    sql = _QUEUE_DRAIN_SQL.format(
        schema=s, _TERMINAL_IN=_TERMINAL_IN, _FINISHED_BOUND=_FINISHED_BOUND, _DUE_NOW=_DUE_NOW
    )
    rows = await conn.fetch(sql, window)  # type: ignore[attr-defined]
    return [dict(r) for r in rows]


# ── 5. Cron fan-out ledger ──────────────────────────────────────────────


# Per-schedule fan-out ledger.  Provenance is the jobs-row metadata
# stamp the cron tick writes (metadata->>'cron_schedule_id'); the
# per-schedule seeks go through the GIN index jobs_metadata_gin_idx
# (jsonb_path_ops) with a bound containment
# metadata @> jsonb_build_object('cron_schedule_id', s.id::text) —
# jsonb_path_ops indexes ONLY containment, so the existence operator
# (?) would be a full-scan filter while the bound @> is an Index Cond
# per schedule row of the (operator-sized) config table's nested-loop
# inner side.
#
# One LATERAL per side (live, archive), each counting FOUR windows of
# the schedule's own fire history in one seek:
#
# * fires_window   — enqueued in the CURRENT window (created_at in
#   [now - $1, now)), live + archive UNION.
# * cleared_window — of those, terminalised (the same closed status
#   set the prune sweep archives).  Clearance lags fires by
#   construction at the window's right edge; the ratio is a trend,
#   not an instant.
# * fires_prior / cleared_prior — the PRIOR equal window
#   (created_at in [now - $2, now - $1), $2 = 2 x window, bound in
#   Python).  Archive rows are terminal by definition, so the archive
#   side's cleared counts are its fires.
# * outstanding    — the schedule's non-terminal population right
#   now, WINDOWLESS: the backlog the fleet still owes, including the
#   future-armed fires (DST allof's second occurrence and any
#   forward-stamped enqueue — both by design).
#
# Index discipline per arm.  The LIVE arm seeks the GIN index
# (jobs_metadata_gin_idx) per schedule — jsonb_path_ops indexes only
# containment, so the bound @> is an Index Cond per schedule row of
# the (operator-sized) config table's nested-loop inner side.  The
# ARCHIVE arm cannot seek a GIN (the archive carries a tags GIN, not a
# metadata GIN), so it takes the archive's finished_at anchor instead:
# a fire created inside the last TWO windows cannot have finished
# before the prior window's start (finishing is strictly after
# creating), so ``finished_at >= statement_timestamp() - $2`` is
# logically IMPLIED by the created_at arms and drops NOTHING — while
# making the bound an Index Cond on jobs_archive_finished_at_idx on
# vanilla Postgres and the chunk-pruning key on hypertables, instead
# of a full-archive scan per schedule.
#
# The archive arm's cleared FILTERS carry the same created_at windows as
# the live arm's.  cleared_window may keep only its lower bound because
# ``created_at`` is the enqueue statement's transaction-start now() and
# so is ALWAYS strictly before this read's statement_timestamp() — the
# upper bound would be vacuously true.  cleared_prior's upper bound is
# NOT vacuous: without it a current-window fire that finished AND pruned
# fast (a short prune retention against a long window — a supported
# configuration) counts as a PRIOR-window clearance, inflating
# cleared_prior and silently suppressing runaway_trending (pinned by
# tests/test_insights.py's attack wave).
#
# runaway_trending is TRUE when fires > cleared in BOTH the current
# and the prior window: two consecutive windows of fan-out outrunning
# clearance is the runaway shape (one window is a burst; two is a
# trend).  DST allof legitimately doubles a fire in the overlap hour —
# read the ledger against the schedule's dst_strategy before calling
# that a runaway.  A budget-deferred fire enqueues NO row: its window
# reads zero here, and the deferral's record is the
# taskq.cron.budget_deferrals counter, not this ledger.
_CRON_LEDGER_SQL = """\
SELECT s.id AS schedule_id,
       s.actor,
       s.cron_expr,
       s.timezone,
       s.dst_strategy,
       s.enabled,
       (coalesce(f.fires_window, 0) + coalesce(a.fires_window, 0))::int AS fires_window,
       (coalesce(f.cleared_window, 0) + coalesce(a.cleared_window, 0))::int AS cleared_window,
       (coalesce(f.fires_prior, 0) + coalesce(a.fires_prior, 0))::int AS fires_prior,
       (coalesce(f.cleared_prior, 0) + coalesce(a.cleared_prior, 0))::int AS cleared_prior,
       coalesce(f.outstanding, 0)::int AS outstanding,
       (coalesce(f.fires_window, 0) + coalesce(a.fires_window, 0)
            > coalesce(f.cleared_window, 0) + coalesce(a.cleared_window, 0)
        AND coalesce(f.fires_prior, 0) + coalesce(a.fires_prior, 0)
            > coalesce(f.cleared_prior, 0) + coalesce(a.cleared_prior, 0))::boolean
           AS runaway_trending
FROM "{schema}".cron_schedules s
LEFT JOIN LATERAL (
    SELECT
        count(*) FILTER (WHERE j.created_at >= statement_timestamp() - $1::interval
                          AND j.created_at < statement_timestamp()) AS fires_window,
        count(*) FILTER (WHERE j.created_at >= statement_timestamp() - $1::interval
                          AND j.created_at < statement_timestamp()
                          AND j.status IN ({_TERMINAL_IN})) AS cleared_window,
        count(*) FILTER (WHERE j.created_at >= statement_timestamp() - $2::interval
                          AND j.created_at < statement_timestamp() - $1::interval) AS fires_prior,
        count(*) FILTER (WHERE j.created_at >= statement_timestamp() - $2::interval
                          AND j.created_at < statement_timestamp() - $1::interval
                          AND j.status IN ({_TERMINAL_IN})) AS cleared_prior,
        count(*) FILTER (WHERE j.status NOT IN ({_TERMINAL_IN})) AS outstanding
    FROM "{schema}".jobs j
    WHERE j.metadata @> jsonb_build_object('cron_schedule_id', s.id::text)
) f ON true
LEFT JOIN LATERAL (
    SELECT
        count(*) FILTER (WHERE j.created_at >= statement_timestamp() - $1::interval
                          AND j.created_at < statement_timestamp()) AS fires_window,
        count(*) FILTER (WHERE j.created_at >= statement_timestamp() - $1::interval) AS cleared_window,
        count(*) FILTER (WHERE j.created_at >= statement_timestamp() - $2::interval
                          AND j.created_at < statement_timestamp() - $1::interval) AS fires_prior,
        count(*) FILTER (WHERE j.created_at >= statement_timestamp() - $2::interval
                          AND j.created_at < statement_timestamp() - $1::interval) AS cleared_prior,
        0::bigint AS outstanding
    FROM "{schema}".jobs_archive j
    WHERE j.metadata @> jsonb_build_object('cron_schedule_id', s.id::text)
      AND j.finished_at >= statement_timestamp() - $2::interval
) a ON true
ORDER BY s.actor"""


def _build_cron_ledger_sql(schema: str) -> str:
    """Return the per-schedule fan-out ledger SQL (schema baked in).

    Two LATERAL arms — live ``jobs`` and ``jobs_archive`` — join the
    config table so the GIN containment seek is bound per schedule id
    (jsonb_path_ops indexes only ``@>``; an existence filter would
    full-scan).  Each schedule row carries fires and clearance over
    the current window, the same pair over the PRIOR equal window
    (``$2`` = twice the window, the trend's second sample), the
    windowless outstanding backlog, and the runaway verdict.
    """
    s = _require_ident(schema)
    return _CRON_LEDGER_SQL.format(schema=s, _TERMINAL_IN=_TERMINAL_IN)


async def fetch_cron_ledger(
    conn: ConnLike,
    *,
    schema: str,
    window: timedelta,
) -> list[dict[str, Any]]:
    """Per-schedule cron fan-out ledger over the trailing *window*.

    * ``fires_window`` / ``cleared_window`` — jobs enqueued by the
      schedule in the window (provenance ``metadata cron_schedule_id``,
      live + archive UNION) and how many of those are terminalised.
    * ``fires_prior`` / ``cleared_prior`` — the PRIOR equal window
      (``$2`` = twice the window): the trend's second sample.
    * ``outstanding`` — the schedule's non-terminal population right
      now, windowless: the backlog the fleet still owes, future-armed
      fires included (by design).
    * ``runaway_trending`` — TRUE when fires > cleared in BOTH the
      current and the prior window.  One window is a burst; two
      consecutive is the runaway shape.

    Confounds the verdict's reader must hold: DST ``allof``
    legitimately doubles a fire in the overlap hour (read against
    ``dst_strategy``); a budget-deferred fire enqueues NO row, so a
    deferral reads as zero fires here, not as a growing backlog (its
    record is the ``taskq.cron.budget_deferrals`` counter).
    """
    if window <= timedelta(0):
        raise ValueError(f"cron ledger window must be positive, got {window!r}")
    s = _require_ident(schema)
    sql = _build_cron_ledger_sql(s)
    rows = await conn.fetch(sql, window, 2 * window)  # type: ignore[attr-defined]
    return [dict(r) for r in rows]
