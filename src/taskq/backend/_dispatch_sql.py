"""Dispatch SQL constants and the asyncpg dispatch_batch helper.

Canonical home for the dispatch CTE SQL so it stays grep-able and
unit-testable independent of the PostgresBackend class.  The worker
module imports from here (worker -> backend is the correct layer
direction).

The strict-FIFO and round-robin variants share ~95% of their CTE body;
the only differences are the ``fairness_rank`` production in the
``candidates`` CTE, the ``ORDER BY`` prefixes in ``ranked`` and
``eligible_candidates``, and the round-robin-only ``rr_keys`` cohort
enumeration CTE.  A single template is rendered into both constants via
:func:`_render_dispatch_sql`.

Depth bounding (issue #130): every jobs access in this statement is a
per-round bounded probe, never a scan of the pending backlog.  The
shipped shape re-examined the whole backlog twice per round (the
``locked`` CTE's ``ranked``-to-``jobs`` re-join and the terminal
UPDATE's join were planned as hash joins over a Seq Scan of every
pending row — measured 1.04 ms at a 1k backlog degrading to 55.8 ms at
200k, O(depth) in both time and buffers) and, in the round-robin
variant, a third time (the candidates lateral's ``ROW_NUMBER`` window
ran over EVERY due row of the (actor, queue) pair before the
``fairness_rank <= residual * oversample`` filter could drop the excess
— a window function cannot short-circuit, so the WindowAgg paid full
backlog depth every tick).  The shipped bounds also rendered as
``(SELECT ... FROM params)`` subquery LIMITs, which the planner cannot
fold into row estimates in ANY plan (custom or generic), so the
candidate chain was estimated at the whole index range and the estimate
cascade is what made those whole-backlog hash joins look affordable.

The geometry below pins each stage to the round's own constants:

* ``per_actor_capacity`` probes per (actor, round queue) through a
  correlated LATERAL — the idle-actor prefilter as a bounded index
  probe, structurally (correlation denies the hash-join path).
* ``candidates`` reads at most ``residual * oversample`` rows per
  (actor, queue) probe — the strict-FIFO lateral directly; the
  round-robin variant per fairness cohort, via the ``rr_keys`` loose
  index scan (a WITH RECURSIVE row-compare walk over the cohort index,
  the classic emulation of a skip scan, which this Postgres generation
  does not offer).
* ``top_ids`` finalizes the LIMIT-ed id set BEFORE the statement
  touches the heap a second time, and ``locked`` then drives ``jobs``
  by primary key through a correlated LATERAL — a materialized CTE is an
  optimization fence: the planner may otherwise choose a nested loop over
  the LIMITing subquery. A bounded, locked CTE whose UPDATE joins by
  id ensures the dispatch never re-optimizes across the candidacy cut
  and respects the admission decision made by top_ids.
* the terminal UPDATE re-finds its rows through
  ``j.id = ANY(ARRAY(SELECT id FROM eligible))`` — the id array
  materializes once as an InitPlan and the ScalarArrayOp is served
  either as a Bitmap Index Scan on the primary key (deep backlogs) or
  as a scan-level filter (shallow ones); both carry at most ``limit_n``
  rows of work per node.
* every LIMIT bound is a direct ``$n`` parameter — because a parameter
  folds to a literal in custom plans, where a subquery bound never folds.  The bounds that must hold even under a generic plan do
  not rely on estimates at all: they are structural (correlated
  laterals, ORDER BY + LIMIT probes, the one-shot id array), which is
  why this CTE family must never return to subquery LIMITs — the v1
  experiment in docs/design/sql-hotpath-followups.md §1 under-dispatched
  (2 rows instead of 50) under ``plan_cache_mode = force_generic_plan``
  with them.

The depth contract — every plan node's row work is independent of
backlog depth, at 1k and at 30k due rows — is pinned by
tests/test_dispatch_backlog_depth_bound.py; the per-probe bound is
``residual * oversample`` candidates per (actor, queue) cohort probe
plus ``limit_n`` locked/eligible rows, exactly what that pin's oracle
asserts.  Deep backlogs still drain: each round takes each cohort's
top-``residual * oversample`` rows, so depth only delays a cohort's
tail across rounds, it never removes any row from consideration.

Routing contract (the running-job-tail fix for
:func:`taskq.actor_config_ops.move_actor_queue`): a pending row's
dispatch routing queue is decided by its ORIGIN.  A row a producer
placed (``assignment_routed`` false) routes by its OWN ``jobs.queue``
label — producer placement governs, so a stale producer's post-move
enqueue to a retired source queue stays served by that queue's
consumers, and an explicit ``enqueue(queue=...)`` override keeps its
queue.  A row a re-pend handed back (``assignment_routed`` true)
routes by its actor's CURRENT stored assignment
(``actor_config.queue``).  Every re-pend path
(``mark_retry``/``mark_failed_or_retry``, the leader's crash-reclaim
sweep, the operator ``retry_job``, the snooze/refund deferral arms)
returns a row to the pending pool still carrying its original queue
label — the label is the audit trail of first placement, and no path
rewrites it — so without the assignment-routed arm a move's
left-behind tails would be claimable only by consumers of the queue
the operator is retiring: stranded the moment the source queue's last
consumer goes away.  The marker is written by the re-pend paths
themselves rather than inferred: ``started_at`` answers only "was
claimed", which misses an operator retry of a job terminalized before
it was ever claimed — a deliberate hand-back that would otherwise
route by its stale label and strand permanently; ``attempt`` is
likewise unusable (the snooze/refund arms give the claim's increment
back, flooring to 0).  The two populations are probed by disjoint arms
with disjoint partial indexes (``jobs_actor_dispatch_idx`` /
``jobs_round_robin_probe_idx`` for producer-placed rows,
``jobs_assignment_routed_probe_idx`` for re-pended rows),
each arm keeping its own ORDER BY + LIMIT probe so the depth contract
holds for both; the re-pended arm's cohort enumeration
(``rr_tail_keys``) walks only the re-pended population, which is empty
in the steady never-retried state the depth oracle seeds.
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
from taskq.obs._redact_exc import record_exception_safe, safe_exception_message

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
-- WITH RECURSIVE: the round-robin variant's rr_keys cohort enumeration
-- is a recursive loose index scan; the strict-FIFO variant defines no
-- recursive arm, and a RECURSIVE keyword over a list with none is a
-- no-op permission, so one template serves both variants.
WITH RECURSIVE params AS (
  SELECT
    $1::text[]   AS queues,
    $2::int      AS limit_n,
    $3::uuid     AS worker_id,
    $4::interval AS lock_lease,
    $5::int      AS oversample
),
__RR_KEYS_CTE__
-- Best-effort under concurrent dispatchers, for the same reason as
-- `running_identities` below: this count is read ONCE, before `locked`
-- takes its FOR UPDATE SKIP LOCKED row locks, and is never recomputed.
-- Two dispatchers running concurrently each see the same in_flight, each
-- admit up to `max_concurrent - in_flight`, and lock DISJOINT pending rows
-- -- so SKIP LOCKED does not serialize them and both succeed. The
-- over-dispatch bound is (num_producers - 1) * max_concurrent per round,
-- and those jobs genuinely run: reclaiming stale locks does not undo an
-- over-dispatch. `max_concurrent` is therefore a per-round admission
-- damper, NOT a hard fleet-wide cap.
-- For a strict fleet-wide cap use the leased-slot ConcurrencyReservation
-- (per-queue `queues.max_concurrent`), where the read and the write are
-- the same statement on the same row so no such window exists.
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
-- Idle-actor prefilter: without it, candidates CROSS JOINs every
-- actor_config row with every subscribed queue and runs the lateral
-- index seek once per (actor, queue) pair even when the actor has no
-- pending rows at all -- at hundreds of registered actors that fan-out
-- dominates every idle dispatch tick.
--
-- The probe is a correlated per-queue LATERAL, not the EXISTS this
-- CTE historically used. An EXISTS is a semi-join, and the planner is
-- free to execute it as a hash semi-join over a Seq Scan of the entire
-- pending backlog -- which it does whenever actor_config's row
-- estimate makes one pass over jobs look cheaper than per-actor
-- probes. actor_config genuinely carries that estimate in production:
-- it holds one row per registered actor, sits far below autovacuum's
-- insert threshold, and is therefore usually never analyzed, leaving
-- the planner on the default guess (~440 rows) even for a one-actor
-- fleet. The LATERAL shape removes the planner's option instead of
-- arguing with its costs: the correlation on ac.actor denies the
-- unparameterized (hashable) inner path, and the per-queue equality
-- from unnest plus the ORDER BY over jobs_actor_dispatch_idx's
-- (actor, queue, priority DESC, ...) key pins the probe to an
-- index-ordered first-entry read, bounded by the number of round
-- queues per actor, never by backlog depth. A queue = ANY(...) array
-- predicate cannot serve that ORDER BY (an ScalarArrayOp breaks the
-- index's single ordered stream), which is why the fan-out is over
-- unnest(queues) with one plain-equality probe per queue.
--
-- The predicate covers exactly the queues in the round's params, NOT
-- the actor's home queue: an enqueue(queue = ...) override that lands
-- a pending row on any subscribed queue keeps that actor probed.
-- Filtering here is selection-neutral -- an actor with no pending rows
-- on the round's queues already contributed zero candidate rows,
-- because the lateral's j2.queue = sq.queue_name equality annihilated
-- every one of its pairs -- so ordering, fairness, and the
-- locked/eligible stages are untouched.
--
-- The probe is scoped to producer-placed rows (NOT assignment_routed):
-- this CTE feeds only the label-routed candidates arm, and a re-pended
-- row (assignment_routed) is that arm's non-candidate -- its routing
-- queue is the actor's assignment, probed by repend_capacity below.
-- An actor whose only dispatchable rows are re-pends is therefore NOT
-- probed here; it enters the round through repend_capacity instead.
per_actor_capacity AS (
  SELECT
    ac.actor,
    CASE WHEN ac.max_concurrent IS NULL
         THEN (SELECT limit_n FROM params)
         ELSE GREATEST(ac.max_concurrent - COALESCE(r.in_flight, 0), 0)
    END AS residual
  FROM "{schema}".actor_config ac
  CROSS JOIN params p
  LEFT JOIN running_per_actor r ON r.actor = ac.actor
  CROSS JOIN LATERAL (
    SELECT 1 AS has_pending
    FROM unnest(p.queues) AS pq(q)
    CROSS JOIN LATERAL (
      SELECT 1
      FROM "{schema}".jobs j
      WHERE j.actor = ac.actor
        AND j.queue = pq.q
        AND NOT j.assignment_routed
        AND j.status = 'pending'
      ORDER BY j.priority DESC, j.scheduled_at, j.id
      LIMIT 1
    ) anyq
    LIMIT 1
  ) hp
  WHERE hp.has_pending IS NOT NULL
),
-- The assignment-routed half of the routing contract: the actors whose
-- CURRENT stored assignment is among this round's subscribed queues,
-- with the same residual arithmetic as per_actor_capacity. The
-- assignment IS the routing here: every re-pended row of such an actor
-- (any queue label, assignment_routed, pending) is a candidate
-- for this round no matter which label it carries -- the
-- move_actor_queue tail contract. Actors whose assignment is not
-- subscribed contribute nothing here, exactly as a label-routed actor
-- with no rows on a subscribed queue contributes nothing above.
-- ac.queue = ANY(p.queues) reads the CROSS JOINed params column (a
-- plain text[] value), NOT a subquery -- = ANY(subquery) iterates the
-- subquery's ROWS, and a one-row array-valued subquery would compare
-- the label against the whole array; the column form is the array
-- membership test. It is a filter over actor_config rows (bounded by
-- the registered-actor count), not an ordering-critical probe, so the
-- array predicate form is correct here where it would be wrong in a
-- probe lateral.
repend_capacity AS (
  SELECT
    ac.actor,
    CASE WHEN ac.max_concurrent IS NULL
         THEN (SELECT limit_n FROM params)
         ELSE GREATEST(ac.max_concurrent - COALESCE(r.in_flight, 0), 0)
    END AS residual
  FROM "{schema}".actor_config ac
  CROSS JOIN params p
  LEFT JOIN running_per_actor r ON r.actor = ac.actor
  WHERE ac.queue = ANY(p.queues)
),
-- Re-pended cohort enumeration: the same recursive loose index scan
-- geometry as _RR_KEYS_CTE, over the re-pended population only
-- (pending AND assignment_routed) and keyed on (actor,
-- COALESCE(fairness_key, '__null__')) -- the cohort identity has no
-- queue column because a re-pended row's routing queue is the actor's
-- assignment, one queue per actor, never the row's label. The walk
-- rides jobs_assignment_routed_probe_idx: each step's
-- row-compare on the two leading key columns is an Index Cond, one
-- bounded seek per DISTINCT cohort, so the enumeration's work is
-- proportional to the re-pended cohort count, never to any cohort's
-- depth. Empty in the never-retried steady state, which is what keeps
-- the depth oracle (seeded exclusively with never-claimed rows)
-- depth-bounded through the assignment-routed arm.
rr_tail_keys AS (
  (
    SELECT j5.actor, COALESCE(j5.fairness_key, '__null__') AS fkey
    FROM "{schema}".jobs j5
    WHERE j5.status = 'pending' AND j5.assignment_routed
    ORDER BY j5.actor, COALESCE(j5.fairness_key, '__null__')
    LIMIT 1
  )
  UNION ALL
  SELECT nxt.actor, nxt.fkey
  FROM rr_tail_keys cur
  CROSS JOIN LATERAL (
    SELECT j6.actor, COALESCE(j6.fairness_key, '__null__') AS fkey
    FROM "{schema}".jobs j6
    WHERE j6.status = 'pending' AND j6.assignment_routed
      AND (j6.actor, COALESCE(j6.fairness_key, '__null__')) > (cur.actor, cur.fkey)
    ORDER BY j6.actor, COALESCE(j6.fairness_key, '__null__')
    LIMIT 1
  ) nxt
),
-- Two disjoint candidate sources, one per routing population:
--   * the label-routed arm (per_actor_capacity x subscribed queues)
--     matches never-claimed rows by their own queue label -- producer
--     placement governs until first claim;
--   * the assignment-routed arm (the repended lateral below, from
--     repend_capacity) matches re-pended rows by their actor's current
--     assignment -- every system re-pend follows the assignment, so a
--     move's running-job tails drain through the target's consumers
--     and never strand on a retired source queue.
-- Disjointness is by the assignment_routed discriminator, so no row can
-- reach identity_dedup twice from the two arms.
candidates AS (
  (SELECT j.id, j.actor, j.identity_key, j.fairness_key,
          __FAIRNESS_RANK_COLUMN__,
          j.priority, j.scheduled_at, pac.residual
  FROM per_actor_capacity pac
  CROSS JOIN LATERAL unnest((SELECT queues FROM params)) AS sq(queue_name)
  CROSS JOIN LATERAL (
__CANDIDATES_LATERAL__
  ) j
  WHERE pac.residual > 0)
  UNION ALL
  (
__REPENDED_LATERAL__
  )
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
-- MATERIALIZED is load-bearing, not documentation: ranked is the fence
-- that finalizes the candidate ranks before top_ids cuts the round's id
-- set. Inlining it would let the planner re-optimize across the cut and
-- re-derive the whole chain per downstream reference (a materialized CTE
-- as an optimization fence prevents that), and the window over the bounded
-- candidate set is cheap to materialize once.
ranked AS MATERIALIZED (
  SELECT id.*,
    ROW_NUMBER() OVER (
      PARTITION BY id.actor
      ORDER BY __RANKED_ORDER_BY__
    ) AS pending_rank
  FROM identity_dedup id
),
-- The round's id set is finalized HERE, before the statement touches
-- the heap again: top_ids carries every column locked needs (rank,
-- fairness rank, ordering keys), so the lock step below never has to
-- re-join a candidate CTE back onto ranked -- a re-join the planner
-- can execute as a materialize-rescan or hash join whose row work
-- grows with the candidate set instead of staying at limit_n.
-- LIMIT $2 (a direct parameter, never a (SELECT ... FROM params)
-- subquery): a parameter folds to its bound value in a custom plan's
-- row estimates, where a subquery bound never folds and the garbage
-- estimate cascades through the CTE chain until the terminal joins
-- believe the round carries millions of rows.
top_ids AS (
  SELECT id, actor, fairness_key, fairness_rank,
         priority, scheduled_at, pending_rank, residual
  FROM ranked
  ORDER BY pending_rank, priority DESC, scheduled_at, id
  LIMIT $2::int
),
-- Lock step: FOR UPDATE row locks taken on a set already bounded by
-- top_ids' LIMIT, driving jobs by primary key through a correlated
-- LATERAL. The correlation on t.id denies the planner's hash-join
-- option -- the option that, at shallow depths, is genuinely cheaper
-- than 50 pkey probes and is therefore chosen on honest costs (a seq
-- scan of a 1k-row backlog beats 50 random probes) -- so the lock
-- step is a nested loop of at most limit_n index probes at EVERY
-- depth. FOR UPDATE inside a FROM-clause subquery is legal, and the
-- pending re-check is the race guard for rows that lost a race for
-- their lock... SKIP LOCKED leaves those rows for the dispatcher that
-- holds them.
locked AS (
  SELECT j.id, j.actor, j.identity_key, j.fairness_key, t.fairness_rank,
         j.priority, j.scheduled_at, t.pending_rank, t.residual
  FROM top_ids t
  CROSS JOIN LATERAL (
    SELECT j2.id, j2.actor, j2.identity_key, j2.fairness_key,
           j2.priority, j2.scheduled_at
    FROM "{schema}".jobs j2
    WHERE j2.id = t.id
      AND j2.status = 'pending'
    FOR UPDATE OF j2 SKIP LOCKED
  ) j
  ORDER BY t.pending_rank, t.priority DESC, t.scheduled_at, t.id
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
  LIMIT $2::int
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
-- The UPDATE finds its rows through a one-shot id array, not a
-- FROM-clause join against eligible: a join's strategy is the
-- planner's choice, and at shallow depths the whole-backlog seq scan
-- plus hash is honestly cheaper than limit_n pkey probes, so the join
-- form re-introduces depth-proportional row work exactly where the
-- backlog is small enough to hide it. ARRAY(SELECT ...) evaluates
-- once as an InitPlan; `id = ANY(<that array>)` is then either a
-- Bitmap Index Scan on jobs_pkey (deep backlogs, where probing is
-- cheaper than scanning) or a scan-level filter (shallow ones, where
-- the filter still emits only the <= limit_n matching rows). Both
-- plans carry at most limit_n rows through every node.
-- j.status = 'pending' stays as the terminal race guard: a candidate
-- that somehow left the pending set between the lock step and this
-- write must never be re-dispatched blind.
WHERE j.id = ANY(ARRAY(SELECT id FROM eligible))
  AND j.status = 'pending'
RETURNING j.*;
"""

# Round-robin cohort enumeration: a recursive loose index scan over the
# (actor, queue, COALESCE(fairness_key, '__null__')) prefix of
# jobs_round_robin_probe_idx. Postgres 18 has no native skip scan
# (no enable_indexskipscan GUC exists), so `SELECT DISTINCT
# fairness_key` over a pair's pending rows is a full scan of them --
# exactly the depth-proportional read this CTE family must not do. The
# recursion replaces it: the seed reads the first cohort key in index
# order, and each step seeks the next strictly-greater
# (actor, queue, key) triple with a row-compare Index Cond -- one
# bounded index seek per DISTINCT COHORT, so the enumeration's work is
# proportional to the number of fairness cohorts in the table, never
# to any cohort's depth.
#
# The recursive term cannot be correlated, so the enumeration is global
# over every (actor, queue, cohort) with a pending row, and the
# candidates lateral below joins it down to the round's (actor, queue)
# pairs; the join filters in memory over the materialized recursion
# output, bounded by the cohort count. The recursive term also cannot
# reference other CTEs, so it cannot pre-scope itself to the round's
# queues; that costs nothing but enumeration steps for other queues'
# cohorts, never probe work.
#
# COALESCE(fairness_key, '__null__') is the partition identity the
# whole round-robin path shares (window PARTITION BY, probe equality,
# and this walk): every unkeyed job forms ONE cohort with any job
# literally keyed '__null__', matching the shipped window semantics
# exactly. The expression is IMMUTABLE and the index repeats it
# VERBATIM -- an expression index serves a query only when the query
# carries the identical expression.
_RR_KEYS_CTE = """\
rr_keys AS (
  (
    SELECT j3.actor, j3.queue,
           COALESCE(j3.fairness_key, '__null__') AS fkey
    FROM "{schema}".jobs j3
    WHERE j3.status = 'pending'
    ORDER BY j3.actor, j3.queue, COALESCE(j3.fairness_key, '__null__')
    LIMIT 1
  )
  UNION ALL
  SELECT nxt.actor, nxt.queue, nxt.fkey
  FROM rr_keys cur
  CROSS JOIN LATERAL (
    SELECT j4.actor, j4.queue,
           COALESCE(j4.fairness_key, '__null__') AS fkey
    FROM "{schema}".jobs j4
    WHERE j4.status = 'pending'
      AND (j4.actor, j4.queue, COALESCE(j4.fairness_key, '__null__'))
          > (cur.actor, cur.queue, cur.fkey)
    ORDER BY j4.actor, j4.queue, COALESCE(j4.fairness_key, '__null__')
    LIMIT 1
  ) nxt
),
"""

# Two-clock split (same doctrine as taskq.backend._sweeps): the
# row-selection bounds in the candidates laterals use statement_timestamp()
# (STABLE) so the planner can serve them as index-level conditions on
# jobs_actor_dispatch_idx / jobs_round_robin_probe_idx — a VOLATILE
# clock_timestamp() bound is only ever a post-scan Filter, and a Filter
# walks every not-yet-due pending row at the head of the index order
# before it can collect LIMIT due rows: measured on a 20k-row
# not-yet-due pending backlog (PG 18, EXPLAIN ANALYZE BUFFERS) the
# volatile bound removed 20,000 rows by filter over 20,172 buffers
# (~5.1 ms) where the stable bound terminates at the range boundary
# (10 buffers, ~0.04 ms). statement_timestamp() is the statement-start
# wall clock — for a LIMIT-ed, sub-second snap it is semantically
# clock_timestamp() evaluated once. The WRITTEN values in the UPDATE
# (started_at / last_heartbeat_at / lock_expires_at) stay
# clock_timestamp(): they must stay co-monotonic with the rows this
# statement writes.
_STRICT_FIFO_CANDIDATES_LATERAL = """\
    SELECT j2.id, j2.actor, j2.identity_key, j2.fairness_key,
           j2.priority, j2.scheduled_at
    FROM "{schema}".jobs j2
    WHERE j2.actor = pac.actor
      AND j2.queue = sq.queue_name
      -- Producer-placed rows only: a re-pended row on this label is the
      -- assignment-routed arm's candidate (see the routing contract in
      -- the module docstring), never this arm's.
      AND NOT j2.assignment_routed
      AND j2.status = 'pending'
      AND j2.scheduled_at <= statement_timestamp()
      AND (j2.schedule_to_close IS NULL OR j2.schedule_to_close > statement_timestamp())
    ORDER BY j2.priority DESC, j2.scheduled_at, j2.id
    -- Direct $5 parameter, not (SELECT oversample FROM params): the
    -- parameter folds to its value in custom-plan estimates; the
    -- subquery form never folds (see top_ids). Execution enforces the
    -- bound either way -- a Limit node stops at its bound regardless
    -- of the plan's estimates -- so this bound is what keeps the
    -- candidate scan itself depth-independent even in plans whose
    -- estimates never saw the value.
    LIMIT pac.residual * $5::int"""

_ROUND_ROBIN_CANDIDATES_LATERAL = """\
    SELECT w.id, w.actor, w.identity_key, w.fairness_key,
           w.fairness_rank, w.priority, w.scheduled_at
    FROM (
      -- Per-cohort bounded probes; the fairness window runs one level
      -- up, over their bounded union. The shipped shape computed
      -- ROW_NUMBER over EVERY due row of the pair and then filtered
      -- fairness_rank <= residual * oversample -- a window cannot
      -- short-circuit, so the WindowAgg (and the scan feeding it) paid
      -- full backlog depth per round even though only the top
      -- residual * oversample rows per cohort could ever survive.
      -- Probing each cohort with ORDER BY + LIMIT residual * oversample
      -- yields the SAME surviving rows with the SAME ranks (rank i
      -- within a cohort is the i-th row of that cohort's priority
      -- order), so selection is bit-identical to the shipped shape
      -- while the window's input is at most
      -- cohorts * residual * oversample rows for the pair.
      --
      -- The probes ride jobs_round_robin_probe_idx
      -- (actor, queue, COALESCE(fairness_key, '__null__'),
      --  priority DESC, scheduled_at, id) WHERE status = 'pending':
      -- the three-column equality prefix is an Index Cond, the
      -- priority DESC order is the index's own within that prefix, and
      -- the STABLE due bounds are index-level conditions — so each
      -- probe is an ordered scan that stops at its LIMIT. The COALESCE
      -- equality is what folds the NULL cohort into one probe: a bare
      -- `fairness_key IS NULL` qual does not combine with the keyed
      -- cohorts' equality probe, and `IS NOT DISTINCT FROM` is never
      -- an Index Cond on this index (measured: a seq scan).
      SELECT c.id, c.actor, c.identity_key, c.fairness_key,
             c.priority, c.scheduled_at,
             ROW_NUMBER() OVER (
               PARTITION BY COALESCE(c.fairness_key, '__null__')
               ORDER BY c.priority DESC, c.scheduled_at, c.id
             ) AS fairness_rank
       FROM rr_keys k
       CROSS JOIN LATERAL (
         SELECT j2.id, j2.actor, j2.identity_key, j2.fairness_key,
                j2.priority, j2.scheduled_at
         FROM "{schema}".jobs j2
         WHERE j2.actor = pac.actor
           AND j2.queue = sq.queue_name
           -- Producer-placed rows only: a re-pended row on this label is
           -- the assignment-routed arm's candidate (see the routing
           -- contract in the module docstring), never this arm's.
           AND NOT j2.assignment_routed
           AND j2.status = 'pending'
           AND COALESCE(j2.fairness_key, '__null__') = k.fkey
           AND j2.scheduled_at <= statement_timestamp()
           AND (j2.schedule_to_close IS NULL OR j2.schedule_to_close > statement_timestamp())
         ORDER BY j2.priority DESC, j2.scheduled_at, j2.id
         LIMIT pac.residual * $5::int
       ) c
      -- The pair restriction sits OUTSIDE the probe: rr_keys is the
      -- global cohort enumeration (the recursive term cannot be
      -- correlated), and this filter narrows it to the lateral's own
      -- (actor, queue) pair over the materialized recursion output --
      -- in-memory filter work bounded by the cohort count, never a
      -- re-walk of any cohort's rows.
      WHERE k.actor = pac.actor
        AND k.queue = sq.queue_name
    ) w"""

# The assignment-routed candidates arm, strict-FIFO variant: re-pended
# rows (assignment_routed, any queue label) of actors whose
# current assignment is subscribed this round, admitted per fairness
# cohort with the same residual * oversample bound as the label-routed
# arm's probes and carrying no fairness rank (this variant ranks by
# priority alone downstream). The per-cohort probes ride
# jobs_assignment_routed_probe_idx: actor equality plus the COALESCE-normalized
# cohort equality is a two-column Index Cond prefix, the index's own
# (priority DESC, scheduled_at, id) order serves the probe's ORDER BY
# without a sort, and the LIMIT stops the scan -- the same depth
# contract the label-routed arm keeps on jobs_actor_dispatch_idx.
# Ranks are not computed here (the strict variant orders by priority
# downstream), but the admission stays per-cohort rather than one
# queue-agnostic probe because the only index over this population is
# cohort-keyed: a single ORDER BY priority probe over it would need a
# sort over every due re-pended row of the actor -- depth-proportional
# work the depth contract forbids.
_REPENDED_STRICT_FIFO_LATERAL = """\
    SELECT p.id, p.actor, p.identity_key, p.fairness_key,
           NULL::bigint AS fairness_rank,
           p.priority, p.scheduled_at, rc.residual
    FROM repend_capacity rc
    CROSS JOIN rr_tail_keys tk
    CROSS JOIN LATERAL (
      SELECT j2.id, j2.actor, j2.identity_key, j2.fairness_key,
             j2.priority, j2.scheduled_at
      FROM "{schema}".jobs j2
      WHERE j2.actor = rc.actor
        AND j2.assignment_routed
        AND j2.status = 'pending'
        AND COALESCE(j2.fairness_key, '__null__') = tk.fkey
        AND j2.scheduled_at <= statement_timestamp()
        AND (j2.schedule_to_close IS NULL OR j2.schedule_to_close > statement_timestamp())
      ORDER BY j2.priority DESC, j2.scheduled_at, j2.id
      LIMIT rc.residual * $5::int
    ) p
    WHERE tk.actor = rc.actor
      AND rc.residual > 0"""

# The assignment-routed candidates arm, round-robin variant: the same
# per-cohort bounded probes, with the fairness window running over the
# bounded probe union exactly as the label-routed arm's does -- one
# partition per cohort, ranks computed within the cohort, so a
# re-pended row gets its cohort turn beside never-claimed rows instead
# of sorting behind them. A cohort carrying BOTH populations produces
# two rank series (the label-routed arm ranks its own probe's rows, this
# arm ranks its own); ties interleave by priority in the downstream
# eligible order, and every cohort's rows from both populations are
# admitted each round, so neither series can starve the other.
_REPENDED_ROUND_ROBIN_LATERAL = """\
    SELECT w.id, w.actor, w.identity_key, w.fairness_key,
           w.fairness_rank, w.priority, w.scheduled_at, rc.residual
    FROM repend_capacity rc
    CROSS JOIN LATERAL (
      SELECT c.id, c.actor, c.identity_key, c.fairness_key,
             c.priority, c.scheduled_at,
             ROW_NUMBER() OVER (
               PARTITION BY COALESCE(c.fairness_key, '__null__')
               ORDER BY c.priority DESC, c.scheduled_at, c.id
             ) AS fairness_rank
      FROM rr_tail_keys tk
      CROSS JOIN LATERAL (
        SELECT j2.id, j2.actor, j2.identity_key, j2.fairness_key,
               j2.priority, j2.scheduled_at
        FROM "{schema}".jobs j2
        WHERE j2.actor = rc.actor
          AND j2.assignment_routed
          AND j2.status = 'pending'
          AND COALESCE(j2.fairness_key, '__null__') = tk.fkey
          AND j2.scheduled_at <= statement_timestamp()
          AND (j2.schedule_to_close IS NULL OR j2.schedule_to_close > statement_timestamp())
        ORDER BY j2.priority DESC, j2.scheduled_at, j2.id
        LIMIT rc.residual * $5::int
      ) c
      -- The actor restriction sits OUTSIDE the probe, same doctrine as
      -- the label-routed arm: rr_tail_keys is the global cohort
      -- enumeration (the recursive term cannot be correlated), and
      -- this filter narrows it over the materialized recursion output.
      WHERE tk.actor = rc.actor
    ) w
    WHERE rc.residual > 0"""


def _render_dispatch_sql(
    template: str,
    *,
    fairness_rank_column: str,
    rr_keys_cte: str,
    candidates_lateral: str,
    repended_lateral: str,
    ranked_order_by: str,
    eligible_candidates_order_by: str,
) -> str:
    """Substitute the per-variant fragments into the shared dispatch template.

    ``{schema}`` placeholders are preserved so the returned constant can be
    rendered with ``.format(schema=...)`` at the call site.  ``rr_keys_cte``
    is empty for the strict-FIFO variant (no label-cohort enumeration arm);
    the template's ``WITH RECURSIVE`` keyword tolerates a list with no
    recursive CTE, so one template serves both variants.  ``rr_tail_keys``
    (the re-pended cohort enumeration) is shared verbatim by both variants
    in the template itself; only the two candidates arms differ per
    variant, through ``candidates_lateral`` (label-routed) and
    ``repended_lateral`` (assignment-routed).
    """
    return (
        template.replace("__FAIRNESS_RANK_COLUMN__", fairness_rank_column)
        .replace("__RR_KEYS_CTE__", rr_keys_cte)
        .replace("__CANDIDATES_LATERAL__", candidates_lateral)
        .replace("__REPENDED_LATERAL__", repended_lateral)
        .replace("__RANKED_ORDER_BY__", ranked_order_by)
        .replace("__ELIGIBLE_CANDIDATES_ORDER_BY__", eligible_candidates_order_by)
    )


DISPATCH_STRICT_FIFO_SQL: str = _render_dispatch_sql(
    _DISPATCH_SQL_TEMPLATE,
    fairness_rank_column="NULL::bigint AS fairness_rank",
    rr_keys_cte="",
    candidates_lateral=_STRICT_FIFO_CANDIDATES_LATERAL,
    repended_lateral=_REPENDED_STRICT_FIFO_LATERAL,
    ranked_order_by="id.priority DESC, id.scheduled_at, id.id",
    eligible_candidates_order_by="l.priority DESC, l.scheduled_at",
)

DISPATCH_ROUND_ROBIN_SQL: str = _render_dispatch_sql(
    _DISPATCH_SQL_TEMPLATE,
    fairness_rank_column="j.fairness_rank",
    rr_keys_cte=_RR_KEYS_CTE,
    candidates_lateral=_ROUND_ROBIN_CANDIDATES_LATERAL,
    repended_lateral=_REPENDED_ROUND_ROBIN_LATERAL,
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
            # Why redacted: this text leaves the trust boundary for whatever
            # telemetry backend is configured. str() of an asyncpg
            # PostgresError appends the server's DETAIL line, which quotes the
            # offending row values -- idempotency_key / identity_key /
            # fairness_key are all caller-supplied.
            span.set_status(StatusCode.ERROR, safe_exception_message(exc))
            record_exception_safe(span, exc)
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
