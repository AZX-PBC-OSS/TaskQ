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

Depth bounding: every jobs access in this statement is a per-round
bounded probe, never a scan of the pending backlog.  The
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

* ``per_actor_capacity`` / ``repend_capacity`` never scan the registry:
  the label-routed actor set is enumerated from jobs by the variant's
  keys walk (``pa_keys`` / ``rr_keys`` — recursive loose index scans,
  one bounded seek per distinct key on the round's queues) and read
  back through actor_config by primary key, while ``repend_capacity``
  filters on the assignment-queue index. A Seq Scan of actor_config is
  a per-round cost proportional to the fleet-wide REGISTERED-actor
  count (pinned by
  tests/test_dispatch_actor_registry_scope_bound.py) and, because the
  table is usually never analyzed, a garbage estimate that cascades
  through the candidate chain's nested loops.
* ``candidates`` reads at most ``residual * oversample`` admitted rows
  per (actor, queue) probe — but the SCAN bound each probe's innermost
  LIMIT carries is the pure parameter expression ``$2 * $5`` (limit_n x
  oversample), and the exact ``residual * oversample`` admission window
  is re-imposed one level up, over the bounded probe output, by a
  rank-window cut (``probe_rank`` / ``cohort_rank``). The split exists
  for the planner, not the executor: a LIMIT the planner cannot fold to
  a constant is estimated as a fixed fraction of the scanned index
  range, so an unfoldable ``residual * oversample`` bound keeps the
  plan's ESTIMATED cost depth-proportional even though execution stops
  at the bound — and past the default ``jit_above_cost`` (100000) that
  estimate makes Postgres JIT-compile the whole statement on every
  dispatch round (~1 s of Optimization+Emission measured at a 30k due
  backlog, where the scan itself is ~1.5 ms; pinned by
  tests/test_dispatch_backlog_depth_bound.py's JIT oracle). The folded
  ``$2 * $5`` bound makes the estimate track the LIMIT that actually
  bounds execution; for an uncapped actor residual IS limit_n so the
  two bounds coincide exactly, and a capped actor with residual above
  limit_n is bound by the round's own limit first (its tail drains on
  later rounds, the depth contract's standing rule). The rank cut sits
  BEFORE identity_dedup deliberately: the shipped window counted
  identity-duplicate rows, so a post-dedup cut would silently widen it.
* ``top_ids`` finalizes the LIMIT-ed id set BEFORE the statement
  touches the heap a second time, and ``locked`` then drives ``jobs``
  by primary key through a correlated LATERAL — a materialized CTE is an
  optimization fence: the planner may otherwise choose a nested loop over
  the LIMITing subquery. A bounded, locked CTE whose UPDATE joins by
  id ensures the dispatch never re-optimizes across the candidacy cut
  and respects the admission decision made by top_ids.
* every actor_config readback outside the two capacity CTEs
  (``capped_ranked``, ``sliding_locked``, ``eligible_candidates``, and
  the ``stamp`` UPDATE) is a correlated primary-key LATERAL or a
  one-shot ``ANY(ARRAY(SELECT ...))`` InitPlan — never a plain join the
  planner can serve as a Seq Scan + hash over the whole registry.
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
Under peer lock contention the window also bounds the slide: a round
whose whole window is row-locked expands the window geometrically
(bounded, in ``taskq.backend._dispatch``) instead of scanning deeper
without a bound — see :data:`DISPATCH_CLAIMABLE_PROBE_SQL`.

Fairness contract (cross-actor rotation): within a round, selection is
``pending_rank`` first — every actor's head job before any actor's
second — then priority. Across rounds, the remaining tie is broken by
``actor_config.last_claimed_at ASC NULLS FIRST`` (never-claimed actors
first, then least-recently-claimed), a durable per-actor stamp the
claim statement's own ``stamp`` CTE writes for every admitted actor
(its SKIP LOCKED driver keeps the claim wait-free — a peer mid-claim
on the same actors costs the stamp, never the round). Without the
stamp the tie falls to ``scheduled_at, id`` — a stable total order
that re-elects the same prefix of actors every round once more actors
hold due work than the round's limit admits, starving the rest
silently (pinned by tests/test_dispatch_actor_cohort_rotation.py and
tests/test_fleet_fairness_starvation.py). Priority still dominates the
stamp, so the operator's priority bias keeps its meaning; the stamp
removes only the accidental starvation among equal-priority peers. The
stamp rides the registry row rather than the jobs table so the
rotation read adds no per-round probe: it is carried through the
already-materialized ``ranked`` window to the cut.

Reservation-headroom contract (consumer-slot isolation): dispatch has
no in-process knowledge of an actor's declared reservations — that
mapping lives in each worker's rate-limit registry — but it can read
the only durable signal there is: which actors' claimed jobs currently
hold live reservation slots (``reservation_slots.job_id`` →
``jobs.actor``), and how many slots in those buckets remain acquirable
right now. ``reservation_holdings`` / ``reservation_headroom`` derive
that once per statement, and both capacity CTEs fold it in:
``residual = LEAST(capacity residual, headroom)``. An actor whose held
bucket is FULL is admitted nothing this round — without the gate its
pending rows are claimed into the shared ``max_concurrency`` consumer
pool, denied by the post-claim ``acquire_for_actor``, and snoozed,
spending one consumer coroutine per row per cycle on work that cannot
run (measured: 25-45% throughput loss for a co-located healthy actor;
pinned by
tests/test_fleet_saturated_actor_consumer_slot_isolation.py). The gate
is deliberately a damper, not an authority: the post-claim acquire
remains the decision of record (a race between the read and a peer's
acquire degrades to one bounded denial, never a wrong admission), the
first claim of a never-running actor always gets through (NULL
headroom leaves the residual untouched — that is what lets capacity
ever be taken), and a full bucket implies a holder already running
whose completion or lease expiry re-opens the gate, so a saturated
actor drains the moment capacity frees (no starvation inversion).
Headroom bounds the round's candidate window (residual * oversample);
final admission can overshoot a partially-free bucket by the
oversample factor, the same best-effort doctrine running_per_actor
documents for max_concurrent, and self-corrects on the next round when
the over-claimed jobs fill the slots. Keyed and queue-cap buckets ride
the same derivation for free — the gate keys off holder state, not
declarations.

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
with disjoint partial indexes (``jobs_unrouted_actor_dispatch_idx`` /
``jobs_unrouted_round_robin_probe_idx`` for producer-placed rows,
``jobs_assignment_routed_probe_idx`` for re-pended rows — each partial
on its own population's half of the marker, so neither arm's probe ever
walks the other's rows),
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
    record_dispatch_failure,
    safe_start_span,
)
from taskq.obs._redact_exc import record_exception_text, render_exception

__all__ = [
    "DISPATCH_CLAIMABLE_PROBE_SQL",
    "DISPATCH_ROUND_ROBIN_SQL",
    "DISPATCH_STRICT_FIFO_SQL",
    "dispatch_batch",
]

logger: structlog.stdlib.BoundLogger = get_logger(__name__)


# Shared dispatch CTE template.  ``{schema}`` is left intact so callers
# (and tests) can ``.format(schema=...)`` at render time; the ``__*__``
# tokens are substituted by _render_dispatch_sql.
_DISPATCH_SQL_TEMPLATE = """\
-- WITH RECURSIVE: both variants' label-routed keys enumerations are
-- recursive loose index scans (pa_keys for strict-FIFO, rr_keys for
-- round-robin), and a RECURSIVE keyword over a list is a no-op
-- permission for the non-recursive arms, so one template serves both
-- variants.
WITH RECURSIVE params AS (
  SELECT
    $1::text[]   AS queues,
    $2::int      AS limit_n,
    $3::uuid     AS worker_id,
    $4::interval AS lock_lease,
    $5::int      AS oversample
),
__KEYS_CTE__
-- The round's label-routed actor set: DISTINCT actors holding at least
-- one pending, producer-placed (NOT assignment_routed) row on one of the
-- round's queues. Read off the keys enumeration (one bounded index seek
-- per distinct key, never a scan) so per_actor_capacity is driven by the
-- round's own population rather than by a scan of actor_config: the
-- registry is one row per REGISTERED actor fleet-wide, and a round's
-- cost must not grow with how many unrelated actors happen to be
-- registered (the registry-scope oracle,
-- tests/test_dispatch_actor_registry_scope_bound.py, pins this).
pa_actors AS (
  SELECT DISTINCT actor FROM __KEYS_SOURCE__
),
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
-- Which (actor, bucket) pairs have a live hold right now, derived from
-- holder state alone: reservation_slots.job_id points at the claimed
-- jobs row, whose actor is the holder. The derivation needs no
-- actor -> bucket declaration mapping (the DB has none — declarations
-- live in the workers' rate-limit registries), so static, keyed, and
-- queue-cap buckets all ride it. "Live" mirrors the acquire statement's
-- acquirability predicate exactly (taskq.ratelimit.reservation): a held
-- row whose lease has not yet expired — with the deliberate clock swap
-- to statement_timestamp() (STABLE), the same two-clock doctrine the
-- candidates laterals keep, so the liveness bound can ride
-- reservation_slots_lease_expires_idx as an Index Cond and the whole
-- statement keeps one snapshot. A lease that expires mid-statement
-- reads as held — a conservative under-admission for one round,
-- self-correcting on the next.
--
-- Best-effort under concurrent dispatchers, on the same doctrine as
-- running_per_actor above: this snapshot is read ONCE, before `locked`
-- takes its FOR UPDATE SKIP LOCKED row locks, and never rechecked; the
-- post-claim acquire_for_actor is the admission authority, so a stale
-- read degrades to one bounded denial round trip, never a wrong one.
--
-- Cost: the outer scan reads only live-held slot rows (the table is
-- bounded by total reservation slots fleet-wide, never by jobs
-- backlog), and each holder's actor is one primary-key probe — the
-- correlated LATERAL with LIMIT 1 defeats the subquery pull-up that
-- would flatten the lookup into a hash join over the whole jobs table
-- (the same doctrine per_actor_capacity relies on). Zero held slots —
-- the common case — costs one scan of an empty/small table and no jobs
-- probes, so the depth oracles' row-visit counts are unchanged.
reservation_holdings AS (
  SELECT DISTINCT hj.actor, lh.bucket_name
  FROM (
    SELECT rs.bucket_name, rs.job_id
    FROM "{schema}".reservation_slots rs
    WHERE rs.job_id IS NOT NULL
      AND (rs.lease_expires_at IS NULL OR rs.lease_expires_at >= statement_timestamp())
  ) lh
  CROSS JOIN LATERAL (
    SELECT hj2.actor
    FROM "{schema}".jobs hj2
    WHERE hj2.id = lh.job_id
    LIMIT 1
  ) hj
),
-- Acquirable slots per held bucket, folded to the actor's binding
-- constraint: an actor's jobs AND-compose their declared reservations,
-- so the least-free held bucket caps how many more of the actor's rows
-- can actually run. The free predicate is the acquire statement's own
-- (job_id IS NULL OR lease expired — an expired lease is the design's
-- abandonment signal and is acquirable on the spot), again on
-- statement_timestamp(). Each probe is a primary-key-prefix range over
-- one bucket's rows — bounded by the bucket's slot count, never by
-- backlog depth. An actor holding nothing is absent here, and
-- LEAST(residual, NULL) is the residual unchanged (LEAST ignores NULL
-- arguments) — the first claim of a never-running actor is never
-- gated.
reservation_headroom AS (
  SELECT h.actor, MIN(f.free_slots) AS headroom
  FROM reservation_holdings h
  CROSS JOIN LATERAL (
    SELECT count(*) AS free_slots
    FROM "{schema}".reservation_slots rs2
    WHERE rs2.bucket_name = h.bucket_name
      AND (rs2.job_id IS NULL OR rs2.lease_expires_at < statement_timestamp())
  ) f
  GROUP BY h.actor
),
-- Per-actor admission for the label-routed arm. The driver is pa_actors
-- (the round's own label-routed population, enumerated from jobs by the
-- keys walk), NOT a scan of actor_config: actor_config holds one row per
-- registered actor FLEET-WIDE, sits far below autovacuum's insert
-- threshold, and is therefore usually never analyzed, so a scan of it is
-- both a registry-proportional cost per round and a garbage estimate
-- (~440 rows even for a one-actor fleet) that cascades through the
-- candidate chain's nested loops. Driving from pa_actors makes the
-- registry read one primary-key probe per live actor — bounded by the
-- round's own population, independent of the registered-actor count.
-- The LATERAL correlation on pa.actor denies the planner the hash-join
-- path it would otherwise take over the unanalyzed table.
--
-- The has_pending probe is retained as a defense-in-depth re-check of
-- exactly the population pa_actors already enumerated (pending,
-- NOT assignment_routed, on one of the round's queues): it is a
-- correlated per-queue LATERAL, not an EXISTS. An EXISTS is a semi-join,
-- and the planner is free to execute it as a hash semi-join over a Seq
-- Scan of the entire pending backlog whenever actor_config's row
-- estimate makes one pass over jobs look cheaper than per-actor probes.
-- The LATERAL shape removes the planner's option instead of arguing
-- with its costs: the correlation denies the unparameterized (hashable)
-- inner path, and the per-queue equality from unnest plus the ORDER BY
-- over jobs_unrouted_actor_dispatch_idx's (actor, queue,
-- priority DESC, ...) key pins the probe to an index-ordered
-- first-entry read, bounded by the number of round queues per actor,
-- never by backlog depth. A
-- queue = ANY(...) array predicate cannot serve that ORDER BY (an
-- ScalarArrayOp breaks the index's single ordered stream), which is why
-- the fan-out is over unnest(queues) with one plain-equality probe per
-- queue.
--
-- The probe is scoped to the queues in the round's params, NOT the
-- actor's home queue: an enqueue(queue = ...) override that lands a
-- pending row on any subscribed queue keeps that actor probed.
per_actor_capacity AS (
  SELECT
    base.actor,
    base.max_concurrent,
    -- The reservation-headroom fold: an actor holding a live slot in a
    -- bucket with no acquirable slot left is admitted NOTHING this
    -- round — its pending rows could only be claimed into consumer
    -- coroutines whose acquire_for_actor must deny them, spending the
    -- worker's shared max_concurrency slots on work that cannot run
    -- (the consumer-slot churn this gate exists to remove). A
    -- partially free held bucket admits at most its free count. An
    -- actor with no live holdings has headroom NULL and LEAST ignores
    -- NULL, so the base residual is untouched — the gate never blocks
    -- a first claim, and a full bucket implies a holder already
    -- running whose completion (or lease expiry) re-opens admission,
    -- so saturated work drains the moment capacity frees.
    LEAST(base.residual, rh.headroom) AS residual,
    base.actor_claimed_at
  FROM (
    SELECT
      pa.actor,
      ac.max_concurrent,
      CASE WHEN ac.max_concurrent IS NULL
           THEN (SELECT limit_n FROM params)
           ELSE GREATEST(ac.max_concurrent - COALESCE(r.in_flight, 0), 0)
      END AS residual,
      -- The cross-round fairness signal, carried from the registry row to
      -- every ORDER BY that cuts a round's admitted set. NULL means "never
      -- claimed": those actors sort first (NULLS FIRST at the cut), then
      -- least-recently-claimed. Without it the cross-actor tiebreak among
      -- rank-1 rows is priority/scheduled_at/id — a STABLE total order that
      -- re-elects the same prefix of actors every round (each winner refills
      -- its own rank-1 slot from its own backlog with the same relative
      -- key), so every actor past the limit starves while the queue drains
      -- briskly. The stamp itself is the stamp CTE at the foot of this
      -- statement; its SKIP LOCKED driver is what lets the write ride the
      -- claim: a plain UPDATE would wait on any peer transaction holding a
      -- claimed actor's registry row (a concurrent dispatcher mid-round,
      -- an operator's move_actor_queue flip), coupling this round's
      -- latency — and, for a peer whose transaction is held open, its
      -- liveness — to the peer's commit. Skipping a locked row degrades
      -- the stamp bounded-ly (that actor re-competes with its older stamp
      -- next round; self-correcting), never the claim's liveness.
      ac.last_claimed_at AS actor_claimed_at
    FROM pa_actors pa
    CROSS JOIN params p
    CROSS JOIN LATERAL (
      SELECT ac.max_concurrent, ac.last_claimed_at
      FROM "{schema}".actor_config ac
      WHERE ac.actor = pa.actor
      -- The LIMIT is not decorative: a bare pkey-equality subquery is
      -- pulled up into a plain join, which the planner then serves as a
      -- Seq Scan + hash over the whole (usually unanalyzed) registry.
      -- LIMIT 1 defeats the pull-up, keeping this a nested-loop pkey
      -- probe per live actor — the same doctrine the has_pending probe
      -- below relies on. Exact because actor is the primary key.
      LIMIT 1
    ) ac
    LEFT JOIN running_per_actor r ON r.actor = pa.actor
    CROSS JOIN LATERAL (
      SELECT 1 AS has_pending
      FROM unnest(p.queues) AS pq(q)
      CROSS JOIN LATERAL (
        SELECT 1
        FROM "{schema}".jobs j
        WHERE j.actor = pa.actor
          AND j.queue = pq.q
          AND NOT j.assignment_routed
          AND j.status = 'pending'
        ORDER BY j.priority DESC, j.scheduled_at, j.id
        LIMIT 1
      ) anyq
      LIMIT 1
    ) hp
    WHERE hp.has_pending IS NOT NULL
  ) base
  LEFT JOIN reservation_headroom rh ON rh.actor = base.actor
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
-- The assignment-routed half of the routing contract: the actors whose
-- CURRENT stored assignment is among this round's subscribed queues,
-- with the same residual arithmetic as per_actor_capacity. The
-- assignment IS the routing here: every re-pended row of such an actor
-- (any queue label, assignment_routed, pending) is a candidate
-- for this round no matter which label it carries -- the
-- move_actor_queue tail contract.
--
-- The driver is the DISTINCT actor set of rr_tail_keys (the re-pended
-- cohort enumeration), NOT a scan of actor_config filtered by
-- assignment: an actor with zero re-pended rows contributes zero
-- candidates downstream (the repended lateral's rr_tail_keys join
-- annihilates its probes), so restricting the driver to actors that
-- actually hold re-pended rows is selection-identical, and the registry
-- read collapses to one primary-key probe per such actor — bounded by
-- the re-pended population, independent of the fleet-wide
-- registered-actor count, and immune to the planner's honest
-- small-registry Seq Scan preference that a plain
-- ac.queue = ANY(queues) filter leaves open. The LIMIT 1 keeps the
-- probe correlated (a bare pkey-equality subquery is pulled up into a
-- plain join and the Seq Scan returns — same doctrine as
-- per_actor_capacity). ac.queue = ANY(p.queues) reads the CROSS JOINed
-- params column (a plain text[] value), NOT a subquery -- =
-- ANY(subquery) iterates the subquery's ROWS, and a one-row
-- array-valued subquery would compare the label against the whole
-- array; the column form is the array membership test.
repend_capacity AS (
  SELECT
    base.actor,
    base.max_concurrent,
    -- The same reservation-headroom fold as per_actor_capacity: a
    -- re-pended row of a reservation-saturated actor (a denied job
    -- coming back through the snooze/promote path routes here by its
    -- assignment_routed marker) is no more runnable than a
    -- producer-placed one — claiming it would churn a consumer slot
    -- into another denial.
    LEAST(base.residual, rh.headroom) AS residual,
    base.actor_claimed_at
  FROM (
    SELECT
      ta.actor,
      ac.max_concurrent,
      CASE WHEN ac.max_concurrent IS NULL
           THEN (SELECT limit_n FROM params)
           ELSE GREATEST(ac.max_concurrent - COALESCE(r.in_flight, 0), 0)
      END AS residual,
      ac.last_claimed_at AS actor_claimed_at
    FROM (SELECT DISTINCT actor FROM rr_tail_keys) ta
    CROSS JOIN params p
    CROSS JOIN LATERAL (
      SELECT ac.max_concurrent, ac.queue, ac.last_claimed_at
      FROM "{schema}".actor_config ac
      WHERE ac.actor = ta.actor
      LIMIT 1
    ) ac
    LEFT JOIN running_per_actor r ON r.actor = ta.actor
    WHERE ac.queue = ANY(p.queues)
  ) base
  LEFT JOIN reservation_headroom rh ON rh.actor = base.actor
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
          j.priority, j.scheduled_at, pac.residual, pac.actor_claimed_at,
          pac.max_concurrent
  FROM per_actor_capacity pac
  -- unnest of the $1 parameter directly, not of a (SELECT queues FROM
  -- params) subquery: in a custom plan the bound parameter folds to a
  -- constant array and the planner estimates the unnest at the array's
  -- true length, where the subquery form keeps the opaque default
  -- guess and every downstream nested loop is costed at ten phantom
  -- fan-out rows per actor.
  CROSS JOIN LATERAL unnest($1::text[]) AS sq(queue_name)
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
      c.id, c.actor, c.fairness_key, c.fairness_rank, c.priority, c.scheduled_at, c.residual,
      c.actor_claimed_at, c.max_concurrent
    FROM candidates c
    LEFT JOIN running_identities ri ON ri.actor = c.actor AND ri.identity_key = c.identity_key
    WHERE ri.identity_key IS NULL
      AND c.identity_key IS NOT NULL
    ORDER BY c.actor, c.identity_key, c.priority DESC, c.scheduled_at, c.id
  )
  UNION ALL
  (
    SELECT c.id, c.actor, c.fairness_key, c.fairness_rank, c.priority, c.scheduled_at, c.residual,
           c.actor_claimed_at, c.max_concurrent
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
-- The lock step is SPLIT by whether the actor carries a concurrency
-- cap, because the two populations need opposite things from the
-- pre-lock cut and one shape cannot serve both.
--
-- Uncapped work (sliding_locked) must NOT be cut before the lock.
-- Cutting first fixes the candidate window, and a peer
-- holding those rows leaves this dispatcher with nothing to fall back
-- to: it reports an empty round while claimable rows sit unlocked
-- behind the window. Two dispatchers compute the same window, so the
-- second adds no throughput at all and a fleet cannot be scaled out.
-- Locking straight down the ranked stream is what fixes it: Postgres
-- places LockRows BELOW Limit when both sit at the same query level,
-- and the lock node moves to its next input row when a lock would
-- block, so SKIP LOCKED slides past a peer's held rows to deeper
-- unlocked ones and the Limit still stops the scan at limit_n
-- ACQUIRED rows. The lock node here reads the MATERIALIZED candidate
-- window rather than the live index: examined rows
-- are bounded by the window itself (residual * oversample per
-- (actor, queue) cohort probe), which is what keeps the depth
-- contract intact -- but a peer holding the WHOLE window still
-- empties the round while deeper rows sit unlocked. Reaching those is
-- the caller's bounded window-expansion pass (_dispatch.py), never an
-- unbounded scan here.
--
-- Capped work (top_ids -> locked) keeps a deliberate pre-lock window,
-- because sliding is the hazard there rather than the fix: a scan free
-- to slide past locked rows would walk an actor's whole backlog
-- admitting rows its cap does not allow. The window is bounded by
-- limit_n before the lock, and `eligible` re-limits admission after it
-- to the capacity actually remaining (actor_rank <= max_concurrent -
-- in_flight), so neither failure direction is reachable: the cap is
-- never exceeded, and the window never outruns the round's bound.
--
-- The cap membership test reads the max_concurrent the capacity CTEs
-- already carried out of actor_config this same statement (one snapshot,
-- so the carried value IS the registry's), instead of re-probing the
-- registry per ranked row: ranked is bounded, but a join here is what
-- the planner used to serve as a Seq Scan + hash over the whole
-- registry per round (the registry-proportional work the registry-scope
-- oracle, tests/test_dispatch_actor_registry_scope_bound.py, forbids).
capped_ranked AS (
  SELECT r.*
  FROM ranked r
  WHERE r.max_concurrent IS NOT NULL
),
-- The windowed cut, capped actors only. LIMIT is a direct $n
-- expression, never a (SELECT ... FROM params) subquery: a parameter
-- folds to its bound value in a custom plan's row estimates, where a
-- subquery bound never folds and the garbage estimate cascades
-- through the CTE chain until the terminal joins believe the round
-- carries millions of rows.
top_ids AS (
  SELECT id, actor, fairness_key, fairness_rank,
         priority, scheduled_at, pending_rank, residual, actor_claimed_at
  FROM capped_ranked
  -- The cross-actor cut rotates: after pending_rank and priority, the
  -- least-recently-claimed actor wins (never-claimed first), so a cohort
  -- beyond the round's limit is served on a later round instead of never.
  -- The stamp column is read from the ranked (already materialized)
  -- window, so the rotation key adds no per-round probe work.
  ORDER BY pending_rank, priority DESC, actor_claimed_at ASC NULLS FIRST,
           scheduled_at, id
  LIMIT $2::int
),
-- Capped lock step: FOR UPDATE on a set already bounded by top_ids'
-- LIMIT, driving jobs by primary key through a correlated LATERAL.
-- The correlation on t.id denies the planner's hash-join option --
-- the option that, at shallow depths, is genuinely cheaper than 50
-- pkey probes and is therefore chosen on honest costs -- so this step
-- is a nested loop of at most limit_n index probes at EVERY depth.
-- The pending re-check is the race guard for rows that lost a race
-- for their lock; SKIP LOCKED leaves those to the holder.
locked AS (
  SELECT j.id, j.actor, j.identity_key, j.fairness_key, t.fairness_rank,
         j.priority, j.scheduled_at, t.pending_rank, t.residual, t.actor_claimed_at
  FROM top_ids t
  CROSS JOIN LATERAL (
    SELECT j2.id, j2.actor, j2.identity_key, j2.fairness_key,
           j2.priority, j2.scheduled_at
    FROM "{schema}".jobs j2
    WHERE j2.id = t.id
      AND j2.status = 'pending'
    FOR UPDATE OF j2 SKIP LOCKED
  ) j
),
-- Uncapped lock step: the ORDER BY, the LIMIT and the row lock all sit
-- at ONE query level, which is what puts LockRows under Limit and lets
-- the skip slide. The jobs side is re-found through a one-shot id
-- array (the terminal UPDATE's own doctrine: ARRAY(SELECT ...) over the
-- already-materialized, already cap-filtered ranked window evaluates
-- once as an InitPlan, and j2.id = ANY(<that array>) is then a Bitmap
-- Index Scan on the primary key at depth or a scan-level filter in the
-- shallows — never a hash build over the whole pending backlog, which
-- an honest-cost planner picks for a plain ranked⋈jobs join exactly
-- where the table is small enough to hide the depth coupling). The
-- ranked re-join for the ordering columns hashes only the bounded
-- materialized window. Ranked is materialized, so this reads the
-- finalized candidate ranks in order and stops at limit_n acquired
-- rows.
sliding_locked AS (
  SELECT j2.id, j2.actor, j2.identity_key, j2.fairness_key, r.fairness_rank,
         j2.priority, j2.scheduled_at, r.pending_rank, r.residual, r.actor_claimed_at
  FROM "{schema}".jobs j2
  JOIN ranked r ON r.id = j2.id
  WHERE j2.id = ANY(ARRAY(
    SELECT r2.id
    FROM ranked r2
    WHERE r2.max_concurrent IS NULL
  ))
    AND j2.status = 'pending'
  -- Same rotation cut as top_ids: the SKIP LOCKED slide walks the
  -- materialized ranked stream in this order, so a peer holding the
  -- window's leading rows yields the least-recently-claimed actors
  -- behind them rather than re-deriving a static prefix.
  ORDER BY r.pending_rank, r.priority DESC, r.actor_claimed_at ASC NULLS FIRST,
           r.scheduled_at, r.id
  LIMIT $2::int
  FOR UPDATE OF j2 SKIP LOCKED
),
claimed AS (
  SELECT * FROM locked
  UNION ALL
  SELECT * FROM sliding_locked
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
  FROM claimed l
  -- Same correlated pkey LATERAL as capped_ranked: one probe per
  -- claimed row (at most limit_n + the capped window), never a hash
  -- over the whole registry; the LIMIT 1 defeats the subquery pull-up
  -- that would flatten this into a plain join. Inner-safe: every
  -- claimed actor holds an actor_config row (per_actor_capacity /
  -- repend_capacity require it).
  CROSS JOIN LATERAL (
    SELECT ac.max_concurrent
    FROM "{schema}".actor_config ac
    WHERE ac.actor = l.actor
    LIMIT 1
  ) ac
  LEFT JOIN running_per_actor r ON r.actor = l.actor
  WHERE ac.max_concurrent IS NULL
     OR COALESCE(r.in_flight, 0) < ac.max_concurrent
),
eligible AS (
  SELECT ec.id, ec.actor
  FROM eligible_candidates ec
  WHERE ec.max_concurrent IS NULL
     OR ec.actor_rank <= ec.max_concurrent - ec.in_flight
  -- The re-limit keeps the same rotation order the lock stages cut on,
  -- so the post-lock admission never re-elects a different prefix.
  ORDER BY ec.pending_rank, ec.fairness_rank NULLS LAST, ec.priority DESC,
           ec.actor_claimed_at ASC NULLS FIRST, ec.scheduled_at
  LIMIT $2::int
),
-- The rotation stamp: every actor this round admitted is stamped with
-- this statement's statement_timestamp() (one value for the whole round,
-- so same-round winners tie on the stamp and fall through to
-- scheduled_at/id — exactly the tie shape the in-memory twin's per-round
-- tick produces). The write is bounded by
-- construction (at most limit_n distinct actors, each a primary-key
-- probe) and CANNOT block: the driver's FOR UPDATE SKIP LOCKED takes
-- only registry rows no peer holds, so the stamp never waits on a
-- concurrent dispatcher or an operator's move flip mid-transaction — the
-- price is a dropped stamp for actors a peer is stamping right now, a
-- bounded, self-correcting rotation degradation rather than a coupled
-- commit. No window function rides the locking arm (PG forbids the
-- combination); the ORDER BY gives every dispatcher's driver the same
-- visit order.
stamp_rows AS (
  SELECT ac2.actor
  FROM "{schema}".actor_config ac2
  WHERE ac2.actor = ANY(ARRAY(SELECT DISTINCT e.actor FROM eligible e))
  ORDER BY ac2.actor
  FOR UPDATE SKIP LOCKED
),
-- The stamp's UPDATE re-finds its rows through the same one-shot id
-- array doctrine as the terminal UPDATE below: ARRAY(SELECT ...) over
-- the bounded stamp_rows materializes once as an InitPlan and
-- ac.actor = ANY(<that array>) is served as a Bitmap Index Scan on the
-- actor_config primary key, so the write touches at most limit_n
-- registry rows. A FROM-clause join against stamp_rows would leave the
-- strategy to the planner, which honestly prefers a Seq Scan + hash of
-- the whole registry whenever actor_config is unanalyzed (the usual
-- production state) — registry-proportional work per round.
stamp AS (
  UPDATE "{schema}".actor_config ac
  SET last_claimed_at = statement_timestamp()
  WHERE ac.actor = ANY(ARRAY(SELECT s.actor FROM stamp_rows s))
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
    -- The increment saturates at the smallint column ceiling (32767 =
    -- constants.MAX_ATTEMPTS_SMALLINT_CEILING, written as a literal here
    -- the same way _sql_templates.py's retry_job raise arm does): a
    -- pre-existing row parked at the ceiling (retry_kind='indefinite'
    -- climbs there — nothing else bounds its counter) must not turn the
    -- whole round's claim into a smallint-out-of-range driver error that
    -- also aborts every healthy job selected beside it. The clamped row
    -- still runs; its terminal write then lands through the existing
    -- budget arms (a transient row at the ceiling is already past its
    -- max_attempts and terminalises on failure; an indefinite one runs
    -- until its deadline arm terminalises it) — never stranded pending.
    -- The repeat attempt number this makes possible (32767 claimed twice)
    -- is absorbed by the ON CONFLICT guard every job_attempts insert
    -- carries, so the audit trail keeps the first record of the number
    -- and no terminal path raises.
    attempt = LEAST(j.attempt + 1, 32767)
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
# jobs_unrouted_round_robin_probe_idx (the producer-placed twin of
# jobs_round_robin_probe_idx, partial on the marker so the walk never
# reads a re-pended row). Postgres 18 has no native skip scan
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
# The recursive term cannot be correlated and cannot reference another
# CTE, but it CAN read the round's bound parameters, and $1 is the
# queue list -- so the walk scopes itself to the round's own queues
# rather than enumerating the whole table's cohorts. That scoping is
# load-bearing, not an optimization: an unscoped walk takes one
# enumeration step per pending cohort anywhere in the fleet, so a
# round's cost grows with other teams' cohort counts and every queue's
# dispatch latency couples to fleet-wide backlog. The predicate also
# narrows to producer-placed rows (NOT assignment_routed), because this
# enumeration feeds only the label-routed arm; re-pended cohorts are
# walked by rr_tail_keys. The candidates lateral below still joins the
# result down to each probe's own (actor, queue) pair -- in-memory
# filtering over the materialized output, now bounded by the round's
# own cohort count.
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
      AND NOT j3.assignment_routed
      AND j3.queue = ANY($1::text[])
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
      AND NOT j4.assignment_routed
      AND j4.queue = ANY($1::text[])
      AND (j4.actor, j4.queue, COALESCE(j4.fairness_key, '__null__'))
          > (cur.actor, cur.queue, cur.fkey)
    ORDER BY j4.actor, j4.queue, COALESCE(j4.fairness_key, '__null__')
    LIMIT 1
  ) nxt
),
"""

# Strict-FIFO label-routed (queue, actor) enumeration: the same
# recursive loose index scan geometry as rr_keys, at (queue, actor)
# grain instead of (actor, queue, cohort) grain, riding
# jobs_queue_actor_dispatch_idx. The queue-leading key order is what
# makes the walk fleet-clean here: queue = ANY($1) is a ScalarArrayOp
# on the index's LEADING column, so Postgres drives one index range per
# round queue, and each step's (queue, actor) > (cur.queue, cur.actor)
# row-compare is an Index Cond within those ranges — one bounded seek
# per distinct (queue, actor) pair on the ROUND's queues, with zero
# entries visited for queues the round does not poll. (The
# actor-leading indexes cannot do this: with actor first, the queue
# predicate degrades to a per-entry filter and the walk reads every
# fleet actor's index entries between matches.)
#
# The index is partial on (status = 'pending' AND NOT
# assignment_routed) — exactly this walk's population (the label-routed
# set per_actor_capacity drives from), so a queue holding only
# re-pended rows costs the walk nothing; re-pended actors enter the
# round through repend_capacity instead.
_PA_KEYS_CTE = """\
pa_keys AS (
  (
    SELECT j3.queue, j3.actor
    FROM "{schema}".jobs j3
    WHERE j3.status = 'pending'
      AND NOT j3.assignment_routed
      AND j3.queue = ANY($1::text[])
    ORDER BY j3.queue, j3.actor
    LIMIT 1
  )
  UNION ALL
  SELECT nxt.queue, nxt.actor
  FROM pa_keys cur
  CROSS JOIN LATERAL (
    SELECT j4.queue, j4.actor
    FROM "{schema}".jobs j4
    WHERE j4.status = 'pending'
      AND NOT j4.assignment_routed
      AND j4.queue = ANY($1::text[])
      AND (j4.queue, j4.actor) > (cur.queue, cur.actor)
    ORDER BY j4.queue, j4.actor
    LIMIT 1
  ) nxt
),
"""

# Two-clock split (same doctrine as taskq.backend._sweeps): the
# row-selection bounds in the candidates laterals use statement_timestamp()
# (STABLE) so the planner can serve them as index-level conditions on
# jobs_unrouted_actor_dispatch_idx /
# jobs_unrouted_round_robin_probe_idx — a VOLATILE
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
    SELECT w.id, w.actor, w.identity_key, w.fairness_key,
           w.priority, w.scheduled_at
    FROM (
      SELECT p.id, p.actor, p.identity_key, p.fairness_key,
             p.priority, p.scheduled_at,
             ROW_NUMBER() OVER (
               ORDER BY p.priority DESC, p.scheduled_at, p.id
             ) AS probe_rank
      FROM (
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
          AND j2.scheduled_at <= statement_timestamp()
          AND (j2.schedule_to_close IS NULL OR j2.schedule_to_close > statement_timestamp())
        ORDER BY j2.priority DESC, j2.scheduled_at, j2.id
        -- The scan bound is a pure parameter expression, NEVER
        -- pac.residual * $5: a LIMIT the planner cannot fold to a
        -- constant is estimated as a fixed fraction of the scanned
        -- index range, which keeps the plan's ESTIMATED cost
        -- depth-proportional even though execution stops at the bound
        -- — and past jit_above_cost that estimate makes Postgres
        -- JIT-compile the plan on every dispatch round. $2 * $5 folds
        -- to its value in custom plans (the same doctrine as top_ids),
        -- so the estimate tracks the bound that actually limits
        -- execution. The window below CANNOT replace this LIMIT: a
        -- window function is logically evaluated before LIMIT, so a
        -- window on the un-bounded probe would read the whole index
        -- range per (actor, queue) — the depth-proportional read the
        -- round-robin variant's own history documents.
        LIMIT $2::int * $5::int
      ) p
    ) w
    -- The exact per-(actor, queue) admission window, cut AFTER the
    -- folded scan bound: probe_rank is the row's position in the same
    -- (priority DESC, scheduled_at, id) order the probe reads, so the
    -- top pac.residual * $5 by probe_rank is row-for-row the set the
    -- shipped single-LIMIT probe read whenever residual <= limit_n
    -- (for an uncapped actor residual IS limit_n, so the two bounds
    -- coincide exactly; for a capped actor deeper than limit_n the
    -- round's own limit binds first and the tail drains on later
    -- rounds — the depth contract's standing rule). Cutting here,
    -- BEFORE identity_dedup, matters: the shipped bound counted
    -- identity-duplicate rows against the window, so a post-dedup cut
    -- would silently widen it.
    ORDER BY w.probe_rank
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
      -- Probing each cohort with ORDER BY + LIMIT yields the SAME
      -- surviving rows with the SAME ranks (rank i within a cohort is
      -- the i-th row of that cohort's priority order), so selection is
      -- bit-identical to the shipped shape while the window's input is
      -- at most cohorts * limit_n * oversample rows for the pair.
      --
      -- The probes ride jobs_unrouted_round_robin_probe_idx
      -- (actor, queue, COALESCE(fairness_key, '__null__'),
      --  priority DESC, scheduled_at, id) WHERE status = 'pending'
      --  AND NOT assignment_routed:
      -- the three-column equality prefix is an Index Cond, the
      -- priority DESC order is the index's own within that prefix, and
      -- the STABLE due bounds are index-level conditions — so each
      -- probe is an ordered scan that stops at its LIMIT. The COALESCE
      -- equality is what folds the NULL cohort into one probe: a bare
      -- `fairness_key IS NULL` qual does not combine with the keyed
      -- cohorts' equality probe, and `IS NOT DISTINCT FROM` is never
      -- an Index Cond on this index (measured: a seq scan).
      --
      -- Two bounds per probe, in different units on purpose. The SCAN
      -- bound ($2 * $5) is a pure parameter expression: a LIMIT the
      -- planner cannot fold to a constant is estimated as a fixed
      -- fraction of the scanned index range, keeping the plan's
      -- ESTIMATED cost depth-proportional past jit_above_cost — which
      -- makes Postgres JIT-compile the plan on every dispatch round
      -- even though execution stops at the bound. The ADMISSION bound
      -- (cohort_rank <= residual * oversample, applied as the outer
      -- LIMIT after the cohort_rank window — windows are logically
      -- evaluated before LIMIT, so the cut cannot share the probe's
      -- own query level) is the shipped per-cohort window exactly:
      -- residual <= limit_n makes the two coincide, and a capped actor
      -- with residual above limit_n is bound by the round's own limit
      -- first, its tail draining on later rounds per the depth
      -- contract.
      SELECT c.id, c.actor, c.identity_key, c.fairness_key,
             c.priority, c.scheduled_at,
             ROW_NUMBER() OVER (
               PARTITION BY COALESCE(c.fairness_key, '__null__')
               ORDER BY c.priority DESC, c.scheduled_at, c.id
             ) AS fairness_rank
        FROM rr_keys k
        CROSS JOIN LATERAL (
          SELECT ck.id, ck.actor, ck.identity_key, ck.fairness_key,
                 ck.priority, ck.scheduled_at
          FROM (
            SELECT p.id, p.actor, p.identity_key, p.fairness_key,
                   p.priority, p.scheduled_at,
                   ROW_NUMBER() OVER (
                     ORDER BY p.priority DESC, p.scheduled_at, p.id
                   ) AS cohort_rank
            FROM (
              SELECT j2.id, j2.actor, j2.identity_key, j2.fairness_key,
                     j2.priority, j2.scheduled_at
              FROM "{schema}".jobs j2
              WHERE j2.actor = pac.actor
                AND j2.queue = sq.queue_name
                -- Producer-placed rows only: a re-pended row on this
                -- label is the assignment-routed arm's candidate (see
                -- the routing contract in the module docstring), never
                -- this arm's.
                AND NOT j2.assignment_routed
                AND j2.status = 'pending'
                AND COALESCE(j2.fairness_key, '__null__') = k.fkey
                AND j2.scheduled_at <= statement_timestamp()
                AND (j2.schedule_to_close IS NULL OR j2.schedule_to_close > statement_timestamp())
              ORDER BY j2.priority DESC, j2.scheduled_at, j2.id
              LIMIT $2::int * $5::int
            ) p
          ) ck
          ORDER BY ck.cohort_rank
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
# contract the label-routed arm keeps on
# jobs_unrouted_actor_dispatch_idx.
# Ranks are not computed here (the strict variant orders by priority
# downstream), but the admission stays per-cohort rather than one
# queue-agnostic probe because the only index over this population is
# cohort-keyed: a single ORDER BY priority probe over it would need a
# sort over every due re-pended row of the actor -- depth-proportional
# work the depth contract forbids.
#
# The two-bound split is the label-routed arm's doctrine: the SCAN bound
# ($2 * $5) is a foldable parameter expression so the plan's ESTIMATED
# cost stops tracking the backlog depth (an unfoldable LIMIT is
# estimated as a fixed fraction of the scanned range, which is what
# pushed this statement past jit_above_cost at depth), and the outer
# cohort_rank cut re-imposes the exact per-cohort admission window
# (residual * oversample) over the bounded probe output — the window
# cannot share the probe's own level because a window is logically
# evaluated before LIMIT.
_REPENDED_STRICT_FIFO_LATERAL = """\
    SELECT p.id, p.actor, p.identity_key, p.fairness_key,
           NULL::bigint AS fairness_rank,
           p.priority, p.scheduled_at, rc.residual, rc.actor_claimed_at,
           rc.max_concurrent
    FROM repend_capacity rc
    CROSS JOIN rr_tail_keys tk
    CROSS JOIN LATERAL (
      SELECT ck.id, ck.actor, ck.identity_key, ck.fairness_key,
             ck.priority, ck.scheduled_at
      FROM (
        SELECT pr.id, pr.actor, pr.identity_key, pr.fairness_key,
               pr.priority, pr.scheduled_at,
               ROW_NUMBER() OVER (
                 ORDER BY pr.priority DESC, pr.scheduled_at, pr.id
               ) AS cohort_rank
        FROM (
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
          LIMIT $2::int * $5::int
        ) pr
      ) ck
      ORDER BY ck.cohort_rank
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
# admitted each round, so neither series can starve the other. The
# two-bound split (foldable $2 * $5 scan bound, then the exact
# residual * oversample cohort cut over the cohort_rank window) is the
# label-routed arm's doctrine — see its comment for why the scan bound
# must fold.
_REPENDED_ROUND_ROBIN_LATERAL = """\
    SELECT w.id, w.actor, w.identity_key, w.fairness_key,
           w.fairness_rank, w.priority, w.scheduled_at, rc.residual, rc.actor_claimed_at,
           rc.max_concurrent
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
        SELECT ck.id, ck.actor, ck.identity_key, ck.fairness_key,
               ck.priority, ck.scheduled_at
        FROM (
          SELECT pr.id, pr.actor, pr.identity_key, pr.fairness_key,
                 pr.priority, pr.scheduled_at,
                 ROW_NUMBER() OVER (
                   ORDER BY pr.priority DESC, pr.scheduled_at, pr.id
                 ) AS cohort_rank
          FROM (
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
            LIMIT $2::int * $5::int
          ) pr
        ) ck
        ORDER BY ck.cohort_rank
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
    keys_cte: str,
    keys_source: str,
    candidates_lateral: str,
    repended_lateral: str,
    ranked_order_by: str,
    eligible_candidates_order_by: str,
) -> str:
    """Substitute the per-variant fragments into the shared dispatch template.

    ``{schema}`` placeholders are preserved so the returned constant can be
    rendered with ``.format(schema=...)`` at the call site.  ``keys_cte`` is
    the variant's label-routed keys enumeration (``pa_keys`` for
    strict-FIFO, ``rr_keys`` for round-robin — the round-robin candidates
    lateral joins that enumeration by name) and ``keys_source`` the CTE name
    ``pa_actors`` reads its DISTINCT actor set from.  ``rr_tail_keys`` (the
    re-pended cohort enumeration) is shared verbatim by both variants in the
    template itself; only the two candidates arms differ per variant,
    through ``candidates_lateral`` (label-routed) and ``repended_lateral``
    (assignment-routed).
    """
    return (
        template.replace("__FAIRNESS_RANK_COLUMN__", fairness_rank_column)
        .replace("__KEYS_CTE__", keys_cte)
        .replace("__KEYS_SOURCE__", keys_source)
        .replace("__CANDIDATES_LATERAL__", candidates_lateral)
        .replace("__REPENDED_LATERAL__", repended_lateral)
        .replace("__RANKED_ORDER_BY__", ranked_order_by)
        .replace("__ELIGIBLE_CANDIDATES_ORDER_BY__", eligible_candidates_order_by)
    )


DISPATCH_STRICT_FIFO_SQL: str = _render_dispatch_sql(
    _DISPATCH_SQL_TEMPLATE,
    fairness_rank_column="NULL::bigint AS fairness_rank",
    keys_cte=_PA_KEYS_CTE,
    keys_source="pa_keys",
    candidates_lateral=_STRICT_FIFO_CANDIDATES_LATERAL,
    repended_lateral=_REPENDED_STRICT_FIFO_LATERAL,
    ranked_order_by="id.priority DESC, id.scheduled_at, id.id",
    eligible_candidates_order_by="l.priority DESC, l.scheduled_at",
)

DISPATCH_ROUND_ROBIN_SQL: str = _render_dispatch_sql(
    _DISPATCH_SQL_TEMPLATE,
    fairness_rank_column="j.fairness_rank",
    keys_cte=_RR_KEYS_CTE,
    keys_source="rr_keys",
    candidates_lateral=_ROUND_ROBIN_CANDIDATES_LATERAL,
    repended_lateral=_REPENDED_ROUND_ROBIN_LATERAL,
    ranked_order_by="id.fairness_rank, id.priority DESC, id.scheduled_at, id.id",
    eligible_candidates_order_by="l.fairness_rank, l.priority DESC, l.scheduled_at",
)


# Empty-round arbiter for the window-expansion loop in
# taskq.backend._dispatch: does ANY pending, dispatch-routable row
# remain on the round's queues? The claim statement's candidate window
# is deliberately bounded (residual * oversample per cohort probe) and
# its SKIP LOCKED slide ranges only within that materialized window, so
# a round whose whole window is row-locked by peers returns empty while
# deeper rows sit unlocked. Re-running the claim with a widened window
# is only worth its round trips when rows actually remain, and this
# probe is what tells the two empty-round causes apart: an idle queue
# (false — the round stays one statement) versus a locked-out window
# (true — expand and re-claim).
#
# The probe mirrors the candidacy ROUTING contract exactly (the same
# two populations the candidates CTE's two arms serve: never-claimed
# rows matched by their own queue label against the round's
# subscription, re-pended rows matched by their actor's current
# assignment) and it shares per_actor_capacity's "has pending" probe
# semantics: existence of pending rows, nothing more. It deliberately
# does NOT re-derive admission — no residual arithmetic, no identity
# anti-join, no due-window predicate. Admission is the claim
# statement's own job; a probe that re-implemented it would be a second
# copy of the candidacy logic, and its per-row subqueries would turn
# depth-proportional exactly on the saturated shapes it ran on. The
# price of the simpler question: when pending rows exist but are all
# currently inadmissible (cap-saturated actor, identity already running)
# the probe answers true and the loop burns its bounded expansions
# before returning empty — bounded wasted work on a transient state,
# never an unbounded scan.
#
# Depth contract: actor_config is read in one pass per EMPTY round
# (bounded by the registered-actor count, and only ever paid by a round
# that already admitted nothing — the claim statement proper no longer
# scans the registry at all; see per_actor_capacity) and every inner
# probe is a LIMIT-1 index read that stops at the first matching entry,
# so the probe's work is bounded by registered actors x round queues,
# never by backlog depth. The inner probes carry no ORDER BY: any
# single matching row answers the question, so the cheapest first match
# is the correct one.
DISPATCH_CLAIMABLE_PROBE_SQL: str = """\
SELECT 1
FROM "{schema}".actor_config ac
WHERE EXISTS (
    SELECT 1
    FROM unnest($1::text[]) AS pq(q)
    CROSS JOIN LATERAL (
        SELECT 1
        FROM "{schema}".jobs j
        WHERE j.actor = ac.actor
          AND j.queue = pq.q
          -- Producer-placed rows only, by the marker — never the
          -- started_at proxy: an operator-retried row that failed
          -- before its first claim is assignment_routed with
          -- started_at still NULL, and the proxy would enumerate its
          -- stale queue label as routable here.
          AND NOT j.assignment_routed
          AND j.status = 'pending'
        LIMIT 1
    ) hit
)
OR (
    ac.queue = ANY($1::text[])
    AND EXISTS (
        SELECT 1
        FROM "{schema}".jobs j
        WHERE j.actor = ac.actor
          -- The assignment-routed half: the marker, not
          -- started_at IS NOT NULL — same divergent-row shape as
          -- above, and this arm rides jobs_assignment_routed_probe_idx
          -- whose partial predicate is the marker itself.
          AND j.assignment_routed
          AND j.status = 'pending'
        LIMIT 1
    )
)
LIMIT 1
"""


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
        t0 = time.monotonic()
        try:
            rows = await conn.fetch(sql, queue_list, limit_n, worker_id, lock_lease, oversample)
        except Exception as exc:
            # A round that raised is still a round the producer spent: the
            # duration and the failure counter are recorded here because
            # every other dispatch signal is emitted only on the success
            # path, so a pod failing every round would otherwise be
            # indistinguishable, in the metric stream, from one polling an
            # empty queue.
            record_dispatch_duration(queue_attr, time.monotonic() - t0)
            record_dispatch_failure(queue_attr)
            # Why redacted: this text leaves the trust boundary for whatever
            # telemetry backend is configured. str() of an asyncpg
            # PostgresError appends the server's DETAIL line, which quotes the
            # offending row values -- idempotency_key / identity_key /
            # fairness_key are all caller-supplied.
            record_exception_text(span, render_exception(exc))
            raise
        elapsed = time.monotonic() - t0
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
