# The MVCC-horizon death spiral, reproduced — and the claim cursor's measured scope

The evidence base: brandur.org/postgres-queues ("Postgres Job Queues +
Failure By MVCC") and PlanetScale's 2026 re-run ("Keeping a Postgres
queue healthy"). The failure: a long or overlapping transaction pins the
MVCC horizon, VACUUM cannot reclaim, and the claim query's B-tree scans
degenerate through the dead tuples the queue's own churn leaves behind —
brandur measured **15x lock-time degradation**; PlanetScale confirmed
SKIP LOCKED only "lifts the floor, not the ceiling" (identical
dead-tuple scans under both). The literature's two cures: the PREDICTOR
(the degradation is visible in the claim-latency distribution before the
backlog explodes) and brandur's fix (a fresh `job_id > cursor` predicate
so the seek lands on live tuples).

This PR ships the predictor (the `taskq.claim.*` gauges, default-on) and
the cursor (opt-in, default off — see the posture decision below).

## Method (public — replicate it)

`tests/perf/test_mvcc_claim_degradation.py`, markers `slow integration
load_sensitive`, runs on demand:

```
uv run pytest tests/perf -m "slow and load_sensitive" -v --capture=no -k mvcc
```

1. **The pin.** A `REPEATABLE READ` transaction writes a row and stays
   open for the whole experiment — brandur's overlapping-transaction
   pattern, the production shape of a long migration, a stuck operator
   session, or an app pool's idle-in-transaction connection. Everything
   that dies after this snapshot is unreclaimable AND un-hintable (the
   executor may not even LP_DEAD the index items, because the pin can
   still see the old versions).
2. **The churn.** Dead versions are grown in bulk to four levels (0 /
   30k / 60k / 90k), rows that were pending in the claim queries' own
   partial indexes dying the way claims kill them (pending → terminal),
   with client-minted UUIDv7 ids in the same clock domain the claim
   cursor lives in.
3. **The statistics control.** `ANALYZE` runs under the pin (legal: it
   samples, it does not reclaim) AFTER each level's live supply lands.
   This matters and is part of the finding: dead rows are invisible to
   every fresh snapshot, so the planner's estimates stay healthy while
   the index rots — with stale estimates the planner serves the probes
   from the id-less order-only indexes (scan-the-pair + sort) and no
   predicate can position those scans.
4. **The measurement.** Both arms run through the REAL shipped SQL on
   one held connection (plan warm; the ratio compares execution):
   - the full strict-FIFO claim statement, render picked by the bound
     exactly as the production wiring picks it, cursor store advancing
     per round;
   - the label-routed candidates probe — the fragment the shipped
     render itself carries, mechanically rebound to standalone
     parameters — with and without the scalar id bound pinned at the
     dead zone's top edge (the production invariant: the claims make
     the dead zone in id order, so the cursor sits at its top).
   Arms are interleaved with alternating order; gates are RELATIVE,
   same process, same table — no absolute budgets.

## Results (the runs that set this record)

Full claim statement, p50 by dead-tuple level (PostgreSQL 18.6, loaded
shared development host — treat as a class; run-to-run p50s swing with
co-tenant load, the FACTORS are the stable quantity):

| dead | naive p50 | cursor p50 |
|---|---|---|
| 0 | 11.8 ms | 11.1 ms |
| 30k | 11.7 ms | 16.5 ms |
| 60k | 100.5 ms | 101.5 ms |
| 90k | 110.2 ms | 110.5 ms |

Degradation factors across five controlled runs: naive 2.8x – 13.2x
(the spiral reproduces, robustly — the load-bearing gate), cursor
2.96x – 12.77x — **the cursor arm's full-path factor tracked the naive
arm's in every run** (2.96 vs 3.53, 2.97 vs 2.80, 3.69 vs 3.85, 9.92 vs
9.32). The bound's win is real on the isolated surface (below) but NOT
robust end-to-end.

The isolated candidates probe (the surface the bound provably fixes):
on the id-carrying `jobs_unrouted_actor_dispatch_idx`, the bound joins
the **Index Cond** (PG18's Index Searches machinery re-descends past
the dead zone): probe-measured **615 → 182 buffers (2.5–3.4x latency)
at 20k dead**, repeatable, both orders — when the bound sits at the
dead zone's top edge and the planner has honest estimates.

## The finding this PR contributes back

brandur's fix transfers to a claim query that scans BY ID (his: `ORDER
BY id LIMIT n FOR UPDATE SKIP LOCKED`) — there the cursor's bound IS
the scan's start position and the fix is close to total. TaskQ's claim
is a multi-surface CTE, and the measured full-path scope of the cursor
is much narrower:

1. **The pair-recursion walks** (the `pa_keys`/`rr_keys` loose scans
   that enumerate (actor, queue) pairs) walk dead entries in
   (queue, actor) order — an id predicate is a filter there, never a
   seek; measured ~6–8 ms of every round at 90k dead, in both arms.
2. **The `has_pending` admission probes** are served, under the
   spiral's planner-blindness, from the id-less `jobs_dispatch_idx`
   (an order-preserving scan + actor filter walking the whole queue
   head): measured ~1.5 ms × the actor count per round, in both arms.
   The cursor bound ships there too, and positions the scan when the
   planner instead picks the id-carrying probe index — but the planner
   cannot know to prefer it: **dead rows are invisible to every fresh
   snapshot, so `ANALYZE`'s estimates stay healthy while the index
   rots.** The spiral blinds the planner to itself.
3. The candidates probe where the bound positions correctly is a
   minority share of the statement's dead-coupled cost at depth.

So: the predictor (the ratio gauges) sees the whole truth and is the
deliverable that works everywhere; the runbook's primary cure is
operational (find the pinning transaction, let VACUUM drain); the
cursor is a bounded, opt-in mitigation whose honest scope is the
candidate surface.

## The stranding bound (the cursor's cost, pinned)

The bound is a selection predicate: a pending row whose id falls below
the worker's cursor (a skewed producer's clock, a row the round's
window never reached) is invisible until the jitter reset forgets the
cursor. Pinned end-to-end:
`tests/test_dispatch_claim_cursor_pg.py::test_a_lower_id_job_is_claimed_within_one_reset_window`
claims a mid-flight lower-id row inside one reset window; the mutation
check (breaking the jitter so the bound never expires) reds exactly
that pin and nothing else. The reset cadence is the knob's mechanism
default (60s, ±50% jitter, armed at the entry's creating claim, never
extended by later claims — a sliding window would let a hot queue's
cursor live forever).
