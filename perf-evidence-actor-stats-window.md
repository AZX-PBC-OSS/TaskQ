# Actors-page stats window — before/after measurement

Before/after evidence for the per-actor executor-stats rework (#230's
stale-failure bullet): the aggregate read over `jobs_archive` became a
read over `jobs_archive` UNION the live terminal population of `jobs`,
gained a `finished_at` window bound (`statement_timestamp() - $1`,
STABLE), and gained a `last_error_class` ordered aggregate. Measured per
the repo's performance-gate method: EXPLAIN (ANALYZE, BUFFERS) on real
Postgres, one unrecorded warm run then three recorded, median by
execution time.

## Method

- **OLD**: the pre-change aggregate — `jobs_archive` LEFT JOIN
  `job_attempts_archive`, the full column set (per-status FILTER counts,
  `avg`/`percentile_cont` over `a.duration_ms`, `max(finished_at)`).
- **NEW**: `_build_stats_sql` in `src/taskq/web/admin/_actor_stats.py` —
  the UNION-live shape, all-time (`window=None`) and windowed
  (`window=timedelta(hours=24)`).
- Engine: `postgres:18-alpine` testcontainer, the test suite's image.
- Corpus: 40,000 `jobs_archive` rows across 40 actors, finished evenly
  over the last 40 days (a 24h window covers ~1/40th ≈ 1,439 rows), plus
  8 live terminal `jobs` rows. `job_attempts(_archive)` empty (the LEFT
  JOIN shape, not its fan-out, is what the plan shows).

## Results

| variant | median exec ms | buffers |
|---|---|---|
| OLD all-time (archive-only) | 26.7 | 1213 |
| NEW all-time (UNION live) | 47.8 | 1214 |
| NEW windowed 24h | 6.7 | 1215 |

## What the plans say

- **The live side of the UNION is two buffers.** Terminal live rows are
  bounded by prune retention (they exist only between finish and prune),
  so the side is a small population by construction. In the windowed
  plan it is a `Bitmap Index Scan on jobs_finished_at_idx` with
  `Index Cond: (finished_at >= (statement_timestamp() - '1 day'::interval))`
  — the STABLE clock claim in `_actor_stats.py`'s docstring, confirmed:
  a VOLATILE `clock_timestamp()` bound could not be an index condition.
- **The archive side's bound is index-eligible; the planner may still
  choose a seq scan.** At ~3.6% selectivity over 40k rows the planner
  preferred a seq scan with `Filter: (finished_at >= …)` /
  `Rows Removed by Filter: 38561` (1223 buffers) over ~1,400 index probes
  plus heap fetches — the planner doing its job, not the bound failing.
  A tighter window or larger table tips it to the index.
- **All-time costs ~1.8x the old aggregate** (47.8 vs 26.7 ms): the
  `last_error_class` ordered aggregate
  (`array_agg(… ORDER BY finished_at DESC NULLS LAST)`) forces a
  sort-based grouping pass over the whole 40k-row input where the old
  shape planned a HashAggregate. Same cost class — an aggregate dominated
  by the archive population, served once per page load through the
  bounded admin pool — and the recency view, the path the feature exists
  for, is ~4x cheaper than the old all-time read.
- **The windowed variant slices the population it aggregates** (6.7 ms,
  1,447 of 40,008 rows): the bound shrinks both the scan and the
  grouped-sort work, which is why the recency view is the fast path.

## Why the UNION is the right shape anyway

The History LIST page already reads `jobs_archive` UNION `jobs`; the
aggregate read catching up to it means an actor's freshest failures
count before the prune sweep archives them. The live side cannot inflate
executor totals (terminal statuses only, the same closed set the prune
sweep archives) and cannot grow unboundedly (retention-bounded by
construction).
