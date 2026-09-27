# Operational Insights (SQL)

`taskq.insights` is a pure-read SQL layer over the job ledger. It answers
the operator questions the live telemetry cannot: how long work **waited**,
whether the fleet is **imbalanced**, whether it is **overprovisioned**, how
long a queue needs to **drain**, and whether a cron schedule is **fanning
out** faster than it clears.

It is a library surface, not an endpoint: every `fetch_*` function takes a
connection you own (an `asyncpg` connection or pool proxy) plus your schema
name, and returns plain dicts. Nothing writes, nothing blocks a worker, and
the module imports without `asyncpg` or FastAPI present — scripts,
notebooks and deploy steps can use it.

```python
import asyncpg
from datetime import timedelta
from taskq.insights import fetch_queue_imbalance

conn = await asyncpg.connect("postgresql://…")
rows = await fetch_queue_imbalance(conn, schema="taskq", worker_liveness_seconds=30)
```

## What every consumer must know first

These five facts define what the numbers mean. They are restated per
metric below.

1. **Wait per attempt = `started_at − scheduled_at`.** The dispatch claim
   stamps `started_at` with the database's clock and never touches
   `scheduled_at`; `scheduled_at` is when the attempt *became due*. That
   difference is the queue wait for a first-delivery attempt.
2. **Deferrals move `scheduled_at` forward** and leave only the coalesced
   `snooze_count` / `rate_limit_blocked_count` counters. For deferred rows
   the measured wait is only the final leg — so the wait distribution
   reports them as a separate `deferred` segment. `started_at − created_at`
   is *not* wait for them: it would fold your own snooze choice into the
   queue's latency.
3. **Retries leave no per-attempt due stamp.** The retry arm re-stamps
   `scheduled_at`; the earlier legs' waits are unrecoverable from the jobs
   row. A retried job with zero deferral counters contributes its final
   attempt's wait to the `clean` segment.
4. **Terminal history lives in `jobs` then `jobs_archive`.** A terminal
   row stays in `jobs` for the prune retention (default 30d), then moves
   to the archive (default 1y). Every wait/throughput/ledger query here is
   a two-sided UNION over both tiers — no blind spot at the prune
   boundary. The analytics window floor is the **archive retention**: a
   window older than the archive keeps answers over the surviving
   population only (on hypertables the retention policy drops whole
   chunks; the queries never re-derive that horizon).
5. **Cron jobs enqueue pending-at-fire** (no pre-arm), except
   future-armed fires (the DST `allof` second occurrence and any
   forward-stamped enqueue), which land as `status = 'scheduled'` with a
   future `scheduled_at`. The DST `allof` double-enqueue is **by-design
   fan-out**, not a defect. A **budget-deferred** fire enqueues *no row at
   all* — it is invisible to the ledger; its record is the
   `taskq.cron.budget_deferrals` counter and the `cron-fire-budget-deferred`
   log event.

**Redis adds nothing here.** This is a PG-only module: the live-only
signals Redis carries are ephemeral, with nothing durable to aggregate.
Identical SQL runs on vanilla PostgreSQL and on TimescaleDB hypertable
deployments (verified on both; on hypertables the window bounds are the
chunk-pruning keys).

**Index discipline.** Every window bound is `statement_timestamp()`
(STABLE), so it is index-eligible: `jobs_finished_at_idx` (terminal
partial) and its archive twin anchor the finished-at arms, the dispatch
and scheduled-wake partial indexes anchor the depth arms,
`job_attempts`' `started_at` index and the hypertable chunking anchor the
busy arms, and the metadata GIN anchors the per-schedule cron seeks. The
EXPLAIN pins in `tests/test_insights.py` hold against a seeded ~6k-row
corpus on both modes. Two documented exceptions where no serving index
exists and the population is retention-bounded: the cron ledger's archive
arm (the archive has a tags GIN, not a metadata GIN — its implied
`finished_at` bound is the anchor) and the busy-ratio statement's
`job_attempts_archive` arm on vanilla PG (no `started_at` index there;
on hypertables chunk pruning bounds it).

## 1. Wait distributions — `fetch_wait_distribution`

```python
rows = await fetch_wait_distribution(conn, schema="taskq", window=timedelta(hours=1))
rows = await fetch_wait_distribution(
    conn, schema="taskq", window=timedelta(hours=6), per_actor=True
)
```

One row per (queue, segment) — or per (actor, queue, segment) with
`per_actor=True` — over terminal rows finished inside the window, live +
archive. Columns: `count`, `p50_wait_s`, `p95_wait_s`, `max_wait_s`
(seconds, double precision).

Segments:

- **`clean`** — `snooze_count = 0 AND rate_limit_blocked_count = 0`:
  first-delivery attempts. **The queue-latency SLO is written against
  this segment.**
- **`deferred`** — rows with a deferral counter: the measured wait
  excludes the deferred time by construction. Read under its own SLO, or
  exclude from the clean percentile entirely.

From a seeded verification run (1h window, `emails` queue):

```
clean     count=10  p50_wait_s=2.5   p95_wait_s=3.0   max_wait_s=3.0
deferred  count=1   p50_wait_s=45.0  p95_wait_s=45.0  max_wait_s=45.0
```

The clean p50 of 2.5 s is the deliverable SLO number; the single deferred
row's 45 s is the operator's own snooze, not the queue's latency. If the
`deferred` segment grows faster than `clean`, look at rate-limit budgets
and snooze usage — not at worker count.

**Interpretation.** `max_wait_s` on `clean` is your outlier alarm; p95 is
the SLO line. A clean p95 that climbs while terminalisation throughput
stays flat means dispatch is falling behind (check utilization below), not
that jobs got slower.

## 2. Imbalance ratios — `fetch_queue_imbalance`, `fetch_actor_backlog`

```python
rows = await fetch_queue_imbalance(conn, schema="taskq")
rows = await fetch_actor_backlog(conn, schema="taskq")
```

Per queue: `depth` (pending **due now** — the dispatch index's own
population), `scheduled_depth` with the armed wave's
`wave_min_scheduled_at` / `wave_max_scheduled_at`, `live_workers`
(workers subscribing the queue whose `last_seen_at` is inside
`worker_liveness_seconds`, default 30 s — the admin UI's liveness window),
`actor_capacity` (sum of the routed actors' `max_concurrent`),
`effective_capacity` (actor capacity × live workers), `utilization`
(depth ÷ effective capacity), `oldest_due_age_s`.

From the same seeded run:

```
emails    depth=12  live_workers=3  effective_capacity=12  utilization=1.0   oldest_due_age_s≈5402
reports   depth=50  live_workers=0  effective_capacity=0   utilization=None  oldest_due_age_s≈600
widgets   depth=0   live_workers=2  effective_capacity=0   utilization=None
```

- `utilization > 1` — **starved**: more due work than one wave of
  capacity can claim. The `emails` row at exactly 1.0 is the balanced
  edge; above it, add workers or raise `max_concurrent`.
- `utilization IS NULL` with `depth > 0` — **the starvation shape**: due
  work and nothing can serve it (`reports`: 50 due, zero live workers).
  This is the "queue has pending jobs but no alive worker" banner's
  arithmetic.
- `oldest_due_age_s` is the fairness alarm: large depth + small age is a
  burst; small depth + large age is a strand. (`emails`' 5402 s oldest
  age in the seeded run is exactly that strand read correctly: six of
  its twelve due rows are un-cleared cron fires that have sat for ~90
  minutes — the imbalance row and the ledger row agree about it, which
  is the point of reading them together.)

Per actor × queue (`fetch_actor_backlog`): `backlog` (pending +
scheduled), `running`, `max_concurrent`, `saturation` (running ÷ cap) and
`unservable_backlog` (backlog beyond the actor's free capacity — rows
still waiting after the next full claim wave). The `queue` column is the
actor's **routed** queue (`actor_config.queue`), the same discriminator
the stranded-jobs detector reads.

## 3. Overprovisioning — `fetch_overprovisioning`, `fetch_worker_busy_ratio`

```python
rows = await fetch_overprovisioning(conn, schema="taskq", window=timedelta(hours=24))
rows = await fetch_worker_busy_ratio(conn, schema="taskq", window=timedelta(hours=24))
one = await fetch_worker_busy_ratio(conn, schema="taskq", window=timedelta(hours=24), worker_id=w)
```

Per queue: `overprovisioned` is TRUE when the queue holds **live workers,
zero due depth, and fewer terminalisations across the whole window than
workers** — fewer than one completion per worker in the entire window.
The raw inputs (`live_workers`, `depth`, `terminalisations`) ride beside
the verdict so a dashboard can trend them: a single-window TRUE is a
hypothesis; three consecutive windows is a fleet to shrink. From the
seeded run (1h window): `widgets` reads `live_workers=2, depth=0,
terminalisations=0 → True`; `emails` (`3` workers but `11`
terminalisations) reads False — the verdict keys on work done, not queue
emptiness.

Per worker: `busy_ms` (summed attempt `duration_ms` over the window,
live attempts + the `job_attempts_archive` twin), `observed_ms` (the
worker's tenure capped at the window) and `busy_ratio`. From the seeded
run (1h window, a worker up 30 min with three 1000 ms attempts):

```
busy_ms=3000  observed_ms≈1800045  busy_ratio≈0.00167
```

Near-zero `busy_ratio` across a sustained window on a worker still
heartbeating is the idle-worker shape; near one is saturation. The
drill-down (`worker_id=…`) is the per-worker idle query: both arms are
bounded by `started_at` (live btree index; hypertable chunk pruning).

## 4. Drain estimation — `fetch_drain_estimates`

```python
rows = await fetch_drain_estimates(conn, schema="taskq", window=timedelta(hours=1))
```

Per queue: `eta_seconds` = depth ÷ (terminalisations over the window,
normalized to completions per second), with `has_traffic` and the armed
wave's span. From the seeded run:

```
emails    depth=12  terminalisations=11  has_traffic=True   eta_seconds≈3927
reports   depth=50  terminalisations=0   has_traffic=False  eta_seconds=None
```

The **confidence caveat, pinned by test**: when the window carried no
traffic, `eta_seconds` is NULL and `has_traffic` is false — never zero,
which would read as "already drained". Widen the window (up to the
archive retention floor) before trusting anything else. `eta_seconds` is
a throughput extrapolation: it assumes the next window looks like the
last one, the workers stay up, and nothing enqueues behind the current
depth. The `scheduled_depth` / wave bounds are the incoming work the
estimate does **not** include — a queue can drain into an immediately
re-arming wave, so read the two together.

## 5. Cron fan-out ledger — `fetch_cron_ledger`

```python
rows = await fetch_cron_ledger(conn, schema="taskq", window=timedelta(hours=1))
```

One row per schedule (provenance: the `cron_schedule_id` metadata stamp,
sought through the jobs metadata GIN, live + archive UNION):

- `fires_window` / `cleared_window` — fires enqueued in the window and
  how many are terminalised. Clearance lags fires by construction at the
  window's right edge; the ratio is a trend, not an instant.
- `fires_prior` / `cleared_prior` — the prior equal window: the trend's
  second sample.
- `outstanding` — the schedule's non-terminal population right now,
  windowless: the backlog the fleet still owes, future-armed fires
  included (by design).
- `runaway_trending` — TRUE when fires > cleared in **both** the current
  and the prior window. One window is a burst; two consecutive is the
  runaway shape.

From the seeded run (1h window, a schedule that fired 6 times, cleared 1,
after a prior window that fired 1 and cleared 0):

```
ticker   fires_window=6  cleared_window=1  fires_prior=1  cleared_prior=0
         outstanding=6   runaway_trending=True
```

Read the verdict against the schedule's `dst_strategy`: an `allof`
schedule legitimately doubles a fire in the DST-overlap hour — that is
designed fan-out, not a runaway. And a budget-deferred fire enqueues no
row: a deferral reads as zero fires here, not as a growing backlog (the
`taskq.cron.budget_deferrals` counter is its record).

## Consuming the metrics: the scaling-operator contract

The `fetch_*` functions above are the **in-database** path. The second
consumption path is the **scrape**: the OTel metric surface the worker
exports (`taskq.obs`; Prometheus-rendered names turn dots into
underscores and suffix counters with `_total`). A future dynamic
worker-scaling operator consumes *that* surface at fleet scale, so this
section is the contract it may build on: the exact series, their labels,
their bounds and their decision semantics. **Diagnostics now; automation
later** — TaskQ ships the measurements, a human makes the scaling call
(see the decision table's framing note).

Sampling cadence: the leader-sampled gauges refresh every
`TASKQ_QUEUE_DEPTH_INTERVAL` (default 15 s) and go STALE on demotion (a
demoted process exports nothing rather than a frozen reading); the
per-worker gauges are sampled unconditionally by every worker.

### The series an operator scrapes

| Prometheus series | Kind | Labels | The operator question it answers (SQL twin) |
|---|---|---|---|
| `taskq_queue_depth` | gauge | `queue` | How much work EXISTS per queue — pending **and** scheduled (the gauge's population is wider than the SQL `depth`, which is due-now only; see `fetch_queue_imbalance`) |
| `taskq_queue_live_workers` | gauge | `queue` | Is anyone consuming it (`live_workers`) |
| `taskq_queue_utilization` | gauge | `queue` | Starvation at a glance (`utilization`) — **first-class, do not recompute client-side** |
| `taskq_jobs_by_status{status}` | gauge | `status` | The live population exactly, per status (no SQL twin needed) |
| `taskq_jobs_scheduled_count` | gauge | — | Label-less twin of `by_status{status="scheduled"}` for direct comparison with the age gauges |
| `taskq_jobs_oldest_due_age_seconds` | gauge | — | Is promotion stalled (`oldest_due_age_s`'s fleet-wide form) |
| `taskq_jobs_oldest_pending_age_seconds` | gauge | `actor`, `queue` | Which (actor, queue) has a stranded head of line |
| `taskq_jobs_actor_backlog` | gauge | `actor`, `queue` | Per-actor pending depth (`fetch_actor_backlog.backlog`) |
| `taskq_jobs_running` | gauge | `actor` | Who holds the fleet's slots (`fetch_actor_backlog.running`) |
| `taskq_jobs_oldest_running_age_seconds` | gauge | `actor` | An attempt with no `start_to_close` |
| `taskq_jobs_stranded{reason}` | gauge | `actor`, `reason` | Rows that can never dispatch, and why (`unservable_backlog`'s diagnosis) |
| `taskq_jobs_running_lease_expired` | gauge | — | Zombie-running (claimed, lease past, still `running`) |
| `taskq_worker_active_jobs`, `taskq_worker_max_concurrency` | gauges | — | Per-process in-flight and its ceiling; the ratio is per-process utilization (the pod is the scrape target's own labels) |
| `taskq_jobs_queue_wait_seconds` | histogram | `actor`, `queue` | What every dispatched job actually waited — the live, per-job form of `fetch_wait_distribution`'s windowed percentiles |
| `messaging_process_duration` | histogram | `actor`, `queue`, `outcome` | Execution duration, outcome-segmented |
| `taskq_jobs_attempt_failures_total` | counter | `actor`, `error_type`, `retryable` | Failure rate, retried vs terminal |
| `taskq_jobs_timeouts_total` | counter | `actor`, `kind` | Budget violations |
| `taskq_cron_consecutive_failures` | up-down counter | `actor` | Failing schedules (reconciled against the database each tick) |
| `taskq_cron_budget_deferrals_total` | counter | `actor` | Budget-deferred fires — invisible to the SQL ledger (§5) |
| `taskq_maintenance_leader_sweep_last_success_seconds` | gauge | `sweep_name` | Is the leader's own sampling alive (staleness = `time() − value`) |

### Not on the scrape — read it in SQL

The honest absences an operator must not paper over:

- **Cron lag is not merged** (#563): there is NO per-schedule
  schedule-vs-fire lag gauge. The stand-ins are the two cron counters
  above and `fetch_cron_ledger`'s `outstanding` / `runaway_trending`
  (§5). Until a lag series exists, cron fan-out is a *feed-the-schedule*
  decision, not a worker-count one.
- **Drain ETA** (`eta_seconds`, §4) is a throughput extrapolation over
  windowed terminalisations — SQL-side only. The scrape provides its
  inputs (`taskq_queue_depth`, `rate(messaging_client_consumed_messages_total)`);
  anything computed client-side must re-derive `has_traffic` honestly:
  no traffic in the window means the estimate is UNDEFINED, never zero.
- **Per-queue actor capacity** is folded into
  `taskq_queue_utilization` by the sampler (the capacity table is the
  leader's to read). The raw inputs are `fetch_queue_imbalance`'s
  `actor_capacity` / `effective_capacity` columns.
- **Zero capacity is a missing series, not a zero.**
  `taskq_queue_utilization` OMITS a queue whose effective capacity is
  zero rather than reporting `0` — a frozen zero reads "idle", the
  opposite of the truth. `taskq_queue_depth > 0` with NO utilization
  series (plus `taskq_jobs_stranded{reason="unserved_queue"}`) is the
  starved-from-zero shape.

### The decision table

**Human-in-the-loop framing.** TaskQ ships diagnostics, not automation.
Every rule below is a decision AID: the thresholds are starting points,
written against the columns that define them, and a human validates each
against their own SLOs before letting anything act on them. The eventual
dynamic operator (a future wave, not this one) is expected to log and
propose first and act only within operator-set bounds — the signal
vocabulary below is what such a proposal would cite.

| Signal | Threshold (source columns) | Action |
|---|---|---|
| Starved queue | `taskq_queue_utilization > 1` sustained (`fetch_queue_imbalance.utilization`, from `depth` ÷ `effective_capacity`) | **Scale up workers** subscribed to the queue (or raise the routed actors' `max_concurrent`) |
| Starved from zero | `taskq_queue_depth > 0`, NO `taskq_queue_utilization` series, `taskq_queue_live_workers == 0` (`utilization IS NULL` with `depth > 0`) | **Scale up from zero / subscribe a worker** — no tuning serves an unserved queue |
| Fairness strand | `taskq_jobs_oldest_due_age_seconds` grows while utilization ≤ 1 (`oldest_due_age_s` vs `depth`: large depth + tiny age is a burst; small depth + large age is a strand) | **Investigate dispatch/priority**, not fleet size |
| Overprovisioned | `overprovisioned == true` over three consecutive windows (`fetch_overprovisioning`: `live_workers`, `depth`, `terminalisations` — live workers, zero due depth, fewer terminalisations than workers) | **Consolidate: scale down.** One window is a hypothesis; three is a fleet to shrink |
| Idle workers | `busy_ratio ≈ 0` sustained on still-heartbeating workers (`fetch_worker_busy_ratio.busy_ratio`) | **Consolidate the pool** |
| Saturated actor, growing backlog | `saturation` at 1 while `unservable_backlog > 0` sustained (`fetch_actor_backlog.saturation`, `.unservable_backlog`) | **Move the actor to a queue with headroom** (re-route in `actor_config`) — worker count cannot lift a per-actor cap |
| Runaway cron schedule | `runaway_trending == true` (`fetch_cron_ledger`: fires > cleared in BOTH the current and prior window) | **Do not scale for this alone** — feed the schedule; read against `dst_strategy` (an `allof` overlap-hour double-fire is by design) |
| Drain budget | `eta_seconds` against the intended change window (`fetch_drain_estimates.eta_seconds`; `has_traffic == false` ⇒ undefined — widen the window first) | **Size the scaling action** to finish inside the window, then re-read |

### The label-cardinality bounds are part of the contract

The operator scrapes at fleet scale, so the label vocabulary is a
guarantee, not an implementation choice:

- **`queue`** is the one open-ended label (caller-supplied per enqueue,
  charset-validated only). It is capped at
  `_MAX_QUEUE_LABEL_VALUES = 100` values **per process**. The job-side
  emitters admit the first 100 distinct names and collapse later ones
  onto the fixed `_other_` value. The leader-sampled per-queue gauges
  (`taskq_queue_depth`, `taskq_queue_live_workers`,
  `taskq_queue_utilization`) partition by VALUE instead: the 100 largest
  queues (ties broken by name) keep their own series and the rest
  collapse onto ONE `_other_` series carrying their summed value — the
  reported total always equals the true total. Either way the series
  count is hard-bounded at **cap + 1 per gauge, per process, forever**,
  however many queue names a deployment mints.
- **`actor`** is bounded by the registered actor set the user ships on
  the job-side instruments; the cron paths' raw schedule-row actor
  string is capped the same way (`_MAX_ACTOR_LABEL_VALUES = 100`,
  overflow `_other_`), because schedule rows accept any string at
  creation time.
- **`bucket`** (rate-limit refund failures) is capped identically
  (`_MAX_BUCKET_LABEL_VALUES = 100`): a keyed bucket's name embeds
  caller-controlled payload.
- **Identity values are never dimensions**: `worker_id`, `job_id`,
  `schedule_id` ride spans and log lines (where cardinality is free),
  never metric labels — the fleet-scale scrape stays bounded through
  deploys, restarts and autoscale events.
- Fleet-wide `sum by (queue)` note: processes admit different queue
  sets, so their `_other_` series are not the same queue; the summed
  overflow keeps the TOTAL honest, but per-queue attribution past the
  cap lives on spans and log lines.

These bounds are **pinned**: `tests/test_obs_scaling_operator_contract.py`
asserts the cap constants, the capped-dimension overflow behaviour, and
the per-queue leader gauges' ≤ cap + 1 series partition;
`tests/test_obs_metric_cardinality.py` asserts identity values never mint
series. A future label addition that breaks the cardinality bounds reds
there before it reaches a scraper.

### The SQL contract is versioned

`taskq.insights.INSIGHTS_CONTRACT_VERSION` (currently **1**) versions the
in-database surface the `fetch_*` functions expose: the function set,
each function's keyword-only parameter names and defaults, and every
statement's return-row column names and Postgres types.
`tests/test_insights_contract.py` pins all three against a real migrated
schema, so a refactor that breaks operator queries reds there. An
additive change (a new function; a new column appended at the END of a
row) ships without a breaking bump; a column removed, renamed, retyped or
reordered is breaking and bumps the version. The both-modes guarantee
from [the top of this guide](#what-every-consumer-must-know-first) —
identical SQL on vanilla Postgres and hypertables — is part of the same
contract.

## Window selector and the retention floor

The named windows are `1h`, `6h`, `24h`, `7d`
(`taskq.insights.INSIGHTS_WINDOWS`). On hypertables the archive's
retention policy — not your window — is the analytics floor: a 7d window
against a 2d archive retention answers over 2d of history. Check
`taskq.backend._retention_floor.retention_policy_floor` (or the
TimescaleDB guide's monitoring section) when a long window reads shorter
than it claims.

## Testing

`tests/test_insights.py` runs the same scenario bodies — a balanced
fleet, a starved queue, an overprovisioned queue, a runaway schedule, a
deferred job's excluded wait, an archived job's UNION inclusion — against
a plain PostgreSQL 18 container **and** a TimescaleDB container with the
retention tables converted, and pins every statement's EXPLAIN plan on
both modes (no Seq Scan on the hot paths). Every number quoted above is
from those seeded runs.
