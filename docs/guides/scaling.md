# Scaling

TaskQ scales along three independent axes, and the knob for each lives in a different place:

- **Vertical** - one process, more concurrency: sized by the connection arithmetic in [Derived Values](configuration.md#derived-values) and [ops.md §4](ops.md#4-sizing-workers-and-postgres-connections).
- **Horizontal** - more worker processes against one Postgres: managed by the workgroup supervisor ([workgroups.md](workgroups.md)) and bounded by the connection budget.
- **Per-queue / per-actor** - caps and dispatch modes that shape throughput without adding processes.

This page is the decision map between them. It does not repeat the full knob tables; each section links the guide that owns the numbers.

---

## 1. Vertical sizing: the connection math

Every worker process holds up to four pools plus dedicated connections ([ops.md §4](ops.md#4-sizing-workers-and-postgres-connections)):

| Resource | Count (default) | DSN | Scales with |
|---|---|---|---|
| `dispatcher_pool` | 4 | **direct** | fixed default (`TASKQ_DISPATCHER_POOL_SIZE`) |
| `heartbeat_pool` | 4 | **direct** | fixed default (`TASKQ_HEARTBEAT_POOL_SIZE`) |
| `worker_pool` | `int(max_concurrency × 1.5)` | **pooled** (may traverse PgBouncer) | `TASKQ_MAX_CONCURRENCY` |
| slot pool (conditional) | `max_concurrency + 1` | **direct** | only when a LOOP-scope `asyncpg.Connection` is registered and `max_concurrency > 1` (workers.md owns the decision; the `loop_scope_conn_dsn_mismatch` startup warning tells you which path your fleet is on); fully warmed at boot |
| `notify_conn` | 1 | **direct** | fixed (LISTEN is session-scoped, cannot pool) |
| `leader_conn` | 1 | **direct** | fixed (advisory-lock election, every worker) |
| leader-only monitor + cron | +2 | **direct** | only on the elected leader |

Two non-worker counts finish the budget: each per-`TaskQ` client (a web/API pod) holds one pool of at most **5** connections, and a workgroup supervisor with child health checks holds `health_workers + 1`. Idle, the pools open at `min_size=1`, so an idle worker holds 5 sessions (7 as leader) and grows to the maxima under load.

The only scaling-sensitive term is `max_concurrency`: raising it raises the derived `worker_pool` (`int(max_concurrency × 1.5)`), the conditional slot pool (`max_concurrency + 1`), and - restart-only, see [ops.md §4](ops.md#4-sizing-workers-and-postgres-connections) - nothing else. The 1.5 factor is burst headroom for terminal writes that land just after a job finishes ([Derived Values](configuration.md#derived-values)).

### Fleet sizing table

The three profiles below are computed with ops.md §4's fleet formula (`direct = M × (dispatcher_pool_size + heartbeat_pool_size + 2) + 2`, `pooled = M × int(max_concurrency × 1.5)`, steady state, no per-slot pool). They are starting points, not recommendations - the derivation and its caveats live in [Starting configurations](configuration.md#starting-configurations).

| Profile | Workers | `TASKQ_MAX_CONCURRENCY` | PG connections (steady) | Rolling-deploy peak |
|---|---|---|---|---|
| Small | 2 | 4 | 34 (22 direct + 12 pooled) | ~68 |
| Medium | 5 | 8 (default) | 112 (52 direct + 60 pooled) | ~224 |
| Large | 10 | 16 | 342 (102 direct + 240 pooled) | ~684 |

Worked example (the Large profile): `direct = 10 × (4 + 4 + 2) + 2 = 102`, `pooled = 10 × int(16 × 1.5) = 240`, `total = 342` - before your application's own pools. On the per-slot path the same shape is 512.

Two budget rules that bite at fleet size:

- **Rolling deploys double the fleet briefly.** Orchestrators start new pods before draining old ones, so plan `max_connections` against roughly 2× steady state. Omitting that term is the most common way the budget gets underestimated.
- **Past 80 total connections, PgBouncer is recommended** (`compute_connection_budget()` flags it; the worker logs it at startup). Route only the `worker_pool` DSN through transaction mode; dispatcher, heartbeat, notify, and leader connections need direct session mode. See the [PgBouncer Configuration Pattern](configuration.md#pgbouncer-configuration-pattern).

---

## 2. Horizontal scale: the workgroup model

Worker processes are stateless: dispatch claims rows with `FOR UPDATE SKIP LOCKED`, so N workers against the same database never pick up the same job, and adding processes is the primary scaling lever ([deployment.md: Horizontal scaling](deployment.md#horizontal-scaling)).

### One leader per schema

Only one worker per schema holds the maintenance leader advisory lock. The lock name is schema-qualified (`taskq:maintenance_leader:<schema>`, built by `taskq.constants.schema_lock_name`), so each schema in a shared database elects its own leader. Followers are not idle replicas of the leader: **every** worker dispatches and executes jobs regardless of election outcome; leadership gates only the repair loops (lock reclaim, deadline sweep, scheduled promotion, cron, prune/archive). Failover from a leader that dies without a graceful stop is bounded by `leader_lease + heartbeat_interval` (40 s + 10 s at defaults, renewed every heartbeat interval); a graceful stop hands the row back before the drain finishes, so a follower's next election cycle takes over while the stopping pod is still draining. See [architecture.md](../architecture.md).

### The workgroup supervisor

When one box should host several independently-configured workers (per-queue poll intervals, isolated concurrency budgets), the workgroup supervisor spawns and manages N `taskq worker` subprocesses from one TOML file - see [workgroups.md](workgroups.md) and [deployment.md: Workgroup Deployment](deployment.md#workgroup-deployment). Runnable references ship in the repo: [`examples/workgroup.toml`](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/examples/workgroup.toml) (a high-throughput `api` child beside a low-concurrency `background` child) and [`examples/docker-compose.yml`](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/examples/docker-compose.yml).

What the supervisor owns, and the floors its sizing must respect:

- **Rolling release / restarts.** A child exit or a failed spawn consumes the same budget: exponential backoff from 0.5 s, doubled per successive crash, capped at 30 s; 10 restarts within a rolling 60 s window latches give-up for that child (one critical log; the workgroup must be restarted to recover it). See [workgroups.md: Restart policy](workgroups.md#restart-policy).
- **Graceful drain.** On SIGTERM the supervisor forwards the signal to every child, waits `shutdown_grace`, then SIGKILLs stragglers. Two floors bound that window, and the startup warning names the numbers when a config falls below either: the **release floor** (40 s at defaults - the cancellation 30 s + cleanup 10 s graces a child needs to land a held release) and the **clean-exit floor** (82 s at defaults - the graces plus the ~42 s bounded-close teardown tail, see [deployment.md's Health probes section](deployment.md#health-probes)). The shipped `shutdown_grace` default of 100 s covers the clean-exit floor with ~22% margin.
- **Database-backed health checks.** Optional per-child liveness via the `workers` table (heartbeat fresher than `stale_after`, default 60 s, checked every 15 s); a hung child is killed and replaced. The supervisor's health pool is sized `health_workers + 1`.

---

## 3. Queue-level throughput

Before adding processes, check whether the bottleneck is a cap or an ordering problem - [ops.md §3](ops.md#3-concurrency-process-actor-queue-fleet) is the knob-by-knob reference.

### Dispatch modes: `strict_fifo` vs `round_robin`

Queue mode is a column on the `{schema}.queues` table, defaulting to `strict_fifo`. On a strict-FIFO queue, `fairness_key` is accepted, stored, and ignored; `round_robin` is what makes it do anything - dispatch interleaves fairness cohorts (typically tenant ids) so one tenant's backlog cannot starve the others. Nothing seeds `queues` rows: set the mode with `taskq queues set-mode <queue> round_robin` (it UPSERTs; a raw `UPDATE` on a missing row silently affects zero rows). Jobs enqueued without a `fairness_key` collapse into one cohort where round-robin degenerates to plain FIFO. See [workers.md: Queue dispatch modes](workers.md#queue-dispatch-modes).

### Per-actor and per-queue caps

| Cap | Scope | Strictness | Change lands |
|---|---|---|---|
| `@actor(max_concurrent=N)` | per actor, fleet-wide | best-effort damper, not a hard cap | immediately - dispatch re-reads `actor_config` every round |
| `@actor(max_pending=N)` | per actor, enqueue-side | hard rejection (`MaxPendingExceededError`) | within seconds - enqueue-side processes hold a TTL-bounded capacity cache (5 s default staleness) |
| `taskq queues set-max-concurrent` | per queue, fleet-wide | strict leased slots (physical rows) | at the next worker restart - each worker reads the cap into memory at startup |

The `max_pending` cache is what makes the cap cheap: a producer re-counts the actor's pending depth from the database at most once per TTL instead of per enqueue, so a live retune via `taskq actor-config set` propagates fleet-wide in seconds with no redeploy. The cap change asymmetry (actor caps live, queue caps restart-bound) is in [deployment.md: `max_concurrent` and `max_pending`](deployment.md#max_concurrent-and-max_pending).

### Dispatch oversample

`TASKQ_DISPATCH_OVERSAMPLE` (default 2) multiplies per-actor candidate gathering in the dispatch SQL: each LATERAL reads `residual × oversample` candidates, absorbing identity-key collisions and multi-producer contention (the default tolerates 50% dupe identities; set 1 when no `identity_key` is used and single-producer). A dispatch round that finds its whole candidate window locked by concurrent dispatchers expands it geometrically, up to 8×, while claimable rows remain - the setting governs the steady state so the common case never pays the expansion round trip. Size it at or above the number of dispatchers that routinely poll the same actor+queue.

### The rate-limit admission path

Worker-side rate-limit acquires (token bucket / sliding window) run on the **worker pool - the pooled DSN - after the claim and before the actor body**: the consumer's denial path snoozes an already-claimed job (`dispatch.py` hands claimed jobs to the consumer with `worker_pool=`; the acquire rides that pool, `consumer.py`'s `_acquire_for_actor_with_denial_retry`). They execute under bounded row-lock budgets (`TASKQ_TOKEN_BUCKET_LOCK_TIMEOUT_MS` / `TASKQ_SLIDING_WINDOW_LOCK_TIMEOUT_MS`, 5000 ms each by default); widening a budget past its default re-derives the command timeout bound upward (`budget / 0.8`). Placement consequence: admission row-lock traffic traverses the SAME pooled capacity as the job queries - size the pooled DSN for both, and do not budget the direct DSN for admission (the dispatcher pool carries no admission acquires). See [Derived Values](configuration.md#derived-values). When Redis is unavailable, admission falls back to Postgres (`TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED=true` by default): a Redis outage funnels every acquire through those row locks, which is why the bounds exist. Keyed reservations and keyed rate limits add per-key granularity with per-process cardinality guardrails (`TASKQ_MAX_KEYED_RESERVATIONS` / `TASKQ_MAX_KEYED_RATE_LIMITS`, 10 000 each by default) - the guardrail is per process and does not scale with the replica count. See [rate-limiting.md](rate-limiting.md) and [rate-limiting.md: Queue-level concurrency cap](rate-limiting.md#queue-level-concurrency-cap).

---

## 4. Storage at scale

Postgres is the only job store; Redis is optional (real-time progress fanout for the admin UI) and never holds job state. Everything below is Postgres-side capacity planning.

### Connection budgeting

See §1 - the short version: split `TASKQ_PG_DSN_DIRECT` (session mode; dispatcher, heartbeat, notify, leader) from `TASKQ_PG_DSN_POOLED` (transaction-mode PgBouncer; `worker_pool` only), count the fleet with ops.md §4's formula, and keep the rolling-deploy doubling plus your application's own pools inside `max_connections`.

### Event volume: TimescaleDB hypertables

`job_events` records every state transition and feeds the crash-reclaim feed (`poll_reclaim_events()`), so its volume tracks fleet activity. Vanilla Postgres is the default and fully supported; with `TASKQ_TIMESCALEDB_HYPERTABLES=true`, the three retention tables (`job_events`, `jobs_archive`, `job_attempts_archive`) become hypertables and retention becomes whole-chunk drops. Chunk intervals derive from the retention settings: **retention / 4, clamped to 1 day - 30 days**, so a retention window spans a handful of chunks and a chunk drop stays bounded work. See [timescaledb.md](timescaledb.md#what-the-conversion-does).

### Sweep and archival throughput

Every repair loop writes in bounded, individually committed batches: sweeps 1-2 transition at most **2 × `event_writer_batch_size`** rows per call (each arm LIMITs at the batch size), drained across at most `TASKQ_SWEEP_DRAIN_BATCHES` (default 8) batches per 30 s tick. That bound is the fleet's crash-recovery throughput ceiling - the full derivation, the ratchet and watermark failure modes it prevents, and the worked 50 000-row backlog example are in [maintenance-sweeps.md §1](maintenance-sweeps.md#1-what-the-sweeps-are). Do not scale out a fleet to escape a stalled sweep: extra workers consume `pending` jobs faster but promote and reclaim nothing.

### Retention defaults

| Data | Default retention | Sweep cadence |
|---|---|---|
| `job_events` | 7 d (`TASKQ_EVENT_RETENTION_PERIOD`) | leader sweep loop; one 10 000-row batch per tick, deliberately not drained |
| Keyed orphaned rows | 1 h idle (`TASKQ_KEYED_ROW_RECLAIM_PERIOD`) | leader sweep loop; one 256-row batch per table per tick |
| Succeeded jobs (hot `jobs` table) | 30 d | daily prune, 03:00 UTC → `jobs_archive` |
| Cancelled jobs (hot) | 30 d (`TASKQ_PRUNE_RETENTION_CANCELLED`) | daily prune, 03:00 UTC — under bulk-cancel-heavy workloads this is the dominant hot-table dwell class |
| Failed / abandoned jobs (hot) | 90 d | daily prune, 03:00 UTC |
| `jobs_archive` | 365 d | daily archive-expiry sweep, 04:00 UTC |

Per-actor `retention_days` metadata can shorten the hot-table dwell for high-volume actors; the row-count arithmetic for sizing the hot table is the storage-planning note in [configuration.md](configuration.md#job-retention-and-archive).

---

## 5. One big worker or N small ones?

Horizontal scale is not free - each added process pays fixed costs that a single fat process pays once:

| Signal | Favour one big worker (higher `TASKQ_MAX_CONCURRENCY`, fewer processes) | Favour N small workers |
|---|---|---|
| **Shutdown/drain tails** | Every process pays the ~42 s bounded-close teardown tail on shutdown (8 sequential closes × 5 s + 2 s publish drain). A batch/broadcast workload with a fleet-wide stop pays N tails and herds N restarts; one fat worker pays one. | Long-running or crash-prone actors: one wedged child takes its concurrency down, not the fleet's. |
| **Postgres failover restart herds** | the worker self-terminates on the `(max_heartbeat_failures + 1)`-th consecutive failed beat (the 4th at the default of 3 - the isolate decision is strictly greater-than) - at defaults roughly ten seconds of failed beats (2.5 s failed-tick pacing) ends **every** worker at once, and the restart herd is sized by your replica count. Fewer replicas, smaller herd. | Fault isolation matters more than herd size (noisy neighbours, memory pressure, deploy blast radius). |
| **Connection budget** | One process at `max_concurrency=16` holds the same pooled count as two at 8 with fewer fixed direct connections; the fleet formula's `+2` leader extras belong to the ONE elected leader per schema (not per process); the pool floors are the per-process constants. | Independent queue subscriptions and rolling deploys per group; a workgroup on one box gets both with one supervisor. |
| **Lease/heartbeat capacity** | The `lock_lease` floor is the failed-beat cascade: `max(heartbeat_interval, heartbeat_command_timeout) + (max_heartbeat_failures + 1) × (heartbeat_interval + heartbeat_command_timeout)` = **58 s at defaults** against the 60 s lease. That floor is per-process-fixed, so it does not shrink with replica count - it constrains how far heartbeat timing can be tuned on any worker, however many you run. | Cross-region or loaded Postgres raises `heartbeat_command_timeout`, which raises the floor; spreading workers changes nothing about that math. |
| **Co-tenancy / density** | Sub-second timing budgets (a tightened `idle_settle_window`, watchdog budgets) need headroom: on a dense host, co-tenant CPU and cache effects contaminate every timed interval, which is why the defaults err heavily towards missing a hang instead of tripping on load ("raise budgets, don't lower them"). A fat worker on an undersized box is the dense-host failure shape. | Pinning busy work to dedicated low-concurrency workers keeps the host - and every timing budget on it - quiet. |

Two density lessons from the repo's own history make the co-tenancy row concrete: the coverage leg runs `-n 2` because halving the traced tree's resident footprint per worker is what bounded its memory ([oom-419-verdict.md](../design/oom-419-verdict.md)), and the timing-scaling rules in the [debugging playbook](../design/debugging-playbook.md) exist because co-tenant CPU made growth-ratio gates lottery tickets. Budget headroom before you budget width.

For the until-idle flavour of drain: the settle window (2 s default) exists to close the race where a producer enqueues between the drain check and the shutdown trigger - shrinking it towards zero on a shared queue converts a settle budget into flaky early exits. See [workers.md: Until-idle mode](workers.md#until-idle-mode).

---

## Scaling checklist

Work the list top-down; each step is cheaper than the one below it.

1. **Read the bottleneck before adding anything.** Utilisation (`taskq.worker.active_jobs / taskq.worker.max_concurrency`) beside `taskq.jobs.oldest_pending_age_seconds`: high utilisation with a climbing age means capacity; low utilisation with a climbing age means a cap, an unserved queue, or a promotion stall - adding workers changes nothing. The decision table is [ops.md §3: When to add workers](ops.md#when-to-add-workers-saturation-not-depth).
2. **Knobs in order of cost:** the actor's `max_concurrent` damper (`taskq actor-config set`, live) → queue mode / `fairness_key` for head-of-line blocking → `TASKQ_DISPATCH_OVERSAMPLE` when many dispatchers poll one actor+queue → `TASKQ_MAX_CONCURRENCY` (restart-only; raises the derived `worker_pool` and, on the per-slot path, the slot pool) → more replicas. [ops.md §12](ops.md#12-scaling-playbook-from-signal-to-knob) routes symptoms to knobs.
3. **Read `taskq.queue.depth`** (pending + scheduled per queue; the leader samples every 15 s, the 100 deepest queues keep their series) against **`taskq.queue.live_workers`** (sampled in the same tick): depth with no live workers is an unserved queue, not a capacity shortage. Sweep health comes from the `taskq.maintenance_leader.*` family - `sweep_last_success_seconds` staleness, `sweep_timeouts` rates, and `sweep_batch_size` vs `_configured` (a latched breaker) - before concluding the fleet is too small. See [observability.md](observability.md).
4. **Re-run the connection arithmetic at every fleet change** - workers × (pools + 2) plus pooled plus your app's pools, at rolling-deploy peak (~2× steady) - and check it against `max_connections`. Route the pooled DSN through PgBouncer once the total crosses the 80-connection threshold. The formula and the worked examples are in [ops.md §4](ops.md#4-sizing-workers-and-postgres-connections).
5. **Shard by schema when a database boundary is the real need.** The advisory-lock namespace is `taskq:{purpose}:{schema}` (election, cron, prune, archive-expiry, migrate locks are all schema-qualified), and Redis keys carry the schema too, so one Postgres database can host independent fleets per schema - each electing its own leader. Two deployments on the *same* schema name are one fleet by design. The isolation boundary and its limits are in [maintenance-sweeps.md §4](maintenance-sweeps.md#4-multi-schema-coexistence) and [architecture.md](../architecture.md).

---

## Related documentation

- [ops.md §4: Sizing](ops.md#4-sizing-workers-and-postgres-connections) - the connection-budget derivation
- [configuration.md: Derived Values](configuration.md#derived-values) - the pool formulas behind §1
- [workgroups.md](workgroups.md) - the supervisor's full configuration reference
- [maintenance-sweeps.md](maintenance-sweeps.md) - the batch bounds and their failure modes
- [deployment.md: Scaling Considerations](deployment.md#scaling-considerations) - platform-level scaling (Kubernetes, Compose)
