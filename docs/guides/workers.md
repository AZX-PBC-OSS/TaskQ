# Worker

The TaskQ worker is a long-running asyncio process that polls a Postgres-backed job queue, dispatches jobs to registered actor handlers, and keeps the cluster healthy through heartbeating, leader election, and graceful shutdown. Every worker process runs a single `asyncio.TaskGroup` containing a fixed set of sibling coroutines that live for the lifetime of the process: a heartbeat loop, a NOTIFY listener, a maintenance-leader loop, a producer stub, and `max_concurrency` consumer loops. All siblings observe a shared `shutdown_event`; when it is set every sibling returns cleanly and the process exits.

## Prerequisites

- Python 3.12+
- TaskQ installed (`uv add taskq-py`) — core includes `asyncpg`, no extra needed
- A running Postgres instance with the TaskQ schema applied (`taskq migrate up`)

See [../getting-started/quick-start.md](../getting-started/quick-start.md) for initial setup and [../architecture.md](../architecture.md) for system-level context.

---

## Internal components

**Producer loop.** Polls the `jobs` table for `pending` rows via the dispatch CTE. Each tick acquires a direct connection from `dispatcher_pool`, runs the strict-FIFO dispatch SQL (atomic `FOR UPDATE SKIP LOCKED` + `UPDATE … SET status='running'`), and pushes dispatched rows onto an in-process `asyncio.Queue[JobRow]` (`local_queue`). The local queue's `maxsize` is set to `max_concurrency`; back-pressure from a full queue naturally throttles the producer.

**Consumer loops.** `max_concurrency` concurrent coroutines drain `local_queue`. Each iteration pops one `JobRow`, resolves the actor's DI scope via `build_actor_scope`, validates the payload against `actor_ref.payload_type`, registers the job with `ActiveJobRegistry`, invokes the actor function, writes the terminal state to Postgres, and deregisters. All Postgres writes inside a consumer are wrapped in `asyncio.shield` so that cancellation during shutdown cannot strand a row in `running` status.

**Heartbeat loop.** On every `heartbeat_interval` tick, acquires one connection from `heartbeat_pool`, opens a single transaction, and atomically updates `workers.last_seen_at`, extends `jobs.lock_expires_at` for all running jobs owned by this worker, extends `reservation_slots.lease_expires_at`, and (if this worker is the leader) pings `maintenance_leader.last_seen_at`. After the transaction commits, runs the cancel-controller's `run_post_tx` to drain any phase-3 abandonment queue. Consecutive failures increment `heartbeat_failures`; exceeding `max_heartbeat_failures` triggers `isolate_self`.

**NOTIFY listener.** Holds a dedicated direct connection (`notify_conn`) subscribed to the `taskq_wake_{schema}` channel. When a NOTIFY arrives, the listener calls `event.set()` on all registered producer wake-subscribers, waking any sleeping producer immediately rather than waiting for the next poll tick. A health-check coroutine issues `SELECT 1` every `notify_health_check_interval` seconds and reconnects with bounded exponential backoff on failure.

External code (for example a bulk-enqueue script) can wake sleeping workers immediately without going through the normal enqueue path:

```sql
SELECT pg_notify('taskq_wake_taskq', '');
```

Replace `taskq_wake_taskq` with `taskq_wake_{schema}` where `{schema}` is the value of `TASKQ_SCHEMA_NAME` (default `taskq`).

**Maintenance leader.** One worker per cluster wins a Postgres advisory lock (`pg_try_advisory_lock`) and becomes the maintenance leader. The leader runs ten cooperative sub-loops inside a single `asyncio.TaskGroup`: `_election_loop`, `_watchdog_loop`, `_scheduled_wake_loop`, `_cron_loop`, `_sweep_loop`, `_prune_loop`, `_archive_expiry_loop`, `_queue_depth_loop`, `_reservation_slots_loop`, and `_stranded_jobs_loop`. Non-leader workers re-attempt election each `heartbeat_interval`.

**Cancel controller.** Runs inside the heartbeat transaction on each tick. Polls `jobs.cancel_phase` for all jobs owned by this worker. Drives three phases: cooperative observation (set `cancel_event`, record timestamp), forced escalation (write `cancel_phase=2` to Postgres, then `task.cancel()`), and abandonment queuing (hand the job ID to `_pending_abandons` for post-transaction cleanup). Phase-3 terminal writes run in `run_post_tx` after the transaction releases its row locks to avoid deadlock.

**Health server.** An `asyncio`-based Unix-domain-socket HTTP server exposing `/live`, `/ready`, and `/metrics`. Started at worker boot when `health_enabled=True`; stopped gracefully during teardown.

The Unix socket is not reachable via Kubernetes `httpGet` probes. Use `exec` probes instead:

```yaml
livenessProbe:
  exec:
    command: ["taskq", "health", "live"]
```

**Shutdown orchestrator.** Handles SIGTERM/SIGINT. Drives the four-phase sequence: DRAINING → CANCELLING → FORCING → ABANDONING, then sets `shutdown_event` so all TaskGroup siblings return.

**DI scope chain.** Three scope containers — `ProcessScope`, `ThreadScope`, `LoopScope` — are bootstrapped in sequence after `open_worker_deps`. They resolve declared dependencies for actors at dispatch time using `build_actor_scope`, which opens a per-invocation TRANSIENT scope. TRANSIENT teardown runs after each job regardless of outcome.

---

## Starting a worker

### Via the CLI

```shell
taskq worker --actors myapp.actors:registry
```

See [cli.md](cli.md) for the full option reference. The `--actors` argument is required. All other settings load from environment variables or `.env` files.

### Programmatically via `worker_main()`

`worker_main` is the production entry point. It sets up logging, starts an `asyncio.Runner`, and calls `_main` which wires the full TaskGroup.

```python
from taskq.settings import WorkerSettings
from taskq.worker.run import worker_main
from myapp.actors import registry

settings = WorkerSettings.load()
exit_code = worker_main(settings, actor_registry=registry)
```

`actor_registry` must be a `Mapping[str, ActorRef]`. Passing `actor_registry=None` runs stub consumers (M0/internal use only). Production code must always pass a registry.

`WorkerSettings.load()` reads all `TASKQ_*` environment variables and applies DSN fallback and invariant validation. Always construct settings through `load()` or `load_from_dict()`, never via the constructor directly, because `_post_load()` must run.

`worker_main` returns an `int` exit code (0 on clean shutdown). In a container entrypoint:

```python
import sys
from taskq.settings import WorkerSettings
from taskq.worker.run import worker_main
from myapp.actors import registry

if __name__ == "__main__":
    sys.exit(worker_main(WorkerSettings.load(), actor_registry=registry))
```

---

## Actor registry

The worker accepts actors as either a `Mapping[str, ActorRef]` (keys are actor names) or an `Iterable[ActorRef]` (names are read from `ActorRef.name`).

**Defining actors in a module:**

```python
# myapp/actors.py
from pydantic import BaseModel
from taskq import actor, RetryPolicy

class SendEmailPayload(BaseModel):
    to: str
    subject: str
    body: str

@actor(queue="email", retry=RetryPolicy(kind="transient", max_attempts=5))
async def send_email(payload: SendEmailPayload) -> None:
    ...

# Iterable form — pass the ActorRef objects directly
registry = [send_email]

# Mapping form — keyed by actor name
registry_map: dict[str, object] = {"send_email": send_email}
```

**CLI invocation using the iterable form:**

```shell
taskq worker --actors myapp.actors:registry
```

**CLI invocation using the mapping form:**

```shell
taskq worker --actors myapp.actors:registry_map
```

The `module:attr` string must resolve to a `Mapping[str, ActorRef]` or an `Iterable[ActorRef]` at import time. If the attribute is neither, the CLI prints an error and exits with code 1.

**Generator registries are unsafe.** The attribute resolved from `MODULE:ATTR` must be a reusable `Mapping` or a `list`/`tuple` of `ActorRef` — not a generator or other one-shot iterable. The CLI iterates the resolved object twice during type-checking: the first pass exhausts a generator, and the second pass sees an empty sequence and silently builds an empty registry, causing all dispatched jobs to be dropped with `dispatch-actor-not-found` errors.

See [actors.md](actors.md) for the full `@actor` decorator reference.

---

## Queue selection

Each worker consumes from one or more named queues. The queue list can be set three ways (highest to lowest precedence):

1. `--queues` CLI flag (one flag per queue name)
2. `TASKQ_QUEUES` environment variable (comma-separated)
3. Default: `["default"]`

**Multiple queues per worker:**

```shell
taskq worker --actors myapp.actors:registry --queues default --queues priority --queues email
```

```shell
TASKQ_QUEUES=default,priority taskq worker --actors myapp.actors:registry
```

The `--queues` flag is a multi-value Typer option: pass it once per queue name. Do not pass a single comma-separated string to `--queues` on the command line — use the environment variable form for comma-separated input.

The dispatch CTE filters `jobs.queue = ANY($queues)`, so one worker process can consume from any subset of queues in a single polling round.

Actors declare which queue they target via `@actor(queue="...")`. A worker that does not include that queue in its `TASKQ_QUEUES` list will never pick up those jobs.

### Queue dispatch modes

Each queue has a `mode` column in the `queues` table that controls how the dispatch CTE
orders candidates. The mode is resolved by querying the `queues` table at dispatch time
(one indexed lookup per batch). Queues not present in the table default to `strict_fifo`.

| Mode | Behaviour |
|---|---|
| `strict_fifo` (default) | Jobs are dispatched in priority-then-time order (`priority DESC, scheduled_at, id`). Every pending job competes freely — a deep queue of one actor can starve others if all candidates share high priority. |
| `round_robin` | **Per-actor lateral dispatch.** Jobs are interleaved by `fairness_key` cohort. Within a cohort, ordering is priority-then-time. Across cohorts, dispatch picks round-robin: one job from each fairness cohort per round. This prevents a deep queue of one tenant/actor from starving all others. |

**When to use `round_robin`:**
- Multi-tenant queues where one busy tenant's backlog must not block others.
- Queues with distinct fairness cohorts (e.g. per-customer processing).
- Any scenario where strict FIFO would cause head-of-line blocking across unrelated
  work streams.

**Setting a queue's mode:**

```sql
UPDATE taskq.queues SET mode = 'round_robin' WHERE name = 'multi';
```

The change takes effect on the next worker restart. Queues not present in the table
default to `strict_fifo`.

**`fairness_key` and `round_robin`:** Actors declare a `fairness_key` callable to
assign jobs to cohorts. Without one, all jobs collapse into a single `__null__` cohort
— dispatch becomes equivalent to `strict_fifo` within that cohort. See
[jobs-clients.md](jobs-clients.md) for `fairness_key` declaration.

**`dispatch_oversample`:** The dispatch CTE gathers `residual × oversample` candidates
per actor in the LATERAL subquery. `residual` is the actor's remaining concurrency
capacity. The `oversample` multiplier (default `2`, env `TASKQ_DISPATCH_OVERSAMPLE`)
absorbs identity-key collisions and multi-producer contention without reducing dispatch
yield. Set to `1` when no `identity_key` is used and single-producer deployment.

**`dispatch_scope_by_home_queue`:** When enabled (`TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE=true`),
the `per_actor_capacity` CTE filters to actors whose home queue is in the worker's
subscribed queue list. This lowers the per-cycle probe count (fewer LATERAL subqueries)
but excludes jobs enqueued via `enqueue(queue=...)` overrides where the actor's home
queue differs from the override queue. Default `false` (override-safe).

---

## Concurrency model

`max_concurrency` (default `8`, env `TASKQ_MAX_CONCURRENCY`) is the upper bound on simultaneously executing jobs. The `local_queue` maxsize equals `max_concurrency`, so the producer can lock at most that many additional rows beyond those already executing.

`worker_pool_size` is derived automatically:

```
worker_pool_size = int(max_concurrency * 1.5)
```

The 1.5 factor provides headroom for terminal writes that occur just after a job finishes while the slot is being recycled. This pool is used for worker-path Postgres writes (`mark_succeeded`, `mark_failed_or_retry`, `mark_cancelled`, `mark_abandoned`). It may route through PgBouncer in transaction mode; see [PgBouncer compatibility](#pgbouncer-compatibility).

`dispatcher_pool_size` (default `4`) and `heartbeat_pool_size` (default `4`) are independent pools; both always use the direct DSN.

The worker spawns exactly `max_concurrency` consumer loop coroutines. They are cooperatively concurrent — asyncio, not threads. CPU-bound work should be offloaded to a thread pool executor via `asyncio.get_running_loop().run_in_executor`.

---

## Dispatch sequence

Each consumer loop iteration follows this sequence:

1. **Dequeue from local queue.** The consumer races `local_queue.get()` against `shutdown_event.wait()`. On shutdown win, the consumer returns cleanly.

2. **Actor lookup.** The job's `actor` field is looked up in `actor_registry`. If not found, logs `dispatch-actor-not-found` and continues to the next job (the row remains `running`; the sweep will reclaim it after `lock_lease` expires).

3. **Payload validation.** `actor_ref.payload_type.model_validate(job.payload)` validates the raw JSONB dict against the declared Pydantic model. A `PayloadValidationError` is non-retryable and immediately fails the job.

4. **DI scope resolution.** `build_actor_scope` opens a TRANSIENT scope, resolves all `Annotated[T, Scope.X]` parameters declared by the actor handler, and returns them as `resolved.di_kwargs`.

5. **JobContext construction.** A `JobContext[P]` is built with the validated payload, a fresh `cancel_event`, the worker ID, attempt number, and an OTel consumer span linked to the producer span via `trace_id` / `span_id` from the job row.

6. **Rate-limit / reservation acquire.** If the actor declares `rate_limits` or `reservations` and a `RateLimitRegistry` is registered at LOOP scope, `acquire_for_actor` is called. On denial (`ReservationUnavailable`), the job is snoozed and the actor is not invoked.

7. **Actor invocation.** The actor function is called with `(payload, ctx, **di_kwargs)`. If a LOOP-scope `asyncpg.Connection` is registered, the invocation and `mark_succeeded_with_conn` are wrapped in a single `conn.transaction()`, making the job status update and any sub-enqueues transactional.

8. **Result / exception handling.** See [Retry and backoff](#retry-and-backoff). All terminal Postgres writes are wrapped in `asyncio.shield`.

9. **TRANSIENT scope teardown.** Runs unconditionally after each invocation regardless of outcome.

10. **Rate-limit release.** `release_for_actor` is called in the `finally` block (best-effort, not shielded).

---

## Retry and backoff

Each actor carries a `RetryPolicy`. The default policy is:

```python
RetryPolicy(
    kind="transient",
    max_attempts=3,
    backoff="exponential",
    base=timedelta(seconds=5),
    cap=timedelta(hours=1),
    jitter=0.2,
)
```

**Policy fields:**

| Field | Type | Default | Description |
|---|---|---|---|
| `kind` | `"transient" \| "indefinite" \| "non_retryable"` | `"transient"` | Retry strategy |
| `max_attempts` | `int` | `3` | Maximum attempts before failing (used by `transient`) |
| `time_budget` | `timedelta \| None` | `None` | Optional wall-clock deadline for `indefinite` retries; see below |
| `backoff` | `"exponential" \| "linear" \| "fixed"` | `"exponential"` | Delay formula |
| `base` | `timedelta` | `5s` | Base delay |
| `cap` | `timedelta` | `1h` | Per-attempt backoff ceiling |
| `jitter` | `float` | `0.2` | Multiplicative jitter fraction in `[0.0, 1.0]` |

**`time_budget` on indefinite retries.** When `kind="indefinite"` and `time_budget` is set, the enqueue path automatically computes `schedule_to_close = enqueue_time + time_budget`. Callers do not need to compute an absolute deadline themselves. See [retries.md](retries.md) for details.

**Policy kinds:**

| `kind` | Behavior |
|---|---|
| `transient` | Retries up to `max_attempts`. `attempt >= max_attempts` → `failed`. |
| `indefinite` | Retries until `schedule_to_close` deadline. `max_attempts` is ignored for the retry decision. |
| `non_retryable` | Never retried; any exception immediately fails the job. |

**Backoff formulas** (all capped at `min(policy.cap, max_retry_backoff)`):

| `backoff` | Formula |
|---|---|
| `exponential` | `base * 2^(attempt-1)` |
| `linear` | `base * attempt` |
| `fixed` | `base` |

After computing the raw delay, multiplicative jitter is applied: `delay = raw * uniform(1 - jitter, 1 + jitter)`. The default `jitter=0.2` gives ±20% variation.

**Global backoff ceiling:** `TASKQ_MAX_RETRY_BACKOFF` (default `24h`) is applied as `effective_cap = min(policy.cap, max_retry_backoff)`. This prevents a misconfigured `cap=timedelta(days=365)` from stranding jobs silently.

**Control-flow exceptions:**

- `Snooze(delay: timedelta)` — re-schedules the job with `mark_snoozed` and increments a `snooze_count` metadata key. If `schedule_to_close` has passed, the job is failed with `DeadlineExceeded`.
- `RetryAfter(delay: timedelta, consume_budget: bool)` — re-schedules the job immediately. When `consume_budget=True`, the re-schedule counts against `max_attempts`. When `False`, it does not. Fails the job if `schedule_to_close` has passed or `max_attempts` is exhausted.

Both exceptions are raised from inside the actor body; they are not errors.

---

## Cancellation

Cancellation is a three-phase protocol coordinated between the API layer (which writes `cancel_phase` to Postgres) and the heartbeat loop (which polls it). The phases map to `CancelPhase` enum values:

| Phase | Value | Trigger | Worker action |
|---|---|---|---|
| `NONE` | 0 | — | Job running normally |
| `COOPERATIVE` | 1 | Cancel flag written by API | Sets `ctx.cancel_event`; actor may observe and return cooperatively |
| `FORCED` | 2 | `cancellation_grace_period` elapsed | Writes `cancel_phase=2` to Postgres, then calls `task.cancel()` |
| `ABANDON_PENDING` | 3 | `cleanup_grace_period` elapsed | Queues job for `mark_abandoned` after heartbeat transaction commits |

**Actor-side cooperative cancellation:**

```python
@actor
async def long_running(payload: MyPayload, ctx: JobContext[MyPayload]) -> None:
    for chunk in chunks:
        if ctx.cancel_event.is_set():
            return  # cooperative exit; job will be marked cancelled
        await process(chunk)
```

The `CancelController` protocol has two methods called by the heartbeat loop:

- `run_in_tx(conn)` — phases 1–3 eligibility check, runs inside the heartbeat transaction.
- `run_post_tx()` — drains the phase-3 abandonment queue after the transaction commits.

`run_post_tx` must always be called after `run_in_tx` on the same tick, even if `run_in_tx` raises. The heartbeat loop calls it unconditionally.

The consumer skips `mark_cancelled` when `cancel_phase >= 3` (ABANDON_PENDING), because `run_post_tx` owns that terminal write.

---

## Heartbeat and liveness

Every `heartbeat_interval` seconds (default `10.0`s, env `TASKQ_HEARTBEAT_INTERVAL`) the heartbeat loop:

1. Acquires a connection from `heartbeat_pool` with a `timeout=interval`.
2. Opens a transaction and executes four SQL statements atomically: update `workers.last_seen_at`, extend `jobs.lock_expires_at` by `lock_lease`, extend `reservation_slots.lease_expires_at`, and (if leader) ping `maintenance_leader.last_seen_at`.
3. Runs the cancel-controller `run_in_tx` inside the same transaction.
4. After the transaction commits, calls `run_post_tx`.

`lock_lease` (default `60.0`s, env `TASKQ_LOCK_LEASE`) is the duration a job's lock remains valid without a heartbeat. The invariant `lock_lease >= 4 * heartbeat_interval` is enforced at startup and prevents the recovery sweep from reclaiming locks on a live worker that experienced transient heartbeat delays.

If `heartbeat_pool.acquire()` times out, raises a connection error, or `run_in_tx` raises an `OSError`, `heartbeat_failures` is incremented. When `heartbeat_failures > max_heartbeat_failures` (default `3`), `isolate_self` is called:

1. Opens a fresh direct connection (bypassing `heartbeat_pool`, which may be exhausted).
2. In a transaction, reads all `running` jobs owned by this worker.
3. For each job: transitions retryable jobs to `pending` (scheduled 5s in the future) and non-retryable jobs to `crashed`. Writes an attempt record with `error_class=HeartbeatLost`.
4. Always sets `shutdown_event` so the process exits.

---

## Leader election

Every worker competes for the Postgres advisory lock `taskq:maintenance_leader` by calling `pg_try_advisory_lock(hashtextextended($1, 0))` on a dedicated direct connection (`leader_conn`). Only one worker per cluster can hold the lock; it is held for the lifetime of the connection (session-scoped advisory lock).

Advisory locks are session-scoped and are dropped when the connection is released. This is why `leader_conn` must use `pg_dsn_direct` and cannot route through PgBouncer in transaction mode — transaction-mode pooling releases the underlying session between transactions, which would silently drop the lock.

The elected leader:

- Upserts a row into `maintenance_leader` (worker ID, elected timestamp).
- Runs the maintenance sweep loop every 30 seconds: `reclaim_expired_locks` (sweep 1), `deadline_sweep` (sweep 2), and, when the backend supports them, `sweep_leaked_reservation_slots` (sweep 4), `sweep_expired_results`, and `cleanup_stale_workers`.
- Runs the scheduled-wake loop every 1 second, transitioning `scheduled` jobs whose `scheduled_at <= now()` back to `pending` and issuing a `pg_notify` wake signal.
- Runs the **prune loop** (Sweep 5) once daily at `TASKQ_PRUNE_SCHEDULE_UTC` (default `03:00` UTC). Moves terminal jobs from `jobs` to `jobs_archive` after their per-status retention period has elapsed. Acquires advisory lock `taskq:prune` to prevent concurrent runs across a rolling deploy.
- Runs the **archive expiry loop** (Sweep 6) once daily at `TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC` (default `04:00` UTC, 1 hour after the prune). Hard-deletes rows from `jobs_archive` once their `expire_at` has passed. Acquires advisory lock `taskq:archive_expiry`.
- Runs the **stranded-jobs detector** (`_stranded_jobs_loop`) every 60 seconds: warns (does not delete or reassign) when `pending`/`scheduled` jobs exist for an actor with no `actor_config` row — typically because the actor was removed from the registry but jobs referencing it are still enqueued.
- Samples queue depth and reservation slot counts every 15 seconds for OTel metrics.

A watchdog coroutine probes `leader_monitor_conn` (a second dedicated direct connection) every 5 seconds. On connection failure, the watchdog clears `is_leader` and closes both leader connections, which releases the advisory lock and allows another worker to win the next election.

Non-leader workers re-attempt election every `heartbeat_interval` seconds.

---

## Graceful shutdown

SIGTERM (or SIGINT) triggers `orchestrate_shutdown`. A second signal fast-advances CANCELLING → FORCING. A third signal calls `sys.exit(1)` (Kubernetes SIGKILL is the hard backstop).

**Phase sequence:**

| Phase | `shutdown_phase` value | Action |
|---|---|---|
| DRAINING | 1 | Sets `producer_stop_event`; calls `drain_local_queue_to_pending` to re-pend locked-but-not-started rows |
| CANCELLING | 2 | Sets `cancel_event` on all in-flight jobs; waits up to `cancellation_grace_period` for cooperative exit |
| FORCING | 3 | Writes `cancel_phase=2` to Postgres for remaining jobs, calls `task.cancel()`; waits up to `cleanup_grace_period` |
| ABANDONING | 4 | Calls `mark_abandoned` on any still-running jobs; closes `leader_conn` (releasing the advisory lock) |

**DRAINING phase detail.** `drain_local_queue_to_pending` re-pends only DB-level rows where `status='running' AND started_at IS NULL`. Jobs already in the in-process asyncio queue are processed normally if the consumer is still running, or reclaimed by the sweep after `lock_lease` expires if not.

**ABANDONING phase detail.** The ABANDONING phase writes terminal state externally (not from the job's own task) via `mark_abandoned`. The job's asyncio task is cancelled, not awaited to completion. The task will receive `CancelledError`; any pending shield calls in the consumer may or may not succeed.

After ABANDONING completes, `shutdown_event` is set and all TaskGroup siblings return.

**Timing constraints** (validated at startup):

```
cancellation_grace_period + cleanup_grace_period < termination_grace_period - 5.0
cancellation_grace_period + cleanup_grace_period < lock_lease
```

Defaults: `cancellation_grace_period=30.0`, `cleanup_grace_period=10.0`, `termination_grace_period=60.0`, `lock_lease=60.0`.

---

## Health server

When `TASKQ_HEALTH_ENABLED=true` (the default), the worker binds a Unix-domain socket at `TASKQ_HEALTH_SOCKET_PATH` (default `/tmp/taskq_health.sock`) and serves three HTTP endpoints over it.

The Unix socket is not reachable via Kubernetes `httpGet` probes. Use `exec` probes:

```yaml
livenessProbe:
  exec:
    command: ["taskq", "health", "live"]
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  exec:
    command: ["taskq", "health", "ready"]
  initialDelaySeconds: 5
  periodSeconds: 10
```

**Endpoints:**

| Path | Success condition | Success response | Failure response |
|---|---|---|---|
| `GET /live` | Event loop responsive within 1.0s | `200 {"status":"ok"}` | `503 {"status":"unresponsive"}` |
| `GET /ready` | `shutdown_phase == NONE` and PG ping succeeds within `health_pg_ping_timeout` | `200 {"ready":true,...}` | `503 {"ready":false,...}` |
| `GET /metrics` | Always | `200` Prometheus text format | — |

The `/ready` response body includes:

```json
{
  "ready": true,
  "redis_configured": false,
  "active_jobs": 3,
  "is_leader": true,
  "shutdown_phase": null
}
```

The `/metrics` response body:

```
# HELP taskq_active_jobs Currently in-flight jobs on this worker.
# TYPE taskq_active_jobs gauge
taskq_active_jobs 3
# HELP taskq_is_leader 1 if this worker holds the maintenance leader lock.
# TYPE taskq_is_leader gauge
taskq_is_leader 1
# HELP taskq_shutdown_phase Current shutdown phase enum value (0=NONE).
# TYPE taskq_shutdown_phase gauge
taskq_shutdown_phase 0
```

Probe these from a Kubernetes sidecar or `taskq health live` / `taskq health ready`. See [cli.md](cli.md) for the CLI commands.

---

## ActorConfig sync

At startup, after `register_worker`, the worker calls `sync_actor_config` for every registered actor. This writes (or updates) rows in `{schema}.actor_config` with `max_concurrent`, `max_pending`, `queue`, `result_ttl`, and `metadata` values taken from the `ActorRef`.

The sync uses a transactional SELECT-then-UPSERT to prevent races between concurrent worker startups. If the stored row for an actor differs from the registered values (a "drift"), two outcomes are possible:

- **`force=False` (default):** raises `ActorConfigDriftList` and the worker refuses to start. The CLI prints the drift details and instructs the operator to re-run with `--force-update-actor-config`.
- **`force=True`:** logs `actor-config-drift-overwrite` at ERROR for each drifted field and overwrites the stored value.

**`ActorConfigDriftList` wraps one `ActorConfigDriftError` per drifted field per actor.** A single startup check can produce multiple `ActorConfigDriftError` instances — one for each combination of actor × field that differs. For example, if two actors each have two drifted fields, the list will contain four errors.

**Drift logs are emitted even when `force=True`.** The `actor-config-drift-overwrite` ERROR log is written regardless of whether the overwrite was intentional. To distinguish intentional overwrites from unexpected drift, filter log lines by the `force=true` field: lines with `force=true` were intentional; lines with `force=false` blocked startup.

**Typical deployment workflow when changing `max_concurrent`:**

```shell
# Deploy once with the flag to overwrite:
TASKQ_FORCE_UPDATE_ACTOR_CONFIG=true taskq worker --actors myapp.actors:registry

# Subsequent deploys without the flag (drift is now gone):
taskq worker --actors myapp.actors:registry
```

The fields checked for drift are: `max_concurrent`, `max_pending`, `queue`, `result_ttl`, and `metadata`. Actor name changes require a migration; rename detection is not implemented.

---

## Running multiple workers

Multiple worker processes against the same database are fully supported. Each process registers a separate row in `{schema}.workers` with its own UUID, hostname, and PID.

**Dispatch safety.** The dispatch CTE uses `SELECT ... FOR UPDATE SKIP LOCKED`, so two workers polling simultaneously cannot pick up the same job. Each job row is locked by exactly one worker at a time.

**Single leader.** Only one worker holds the `taskq:maintenance_leader` advisory lock at a time. Other workers retry election on every `heartbeat_interval` tick. If the leader pod dies, the lock is released when the connection closes, and another worker wins the next election.

**Rolling deploy gotcha.** If old and new worker versions declare different `max_concurrent`, `queue`, or `metadata` for the same actor name, new worker pods will fail startup with `ActorConfigDriftList`. Best practice for rolling deploys:

1. Deploy the first new pod with `--force-update-actor-config` (or `TASKQ_FORCE_UPDATE_ACTOR_CONFIG=true`). This overwrites the stored config and logs the change at ERROR with `force=true`.
2. Deploy all remaining pods without the flag. By the time they start, the stored config already matches the new registration, so no drift is detected.

Do not leave `--force-update-actor-config` set permanently. It allows any future config drift to be silently overwritten, removing the startup guard that protects against accidental actor-config changes.

---

## Workgroup supervisor

The workgroup supervisor (`taskq workgroup start`) manages multiple worker processes with per-worker configuration from a TOML file. It handles child health checks, crash restart, and graceful shutdown propagation.

### Configuration

```toml
# workgroup.toml
actors = "myapp.actors:registry"

[defaults]
poll_interval = 1.0
max_concurrency = 4

[[workers]]
name = "api"
queues = ["default"]
max_concurrency = 8
poll_interval = 0.5

[workers.health]
enabled = true
check_interval = 15
stale_after = 60

[[workers]]
name = "media"
queues = ["media"]
max_concurrency = 2
```

### Starting

```shell
taskq workgroup start workgroup.toml
```

### Key behaviours

- Each child process is launched as `taskq worker` with per-worker CLI args derived from the TOML spec.
- The supervisor assigns a `workgroup_instance` UUIDv7 to correlate child workers in the `workers` table.
- **Health checks** (when `health.enabled = true`) poll the `workers.last_seen_at` column for the child's registered PID. Stale workers are SIGKILL'd.
- **Crash restart** with burst-limiting: the supervisor tracks restart counts and throttles rapid restarts.
- **Graceful shutdown:** sends SIGTERM to all children on supervisor shutdown. Children that don't exit within a timeout are SIGKILL'd.
- The supervisor label (`worker_label`) and workgroup instance (`workgroup_instance`) are stored in WorkerSettings and the `workers` table for cross-process correlation.

---

## PgBouncer compatibility

The worker opens three asyncpg connection pools and two dedicated connections. Each targets a specific DSN for correctness reasons:

| Connection | DSN used | Why |
|---|---|---|
| `dispatcher_pool` | `pg_dsn_direct` | Shares infrastructure with session-mode connections; direct connection avoids transaction-mode complications |
| `heartbeat_pool` | `pg_dsn_direct` | Same rationale as dispatcher_pool |
| `worker_pool` | `pg_dsn_pooled` | Terminal writes use short transactions; transaction-mode PgBouncer is safe here |
| `notify_conn` | `pg_dsn_direct` | LISTEN state is session-scoped; a transaction-mode pool would drop the subscription between transactions |
| `leader_conn` | `pg_dsn_direct` | `pg_try_advisory_lock` produces a session-scoped lock; a transaction-mode pool would release the lock between transactions, allowing another worker to win the lock silently |

**Configuration:**

```shell
TASKQ_PG_DSN_DIRECT=postgresql://user:pass@pg-primary:5432/mydb
TASKQ_PG_DSN_POOLED=postgresql://user:pass@pgbouncer:5432/mydb
```

When neither is set, both fall back to `TASKQ_PG_DSN`. When both DSNs are the same (no PgBouncer), the worker operates identically.

If a LOOP-scope `asyncpg.Connection` provider is registered but the two DSNs differ, the worker emits a `loop_scope_conn_dsn_mismatch` warning at startup. PgBouncer in transaction mode breaks session semantics required by LOOP-scope connection providers. Either set both DSNs to the same direct endpoint for workers that use LOOP-scope connections, or omit the LOOP-scope connection provider and use the autonomous commit path.

---

## WorkerSettings reference

All variables use the `TASKQ_` prefix. `WorkerSettings` extends `TaskQSettings`; variables from `TaskQSettings` are marked with a dagger (†).

| Env var | Type | Default | Description |
|---|---|---|---|
| `TASKQ_PG_DSN` † | `PostgresDsn` | `postgresql://taskq:taskq@localhost:5432/taskq` | Direct DSN, used as fallback for both split DSNs |
| `TASKQ_SCHEMA_NAME` † | `str` | `taskq` | Postgres schema for all TaskQ tables |
| `TASKQ_REDIS_URL` † | `RedisDsn \| None` | `None` | Redis URL; required for rate-limiting and real-time progress |
| `TASKQ_ENVIRONMENT` † | `str \| None` | `None` | Deployment environment label |
| `TASKQ_ADMIN_HOST` † | `str` | `0.0.0.0` | Bind address for `taskq ui serve` |
| `TASKQ_ADMIN_PORT` † | `int` | `8080` | Bind port for `taskq ui serve` |
| `TASKQ_ADMIN_URL` † | `str` | `http://localhost:8080` | Public base URL of the admin UI |
| `TASKQ_PG_DSN_DIRECT` | `PostgresDsn \| None` | `None` (falls back to `TASKQ_PG_DSN`) | Direct Postgres DSN; bypasses PgBouncer |
| `TASKQ_PG_DSN_POOLED` | `PostgresDsn \| None` | `None` (falls back to `TASKQ_PG_DSN`) | Pooled DSN; may route through PgBouncer |
| `TASKQ_DISPATCHER_POOL_SIZE` | `int` | `4` | Max connections in `dispatcher_pool` |
| `TASKQ_HEARTBEAT_POOL_SIZE` | `int` | `4` | Max connections in `heartbeat_pool` |
| `TASKQ_MAX_CONCURRENCY` | `int` | `8` | Max concurrent jobs; `worker_pool_size = int(max_concurrency * 1.5)` |
| `TASKQ_HEARTBEAT_INTERVAL` | `float` | `10.0` | Seconds between heartbeat ticks |
| `TASKQ_LOCK_LEASE` | `float` | `60.0` | Seconds before a lock is reclaimed; must be `>= 4 * heartbeat_interval` |
| `TASKQ_MAX_HEARTBEAT_FAILURES` | `int` | `3` | Consecutive heartbeat failures before `isolate_self` |
| `TASKQ_TERMINATION_GRACE_PERIOD` | `float` | `60.0` | Total seconds from SIGTERM to forced exit |
| `TASKQ_CANCELLATION_GRACE_PERIOD` | `float` | `30.0` | Seconds for cooperative cancel phase |
| `TASKQ_CLEANUP_GRACE_PERIOD` | `float` | `10.0` | Seconds for force-cancel cleanup phase |
| `TASKQ_MAX_RETRY_BACKOFF` | `timedelta` | `PT24H` | Global ceiling on per-attempt retry backoff |
| `TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED` | `bool` | `True` | Fall back to Postgres when Redis errors occur during rate limiting |
| `TASKQ_HEALTH_ENABLED` | `bool` | `True` | Enable the Unix-socket health server |
| `TASKQ_HEALTH_SOCKET_PATH` | `str` | `/tmp/taskq_health.sock` | Path for the health Unix socket |
| `TASKQ_HEALTH_PG_PING_TIMEOUT` | `float` | `0.2` | Seconds to wait for the readiness PG ping |
| `TASKQ_POLL_INTERVAL` | `float` | `1.0` | Fallback producer polling cadence (seconds) when NOTIFY is disabled |
| `TASKQ_NOTIFY_ENABLED` | `bool` | `true` | When `true`, the worker uses LISTEN/NOTIFY for near-zero-latency dispatch wakeups. When `false`, uses poll-only dispatch with `poll_interval`. |
| `TASKQ_NOTIFY_POLL_INTERVAL` | `float` | `5.0` | Fallback poll cadence when NOTIFY is enabled (rarely reached — NOTIFY handles the common case). Uses `poll_interval` when NOTIFY is disabled. |
| `TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL` | `float` | `5.0` | How often the NOTIFY listener health-checks its connection |
| `TASKQ_NOTIFY_RECONNECT_BACKOFF_INITIAL` | `float` | `1.0` | Initial backoff before first NOTIFY reconnect attempt (doubles per attempt, capped at 30s) |
| `TASKQ_QUEUES` | `list[str]` | `["default"]` | Queue names this worker consumes; comma-separated |
| `TASKQ_POOL_MAX_INACTIVE_LIFETIME` | `float` | `300.0` | Seconds before an idle pool connection is closed |
| `TASKQ_WORKER_LABEL` | `str \| None` | `None` | Human-readable label for this worker, stored in `workers.worker_label` |
| `TASKQ_WORKGROUP_INSTANCE` | `str \| None` | `None` | UUIDv7 identifying the workgroup orchestrator that launched this worker |
| `TASKQ_OTEL_ENABLED` | `bool` | `True` | Enable OTel span and metric creation |
| `TASKQ_WORKER_GROUP` | `str` | `default` | Consumer group name on CONSUMER spans |
| `TASKQ_LOG_FORMAT` | `str` | `json` | `json` or `console` |
| `TASKQ_LOG_LEVEL` | `str` | `INFO` | Root logger level |
| `TASKQ_FORCE_UPDATE_ACTOR_CONFIG` | `bool` | `False` | Overwrite drifted actor-config rows without raising; see [ActorConfig sync](#actorconfig-sync) |
| `TASKQ_PRUNE_SCHEDULE_UTC` | `str` | `03:00` | Daily fire time for the prune sweep (Sweep 5) in `HH:MM` UTC. Ignored when `TASKQ_PRUNE_CRON_EXPR` is set. |
| `TASKQ_PRUNE_CRON_EXPR` | `str \| None` | `None` | Full 5-field cron for the prune sweep; overrides `TASKQ_PRUNE_SCHEDULE_UTC`. |
| `TASKQ_PRUNE_BATCH_SIZE` | `int` | `10000` | Rows per prune CTE batch. |
| `TASKQ_PRUNE_RETENTION_PERIOD` | `timedelta` | `30d` | Global fallback retention in `jobs` before archival. |
| `TASKQ_PRUNE_RETENTION_SUCCEEDED` | `timedelta` | `30d` | Per-status retention for `succeeded` jobs. |
| `TASKQ_PRUNE_RETENTION_FAILED` | `timedelta` | `90d` | Per-status retention for `failed` jobs. |
| `TASKQ_PRUNE_RETENTION_CANCELLED` | `timedelta` | `30d` | Per-status retention for `cancelled` jobs. |
| `TASKQ_PRUNE_RETENTION_ABANDONED` | `timedelta` | `90d` | Per-status retention for `abandoned` and `crashed` jobs. |
| `TASKQ_ARCHIVE_RETENTION_PERIOD` | `timedelta` | `365d` | How long a row stays in `jobs_archive` before hard-deletion by Sweep 6. |
| `TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC` | `str` | `04:00` | Daily fire time for the archive expiry sweep (Sweep 6) in `HH:MM` UTC. |
| `TASKQ_ARCHIVE_EXPIRY_CRON_EXPR` | `str \| None` | `None` | Full 5-field cron for the archive expiry sweep; overrides `TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC`. |
