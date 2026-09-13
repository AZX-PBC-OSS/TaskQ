# Operations & Adoption

This is the page to read when you have been told to move your workers to TaskQ and want to know
the footguns, the patterns to follow, and the antipatterns to avoid — before your first incident.
It compresses the operational content of the whole guide set into one pass: what each knob actually
does, how the sizing math works, how to shape fan-out jobs, and how to classify failures.

Mechanics live in the canonical guides (linked throughout); this page is about **decisions**.

!!! tip "Read order for a new adopter"
    1. This page, top to bottom.
    2. [retries.md — `start_to_close` vs `schedule_to_close`](retries.md#7-start_to_close-vs-schedule_to_close)
    3. [deployment.md — Production Checklist](deployment.md#production-checklist)
    4. [observability.md — setup](observability.md#1-opentelemetry-setup)

!!! warning "Know which TaskQ you are actually running"
    The published PyPI release can be far behind `main`: everything under `[Unreleased]` in the
    [CHANGELOG](../changelog.md) — keyed rate-limit refs, `cancel_where`, enqueue tags,
    `actor-config` live tuning, the PG rate-limit backend on the dispatch path, dozens of
    operational fixes — exists only in git pins. A `taskq-py>=0.2.2` range can silently resolve
    to the stale release, which carries known hazards the current docs no longer describe (for
    example: `start_to_close` failing to cancel the actor body, `unique_for` races, a
    payload-defect retry loop, and JSON results reviving UUID-shaped strings into `UUID`
    objects). If you pin a git rev, assert the installed distribution really resolves to it
    (a three-line test against `importlib.metadata`); if you use the release, read the docs
    *for that release*, not `main`.

---

## Contents

1. [The operating model in five minutes](#1-the-operating-model-in-five-minutes)
2. [Timeouts: `start_to_close` and `schedule_to_close`](#2-timeouts-start_to_close-and-schedule_to_close)
3. [Concurrency: process, actor, queue, fleet](#3-concurrency-process-actor-queue-fleet)
4. [Sizing: workers and Postgres connections](#4-sizing-workers-and-postgres-connections)
5. [Fan-out at scale: chunks, cursors, idempotency](#5-fan-out-at-scale-chunks-cursors-idempotency)
6. [Classifying failures: terminal, retryable, transient](#6-classifying-failures-terminal-retryable-transient)
7. [Waiting politely: rate limits, `Snooze`, `RetryAfter`, `Retry-After`](#7-waiting-politely-rate-limits-snooze-retryafter-retry-after)
8. [Observability and alerting](#8-observability-and-alerting)
9. [Migrating from Celery](#9-migrating-from-celery)
10. [The footgun index](#10-the-footgun-index)
11. [Adoption checklist](#11-adoption-checklist)

---

## 1. The operating model in five minutes

**A worker is one asyncio process.** It runs a producer task, exactly `max_concurrency` consumer
coroutines, a heartbeat loop, a LISTEN/NOTIFY connection, and a leader-election loop. Scaling is
*processes*, not threads — `FOR UPDATE SKIP LOCKED` makes concurrent workers safe against the same
tables. See [workers.md](workers.md).

**Queues route work.** Every job sits in a queue; a worker only dispatches from the queues in its
`TASKQ_QUEUES`. Partition fleets by queue to keep a deep backlog on one workload from starving
another. See [workers.md — Queue selection](workers.md#queue-selection) — and note that a queue
with a deep `strict_fifo` backlog starves *everything behind it*, which is a
[priority problem](#starvation-priority-and-fairness), not only a partitioning problem.

**One leader per fleet.** An advisory lock elects one worker to run the maintenance loops: sweeps
(crash reclaim, deadline enforcement), scheduled-job promotion, cron. If the leader dies, another
worker takes over at the next election. Nothing needs a dedicated process.

**The database clock is the only arbiter.** Every lease, deadline, and `scheduled_at` comparison
uses Postgres `clock_timestamp()`. Worker clocks never decide anything — which is why absolute
per-call `schedule_to_close` datetimes are deprecated in favour of interval budgets.

**Delivery is at-least-once.** A worker that dies mid-job has its jobs reclaimed after the lock
lease expires and retried (or marked `crashed` if the budget is gone). Design actors to be
idempotent; use the dedup tools in [§5](#5-fan-out-at-scale-chunks-cursors-idempotency).

**`worker_group` is a label, not a routing key.** It is emitted as the OTel
`messaging.consumer.group.name` span attribute and nothing else. It does not partition queues and
carries no concurrency semantics. A "workgroup" in the deployment sense is the *workgroup
supervisor* — a process orchestrator that spawns N child workers from one TOML file
([workgroups.md](workgroups.md)). Note for managed-identity deployments: the stock supervisor
spawns plain `taskq worker` subprocesses and cannot carry a `--pg-credential-provider` flag, so
dual-auth-mode (DSN *or* Entra) deployments run their own supervisor — see
[managed-identities.md](managed-identities.md).

---

## 2. Timeouts: `start_to_close` and `schedule_to_close`

The two names sound alike and bound different things. Full mechanics:
[retries.md §7](retries.md#7-start_to_close-vs-schedule_to_close).

| | `start_to_close` | `schedule_to_close` |
|---|---|---|
| Bounds | **One attempt's** wall-clock execution | The job's **entire lifetime**, all attempts |
| Type | `timedelta` (interval) | interval via actor `retry.time_budget` |
| Enforced by | `asyncio.wait_for` around the actor call | SQL guards at dispatch/retry/snooze + leader sweep |
| When it fires | Attempt is cancelled, recorded as `TimeoutError`, classified by the retry policy like any failure | Job fails permanently with `error_class="DeadlineExceeded"` — but **never kills a running attempt** |
| Default | **Unbounded** | **Unset** (only `kind="indefinite"` actors get one, via `time_budget`) |

### Decide per workload class

| Workload | `start_to_close` | `schedule_to_close` |
|---|---|---|
| Webhook / API call | 30–60 s | `RetryPolicy(kind="transient", max_attempts=5)` — no deadline needed |
| ETL chunk (see [§5](#5-fan-out-at-scale-chunks-cursors-idempotency)) | 5–15 min | unset, or a generous `time_budget` (requires `kind="indefinite"` — `time_budget` is inert on any other kind) |
| Poll-until-ready (invoice, export) | short | unset — the actor [`Snooze`](#7-waiting-politely-rate-limits-snooze-retryafter-retry-after) loop owns timing |
| Batch finalizer | minutes | size generously — see the finalizer warning in [§5](#5-fan-out-at-scale-chunks-cursors-idempotency) |

Set a fleet-wide safety net and override per actor:

```bash
# Every actor on this worker gets a per-attempt ceiling unless it declares its own.
export TASKQ_DEFAULT_START_TO_CLOSE=5m
```

```python
@actor(start_to_close=timedelta(hours=1))  # opts this actor up
async def reindex_bucket(payload: Payload) -> None: ...
```

### Gotchas

- **There is no opt-*out*.** The precedence chain is per-enqueue > `@actor(...)` >
  `TASKQ_DEFAULT_START_TO_CLOSE`. An actor that needs to run longer than the worker default must
  declare its own larger `start_to_close`; declaring `None` falls through to the worker default.
  Cron fires and admin "run now" currently do not propagate per-actor `start_to_close`
  (or `schedule_to_close_interval`) — only the fleet-wide `TASKQ_DEFAULT_START_TO_CLOSE`
  protects those paths.
- **A timed-out attempt consumes budget.** `attempt` is incremented at dispatch, so a
  `start_to_close` timeout counts against `max_attempts` exactly like a raised exception.
- **Sync actors are not stopped by `start_to_close`.** `def` actors run via `asyncio.to_thread`;
  the timeout cancels the *await*, marks the job, and frees the slot — but the thread keeps
  running to completion, including its side effects. Poll `ctx.should_abort()` in sync actors
  ([actors.md — Sync actors](actors.md#sync-actors)). The same applies to subprocess-spawning
  actors: a `SIGKILL`'d or force-exited worker leaves the child running — reap your own children.
- **`schedule_to_close` never kills a running attempt.** It gates *future* dispatches: expired
  pending jobs are never dispatched, retries/snoozes that would land past the deadline fail with
  `DeadlineExceeded`, and a leader sweep fails expired queued jobs. A running attempt that finishes
  after the deadline still succeeds.
- **The enqueue-time `heartbeat_timeout` parameter is currently inert.** It is stored on the
  job row (and reaches the job-detail template context) but is not rendered in the admin UI,
  and no code path reads it. All running jobs are leased for the global
  `TASKQ_LOCK_LEASE` (default 60 s).
- **The stored `error_message` of a genuine `start_to_close` timeout is the literal string
  `"start_to_close"`** — alert on `error_class == "TimeoutError"` if you want all timeouts, and
  remember an actor raising its own `TimeoutError` is indistinguishable by class.

### How a worker death is recovered

`lock_lease` (60 s default) is the visibility timeout. The leader's sweep runs every
`sweep_interval` (30 s default), so worst-case reclaim is ~90 s, then the job retries with a 5 s
backoff — or lands `crashed` if attempts are exhausted. Invariants that keep this safe:
`lock_lease >= 4 * heartbeat_interval`, and the watchdog kills a stalled loop *before* its leases
expire. Don't lower `TASKQ_LOCK_LEASE` without re-checking both
([configuration.md — Validation Constraints](configuration.md#validation-constraints)).

---

## 3. Concurrency: process, actor, queue, fleet

Five different knobs are called "concurrency". Picking the wrong one is the most common
misconfiguration:

| Knob | Scope | Semantics |
|---|---|---|
| `TASKQ_MAX_CONCURRENCY` | per worker **process** | Hard bound on simultaneously *executing* jobs (default 8). One consumer coroutine each. |
| `@actor(max_concurrent=N)` | per actor, **fleet-wide** | **Best-effort admission damper, not a hard cap** — see below. Operator-tunable live. `0` = drain mode: no jobs of this actor dispatch, silently. |
| `taskq queues set-max-concurrent` | per **queue**, fleet-wide | **Strict** leased-slot cap. Read at worker startup — needs a restart to change. |
| `ConcurrencyReservation` | per **bucket**, fleet-wide, cross-actor | **Strict** leased slots in Postgres, declarable inline on the actors — the only strict mechanism that can serialize two *different* actors (e.g. against one thread-unsafe library). See [rate-limiting.md](rate-limiting.md#concurrencyreservation). |
| `@actor(singleton=True)` | per actor, fleet-wide | At most one *active* job (pending/scheduled/running). Second enqueue raises `SingletonCollisionError`. |

!!! warning "`max_concurrent` is not a hard cap"
    Dispatch reads the running-count once per round before locking rows. Two replicas dispatching
    concurrently each admit up to the cap; the over-dispatch bound is
    `(num_producers - 1) * max_concurrent` per round, and those jobs genuinely run. For
    memory- or GPU-bound actors use a strict leased-slot cap (per-queue or
    `ConcurrencyReservation`) instead. Full worked warning:
    [deployment.md — `max_concurrent` and `max_pending`](deployment.md#max_concurrent-and-max_pending).

!!! warning "A reservation's lease must exceed the actor's worst-case runtime"
    Reservation slots are leased; a slot whose lease expired is acquirable by the next job. If an
    actor can run longer than its bucket's `lease`, two of them will hold one slot. Size the lease
    above your p-max runtime, not your p99.

### Capacity ownership: the seed-only trap

The `@actor(max_concurrent=...)` literal only *seeds* the `actor_config` row on first
registration. Two production-shaped consequences:

- **Stored rows win.** Once a row exists, code changes to `max_concurrent` are ignored (logged as
  `actor-config-capacity-override`). Tune live without restarts:
- **A stored `NULL` means *uncapped* — and pre-existing rows can null your literals.** A schema
  first populated before an actor grew a capacity literal keeps `max_concurrent` NULL forever:
  every decorator literal in the repo is dead config, nothing fails, and the fleet runs uncapped.
  This has happened across whole fleets (rows seeded by an early deploy, literals added later,
  zero signal). Audit with:

```bash
taskq actor-config list    # treat any NULL capacity as a decision you never made
taskq actor-config set my_actor --max-concurrent 10
```

The exception: **`max_pending`** — a NULL stored value falls back to the code literal, so its
declarations work on deploy. (`max_pending` rejects enqueues past the queued depth with
`MaxPendingExceededError`, surfaced as `taskq.backpressure.errors`.)

Two viable ownership postures — pick one deliberately:

1. **Operator-owned** (TaskQ's default): literals are seeds; operators tune with the CLI; drift
   is logged. Requires someone to watch the log line.
2. **Boot-time convergence**: your startup code writes the literals into `actor_config` on every
   boot, making code the source of truth. An operator's live tune then reverts on the next deploy —
   document that.

Structural fields are different: changing an actor's `queue` or adding `metadata` after a row
exists **refuses to boot** (`ActorConfigDriftList`) — move an actor between queues in two steps
(deregister/update, then boot), or set `TASKQ_FORCE_UPDATE_ACTOR_CONFIG=true` for exactly one
boot. Never leave it set. See [workers.md — ActorConfig sync](workers.md#actorconfig-sync).

### Starvation: priority and fairness

A deep backlog on one queue starves everything queued behind it — including small, urgent jobs —
no matter how much concurrency you add. "No output from family X" with family X jobs sitting
`pending` is *head-of-line blocking*, and it is fixed with ordering, not capacity:

- **`@actor(priority=N)` / `enqueue(priority=N)`** — higher priority dispatches first. Priority is
  stamped **at enqueue time**: a standing backlog keeps whatever priority it was enqueued with, so
  after an incident you may need to bulk-`UPDATE` the backlog's priority (or cancel and re-enqueue)
  to promote the victim family.
- **`round_robin` queue mode + `fairness_key`** — interleaves dispatch across `fairness_key`
  cohorts (typically tenant or entity ids) so one tenant's 10,000-job sync cannot starve the
  others. Two traps: the mode is **not the default** and nothing in TaskQ sets it for you — run
  `taskq queues set-mode <queue> round_robin` once or seed the `queues` row in your bootstrap
  (otherwise `fairness_key` is a column TaskQ writes and never reads); and jobs enqueued *without*
  a `fairness_key` all collapse into one `__null__` cohort where round-robin degenerates to plain
  FIFO — nothing warns you. See [workers.md — Queue dispatch modes](workers.md#queue-dispatch-modes).

### Thread-unsafe native libraries

A `def` actor dispatched through `asyncio.to_thread` runs on a real OS thread pool — several jobs
in one process can drive one native library from several threads at once. Libraries that are not
thread-safe (PDF rendering toolkits are a classic) will segfault the interpreter, kill every
in-flight job in the process, and corrupt output. The fix is not a number in a config file — it
is a strict `ConcurrencyReservation` with one slot shared by every actor that touches the
library, across queues and worker processes. "Any safety property expressed as a number in a
config file is one out-of-band edit away from being lost."

**Sync actors are bounded by the thread pool, not by `max_concurrency` alone.** A fleet of
blocking `def` actors is limited by CPython's default executor (`min(32, cpus + 4)` threads per
process) — and each blocked thread still holds its job slot. Prefer async actors; for CPU work use
a dedicated low-`max_concurrency` worker pool.

---

## 4. Sizing: workers and Postgres connections

### Throughput math

Each worker process sustains, for one queue mix:

```
jobs_per_second ≈ max_concurrency / mean_job_seconds
```

I/O-bound actors multiplex cheaply (16–64 per process); CPU-bound actors block the event loop
(2–4 per core). The workload table is in
[deployment.md — Concurrency tuning](deployment.md#concurrency-tuning). Scale horizontally by
adding processes; the fleet-wide caps from [§3](#3-concurrency-process-actor-queue-fleet) still
apply. Remember your own app-side pools: an actor that opens a DB session per job needs its pool
sized to at least `max_concurrency` too — that counts against the same `max_connections`.

### The connection budget

A worker holds **three pools plus dedicated connections**:

| Resource | Count (default) | DSN | Notes |
|---|---|---|---|
| `dispatcher_pool` | 4 | **direct** | dispatch claims, sweeps, leader loops |
| `heartbeat_pool` | 4 | **direct** | heartbeat transaction every 10 s |
| `worker_pool` | `int(max_concurrency * 1.5)` | **pooled** (may traverse PgBouncer) | terminal writes, sub-enqueues, PG-backed rate limits |
| `notify_conn` | 1 | **direct** | LISTEN (session-scoped — cannot pool) |
| `leader_conn` | 1 | **direct** | advisory-lock election (every worker) |
| leader-only monitor + cron | +2 | **direct** | only on the elected leader |

- Idle floor: pools open with `min_size=1`, so an idle worker holds **5 sessions (7 as leader)**,
  growing under load to the maxima.
- Default worker (`max_concurrency=8`): 10 direct + 12 pooled = **22 max**; 24 on the leader.
- Per-`TaskQ` client (web/API pod): one pool, ≤ **5** connections.
- Workgroup supervisor with child health checks: `health_workers + 1` extra.

Fleet formula:

```
direct  = M * (dispatcher_pool_size + heartbeat_pool_size + 2) + 2   # +2 = the leader's extras
pooled  = M * int(max_concurrency * 1.5)
total   = direct + pooled + web_pods * 5 (+ supervisor health pool) + your app's own pools
```

Worked example — 10 workers at `max_concurrency=16`, defaults elsewhere:

```
direct = 10 * (4 + 4 + 2) + 2 = 102
pooled = 10 * 24              = 240
total  = 342  + your application's own connections
```

Check this against Postgres `max_connections` **including the app's pools**, and **including the
rolling-deploy doubling**: orchestrators (Kubernetes rolling updates, Azure Container Apps
revisions) bring the new pods up before draining the old ones, so your fleet briefly runs at
~2× steady state. Omitting that term is the single most common way this budget gets
underestimated.

The worker logs its own budget at startup (`worker-startup-budget`); the same arithmetic is
`taskq.worker.budget.compute_connection_budget()`, which flags `pgbouncer_recommended` when the
total exceeds 80 (conservative below the Postgres default `max_connections=100`). **Small SKUs
break that assumption**: a 50-connection managed Postgres with ~15 reserved has ~35 usable and
often cannot run PgBouncer at all — two default workers (44–48 connections) already exceed it.
On small SKUs, lower `TASKQ_MAX_CONCURRENCY` (the derived `worker_pool` shrinks with it) until
`M × budget + app pools + rollout doubling` fits.

!!! warning "PgBouncer: pooled DSN only"
    Only `worker_pool` may route through transaction-mode PgBouncer (`TASKQ_PG_DSN_POOLED`).
    `LISTEN`, advisory locks, and the sweeps require direct sessions
    (`TASKQ_PG_DSN_DIRECT`). Splitting the two DSNs is the supported pattern — see
    [configuration.md](configuration.md) and
    [workers.md — PgBouncer compatibility](workers.md#pgbouncer-compatibility).

### Bring-your-own pools change the arithmetic

If you pass your own `PoolFactory` through `WorkerConnections`
([managed-identities.md](managed-identities.md)), TaskQ invokes it **once per role** —
dispatcher, heartbeat, and worker pools become **three distinct full-sized pools**, not one
shared pool. Size and count accordingly.

!!! danger "Never hand TaskQ a pool your request handlers already use"
    asyncpg's `Pool.acquire()` has no default timeout. A request that holds one connection from a
    shared pool and then enqueues (which needs a *second* connection from the same pool) will, at
    `pool_max` concurrent requests, wait forever — a silent, permanent deadlock of the whole
    process, with green health probes. Give the `TaskQ` client its own small pool (≤5), always.

### Managed identities and token rotation

With token-based auth (Entra ID/AAD, IAM, Vault), asyncpg pools keep the password they were built
with: a worker that never rebuilds its pools looks perfectly healthy for the first hour and then
cannot open a new connection. Size `TASKQ_RELOAD_INTERVAL` inside your token lifetime with margin
(a common default is 40 minutes against ~60-minute tokens), or `SIGHUP` to force a reload.
Cover **all five connection roles** (dispatcher/heartbeat/worker pools, notify, leader) in your
factories — a partial override yields a worker that starts, dispatches nothing, and never logs
`leader-elected`. Full reference: [managed-identities.md](managed-identities.md).

---

## 5. Fan-out at scale: chunks, cursors, idempotency

### The antipattern: one giant job

A "sync everything" job that runs for five hours:

- occupies a concurrency slot for the whole run and **bottlenecks every pipeline behind it** —
  nothing downstream proceeds until *all* the work succeeds;
- gets killed by `start_to_close` if you set one (and if you didn't, a hang holds the slot
  forever), then **retried into the same death** until `max_attempts` or `schedule_to_close`
  gives up;
- makes a resync repeat the entire five hours — the processing never proceeds.

Batch-shaped work must be **chunked**. Three supported shapes:

For recurring passes over a population — the cron-triggered sweep — [sweeps.md](sweeps.md) develops
Pattern A into a full prescription: why the cron period must not become the throughput ceiling, the
keyset-vs-`OFFSET` drift mechanism, the cursor-column rule, and the root/successor/per-item
idempotency split that keeps a self-enqueuing chain alive.

### Pattern A — cursor chain (recommended default)

Process one page per job; each job enqueues the next page. Progress is durable, per-chunk
retries are cheap, downstream consumers start as soon as the *first* chunk lands, and the queue
drain rate is bounded by your concurrency/rate-limit settings instead of one job's runtime.

```python
from datetime import timedelta

from pydantic import BaseModel

from taskq import JobContext, actor


class RowPayload(BaseModel):
    run_id: str
    row: dict  # whatever `fetch_page` returns per row


class SyncPagePayload(BaseModel):
    run_id: str
    cursor: int  # your own pagination cursor — TaskQ has none to pass for you


async def fetch_page(cursor: int, limit: int) -> tuple[list[dict], int | None]: ...


@actor(start_to_close=timedelta(minutes=10))
async def sync_page(payload: SyncPagePayload, ctx: JobContext[SyncPagePayload]) -> None:
    rows, next_cursor = await fetch_page(payload.cursor, limit=500)
    for row in rows:
        await ctx.jobs.enqueue(
            process_row,
            RowPayload(run_id=payload.run_id, row=row),
            # Stable per-row key: without this a parent retry re-enqueues every
            # child (autonomous sub-enqueues commit immediately and do not roll
            # back with the parent).
            idempotency_key=f"row:{payload.run_id}:{row['id']}",
        )
    if next_cursor is not None:
        # Stable key: re-enqueueing this page after any failure dedups instead of duplicating.
        await ctx.jobs.enqueue(
            sync_page,
            SyncPagePayload(run_id=payload.run_id, cursor=next_cursor),
            idempotency_key=f"sync:{payload.run_id}:{next_cursor}",
        )
```

Why the pieces matter:

- **The cursor lives in the payload.** TaskQ's `cursor` type is keyset pagination for *read APIs*
  (`client.list(...)`) — there is no built-in job-continuation cursor. Your cursor is your data.
- **The idempotency keys make re-dispatch safe.** Both the per-row enqueues and the
  successor page carry stable keys. If the parent is retried after enqueuing children,
  the duplicates are dropped and the existing jobs returned (`was_existing=True`) — the
  chain cannot fork and rows are not processed twice.
- **Transactional vs autonomous sub-enqueue — know which one you are on.** Two modes, decided by
  your DI setup, not by the call:
    - *Autonomous* (the default worker): no LOOP-scope `asyncpg.Connection` is registered, so
      `ctx.jobs.enqueue` commits each child **immediately** through the worker pool. A parent
      failure does **not** roll back its children — the startup log warns
      `sub_enqueue_autonomous_fallback`. The idempotency keys are your correctness backstop (a
      re-run parent re-enqueues the same children, which dedup).
    - *Transactional*: register a LOOP-scope `asyncpg.Connection` and children join the parent's
      transaction (rollback on failure). **But the loop connection is shared by every consumer
      slot — the transactional path is only correct on a single-slot worker
      (`TASKQ_MAX_CONCURRENCY=1`)**; concurrent jobs on one connection error out. Fleets that
      need both transactional sub-enqueue *and* throughput run a dedicated one-slot worker for
      the chaining actor.
- For a *self*-continuing poll loop (same job comes back), `raise Snooze(delay)` instead of
  enqueueing a successor — see [§7](#7-waiting-politely-rate-limits-snooze-retryafter-retry-after).

### Pattern B — fan-out batch + finalizer

Enqueue up to 1,000 children in one `enqueue_batch(...)` call (or stream chunks of 1,000) and
gate completion with a finalizer that snoozes until all children are terminal. From inside an
actor, pass an explicit `batch_id` and enqueue the finalizer as its own job:

```python
from uuid import NAMESPACE_URL, uuid5

from taskq.batch import EnqueueItem


items = [
    EnqueueItem(actor_ref=process_chunk, payload=p, idempotency_key=f"chunk:{run_id}:{i}")
    for i, p in enumerate(chunks)
]
# Deterministic id: a parent retry must reuse the same batch id. `new_uuid()`
# (UUIDv7) mints a fresh id per attempt, so deduped children keep attempt one's
# batch id while the finalizer points at an empty batch id — and
# `wait_for_batch` then returns `is_complete=True` on the wrong id. `uuid5`
# keeps the id stable across attempts; the finalizer's own idempotency key
# dedups the second enqueue (`was_existing=True`).
batch_id = uuid5(NAMESPACE_URL, f"taskq-batch:{run_id}")
await ctx.jobs.enqueue_batch(items, batch_id=batch_id)
await ctx.jobs.enqueue(
    finalize_run,
    FinalizePayload(batch_id=batch_id, expected=len(items)),
    idempotency_key=f"finalize:{run_id}",
)
```

!!! note "Always UUIDv7, never `uuid4()` — except retry-safe in-actor batch ids"
    TaskQ generates every internal id (jobs, workers, batches) as UUIDv7 — time-ordered, so
    B-tree index inserts stay local and ids sort by creation time. Use `new_uuid` (exported from
    `taskq`) for any id you choose yourself, such as an explicit `batch_id` — **unless** the
    batch is enqueued from inside an actor that may retry, in which case derive the id
    deterministically from the run (e.g. `uuid5`) as above so every attempt reuses it.

The `finalizer=` parameter exists on the *client's* `enqueue_batch` (`client.enqueue_batch(items,
finalizer=...)`); the in-actor `SubJobEnqueuer.enqueue_batch` takes only `batch_id`, so enqueue the
finalizer separately as above. The finalizer's `wait_for_batch(db, batch_id)` raises `Snooze` on a
10 s interval until the batch completes, then runs once. Worked example:
`examples/actors/batch.py` and
[jobs-clients.md — Batch finalizer](jobs-clients.md#batch-finalizer).

!!! warning "Size the finalizer's budget generously"
    A snooze whose `now + delay` would land past `schedule_to_close` fails the job with
    `DeadlineExceeded` instead of rescheduling. A finalizer that snoozes for hours needs either
    no deadline or (on a `kind="indefinite"` actor — `time_budget` is inert on any other kind)
    a `retry.time_budget` that comfortably exceeds the expected batch duration (plus retries).
    Note a default-kind (`transient`) finalizer that snoozes has neither a deadline nor an
    attempt ceiling — snooze bumps `max_attempts` instead of consuming it — so make it
    `indefinite` with a budget if you want it bounded.

### Pattern C — app-level run accounting + finalize sweep

When completion spans *generations* of jobs (a chunk enqueues chunks), a single `batch_id` cannot
cover the chain. Keep a durable counter on your own run/entity rows: increment at enqueue
(**gated on `was_existing`** — deduplicated enqueues must not double-count), decrement when a
job lands terminal (in the actor, or via `on_retry_exhausted`), and run a periodic sweep cron
that force-closes runs whose counter has been nonzero too long. This also catches wedged runs
that a pure finalizer would wait on forever.

### Choosing the enqueue API

| API | Size | Idempotency | Notes |
|---|---|---|---|
| `enqueue_batch` | ≤ 1,000 | per-item keys honored; collisions return existing jobs | single transaction; enforces `max_pending` |
| `enqueue_batch_streaming` | unbounded (chunks of ≤ 1,000) | per-item keys honored | generator input; **does not enforce `max_pending`** |
| `enqueue_batch_fast` | ≤ 50,000 | **none** — any duplicate key aborts the whole COPY | bulk-import semantics; returns a count only; **no `max_pending`** |

See [jobs-clients.md](jobs-clients.md) for the full tradeoff table.

### Choosing a dedup mechanism

| Mechanism | Semantics | Use when |
|---|---|---|
| `idempotency_key` (+ `idempotency_scope`) | **Exact.** DB unique index; duplicates return the existing job, no error | re-enqueue after failure/crash; exactly-once *scheduling* |
| `unique_for` + `identity_key` | **Windowed, best-effort.** Advisory-lock preflight; concurrent enqueues may both insert but only one runs | suppressing duplicate *triggers* inside a freshness window |
| `singleton=True` | One active job per actor | "never two of me at once" (mind the cron interaction below) |

**Idempotency-key discipline** — every rule below was learned from a production incident:

- **There is no TTL.** A key dedups against any row that still exists — the horizon is retention:
  **30 d for `succeeded` rows, 90 d for `failed` rows** by default. Need a time window? Encode it
  in the scope: `idempotency_scope=f"daily-sync:{date}"`.
- **Keys are status-blind.** A terminal-`failed` job's key absorbs every re-post onto the dead
  job — your post-fix rerun of a failed batch will silently no-op. Encode an attempt or window
  axis (`key=f"...:{attempt}"`, or bump a version in the key when the entity's state changes).
- **Keys must encode every distinguishing payload dimension.** TaskQ cannot see payload contents:
  a key that carries only one dimension of a composite identity collapses two *different* work
  items onto one job, and the second item's work silently never runs — work *loss*, not
  duplication.
- **No stable key on a self-continuation successor.** A continuation job carrying the same key as
  its (now-`succeeded`) predecessor collides with it and is silently dropped. Successors of the
  same actor need either a fresh key dimension (the next cursor) or no key.
- **Namespace client-supplied keys.** A raw user-supplied string as the whole key lets one caller
  squat another caller's dedup slot; mix the caller identity into the key.
- **Length cap: 1024 bytes** (`TASKQ_IDEMPOTENCY_KEY_MAX_BYTES`); oversize keys raise at enqueue.
  Hash over-cap values (keep legible keys below the cap).
- `unique_for` without `identity_key` at enqueue is a **silent no-op** (logged warning).
- `unique_for` on a thin root does not single-flight its fan-out: a root that finishes in seconds
  frees the window while its child chain still runs — a cron cadence faster than the chain will
  overlap runs. Single-flight the *work*, not the root (a running-run guard in your own state, or
  a strict reservation).
- `metadata.batch_id` is library-reserved and stripped from caller-supplied metadata; a
  caller-supplied `metadata.singleton=True` on an ordinary actor is **not** stripped — it
  survives onto the row and triggers real single-flight behaviour (enqueue preflight and the
  `jobs_singleton_uniq` index key on it).
- For fleet-wide completion *reactions* (rather than a finalizer), the durable-cursor pattern
  `TaskQ.watch_reclaims()` is documented in
  [architecture.md — which component drives each transition](../architecture.md#which-component-drives-each-transition).

**`tags`** are the group-operation tool: `enqueue(..., tags=[f"run-{run_id}"])` then
`JobFilter(tags=(...))` for status polling and bulk `cancel_where`. Tags must match
`\A\w(?:[\w\-]*\w)?\Z` — **no colons** — and invalid tags raise at enqueue, so a bad tag factory fails
every trigger, not just one.

### Cron and scheduled workloads

- **`scheduled_at` is timezone-aware only** (naive datetimes raise), and promotion runs on the
  leader's ~1 s tick — that's your timing precision.
- **Cron fires on schedule regardless of the previous fire.** A cron actor slower than its
  cadence piles up overlapping runs. Overlap suppression options, with their traps:
    - `singleton=True` on the actor: the still-active previous fire makes the next fire's INSERT
      raise — and **three consecutive fire failures auto-disable the schedule permanently**
      (`enabled=false`, `cron_auto_disable_threshold=3`). Only safe when the actor always
      finishes well inside the cadence and never snoozes (a snoozed job is `scheduled` = active).
    - `identity_key` on the schedule + `unique_for` on the actor: fires inside the window dedup
      silently (no strike). The window must cover the cadence, and it single-flights the *root*
      only (see the dedup rule above).
    - An app-level running-run guard is the robust option for chains that outrun their cadence.
- **Decorator registration is create-only.** `@cron(...)` creates the schedule row on first sight
  and never updates it — a changed expression, timezone, or a *removed* decorator has no effect
  on an existing row. Manage live schedules with `client.update_schedule` / `delete_schedule`
  (an empty expression does not disable a previously-registered schedule).
- **Auto-disable is permanent until re-enabled** — check `taskq.cron.disabled_schedules` and the
  `cron schedule auto-disabled` log line (`kind="cron_fire"`; `cron.auto_disabled` is only an
  OTel span event, not a log event); a silently-disabled schedule is an outage with one log line.
- Give cron actors a `start_to_close` — an unbounded cron actor that hangs holds its queue.
  Note cron fires currently do not propagate per-actor `start_to_close` (only the fleet-wide
  `TASKQ_DEFAULT_START_TO_CLOSE` applies), so the safety net above is what protects cron.

---

## 6. Classifying failures: terminal, retryable, transient

TaskQ has **no exception subclass that marks "permanent"**. You classify by configuration, in
this order (first match wins):

1. `non_retryable_exceptions=(MyError,)` on the actor — isinstance match, subclasses included.
2. Auto-fail classes: `PayloadValidationError`, `ResultTooLarge`, pydantic `ValidationError`
   (a re-run reproduces the same problem).
3. The actor's `RetryPolicy.kind`: `non_retryable` → fail; `transient` → retry while
   `attempt < max_attempts`; `indefinite` → retry until `schedule_to_close`.
4. Per-instance refinement: a `retry_classifier` hook returning `RetryOverride(kind=..., delay=...)`.

Doctrine — put each failure class where it belongs:

| Failure class | Classification | Why |
|---|---|---|
| Malformed payload, schema violations, bad arguments | terminal (`PayloadValidationError` is auto-terminal) | deterministic — retries reproduce it |
| Auth failures, permission denied, "unknown resource" for the **job's whole subject** | terminal via `non_retryable_exceptions` or classifier `kind="non_retryable"` | the condition will not heal |
| **Per-item** faults inside a fan-out (one deleted file in a 500-item page) | per-item fault, skip and continue — **not** a job failure | one unreadable item must never fail the whole page/run; record it and move on |
| Downstream 429 / `Retry-After` | transient with **the server's delay** — [`RetryAfter`](#7-waiting-politely-rate-limits-snooze-retryafter-retry-after) or classifier `RetryOverride(delay=...)` | retrying *now* makes it worse; retrying *then* succeeds |
| 5xx, timeouts, connection resets | `transient` with bounded `max_attempts` | healable, but bound the budget |
| "Down until Tuesday" (maintenance window) | `Snooze` (no budget consumed) or `kind="indefinite"` + `time_budget` | known-duration waits should not burn attempts |

**Size the retry window to span a routine provider blip.** `RetryPolicy(max_attempts=4,
base=timedelta(seconds=10))` covers only ~70 s of cumulative delay (10 + 20 + 40 jitter-free); an ordinary object-store 5xx
blip outlasts that, exhaustion is terminal, and (if your `on_retry_exhausted` marks entities
dead) the entity silently disappears from downstream surfaces. For network-bound actors, prefer
fewer attempts with a longer `base` over many fast ones.

**Multiplicative jitter does not decorrelate a synchronized cohort.** The default ±20% around a
5 s base re-clusters a fleet that failed together into a ~2 s window at T+5s — at large fan-out
that is a synchronized retry wave. Disperse cohorts with a longer `base` and higher `jitter`, or
shed load with `max_pending` before the wave forms.

!!! warning "Never catch a failure and return a result"
    An actor that catches the provider's 429/5xx and returns `(ok=False, retryable=True)` has
    **succeeded** as far as TaskQ is concerned: the job is `succeeded`, the retry policy, the
    classifier, the advertised `Retry-After`, and `on_retry_exhausted` never engage. Raise — or
    translate to your typed retryable exception and raise that.

There is also **no poison state and no dead-letter queue**. A job whose budget is exhausted lands
in terminal `failed` with `error_class` `MaxAttemptsExceeded` / `DeadlineExceeded` / your
exception's class, and stays queryable in `jobs` until retention prunes it. Routing to a real DLQ
is your job, at the two designed hook points: `on_retry_exhausted` (per actor) and the
`ErrorReporter` (per worker, e.g. Sentry forwarding). Note: `on_retry_exhausted` receives a bare
`JobRow` with no DI scope — hooks must build their own connections and cannot inject. See
[retries.md — `on_retry_exhausted`](retries.md#8-on_retry_exhausted-hook) and
[observability.md — Error reporting](observability.md#9-error-reporting-errorreporter-protocol).

Two more states worth naming because they mean *infrastructure*, not your code:

- **`crashed`** — the worker died (or lost its heartbeat) mid-attempt with the retry budget
  exhausted. Crash labels are `WorkerCrashed` (assumed gone) and `HeartbeatLost` (alive but
  partitioned).
- **`abandoned`** — only ever produced by *cancellation* escalation: a force-cancel whose cleanup
  did not finish within the grace periods. A timeout or exception never produces `abandoned`.

!!! note "Infra failures during the terminal write leave the job `running` — on purpose"
    If Postgres itself errors while recording the outcome, the row is left `running` and
    reclaimed by lease expiry (~90 s worst case with defaults) rather than misclassifying an
    infrastructure blip as your actor's failure. Don't be alarmed by a short-lived `running` row
    on a dead worker; the sweep owns it.

!!! warning "A routine SIGTERM drain sets the same cancel event an operator cancel does"
    `ctx.cancel_event` / `ctx.should_abort()` fire during *every* rolling deploy's drain phase —
    if your actor treats "cancelled" as "close the run as cancelled and return", the job records
    **`succeeded`** (it returned normally), no retry runs, and successor enqueues never happen:
    the chain dies silently on every deploy. Distinguish drain from operator intent with your own
    persisted state, and on cancel, either re-raise or arrange a successor — never return
    normally. See [cancellation.md](cancellation.md).

---

## 7. Waiting politely: rate limits, `Snooze`, `RetryAfter`, `Retry-After`

### Rate-limit denial is a snooze, not a failure

When an actor's rate limit or reservation denies admission, the actor body **never runs**: the
job is rescheduled at `now + retry_after` (computed by the limiter store in the DB/Redis clock
domain), the slot is freed, and **no retry budget is consumed**. The attempt row records
`rate_limit_denied` / `reservation_denied` and `metadata.awaiting` names the bucket. A denied job
waits in Postgres — this is not busy-spinning in the worker.

```python
from taskq.ratelimit import SlidingWindow, TokenBucket, registry

# Downstream allows 50 req/s fleet-wide (`capacity` is the burst allowance,
# `refill_per_second` is the sustained rate):
registry.register(
    TokenBucket(name="partner_api", capacity=50, refill_per_second=50.0, backend="redis")
)


@actor(rate_limits=["partner_api"])
async def call_partner(payload: Payload) -> Result: ...
```

Per-tenant / per-entity quotas use keyed refs — the bucket name is derived per payload key:

```python
from taskq.ratelimit import KeyedRateLimitRef


@actor(
    rate_limits=[
        KeyedRateLimitRef.typed(
            Payload,
            base_name="partner_per_tenant",
            key_fn=lambda p: p.tenant_id,
            capacity=10,
            refill_per_second=0.5,
            backend="redis",
        ),
    ]
)
async def call_partner_per_tenant(payload: Payload) -> Result: ...
```

**Scope matching:** `rate_limits` is per *actor*. A quota shared across *several* actors (one
provider, many callers) needs a single shared bucket referenced by all of them — the limit
registry is global, so register the bucket once and list it on every actor that spends it. This
is separate from your HTTP-gateway rate limiting (429s at the edge); TaskQ limits govern
*background* consumption.

Backends: `redis` (default for `TokenBucket`/`SlidingWindow`; requires `TASKQ_REDIS_URL` — the
worker **fails fast at startup** if a Redis-backed limiter is served without Redis), `postgres`
(needs only the standard TaskQ schema), or `memory` (per-process, tests only). On Redis
connection errors the limiter falls back to the PG implementation by default
(`TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED`). Full reference:
[rate-limiting.md](rate-limiting.md).

!!! warning "A drained fixed-quota bucket repolls every 5 s — forever"
    A `TokenBucket` with `refill_per_second=0` never refills and returns `retry_after=None`, so
    denials fall back to the 5 s default backoff. Without a `schedule_to_close`, such a job
    re-queues every 5 s indefinitely. Either give the bucket a refill rate, or give the actor a
    deadline (`retry.time_budget` requires `kind="indefinite"`).

!!! warning "Dev/prod Redis asymmetry"
    A common compose/dev setup sets `TASKQ_REDIS_URL` while production deliberately runs
    Postgres-only. A Redis-backed limiter added in dev then **refuses to start the production
    worker** — fail-fast at bootstrap, naming the limiters. Decide the backend per environment,
    not per habit.

### The three "come back later" signals

| Signal | Consumes budget | Reschedules at | Use for |
|---|---|---|---|
| `raise Snooze(delay)` | **No** — `max_attempts` is bumped to keep the invariant `attempt < max_attempts` | `now + delay` | waiting for a condition (batch completion, external state) |
| `raise RetryAfter(delay)` | **Yes** (default) — bounded by `max_attempts`, terminal `MaxAttemptsExceeded` when out | `now + delay` | a retry that should wait a *known* time (429s) |
| `raise RetryAfter(delay, consume_budget=False)` | No (snooze semantics) | `now + delay` | known-delay wait that must never exhaust the budget |

A classifier `RetryOverride(delay=...)` is the type-level variant — it applies to a whole
exception class and **is** clamped by `max_retry_backoff`; `RetryAfter`'s delay is *not* clamped
(only `schedule_to_close` gates it).

### Honoring `Retry-After` (and `x-retry-after`) headers

TaskQ does **not** automatically honor any HTTP header — there is no setting or integration that
does this. The supported pattern is to read the header yourself and translate it into a signal:

```python
@actor(retry=RetryPolicy(kind="transient", max_attempts=8))
async def call_api(payload: Payload) -> Result:
    resp = await http_post(payload.url)
    if resp.status == 429:
        # Standard header (seconds) or your gateway's x-retry-after — same translation.
        wait = float(resp.headers.get("Retry-After") or resp.headers.get("x-retry-after") or 60)
        raise RetryAfter(delay=timedelta(seconds=wait))
    if resp.status in (404, 410):
        raise UnknownResource(payload.url)  # listed in non_retryable_exceptions
    return parse(resp)
```

For per-status-code policy (429 → wait the header's delay, 404 → terminal, 5xx → bounded
transient), use the `retry_classifier` example in
[retries.md §5](retries.md#5-retry_classifier-hook-per-instance-retry-overrides) — it carries the
header value through `RetryOverride(delay=...)` and inherits the `max_retry_backoff` clamp, so a
malformed `Retry-After: 999999999` cannot strand the job.

### Scheduled re-enqueue as the "snooze until it can run" fallback

When the wait is longer than any job should hold a slot, don't snooze — enqueue a successor with
a `scheduled_at` (timezone-aware; promotion tick ~1 s) plus an `idempotency_key`, and return
success. The job queue is your timer, and dedup makes the pattern crash-safe.

---

## 8. Observability and alerting

### Wiring an exporter

TaskQ emits via the OpenTelemetry **API** and never overrides standard OTel vars; the SDK and
exporter are your boilerplate. Two supported shapes:

- **OTLP** (collector, Jaeger, Tempo, Datadog, Sentry, PostHog): set
  `OTEL_EXPORTER_OTLP_ENDPOINT` (`:4317` gRPC / `:4318` HTTP), `OTEL_SERVICE_NAME`,
  `OTEL_RESOURCE_ATTRIBUTES` — or initialize the SDK in-process
  (`examples/otel_setup.py`).
- **Vendor SDK in-process** (e.g. Azure Monitor / App Insights): call the vendor's
  `configure_azure_monitor(...)` **inside the worker process** before it starts.

!!! warning "An exporter env var set on the container does nothing by itself"
    `APPLICATIONINSIGHTS_CONNECTION_STRING` on a worker container exports nothing unless the
    worker process itself configures the exporter; with no exporter configured, OTel drops
    spans and metrics **silently** — health stays green while nothing is collected. Verify
    telemetry actually arrives (one trace, one metric series) before trusting the pipeline.

Every job emits an `enqueue` PRODUCER span, a `process` CONSUMER span (linked, with
`lifecycle.*` events), and an `attempt.N` child. Prometheus scraping and vendor receivers:
[observability.md](observability.md) and
[deployment.md — Observability Setup](deployment.md#observability-setup).

### The metrics that catch the failure modes in this page

| Metric / signal | Catches |
|---|---|
| `taskq.queue.depth` (by queue — counts `pending` **and** `scheduled`) | backlog growth, starved queues, fan-out storms |
| scheduled depth specifically (SQL below) | the promotion stall described below |
| `taskq.jobs.stranded` (gauge) | jobs whose actor has no `actor_config` row — can never dispatch |
| `taskq.dispatch.duration` | dispatch contention (PgBouncer/pool trouble) |
| `messaging.process.duration` | actor latency, slow chunks |
| `taskq.lock.expires_in_seconds` | heartbeat trouble before it becomes `crashed` jobs |
| `taskq.deadline_exceeded_sweep.jobs_failed` | `schedule_to_close` too tight |
| `taskq.backpressure.errors` | `max_pending` rejections — producer pressure |
| `taskq.cron.disabled_schedules` | a cron outage with one log line |
| `taskq.maintenance_leader.is_leader` summed != 1 | leader split-brain / no leader |

When metrics are unavailable or suspect, **query the tables, not the logs** — during one
production outage the console logs looked clean while the worker was down:

```sql
-- Replace {schema} with your TASKQ_SCHEMA_NAME.
SELECT id, worker_label, last_seen_at,
       last_seen_at < clock_timestamp() - interval '60 seconds' AS stale
FROM {schema}.workers ORDER BY last_seen_at DESC;

SELECT queue, status, count(*) FROM {schema}.jobs
WHERE status IN ('pending','scheduled','running')
GROUP BY queue, status ORDER BY count(*) DESC;
```

### Watch: large scheduled backlogs

A very large set of *due* `scheduled` jobs can stall promotion itself: the leader's
promotion sweep selects all due rows in one transaction and writes one state-change event row
per promoted job, under a single `dispatcher_command_timeout` (default 5 s) deadline for the
whole iteration. If the set is too large to promote inside the deadline, the transaction rolls
back — and the next tick faces the same (or a larger) set. This is a known livelock class under
active repair — check the project's GitHub issues for current status. The detection guidance
below stays valuable regardless, because it catches *any* failure mode that grows the scheduled
backlog. Symptoms: `scheduled` depth grows, `pending` stays empty, throughput is zero, and
**health probes stay green** (the failure is transient-classified; the loop is alive). Detect it
before it bites:

```sql
SELECT count(*) FROM {schema}.jobs
WHERE status = 'scheduled' AND scheduled_at <= clock_timestamp();
```

Alert on that count and on the `scheduled-wake-failed` / `sweep-deadline-exceeded-failed`
log events (`kind="scheduled_wake_failed"` / `kind="sweep_deadline_exceeded_failed"` — alert on
the `event` name, not the `kind` value, for field-scoped matchers). Prevent it with `max_pending` on high-fan-out actors, retry policies that disperse
cohorts (longer `base`, higher `jitter` — see [§6](#6-classifying-failures-terminal-retryable-transient)),
and bounded fan-out per job (chunk sizes in the hundreds, not the tens of thousands).

### Alert rules

Ship-ready alert rules for the metrics above exist in the repo and are ready to import:
[`src/taskq/contrib/prometheus/rules.yaml`](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/src/taskq/contrib/prometheus/rules.yaml)
(9 rules: queue depth, heartbeat misses, crashed-job rate, abandoned jobs, lock TTL, leader
split-brain, dispatch latency, progress failures, disabled cron) and the equivalent PrometheusRule
CRD at `src/taskq/contrib/kubernetes/prometheus_rule.yaml`. Importing them is not enough — make
sure something **scrapes** `/jobs/health/metrics` (see
[deployment.md — Prometheus scrape](deployment.md#observability-setup)).

---

## 9. Migrating from Celery

Concept mapping for teams porting workers:

| Celery | TaskQ | Notes |
|---|---|---|
| `--concurrency` (prefork) | `TASKQ_MAX_CONCURRENCY` | asyncio coroutines, not processes — CPU-bound work must move to threads/executors |
| `soft_time_limit` | `start_to_close` | cooperative cancellation of the attempt |
| `time_limit` (SIGKILL) | — | no per-job process kill; see the sync-actor caveat in [§2](#2-timeouts-start_to_close-and-schedule_to_close) |
| `visibility_timeout` | `lock_lease` (60 s) + leader sweep | worker death → reclaim → retry |
| `acks_late` / `reject_on_worker_lost` | default behavior | at-least-once via lease reclaim |
| `max_retries` + `retry_backoff` | `RetryPolicy(max_attempts, backoff, base, cap, jitter)` | jitter is ±20% multiplicative, not full jitter — and it does not decorrelate synchronized cohorts (see [§6](#6-classifying-failures-terminal-retryable-transient)) |
| `autoretry_for` | default (all exceptions retry under the policy) | list the *terminal* ones instead: `non_retryable_exceptions` |
| `rate_limit="100/m"` | `TokenBucket` / `SlidingWindow` on the actor | fleet-wide, Redis- or PG-backed; keyed refs for per-tenant quotas |
| `countdown=` / `eta=` | `scheduled_at` | timezone-aware; ~1 s promotion precision |
| `expires=` | `retry.time_budget` → `schedule_to_close` (`time_budget` requires `kind="indefinite"`) | interval from enqueue, server clock |
| `task_id` dedup hacks | `idempotency_key` (+ scope) | DB-enforced; duplicates return the existing job — mind the key-discipline rules in [§5](#5-fan-out-at-scale-chunks-cursors-idempotency) |
| `chord` | batch `finalizer` + `wait_for_batch`, or app-level run accounting | [§5 Patterns B/C](#5-fan-out-at-scale-chunks-cursors-idempotency) |
| `chain` / `canvas` | `ctx.jobs.enqueue(...)` from the actor body | transactional on a LOOP-scope conn (single-slot), autonomous otherwise — [§5](#5-fan-out-at-scale-chunks-cursors-idempotency) |
| `task_routes` | `@actor(queue=...)` + worker `TASKQ_QUEUES` | routing is by queue, not by broker exchange |
| `priority=` (queue priority) | `@actor(priority=...)` / `enqueue(priority=...)` | higher dispatches first; stamped at enqueue — see [§3](#starvation-priority-and-fairness) |
| `fair_queues` / per-tenant fairness | `round_robin` mode + `fairness_key` | the mode must be set explicitly — see [§3](#starvation-priority-and-fairness) |
| worker `-n` names | `worker_label` (+ `worker_group` OTel label) | labels only — no routing semantics |
| `celery beat` | `@cron(...)` | fires on schedule regardless of the previous fire; overlap suppression has traps — see [§5 — Cron](#cron-and-scheduled-workloads) |
| DLQ plugin | `on_retry_exhausted` + `ErrorReporter` | no built-in queue — route where you want it |

---

## 10. The footgun index

The condensed "know this before your first incident" list. Each row links to the deep dive.

**Configuration & routing**

| Footgun | Symptom | Fix / deep dive |
|---|---|---|
| `worker_group` expected to route or partition | jobs "not reaching" a group | it's an OTel label only — route with queues ([§1](#1-the-operating-model-in-five-minutes)) |
| Worker consumes no queue an actor enqueues to | jobs sit `pending` forever | check `actors-on-unconsumed-queues` warnings — [workers.md](workers.md#actors-on-unconsumed-queues) |
| API and worker on different `TASKQ_SCHEMA_NAME` | every job stuck `pending`, no error anywhere | one source of truth for the schema, refuse-don't-default |
| `round_robin` expected but never set (or no `fairness_key`) | fairness silently off; `__null__` cohort = plain FIFO | set the queue mode; key every enqueue — [§3](#starvation-priority-and-fairness) |
| `TASKQ_MIGRATE_ON_START=true` expected to run in the worker | warning `migrate-on-start-ignored-by-worker` | run migrations in a pre-deploy job / admin UI ([deployment.md](deployment.md#migration-strategy)) |
| `taskq queues set-max-concurrent` changed, nothing happened | cap unchanged | queue caps are read at worker startup — restart workers |
| `@actor(max_concurrent=...)` added in code, no effect | `actor-config-capacity-override` log | stored `actor_config` wins; NULL = uncapped — audit with `taskq actor-config list` ([§3](#capacity-ownership-the-seed-only-trap)) |
| Actor's `queue`/`metadata` changed after first registration | worker refuses to boot (`ActorConfigDriftList`) | two-step move or one-boot `TASKQ_FORCE_UPDATE_ACTOR_CONFIG` ([§3](#capacity-ownership-the-seed-only-trap)) |
| `.env` files expected to lose to (or beat) process env | config resolves "backwards" | dotenvmodel 1.x: **process env beats `.env`** unless `DOTENV_OVERRIDE=true` — [configuration.md](configuration.md); and mind the loading CWD |
| Actor registry passed as a generator (or generated actors missing from it) | zero/n actors registered | pass a mapping; test that every actor name is in the registry — [workers.md — Actor registry](workers.md#actor-registry) |

**Timeouts & retries**

| Footgun | Symptom | Fix / deep dive |
|---|---|---|
| No `start_to_close` anywhere; an actor hangs | slot held forever | set `TASKQ_DEFAULT_START_TO_CLOSE` + per-actor overrides ([§2](#2-timeouts-start_to_close-and-schedule_to_close)) |
| Expecting `schedule_to_close` to kill a running attempt | long attempt survives past deadline | it only gates *future* dispatches ([§2](#2-timeouts-start_to_close-and-schedule_to_close)) |
| `start_to_close` expected to kill a sync actor's thread | job marked timed out, side effects continue anyway | sync actors keep running — poll `ctx.should_abort()` ([actors.md](actors.md#sync-actors)) |
| `heartbeat_timeout` set at enqueue | nothing changes | currently stored but not enforced ([§2](#2-timeouts-start_to_close-and-schedule_to_close)) |
| Retry window shorter than routine provider blips | terminal exhaustion on an ordinary 5xx | fewer attempts, longer `base` ([§6](#6-classifying-failures-terminal-retryable-transient)) |
| Snoozing finalizer with a tight `time_budget` | `DeadlineExceeded` mid-batch | size the budget to batch duration + retries; `time_budget` requires `kind="indefinite"` ([§5](#5-fan-out-at-scale-chunks-cursors-idempotency)) |
| `max_retry_backoff` raised, long backoffs still capped | retries at 24 h ceiling | the ceiling is `min(policy.cap, TASKQ_MAX_RETRY_BACKOFF)` ([retries.md](retries.md#3-backoff-algorithms)) |
| Catching failures and returning a result | job `succeeded`, retry machinery never engaged | raise — [§6](#6-classifying-failures-terminal-retryable-transient) |
| Actor returns normally during a drain-cancel | job records `succeeded`, chain dies on every deploy | drain sets the same cancel event — never return normally on cancel ([§6](#6-classifying-failures-terminal-retryable-transient)) |
| Cron actor overlap "suppressed" with `singleton=True` | schedule auto-disables after 3 collisions | fires strike out at 3 — see [§5 — Cron](#cron-and-scheduled-workloads) |

**Fan-out & dedup**

| Footgun | Symptom | Fix / deep dive |
|---|---|---|
| One giant "sync everything" job | pipeline blocked for hours; killed by `start_to_close`; resync repeats | chunk it — [§5 Pattern A](#5-fan-out-at-scale-chunks-cursors-idempotency) |
| No `idempotency_key` on chunk jobs | retries fork the chain, duplicate work | stable business key per chunk ([§5](#5-fan-out-at-scale-chunks-cursors-idempotency)) |
| Stable key on a self-continuation successor | continuation silently dropped (collides with succeeded predecessor) | fresh key dimension or no key ([§5](#5-fan-out-at-scale-chunks-cursors-idempotency)) |
| Key missing a payload dimension / re-posting after terminal failure | work silently *lost* / rerun no-ops onto the dead job | keys are payload-blind and status-blind — encode dimensions and attempts ([§5](#5-fan-out-at-scale-chunks-cursors-idempotency)) |
| `unique_for` without `identity_key` | dedup silently off | pass `identity_key` at enqueue — [actors.md](actors.md#unique_for-deduplication) |
| `unique_for` on a thin root expected to single-flight the chain | overlapping runs despite the window | single-flight the work, not the root ([§5](#5-fan-out-at-scale-chunks-cursors-idempotency)) |
| `enqueue_batch_fast` with duplicate keys | whole COPY aborts | pre-dedup or use `enqueue_batch` ([§5](#5-fan-out-at-scale-chunks-cursors-idempotency)) |
| Streaming/fast batches assumed to enforce `max_pending` | unbounded queue growth | only `enqueue`/`enqueue_batch` enforce it ([§5](#5-fan-out-at-scale-chunks-cursors-idempotency)) |
| Huge synchronized `scheduled` cohort (mass retry wave) | promotion stalls; health green; throughput zero | disperse cohorts, `max_pending`, scheduled-depth alert — [§8](#watch-large-scheduled-backlogs) |
| Tag factory emitting colons | every enqueue 500s | tags must match `\A\w(?:[\w\-]*\w)?\Z` — [§5](#5-fan-out-at-scale-chunks-cursors-idempotency) |

**Runtime & infrastructure**

| Footgun | Symptom | Fix / deep dive |
|---|---|---|
| Everything through transaction-mode PgBouncer | LISTEN/advisory-lock failures | only `worker_pool` may pool — split the DSNs ([§4](#4-sizing-workers-and-postgres-connections)) |
| Fleet sized without connection math | `too many connections` under load | per-worker 22 (default), rollout ×2 — the budget formula ([§4](#4-sizing-workers-and-postgres-connections)) |
| BYO `PoolFactory` counted as one pool | three full-sized pools, budget blown | factory is invoked once per role ([§4](#bring-your-own-pools-change-the-arithmetic)) |
| TaskQ client handed the request handlers' pool | silent whole-process deadlock at `pool_max` | dedicated small pool — [§4](#bring-your-own-pools-change-the-arithmetic) |
| Token auth without reload | healthy for an hour, then cannot connect | `TASKQ_RELOAD_INTERVAL` inside token lifetime, all five roles ([§4](#managed-identities-and-token-rotation)) |
| `terminationGracePeriodSeconds` < shutdown worst case | SIGKILL mid-drain, `crashed` jobs | grace ≥ cancellation + cleanup + ~27 s tail ([deployment.md](deployment.md#health-probes)) |
| One job longer than the shutdown budget | watchdog force-exits; *sibling* in-flight jobs die too | size the grace to your slowest actor, or cap it with `start_to_close` ([§2](#2-timeouts-start_to_close-and-schedule_to_close)) |
| Drained `refill_per_second=0` bucket, no deadline | job re-queues every 5 s forever | add refill or `schedule_to_close` ([§7](#7-waiting-politely-rate-limits-snooze-retryafter-retry-after)) |
| Redis in dev, none in prod | worker refuses to start in prod only | decide the limiter backend per environment ([§7](#7-waiting-politely-rate-limits-snooze-retryafter-retry-after)) |
| Worker without a Redis client expected to publish progress | SSE/progress silently empty, no error | progress fanout needs Redis; PG snapshotting still works — [progress.md](progress.md) |
| `max_concurrent=0` set as a "disable" | actor silently stops dispatching; jobs pile up `pending` | `0` is drain mode — use it deliberately, drain with it, then restore |
| Redis-backed limiter without `TASKQ_REDIS_URL` | worker refuses to start | fail-fast by design — configure Redis or use `backend="postgres"` ([§7](#7-waiting-politely-rate-limits-snooze-retryafter-retry-after)) |
| Suppressing `asyncio.CancelledError` in an actor | cancel phase escalates to `abandoned` | never catch it — [cancellation.md](cancellation.md) |
| Two workers, same health socket path | second worker's health server fails | unique `TASKQ_HEALTH_SOCKET_PATH` per process |
| Cancel expected to survive a crash-retry | retried attempt runs uncancelled | retry arms reset cancel state by design — re-cancel if still needed |
| Exporter env var set, telemetry expected | nothing collected, silently | exporter must be configured in-process — [§8](#8-observability-and-alerting) |
| PyPI release assumed current | fixed bugs recur; documented features missing | check the [CHANGELOG](../changelog.md), pin a rev, assert the pin (see the intro warning) |

---

## 11. Adoption checklist

Everything to verify before pointing production traffic at a new TaskQ fleet. (The deployment
mechanics of each item: [deployment.md — Production Checklist](deployment.md#production-checklist).)

- [ ] **Version** — git pin asserted by test, or release docs matched to the release (see the
      intro warning)
- [ ] **Migrations** run as a pre-deploy step (the worker ignores `TASKQ_MIGRATE_ON_START`);
      schema name is one source of truth shared by API and worker
- [ ] **Timeouts**: `TASKQ_DEFAULT_START_TO_CLOSE` set as a safety net; every long-runner declares
      its own `start_to_close`; every `kind="indefinite"` actor has a `time_budget`
- [ ] **Failure classification**: terminal exception types listed in `non_retryable_exceptions`
      (or classified via `retry_classifier`); per-item faults skip-and-continue; default
      `RetryPolicy()` (3 attempts) reviewed per actor; no actor catches failures and returns
- [ ] **Dedup**: batch-shaped work chunked ([§5](#5-fan-out-at-scale-chunks-cursors-idempotency));
      chunk/continuation jobs carry stable, dimension-complete `idempotency_key`s; self-continuations
      carry a fresh key dimension
- [ ] **Concurrency**: `TASKQ_MAX_CONCURRENCY` matched to workload class; strict caps (queue or
      `ConcurrencyReservation`) where a soft damper is not enough; thread-unsafe libraries
      serialized by a shared reservation
- [ ] **Capacity audit**: `taskq actor-config list` — every actor's `max_concurrent`/`max_pending`
      is the value you decided (NULL = uncapped); an ownership posture chosen (operator-owned vs
      boot-time convergence)
- [ ] **Queue modes**: `round_robin` + `fairness_key` set where tenants share queues; priorities
      assigned to latency-sensitive actors
- [ ] **Cron**: every schedule registered by this deploy still intended; actors have
      `start_to_close` (note cron fires currently ignore per-actor values — the fleet-wide
      `TASKQ_DEFAULT_START_TO_CLOSE` is what protects cron); overlap strategy chosen with its
      trap understood
      ([§5 — Cron](#cron-and-scheduled-workloads))
- [ ] **Connection budget**: computed for the target fleet size against `max_connections`
      (including app pools, BYO factory fan-out, and rollout doubling); `pgbouncer_recommended`
      checked
- [ ] **DSN split**: `TASKQ_PG_DSN_DIRECT` (worker core) vs `TASKQ_PG_DSN_POOLED` (worker_pool)
- [ ] **Credentials**: reload interval inside token lifetime; all five connection roles covered
- [ ] **Shutdown budget**: supervisor/`terminationGracePeriodSeconds` ≥
      `cancellation_grace + cleanup_grace + ~27 s` (default model: 67 s), and ≥ your slowest actor
- [ ] **Health probes**: `taskq health live/ready` wired (exec probes; TCP `TASKQ_HEALTH_PORT`
      only where `httpGet` is forced); unique socket path per process
- [ ] **Redis** provisioned iff using Redis-backed limiters or real-time progress; PG fallback
      decision made; dev/prod symmetry checked
- [ ] **Observability**: exporter wired *in-process* and verified to arrive; the alert rules from
      [§8](#8-observability-and-alerting) imported **and scraped**; someone watches
      `taskq.queue.depth`, scheduled depth, and `taskq.backpressure.errors`
- [ ] **DLQ routing** decided: `on_retry_exhausted` / `ErrorReporter` target chosen (there is no
      built-in dead-letter queue)
- [ ] **Idempotency of actor bodies** audited — delivery is at-least-once, including crash-reclaim
      re-dispatch

---

## Related documentation

- [sweeps.md](sweeps.md) — recurring passes: page/fan-out/recurse, keyset cursors, idempotency by role
- [retries.md](retries.md) — retry policies, `start_to_close` vs `schedule_to_close`, classifier hooks
- [workers.md](workers.md) — worker internals, concurrency model, `WorkerSettings` reference
- [jobs-clients.md](jobs-clients.md) — enqueue/batch APIs, idempotency, dedup mechanics
- [actors.md](actors.md) — `@actor` options, `unique_for`, sync actors, sub-job enqueuing
- [rate-limiting.md](rate-limiting.md) — primitives, backends, reservations, queue-level strict caps
- [cron.md](cron.md) — schedule registration, DST strategies, per-property dedup
- [managed-identities.md](managed-identities.md) — credential providers, BYO connections, token rotation
- [deployment.md](deployment.md) — production checklist, Kubernetes/systemd/Compose, scaling
- [observability.md](observability.md) — OTel setup, metrics reference, error reporting
- [troubleshooting.md](troubleshooting.md) — symptom-indexed diagnosis for everything above going wrong
