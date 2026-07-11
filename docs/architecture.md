# Architecture Reference

Internal architecture reference for TaskQ. Covers component topology,
the backend protocol, state machine, dispatch mechanics, DI engine, cancellation
protocol, leader election, NOTIFY wiring, rate limiting, schema design, and
observability.

This document is useful both for contributors working on TaskQ internals and
for users who want to understand the system's correctness guarantees before
relying on it in production.

Related docs: [api-reference/testing.md](api-reference/testing.md), [index.md](index.md),
[guides/workers.md](guides/workers.md), [guides/actors.md](guides/actors.md), [guides/rate-limiting.md](guides/rate-limiting.md).

---

## High-Level Component Diagram

```
                  ┌─────────────┐
                  │ JobsClient  │
                  └──────┬──────┘
                         │ enqueue()
                         ▼
┌──────────────────────────────────────────┐
│              Backend (Protocol)          │
│   PostgresBackend / InMemoryBackend      │
└──────────────┬───────────────────────────┘
               │ asyncpg
               ▼
         ┌──────────┐
         │ Postgres │
         └──────────┘
               ▲
               │ LISTEN / NOTIFY
               │
┌──────────────────────────────────────────────────────────┐
│                     Worker TaskGroup                     │
│                                                          │
│  ┌───────────────┐   ┌───────────────┐                  │
│  │  HeartbeatLoop │   │ NotifyListener │                  │
│  │  (cancel-poll, │   │ (wake channel) │                  │
│  │   lock renewal)│   └───────┬────────┘                 │
│  └───────────────┘           │ asyncio.Event             │
│                               ▼                          │
│  ┌───────────────┐   ┌───────────────────────────────┐   │
│  │ MaintenanceLeader │   │     ProducerLoop             │   │
│  │ (advisory lock,│   │  dispatch_batch() →           │   │
│  │  sweeps, cron) │   │  ConsumerLoop × N             │   │
│  └───────────────┘   └───────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
               ▲
               │ FastAPI routes
               │
         ┌──────────┐
         │ Admin UI │
         └──────────┘
```

The `Backend` protocol is the single abstraction layer. `PostgresBackend` wires to
Postgres via asyncpg; `InMemoryBackend` holds all state in Python dicts and is used
exclusively in tests.

---

## Backend Protocol

Defined in `src/taskq/backend/_protocol.py`.

### Protocol declaration

```python
@runtime_checkable
class Backend(Protocol):
    BACKEND_PROTOCOL_VERSION: ClassVar[int]
    supports_transactional_simulation: ClassVar[bool]

    # Enqueue
    async def enqueue(self, args: EnqueueArgs) -> JobRow: ...
    async def enqueue_with_conn(self, conn, args: EnqueueArgs) -> JobRow: ...

    # Dispatch
    async def dispatch_batch(self, worker_id, queues, limit, lock_lease) -> list[JobRow]: ...

    # Heartbeat
    async def heartbeat_jobs(self, worker_id, lock_lease) -> int: ...
    async def extend_reservation_leases(self, worker_id, lock_lease) -> int: ...

    # Terminal writes
    async def mark_succeeded(self, job_id, worker_id, result, ...) -> bool: ...
    async def mark_succeeded_with_conn(self, conn, job_id, worker_id, result, ...) -> bool: ...
    async def mark_failed_or_retry(self, job_id, worker_id, error_info, next_scheduled_at, ...) -> JobRow: ...
    async def mark_cancelled(self, job_id, worker_id, ...) -> bool: ...
    async def write_cancel_escalation(self, job_id, worker_id, phase: Literal[2]) -> bool: ...
    async def mark_abandoned(self, job_id, ...) -> bool: ...
    async def mark_snoozed(self, job_id, worker_id, delay, ...) -> Literal["scheduled","failed","noop"]: ...
    async def mark_retry_after(self, job_id, worker_id, delay, ...) -> Literal["scheduled","failed:DeadlineExceeded","failed:MaxAttemptsExceeded","noop"]: ...

    # Attempt history
    async def write_attempt(self, attempt: AttemptRow) -> None: ...
    async def get_attempts(self, job_id) -> list[AttemptRow]: ...

    # Cancel signals
    async def write_cancel_request(self, job_id, reason) -> bool: ...
    async def poll_cancel_flags(self, worker_id) -> list[CancelFlag]: ...

    # Admin operations
    async def retry_job(self, job_id) -> bool: ...

    # Scheduling / sweeps
    async def scheduled_to_pending(self, now) -> int: ...
    async def deadline_sweep(self, now) -> int: ...
    async def reclaim_expired_locks(self, now, cancel_grace, cleanup_grace) -> int: ...

    # Read
    async def get(self, job_id) -> JobRow | None: ...
    async def list_jobs(self, filters: JobFilter) -> list[JobRow]: ...

    # NOTIFY hook
    def subscribe_wake(self) -> AsyncContextManager[asyncio.Event]: ...
    def subscribe_cancel_wake(self) -> AsyncContextManager[asyncio.Event]: ...
```

`BACKEND_PROTOCOL_VERSION` is a `ClassVar[int]` (currently `2`). Both backends
assert this constant matches at import time, preventing silent protocol drift.

`retry_job` resets a terminal job (`failed`, `crashed`, or `cancelled`) back to
`pending` so it can be re-dispatched. Returns `True` if the job was retried,
`False` if it was not in a retryable state. The admin UI exposes this via the
`POST /jobs/{job_id}/retry` endpoint.

`subscribe_cancel_wake` is the cancel-signal analogue of `subscribe_wake`: it
yields an `asyncio.Event` that is set whenever a cancel NOTIFY arrives, allowing
the heartbeat loop to interrupt its sleep immediately on cancellation rather
than waiting for the next scheduled tick.

### Why Protocol, not ABC

The `Backend` is a `Protocol` (structural subtyping) rather than an abstract base
class. This means:

- `InMemoryBackend` and `PostgresBackend` satisfy it without inheriting from it.
- Third-party backends can satisfy the interface without importing TaskQ internals.
- `@runtime_checkable` allows `isinstance(obj, Backend)` checks at wiring time.

### `supports_transactional_simulation`

`PostgresBackend` sets this to `False` — atomicity comes from real PG transactions.
`InMemoryBackend` sets it to `True` — `SubJobEnqueuer` buffers sub-job `EnqueueArgs`
and flushes on success or discards on failure to simulate rollback semantics.

---

## State Machine

Defined in `src/taskq/backend/statemachine.py` and mirrored as a PG enum in
`src/taskq/migrations/01.00.00_01_pre_initial.sql`.

### Statuses

| Status | Terminal | Description |
|---|---|---|
| `pending` | No | Queued, ready for dispatch |
| `scheduled` | No | Deferred; `scheduled_at` is in the future |
| `running` | No | Dispatched, held by a worker lock |
| `succeeded` | Yes | Actor returned successfully |
| `failed` | Yes | Actor raised a non-retryable error, or retry budget exhausted |
| `cancelled` | Yes | Cancelled before or during execution |
| `crashed` | Yes | Worker died (lock expired) with no retries remaining |
| `abandoned` | Yes | Forced cancellation completed (cancel_phase=2 + grace elapsed) |

### Valid transitions

```
pending   → running (dispatch), cancelled (cancel request), failed (deadline sweep)
scheduled → pending (scheduled_to_pending sweep), cancelled, failed (deadline sweep)
running   → succeeded, failed, cancelled, crashed, abandoned, scheduled (snooze/retry/RetryAfter)
succeeded → (terminal)
failed    → (terminal)
cancelled → (terminal)
crashed   → (terminal)
abandoned → (terminal)
```

`assert_valid_transition(from_status, to_status, job_id)` is the application-level
guard. The SQL `WHERE status = 'X'` predicate is the authoritative serialization
gate — two concurrent writers cannot both transition the same row because only one
can hold the row lock from the dispatch CTE's `FOR UPDATE SKIP LOCKED`.

### Which component drives each transition

| Transition | Driver |
|---|---|
| pending → running | Dispatch CTE (producer loop) |
| scheduled → pending | `scheduled_to_pending` sweep (leader) |
| running → succeeded | Consumer after actor returns |
| running → failed | Consumer after error / deadline |
| running → scheduled | Consumer on `Snooze` / `RetryAfter` / transient retry |
| running → cancelled | Consumer after cancel_phase=1 (cooperative) |
| running → abandoned | `CancelController.run_post_tx` (heartbeat, post-phase-3) |
| running → crashed | `reclaim_expired_locks` sweep (leader, Sweep 1) |
| pending/scheduled → cancelled | `write_cancel_request` (client) |
| pending/scheduled → failed | `deadline_sweep` (leader, Sweep 2) |

---

## Dispatch CTE

Source: `src/taskq/backend/_dispatch_sql.py`.

The dispatch CTE is a single atomic `UPDATE … RETURNING *` statement. It acquires
row locks and transitions `pending` → `running` for a batch of jobs. TaskQ ships
two dispatch SQL variants selected per-queue at dispatch time:

- **`DISPATCH_STRICT_FIFO_SQL`** — priority-then-time ordering. Best for queues
  with no fairness requirements.
- **`DISPATCH_ROUND_ROBIN_SQL`** — per-fairness-key interleaving (lateral dispatch).
  Prevents deep queues of one actor or tenant from starving others. See [Queue modes](#queue-modes) below.

### Queue modes

Each queue has a `mode` column in the `queues` table: `strict_fifo` (default) or
`round_robin`. The dispatch batch method queries the `queues` table via
`_resolve_queue_modes()` to select the SQL variant — one indexed query per batch.
Queues absent from the table default to `strict_fifo`.

| Mode | Ordering | Use case |
|---|---|---|
| `strict_fifo` | `priority DESC, scheduled_at, id` | No fairness requirement; simple priority queue |
| `round_robin` | `fairness_rank, priority DESC, scheduled_at` | Multi-tenant or multi-cohort queues where one busy actor must not starve others |

The round-robin mode computes `fairness_rank` via:
```sql
ROW_NUMBER() OVER (PARTITION BY COALESCE(fairness_key, '__null__')
                   ORDER BY priority DESC, scheduled_at)
```
Jobs without a `fairness_key` collapse into a single `__null__` cohort — equivalent
to `strict_fifo` within that cohort. See [guides/jobs-clients.md](guides/jobs-clients.md) for `fairness_key` usage.

### Common CTE structure

Both variants share the same CTE shape up to the `candidates` phase:

```
params              → bind $1 queues, $2 limit_n, $3 worker_id, $4 lock_lease, $5 oversample
running_per_actor   → count running jobs per actor (for max_concurrent cap)
running_identities  → set of (actor, identity_key) in running status
per_actor_capacity  → residual = max_concurrent - in_flight per actor
candidates          → CROSS JOIN LATERAL per-actor, filtered by queue/status/scheduled_at,
                      limited to residual * oversample per actor
```

The `candidates` CTE differs between modes:
- **`strict_fifo`:** sorts by `priority DESC, scheduled_at, id`; `fairness_rank` is `NULL`.
- **`round_robin`:** computes `fairness_rank` via `ROW_NUMBER() OVER (PARTITION BY COALESCE(fairness_key, '__null__') …)`.

After `candidates`, both variants share identical downstream CTEs:

```
identity_dedup      → DISTINCT ON (actor, identity_key) for identity-gated jobs
                      UNION ALL non-identity jobs
ranked              → ROW_NUMBER() OVER (PARTITION BY actor ORDER BY …) as pending_rank
                      (round_robin: ORDER BY fairness_rank, priority; strict_fifo: ORDER BY priority)
locked              → FOR UPDATE SKIP LOCKED, LIMIT limit_n
eligible_candidates → LEFT JOIN actor_config for max_concurrent
                      LEFT JOIN running_per_actor for in_flight count
                      BOOLEAN gate: in_flight < max_concurrent
                      ROW_NUMBER() OVER (PARTITION BY actor …) for per-actor ranking
eligible            → cap: actor_rank <= max_concurrent - in_flight, LIMIT limit_n
UPDATE jobs         → WHERE j.id IN eligible AND j.status = 'pending'
                      SET status='running', started_at=now(), attempt=attempt+1, …
```

### Key correctness invariants

1. `FOR UPDATE SKIP LOCKED` is confined to the `locked` CTE. PostgreSQL forbids
   window functions and `FOR UPDATE` in the same `SELECT`; the `candidates`
   passthrough CTE is mandatory.

2. The boolean gate (`in_flight < max_concurrent`) is necessary but not sufficient
   alone. Two concurrent producers seeing `in_flight=0` would both dispatch up to
   `limit_n` jobs for the same actor. The `actor_rank <= max_concurrent - in_flight`
   cap in `eligible` closes this gap.

3. The final `WHERE j.status = 'pending'` race guard prevents re-dispatch if
   another producer transitioned the row between lock acquisition and the UPDATE.

4. Expected over-count: `(num_producers - 1) * max_concurrent` jobs may be
   dispatched beyond the cap per round under concurrent producers. This is a
   documented, bounded tradeoff — the sweep loop reclaims stale locks.

5. Per-actor oversampling (`LIMIT pac.residual * oversample`, default `oversample=2`
   via `TASKQ_DISPATCH_OVERSAMPLE`) absorbs filtering from max_concurrent caps and
   identity serialization. `residual` is the actor's remaining dispatch slots this
   round; oversampling reads a multiple of that per-actor LATERAL, not a multiple of
   the overall `limit_n`. Under pathological workloads (all candidates share one
   identity) the producer retries on the next tick.

---

## DI Engine

Source: `src/taskq/_di/`.

### Component overview

| File | Role |
|---|---|
| `registry.py` | `ProviderRegistry` — registration, validation, plan cache |
| `scope.py` | Re-export shim for `Scope` — the canonical definition lives in `src/taskq/_scope.py` (PROCESS=0, THREAD=1, LOOP=2, TRANSIENT=3) |
| `scopes.py` | `ScopeContainer`, `ProcessScope`, `ThreadScope`, `LoopScope`, `build_actor_scope` |
| `solver.py` | `solve_dependencies` — resolves kwargs dict for a callable |
| `lifecycle.py` | Detects provider lifecycle from class/factory shape |
| `_validate.py` | Five-phase startup validation (cycle detection, scope rules, missing providers) |

### Scope nesting

```
PROCESS (widest)
  └── THREAD
        └── LOOP
              └── TRANSIENT (narrowest, per actor invocation)
```

A narrower scope may depend on a wider scope (a TRANSIENT provider may inject a
LOOP-scoped connection). A wider scope must not depend on a narrower scope — this
would mean the longer-lived singleton depends on something that might not exist.
Violations are detected at `registry.validate()` time and raise `ScopeViolation`.

### Solver algorithm

`solve_dependencies(func, registry, scope_containers, passthrough_kwargs)`:

1. Calls `get_type_hints(func, include_extras=True)` to collect annotated parameter
   types.
2. For each parameter (excluding `return` and any name present in the caller-supplied
   `passthrough_kwargs` dict — in practice this is how `payload` and `ctx` are
   excluded from DI lookup, since callers pass them through by name rather than the
   solver hardcoding those parameter names):
   - Unwraps `Annotated[T, Scope.X]` to extract the type `T` and any scope override.
   - Looks up `T` in the registry to get the `ProviderEntry`.
   - Selects the effective scope (override if present, else the entry's registered
     scope).
   - Calls `scope_containers[effective_scope].get_or_create(T, entry)`.
3. Returns a `kwargs` dict ready for `**kwargs` injection.

The solver never calls factories directly. Factory invocation, caching, and
teardown registration are the `ScopeContainer`'s responsibility (Decision 6).

### Per-invocation actor scope

`build_actor_scope` (an async context manager) opens a `TRANSIENT` scope container,
resolves all DI kwargs for the actor function, yields a `ResolvedActorScope`, and
on exit closes the TRANSIENT scope in LIFO order via the log-and-continue teardown
policy (every teardown runs even if earlier teardowns fail; `CancelledError` is
re-raised after all teardowns complete).

The TRANSIENT container teardown is shielded with `asyncio.shield` to prevent
a cancellation in the actor body from short-circuiting cleanup and leaking resources.

---

## Cancellation Protocol

Source: `src/taskq/worker/cancel.py`.

Cancellation proceeds through three in-DB phases plus one in-process sentinel:

| Phase | Value | Location | Meaning |
|---|---|---|---|
| `NONE` | 0 | PG + in-process | No cancellation requested |
| `COOPERATIVE` | 1 | PG + in-process | Cancel requested; actor's `cancel_event` will be set |
| `FORCED` | 2 | PG + in-process | Grace elapsed; asyncio task cancelled |
| `ABANDON_PENDING` | 3 | In-process only | Queued for post-transaction `mark_abandoned` |

`ABANDON_PENDING` is never written to PG (`cancel_phase BETWEEN 0 AND 2` check
constraint enforces this).

### Three-phase walkthrough

`CancelController.run_in_tx(conn)` runs inside the heartbeat transaction on every
tick:

**Phase 1 — Cooperative**

The heartbeat reads `cancel_requested_at IS NOT NULL AND status='running'` rows for
this worker via `POLL_CANCEL_FLAGS_SQL`. On first observation of `db_phase >= 1`:
- Sets `active.ctx.cancel_event.set()` (signals the actor).
- Records `cancel_observed_at = loop.time()` (monotonic, not wall clock).
- Sets local `cancel_phase = COOPERATIVE`. No PG write in this phase.

**Fast-advance**

If `db_phase == FORCED` while local is still `< FORCED`, the controller advances
locally without writing to PG (another controller already escalated).

**Phase 2 — Forced**

After `cancellation_grace_period` elapses since `cancel_observed_at`:
- Executes `CANCEL_ESCALATION_SQL` (`SET cancel_phase = 2 WHERE cancel_phase = 1`).
- Inserts a `job_events` row (`kind='state_change'`, phase 1→2 detail).
- Calls `active.task.cancel()` (asyncio task cancellation).
- **PG write happens BEFORE `task.cancel()` with no intervening `await`.**

**Phase 3 — Abandonment**

After `cancellation_grace_period + cleanup_grace_period` elapses:
- Sets `active.cancel_phase = ABANDON_PENDING` (in-process sentinel).
- Appends `job_id` to `_pending_abandons` deque.
- Does NOT call `mark_abandoned` here — the heartbeat transaction holds an UPDATE
  lock on the row; calling `mark_abandoned` (which opens a separate pool connection)
  would self-deadlock.

`CancelController.run_post_tx()` runs after the heartbeat transaction commits:
- Drains `_pending_abandons`.
- Calls `mark_abandoned(job_id)` (gated on `cancel_phase = 2`).
- Calls `active_jobs.deregister(job_id)`.

### Consumer skip guard

The consumer skips `mark_cancelled` when `cancel_phase >= ABANDON_PENDING` (phase 3).
`run_post_tx` owns the terminal write for phase-3 jobs. This prevents a race where
both the consumer and the heartbeat attempt a terminal write.

### `CancelController` Protocol

```python
@runtime_checkable
class CancelController(Protocol):
    async def run_in_tx(self, conn: asyncpg.Connection) -> None: ...
    async def run_post_tx(self) -> None: ...
```

Test stubs need only implement these two methods. The production implementation is
`_CancelController`, constructed via `make_cancel_controller(deps, worker_id, backend)`.

---

## Leader Election

Source: `src/taskq/worker/leader.py`.

### Mechanism

Leader election uses a PostgreSQL session-level advisory lock
(`pg_try_advisory_lock`) on a well-known name (`taskq:maintenance_leader`). The
lock is acquired over `deps.leader_conn` — a dedicated, non-pooled connection.

On each heartbeat tick, each pod calls `pg_try_advisory_lock`:
- If acquired: upserts `maintenance_leader` table row, sets `deps.is_leader` event.
- If not acquired: waits; retries on next tick.

The `maintenance_leader` table is queryable for observability and the admin UI, but
the advisory lock is the authoritative source of truth for election.

### What the leader does

`MaintenanceLeader` runs ten cooperative loops in a `TaskGroup`:

1. **Election loop** — acquires and renews the advisory lock.
2. **Watchdog** — detects stale lock state; refreshes `last_seen_at`.
3. **Scheduled-wake (Sweep 3)** — promotes `scheduled` → `pending` when
   `scheduled_at <= now()`. Sends `pg_notify` after promoting to wake consumer loops.
4. **Cron** — fires cron-scheduled actors at their declared cadence.
5. **Sweep (Sweeps 1, 2, 4)** — **leader-only** (gated on `ctx.deps.is_leader`),
   runs every 30 s: `reclaim_expired_locks` (Sweep 1, uses `FOR UPDATE SKIP LOCKED`),
   `deadline_sweep` (Sweep 2), and, when the backend supports them,
   `sweep_leaked_reservation_slots` (Sweep 4), `sweep_expired_results`, and
   `cleanup_stale_workers`.
6. **Prune (Sweep 5)** — runs daily (default 03:00 UTC). Moves terminal jobs
   (`succeeded`, `failed`, `cancelled`, `crashed`, `abandoned`) from `jobs` to
   `jobs_archive` once their per-status retention period has elapsed. Batched at
   10 000 rows per CTE; atomic move+delete within each batch. Controlled by
   `TASKQ_PRUNE_*` settings.
7. **Archive expiry (Sweep 6)** — runs daily (default 04:00 UTC, 1 hour after
   prune). Hard-deletes rows from `jobs_archive` once their `expire_at` has
   passed. Cascades to `job_attempts_archive`. Controlled by
   `TASKQ_ARCHIVE_EXPIRY_*` settings.
8. **Queue-depth / reservation sampling** — samples queue counts and reservation
   slot usage every 15 seconds for OTel gauges.
9. **Stranded-jobs detector** — runs every 60 s. Warns about pending/scheduled
   jobs whose actor has no `actor_config` row (e.g. the actor was removed from
   the registry but jobs remain enqueued).

Failover SLA: leader gap ≤ `heartbeat_interval + 1s` on worker kill.

---

## NOTIFY / Wake Mechanism

Source: `src/taskq/worker/notify.py`, `src/taskq/constants.py`.

### Channel name

```python
WAKE_CHANNEL_FMT = "taskq_wake_{schema}"
```

`wake_channel(schema)` validates the schema identifier against `_IDENT_RE` before
interpolation. Each schema gets its own channel, enabling multi-tenant deployments
on a single PG instance.

### Enqueue path

After a successful INSERT into `jobs`, `PostgresBackend.enqueue` executes:

```sql
SELECT pg_notify('taskq_wake_<schema>', '')
```

The empty payload is intentional — consumers do not need to parse it; the
notification alone is sufficient to trigger a dispatch poll.

### Consumer path

`notify_listener_loop` holds a dedicated `deps.notify_conn` (non-pooled, direct
DSN, TCP keepalives enabled). It calls `await conn.add_listener(channel, callback)`
where `callback` iterates `backend._wake_subscribers` and calls `event.set()` on
each.

Consumer loops register via `backend.subscribe_wake()` (an async context manager)
which adds a fresh `asyncio.Event` to `_wake_subscribers` on enter and removes it
on exit. The consumer loop awaits the event; on wake it polls `dispatch_batch`.

A `_health_check_loop` runs concurrently with the listener, executing `SELECT 1`
on the notify connection at `notify_health_check_interval`. On failure it
reconnects with bounded exponential backoff (initial delay × 2, max 30s). After
reconnect, the callback is re-registered and fires once to drain any jobs that
arrived while disconnected.

---

## Rate Limiting Architecture

Source: `src/taskq/ratelimit/`.

### Backend options

`RateLimitBackend = Literal["redis", "postgres", "memory"]`

- `redis`: token-bucket and sliding-window using Lua scripts against Redis. Requires
  the `redis` extra.
- `postgres`: falls back to the `rate_limit_buckets` table (token bucket) or
  `rate_limit_window_entries` table (sliding window) in PG.
- `memory`: in-process only; useful for tests.

### `RateLimitRegistry`

Actors declare rate limits via `rate_limits: list[str]` and concurrency
reservations via `reservations: list[str]` on the `@actor` decorator — plain
named-bucket strings, not typed ref objects. At startup,
`ProviderRegistry.validate(...)` (`src/taskq/_di/registry.py`) runs the DI
validation algorithm in `src/taskq/_di/_validate.py::run_validation`, which
includes a phase that checks each actor's `rate_limits` and `reservations`
name lists against the `RateLimitRegistry`'s registered names, raising
`MissingProvider` for unknown names.

### Dispatch integration

Before executing the actor body, `consume_one_job` checks the rate-limit decision:
- If `RateLimitDecision.allowed`: proceed.
- If denied with `retry_after`: call `mark_retry_after(delay=retry_after)` and
  release the job back to `scheduled` status without consuming the retry budget
  (when `consume_budget=False`).

Reservation slots are pre-allocated in `reservation_slots` rows and held with a
lease for the job's duration; `extend_reservation_leases` renews them on heartbeat.

---

## Schema Design Decisions

Source: `src/taskq/migrations/`.

### Forward-only migrations

Migrations only ever ADD to the schema. Destructive changes (DROP COLUMN, DROP
TABLE) require a `post` migration applied after all workers are on the new version.
The `pre`/`post` phase distinction is explicit in the filename and prevents
rolling-deploy races.

### `{schema}` placeholder

Every migration uses `{schema}` as a placeholder for the Postgres schema name.
The migration runner substitutes it at apply time after validating the name against
`_IDENT_RE`. This enables multi-tenancy: multiple isolated TaskQ instances can
coexist in the same Postgres cluster in different schemas.

**Never hardcode the schema name** in SQL files or application code. Always use
the placeholder in SQL files and `_IDENT_RE`-validated interpolation in Python.

### `jobs` vs `job_attempts` vs `job_events`

- `jobs` is the hot table. Columns hold the current snapshot: `status`,
  `attempt`, `locked_by_worker`, `error_class`, `result`, etc.
- `job_attempts` records every execution attempt with outcome, duration, and
  error. Pruned via `ON DELETE CASCADE`.
- `job_events` records every state transition and cancel request as an immutable
  audit log. Also pruned via `ON DELETE CASCADE`.

This separation keeps the `jobs` hot path narrow (fewer columns updated per
transaction) while providing full per-attempt forensics in `job_attempts` and
a queryable audit trail in `job_events`.

### `jobs_archive` and `job_attempts_archive`

When the prune sweep (Sweep 5) moves a terminal job out of `jobs`, it inserts
an identical row into `jobs_archive` plus two extra columns:

- `archived_at` (`timestamptz`) — wall-clock time the row was moved.
- `expire_at` (`timestamptz`) — when the row becomes eligible for hard-deletion
  by Sweep 6. Computed as `archived_at + archive_retention_period` (default
  1 year).

`job_attempts_archive` mirrors `job_attempts` with the same schema and an FK to
`jobs_archive(id) ON DELETE CASCADE`. Sweeps 5 and 6 are both batched atomic
CTEs, so `jobs_archive` and `job_attempts_archive` stay in sync by construction.

`job_events` rows are **not** archived — they are deleted by cascade when the
parent `jobs` row is pruned. Historical event data is not available in the
archive. The admin UI job-detail page shows an empty event log for archived
jobs and displays an "archived" banner to make this clear.

### No FK on `locked_by_worker`

`jobs.locked_by_worker` is a UUID column with no foreign key to `workers(id)`.
A real FK would cause an implicit `FOR KEY SHARE` lock on the `workers` row
during every dispatch UPDATE, creating SLRU contention under concurrent dequeue.
Worker liveness is tracked separately via `workers.last_seen_at`.

### Identifier validation

`_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")` is the canonical guard
before any schema-name interpolation into SQL. asyncpg does not support
parameter binding for SQL identifiers, so interpolation is unavoidable. Rather
than relying on a single check, TaskQ uses **defence-in-depth**: the schema
name is validated at `PostgresBackend.__init__` (and `migrate.py`) construction
time, **and** independently re-checked at every call site that interpolates the
schema into a SQL string — 20+ sites across `backend/`, `worker/`, `ratelimit/`,
`web/admin/`, `testing/pg.py`, and `batch.py`. Each call site runs
`_IDENT_RE.match(schema)` immediately before the f-string/`.format()` that
embeds it, so a schema that bypassed construction-time validation (e.g. one
sourced from a different code path or a test fixture) is still rejected before
it reaches the database. All user-supplied values continue to use `$N`
parameter binding; only the schema identifier is interpolated, and it is
validated at both the construction boundary and each use site.

---

## Observability Architecture

Source: `src/taskq/obs/`.

### OTel span hierarchy

```
PRODUCER span: "send <queue>" (SpanKind.PRODUCER)
  → trace_id + span_id stored on jobs row at enqueue

DISPATCH span: "dispatch" (SpanKind.INTERNAL)
  → wraps the dispatch_batch SQL call

CONSUMER span: "process <actor>" (SpanKind.CONSUMER)
  → linked to PRODUCER span via trace_id/span_id from job row
  → wraps the full actor execution (payload validation → terminal write)
```

The consumer span is linked (not a child) to the producer span, matching
messaging semconv: the producer and consumer are separate traces that happen
to be causally related.

### OTel metric names

| Metric | Kind | Description |
|---|---|---|
| `taskq.dispatch.duration` | Histogram | SQL-execution latency for dispatch_batch |
| `messaging.process.duration` | Histogram | Full actor execution duration |
| `messaging.client.consumed.messages` | Counter | Count of completed jobs by actor/queue/outcome |
| `taskq.backpressure.errors` | Counter | `MaxPendingExceededError` count by actor/kind |
| `taskq.deadline_exceeded_sweep.jobs_failed` | Counter | Jobs failed by the deadline sweep |
| `taskq.cancellation.requested` | Counter | Bumped once per `JobsClient.cancel()` call (regardless of outcome) |
| `taskq.cancellation.phase_transitions` | Counter | Cancel phase changes |
| `taskq.notify.received` | Counter | NOTIFY callbacks from asyncpg |
| `taskq.notify.reconnects` | Counter | NOTIFY connection reconnects |
| `taskq.notify.connected` | Observable Gauge | 1 if NOTIFY listener healthy |
| `taskq.maintenance_leader.is_leader` | Observable Gauge | 1 on elected pod |

The table above is illustrative, not exhaustive — the codebase defines 25+ instruments. For the complete list, see `src/taskq/obs/_otel.py` and the worker observability modules in `src/taskq/worker/` (`notify.py`, `cancel.py`, `leader.py`, `_leader_shared.py`, `heartbeat.py`).

### structlog context propagation

`bind_job_context` adds `job_id`, `actor`, `queue`, `attempt`, `identity_key`,
and `trace_id` to the structlog context for the duration of a job execution. Every
log line emitted inside an actor or consumer path carries these fields automatically.

### Vendor-neutral design

TaskQ never imports Sentry, Datadog, PostHog, or App Insights SDKs. All
observability is emitted via OTLP. Point `OTEL_EXPORTER_OTLP_ENDPOINT` at
whichever backend's collector is in the stack.

---

## Key Invariants

These invariants must remain true across all changes.

1. **`lock_lease >= 4 × heartbeat_interval`** — the lock lease must outlive
   several heartbeat intervals so a slow heartbeat tick does not expire the lock
   before the next renewal arrives.

2. **PG-write before task.cancel()** — in the phase-2 cancel path, the
   `CANCEL_ESCALATION_SQL` UPDATE is executed and the `job_events` row is
   inserted BEFORE `active.task.cancel()` is called, with no intervening `await`.
   If the write fails, the exception propagates and `task.cancel()` is never
   called — the job retains phase 1 and the heartbeat retries on the next tick.

3. **Terminal writes own their row** — `mark_succeeded`, `mark_failed_or_retry`,
   `mark_cancelled`, `mark_abandoned` all guard with
   `WHERE status = 'running' AND locked_by_worker = $worker_id`. A rowcount of 0
   means the write was a no-op (concurrent writer already moved the row).
   `WorkerOwnershipMismatch` is raised for unexpected ownership failures.

4. **Schema identifier validation is defence-in-depth, not single-point** —
   `PostgresBackend.__init__` validates `schema_name` against `_IDENT_RE` once
   at construction, and every call site that interpolates the schema into SQL
   re-validates it independently (20+ sites). asyncpg cannot bind identifiers
   as parameters, so interpolation is unavoidable; the redundant per-site
   checks ensure a schema reaching SQL through any path is always rejected if
   it is not a plain `[A-Za-z_][A-Za-z0-9_]*` identifier. All user-supplied
   values use `$N` parameter binding.

5. **`ABANDON_PENDING` is in-process only** — `CancelPhase.ABANDON_PENDING = 3`
   is never written to PG. `parse_cancel_phase(value)` raises `ValueError` if it
   encounters value `3` from a PG row.

6. **`InMemoryBackend` is single-threaded** — do not share an `InMemoryBackend`
   across threads or event loops. The single-writer contract is enforced by
   documentation; the `_single_threaded()` guard is a no-op.

7. **Migration files are append-only** — never modify an applied migration.
   The migration runner stores a checksum of each applied file in
   `schema_migrations` and rejects re-runs with a checksum mismatch.

8. **`BACKEND_PROTOCOL_VERSION` is checked at import time** — both
   `PostgresBackend` and `InMemoryBackend` assert the version constant at module
   load, not at runtime. A version bump without updating both implementations
   raises `RuntimeError` on import, not on the first query.
