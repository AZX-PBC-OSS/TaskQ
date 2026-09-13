# Troubleshooting

A problem-solution reference for common TaskQ operational issues. Each entry covers **symptom**, **cause**, **diagnosis**, and **fix**. Replace `{schema}` in SQL queries with your `TASKQ_SCHEMA_NAME` (default `taskq`).

---

## 1. Jobs stuck in `pending`

### Symptom

Jobs show `status = 'pending'` but no worker picks them up. The pending count grows without bound.

### Cause

| Cause | Detail |
|---|---|
| No worker running | No `taskq worker` process is consuming the queue. |
| Wrong queue name | The actor declares `@actor(queue="email")` but the worker's `TASKQ_QUEUES` does not include `email`. |
| Actor not in registry | The job's `actor` field matches no `ActorRef.name` in the registry. The consumer logs `dispatch-actor-not-found` and leaves the row in `running` until the lock expires. |
| Stranded jobs | The actor was removed from the registry but jobs still reference it — no `actor_config` row exists. |
| `max_concurrent` saturated | All dispatch slots for the actor are occupied by in-flight jobs. |

### Diagnosis

```sql
SELECT queue, actor, count(*) AS cnt
FROM {schema}.jobs WHERE status = 'pending'
GROUP BY queue, actor ORDER BY cnt DESC;
```

Check actor config, stranded jobs, and whether any worker is consuming:

```sql
SELECT ac.actor, ac.queue, ac.max_concurrent, ac.max_pending
FROM {schema}.actor_config ac ORDER BY ac.actor;

SELECT j.actor, count(*) AS stranded FROM {schema}.jobs j
WHERE j.status IN ('pending','scheduled')
  AND NOT EXISTS (SELECT 1 FROM {schema}.actor_config ac WHERE ac.actor = j.actor)
GROUP BY j.actor;

SELECT id, hostname, pid, last_seen_at FROM {schema}.workers ORDER BY last_seen_at DESC;
```

Check worker logs for `dispatch-actor-not-found` or `stranded-jobs-no-actor-config`. For the "wrong queue name" cause, the worker also logs `actors-on-unconsumed-queues` **at bootstrap** when a registered actor targets a queue that worker does not consume — see [workers.md](workers.md#actors-on-unconsumed-queues) for the warning's semantics (it fires even in legitimate split-queue topologies). A worker logging `worker-consumes-no-queues` at bootstrap has an empty `TASKQ_QUEUES` and will never dispatch anything — see [workers.md](workers.md#worker-consumes-no-queues).

### Fix

- **No worker:** start one — `taskq worker --actors myapp.actors:registry`.
- **Wrong queue:** add the actor's queue — `TASKQ_QUEUES=default,email taskq worker --actors myapp.actors:registry`.
- **Actor not in registry:** ensure the actor is decorated with `@actor` and exported from the registry module. Verify the `module:attr` string resolves to a `Mapping[str, ActorRef]` or `Iterable[ActorRef]` at import time.
- **Stranded jobs:** re-add the actor to the registry and restart, or cancel the orphaned jobs via `JobsClient.cancel()`. The detector only warns — it does not delete or reassign.
- **`max_concurrent` saturated:** run `taskq actor-config set <actor> --max-concurrent N` — takes effect on the next dispatch cycle, no restart. See [workers.md](workers.md#actorconfig-sync).
- **Generator registry:** if the registry attribute is a generator, the CLI iterates it twice and silently builds an empty registry. Use a `list`, `tuple`, or `dict` instead.

---

## 2. Jobs stuck in `scheduled`

### Symptom

Jobs remain `scheduled` even though their `scheduled_at` has passed.

### Cause

The `scheduled_to_pending` sweep (Sweep 3) runs every 1 second **on the leader only**, promoting `scheduled` jobs to `pending` when `scheduled_at <= clock_timestamp()`. If no leader is elected, jobs are never promoted. (`scheduled_at` still in the future is expected, not a bug.)

| Cause | Detail |
|---|---|
| No leader elected | No worker holds the `taskq:maintenance_leader` advisory lock. |
| Leader process died | Watchdog released the lock but no other worker has won election. |
| PgBouncer in transaction mode | `leader_conn` drops the session-scoped advisory lock between transactions. |
| Sweep times out under a large backlog | A leader **is** healthy, but the sweep cannot finish inside `dispatcher_command_timeout` and retries forever. See the warning below. |

!!! danger "Large `scheduled` backlog + healthy leader = sweep livelock ([#102](https://github.com/AZX-PBC-OSS/TaskQ/issues/102))"
    The sweep writes one `job_events` row per promoted job in a sequential loop
    inside a single transaction, bounded by one `dispatcher_command_timeout`
    deadline (default `5.0s`). Once the due-row count exceeds what that many
    sequential round-trips fit in the deadline, the sweep **can never commit**:
    it times out, rolls back, retries, and times out again. Nothing is promoted,
    and because nothing drains, the backlog grows — the failure is
    self-reinforcing rather than self-correcting.

    Order-of-magnitude: at ~0.5 ms RTT roughly 10k rows already exceeds a 5s
    deadline; on managed Postgres at 1–3 ms RTT the cliff arrives several times
    sooner.

    **Recognising it** — this is the dangerous part, because the fleet looks
    healthy from the outside. Workers heartbeat normally, dispatch runs cleanly
    reporting `count: 0`, and no job is in a failed state. The signature is all
    four of:

    - `scheduled-wake-failed` with `"error":"TimeoutError()"` repeating on the
      leader about **once a second** — `_scheduled_wake_loop` sleeps `1.0s`
      between ticks, so a doomed transaction is opened, times out, rolls back
      and is retried at that cadence
    - workers heartbeating normally (`last_seen_at` fresh)
    - dispatch logging `count: 0` — there is genuinely nothing `pending`
    - the `scheduled` overdue count **flat or growing**, never falling

    The metrics surface will not tell you either: `taskq_active_jobs 0` with
    `taskq_is_leader 1` is exactly what a healthy idle fleet reads. The overdue
    `scheduled` count is the only signal that distinguishes the two, so it is
    the thing worth alerting on.

    ```
    {"kind":"scheduled_wake_failed","error":"TimeoutError()","logger":"taskq.worker.leader","event":"scheduled-wake-failed"}
    ```

    Do not look for a `leader-retry` line alongside it: that event belongs to
    the *election* loop and is logged by a worker that did **not** win the
    lock, with `next_retry_secs` = `heartbeat_interval`. It is unrelated to
    sweep failure, and its presence in a fleet with a healthy leader is normal.

    Sweep 2 (`sweep_deadline_exceeded`) fails differently and needs its own
    alert: it runs in a different loop with no `asyncio.timeout` wrapper — its
    bound comes from the pool's `command_timeout` — and it logs
    `sweep_deadline_exceeded_failed`, not `scheduled_wake_failed`. Alerting on
    only the event in this section's title would miss a mass-expiry cohort
    hitting the same underlying shape.

    Any workload that accumulates a five-figure `scheduled` cohort can reach
    this: deferred jobs, retry-heavy actors, a paused-then-resumed fleet, or a
    concurrency increase that reschedules a large backlog at once.

    **Mitigation** (until #102 lands): drain the cohort in bounded batches so
    each promotion commits well inside the deadline, rather than raising
    `dispatcher_command_timeout` — which
    [cannot be raised far](configuration.md#dispatcher-command-timeout-vs-staleness-budget-watchdog-on)
    and will fail settings validation and crash-loop the worker.

    ```sql
    -- Promote a bounded slice, mirroring Sweep 3's own transition.
    -- Repeat until the overdue count reaches zero.
    WITH snap AS (
        SELECT id FROM {schema}.jobs
        WHERE status = 'scheduled' AND scheduled_at <= clock_timestamp()
        ORDER BY scheduled_at
        LIMIT 1000
        FOR UPDATE SKIP LOCKED
    )
    UPDATE {schema}.jobs j SET status = 'pending'::{schema}.job_status
    FROM snap WHERE j.id = snap.id;

    -- Then wake the producers (default schema shown; the channel is
    -- taskq_wake_<schema>).
    SELECT pg_notify('taskq_wake_taskq', '');
    ```

    `FOR UPDATE SKIP LOCKED` keeps this from contending with the leader's own
    retrying sweep. Verify the overdue count falls between runs. This skips the
    per-row `job_events` audit rows the sweep would have written — the state
    transition itself is complete and correct, but those promotions will not
    appear in job event history.

### Diagnosis

```sql
SELECT ml.worker_id, w.hostname, w.pid, ml.last_seen_at
FROM {schema}.maintenance_leader ml
JOIN {schema}.workers w ON ml.worker_id = w.id;

SELECT actor, count(*) AS overdue FROM {schema}.jobs
WHERE status = 'scheduled' AND scheduled_at <= clock_timestamp()
GROUP BY actor;
```

No rows from the first query = no leader. Check the admin UI at `/admin/leader` — a healthy leader shows `last_seen_at` within 30s of now.

### Fix

- **No leader:** ensure at least one worker is running. Failover SLA is `heartbeat_interval + 1s`.
- **PgBouncer:** set `TASKQ_PG_DSN_DIRECT` to bypass PgBouncer. See [PgBouncer compatibility](workers.md#pgbouncer-compatibility).
- **Stale leader:** force-release the advisory lock by terminating the backend:

```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE query LIKE '%pg_try_advisory_lock%taskq:maintenance_leader%';
```

!!! warning
    Only use `pg_terminate_backend` when the leader is confirmed stale (no `last_seen_at` update for > 60s). It forces an election cycle.

---

## 3. Jobs in `crashed` state

### Symptom

Jobs appear with `status = 'crashed'` and `error_class = 'WorkerCrashed'` or `error_class = 'HeartbeatLost'`.

### Cause

| `error_class` | Mechanism | Trigger |
|---|---|---|
| `WorkerCrashed` | Reclaim sweep (Sweep 1) | Worker died (OOM, SIGKILL, eviction). `lock_expires_at` passed; the sweep reclaimed the job. |
| `HeartbeatLost` | `isolate_self` | Heartbeat failed > `max_heartbeat_failures` times. Worker self-isolated and shut down. |

Both transition `running → crashed` only when the job is **non-retryable** (`retry_kind = 'non_retryable'` or `attempt >= max_attempts`). Retryable jobs are re-pended with `scheduled_at = clock_timestamp() + 5s`.

### Diagnosis

```sql
SELECT id, actor, attempt, max_attempts, error_class, error_message, finished_at
FROM {schema}.jobs WHERE status = 'crashed'
ORDER BY finished_at DESC LIMIT 20;

SELECT j.locked_by_worker, w.hostname, w.pid, w.last_seen_at,
       clock_timestamp() - w.last_seen_at AS stale_for
FROM {schema}.jobs j
LEFT JOIN {schema}.workers w ON j.locked_by_worker = w.id
WHERE j.id = $1;
```

Check container/OS logs for OOM kills or SIGKILL on the worker host.

### Fix

- **OOM kills:** increase the container memory limit or reduce `TASKQ_MAX_CONCURRENCY`.
- **Retry crashed jobs:** use the admin UI Retry button (`TASKQ_ADMIN_ACTIONS_ENABLED=true`) or `backend.retry_job()`.
- **Prevent recurrence:** set `retry_kind="transient"` with appropriate `max_attempts` so the sweep re-pends instead of crashing. The reclaim sweep runs on **every worker** (not just the leader) using `FOR UPDATE SKIP LOCKED`.

---

## 4. Jobs in `abandoned` state

### Symptom

Jobs appear with `status = 'abandoned'`. The event log shows a `state_change` with `cancel_phase_from=2`.

### Cause

The job was cancelled but the actor did not exit within the combined grace period. The three-phase protocol escalated:

| Phase | Trigger | Worker action |
|---|---|---|
| `COOPERATIVE` (1) | `cancel()` writes cancel flag | Sets `ctx.cancel_event`; actor may return cooperatively |
| `FORCED` (2) | `cancellation_grace_period` (default 30s) elapsed | Writes `cancel_phase=2`, calls `task.cancel()` |
| `ABANDON_PENDING` (3) | `cleanup_grace_period` (default 10s) elapsed | Queues job for `mark_abandoned` post-transaction |

Total time before abandonment: `cancellation_grace_period + cleanup_grace_period` (default 40s).

### Diagnosis

```sql
SELECT id, status, cancel_phase, cancel_requested_at, finished_at
FROM {schema}.jobs WHERE id = $1;

SELECT kind, detail, created_at FROM {schema}.job_events
WHERE job_id = $1 ORDER BY created_at DESC;
```

Check whether the actor suppresses `asyncio.CancelledError` — a `try/except asyncio.CancelledError: pass` pattern prevents the forced-cancel path from working.

### Fix

- **Always re-raise `asyncio.CancelledError`:** never swallow it. Let it propagate so the consumer can call `mark_cancelled`.
- **Check cancellation boundaries:** ensure the actor observes `ctx.cancellation_requested` at natural loop boundaries. For single long `await` calls, use `ctx.cancel_event.wait()`.
- **Increase grace periods:** if the actor needs more cleanup time, raise `TASKQ_CANCELLATION_GRACE_PERIOD` and `TASKQ_CLEANUP_GRACE_PERIOD`. Constraints: `cancellation + cleanup < lock_lease` and `< termination_grace_period - 5.0`.
- **Not retryable:** `abandoned` jobs cannot be retried via `backend.retry_job()`. Only `failed`, `crashed`, and `cancelled` can be retried.

---

## 5. NOTIFY connection failures

### Symptom

Worker logs `notify-conn-error` and repeated `notify-reconnect-attempt`. Dispatch latency increases as the producer falls back to polling.

### Cause

The NOTIFY listener holds a dedicated direct connection (`notify_conn`) subscribed to `taskq_wake_{schema}`. A health-check issues `SELECT 1` every `notify_health_check_interval` (default 5s). On failure, it reconnects with bounded exponential backoff (initial 1s, doubling, max 30s). Common triggers: `pg_terminate_backend`, network partition, PgBouncer in transaction mode (LISTEN is session-scoped), or Postgres restart (`AdminShutdownError` is treated as reconnectable).

### Diagnosis

Verify the worker uses the direct DSN and check for active LISTEN connections:

```shell
echo $TASKQ_PG_DSN_DIRECT
```

```sql
SELECT pid, client_addr, state FROM pg_stat_activity
WHERE query LIKE '%LISTEN%taskq_wake%';
```

If `TASKQ_PG_DSN_DIRECT` is empty, `notify_conn` falls back to `TASKQ_PG_DSN`, which may route through PgBouncer.

### Fix

- **Set `TASKQ_PG_DSN_DIRECT`** to a direct Postgres endpoint that bypasses PgBouncer.
- **Wait for reconnection:** the listener auto-reconnects with backoff. After reconnect, it re-registers LISTEN and fires a simulated wake notify to drain jobs that arrived while disconnected.
- **Reduce health-check interval:** set `TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL=2.0` for faster detection.
- **Disable NOTIFY:** if your environment cannot maintain a long-lived direct connection, set `TASKQ_NOTIFY_ENABLED=false` to use poll-only dispatch with `TASKQ_POLL_INTERVAL` (default 1.0s). Trades latency for resilience.

---

## 6. Migration errors

### Symptom

Worker fails to start, `taskq migrate up` reports errors, or queries raise `UndefinedTableError`.

### Cause

| Error | Detail |
|---|---|
| Checksum mismatch | An applied migration file was modified after recording. Runner logs `migration-checksum-drift`. |
| Forward-only constraint | No `down` operation. Reverting requires a database backup restore. |
| Schema not migrated | `schema_migrations` table or TaskQ tables do not exist. |
| Concurrent migration races | Two workers starting simultaneously both attempt migrations. |

### Diagnosis

```shell
taskq migrate status
```

```sql
SELECT version, checksum FROM {schema}.schema_migrations ORDER BY version;
SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}';
```

Search worker logs for `migration-checksum-drift`.

### Fix

- **Schema not migrated:** `taskq migrate up` against the correct `TASKQ_PG_DSN` and `TASKQ_SCHEMA_NAME`.
- **Checksum mismatch:** restore the original migration file from git. Migration files are append-only — never modify an applied migration. If intentional, restore the database from backup and re-apply. Checksums are SHA-256 of the rendered SQL; a mismatch risks silent query failures at runtime.
- **Forward-only revert:** restore from a pre-migration backup snapshot. There is no rollback.
- **Concurrent races:** `apply_pending_locked` uses `pg_advisory_lock(1234567)` to serialize. If stuck (a worker crashed mid-migration):

```sql
SELECT pg_advisory_unlock(1234567);
```

---

## 7. Heartbeat failures

### Symptom

Worker logs `heartbeat-tick-failure` with increasing `consecutive_failures`, then `isolate-self-complete` and shutdown.

### Cause

The heartbeat loop ticks every `heartbeat_interval` (default 10s). If a tick fails (`TimeoutError`, `PostgresConnectionError`, `QueryCanceledError`, `OSError`), `heartbeat_failures` increments. When `heartbeat_failures > max_heartbeat_failures` (default 3), `isolate_self` is called: it opens a fresh direct connection, transitions running jobs (retryable → `pending` with 5s delay, non-retryable → `crashed`), writes attempts with `error_class='HeartbeatLost'`, and exits. An early warning fires at `max_heartbeat_failures // 2`.

### Diagnosis

```sql
SELECT id, hostname, pid, last_seen_at,
       clock_timestamp() - last_seen_at AS stale_for
FROM {schema}.workers ORDER BY last_seen_at DESC;
```

A healthy worker's `stale_for` should be under `heartbeat_interval` (default 10s). Check worker logs for the failure progression: `heartbeat-tick-failure` → `heartbeat-failures-approaching-limit` → `isolate-self-complete`.

### Fix

- **Connection issues:** verify `TASKQ_PG_DSN_DIRECT` resolves to a reachable Postgres. Check `heartbeat_pool_size` (default 4) is sufficient.
- **Increase tolerance:** set `TASKQ_MAX_HEARTBEAT_FAILURES` higher (e.g. `5`) to absorb transient blips. Keep `lock_lease >= 4 * heartbeat_interval`.
- **Pool exhaustion:** if `heartbeat_pool.acquire()` times out, increase `TASKQ_HEARTBEAT_POOL_SIZE`.
- **After self-isolation:** restart the worker via your process supervisor. Its running jobs were already transitioned — retryable jobs are re-pended with a 5s delay. `HeartbeatLost` is intentionally distinct from `WorkerCrashed` (Sweep 1): a heartbeat-lost worker may still be alive but partitioned.

---

## 8. Leader election issues

### Symptom

No leader is elected, maintenance sweeps do not run, or `/admin/leader` shows no leader or a stale leader.

### Cause

| Issue | Detail |
|---|---|
| No leader elected | `maintenance_leader` table is empty; no worker holds the advisory lock. |
| Stale leader | Leader died but its advisory lock was not released (TCP keepalive did not detect). |
| PgBouncer interference | `leader_conn` routes through transaction-mode pooling, silently dropping the session-scoped lock. |

### Diagnosis

```sql
SELECT * FROM {schema}.maintenance_leader;
SELECT pid, granted FROM pg_locks
WHERE locktype = 'advisory' AND mode = 'exclusive';
```

Check the admin UI at `/admin/workers` — the `is_leader` column shows which worker holds the lock.

### Fix

- **No leader:** ensure at least one worker is running with a valid `TASKQ_PG_DSN_DIRECT`. Election is attempted every `heartbeat_interval`.
- **PgBouncer:** set `TASKQ_PG_DSN_DIRECT` to bypass PgBouncer. Session-level advisory locks are silently released by transaction-mode pooling.
- **Stale leader:** if the watchdog has not detected it, force-release by terminating the backend:

```sql
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE pid IN (SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND mode = 'exclusive');
```

- **Multiple schemas:** each schema gets its own advisory lock namespace. Verify `TASKQ_SCHEMA_NAME` is consistent across all workers. Failover SLA is `heartbeat_interval + 1s`; if slower, check that `leader_conn` uses a direct DSN and the watchdog health check (every 5s) is not blocked.

---

## 9. Admin UI not loading

### Symptom

`taskq ui serve` exits with `RuntimeError`, or the UI loads but shows stale data or a "polling mode" badge.

### Cause

| Issue | Detail |
|---|---|
| Auth failure | `create_router()` raises `RuntimeError` if no `auth_dependency` and `TASKQ_ENVIRONMENT` is not `dev`/`development`. Fail-closed by default. |
| Redis not configured | `TASKQ_REDIS_URL` not set. UI falls back to polling mode — functional but less fresh. |
| `[fastapi]` extra missing | Admin UI requires the `fastapi` optional dependency. |
| Health token required | In non-dev, `taskq ui serve` fails closed if `TASKQ_HEALTH_TOKEN` is empty and `TASKQ_HEALTH_REQUIRE_TOKEN=true`. |

### Diagnosis

A `RuntimeError` with "admin UI requires auth_dependency" means the fail-closed check triggered. Check the mode badge in the top-right corner: **real-time mode** (Redis reachable), **polling mode** (no Redis), or **polling mode (Redis unavailable)**.

### Fix

- **Auth (dev):** `TASKQ_ENVIRONMENT=development taskq ui serve`.
- **Auth (production):** pass an `auth_dependency` to `create_router()`, or set `TASKQ_ADMIN_UI_REQUIRE_AUTH=false` behind a reverse proxy that enforces auth.
- **Redis:** `TASKQ_REDIS_URL=redis://redis:6379/0 taskq ui serve`.
- **Missing `[fastapi]` extra:** `uv add "taskq-py[fastapi]"`.
- **Health token:** set `TASKQ_HEALTH_TOKEN` to a strong token, or `TASKQ_HEALTH_REQUIRE_TOKEN=false` if relying on network policy.
- **Polling fallback is safe:** all pages remain functional (default 2.0s refresh).

---

## 10. Rate limiter not working

### Symptom

Rate limits are not enforced, jobs are denied with `ReservationUnavailable` unexpectedly, or rate-limit state is inconsistent across workers.

### Cause

| Issue | Detail |
|---|---|
| Redis not available | Backend raises `ConnectionError`; PG fallback (if enabled) kicks in but is slower. |
| `[redis]` extra missing | `TokenBucket(backend="redis")` without the `redis` package raises `ImportError` at acquire time. |
| In-memory backend | `backend="memory"` is per-process only — state not shared across workers. |
| Primitives not registered | Actor references names not in the `RateLimitRegistry`. DI validation raises `MissingProvider` at startup. |
| Reservation slots not synced | `reservation_slots` table has the wrong row count for the configured `slots`. |

### Diagnosis

Check the admin UI at `/admin/rate-limits` for Postgres and Redis state. Check reservation slots and snoozed jobs:

```sql
SELECT bucket_name,
       count(*) FILTER (WHERE job_id IS NOT NULL) AS held,
       count(*) FILTER (WHERE job_id IS NULL) AS free,
       count(*) AS total
FROM {schema}.reservation_slots GROUP BY bucket_name;

SELECT actor, count(*) AS snoozed FROM {schema}.jobs
WHERE status = 'scheduled' AND snooze_count > 0
GROUP BY actor;
```

### Fix

- **Redis not available:** verify `TASKQ_REDIS_URL` and connectivity. PG fallback (`TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED=true`, the default) keeps limits functional but slower.
- **Missing `[redis]` extra:** `uv add "taskq-py[redis]"`.
- **In-memory backend:** switch to `backend="redis"` or `backend="postgres"` for multi-worker deployments. Memory is for tests only.
- **Primitives not registered:** register all primitives on the `registry` singleton before the worker starts. DI validation checks each actor's `rate_limits`/`reservations` names at startup.
- **Reservation slots out of sync:** call `sync_slots()` after changing slot counts. Sustained rate limiting accumulates jobs as `snoozed` (no retry budget consumed) — monitor queue depth, as there is no built-in backpressure beyond `max_pending`.

```python
from taskq.ratelimit import sync_slots

result = await sync_slots([my_reservation], pool=pg_pool)
```

---

## 11. Worker won't start

### Symptom

The worker process exits immediately with a non-zero exit code and an error or traceback on stderr.

### Cause

| Failure | Detail |
|---|---|
| Migration not applied | TaskQ tables do not exist; queries raise `UndefinedTableError`. |
| Actor registry import error | `module:attr` does not resolve: module not found, attribute missing, or wrong type. |
| DI validation failure | `MissingProvider`, `ScopeViolation`, or `DependencyCycle` during `registry.validate()`. |
| `ActorConfigDriftList` | Registered `queue` or `metadata` differs from the stored `actor_config` row (structural drift); `--force-update-actor-config` not set. `max_concurrent` / `max_pending` / `result_ttl` never cause this — those are operator-owned and cannot drift; see [ActorConfig sync](workers.md#actorconfig-sync). |
| Timing invariant violation | `lock_lease < 4 * heartbeat_interval`, or `cancellation + cleanup >= termination_grace - 5.0` or `>= lock_lease`. |

### Diagnosis

Read the stderr output. `ActorConfigDriftList` produces a clean one-line error; other failures produce a traceback. Check migration status and actor config, and compare against your `@actor` decorator parameters (only `queue` and `metadata` participate in drift; `max_concurrent`, `max_pending`, and `result_ttl` are operator-owned once a row exists):

```shell
taskq migrate status
taskq actor-config list
taskq actor-config diff --actors myapp.actors:registry
```

```sql
SELECT actor, max_concurrent, max_pending, queue FROM {schema}.actor_config ORDER BY actor;
```

### Fix

- **Migration not applied:** `taskq migrate up`, then restart.
- **Import error:** verify the `module:attr` string resolves to a `Mapping[str, ActorRef]` or `Iterable[ActorRef]`:
  ```shell
  python -c "from myapp.actors import registry; print(type(registry))"
  ```
- **DI validation failure:** `MissingProvider` = missing provider. `ScopeViolation` = wider scope depends on narrower. `DependencyCycle` = provider cycle. Register the missing provider or fix the scope/cycle. See [dependency-injection.md](dependency-injection.md).
- **ActorConfigDriftList:** this is always a `queue` or `metadata` mismatch. Deploy the first pod with `--force-update-actor-config`, then remaining pods without it. Do not leave it set permanently. If you only meant to change `max_concurrent` / `max_pending` / `result_ttl`, you don't need this flag at all — deploy normally, then run `taskq actor-config set <actor> ...`.
- **Timing invariant violations:** adjust settings so `lock_lease >= 4 * heartbeat_interval` and `cancellation + cleanup < termination_grace - 5.0` and `< lock_lease`.

---

## 12. Performance issues

### Symptom

Dispatch throughput is lower than expected, job latency is high, or the database shows high CPU/lock contention.

### Cause

| Issue | Detail |
|---|---|
| Dispatch oversampling | `TASKQ_DISPATCH_OVERSAMPLE` (default 2) gathers `residual × oversample` candidates per actor. High oversample with many actors increases query cost. |
| Pool too small | `dispatcher_pool_size` (default 4) or `heartbeat_pool_size` (default 4) insufficient for concurrency. |
| `max_concurrent` too high | Worker spawns `max_concurrency` consumers; `worker_pool_size = int(max_concurrency * 1.5)`. Too high exhausts PG connections and event-loop capacity. |
| `max_concurrent` too low | Actor's `max_concurrent` bottlenecks throughput even when capacity is available. |
| Queue depth starvation | `strict_fifo` mode lets a deep queue of one actor starve others at the same priority. |

### Diagnosis

```sql
SELECT actor, queue, status, count(*) AS cnt
FROM {schema}.jobs WHERE status IN ('pending','scheduled','running')
GROUP BY actor, queue, status ORDER BY cnt DESC;

SELECT j.actor, count(*) AS in_flight, ac.max_concurrent
FROM {schema}.jobs j JOIN {schema}.actor_config ac ON j.actor = ac.actor
WHERE j.status = 'running'
GROUP BY j.actor, ac.max_concurrent ORDER BY in_flight DESC;
```

Check dispatch latency via OTel or the `/metrics` endpoint (`taskq health metrics | grep taskq_dispatch_duration`).

### Fix

- **Reduce oversampling:** `TASKQ_DISPATCH_OVERSAMPLE=1` if you do not use `identity_key` and run a single-producer deployment.
- **Enable scoped dispatch:** `TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE=true` filters the `per_actor_capacity` CTE to actors whose home queue is in the worker's subscribed list. Lowers probe count but excludes `enqueue(queue=...)` override jobs.
- **Tune pool sizes:** increase `TASKQ_DISPATCHER_POOL_SIZE` and `TASKQ_HEARTBEAT_POOL_SIZE` if `acquire()` timeouts appear. Keep `worker_pool_size` derived.
- **Tune `max_concurrent`:** run `taskq actor-config set <actor> --max-concurrent N` to match external resource capacity. Takes effect on the next dispatch cycle, no restart.
- **Switch to `round_robin`:** for multi-tenant queues where one tenant starves others:
  ```bash
  taskq queues set-mode multi round_robin
  ```
  Takes effect on the next dispatch cycle — no worker restart needed
  (`_resolve_queue_modes` re-reads the table every batch). In SQL, note the
  **UPSERT**: queues are implicit — no runtime path inserts a `queues` row (only
  the admin surface does, and `set-mode` above is itself an upsert), so a plain
  `UPDATE ... WHERE name = ...` matches zero rows and silently does nothing on a
  fresh deployment.
  ```sql
  INSERT INTO {schema}.queues (name, mode) VALUES ('multi', 'round_robin')
  ON CONFLICT (name) DO UPDATE SET mode = EXCLUDED.mode, updated_at = clock_timestamp();
  ```
  **This is inert on its own.** Cohorts come from `fairness_key`, which is set at
  enqueue time; with no keys every job lands in one `__null__` cohort and the
  queue behaves exactly like `strict_fifo`. See
  [workers.md — Queue dispatch modes](workers.md#queue-dispatch-modes).
- **Scale horizontally:** add worker processes. `FOR UPDATE SKIP LOCKED` prevents duplicate dispatch. Use unique `--health-socket-path` per worker on the same host.
- **Offload CPU-bound work:** the worker is asyncio-based — CPU-bound actors block the event loop. Use `run_in_executor()`. Monitor `taskq.dispatch.duration` and `messaging.process.duration` via OTel: rising dispatch duration with flat process duration = DB contention; rising process duration = actor bottleneck.

---

## 13. Worker hangs or wedged shutdown

### Symptom

The worker stops dispatching but the process stays alive, or it ignores SIGTERM and does not exit within `termination_grace_period`. `/ready` returns 503 with a `stale_loops` reason, or `loop_tick_ages` in the `/ready` body keeps growing.

### Cause

Something has wedged inside the process: a blocked event loop (CPU-bound or synchronous work inside an actor), a deadlocked interval-driven sibling loop, a sibling that returned silently while no shutdown was in progress, or a shutdown that cannot complete because a sibling is hanging the TaskGroup exit. The in-worker watchdog (`TASKQ_WATCHDOG_ENABLED=true`, the default) covers all four cases. On a trip it logs `worker-watchdog-trip` at CRITICAL (labelled by detector), dumps every live asyncio task, and force-exits with code 2. In-flight jobs are reclaimed by the leader sweep once `lock_lease` expires, and the non-zero exit makes the supervisor restart the worker.

### Diagnosis

Get the live task stacks first; they name what every task is waiting on:

```shell
# On-demand dump to the worker's log and stderr (not available on Windows):
kill -USR2 <worker-pid>

# Same payload as JSON, when the endpoint is enabled:
curl --unix-socket /tmp/taskq_health.sock http://localhost/tasks
```

- `GET /tasks` is privileged and disabled by default (`TASKQ_HEALTH_TASKS_ENABLED=false`): the dump reveals code structure, file paths, and task names (never locals or payload values). Enabling it also tightens the health socket to mode `0600`. While disabled, the endpoint returns 404, indistinguishable from a missing route.
- Read `loop_tick_ages` and `shutdown_elapsed_seconds` in the `/ready` body to see which loop went silent and how long shutdown has been in progress.
- Correlate with the OTel metrics `taskq.worker.watchdog_trips_total` (by detector), `taskq.worker.loop_tick_age_seconds` (by loop), and `taskq.worker.sibling_crashes_total` (by loop).

### Fix

- Read the await-site frames in the dump to find the blocked call; move CPU-bound or blocking work off the event loop with `run_in_executor()`.
- If trips fire under legitimate load (large GC pauses, host starvation), raise `TASKQ_WATCHDOG_LOOP_LAG_BUDGET`, `TASKQ_WATCHDOG_TICK_GRACE_FACTOR`, or `TASKQ_WATCHDOG_STALE_FLOOR` rather than disabling the watchdog. See [configuration.md](configuration.md#watchdog-hang-and-deadlock-detection). Note these interact with `TASKQ_DISPATCHER_COMMAND_TIMEOUT`: *lowering* `TASKQ_WATCHDOG_STALE_FLOOR` or `TASKQ_WATCHDOG_TICK_GRACE_FACTOR` can shrink the staleness budget below the configured timeout and make the worker fail settings validation at startup. See [Dispatcher command timeout vs staleness budget](configuration.md#dispatcher-command-timeout-vs-staleness-budget-watchdog-on).
- Ensure the supervisor restarts on any non-zero exit; watchdog trips always exit with code 2.

---

## 14. Worker looks healthy in logs but is doing no work

### Symptom

Logs are clean — telemetry emits, no exceptions, and a shutdown (if any) looked orderly — yet no jobs complete. Dashboards built on log volume look normal.

### Cause

**Logs are not a liveness signal.** A worker can emit well-formed telemetry, log a tidy shutdown sequence, and still be dispatching nothing: the process can be alive with its consumer loops parked, isolated after heartbeat loss, holding no leader lock, subscribed to queues nothing publishes to, or capped to zero concurrency. None of those produce an error line.

**The database is the source of truth.** Worker liveness lives in `{schema}.workers.last_seen_at`, and progress lives in job state transitions. Both are observable independently of anything the worker chooses to log.

| Cause | Detail |
|---|---|
| Worker not actually registered | No row in `{schema}.workers`, or `last_seen_at` is stale — the process is up but its heartbeat is not. |
| No state transitions | Workers fresh, but no job has changed state — dispatch is finding nothing eligible (see §1) or capacity is zero. |
| Capacity pinned to zero | A stored `actor_config.max_concurrent = 0` is drain mode; a queue cap of `0` is rejected, but an actor's is not. |
| Queue mismatch | `TASKQ_QUEUES` does not include the queue jobs are enqueued on, so this worker is healthy and irrelevant. |

### Diagnosis

Never conclude from logs. Run all three:

```sql
-- 1. Which workers does the DB believe are alive? stale_for should be
--    under heartbeat_interval (default 10s).
SELECT id, hostname, pid, queues, last_seen_at,
       clock_timestamp() - last_seen_at AS stale_for
FROM {schema}.workers ORDER BY last_seen_at DESC;

-- 2. Is anything actually progressing? Empty = no work completed,
--    regardless of what the logs say.
SELECT status, count(*) FROM {schema}.jobs
WHERE finished_at > clock_timestamp() - interval '5 minutes'
GROUP BY status;

-- 3. Is capacity pinned to zero, or unset?
SELECT actor, queue, max_concurrent, max_pending FROM {schema}.actor_config
ORDER BY actor;
```

Re-run query 2 a minute apart: **unchanged counts mean no progress**, whatever the logs show. Cross-check that `workers.queues` overlaps the queues jobs are enqueued on.

### Fix

- **Stale or missing worker rows:** the process is not heartbeating — treat it as down and restart it, then see [Heartbeat failures](#7-heartbeat-failures).
- **Workers fresh but nothing progressing:** work through [Jobs stuck in `pending`](#1-jobs-stuck-in-pending) and [Jobs stuck in `scheduled`](#2-jobs-stuck-in-scheduled) — including the sweep livelock, whose whole signature is healthy workers plus zero promotion.
- **`max_concurrent = 0`:** drain mode. `taskq actor-config set <actor> --max-concurrent N` to restore; effective next dispatch cycle.
- **`max_concurrent` unexpectedly `NULL`:** the decorator literal never reached this deployment — capacity fields are seed-only. See [ActorConfig sync](workers.md#actorconfig-sync).
- **Queue mismatch:** align `TASKQ_QUEUES` with the queues actually used, and confirm via `workers.queues`.
- **Alert on the DB, not on logs:** page on `max(clock_timestamp() - last_seen_at)` across `{schema}.workers` and on job-completion throughput. A log-based liveness alert cannot detect this failure mode — it is what let it run unnoticed.

---

## 15. A job chain stops after one link

### Symptom

A self-enqueuing chain (a paging sweep, a multi-step pipeline) runs its first job and stops. The
first job is `succeeded`, no job is `failed`, and the successor **does not exist** — not `pending`,
not `scheduled`, no row at all. The enqueue call returned a `JobHandle` without raising.

### Cause

**`idempotency_key` dedup has no status predicate.** The insert arbitrates on
`ON CONFLICT (idempotency_scope, idempotency_key) ... DO NOTHING`, which collides against a row in
*any* status — including the predecessor's own `succeeded` row — for as long as that row survives
`prune_retention_*` (30 d succeeded/cancelled, 90 d failed/abandoned). On a collision the enqueue
returns the **existing** handle with `was_existing=True`, so the caller sees success and no job is
created.

| Cause | Detail |
|---|---|
| Successor reuses the predecessor's key | A key like `sweep:{run_id}` is identical on every link. Link 2 collides with link 1's `succeeded` row and vanishes. |
| Key component that does not advance | A key built on a row's `updated_at` never moves when the job is **cancelled** — the handler never ran — so every re-enqueue for the retention window collides with the cancelled corpse. |
| Per-parent ordinal in the key | `{run_id}:{index}` where `index` restarts per parent: link 2's item 0 collides with link 1's succeeded item 0. |
| `unique_for` on the successor actor | The successor dedups against its own still-`running` parent, which is in the default `unique_states`. |

### Diagnosis

Look for the predecessor row the successor collided with — its key is the successor's key:

```sql
-- 1. Does a terminal row already hold the key the successor would use?
SELECT id, actor, status, created_at, finished_at, idempotency_scope, idempotency_key
FROM {schema}.jobs
WHERE idempotency_key = 'sweep:RUN_ID'          -- the successor's key
ORDER BY created_at DESC;

-- 2. How many links did the chain actually create?
SELECT status, count(*), min(created_at), max(created_at)
FROM {schema}.jobs WHERE actor = 'my_sweep' GROUP BY status;

-- 3. Is the actor declaring unique_for, which would dedup against a running parent?
SELECT actor, queue, metadata FROM {schema}.actor_config WHERE actor = 'my_sweep';
```

A single `succeeded` row from query 2 with nothing after it, plus a matching row in query 1, is the
collision. In application code, `handle.was_existing is True` on a successor enqueue is the same
finding at the call site.

### Fix

- **Successor reuses the key:** put the advancing value *in* the key —
  `f"sweep:{run_id}:{next_cursor}"`. A successor's key must differ from every key its predecessors
  could have had.
- **Non-advancing component:** never key on a value the handler is responsible for advancing.
  Cancellation is the case that breaks it, because it is the one terminal status reached without the
  handler executing. Use the cursor, the link index, or an attempt axis.
- **Per-parent ordinals:** derive from something globally distinguishing — the item's own id, or the
  cursor plus the ordinal.
- **`unique_for` on the successor:** remove it. `unique_for` belongs on the chain's *root*, where
  "not while one is live" is what you want; see
  [sweeps.md — Idempotency by role](sweeps.md#idempotency-by-role).
- **Assert it in code:** treat `was_existing is True` on a successor enqueue as an error rather
  than a success — it is the only signal this failure produces.

---

## 16. A sweep reports green but nothing moves

### Symptom

Every scheduled pass succeeds, logs are clean, and dashboards show jobs completing — yet the
backlog does not shrink. Often accompanied by a log line the sweep emits about its own truncation
(`capped: true`, "more rows remain").

### Cause

| Cause | Detail |
|---|---|
| The cadence is the throughput | A fixed-page pass that returns after one page is capped at `page_size` per period forever. At 500/pass twice daily, 10,000 rows take ten days; if arrivals exceed 1,000/day it never drains. |
| `OFFSET` pagination over a shrinking set | As rows become ineligible, offsets slide onto already-covered rows while rows collapsing backwards are never visited. Measured shape: 14,500 visits over ~9,900 rows with 27% never visited. |
| Cursor over a column the walk writes | An LRU sweep cursored on `last_synced_at` re-serves rows its own children just stamped — every pass is full, the chain never terminates, throughput is zero. |
| One bad item aborts the pass | An unhandled exception from one malformed row ends the enumeration for every other row, and the retry meets the same row again. |
| Chain never started | The successor collided on a status-blind key — see [§15](#15-a-job-chain-stops-after-one-link). |

### Diagnosis

Pass success says nothing here. Measure the population instead:

```sql
-- 1. Is the eligible population falling? Run twice, minutes apart.
SELECT count(*) FROM my_eligible_view;

-- 2. How many jobs did the sweep's leaf actor actually complete per hour?
SELECT date_trunc('hour', finished_at) AS hr, status, count(*)
FROM {schema}.jobs
WHERE actor = 'my_leaf' AND finished_at > clock_timestamp() - interval '24 hours'
GROUP BY hr, status ORDER BY hr DESC;

-- 3. How many links did one run produce? One row per run = a fixed-page sweep.
SELECT count(*) AS links, min(created_at), max(created_at)
FROM {schema}.jobs WHERE actor = 'my_sweep_page';
```

Compare query 2's hourly completion count against `page_size` per period: equality is the
signature of a cadence-bound sweep. A flat or rising count in query 1 while query 2 shows steady
throughput means the walk is re-serving covered rows.

### Fix

- **Cadence-bound:** a full page must enqueue its successor immediately, and a short page must
  enqueue nothing. Treat any "capped" log line that returns as an unfinished implementation —
  [sweeps.md](sweeps.md#the-cron-period-becomes-the-throughput).
- **`OFFSET`:** switch to a keyset predicate ordered by a tuple ending in a unique column —
  [sweeps.md](sweeps.md#why-keyset-not-offset).
- **Cursor on a written column:** cursor on an immutable key and express freshness in the
  *selection* predicate, or page a run-scoped snapshot of the eligible ids —
  [sweeps.md](sweeps.md#the-cursor-column-rule).
- **One bad item:** wrap each item in `try`/`except` and continue, keeping the successor enqueue
  *outside* that block so a failed enqueue still raises. Emit a skip counter — a rising skip count
  is coverage loss a green status hides.
- **Alert on the population, not the pass:** page on the eligible-population depth and its trend.
  A pass-success alert cannot detect any cause in this table.

---

## 17. Raising a concurrency cap changed nothing

### Symptom

A concurrency limit was raised — in the `@actor` decorator, via `taskq actor-config set`, on the
queue, or in `TASKQ_MAX_CONCURRENCY` — and observed throughput is unchanged. No error, no warning,
and the in-flight count sits at exactly the old ceiling.

### Cause

The effective limit is the **minimum** across four independent scopes
([workers.md](workers.md#concurrency-model)), so raising a non-binding one has no effect. Each
layer also has its own reason for ignoring a change:

| Cause | Detail |
|---|---|
| Decorator literal, existing row | Capacity fields are **seed-only**: `max_concurrent` / `max_pending` / `result_ttl` populate the `actor_config` row on first registration and are never rewritten. A differing literal logs `actor-config-capacity-override` at **info** level and is otherwise ignored — `--force-update-actor-config` does not help, it only overrides *structural* drift. |
| Another layer binds lower | A queue cap, a `ConcurrencyReservation`, or the per-process bound is below the value you raised. |
| Queue cap not restarted | Queue caps are read **once at worker startup**, not per dispatch cycle. |
| Deployed env differs from code | An image deploy does not update container environment variables, so a changed default in the repo and the value the pod actually sets are two separate facts. |
| Workgroup child overrides the env | The supervisor passes `--max-concurrency` from the TOML on each child's command line, which overwrites the loaded setting — `TASKQ_MAX_CONCURRENCY` on a workgroup pod is ignored by every child. |
| Not a concurrency problem | Provider `429`s, a rate limiter, or a reservation are throttling the work; more slots cannot help. |

### Diagnosis

Read the stored values rather than the code, and compare in-flight against each ceiling:

```sql
-- 1. What the fleet actually enforces per actor (NULL = uncapped).
SELECT actor, queue, max_concurrent, max_pending FROM {schema}.actor_config ORDER BY actor;

-- 2. The queue-level strict cap.
SELECT name, mode, max_concurrent FROM {schema}.queues ORDER BY name;

-- 3. In-flight vs the actor ceiling — equality identifies the binding layer.
SELECT j.actor, count(*) AS in_flight, ac.max_concurrent
FROM {schema}.jobs j LEFT JOIN {schema}.actor_config ac ON ac.actor = j.actor
WHERE j.status = 'running' GROUP BY j.actor, ac.max_concurrent ORDER BY in_flight DESC;

-- 4. Are jobs waiting on a limiter rather than a slot?
SELECT actor, count(*) FROM {schema}.jobs
WHERE status = 'scheduled' AND metadata ? 'awaiting' GROUP BY actor;
```

Then check the *running container's* environment for `TASKQ_MAX_CONCURRENCY` — not the repository
default — and the workgroup TOML if one is in use. Worker logs carry
`actor-config-capacity-override` at info level when a literal was ignored.

### Fix

- **Seed-only capacity:** tune the stored row — `taskq actor-config set <actor> --max-concurrent N`
  — which takes effect on the next dispatch cycle with no restart. To make code the source of
  truth instead, converge the values at boot and accept that a live operator tune reverts on the
  next deploy ([ops.md](ops.md#capacity-ownership-the-seed-only-trap)).
- **A stored `NULL`:** for `max_concurrent` that means *uncapped*, not "use the literal" — treat
  every NULL in query 1 as a decision nobody made.
- **Queue cap:** restart the workers after changing it.
- **Env drift:** fix the deployment's environment (or manifest); a code change alone cannot move
  it. See [workers.md](workers.md#finding-the-layer-that-actually-binds).
- **Workgroup:** edit `[[workers]] max_concurrency` in the TOML and restart the supervisor.
- **Throttled, not capped:** a provider rate limit is not a concurrency problem. Raising slots
  increases denials; adjust the limiter or the provider quota instead
  ([rate-limiting.md](rate-limiting.md)).
- **Never lower a queue cap to protect one actor:** it applies to every actor on the queue and
  starves the rest. Put the constraint on the actor — see the warning in
  [workers.md](workers.md#concurrency-model).

---

## 18. A cancelled job's work never reprocesses

### Symptom

A job was cancelled (deliberately, by a drain, or by cancel escalation) and the underlying work
item is never picked up again. Re-triggering it appears to succeed but creates no job. The row
stays stuck for days while the sweep or trigger that should re-enqueue it reports success on every
pass.

### Cause

**A cancelled job's row still owns its idempotency key, and the handler never ran.** Dedup is
status-blind, so the cancelled row absorbs every re-enqueue until it is pruned — 30 days by default
for `cancelled`. This is worse than the `succeeded` case because the work was never done, so the
collision is pure work loss.

| Cause | Detail |
|---|---|
| Key derived from a value the handler advances | Keying on the row's `updated_at` (or a version the handler bumps) means cancellation leaves the key unchanged forever — the corpse swallows every retrigger. |
| Fixed business key, no attempt axis | `f"extract:{file_id}"` collides with the cancelled row for the whole retention window. |
| Re-posting after any terminal status | The same applies to `failed`: a post-fix rerun of a failed batch silently no-ops onto the dead jobs. |

### Diagnosis

```sql
-- 1. Is a terminal row holding the key the retrigger would use?
SELECT id, actor, status, created_at, finished_at, attempt, idempotency_key
FROM {schema}.jobs
WHERE idempotency_key = 'extract:FILE_ID';

-- 2. Cancelled or failed rows that have never been superseded.
SELECT actor, status, count(*), min(finished_at) AS oldest
FROM {schema}.jobs
WHERE status IN ('cancelled', 'failed', 'crashed')
GROUP BY actor, status ORDER BY oldest;
```

A single `cancelled` row in query 1, with no later row for the same key, is the collision. At the
call site the retrigger returns `was_existing=True`.

### Fix

- **Add an axis that moves on a retrigger:** an attempt or epoch counter you own
  (`f"extract:{file_id}:{attempt}"`), or an `idempotency_scope` carrying the run
  (`idempotency_scope=run_id`) so a later run can reprocess the same item without waiting for
  prune.
- **Never key on a handler-advanced value:** cancellation is the case that breaks it, because it is
  the terminal status reached *without* the handler executing.
- **For a one-off recovery:** cancel is not undoable and the key is not reusable — re-enqueue under
  a new key (or a new scope). Do not delete rows from `{schema}.jobs` to free a key.
- **Design rule:** [sweeps.md](sweeps.md#version-components-that-do-not-version) has the general
  test — can this key component stay the same across two enqueues that must both produce a job?

---

## 19. `job_events` and `job_attempts` grow without bound

### Symptom

The `taskq` schema becomes the largest thing in the database and keeps growing, while
job throughput is unremarkable. `job_events` runs to millions of rows. `jobs_archive`
is empty or tiny, so it looks like `prune` has never reclaimed anything.

A production deployment measured 2,074,421 `reservation_denied` attempts against
168,963 successes — a **12.3:1 ratio** — for 2.5 GB across `job_events` (1,602 MB) and
`job_attempts` (911 MB): **32% of a 7.85 GB database was denial bookkeeping.**

### Cause

A job denied a `ConcurrencyReservation` slot writes **four durable rows per denial
cycle** — one `job_attempts` row with `outcome='reservation_denied'` plus three
`job_events` transitions (`running→scheduled`, `scheduled→pending`, `pending→running`)
— and then tries again. Measured at ~1,193 bytes per denial.

Two properties make that unbounded rather than self-limiting:

1. **A denial does not consume retry budget.** `mark_snoozed`'s `SET` list contains no
   `attempt` assignment, and `attempt` is incremented only by the dispatch lease — so
   both sides of `attempt < max_attempts` advance together and the gap is invariant. The
   failure gate is unreachable via denials. The only terminal exit is `deadline_failed`,
   which requires `schedule_to_close`: **a job without `schedule_to_close` can be denied
   forever.** One production job reached `attempt=1015/1018` over 6h12m before
   succeeding.
2. **`prune` cannot reach the rows.** `prune_terminal_jobs` keys on
   `status = ANY(TERMINAL_STATUSES) AND finished_at < cutoff`. A denial-looping job is
   never terminal and has `finished_at` reset to `NULL` on every denial, so the FK
   cascade that removes `job_events` never fires. `job_events` also has no archive table
   and appears in no expiry CTE — note the asymmetry, since `job_attempts` *is*
   preserved into `job_attempts_archive`.

There is no setting that suppresses the event trail: the three inserts are
unconditional and run inside `asyncio.shield`.

### Diagnosis

```sql
-- Denial-to-success ratio, and which actors are paying it.
SELECT j.queue, a.outcome, count(*) AS attempts, count(DISTINCT j.actor) AS actors
FROM taskq.job_attempts a JOIN taskq.jobs j ON j.id = a.job_id
WHERE a.outcome IN ('reservation_denied', 'succeeded')
GROUP BY 1, 2 ORDER BY 3 DESC;

-- Attempts per job: ~1.0 is healthy, >2 means churn.
SELECT j.actor, count(*) AS attempts, count(DISTINCT j.id) AS jobs,
       round(count(*)::numeric / nullif(count(DISTINCT j.id), 0), 3) AS per_job
FROM taskq.job_attempts a JOIN taskq.jobs j ON j.id = a.job_id
GROUP BY 1 HAVING count(*) > 1000 ORDER BY per_job DESC;

-- Is any of it prunable? Rows hanging off a non-terminal job never are.
SELECT j.status, count(e.*) AS events, pg_size_pretty(sum(pg_column_size(e.*))) AS sz
FROM taskq.job_events e JOIN taskq.jobs j ON j.id = e.job_id
GROUP BY 1 ORDER BY 2 DESC;
```

### Fix

The churn is **oversubscription**, not load. Excess admission pressure is
`Σ(per-actor max_concurrent) ÷ queue ceiling`; denial rate scales with it. One
production tier ran 389 actor slots against a ceiling of 20 (**19.4×**) and minted
~1.06M denials/day. Splitting the socket-bound actors onto their own tier took the same
fleet to **1.60×** and **6,341 denials/day** — a 167× reduction, 1.27 GB/day to ~8 MB/day.

- **Separate tiers by what a job OCCUPIES**, not by how expensive it feels. An LLM call
  holds a socket, not a core; putting it on a CPU tier sized for OCR is what creates the
  excess pressure.
- **Prefer a blocking limiter to a releasing one for rate limits.** An actor that
  `await asyncio.sleep()`s *inside* the job holds its slot and **cannot** generate a
  denial. An actor that releases its slot and re-queues is what mints them. This is the
  bound that matters — not the size of the ceiling.
- **Set `schedule_to_close`** on anything that claims a reservation, so a denial loop has
  a terminal exit at all.
- **Do not expect retention to reclaim a live loop.** Retention
  (`TASKQ_PRUNE_RETENTION_SUCCEEDED`, default 30d) only helps once the owning jobs reach
  a terminal status; rows under a still-looping job have no reachable cleanup path.

---

## See also

- [sweeps.md](sweeps.md) — the page/fan-out/recurse pattern, cursor rules, and idempotency by role (the preventive counterpart of entries 15, 16 and 18)
- [ops.md](ops.md) — operations & adoption guide: sizing, timeout policy, fan-out patterns, and the footgun index (the preventive counterpart of this page)
- [workers.md](workers.md) — worker internals, settings, PgBouncer
- [cancellation.md](cancellation.md) — cancellation protocol
- [rate-limiting.md](rate-limiting.md) — rate-limit backends
- [admin-ui.md](admin-ui.md) — admin UI routes and auth
- [observability.md](observability.md) — OTel metrics and logging
- [architecture.md](../architecture.md) — state machine, dispatch, leader election
