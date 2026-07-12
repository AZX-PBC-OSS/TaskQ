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

Check worker logs for `dispatch-actor-not-found` or `stranded-jobs-no-actor-config`.

### Fix

- **No worker:** start one — `taskq worker --actors myapp.actors:registry`.
- **Wrong queue:** add the actor's queue — `TASKQ_QUEUES=default,email taskq worker --actors myapp.actors:registry`.
- **Actor not in registry:** ensure the actor is decorated with `@actor` and exported from the registry module. Verify the `module:attr` string resolves to a `Mapping[str, ActorRef]` or `Iterable[ActorRef]` at import time.
- **Stranded jobs:** re-add the actor to the registry and restart, or cancel the orphaned jobs via `JobsClient.cancel()`. The detector only warns — it does not delete or reassign.
- **`max_concurrent` saturated:** increase the actor's `max_concurrent` in `@actor(...)`, then deploy with `--force-update-actor-config` on the first pod. See [workers.md](workers.md#actorconfig-sync).
- **Generator registry:** if the registry attribute is a generator, the CLI iterates it twice and silently builds an empty registry. Use a `list`, `tuple`, or `dict` instead.

---

## 2. Jobs stuck in `scheduled`

### Symptom

Jobs remain `scheduled` even though their `scheduled_at` has passed.

### Cause

The `scheduled_to_pending` sweep (Sweep 3) runs every 1 second **on the leader only**, promoting `scheduled` jobs to `pending` when `scheduled_at <= now()`. If no leader is elected, jobs are never promoted. (`scheduled_at` still in the future is expected, not a bug.)

| Cause | Detail |
|---|---|
| No leader elected | No worker holds the `taskq:maintenance_leader` advisory lock. |
| Leader process died | Watchdog released the lock but no other worker has won election. |
| PgBouncer in transaction mode | `leader_conn` drops the session-scoped advisory lock between transactions. |

### Diagnosis

```sql
SELECT ml.worker_id, w.hostname, w.pid, ml.last_seen_at
FROM {schema}.maintenance_leader ml
JOIN {schema}.workers w ON ml.worker_id = w.id;

SELECT actor, count(*) AS overdue FROM {schema}.jobs
WHERE status = 'scheduled' AND scheduled_at <= now()
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

Both transition `running → crashed` only when the job is **non-retryable** (`retry_kind = 'non_retryable'` or `attempt >= max_attempts`). Retryable jobs are re-pended with `scheduled_at = now() + 5s`.

### Diagnosis

```sql
SELECT id, actor, attempt, max_attempts, error_class, error_message, finished_at
FROM {schema}.jobs WHERE status = 'crashed'
ORDER BY finished_at DESC LIMIT 20;

SELECT j.locked_by_worker, w.hostname, w.pid, w.last_seen_at,
       now() - w.last_seen_at AS stale_for
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
       now() - last_seen_at AS stale_for
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
| `ActorConfigDriftList` | Registered config differs from stored `actor_config` rows; `--force-update-actor-config` not set. |
| Timing invariant violation | `lock_lease < 4 * heartbeat_interval`, or `cancellation + cleanup >= termination_grace - 5.0` or `>= lock_lease`. |

### Diagnosis

Read the stderr output. `ActorConfigDriftList` produces a clean one-line error; other failures produce a traceback. Check migration status and actor config, and compare against your `@actor` decorator parameters:

```shell
taskq migrate status
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
- **ActorConfigDriftList:** deploy the first pod with `--force-update-actor-config`, then remaining pods without it. Do not leave it set permanently.
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
- **Tune `max_concurrent`:** set the actor's `max_concurrent` to match external resource capacity. Re-deploy with `--force-update-actor-config` on the first pod.
- **Switch to `round_robin`:** for multi-tenant queues where one tenant starves others:
  ```sql
  UPDATE {schema}.queues SET mode = 'round_robin' WHERE name = 'multi';
  ```
  Takes effect on the next worker restart.
- **Scale horizontally:** add worker processes. `FOR UPDATE SKIP LOCKED` prevents duplicate dispatch. Use unique `--health-socket-path` per worker on the same host.
- **Offload CPU-bound work:** the worker is asyncio-based — CPU-bound actors block the event loop. Use `run_in_executor()`. Monitor `taskq.dispatch.duration` and `messaging.process.duration` via OTel: rising dispatch duration with flat process duration = DB contention; rising process duration = actor bottleneck.

---

## See also

- [workers.md](workers.md) — worker internals, settings, PgBouncer
- [cancellation.md](cancellation.md) — cancellation protocol
- [rate-limiting.md](rate-limiting.md) — rate-limit backends
- [admin-ui.md](admin-ui.md) — admin UI routes and auth
- [observability.md](observability.md) — OTel metrics and logging
- [architecture.md](../architecture.md) — state machine, dispatch, leader election
