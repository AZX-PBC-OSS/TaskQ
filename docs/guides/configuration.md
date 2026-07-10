# Configuration Reference

## Overview

All TaskQ configuration is provided through `TASKQ_*` environment variables, loaded via `dotenvmodel`. TaskQ never reads raw `os.environ` — use `TaskQSettings.load()` for all commands, or `WorkerSettings.load()` for worker processes.

There are two settings classes:

- **`TaskQSettings`** — base class; applies to every command (`worker`, `migrate`, `ui serve`, `health`).
- **`WorkerSettings`** — extends `TaskQSettings`; additional fields used only by the worker process.

dotenvmodel resolves a cascading chain of `.env` files at load time:

1. `.env` — base defaults, committed to the repo
2. `.env.local` — local overrides, never committed
3. `.env.{TASKQ_ENVIRONMENT}` — e.g. `.env.production`
4. `.env.{TASKQ_ENVIRONMENT}.local` — local env-specific overrides, never committed

Later files in the chain take precedence over earlier ones. Never commit `.env.local` or production env files.

---

## `.env` File Setup

Minimal `.env` for a real deployment:

```bash
# Required for any real deployment
TASKQ_PG_DSN=postgresql://user:pass@localhost:5432/mydb

# Optional — enables real-time admin UI updates
TASKQ_REDIS_URL=redis://localhost:6379/0

# Schema name (default: taskq)
TASKQ_SCHEMA_NAME=taskq

# Suppress unauthenticated-admin warning in dev
TASKQ_ENVIRONMENT=development
```

`.env` is the committed base. `.env.local` overrides it on a developer's machine without affecting others. When `TASKQ_ENVIRONMENT=production`, dotenvmodel additionally loads `.env.production` and `.env.production.local`. Never commit `.env.local` or production env files.

---

## TaskQSettings Reference

Applies to all commands: `worker`, `migrate`, `ui serve`, `health`.

| Env Var | Type | Default | Description | Used By |
|---|---|---|---|---|
| `TASKQ_PG_DSN` | `PostgresDsn` | `postgresql://taskq:taskq@localhost:5432/taskq` | Direct (non-PgBouncer) DSN. LISTEN/NOTIFY and advisory locks require a session-mode connection. | all |
| `TASKQ_SCHEMA_NAME` | `str` | `taskq` | Postgres schema for all TaskQ tables. Must match `^[A-Za-z_][A-Za-z0-9_]*$`. | all |
| `TASKQ_REDIS_URL` | `RedisDsn \| None` | `None` | Optional Redis URL. Required for real-time SSE progress fanout in the admin UI. | worker, ui serve |
| `TASKQ_ENVIRONMENT` | `str \| None` | `None` | Deployment label. Values `dev` or `development` suppress the unauthenticated-admin warning. Any other value triggers it. | all |
| `TASKQ_ADMIN_MAX_SSE_CONNECTIONS` | `int` | `50` | Maximum concurrent SSE connections the admin UI will serve. Min: 1. | ui serve |
| `TASKQ_ADMIN_HOST` | `str` | `0.0.0.0` | Bind address for `taskq ui serve`. | ui serve |
| `TASKQ_ADMIN_PORT` | `int` | `8080` | Bind port for `taskq ui serve`. Range: 1–65535. | ui serve |
| `TASKQ_ADMIN_URL` | `str` | `http://localhost:8080` | Public base URL of the admin UI as seen from a browser. Used to construct redirect URLs. Override when admin and app run on different hosts or ports. | ui serve |
| `TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS` | `float` | `2.0` | How often the admin UI polls PG when in polling/degraded mode. Min: 0.1. | ui serve |
| `TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET` | `bool` | `false` | When `True`, the admin UI shows a reset button on the rate-limits page and serves the `POST /rate-limits/{bucket_name}/reset` endpoint. Default `False` for safety. | ui serve |
| `TASKQ_MIGRATE_ON_START` | `bool` | `false` | Apply pending migrations before the process accepts its first request. Aborts startup if migrations fail. | ui serve |
| `TASKQ_EXAMPLE_HOST` | `str` | `0.0.0.0` | Bind address for the example trigger app. Ignored by worker and admin. | example app |
| `TASKQ_EXAMPLE_PORT` | `int` | `8000` | Bind port for the example trigger app. Ignored by worker and admin. | example app |

See [admin-ui.md](admin-ui.md) for admin-specific behaviour driven by these vars.

---

## WorkerSettings Reference

Extends `TaskQSettings`. All fields below apply to the worker process only.

### Database Connections

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_PG_DSN_DIRECT` | `PostgresDsn \| None` | falls back to `TASKQ_PG_DSN` | Bypasses PgBouncer. Used by `dispatcher_pool`, `heartbeat_pool`, `notify_conn`, and `leader_conn`. | — |
| `TASKQ_PG_DSN_POOLED` | `PostgresDsn \| None` | falls back to `TASKQ_PG_DSN` | May route through PgBouncer transaction mode. Used by `worker_pool` only. | — |

### Pool Sizing

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_DISPATCHER_POOL_SIZE` | `int` | `4` | Max connections for the dispatcher pool. | Min: 1 |
| `TASKQ_DISPATCH_OVERSAMPLE` | `int` | `2` | Multiplier for per-actor candidate gathering in the dispatch SQL. Each LATERAL reads `residual × oversample` candidates. Higher values absorb more identity-key collisions and multi-producer contention. Default 2 (tolerates 50% dupe identities). Set 1 when no `identity_key` is used and single-producer. Range: 1–1000. | Min: 1; Max: 1000 |
| `TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE` | `bool` | `false` | When `true`, restrict `per_actor_capacity` to actors whose home queue (`actor_config.queue`) the worker subscribes to. Lowers per-cycle probe count at the cost of not dispatching `enqueue(queue=...)` override jobs whose actor's home queue is not subscribed. Default `false` (override-safe). | — |
| `TASKQ_HEARTBEAT_POOL_SIZE` | `int` | `4` | Max connections for the heartbeat pool. | Min: 1 |
| `TASKQ_MAX_CONCURRENCY` | `int` | `8` | Max concurrent jobs per worker process. `worker_pool` size is derived as `int(max_concurrency * 1.5)`. | Min: 1 |

### Timing and Liveness

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_HEARTBEAT_INTERVAL` | `float` (seconds) | `10.0` | Period between heartbeat ticks. | Min: 0.5 |
| `TASKQ_LOCK_LEASE` | `float` (seconds) | `60.0` | Time before an unrenewed job lock is reclaimed by the sweep. Must be >= 4 × `TASKQ_HEARTBEAT_INTERVAL`. | Min: 1.0; see [Validation Constraints](#validation-constraints) |
| `TASKQ_MAX_HEARTBEAT_FAILURES` | `int` | `3` | Consecutive heartbeat failures before the worker self-terminates. | Min: 1 |

### Graceful Shutdown

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_TERMINATION_GRACE_PERIOD` | `float` (seconds) | `60.0` | Total budget from SIGTERM to forced exit. Must satisfy: `cancellation_grace + cleanup_grace < termination_grace − 5`. | Min: 5.0; see [Validation Constraints](#validation-constraints) |
| `TASKQ_CANCELLATION_GRACE_PERIOD` | `float` (seconds) | `30.0` | Duration of the cooperative cancel phase before force-cancel. | Min: 0.0 |
| `TASKQ_CLEANUP_GRACE_PERIOD` | `float` (seconds) | `10.0` | Force-cancel cleanup grace period. | Min: 0.0 |

See [workers.md](workers.md) for the shutdown sequence these values control.

### Retry

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_MAX_RETRY_BACKOFF` | `timedelta` | `24h` | Global ceiling on per-attempt retry backoff. Caps `RetryPolicy.cap` fleet-wide to prevent misconfigured actors from stranding jobs indefinitely. | — |
| `TASKQ_DEFAULT_START_TO_CLOSE` | `timedelta \| None` | `None` (unbounded) | Worker-wide fallback per-attempt execution timeout, applied only when a job has no `start_to_close` of its own (neither passed at enqueue time nor declared as an `@actor(start_to_close=...)` default). Gives every actor on the worker a safety-net wall-clock budget per attempt without configuring it individually. | — |

See [retries.md](retries.md#7-start_to_close-vs-schedule_to_close) for the full `start_to_close` vs `schedule_to_close` precedence chain.

### Rate Limiting

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED` | `bool` | `true` | When `false`, Redis errors propagate instead of triggering the Postgres rate-limit fallback. | — |

See [rate-limiting.md](rate-limiting.md) for the fallback behaviour.

### Health Server

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_HEALTH_ENABLED` | `bool` | `true` | Enable the Unix-socket health server. | — |
| `TASKQ_HEALTH_SOCKET_PATH` | `str` | `/tmp/taskq_health.sock` | Unix socket path for the health server. | — |
| `TASKQ_HEALTH_PG_PING_TIMEOUT` | `float` (seconds) | `0.2` | Timeout for the readiness PG ping. | Min: 0.0 |

### NOTIFY Listener

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_POLL_INTERVAL` | `float` (seconds) | `1.0` | Fallback polling cadence when the NOTIFY listener is unavailable. | — |
| `TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL` | `float` (seconds) | `5.0` | How often the NOTIFY health check issues `SELECT 1`. Detection latency before reconnect is at most this interval. | — |
| `TASKQ_NOTIFY_RECONNECT_BACKOFF_INITIAL` | `float` (seconds) | `1.0` | Initial backoff before the first NOTIFY reconnect. Doubles each attempt, capped at 30 s. Sequence: 1, 2, 4, 8, 16, 30. | — |
| `TASKQ_NOTIFY_ENABLED` | `bool` | `true` | When `true`, the worker uses LISTEN/NOTIFY for near-zero-latency dispatch wakeups with poll interval as fallback. When `false`, the worker uses poll-only dispatch. | — |
| `TASKQ_NOTIFY_POLL_INTERVAL` | `float` (seconds) | `5.0` | Fallback poll cadence when NOTIFY is enabled. Uses `TASKQ_POLL_INTERVAL` when NOTIFY is disabled. | Min: 0.5 |

### Queue Selection

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_QUEUES` | `list[str]` | `["default"]` | Queue names this worker consumes. Set as a comma-separated string: `TASKQ_QUEUES=default,priority`. | — |
| `TASKQ_POOL_MAX_INACTIVE_LIFETIME` | `float` (seconds) | `300.0` | Closes asyncpg connections idle longer than this. Applied to all three pools. | Min: 0.0 |
| `TASKQ_WORKER_LABEL` | `str \| None` | `None` | Human-readable label for this worker. Stored in `workers.worker_label` for correlation with workgroup supervisors and external monitoring. When omitted, hostname + pid is used. | — |
| `TASKQ_WORKGROUP_INSTANCE` | `str \| None` | `None` | UUIDv7 identifying the workgroup orchestrator that launched this worker. Stored in `workers.workgroup_instance` for cross-process correlation. Set automatically by the workgroup supervisor. | — |

### Observability

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_OTEL_ENABLED` | `bool` | `true` | When `false`, suppresses all OTel span and metric creation. Operations still succeed. | — |
| `TASKQ_WORKER_GROUP` | `str` | `default` | Consumer group name emitted as `messaging.consumer.group.name` on spans. | — |
| `TASKQ_LOG_FORMAT` | `str` | `json` | Log renderer. `json` for production; `console` for human-readable dev output. Only these two values are valid. | Must be `json` or `console` |
| `TASKQ_LOG_LEVEL` | `str` | `INFO` | Root logger level. | — |
| `TASKQ_METRICS_PORT` | `int` | `9090` | Bind port for the standalone Prometheus metrics server. Used by the `prometheus` contrib exporter; the in-process FastAPI health `/metrics` endpoint ignores this field. | Range: 1–65535 |

See [observability.md](observability.md) for OTel configuration.

### Actor Config

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_FORCE_UPDATE_ACTOR_CONFIG` | `bool` | `false` | When `true`, silently overwrites stored `actor_config` rows that differ from registered values. When `false`, drift raises `ActorConfigDriftList` and the worker refuses to start. Use for one deploy when intentionally changing `max_concurrent`, `queue`, or `metadata`, then unset. | — |

### Cron Scheduler

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_CRON_CATCH_UP_WINDOW` | `timedelta` | `1h` | Missed firings within this window are caught up sequentially; older misses are skipped. | Must not be negative |
| `TASKQ_CRON_AUTO_DISABLE_THRESHOLD` | `int` | `3` | Consecutive failures before a schedule is auto-disabled. | Min: 1 |

See [cron.md](cron.md) for cron scheduling details.

### Progress Fanout

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_PROGRESS_COALESCE_INTERVAL` | `float` (seconds) | `0.5` | How long the flush loop waits between Redis publishes for a single job. Lower values increase publish frequency. | Min: 0.1 |
| `TASKQ_PROGRESS_DATA_MAX_BYTES` | `int` | `16384` | Maximum serialised byte length of the `data` dict in a single progress call. Exceeding this raises `ProgressTooLarge`. | Range: 1024–1048576 |
| `TASKQ_PROGRESS_PUBLISH_GLOBAL` | `bool` | `true` | When `true`, progress updates are published to the global fanout channel (e.g. Redis). When `false`, progress updates are only written to Postgres. | — |

See [progress.md](progress.md) for progress tracking details.

### Job Retention and Archive

The **prune sweep** (Sweep 5) runs once daily and moves terminal jobs from `jobs` into `jobs_archive` after their per-status retention period has elapsed. The **archive expiry sweep** (Sweep 6) runs once daily and hard-deletes rows from `jobs_archive` once their archive retention period has expired. Both sweeps are batched, atomic, and advisory-locked.

#### Prune schedule and batch size

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_PRUNE_SCHEDULE_UTC` | `str` | `03:00` | Daily fire time for the prune sweep in `HH:MM` UTC format. Ignored when `TASKQ_PRUNE_CRON_EXPR` is set. | — |
| `TASKQ_PRUNE_CRON_EXPR` | `str \| None` | `None` | Full 5-field cron expression for the prune sweep. Takes precedence over `TASKQ_PRUNE_SCHEDULE_UTC`. | — |
| `TASKQ_PRUNE_BATCH_SIZE` | `int` | `10000` | Rows processed per CTE batch. The sweep repeats until no rows remain. | Min: 1 |

#### Per-status retention

These control how long a terminal job stays in the `jobs` table before being moved to `jobs_archive`. Shorter values keep the hot `jobs` table smaller; longer values make recent history available without querying the archive.

| Env Var | Type | Default | Description |
|---|---|---|---|
| `TASKQ_PRUNE_RETENTION_PERIOD` | `timedelta` | `30d` | Global fallback retention when no per-status override applies. |
| `TASKQ_PRUNE_RETENTION_SUCCEEDED` | `timedelta` | `30d` | Retention for `succeeded` jobs. |
| `TASKQ_PRUNE_RETENTION_FAILED` | `timedelta` | `90d` | Retention for `failed` jobs. |
| `TASKQ_PRUNE_RETENTION_CANCELLED` | `timedelta` | `30d` | Retention for `cancelled` jobs. |
| `TASKQ_PRUNE_RETENTION_ABANDONED` | `timedelta` | `90d` | Retention for `abandoned` and `crashed` jobs. |

Per-actor retention overrides can be set in `actor_config.metadata` as `retention_days` (an integer). When set, an actor's jobs are pruned at `min(retention_days, global_per_status_retention)`. This allows short-lived high-volume actors (e.g. ping jobs) to be pruned faster without affecting the global defaults.

#### Archive retention and expiry schedule

| Env Var | Type | Default | Description | Constraints |
|---|---|---|---|---|
| `TASKQ_ARCHIVE_RETENTION_PERIOD` | `timedelta` | `365d` | How long a row stays in `jobs_archive` before the expiry sweep hard-deletes it. | Must be positive |
| `TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC` | `str` | `04:00` | Daily fire time for the archive expiry sweep in `HH:MM` UTC format. Ignored when `TASKQ_ARCHIVE_EXPIRY_CRON_EXPR` is set. | — |
| `TASKQ_ARCHIVE_EXPIRY_CRON_EXPR` | `str \| None` | `None` | Full 5-field cron expression for the archive expiry sweep. Takes precedence over `TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC`. | — |

> **Storage planning.** Each job row is approximately 1–4 KB depending on payload and result sizes. With the default retention settings (30 days in `jobs`, 365 days in `jobs_archive`) and 100 000 jobs/day, `jobs` holds roughly 3 M rows and `jobs_archive` holds roughly 35 M rows. Tune the retention values and monitor table sizes with `SELECT pg_size_pretty(pg_total_relation_size('"taskq".jobs_archive'))`.

See [../architecture.md](../architecture.md) for the prune/archive schema design and the `jobs_archive` table structure.

---

## Validation Constraints

These cross-field constraints are enforced in `_post_load` at startup. Violations raise `ValueError` before the process enters its main loop.

### Lock lease vs heartbeat interval

```
lock_lease >= 4 × heartbeat_interval
```

Rationale: tolerates three consecutive missed heartbeats before the sweep reclaims the lock, preventing false abandonment under transient PG connectivity issues.

Error pattern: `lock_lease must be >= 4 * heartbeat_interval`

### Termination budget

```
cancellation_grace_period + cleanup_grace_period < termination_grace_period − 5
```

Rationale: reserves at least 5 seconds for post-shutdown bookkeeping after both cancel phases complete.

Error pattern: `cancellation_grace_period + cleanup_grace_period must be < termination_grace_period - 5`

### Cancellation phases vs lock lease

```
cancellation_grace_period + cleanup_grace_period < lock_lease
```

Rationale: ensures the worker finishes its shutdown sequence before the job lock expires and the sweep can reclaim the job.

Error pattern: `cancellation_grace_period + cleanup_grace_period must be < lock_lease`

### Log format

```
log_format in {"json", "console"}
```

Error pattern: `log_format must be 'json' or 'console'`

---

## Derived Values

These values are computed from settings rather than set directly.

### `worker_pool_size`

```python
worker_pool_size = int(max_concurrency * 1.5)
```

The worker pool is sized at 1.5× `TASKQ_MAX_CONCURRENCY` to provide burst headroom: jobs that briefly block on I/O can release connections while new ones are dispatched, preventing pool exhaustion at full concurrency.

### `resolved_pg_dsn_direct`

```
TASKQ_PG_DSN_DIRECT  →  falls back to TASKQ_PG_DSN when unset
```

Used by `dispatcher_pool`, `heartbeat_pool`, `notify_conn`, and `leader_conn`. Always points to a session-mode connection that supports LISTEN/NOTIFY and advisory locks.

### `resolved_pg_dsn_pooled`

```
TASKQ_PG_DSN_POOLED  →  falls back to TASKQ_PG_DSN when unset
```

Used exclusively by `worker_pool`. May safely route through PgBouncer in transaction mode because the worker pool does not use session-level features.

---

## PgBouncer Configuration Pattern

When running PgBouncer in front of Postgres, split the DSN by connection type:

```bash
# Direct connection — used for LISTEN/NOTIFY, advisory locks, dispatcher, heartbeat
TASKQ_PG_DSN_DIRECT=postgresql://taskq:pass@postgres:5432/taskq

# Pooled connection — can go through PgBouncer transaction mode
TASKQ_PG_DSN_POOLED=postgresql://taskq:pass@pgbouncer:5432/taskq
```

If neither `TASKQ_PG_DSN_DIRECT` nor `TASKQ_PG_DSN_POOLED` is set, both resolve to `TASKQ_PG_DSN`. In that case `TASKQ_PG_DSN` must point directly at Postgres (not PgBouncer), because the direct-connection pools require session mode.

---

## Production Example `.env`

```bash
TASKQ_PG_DSN=postgresql://taskq:secret@postgres.internal:5432/taskq
TASKQ_PG_DSN_DIRECT=postgresql://taskq:secret@postgres.internal:5432/taskq
TASKQ_PG_DSN_POOLED=postgresql://taskq:secret@pgbouncer.internal:5432/taskq
TASKQ_REDIS_URL=redis://redis.internal:6379/0
TASKQ_SCHEMA_NAME=taskq
TASKQ_ENVIRONMENT=production
TASKQ_MAX_CONCURRENCY=16
TASKQ_QUEUES=default,priority
TASKQ_LOG_FORMAT=json
TASKQ_LOG_LEVEL=INFO
TASKQ_OTEL_ENABLED=true
TASKQ_HEALTH_SOCKET_PATH=/run/taskq/health.sock
TASKQ_TERMINATION_GRACE_PERIOD=120
TASKQ_CANCELLATION_GRACE_PERIOD=60
TASKQ_CLEANUP_GRACE_PERIOD=20
TASKQ_LOCK_LEASE=90
TASKQ_HEARTBEAT_INTERVAL=10
```

These values satisfy all cross-field constraints:
- `lock_lease (90) >= 4 × heartbeat_interval (10)` — 90 >= 40 ✓
- `cancellation_grace (60) + cleanup_grace (20) < termination_grace (120) − 5` — 80 < 115 ✓
- `cancellation_grace (60) + cleanup_grace (20) < lock_lease (90)` — 80 < 90 ✓

---

## Extending Settings

Subclass `WorkerSettings` to add application-specific config alongside TaskQ settings:

```python
from taskq.settings import WorkerSettings
from dotenvmodel import Field

class AppSettings(WorkerSettings):
    stripe_api_key: str = Field(description="Stripe secret key")
    sentry_dsn: str | None = Field(default=None)
```

Load with `AppSettings.load()`. All `TASKQ_*` validation constraints still apply. Additional fields follow the same dotenvmodel env-var resolution and `.env` cascade.
