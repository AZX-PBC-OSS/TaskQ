# CLI reference

The `taskq` CLI is the primary operational interface for managing migrations, running workers, probing health, and serving the admin UI. All commands load settings from `TASKQ_*` environment variables or `.env` files via dotenvmodel.

## Installation

The `taskq` command is installed as part of the `taskq-py` package:

```shell
uv add taskq-py
```

The console-script entry point is `taskq.cli:main`. See [../getting-started/quick-start.md](../getting-started/quick-start.md) for initial environment setup.

---

## Global environment variables

These variables are read by all commands via `TaskQSettings.load()` or `WorkerSettings.load()`.

| Variable | Default | Description |
|---|---|---|
| `TASKQ_PG_DSN` | `postgresql://taskq:taskq@localhost:5432/taskq` | Postgres connection string used by migrate and health commands |
| `TASKQ_SCHEMA_NAME` | `taskq` | Postgres schema for all TaskQ tables |

Worker-specific variables (pool sizes, heartbeat timing, cancellation grace periods, etc.) are documented in [workers.md](workers.md#workersettings-reference).

---

## `taskq dev`

Starts a worker in development mode with automatic restart on file changes. Useful during
local development — saves manually killing and restarting the worker after every code edit.

```shell
taskq dev MODULE:ATTR [OPTIONS]
```

**Arguments:**

| Argument | Description |
|---|---|
| `MODULE:ATTR` | `module:attr` reference to the actor registry — same syntax as `taskq worker --actors`. |

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `--watch` | `str` (repeatable) | current working directory | Path to watch for changes. Pass the flag multiple times to watch several directories. |
| `--grace-period` | `int` | `5` | Seconds between SIGTERM and SIGKILL when stopping the worker before a restart. `0` means send SIGTERM then kill immediately if still alive. |

**How it works:**

1. Imports `MODULE:ATTR` at startup and exits with a clear error if the module is not found or
   the attribute does not exist.
2. Spawns `taskq worker --actors MODULE:ATTR` as a child subprocess. The child inherits the
   parent's full environment, so `TASKQ_*` variables set in the shell are picked up normally.
3. Watches the specified paths with `watchfiles.awatch()` using `DefaultFilter` (ignores
   `.git`, `__pycache__`, editor temp files, etc.) and a 400 ms debounce window.
4. On any file change: sends SIGTERM to the running worker, waits up to `--grace-period`
   seconds, then SIGKILL if still alive. Re-validates the import, then spawns a fresh worker.
   If the re-import fails (e.g. a syntax error) it logs a warning and waits for the next
   change rather than crashing.
5. `Ctrl-C` (SIGINT) stops the worker cleanly and exits with code 0.

Each restart gets a **fresh Python interpreter** — no `importlib.reload()` is involved, so
module-level state (Pydantic models, actor registries, config objects) is always clean.

**Requirements:**

`taskq dev` requires the `reload` extra:

```shell
uv add "taskq-py[reload]"
```

This installs `watchfiles`. Without it the command prints an install hint and exits with code 1.

**Example: watch a single package directory**

```shell
taskq dev myapp.actors:registry --watch src/myapp
```

**Example: watch multiple directories**

```shell
taskq dev myapp.actors:registry --watch src/myapp --watch config
```

**Example: fast restart (no grace period)**

```shell
taskq dev myapp.actors:registry --grace-period 0
```

**Exit codes:**

| Code | Condition |
|---|---|
| `0` | Clean exit via Ctrl-C |
| `1` | Bad `MODULE:ATTR` syntax, import failure, or `watchfiles` not installed |

> **Note:** `taskq dev` is intended for local development only. Do not run it in production —
> use `taskq worker` directly under a process supervisor (systemd, Docker, Kubernetes).

---

## `taskq migrate status`

Shows which migrations have been applied and which are pending.

```shell
taskq migrate status
```

Connects to `TASKQ_PG_DSN`, queries `{schema}.schema_migrations`, and prints one line per discovered migration file.

**Example output:**

```
schema: taskq
applied: 1
  [✔] 01.00.00_01_pre_initial.sql
```

A `✔` marker indicates the migration has been applied. An empty marker indicates it is pending. Migrations are discovered from the built-in migration directory in the package.

**No options.** Uses `TASKQ_PG_DSN` and `TASKQ_SCHEMA_NAME` from the environment.

---

## `taskq migrate up`

Applies pending migrations in order.

```shell
taskq migrate up [OPTIONS]
```

**Options:**

| Option | Type | Default | Description |
|---|---|---|---|
| `--phase` | `pre \| post \| None` | `None` | Restrict to only `pre` or only `post` phase migrations. When absent, applies both phases in order. |
| `--target` | `str \| None` | `None` | Stop after applying this migration version (inclusive). Version format matches the filename prefix, e.g. `01.00.00_01`. |
| `--max-steps` | `int \| None` | `None` | Maximum number of migrations to apply in this invocation. |

The command is idempotent: each migration is recorded in `{schema}.schema_migrations` and is skipped on subsequent runs. Running `taskq migrate up` with no options applies all pending migrations.

**Example: apply all pending:**

```shell
taskq migrate up
```

**Example output:**

```
applied 2 migration(s):
  01.01.00_01_pre_add_reservation_slots.sql
  01.01.00_02_post_add_reservation_slots.sql
```

When there is nothing to apply:

```
no pending migrations
```

**Example: apply only pre-phase migrations up to a target version:**

```shell
taskq migrate up --phase pre --target 01.01.00_01
```

---

## `taskq worker`

Starts a TaskQ worker process. Blocks until SIGTERM or SIGINT.

```shell
taskq worker --actors MODULE:ATTR [OPTIONS]
```

**Options:**

| Option | Type | Default | Env var override | Description |
|---|---|---|---|---|---|
| `--actors` | `str` | *required* | — | `module:attr` reference to the actor registry |
| `--queues` | `list[str]` | `None` | `TASKQ_QUEUES` | Queue names to consume; repeat the flag once per queue name |
| `--max-concurrency` | `int` | `None` | `TASKQ_MAX_CONCURRENCY` | Upper bound on concurrent jobs |
| `--poll-interval` | `float` | `None` | `TASKQ_POLL_INTERVAL` | Producer loop fallback polling cadence (seconds) |
| `--worker-group` | `str` | `None` | `TASKQ_WORKER_GROUP` | Consumer group name for observability spans |
| `--worker-label` | `str` | `None` | `TASKQ_WORKER_LABEL` | Human-readable label stored in the workers table |
| `--workgroup-instance` | `str` | `None` | `TASKQ_WORKGROUP_INSTANCE` | UUIDv7 identifying the workgroup orchestrator that launched this worker |
| `--health-socket-path` | `str` | `None` | `TASKQ_HEALTH_SOCKET_PATH` | Unix socket path for the health server (use unique paths when running multiple workers) |
| `--force-update-actor-config` | `bool` | `False` | `TASKQ_FORCE_UPDATE_ACTOR_CONFIG` | Overwrite drifted actor-config rows at startup |

All other worker settings are read from environment variables. See [workers.md](workers.md#workersettings-reference) for the full list.

### `--actors` format

The value must be a `module:attr` string with exactly one colon separator. Both `module` and `attr` must be non-empty.

```
myapp.actors:registry          # module=myapp.actors, attr=registry
myapp.workers.email:handlers   # module=myapp.workers.email, attr=handlers
```

The module is imported at startup via `importlib.import_module`. The attribute must resolve to one of:

- `Mapping[str, ActorRef]` — keys are actor names, values are `ActorRef` instances.
- `Iterable[ActorRef]` — names are read from `ActorRef.name` on each element.

Any other type (including a plain list of non-`ActorRef` objects) prints an error and exits with code 1.

**Example: iterable form**

```python
# myapp/actors.py
from taskq.actor import actor
from pydantic import BaseModel

class EmailPayload(BaseModel):
    to: str
    subject: str

@actor(queue="email")
async def send_email(payload: EmailPayload) -> None: ...

registry = [send_email]
```

```shell
taskq worker --actors myapp.actors:registry
```

**Example: mapping form**

```python
# myapp/actors.py
registry = {"send_email": send_email, "resize_image": resize_image}
```

```shell
taskq worker --actors myapp.actors:registry
```

### `--queues` flag

`--queues` is a multi-value flag. Pass it once for each queue name:

```shell
taskq worker --actors myapp.actors:registry --queues default --queues priority --queues email
```

Do not pass a comma-separated string directly to `--queues` on the command line. Use the environment variable for comma-separated input:

```shell
TASKQ_QUEUES=default,priority,email taskq worker --actors myapp.actors:registry
```

The help text for `--queues` currently says "Comma-separated list of queue names" — this refers to the `TASKQ_QUEUES` environment variable format, not the CLI flag invocation. On the command line the flag must be repeated once per queue.

### `--force-update-actor-config`

At startup the worker compares each registered actor's `max_concurrent`, `max_pending`, `queue`, and `metadata` values against the stored rows in `{schema}.actor_config`. If any field differs and this flag is absent, the worker refuses to start and prints:

```
ActorConfigDriftList: ...
Re-run with --force-update-actor-config to overwrite, or set TASKQ_FORCE_UPDATE_ACTOR_CONFIG=true.
```

Use this flag on the first new pod of a rolling deploy when actor config has changed (`max_concurrent`, `queue`, `metadata`). Remove it for subsequent pods — it is not safe to run permanently as it allows silent config drift. See [workers.md](workers.md#actorconfig-sync) for the full drift protocol.

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Clean shutdown (SIGTERM received and shutdown completed) |
| `1` | Startup failure or runtime error |

`ActorConfigDriftList` is caught by the CLI and produces a clean one-line error message on stderr. Other bootstrap failures (import errors, wrong attribute type, etc.) produce a Python traceback on stderr. Both exit with code 1.

---

## `taskq health live`

Probes the worker's liveness endpoint.

```shell
taskq health live
```

Connects to the Unix socket at `TASKQ_HEALTH_SOCKET_PATH` (default `/tmp/taskq_health.sock`) and issues `GET /live`. The health server schedules a `loop.call_later(0.01, ...)` callback and waits up to 1.0s for it to fire, confirming the event loop is responsive.

The Unix socket is not reachable via Kubernetes `httpGet` probes. Use an `exec` probe:

```yaml
livenessProbe:
  exec:
    command: ["taskq", "health", "live"]
  initialDelaySeconds: 5
  periodSeconds: 10
```

**Exit codes:**

| Code | Condition |
|---|---|
| `0` | HTTP 2xx — event loop is responsive (`{"status":"ok"}`) |
| `1` | HTTP 5xx — event loop unresponsive or timeout exceeded |
| `1` | Socket unreachable (worker not running or wrong socket path) |

**Example:**

```shell
taskq health live && echo "worker is alive"
```

---

## `taskq health ready`

Probes the worker's readiness endpoint.

```shell
taskq health ready
```

Connects to the Unix socket and issues `GET /ready`. The health server checks:

1. `shutdown_phase == NONE` (worker is not draining or shutting down)
2. PG ping succeeds within `TASKQ_HEALTH_PG_PING_TIMEOUT` (default 0.2s)

Both conditions must pass for the response to be `200`. During any shutdown phase, the response is `503` regardless of the PG ping result.

**Response body (200):**

```json
{
  "ready": true,
  "redis_configured": false,
  "active_jobs": 4,
  "is_leader": false,
  "shutdown_phase": null
}
```

**Response body (503):**

```json
{
  "ready": false,
  "redis_configured": false,
  "active_jobs": 2,
  "is_leader": false,
  "shutdown_phase": 1
}
```

`shutdown_phase` is `null` when `NONE`; otherwise the integer value (1=DRAINING, 2=CANCELLING, 3=FORCING, 4=ABANDONING).

**Exit codes:**

| Code | Condition |
|---|---|
| `0` | HTTP 200 — worker is ready |
| `1` | HTTP 503 — not ready (shutting down or PG ping failed) |
| `1` | Socket unreachable |

**Example (Kubernetes readiness probe via exec):**

```yaml
readinessProbe:
  exec:
    command: ["taskq", "health", "ready"]
  initialDelaySeconds: 5
  periodSeconds: 10
```

---

## `taskq health metrics`

Fetches the worker's Prometheus-format metrics.

```shell
taskq health metrics
```

Connects to the Unix socket and issues `GET /metrics`. Always returns `200`. Prints the response body to stdout.

**Example output:**

```
# HELP taskq_active_jobs Currently in-flight jobs on this worker.
# TYPE taskq_active_jobs gauge
taskq_active_jobs 3
# HELP taskq_is_leader 1 if this worker holds the maintenance leader lock.
# TYPE taskq_is_leader gauge
taskq_is_leader 0
# HELP taskq_shutdown_phase Current shutdown phase enum value (0=NONE).
# TYPE taskq_shutdown_phase gauge
taskq_shutdown_phase 0
```

**Exit codes:**

| Code | Condition |
|---|---|
| `0` | HTTP 200 — metrics returned |
| `1` | Socket unreachable or request timed out |

---

## `taskq ui serve`

Starts the admin UI server (FastAPI + uvicorn).

```shell
taskq ui serve [OPTIONS]
```

**Options:**

| Option | Type | Env var fallback | Default | Description |
|---|---|---|---|---|
| `--pg-dsn` | `str` | `TASKQ_PG_DSN` | `postgresql://taskq:taskq@localhost:5432/taskq` | Postgres DSN for the admin UI pool |
| `--schema` | `str` | `TASKQ_SCHEMA_NAME` | `taskq` | Postgres schema |
| `--redis-url` | `str` | `TASKQ_REDIS_URL` | `None` | Redis URL for real-time SSE progress |
| `--host` | `str` | `TASKQ_ADMIN_HOST` | `0.0.0.0` | Bind address |
| `--port` | `int` | `TASKQ_ADMIN_PORT` | `8080` | Bind port |
| `--migrate` | `bool` | `TASKQ_MIGRATE_ON_START` | `false` | Apply pending migrations before starting. Aborts startup if migrations fail. |

**Relevant environment variables:**

| Variable | Default | Description |
|---|---|---|
| `TASKQ_ADMIN_MAX_SSE_CONNECTIONS` | `50` | Maximum concurrent SSE connections the admin UI will serve. Controls the connection-limit semaphore. |

All options fall back to the corresponding `TASKQ_*` environment variable when not supplied on the command line. The admin UI requires the `fastapi` optional dependency group:

```shell
uv add "taskq-py[fastapi]"
```

The server creates a small asyncpg pool (`min_size=1, max_size=4`) and optionally a Redis client. The admin router is mounted at `/admin`. See [admin-ui.md](admin-ui.md) for the full UI reference.

**Example:**

```shell
taskq ui serve --port 8001
```

```shell
TASKQ_ADMIN_PORT=8001 taskq ui serve
```

The process blocks until killed. There is no graceful-shutdown option; use a process manager or container lifecycle hook.

---

## `taskq workgroup validate`

Validates a workgroup TOML configuration file without starting any workers.

```shell
taskq workgroup validate CONFIG
```

**Arguments:**

| Argument | Type | Description |
|---|---|---|
| `CONFIG` | `PATH` | Path to the workgroup TOML configuration file. |

Prints a summary of each worker's configuration. Exits 1 if the config is missing, malformed, or contains invalid values (e.g. negative poll interval, misconfigured health check thresholds).

**Example:**

```shell
taskq workgroup validate workgroup.toml
# config OK — 2 worker(s), actors='myapp.actors:registry'
#   api: queues=['default'] poll=0.5s concurrency=8 health=off
#   batch: queues=['email', 'report'] poll=5.0s concurrency=2 health=on
```

---

## `taskq workgroup start`

Starts a workgroup supervisor that manages multiple worker subprocesses from a TOML configuration file.

```shell
taskq workgroup start CONFIG
```

**Arguments:**

| Argument | Type | Description |
|---|---|---|
| `CONFIG` | `PATH` | Path to the workgroup TOML configuration file. |

The supervisor spawns one `taskq worker` subprocess per `[[workers]]` entry, manages their lifecycle, and cleanly shuts them down on SIGTERM/SIGINT. See [workgroups.md](workgroups.md) for the full configuration reference and operational guidance.

**Example config:**

```toml
actors = "myapp.actors:registry"

[[workers]]
name = "api"
queues = ["default"]
max_concurrency = 8

[[workers]]
name = "batch"
queues = ["email", "report"]
poll_interval = 5.0
max_concurrency = 2
```

**Example:**

```shell
taskq workgroup start workgroup.toml
```

**Exit codes:**

| Code | Condition |
|---|---|
| `0` | Clean shutdown after SIGTERM or SIGINT |
| `1` | Config file not found, invalid config, or health-check PG pool initialization failure |

---

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Any failure: bad arguments, import errors, config drift, PG connection failures, health probe negative result |

The `taskq worker` command exits with the code returned by `worker_main()`. On clean SIGTERM, `worker_main()` returns `0`. A third SIGTERM calls `sys.exit(1)`.

The `taskq workgroup start` command exits 0 on clean shutdown. It exits 1 if the config file is missing, invalid, or if the optional health-check PG pool fails to initialise.
