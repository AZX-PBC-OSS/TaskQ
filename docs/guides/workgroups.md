# Workgroup supervisor

A workgroup is a lightweight process orchestrator that manages multiple `taskq worker` subprocesses within a single container or machine. Each child worker can be configured independently (different queues, poll intervals, concurrency caps) while the supervisor handles process lifecycle, crash recovery, and optional database-backed health checking.

The supervisor itself is a single async process that spawns N child processes and loops until a signal arrives. It is intentionally thin: all job execution logic lives in the worker processes, which remain unchanged.

## When to use a workgroup

- **Multi-queue deployments.** Run one worker per queue group with independent concurrency budgets. For example: high-throughput `api` workers polling at 0.5 s alongside a `batch` worker polling at 5 s.
- **Single-container deployments.** Bundle several logically distinct workers into one container or pod instead of managing separate deployment units.
- **Resource isolation.** CPU-bound actors can be assigned to a dedicated worker with a low concurrency cap without throttling I/O-bound actors.

## Configuration

Workgroup configuration is a TOML file. The only required top-level key is `actors` (the `module:attr` reference shared by all child workers).

### Full reference

```toml
# Required: actor registry shared by all workers.
actors = "myapp.actors:registry"

# Optional: default values inherited by any worker that omits them.
[defaults]
poll_interval = 1.0
max_concurrency = 4
worker_group = "default"

# Optional: global supervisor behaviour.
[supervisor]
shutdown_grace = 100.0      # seconds to wait for children during shutdown (covers a child's 82s worst-case clean exit with margin)
backoff_initial = 0.5       # first restart delay (seconds)
backoff_max = 30.0          # ceiling on restart delay
backoff_factor = 2.0        # multiplier per successive crash
burst_limit = 10            # max restarts within burst_window
burst_window = 60.0         # rolling window for burst counting (seconds)

# Required: at least one worker definition.
[[workers]]
name = "api"                # unique label, <= 43 chars (health socket path budget); used for DB correlation
queues = ["default"]        # >= 1 queue this worker consumes; omit to inherit [defaults].queues / ["default"]
max_concurrency = 8         # concurrent job limit
poll_interval = 0.5         # producer polling cadence (seconds)
worker_group = "default"    # observability span group name
force_update_actor_config = false  # set true for one deploy when an actor's queue/metadata changes

# Optional: per-worker health checking via the database.
[workers.health]
enabled = true
check_interval = 15         # seconds between DB checks
stale_after = 60            # seconds without a heartbeat before declaring hung
startup_grace = 15.0        # grace period after spawn before first health check
consecutive_failure_limit = 3  # consecutive DB query failures before declaring dead

[[workers]]
name = "batch"
queues = ["email", "report", "cleanup"]
poll_interval = 5.0
max_concurrency = 2
```

### `[supervisor]` options

| Key | Type | Default | Description |
|---|---|---|---|
| `shutdown_grace` | `float` | `100.0` | Seconds to wait for children to exit gracefully after SIGTERM. Children still alive after this window are SIGKILL'd. Sized from the children's modelled worst-case clean exit (`cancellation_grace_period` 30s + `cleanup_grace_period` 10s + the ~42s bounded-close teardown tail = 82s) plus ~22% margin for loop jitter and the watchdog's non-instantaneous deadline trip. The previous 30.0 default sat below even the 40s release floor, so a default workgroup SIGKILLed children before a held release landed and interrupted jobs rode the lease-expiry crash path. A startup warning names the numbers when a configured grace falls below either floor. |
| `backoff_initial` | `float` | `0.5` | Delay before the first restart attempt (seconds). |
| `backoff_max` | `float` | `30.0` | Ceiling on restart delay. The delay never exceeds this value. |
| `backoff_factor` | `float` | `2.0` | Multiplier applied to the delay on each successive crash. Must be >= 1.0. |
| `burst_limit` | `int` | `10` | Maximum restarts within `burst_window` before the supervisor stops restarting that worker and logs a critical error. |
| `burst_window` | `float` | `60.0` | Rolling window for burst counting (seconds). After a stable period of at least this length, the burst counter and backoff are reset. |
| `health_pg_dsn` | `str` | *none* | Override the Postgres DSN used for health checks. Falls back to `TASKQ_PG_DSN_DIRECT` from the environment when unset. |
| `health_pg_schema` | `str` | *none* | Override the Postgres schema for health checks. Falls back to `TASKQ_SCHEMA_NAME` when unset. |

### `[workers.health]` options

| Key | Type | Default | Description |
|---|---|---|---|
| `enabled` | `bool` | `false` | When `true`, the supervisor queries the `workers` table for this child's heartbeat and kills the process if it appears hung. Requires a Postgres connection. |
| `check_interval` | `float` | `15.0` | Seconds between health-check queries for this worker. |
| `stale_after` | `float` | `60.0` | If the worker's `last_seen_at` is older than this many seconds, the process is considered hung and killed. |
| `startup_grace` | `float` | `15.0` | Grace period after spawn during which health checks are skipped. Prevents killing a worker that hasn't had time to register and heartbeat yet. |
| `consecutive_failure_limit` | `int` | `3` | Number of consecutive failed health-check queries before the worker is declared dead. Prevents a transient DB outage from killing healthy workers. |

## Starting a workgroup

```shell
taskq workgroup start workgroup.toml
```

The supervisor blocks until SIGTERM or SIGINT. SIGHUP is not a stop signal: it is forwarded to every living child for credential rotation (see "Credential rotation (SIGHUP)" below).

### Validating a config

```shell
taskq workgroup validate workgroup.toml
```

Prints a summary of the config and each worker without starting any processes. Exits 1 if the config is invalid, including an `actors` reference that cannot be resolved.

Validation imports the `actors` module to resolve that reference, so run it where the application is importable: the deployment image, not a bare CI checkout. Resolving up front turns a spawn-crash-and-respawn cascade, whose real cause is buried under the restarts, into one message naming the reference. It also lets validate warn about an actor whose queue no `[[workers]]` entry consumes; that stays a warning, because another workgroup or deployment may consume it and no single supervisor knows the whole fleet.

On shutdown:
1. SIGTERM is forwarded to every child process.
2. The supervisor waits up to `shutdown_grace` seconds for children to exit.
3. Any remaining children are force-killed with SIGKILL.
4. The supervisor cleans up stream tasks and the health-check PG pool, then exits 0.

### Exit codes

| Code | Condition |
|---|---|
| `0` | Clean shutdown after signal |
| `1` | Config file not found, invalid config, or health-check PG pool failed to initialise |

## How health checking works

The health-check loop queries the `workers` table with the supervisor's unique UUIDv7 instance ID and the worker's label:

```sql
SELECT pid, last_seen_at FROM workers
WHERE workgroup_instance = $1 AND worker_label = $2
ORDER BY last_seen_at DESC LIMIT 1
```

The query is covered by the `workers_wg_lookup_idx` partial index. A worker is considered healthy when:
- A matching row exists in the table.
- The `pid` in the row matches the OS process ID of the child.
- `last_seen_at` is within `stale_after` seconds of the current time.

If either the PID mismatches or the heartbeat is stale, the supervisor terminates and restarts the child process.

## Correlation model

Each workgroup instance generates a UUIDv7 at startup. This is passed to every child worker via `--workgroup-instance`. The child stores it in the `workers.workgroup_instance` column alongside its `worker_label`. Together these two columns uniquely identify a logical worker across process restarts, container replicas, and deployments.

- `worker_label` = the `name` from the `[[workers]]` TOML entry (e.g. `"api"`, `"batch"`).
- `workgroup_instance` = a UUIDv7 generated by the supervisor at startup. Different each time the supervisor starts, even across pod restarts.

## Restart policy

Two failures trigger a restart, and both consume the same budget:

- A child process exits (non-zero or zero).
- A child fails to spawn at all: a broken command line, a missing binary, a permission error. Never-spawned children are retried by the liveness monitor exactly like exited ones.

On either trigger the supervisor:

1. Records the attempt time in a rolling window of `burst_window` seconds.
2. If the number of attempts in the window exceeds `burst_limit`, latches give-up for that child: one critical `workgroup-burst-limit-exceeded` is logged and the child is never scheduled again; the workgroup supervisor deliberately stops handling it, so recovery requires restarting the workgroup itself (the deployment's process supervisor, or an operator).
3. Otherwise, waits for the exponential backoff delay, then spawns a fresh child process with the same configuration.

Each retry logs `workgroup.restart_scheduled` with a `reason` of `child_exit` or `spawn_failed`, so the two failure classes are distinguishable in the logs.

The backoff resets to `backoff_initial` after a stable period (no restart attempts within `burst_window`).

A failed output pump is not a restart trigger: the supervisor logs `workgroup.stream_pump_failed` and the child keeps running, with its output no longer forwarded.

## Credential rotation (SIGHUP)

Each child worker supports SIGHUP credential hot-reload: the signal triggers the child's reload coordinator, which rebuilds every factory-backed pool, connection, and Redis client with fresh credentials without dropping the process (see the workers guide's reload section). A single SIGHUP can therefore rotate credentials for a whole workgroup:

```shell
kill -HUP $(pgrep -f "taskq workgroup start")
```

The supervisor has a SIGHUP handler of its own. Without one, the signal's default disposition would terminate the supervisor itself and orphan every child. The handler instead forwards SIGHUP to each living child (`workgroup-reload-signal` is logged with the child count), and each child then runs its own hot-reload independently.

Details that matter in production:

- **Children mid-restart are skipped.** A child with no live process (spawn failed, or exited and awaiting its backoff respawn) receives nothing; the replacement child starts with current credentials anyway. A child that exits between the liveness check and the signal delivery is similarly ignored rather than errored on; the liveness monitor owns that child's next lifecycle step.
- **A SIGHUP during shutdown is ignored.** Reloading pools a child is about to tear down is wasted churn, and the shutdown path may already be draining them.
- **Reload failures never kill the child.** A failed rotation is logged by the child (`credentials-reload-failed`) and the old resources stay live; re-send SIGHUP after fixing the credential source.
- **Windows has no SIGHUP.** The handler is only registered where the signal exists; on platforms without it, use the child-level `TASKQ_RELOAD_INTERVAL` timer rotation instead.
- **The supervisor's own health-check pool is not rotated by SIGHUP.** Its connection is rebuilt on supervisor restart; if the health DSN's credentials rotate while a supervisor runs for a long time, plan a supervisor restart into the rotation.

## Running in production

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: taskq-workers
spec:
  replicas: 1
  template:
    spec:
      containers:
        - name: workgroup
          image: myapp:latest
          command: ["taskq", "workgroup", "start", "/etc/taskq/workgroup.toml"]
          env:
            - name: TASKQ_PG_DSN
              valueFrom:
                secretKeyRef:
                  name: taskq-db
                  key: dsn
```

Each replica runs its own supervisor with its own `workgroup_instance` UUID. Workers spawned by different replicas are distinguishable in the database by `workgroup_instance`.

### Docker Compose / systemd

```shell
taskq workgroup start /etc/taskq/workgroup.toml
```

The process runs in the foreground, with no daemonisation. Use your init system or process manager to keep it alive.

## Limits

- The workgroup supervisor is a single process. If the supervisor itself crashes (e.g. OOM), all managed workers become orphaned and will eventually be reclaimed by the PG recovery sweep. Run the supervisor under a process manager (systemd, Docker, Kubernetes) for automatic restart.
- Worker processes do not inherit the supervisor's config changes at runtime. To change a worker's configuration, update the TOML file and restart the supervisor.
- Health checking requires a PG connection per supervisor instance. The pool is sized to `health_worker_count + 1`.
