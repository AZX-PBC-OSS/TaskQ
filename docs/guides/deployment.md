# Production Deployment

TaskQ is an async-native, Postgres-backed background job library for Python 3.12+. This guide covers running TaskQ workers, the admin UI, and supporting infrastructure in production: container orchestration, database and Redis configuration, observability, scaling, and security hardening.

!!! warning "Pre-1.0 stability"
    TaskQ is pre-1.0. Breaking changes — including schema changes — may land in
    minor version bumps (`0.x.0`), not only majors. Pin your `taskq-py` version
    and review the [Changelog](../changelog.md) before every upgrade. See
    [Upgrading](upgrading.md) for the forward-only migration policy and backup
    requirements.

---

## Production Checklist

- [ ] **Postgres** — dedicated database or schema with `taskq migrate up` applied
- [ ] **Direct DSN** — `TASKQ_PG_DSN` (or `TASKQ_PG_DSN_DIRECT`) points at Postgres directly, **not** a transaction-mode PgBouncer
- [ ] **Migrations** — `taskq migrate up` run before workers start (or `TASKQ_MIGRATE_ON_START=true` for the admin UI)
- [ ] **Worker supervisor** — systemd unit, Docker container, or Kubernetes Deployment
- [ ] **Health probes** — `taskq health live` / `taskq health ready` wired to exec probes (not `httpGet` — the worker serves on a Unix socket)
- [ ] **Shutdown budget** — `termination_grace_period` > `cancellation_grace_period + cleanup_grace_period + 5`
- [ ] **Admin UI auth** — `auth_dependency` hook or reverse proxy with auth; `TASKQ_ADMIN_UI_REQUIRE_AUTH` left at default (`true`)
- [ ] **Admin actions** — `TASKQ_ADMIN_ACTIONS_ENABLED` left at `false` unless operators need cancel/retry/run-now
- [ ] **OTel exporter** — `OTEL_EXPORTER_OTLP_ENDPOINT` pointed at a collector or OTLP-compatible backend
- [ ] **Log format** — `TASKQ_LOG_FORMAT=json` for structured log aggregation
- [ ] **Redis** (optional) — provisioned if you need real-time progress fanout or Redis-backed rate limiters
- [ ] **Resource limits** — CPU and memory limits on worker containers
- [ ] **Backups** — Postgres backup or PITR window confirmed; forward-only migrations have no `down` path

---

## Worker Deployment

The worker is a single asyncio process running a `TaskGroup` of sibling coroutines: heartbeat, NOTIFY listener, maintenance leader, producer, and `max_concurrency` consumer loops. It blocks until SIGTERM/SIGINT and exits `0` on clean shutdown.

### Container

```dockerfile
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra otel --extra redis
COPY . .
CMD ["uv", "run", "taskq", "worker", "--actors", "myapp.actors:registry"]
```

### systemd

```ini
[Unit]
Description=TaskQ Worker
After=network-online.target postgresql.service

[Service]
Type=simple
User=taskq
WorkingDirectory=/opt/myapp
EnvironmentFile=/etc/taskq/worker.env
ExecStart=/opt/myapp/.venv/bin/taskq worker --actors myapp.actors:registry
Restart=on-failure
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=120
```

`TimeoutStopSec` must exceed `TASKQ_TERMINATION_GRACE_PERIOD` so systemd does not SIGKILL the worker before it finishes its drain/cancel/abandon sequence. For custom bootstrap (DI providers, ErrorReporter), use `worker_main` programmatically — see [workers.md](workers.md#programmatically-via-worker_main).

### Concurrency tuning

`TASKQ_MAX_CONCURRENCY` (default `8`) bounds simultaneously executing jobs. The derived `worker_pool_size` is `int(max_concurrency * 1.5)`.

| Workload type | Recommended `max_concurrency` | Rationale |
|---|---|---|
| I/O-bound (HTTP, DB queries) | 16–64 | asyncio multiplexes I/O cheaply |
| Mixed I/O + CPU | 8–16 | Offload CPU work to `run_in_executor` |
| CPU-bound (image, ML) | 2–4 per core | CPU work blocks the event loop |

!!! tip "CPU-bound actors"
    asyncio consumers are cooperatively concurrent, not threaded. CPU-bound
    work blocks the event loop and starves heartbeats. Offload it with
    `await loop.run_in_executor(None, blocking_fn, ...)` or assign CPU-bound
    actors to a dedicated worker with low `max_concurrency`.

See [workers.md](workers.md) for the full concurrency model and pool sizing.

---

## Database Configuration

### Direct connection requirement

TaskQ relies on session-scoped Postgres features — `LISTEN/NOTIFY` and `pg_try_advisory_lock` — that break under transaction-mode PgBouncer. The worker opens five connection paths:

| Connection | DSN used | Why |
|---|---|---|
| `dispatcher_pool` | `pg_dsn_direct` | Dispatch SQL uses `FOR UPDATE SKIP LOCKED` |
| `heartbeat_pool` | `pg_dsn_direct` | Heartbeat extends job locks |
| `notify_conn` | `pg_dsn_direct` | `LISTEN` state is session-scoped |
| `leader_conn` | `pg_dsn_direct` | Advisory lock is session-scoped |
| `worker_pool` | `pg_dsn_pooled` | Short transactions; safe through PgBouncer |

```bash
TASKQ_PG_DSN_DIRECT=postgresql://taskq:secret@postgres.internal:5432/taskq
TASKQ_PG_DSN_POOLED=postgresql://taskq:secret@pgbouncer.internal:6432/taskq
```

Without PgBouncer, set only `TASKQ_PG_DSN` — both split DSNs fall back to it. **Never** point `TASKQ_PG_DSN` at a transaction-mode PgBouncer. See [workers.md — PgBouncer compatibility](workers.md#pgbouncer-compatibility).

### Schema isolation and multi-tenancy

`TASKQ_SCHEMA_NAME` (default `taskq`) isolates all TaskQ tables into a dedicated Postgres schema. Multiple clusters can share one database:

```bash
TASKQ_SCHEMA_NAME=taskq_billing    TASKQ_PG_DSN=postgresql://app:secret@postgres:5432/appdb
TASKQ_SCHEMA_NAME=taskq_notifications TASKQ_PG_DSN=postgresql://app:secret@postgres:5432/appdb
```

Each schema gets its own migration set, NOTIFY channel (`taskq_wake_{schema}`), and advisory-lock keyspace. Must match `^[A-Za-z_][A-Za-z0-9_]*$`.

### Migration strategy

Migrations are **forward-only** and idempotent. Run before workers start:

```shell
taskq migrate up
```

Each migration is recorded in `{schema}.schema_migrations` with a SHA-256 checksum. Re-running is a no-op when all are applied.

!!! warning "No down migrations"
    There is no `down` migration. To revert, restore the database from a
    backup taken before the migration was applied. Always take a backup (or
    confirm a PITR window) before upgrading. See [Upgrading](upgrading.md).

**Deployment order:** (1) apply migrations as a pre-deploy job or init container, (2) start workers — they call `sync_actor_config` at startup and fail with `ActorConfigDriftList` if a registered actor's **structural** config (`queue`, `metadata`) differs from the stored row. Note this does **not** detect a stale *schema*: no schema-version precheck runs on the worker path, so a worker started against an unmigrated database fails later, at its first query, rather than at startup, (3) start the admin UI (optionally with `TASKQ_MIGRATE_ON_START=true`).

For rolling deploys where actor config changes, deploy the first pod with `TASKQ_FORCE_UPDATE_ACTOR_CONFIG=true` to overwrite stored config, then deploy the rest without it. See [workers.md — ActorConfig sync](workers.md#actorconfig-sync).

---

## Redis (Optional)

Redis is optional. TaskQ degrades gracefully without it:

| Feature | With Redis | Without Redis |
|---|---|---|
| Progress fanout | Redis pub/sub → SSE push | Postgres only; admin UI polls |
| Admin UI mode | Real-time (SSE) | Polling (2s interval) |
| Rate limiters | Redis backend (shared, low-latency) | Postgres fallback (default) or in-memory (per-process) |
| Reservation slots | Redis state in admin UI | Postgres state only |

```bash
TASKQ_REDIS_URL=redis://redis.internal:6379/0
```

!!! tip "When to provision Redis"
    Provision Redis if you use Redis-backed rate limiters across multiple
    workers, or if operators need real-time admin UI updates. Without Redis,
    rate limiting falls back to Postgres (higher latency, more DB load).

See [rate-limiting.md](rate-limiting.md) and [progress.md](progress.md) for details.

---

## Admin UI in Production

The admin UI (`taskq ui serve`) is a FastAPI + Jinja2 dashboard on `TASKQ_ADMIN_HOST:TASKQ_ADMIN_PORT` (defaults: `0.0.0.0:8080`). It **fails closed by default** — in non-dev environments, `create_router()` raises `RuntimeError` if no `auth_dependency` and `TASKQ_ADMIN_UI_REQUIRE_AUTH=true` (the default).

| Variable | Default | Description |
|---|---|---|
| `TASKQ_ADMIN_UI_REQUIRE_AUTH` | `true` | Raises `RuntimeError` at startup if no `auth_dependency` in non-dev |
| `TASKQ_ADMIN_ACTIONS_ENABLED` | `false` | When `false`, cancel/retry/run-now return `403` |
| `TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET` | `false` | Gates the rate-limit reset endpoint |
| `TASKQ_HEALTH_TOKEN` | _(none)_ | Bearer token for machine-to-machine health/metrics |
| `TASKQ_HEALTH_REQUIRE_TOKEN` | `true` | Fails closed if `TASKQ_HEALTH_TOKEN` empty in non-dev |

### Reverse proxy authentication

Run `taskq ui serve` behind an authenticating reverse proxy and set `TASKQ_ADMIN_UI_REQUIRE_AUTH=false`:

```nginx
server {
    listen 443 ssl;
    server_name admin.example.com;
    location /admin/ {
        auth_basic "TaskQ Admin";
        auth_basic_user_file /etc/nginx/.htpasswd;
        proxy_pass http://127.0.0.1:8080;
    }
}
```

Set `TASKQ_ADMIN_UI_REQUIRE_AUTH=false` and `TASKQ_HEALTH_REQUIRE_TOKEN=false` to suppress the fail-closed checks when relying on the proxy for auth.

Run the admin UI as a **separate process** from the worker — different scaling, exposure, and resource characteristics. See [admin-ui.md](admin-ui.md) for `create_router()` embedding and SSO configuration.

---

## Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: taskq-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: taskq-worker
  template:
    metadata:
      labels:
        app: taskq-worker
    spec:
      terminationGracePeriodSeconds: 120
      initContainers:
        - name: migrate
          image: myapp:latest
          command: ["taskq", "migrate", "up"]
          env:
            - name: TASKQ_PG_DSN
              valueFrom:
                secretKeyRef:
                  name: taskq-db
                  key: dsn
      containers:
        - name: worker
          image: myapp:latest
          command: ["taskq", "worker", "--actors", "myapp.actors:registry"]
          env:
            - name: TASKQ_PG_DSN
              valueFrom:
                secretKeyRef:
                  name: taskq-db
                  key: dsn
            - name: TASKQ_REDIS_URL
              valueFrom:
                secretKeyRef:
                  name: taskq-redis
                  key: url
            - name: TASKQ_ENVIRONMENT
              value: production
            - name: TASKQ_MAX_CONCURRENCY
              value: "16"
            - name: TASKQ_QUEUES
              value: default,priority
            - name: OTEL_EXPORTER_OTLP_ENDPOINT
              value: http://otel-collector:4317
            - name: OTEL_SERVICE_NAME
              value: taskq-worker
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
          resources:
            requests:
              cpu: "500m"
              memory: "512Mi"
            limits:
              cpu: "2000m"
              memory: "1Gi"
```

See [configuration.md](configuration.md#production-example-env) for the full set of `TASKQ_*` environment variables and cross-field validation constraints.

### Health probes

The worker serves the same two endpoints — `/live` and `/ready` — over two transports:

| Transport | Enabled by | Reachable from |
| --- | --- | --- |
| Unix socket at `TASKQ_HEALTH_SOCKET_PATH` (default `/tmp/taskq_health.sock`) | `TASKQ_HEALTH_ENABLED` (on by default) | inside the container: `taskq health live` / `taskq health ready`, so Kubernetes `exec` probes |
| TCP, `TASKQ_HEALTH_HOST`:`TASKQ_HEALTH_PORT` | setting `TASKQ_HEALTH_PORT` (unset by default) | anything that can reach the container port: `httpGet` and `tcpSocket` probes |

Responses are what orchestrators expect: **200** when healthy, **503** when not, **404** for an
unknown path. Kubernetes and Azure Container Apps both treat 200-399 as probe success, so a 503
reliably fails a probe on either.

`/live` answers whether the event loop is responsive — a failing liveness probe means *restart
me*. `/ready` additionally pings Postgres, checks for stale worker loops, fails during shutdown,
and runs any checks you registered (see below) — a failing readiness probe means *stop sending me
work*, not *restart me*.

!!! danger "A configured probe port that cannot bind fails startup"
    If `TASKQ_HEALTH_PORT` is set and the port cannot be bound, the worker **refuses to start**
    rather than running with its probes silently dead. That is deliberate: a worker that came up
    with a dead listener would fail every probe, or — worse, under a `tcpSocket` probe against a
    port some other process holds — pass every probe while nothing checked it.

#### Azure Container Apps

ACA supports **only `httpGet` and `tcpSocket` probes — there is no `exec` probe type**
([Health probes in Azure Container Apps][aca-probes], "Restrictions"). A Unix socket is therefore
unprobeable there, and `taskq health ready` cannot be used as a probe. Set `TASKQ_HEALTH_PORT` and
probe it over HTTP:

```yaml
containers:
  - image: myregistry.azurecr.io/taskq-worker:1.0.0
    name: worker
    env:
      - name: TASKQ_HEALTH_PORT
        value: "8600"
    probes:
      - type: Liveness
        httpGet:
          path: /live
          port: 8600
        initialDelaySeconds: 5
        periodSeconds: 10
        failureThreshold: 3
      - type: Readiness
        httpGet:
          path: /ready
          port: 8600
        initialDelaySeconds: 5
        periodSeconds: 5
        timeoutSeconds: 5
        failureThreshold: 48
      - type: Startup
        httpGet:
          path: /live
          port: 8600
        initialDelaySeconds: 3
        periodSeconds: 3
        failureThreshold: 40
```

!!! tip "Replacing a TCP↔Unix bridge"
    Before `TASKQ_HEALTH_PORT` existed, the only way to probe a worker on ACA was to run a second
    TCP listener alongside it that forwarded each connection to the Unix socket. If you built one,
    you can now delete it and set `TASKQ_HEALTH_PORT` to the port it used to listen on: the probe
    config above is unchanged, and the endpoints, status codes and body shape are identical
    because the bridge was forwarding to this same handler. Drop the bridge module, its
    `start`/`stop` calls in your worker entrypoint, and its host/port settings; keep the `port` in
    your infrastructure template pointed at the same number.

Notes drawn from the ACA probe reference:

- The probe port does **not** have to be the ingress target port — a worker with no ingress at all
  can still expose 8600 purely for probes. Port values must be integers; named ports are not
  supported.
- Only one probe of each type per container.
- If you enable ingress and define no probes, the portal adds default **TCP** probes against the
  ingress target port. A TCP probe only proves something accepted a connection, so prefer
  `httpGet` against `/ready` — that is the only variant that reflects Postgres reachability and
  shutdown state.
- `TASKQ_HEALTH_HOST` defaults to `0.0.0.0`, which is what the ACA runtime needs to reach the
  replica. Do not narrow it to `127.0.0.1` unless a sidecar in the same network namespace is the
  only prober.

#### Kubernetes

Both styles work. `httpGet` is the portable choice and the one to reach for if the same manifest
must also run on ACA:

```yaml
          ports:
            - name: health
              containerPort: 8600
          livenessProbe:
            httpGet:
              path: /live
              port: 8600
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /ready
              port: 8600
            initialDelaySeconds: 5
            periodSeconds: 10
```

with `TASKQ_HEALTH_PORT=8600` in the container env. The `exec` probes shown in the Deployment
above remain valid on Kubernetes and need no open port; they are the better choice when you would
rather not expose one.

#### Azure App Service

App Service Health check pings a single path on the app's own HTTP port every minute and treats
**200-299** as healthy ([Monitor the health of App Service instances][appsvc-health]). It has no
liveness/readiness split, so point it at `/ready` — the endpoint that reflects dependencies — and
run the worker's health listener on the port App Service routes to.

#### Registering your own readiness checks

`/ready` covers TaskQ's own dependencies. To add your application's, register a check before
starting the worker:

```python
from taskq.worker.health import register_readiness_check


async def search_index_reachable() -> str | None:
    """Return None when healthy, or a reason string when not."""
    if not await index.ping():
        return "search index unreachable"
    return None


register_readiness_check("search_index", search_index_reachable)
```

Registered checks run on both transports and on `taskq health ready`. They fail closed: a check
that raises, or that outruns `TASKQ_HEALTH_READINESS_CHECK_TIMEOUT`, makes the worker unready and
its reason appears in the `/ready` body.

[aca-probes]: https://learn.microsoft.com/en-us/azure/container-apps/health-probes
[appsvc-health]: https://learn.microsoft.com/en-us/azure/app-service/monitor-instances-health-check

Add a `PodDisruptionBudget` (`minAvailable: 1`, selector matching `app: taskq-worker`) to prevent voluntary evictions from taking all workers offline during node drains.

!!! warning "terminationGracePeriodSeconds"
    Setting this just above `TASKQ_TERMINATION_GRACE_PERIOD` is **not enough**,
    and is the sizing mistake most likely to bite you.

    The shutdown phases (DRAINING → CANCELLING → FORCING → ABANDONING) are
    bounded by `TASKQ_CANCELLATION_GRACE_PERIOD` + `TASKQ_CLEANUP_GRACE_PERIOD`,
    but the bounded-close tail that unwinds *after* them is **additive**, and
    nothing *enforces* that it fits inside `TASKQ_TERMINATION_GRACE_PERIOD` —
    the shipped default does cover it, but a custom value below the worst case
    only warns at startup. Against a dead or hung
    Postgres/Redis — an Azure Cache failover, or a token expiry dropping every
    connection, i.e. exactly when you are being SIGTERMed — each of the 5
    sequential bounded closes can take `CLOSE_TIMEOUT_SECS` (5s), plus a 2s
    progress-publish drain: **27s of tail**.

    Size the pod grace from the whole budget:

    ```
    terminationGracePeriodSeconds
        >= TASKQ_CANCELLATION_GRACE_PERIOD
         + TASKQ_CLEANUP_GRACE_PERIOD
         + 27      # 5 bounded closes x 5s + 2s publish drain
    ```

    At TaskQ's defaults (75 / 30 / 10) the modelled worst case is **67s**, so
    `terminationGracePeriodSeconds: 80` is a safe value at defaults — not the
    `60`-ish the old advice implied. (The default grace itself covers the
    model; it is 75 rather than 67 so the ~72s sibling-crash path — six
    sequential closes where the orchestrated leader-conn close never ran —
    fits inside it too.) The worker logs
    `shutdown-budget-exceeds-termination-grace` at startup, with the computed
    number, whenever the configured budget does not cover it — i.e. for
    custom grace combinations, not for the shipped defaults.

    If the kubelet SIGKILLs mid-unwind, in-flight `write_cancel_escalation` /
    `mark_abandoned` terminal writes may not land; those jobs are left `running`
    and recovered later by the leader's crash-reclaim sweep after `lock_lease`
    expires, rather than finalizing cleanly.

---

## Docker Compose for Production

A hardened Compose file with resource limits, healthchecks, restart policies, and a migration gate. Postgres and Redis are assumed to exist with healthchecks configured (see the [dev docker-compose.yml](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/docker-compose.yml) for reference service definitions):

```yaml
services:
  migrate:
    image: myapp:latest
    command: ["taskq", "migrate", "up"]
    environment:
      TASKQ_PG_DSN: postgresql://taskq:${POSTGRES_PASSWORD}@postgres:5432/taskq
    depends_on:
      postgres:
        condition: service_healthy
    restart: "no"

  worker:
    image: myapp:latest
    command: ["taskq", "worker", "--actors", "myapp.actors:registry"]
    restart: unless-stopped
    environment:
      TASKQ_PG_DSN: postgresql://taskq:${POSTGRES_PASSWORD}@postgres:5432/taskq
      TASKQ_REDIS_URL: redis://redis:6379/0
      TASKQ_ENVIRONMENT: production
      TASKQ_MAX_CONCURRENCY: "16"
      TASKQ_QUEUES: default,priority
      TASKQ_TERMINATION_GRACE_PERIOD: "120"
      TASKQ_CANCELLATION_GRACE_PERIOD: "60"
      TASKQ_CLEANUP_GRACE_PERIOD: "20"
      TASKQ_LOCK_LEASE: "90"
      OTEL_EXPORTER_OTLP_ENDPOINT: http://otel-collector:4317
      OTEL_SERVICE_NAME: taskq-worker
    depends_on:
      migrate:
        condition: service_completed_successfully
      redis:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "taskq", "health", "ready"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 15s
    stop_grace_period: 130s
    deploy:
      resources:
        limits:
          cpus: "2.0"
          memory: 1G
```

The admin UI service follows the same pattern with `command: ["taskq", "ui", "serve"]`, `TASKQ_ADMIN_UI_REQUIRE_AUTH: "false"`, and `TASKQ_HEALTH_REQUIRE_TOKEN: "false"` (see [Admin UI in Production](#admin-ui-in-production)). Add a `migrate` dependency to both worker and admin services so they wait for migrations to complete.

!!! warning "stop_grace_period must exceed termination_grace_period"
    Docker's `stop_grace_period` (default 10s) controls how long Compose waits
    between SIGTERM and SIGKILL. Set it above `TASKQ_TERMINATION_GRACE_PERIOD`
    **plus the ~27s bounded-close tail** (see the terminationGracePeriodSeconds
    warning above)
    so the worker can complete its shutdown sequence.

---

## Workgroup Deployment

The workgroup supervisor (`taskq workgroup start`) manages multiple `taskq worker` subprocesses within a single container, each with independent queue subscriptions and concurrency caps.

| Approach | Use when |
|---|---|
| **Workgroup** | Multiple queue groups in one container; single-pod simplicity |
| **Separate Deployments** | Independent scaling per queue; independent rolling deploys |

```toml
actors = "myapp.actors:registry"

[defaults]
poll_interval = 1.0
max_concurrency = 4

[[workers]]
name = "api"
queues = ["default", "priority"]
max_concurrency = 16
poll_interval = 0.5

[workers.health]
enabled = true
check_interval = 15
stale_after = 60

[[workers]]
name = "batch"
queues = ["email", "report", "cleanup"]
max_concurrency = 2
poll_interval = 5.0
```

```shell
taskq workgroup start /etc/taskq/workgroup.toml
```

The supervisor assigns a `workgroup_instance` UUIDv7 for cross-process correlation in the `workers` table.

!!! warning "Supervisor is a single point of failure"
    If the supervisor crashes (e.g. OOM), managed workers become orphaned and
    are reclaimed by the leader's sweep after `lock_lease` expires. Always run
    it under a process manager (systemd, Docker, Kubernetes) for restart.

See [workgroups.md](workgroups.md) for the full configuration reference and restart policy.

---

## Observability Setup

TaskQ instruments itself with OpenTelemetry (vendor-neutral) and structlog. No vendor SDK is bundled — export to any OTLP-compatible backend via standard OTel environment variables. Install the OTel extra: `uv add "taskq-py[otel]"`.

### OTel exporter configuration

| Variable | Example | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | OTLP gRPC endpoint (`:4318` for HTTP) |
| `OTEL_SERVICE_NAME` | `taskq-worker` | Service name for all spans/metrics |
| `OTEL_RESOURCE_ATTRIBUTES` | `deployment.environment=production,k8s.pod.name=worker-0` | Resource attributes on all telemetry |

TaskQ does not override standard OTel variables. All spans and metrics use instrumentation name `"taskq"` and OTel messaging semconv attributes for dashboard compatibility.

### Prometheus scrape

The worker exposes Prometheus metrics on its Unix socket at `GET /metrics`. For HTTP scraping, use the admin UI's `/jobs/health/metrics` endpoint (requires `taskq[prometheus]`): point a scrape job at `admin:8080` with `metrics_path: /jobs/health/metrics`.

### Structured logging

Use `TASKQ_LOG_FORMAT=json` (the default) in production. Every log line includes `worker_id`, `timestamp` (ISO 8601 UTC), `level`, and `trace_id`/`span_id` from the active OTel span. See [observability.md](observability.md) for the full span hierarchy and metrics reference.

---

## Scaling Considerations

### Horizontal scaling

Adding worker processes is the primary scaling lever. Multiple workers against the same database are fully supported — dispatch uses `FOR UPDATE SKIP LOCKED`, so concurrent workers never pick up the same job. Only one worker per cluster holds the maintenance leader advisory lock; if the leader dies, another worker wins the next election. Scale by increasing replica count (`kubectl scale deployment taskq-worker --replicas=10`).

### Queue partitioning

Partition work across named queues and assign workers to specific subsets to prevent a deep backlog on one queue from starving others:

```bash
TASKQ_QUEUES=default,priority taskq worker --actors myapp.actors:registry
TASKQ_QUEUES=media taskq worker --actors myapp.actors:registry
```

For multi-tenant queues, set `round_robin` mode to interleave by `fairness_key` cohort:

```bash
taskq queues set-mode multi round_robin
```

See [workers.md — Queue dispatch modes](workers.md#queue-dispatch-modes).

### max_concurrent and max_pending

`max_concurrent` (per-actor via `@actor(max_concurrent=N)`) is a **best-effort** fleet-wide damper on how many jobs for an actor run simultaneously — distinct from `TASKQ_MAX_CONCURRENCY` (total jobs per process).

!!! warning "`max_concurrent` is not a hard cap"
    Dispatch reads its `running` count once per round, before taking row locks, and never rechecks it. Two worker replicas dispatching concurrently each see the same count, each admit up to the cap, and lock *disjoint* rows — so both succeed. The over-dispatch bound is `(num_producers - 1) * max_concurrent` per round, and those jobs genuinely run; reclaiming stale locks does not undo an over-dispatch.

    With `max_concurrent=2` and 3 replicas you can see 6 concurrent executions. For a memory- or GPU-bound actor that is an OOMKill, a restart, and a re-dispatch.

    **If you need a strict cap**, use the per-queue leased-slot reservation instead: `taskq queues set-max-concurrent <queue> --max-concurrent N`. Slots are physical rows and each acquire is a single read-and-write statement on one row, so there is no read-then-decide window. See [rate-limiting.md](rate-limiting.md#concurrency-reservations). Note it is read once at worker startup, so changing it needs a worker restart, and it bounds a *queue*, not an actor. `max_pending` (per-actor via `@actor(max_pending=N)`) caps queued `pending` jobs; when exceeded, `enqueue` is rejected and `taskq.backpressure.errors` is incremented. Monitor `taskq.queue.depth` (leader samples every 15s) for backlog and `taskq.backpressure.errors` for sustained producer pressure.

### Connection pool sizing

| Pool | Default | Scales with |
|---|---|---|
| `dispatcher_pool` | 4 | `TASKQ_DISPATCHER_POOL_SIZE` |
| `heartbeat_pool` | 4 | `TASKQ_HEARTBEAT_POOL_SIZE` |
| `worker_pool` | `int(max_concurrency * 1.5)` | `TASKQ_MAX_CONCURRENCY` |
| `notify_conn` + `leader_conn` | 2 (dedicated) | Fixed |

Total per worker ≈ `dispatcher + heartbeat + worker_pool + 2`. For 10 workers at `max_concurrency=16`: ~10 × 34 = 340 connections. Ensure Postgres `max_connections` accommodates this plus your application's connections.

---

## Security Hardening

### Admin UI authentication

1. **Embed in your FastAPI app** with an `auth_dependency` callable (HTTPBearer, OIDC, session middleware). See [admin-ui.md](admin-ui.md#protecting-the-router-with-fastapi-authentication).
2. **Or run behind a reverse proxy** with auth (nginx basic auth, OAuth2 proxy, mTLS) and set `TASKQ_ADMIN_UI_REQUIRE_AUTH=false`.
3. **Keep `TASKQ_ADMIN_ACTIONS_ENABLED=false`** unless operators need cancel/retry/run-now — these are write operations that modify job state.
4. **Set `TASKQ_HEALTH_TOKEN`** for machine-to-machine health/metrics, or explicitly set `TASKQ_HEALTH_REQUIRE_TOKEN=false` if relying on network policy.

### Network policies

| Port | Service | Exposed to |
|---|---|---|
| 5432 | Postgres | Workers, admin UI, migrate jobs only |
| 6379 | Redis | Workers, admin UI only |
| 8080 | Admin UI | Internal operators only; never public |
| Unix socket | Worker health | Same pod only (exec probes) |

The admin UI should never be exposed to the public internet without an authentication layer. Use Kubernetes NetworkPolicy resources to restrict pod-to-pod communication.

### Database credentials

Store `TASKQ_PG_DSN` and `TASKQ_REDIS_URL` in a secret manager (Kubernetes `Secret` + `secretKeyRef` as shown in the [Deployment manifest](#kubernetes-deployment)), not in image layers or git. Use a dedicated Postgres role for TaskQ with least-privilege permissions: `CREATE` on the schema for migrations, `SELECT, INSERT, UPDATE, DELETE` on all TaskQ tables for runtime.

### Redis ACLs

If Redis is shared, restrict the TaskQ user to its keyspace:

```shell
redis-cli ACL SETUSER taskq on >${REDIS_PASSWORD} ~taskq:* +@all -@dangerous
```

!!! tip "Redis TLS"
    For managed Redis (ElastiCache, MemoryDB, Azure Cache), use `rediss://`
    to enable TLS: `TASKQ_REDIS_URL=rediss://redis.internal:6379/0`.
