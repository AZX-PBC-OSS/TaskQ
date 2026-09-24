# Production Deployment

TaskQ is an async-native, Postgres-backed background job library for Python 3.12+. This guide covers running TaskQ workers, the admin UI, and supporting infrastructure in production: container orchestration, database and Redis configuration, observability, scaling, and security hardening.

!!! warning "Pre-1.0 stability"
    TaskQ is pre-1.0. Breaking changes (including schema changes) may land in
    minor version bumps (`0.x.0`), not only majors. Pin your `taskq-py` version
    and review the [Changelog](../changelog.md) before every upgrade. See
    [Upgrading](upgrading.md) for the forward-only migration policy and backup
    requirements.

---

## Production Checklist

- [ ] **Postgres**: dedicated database or schema with `taskq migrate up` applied
- [ ] **Direct DSN**: `TASKQ_PG_DSN` (or `TASKQ_PG_DSN_DIRECT`) points at Postgres directly, **not** a transaction-mode PgBouncer
- [ ] **Migrations**: `taskq migrate up` run before workers start (or `TASKQ_MIGRATE_ON_START=true` for the admin UI)
- [ ] **Worker supervisor**: systemd unit, Docker container, or Kubernetes Deployment
- [ ] **Health probes**: `taskq health live` / `taskq health ready` as exec probes, or `httpGet`/`tcpSocket` probes against the optional TCP listener (`TASKQ_HEALTH_PORT`) on platforms without exec probes; see [Listener deployment recipes](#listener-deployment-recipes)
- [ ] **Shutdown budget**: `termination_grace_period` > `cancellation_grace_period + cleanup_grace_period + 5`, and the platform stop grace (Kubernetes `terminationGracePeriodSeconds`, Compose `stop_grace_period`, systemd `TimeoutStopSec`) above the worker's whole worst case, not just this setting: see the [grace warning](#health-probes)
- [ ] **Job timeouts**: `TASKQ_DEFAULT_START_TO_CLOSE` set as a fleet safety net; every long-running actor declares its own `start_to_close`; every `kind="indefinite"` actor has a `retry.time_budget` (see [ops.md](ops.md#2-timeouts-start_to_close-and-schedule_to_close))
- [ ] **Connection budget**: fleet connection count computed against Postgres `max_connections` including application pools (see [ops.md](ops.md#4-sizing-workers-and-postgres-connections))
- [ ] **DLQ routing**: `on_retry_exhausted` / `ErrorReporter` target chosen; there is no built-in dead-letter queue
- [ ] **Admin UI auth**: `auth_dependency` hook or reverse proxy with auth; `TASKQ_ADMIN_UI_REQUIRE_AUTH` left at default (`true`)
- [ ] **Progress router auth**: `auth_dependency` on `taskq.web.progress.create_router` (the admin UI forwards its own); `TASKQ_PROGRESS_REQUIRE_AUTH` left at default (`true`)
- [ ] **Admin actions**: `TASKQ_ADMIN_ACTIONS_ENABLED` left at `false` unless operators need cancel/retry/run-now
- [ ] **OTel exporter**: `OTEL_EXPORTER_OTLP_ENDPOINT` pointed at a collector or OTLP-compatible backend
- [ ] **Log format**: `TASKQ_LOG_FORMAT=json` for structured log aggregation
- [ ] **Redis** (optional): provisioned if you need real-time progress fanout or Redis-backed rate limiters; when set, the admin router must also receive the client (`create_router(redis_client=...)`, see [Redis (Optional)](#redis-optional))
- [ ] **Resource limits**: CPU and memory limits on worker containers
- [ ] **Backups**: Postgres backup or PITR window confirmed; forward-only migrations have no `down` path

---

## Worker Deployment

The worker is a single asyncio process running a `TaskGroup` of sibling coroutines: heartbeat, NOTIFY listener, maintenance leader, producer, and `max_concurrency` consumer loops. It blocks until SIGTERM/SIGINT and exits `0` on clean shutdown.

### Container

The repo carries a production image definition at
[`Dockerfile`](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/Dockerfile) at
the repository root; CI builds it on every release tag (build only, no push).
It is a multi-stage build: a uv builder stage resolves the locked dependency
set with `uv sync --frozen --no-dev` (dev group excluded, `otel` and `redis`
extras installed), and a slim runtime stage ships only the resulting venv,
run as a non-root `taskq` user. The image sets `TASKQ_HEALTH_PORT=8600` and
`EXPOSE 8600` so orchestrators without exec probes can route HTTP probes at
the TCP health listener, and its `HEALTHCHECK` runs `taskq health ready`
against the in-container Unix socket, the same exec probe the Kubernetes and
Compose recipes below use.

Applications that use TaskQ extend the image rather than duplicating it;
`PATH` already points at the installed venv:

```dockerfile
FROM taskq-worker        # this repo's Dockerfile, as built by CI
COPY myapp/ myapp/
CMD ["taskq", "worker", "--actors", "myapp.actors:registry"]
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

`TimeoutStopSec` must exceed `TASKQ_TERMINATION_GRACE_PERIOD` so systemd does not SIGKILL the worker before it finishes its drain/cancel/abandon sequence. For custom bootstrap (DI providers, ErrorReporter), use `worker_main` programmatically; see [workers.md](workers.md#programmatically-via-worker_main).

### Concurrency tuning

`TASKQ_MAX_CONCURRENCY` (default `8`) bounds simultaneously executing jobs. The derived `worker_pool_size` is `int(max_concurrency * 1.5)`.

| Workload type | Recommended `max_concurrency` | Rationale |
|---|---|---|
| I/O-bound (HTTP, DB queries) | 16-64 | asyncio multiplexes I/O cheaply |
| Mixed I/O + CPU | 8-16 | Offload CPU work to `run_in_executor` |
| CPU-bound (image, ML) | 2-4 per core | CPU work blocks the event loop |

!!! tip "CPU-bound actors"
    asyncio consumers are cooperatively concurrent, not threaded. CPU-bound
    work blocks the event loop and starves heartbeats. Offload it with
    `await loop.run_in_executor(None, blocking_fn, ...)` or assign CPU-bound
    actors to a dedicated worker with low `max_concurrency`.

See [workers.md](workers.md) for the full concurrency model and pool sizing, and [ops.md](ops.md#4-sizing-workers-and-postgres-connections) for the worked fleet-sizing worksheet (throughput math and the Postgres connection budget).

---

## Database Configuration

### Direct connection requirement

TaskQ relies on session-scoped Postgres features (`LISTEN/NOTIFY` and `pg_try_advisory_lock`) that break under transaction-mode PgBouncer. The worker opens five connection paths:

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

Without PgBouncer, set only `TASKQ_PG_DSN`: both split DSNs fall back to it. **Never** point `TASKQ_PG_DSN` at a transaction-mode PgBouncer. See [workers.md: PgBouncer compatibility](workers.md#pgbouncer-compatibility).

### Schema isolation and multi-tenancy

`TASKQ_SCHEMA_NAME` (default `taskq`) isolates all TaskQ tables into a dedicated Postgres schema. Multiple clusters can share one database:

```bash
TASKQ_SCHEMA_NAME=taskq_billing    TASKQ_PG_DSN=postgresql://app:secret@postgres:5432/appdb
TASKQ_SCHEMA_NAME=taskq_notifications TASKQ_PG_DSN=postgresql://app:secret@postgres:5432/appdb
```

Each schema gets its own migration set, NOTIFY channels (derived from a hash of the schema name; see [architecture.md](../architecture.md#notify--wake-mechanism)), and advisory-lock keyspace. Must match `^[A-Za-z_][A-Za-z0-9_]*$` and be at most 63 characters.

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

Some migrations are split into a `pre` and a `post` phase. The `pre` phase adds the new
structures while keeping every structure the *old* release still needs; the `post` phase
removes those old structures. A bare `taskq migrate up` applies **both** phases, right for a
fresh install or a stop-and-replace redeploy, but wrong anywhere a rollout can overlap: a
`post` phase applied while old pods still serve breaks them mid-rollout (dropping the old
single-column idempotency index, for example, turns every enqueue from a pre-upgrade worker
into `InvalidColumnReferenceError`, SQLSTATE 42P10; see the phase-obligation notes in
`01.00.03_01_pre_idempotency_scope.sql`). On a rolling deploy the sequence is:

1. Apply `taskq migrate up --phase pre` as the pre-deploy job or init container, safe
   against old and new code, before or during the rollout.
2. Roll the fleet; wait until every pod runs the new release.
3. A human confirms the rollout completed, then applies `taskq migrate up --phase post`
   once, by hand (or from a manually triggered one-off job). Never wire the `post` phase to
   an init container, a pod lifecycle hook, or any step that fires unsequenced per pod,
   those run mid-rollout, while old pods still need the structures `post` removes.

A worker started against a schema with a pending `pre`-phase migration **refuses to boot**,
naming the missing migrations; a pending `post`-phase migration never blocks boot; that
middle state is the rollout window by design.

**Deployment order:** (1) apply migrations as a pre-deploy job or init container, with
`--phase pre` on anything that rolls, (2) start workers: they call `sync_actor_config` at startup and fail with `ActorConfigDriftList` if a registered actor's **structural** config (`metadata`) differs from the stored row (a differing `queue` literal is the rolling-deploy window of `taskq actor-config move-queue`: it logs `actor-config-queue-override` and boots). That drift check does **not** detect a stale *schema*: it compares config rows, never a schema version, but a stale schema is caught anyway: a worker whose schema has a pending `pre`-phase migration **refuses to boot**, naming the missing migrations (see above), (3) start the admin UI (optionally with `TASKQ_MIGRATE_ON_START=true`), (4) once the rollout is confirmed everywhere, apply `--phase post` as described above.

For rolling deploys where actor config changes, deploy the first pod with `TASKQ_FORCE_UPDATE_ACTOR_CONFIG=true` to overwrite stored config, then deploy the rest without it. See [workers.md: ActorConfig sync](workers.md#actorconfig-sync).

### Migrations under a running fleet: what rides, what must wait

Ops sometimes runs DDL against the tables while workers and admins still hold
pools of cached prepared statements (asyncpg caches prepared statements per
connection). The behaviour under that, measured against PostgreSQL 18.6 with a
warm worker pool:

* **`ADD COLUMN ... NOT NULL DEFAULT` is safe under a live fleet.** Cached
  plans over the old column set stay valid (PG stores the default as a fast
  default: no table rewrite, no plan invalidation), and an old-shape INSERT
  that omits the new column reads the default back invisibly. No error
  reaches the worker.
* **A cached plan that a DDL invalidates inside an open transaction**
  (a result-type change: `ALTER COLUMN TYPE`, a `DROP COLUMN` under
  `SELECT *`) surfaces as `InvalidCachedStatementError`, SQLSTATE 0A000.
  TaskQ classifies the 0A000 family as transient infrastructure: the loop
  retries next tick and the statement re-prepares against the new schema
  (asyncpg has already cleared its pool-wide statement cache by the time the
  error propagates). A log line, a tick, not a restart.
* **The migration's own lock queue is the freeze risk.** `ALTER TABLE` queues
  ACCESS EXCLUSIVE behind any open job transaction, and once queued every
  later statement on that table queues behind IT: one 30s job can freeze the
  whole fleet's dispatch, enqueue, and heartbeat for 30s. The runner bounds
  the wait (`ddl_lock_timeout`, default 30s) and gives up with
  `MigrationLockTimeoutError`, nothing applied; find the holder in
  `pg_stat_activity`/`pg_locks`, end it or let it finish, then re-run
  `taskq migrate up`. Do not raise the bound past your longest job: the bound
  is the fleet's worst freeze window.
* **What must still wait for the contract: `DROP`/`RENAME` of anything the
  old code still references.** Old workers' SQL then fails with
  `UndefinedColumnError` (42703) on every execution, permanently - the text
  itself no longer matches the schema. That failure stays loud and fatal by
  design (42703 is deliberately not transient; classifying it as such would
  livelock a fleet that can never succeed again). Drop old structures only in
  the `post` phase, after the rollout completed.

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

### The admin portal's Redis client requirement

`TASKQ_REDIS_URL` alone does not give the portal live progress: the admin router must receive
the Redis client itself, as `create_router(redis_client=...)`, so `setup_admin_state` puts it on
`app.state` for the progress stream to subscribe with. `taskq ui serve` wires both from the same
setting; a host application mounting the router itself must pass both halves. The two failure
shapes, and the log lines that name them:

- **Redis configured, client not wired** (the misconfiguration): the portal boots with one
  `admin-ui-no-redis-client` warning; live SSE progress is unavailable, the dashboard falls back
  to polling, and the job pages show the `polling mode` badge. Fix: pass the client to
  `create_router`.
- **No TaskQ Redis configured at all** (legitimate polling mode): no warning. The job pages show
  the `polling mode` badge, and the first refused progress stream logs
  `progress-stream-polling-degraded` once, naming the `503 redis_not_configured` answer and the
  polling fallback. Polling pages are first-class: an unchanged poll tick downloads nothing
  (conditional `If-None-Match` on the progress sequence, answered `304`) and writes nothing to
  the DOM; see [the polling UX contract](admin-ui.md#the-polling-ux-contract).

!!! tip "When to provision Redis"
    Provision Redis if you use Redis-backed rate limiters across multiple
    workers, or if operators need real-time admin UI updates. Without Redis,
    rate limiting falls back to Postgres (higher latency, more DB load).

See [rate-limiting.md](rate-limiting.md) and [progress.md](progress.md) for details.

---

## Admin UI in Production

The admin UI (`taskq ui serve`) is a FastAPI + Jinja2 dashboard on `TASKQ_ADMIN_HOST:TASKQ_ADMIN_PORT` (defaults: `0.0.0.0:8080`). It **fails closed by default**: in non-dev environments, `create_router()` raises `RuntimeError` if no `auth_dependency` and `TASKQ_ADMIN_UI_REQUIRE_AUTH=true` (the default).

| Variable | Default | Description |
|---|---|---|
| `TASKQ_ADMIN_UI_REQUIRE_AUTH` | `true` | Raises `RuntimeError` at startup if no `auth_dependency` in non-dev |
| `TASKQ_PROGRESS_REQUIRE_AUTH` | `true` | Raises `RuntimeError` at startup if the progress router has no `auth_dependency` in non-dev (the admin UI forwards its own `auth_dependency`, so this only bites standalone mounts and fully unauthenticated `taskq ui serve`) |
| `TASKQ_ADMIN_ACTIONS_ENABLED` | `false` | When `false`, cancel/retry/run-now return `403` |
| `TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET` | `false` | Gates the rate-limit reset endpoint |
| `TASKQ_HEALTH_TOKEN` | _(none)_ | Bearer token for the health/metrics endpoints `taskq ui serve` exposes; the worker's TCP health listener and scrape endpoint never check it |
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

Set `TASKQ_ADMIN_UI_REQUIRE_AUTH=false`, `TASKQ_PROGRESS_REQUIRE_AUTH=false` and `TASKQ_HEALTH_REQUIRE_TOKEN=false` to suppress the fail-closed checks when relying on the proxy for auth.

Run the admin UI as a **separate process** from the worker: different scaling, exposure, and resource characteristics. See [admin-ui.md](admin-ui.md) for `create_router()` embedding and SSO configuration.

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
        # `--phase pre` is essential to the invariant: the init container runs on every new
        # pod DURING the rolling update, while old pods still serve. A bare
        # `migrate up` would also apply `post`-phase migrations, which remove
        # structures the old release still needs, breaking the old pods'
        # enqueue path mid-rollout. See "Migration strategy" above.
        - name: migrate
          image: myapp:latest
          command: ["taskq", "migrate", "up", "--phase", "pre"]
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

Once the rollout has completed (every pod confirmed on the new release), close out the
phase split with the `post` phase, exactly once, as a deliberate manual step (never an
init container or automated post-deploy hook: those fire unsequenced, mid-rollout):

```shell
kubectl exec deploy/taskq-worker -- taskq migrate up --phase post
```

A pending `post` phase is a safe steady state meanwhile: workers boot and run normally
against it, so there is no urgency that would justify automating the step away. See
[Migration strategy](#migration-strategy) for what each phase guarantees.

See [configuration.md](configuration.md#production-example-env) for the full set of `TASKQ_*` environment variables and cross-field validation constraints.

### Health probes

The worker serves the same two endpoints (`/live` and `/ready`) over two transports:

| Transport | Enabled by | Reachable from |
| --- | --- | --- |
| Unix socket at `TASKQ_HEALTH_SOCKET_PATH` (default `/tmp/taskq_health.sock`) | `TASKQ_HEALTH_ENABLED` (on by default) | inside the container: `taskq health live` / `taskq health ready`, so Kubernetes `exec` probes |
| TCP, `TASKQ_HEALTH_HOST`:`TASKQ_HEALTH_PORT` | setting `TASKQ_HEALTH_PORT` (unset by default) | anything that can reach the container port: `httpGet` and `tcpSocket` probes |

Responses are what orchestrators expect: **200** when healthy, **503** when not, **404** for an
unknown path. Kubernetes and Azure Container Apps both treat 200-399 as probe success, so a 503
reliably fails a probe on either.

`/live` answers whether the event loop is responsive; a failing liveness probe means *restart
me*. `/ready` additionally pings Postgres, checks for stale worker loops, fails during shutdown,
and runs any checks you registered (see below); a failing readiness probe means *stop sending me
work*, not *restart me*.

!!! note "What authenticates where: the token is a `taskq ui serve` feature, not a worker one"
    `TASKQ_HEALTH_TOKEN` protects only the health/metrics routes that `taskq ui serve` exposes;
    the worker's listeners below never check it. What guards each transport instead:

    | Surface | Endpoints | Auth |
    | --- | --- | --- |
    | Unix socket | `/live`, `/ready`, `/metrics`, opt-in `/tasks` | filesystem permissions on the socket (`0600` when `TASKQ_HEALTH_TASKS_ENABLED=true`), so same-pod only |
    | TCP listener (`TASKQ_HEALTH_PORT`) | `/live`, `/ready` | none: keep the port pod-network-only |
    | Scrape listener (`TASKQ_METRICS_PORT`) | `/metrics` | none: keep it pod-network, security-group-scoped, or loopback |

    Both TCP listeners bind nothing until you set their port, so the port setting is the opt-in
    (see [Listener deployment recipes](#listener-deployment-recipes) for the full matrix and the
    fail-closed bind semantics). If your probes or scraper must traverse an untrusted network,
    put the auth in front (proxy, network policy); no header check exists at the endpoint.

!!! warning "A unix-socket collision does not stop the boot: the WARN does"
    If the Unix socket path cannot be bound, almost always a path a still-live peer (or a
    previous, still-draining replica) owns, the worker logs `health-server-unavailable` at
    WARN with the socket path and the errno, and **keeps booting without that listener**: it
    still registers, heartbeats, and claims work, because refusing to boot would crash-loop a
    healthy pair during a rolling restart. The failure is never silent: the WARN is the named,
    alertable record. What the configured address then answers depends on who holds it: a
    live peer's listener answers for the *peer* (its leadership, its shutdown phase, not this
    worker's), and an address nobody holds refuses connections, so treat the WARN as an
    action item: give each replica a unique socket path (or a per-replica directory), because
    until you do this replica's health is not observable at the address you configured.

    When `TASKQ_HEALTH_PORT` **is** set, the collision costs the Unix surface alone: the TCP
    probe listener is bound anyway, so the port your manifest routes probes to keeps answering
    for this replica (the WARN carries `tcp_listener=serving` to say so). The Unix surface
    stays the action item above: unique path per replica, and with no port configured the
    answer is unchanged: warn, boot, no listener anywhere.

!!! danger "A TCP probe port that cannot bind refuses startup"
    The TCP listener is a different contract: the deployment manifest routed health probes
    for THIS replica to `TASKQ_HEALTH_PORT`, and a worker that boots without it answers
    nothing there, or worse, under a `tcpSocket` probe on a port some other process holds,
    the probes pass against the wrong process. When the port cannot be bound the worker logs
    `health-http-bind-failed` (ERROR) and exits with `HealthTcpBindError` instead of running
    with probes silently dead. Give each replica a unique port (a downward-API pod port or
    an orchestrator-managed value), not a shared one.

#### Azure Container Apps

ACA supports **only `httpGet` and `tcpSocket` probes; there is no `exec` probe type**
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

- The probe port does **not** have to be the ingress target port: a worker with no ingress at all
  can still expose 8600 purely for probes. Port values must be integers; named ports are not
  supported.
- Only one probe of each type per container.
- If you enable ingress and define no probes, the portal adds default **TCP** probes against the
  ingress target port. A TCP probe only proves something accepted a connection, so prefer
  `httpGet` against `/ready`: that is the only variant that reflects Postgres reachability and
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
liveness/readiness split, so point it at `/ready` (the endpoint that reflects dependencies) and
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

    The shutdown phases (DRAINING → CANCELLING → FORCING → RELEASING) are
    bounded by `TASKQ_CANCELLATION_GRACE_PERIOD` + `TASKQ_CLEANUP_GRACE_PERIOD`,
    but the bounded-close tail that unwinds *after* them is **additive**, and
    nothing *enforces* that it fits inside `TASKQ_TERMINATION_GRACE_PERIOD`,
    the shipped default does cover it, but a custom value below the worst case
    only warns at startup. Against a dead or hung
    Postgres/Redis: an Azure Cache failover, or a token expiry dropping every
    connection, i.e. exactly when you are being SIGTERMed; each of the 8
    sequential bounded closes (four pools: dispatcher, heartbeat, worker, and
    the conditional per-slot transaction pool; plus `notify_conn`, the
    Redis client, and the two credential-provider closes a managed-identity
    deployment resolves) can take `CLOSE_TIMEOUT_SECS` (5s), plus a 2s
    progress-publish drain: **42s of tail**.

    Size the pod grace from the whole budget:

    ```
    terminationGracePeriodSeconds
        >= TASKQ_CANCELLATION_GRACE_PERIOD
         + TASKQ_CLEANUP_GRACE_PERIOD
         + 42      # 8 bounded closes x 5s + 2s publish drain
    ```

    At TaskQ's defaults (85 / 30 / 10) the modelled worst case is **82s**, so
    `terminationGracePeriodSeconds: 90` is a safe value at defaults, not the
    `60`-ish the old advice implied. (The default grace covers the model with
    3s to spare, but the ~87s sibling-crash path, nine sequential closes
    where the orchestrated leader-conn close never ran, including the
    conditional per-slot pool, now exceeds the default by 2s on per-slot
    workers. Deployments running the per-slot path with tight crash budgets
    should raise `TASKQ_TERMINATION_GRACE_PERIOD` accordingly.) The worker logs
    `shutdown-budget-exceeds-termination-grace` at startup, with the computed
    number, whenever the configured budget does not cover it, i.e. for
    custom grace combinations, not for the shipped defaults.

    If the kubelet SIGKILLs mid-unwind, in-flight `write_cancel_escalation` /
    `mark_interrupted` release writes may not land; those jobs are left `running`
    and recovered later by the leader's crash-reclaim sweep after `lock_lease`
    expires, rather than being released cleanly.

### Listener deployment recipes

The worker has up to three listeners, and every one of them is **off unless you set it**:

| Listener | Enabled by | Serves | Auth |
|---|---|---|---|
| Unix health socket | `TASKQ_HEALTH_ENABLED` (default `true`) | `/live`, `/ready`, `/metrics`, opt-in `/tasks` | filesystem permissions on the socket (`0600` when `TASKQ_HEALTH_TASKS_ENABLED=true`) |
| TCP health listener | `TASKQ_HEALTH_PORT` | `/live`, `/ready` | none; `TASKQ_HEALTH_TOKEN` on `taskq ui serve` does not apply to it, so keep the port pod-network-only |
| Prometheus scrape listener | `TASKQ_METRICS_PORT` (needs `taskq[prometheus]` + `TASKQ_OTEL_AUTOCONFIGURE=true`) | `/metrics` | **unauthenticated** (see [SECURITY.md](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/SECURITY.md)); keep it on the pod network or loopback |

Fail-closed semantics, identical across the platforms below:

- **Both TCP listeners bind nothing until you set a port.** A worker that binds a port nobody asked for is a surprise network surface; setting the port is the opt-in.
- **A TCP health listener that cannot bind refuses startup.** The worker logs `health-http-bind-failed` (ERROR) and exits with `HealthTcpBindError` rather than run with the manifest's probe port dead (a `tcpSocket` probe on a port another process holds would pass while checking nothing). The unix socket's collision is deliberately softer: a collision there means a live peer owns the path, so the boot warns (`health-server-unavailable`) and continues, and because the two transports bind independently, the TCP listener still comes up, so the port the manifest routes probes to keeps answering; only with no `TASKQ_HEALTH_PORT` configured does the collision leave the worker with no listener at all.
- **The scrape listener refuses startup.** If `[prometheus]` or autoconfigure is missing the worker tells you and binds nothing; if the listener itself cannot be configured or bound, `OtelExporterConfigurationError` exits the worker with code 1 rather than run with its scrape silently dead.
- The scrape endpoint answers without a token: keep it on interfaces only your scraper reaches.

#### Kubernetes

`httpGet` probes and annotations-driven scraping, both over the pod network (`TASKQ_HEALTH_HOST` defaults to `0.0.0.0`, which pod-network probers need):

```yaml
metadata:
  annotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "9464"
    prometheus.io/path: /metrics
spec:
  template:
    spec:
      containers:
        - name: worker
          env:
            - name: TASKQ_HEALTH_PORT
              value: "8600"
            - name: TASKQ_METRICS_PORT
              value: "9464"
          ports:
            - name: health
              containerPort: 8600
            - name: metrics
              containerPort: 9464
          livenessProbe:
            httpGet: { path: /live, port: 8600 }
          readinessProbe:
            httpGet: { path: /ready, port: 8600 }
```

Loopback sidecar scraper next to pod-network probers: leave the probes on the pod network and pin the scrape listener to loopback with `TASKQ_METRICS_HOST`, so the unauthenticated endpoint is reachable only inside the pod:

```yaml
            - name: TASKQ_METRICS_PORT
              value: "9464"
            - name: TASKQ_METRICS_HOST
              value: 127.0.0.1
          containers:
            - name: worker
              ...
            - name: prometheus-sidecar
              # scrapes http://127.0.0.1:9464/metrics and remote-writes
```

#### Azure Container Apps

ACA probes support only `httpGet`/`tcpSocket`, so **`TASKQ_HEALTH_PORT` is required there**: the Unix socket is unprobeable and the worker has no other surface a probe can reach.

```yaml
env:
  - name: TASKQ_HEALTH_PORT
    value: "8600"
  - name: TASKQ_METRICS_PORT
    value: "9464"
```

Keep `TASKQ_HEALTH_HOST` at its `0.0.0.0` default: the ACA runtime reaches the replica over the pod network, and a loopback bind would fail every probe. The full probe definitions (liveness, readiness, startup) are in [Health probes](#azure-container-apps) above.

#### AWS EC2

On a plain EC2 host (systemd unit as above) there is no orchestrator probe, so pick the shape that matches who reads the endpoints:

```ini
# /etc/taskq/worker.env
TASKQ_HEALTH_PORT=8600
TASKQ_METRICS_PORT=9464
```

- **Loopback bind + reverse proxy** (the default posture): set `TASKQ_HEALTH_HOST=127.0.0.1` and front the worker with nginx, which terminates the ALB target-group health check on `http://127.0.0.1:8600/ready` and can require-scrape `/metrics`. Nothing outside the host can reach either listener.
- **Security-group-scoped `0.0.0.0`**: leave `TASKQ_HEALTH_HOST` at its default and restrict the security group so only the scraper (or ALB) security group may reach 8600 and 9464. The scrape endpoint is unauthenticated, so the SG is the only thing standing between it and the VPC.

Either way the metrics listener still refuses startup on a bind failure (exit 1), and a health-port collision is a WARN plus failed probes, so give each instance on a shared host its own port.

#### docker / docker-compose

Publish the probe port, keep the health socket on a tmpfs, and let a sidecar scraper in the worker's network namespace see the metrics:

```yaml
services:
  worker:
    image: myapp:latest
    environment:
      TASKQ_HEALTH_SOCKET_PATH: /run/taskq/health.sock   # tmpfs, not the image layer
      TASKQ_HEALTH_PORT: "8600"
      TASKQ_METRICS_PORT: "9464"
      TASKQ_METRICS_HOST: "127.0.0.1"   # scrape listener loopback-only
    tmpfs:
      - /run/taskq:size=1m,mode=0700
    ports:
      - "8600:8600"        # host probes / load balancer health check
    healthcheck:
      test: ["CMD", "taskq", "health", "ready"]
      interval: 10s
      timeout: 5s
      retries: 5
  prometheus:
    image: prom/prometheus
    network_mode: "service:worker"   # shares the worker loopback, scrapes 127.0.0.1:9464/metrics
```

The `healthcheck` uses the Unix socket and needs no published port; the `8600` mapping exists for host-level probes. If your scraper is a plain linked container on the compose bridge network instead of a loopback sidecar, drop `TASKQ_METRICS_HOST` so the scrape listener falls back to `0.0.0.0` and reach it as `worker:9464`, scoped by whatever firewall fronts the bridge.

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
    **plus the ~42s bounded-close tail** (see the terminationGracePeriodSeconds
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

TaskQ instruments itself with OpenTelemetry (vendor-neutral) and structlog. No vendor SDK is bundled; export to any OTLP-compatible backend via standard OTel environment variables. Install the OTel extra: `uv add "taskq-py[otel]"`.

### OTel exporter configuration

| Variable | Example | Description |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://otel-collector:4317` | OTLP gRPC endpoint (`:4318` with `OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`) |
| `OTEL_TRACES_EXPORTER` / `OTEL_METRICS_EXPORTER` | `otlp` | Exporter per signal; defaults to `otlp` once an endpoint is set |
| `OTEL_SERVICE_NAME` | `taskq-worker` | Service name for all spans/metrics |
| `OTEL_RESOURCE_ATTRIBUTES` | `deployment.environment=production,k8s.pod.name=worker-0` | Resource attributes on all telemetry |

With the `[otel]` extra installed, `taskq worker` installs SDK tracer and meter providers from these variables at startup (through the SDK's own configurator, the same as `opentelemetry-instrument taskq worker`) and logs `otel-exporter-configured traces=... metrics=... source=env`. Verify that line in the worker's startup log: `otel-exporter-unavailable` means the variables are set but the extra is missing, and nothing is exported. `TASKQ_OTEL_AUTOCONFIGURE=false` opts out when the embedding application configures the SDK itself. TaskQ does not override standard OTel variables. All spans and metrics use instrumentation name `"taskq"` and OTel messaging semconv attributes for dashboard compatibility.

### Prometheus scrape

The worker series (leader-sampled gauges, dispatch/consume counters, attempt failures, the watchdog family) exist only in the **worker** processes; the admin UI's `/jobs/health/metrics` serves the admin process's own activity and none of them. Scrape every worker pod: install `taskq[prometheus]`, set `TASKQ_METRICS_PORT=9464`, and point a scrape job at each worker on that port with `metrics_path: /metrics` (a pod-role discovery with a `taskq-worker` selector, or a headless Service). The listener binds `TASKQ_METRICS_HOST` when set, falling back to `TASKQ_HEALTH_HOST` (`0.0.0.0`); set `TASKQ_METRICS_HOST=127.0.0.1` for a sidecar scraper that shares the pod's loopback while the health probes stay on the pod network. The port and host writes are scoped to the SDK call and rolled back, so neither leaks to child processes. A worker missing the `[prometheus]` extra binds no listener and says so; one that cannot configure or bind the listener exits 1 rather than run with its scrape silently dead. The endpoint is unauthenticated (see [SECURITY.md](https://github.com/AZX-PBC-OSS/TaskQ/blob/main/SECURITY.md)), so keep it on the pod network or loopback. The worker's health socket additionally serves three process gauges at `GET /metrics` without any extra. See [observability.md: Serving the metrics](observability.md#serving-the-metrics-the-prometheus-endpoint).

### Structured logging

Use `TASKQ_LOG_FORMAT=json` (the default) in production. Every log line includes `worker_id`, `timestamp` (ISO 8601 UTC), `level`, and `trace_id`/`span_id` from the active OTel span. See [observability.md](observability.md) for the full span hierarchy and metrics reference.

---

## Scaling Considerations

### Horizontal scaling

Adding worker processes is the primary scaling lever. Multiple workers against the same database are fully supported: dispatch uses `FOR UPDATE SKIP LOCKED`, so concurrent workers never pick up the same job. Only one worker per schema holds the maintenance leader advisory lock (the lock name is schema-qualified, so each schema in a shared database elects its own leader); if the leader dies, another worker wins the next election. Scale by increasing replica count (`kubectl scale deployment taskq-worker --replicas=10`).

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

See [workers.md: Queue dispatch modes](workers.md#queue-dispatch-modes).

### max_concurrent and max_pending

`max_concurrent` (per-actor via `@actor(max_concurrent=N)`) is a **best-effort** fleet-wide damper on how many jobs for an actor run simultaneously, distinct from `TASKQ_MAX_CONCURRENCY` (total jobs per process).

!!! warning "`max_concurrent` is not a hard cap"
    Dispatch reads its `running` count once per round, before taking row locks, and never rechecks it. Two worker replicas dispatching concurrently each see the same count, each admit up to the cap, and lock *disjoint* rows, so both succeed. The over-dispatch bound is `(num_producers - 1) * max_concurrent` per round, and those jobs genuinely run; reclaiming stale locks does not undo an over-dispatch.

    With `max_concurrent=2` and 3 replicas you can see 6 concurrent executions. For a memory- or GPU-bound actor that is an OOMKill, a restart, and a re-dispatch.

    **If you need a strict cap**, use the per-queue leased-slot reservation instead: `taskq queues set-max-concurrent <queue> --max-concurrent N`. Slots are physical rows and each acquire is a single read-and-write statement on one row, so there is no read-then-decide window. See [rate-limiting.md](rate-limiting.md#queue-level-concurrency-cap). Note the asymmetry with the per-actor damper: `taskq queues set-max-concurrent` writes the `queues` row immediately, but each running worker read the cap into memory at startup, so every worker keeps enforcing the OLD value until it restarts; a cap change lands fleet-wide only after the workers serving that queue have been restarted (a rolling restart is fine, one replica at a time). A per-actor change via `taskq actor-config set` needs no restart: dispatch re-reads `actor_config` every round and picks the new value up on the next cycle. `taskq queues get` shows the database value; it cannot tell you which workers have restarted onto it, so treat the change as live only after the fleet has rolled. `max_pending` (per-actor via `@actor(max_pending=N)`) caps queued `pending` jobs; when exceeded, `enqueue` is rejected and `taskq.backpressure.errors` is incremented with `kind="max_pending"`. Monitor `taskq.queue.depth` (leader samples every 15s) for backlog and `taskq.backpressure.errors` for sustained producer pressure; filter on the `kind` label to the capacity kinds (`max_pending`, `max_pending_lock_timeout`), because the same counter also carries identity-serialization refusals (`unique_for_lock_timeout`, `idempotency_lock_timeout`) that page on contention unrelated to capacity.

### Connection pool sizing

| Pool | Default | Scales with |
|---|---|---|
| `dispatcher_pool` | 4 | `TASKQ_DISPATCHER_POOL_SIZE` |
| `heartbeat_pool` | 4 | `TASKQ_HEARTBEAT_POOL_SIZE` |
| `worker_pool` | `int(max_concurrency * 1.5)` | `TASKQ_MAX_CONCURRENCY` |
| slot pool (conditional) | `max_concurrency + 1` (direct DSN) | `TASKQ_MAX_CONCURRENCY` |
| `notify_conn` + `leader_conn` | 2 (dedicated) | Fixed |

The slot pool exists only when a LOOP-scope `asyncpg.Connection` is registered and `max_concurrency > 1`, the per-slot transaction pool that carries every per-job transaction (the actor's own writes via its injected slot connection, the terminal write, transactional sub-enqueues). Total per worker ≈ `dispatcher + heartbeat + worker_pool + 2`, plus `max_concurrency + 1` direct connections on the per-slot path. For 10 workers at `max_concurrency=16`: ~10 × 34 = 340 connections, or ~10 × 51 = 510 on the per-slot path (each worker adds 17 direct). On that path `TASKQ_MAX_CONCURRENCY` is boot-blocking: the worker opens `max_concurrency + 1` direct connections at startup and fails to boot if it cannot, and a credential-rotation window peaks at `2 × (max_concurrency + 1)` slot connections per worker (old pool draining + new pool warm, bounded by the reload drain timeout), so plan `max_connections` against that peak whenever rotations can coincide across the fleet (a scheduled `TASKQ_RELOAD_INTERVAL` is exactly that), against the steady state only when rotations are staggered. Ensure Postgres `max_connections` accommodates this plus your application's connections. The full budget formula (idle floors, leader extras, client pods, PgBouncer compression) is in [ops.md: Sizing](ops.md#4-sizing-workers-and-postgres-connections).

---

## Security Hardening

### Admin UI authentication

1. **Embed in your FastAPI app** with an `auth_dependency` callable (HTTPBearer, OIDC, session middleware). See [admin-ui.md](admin-ui.md#protecting-the-router-with-fastapi-authentication).
2. **Or run behind a reverse proxy** with auth (nginx basic auth, OAuth2 proxy, mTLS) and set `TASKQ_ADMIN_UI_REQUIRE_AUTH=false`.
3. **Keep `TASKQ_ADMIN_ACTIONS_ENABLED=false`** unless operators need cancel/retry/run-now: these are write operations that modify job state.
4. **Set `TASKQ_HEALTH_TOKEN`** for machine-to-machine auth on the health/metrics endpoints `taskq ui serve` exposes (the worker's own TCP health and scrape listeners do not check it), or explicitly set `TASKQ_HEALTH_REQUIRE_TOKEN=false` if relying on network policy.

### Network policies

| Port | Service | Exposed to |
|---|---|---|
| 5432 | Postgres | Workers, admin UI, migrate jobs only |
| 6379 | Redis | Workers, admin UI only |
| 8080 | Admin UI | Internal operators only; never public |
| 8600 (`TASKQ_HEALTH_PORT`) | Worker TCP health listener (`/live`, `/ready`) | Orchestrator probes / load balancer health checks only (pod network, SG or loopback plus proxy) |
| 9464 (`TASKQ_METRICS_PORT`) | Worker Prometheus scrape listener (`/metrics`, unauthenticated) | The scraper only: pod network, security-group-scoped, or loopback sidecar |
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
