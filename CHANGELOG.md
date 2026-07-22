# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Connection hook points for managed-identity / BYO connections** —
  `WorkerConnections` dataclass with per-role pre-constructed resources
  (caller-owned) or zero-arg async factories (TaskQ-owned) for the worker's
  three PG pools, notify/leader dedicated connections, and Redis client.
  `worker_main(..., connections=...)` and `open_worker_deps(...,
  connections=...)` accept it; fields left `None` fall back to DSN
  construction. `PoolFactory`, `ConnFactory`, `RedisFactory` type aliases
  exported from `taskq` top-level.
- **Vendor-neutral credential provider abstraction** (`taskq.auth`) —
  `PgCredentialProvider` and `RedisCredentialProvider` async Protocols
  with reusable `make_pg_pool_factory`, `make_dedicated_conn_factory`,
  `make_redis_client_factory` builders. Any provider implementing the
  Protocols gets all factory builders for free. The PG factories pass the
  credential to asyncpg as `user=` / `password=` keyword arguments
  (which take precedence over both DSN userinfo and DSN query
  parameters), so the token never appears in the DSN string;
  `enrich_pg_dsn` remains as the string-helper variant (writes the
  credential into DSN userinfo; adds `sslmode=require` only when the DSN
  has no explicit sslmode — `verify-full` is never downgraded). All four
  helpers are exported from the `taskq` top level as well as
  `taskq.auth`.
- **`taskq[aad]` extra** — `taskq.aad` module with Microsoft Entra ID
  providers (`EntraIdProvider`, `EntraIdPgProvider`, `EntraIdRedisProvider`)
  backed by `azure.identity.aio` (the extra includes `aiohttp`, required
  by the async credentials). Providers constructed with `credential=None`
  lazily create one `DefaultAzureCredential` and reuse it; sync
  `azure.identity` credentials are supported and offloaded to a thread.
  See `docs/guides/managed-identities.md`.
- **`taskq[aws]` extra** — `taskq.aws` module with `RdsIamProvider` for
  AWS IAM RDS Postgres authentication, backed by `boto3`.
- **`taskq[vault]` extra** — `taskq.vault` module with
  `VaultDynamicDbProvider` for HashiCorp Vault database secrets engine
  dynamic credentials, backed by `hvac`.
- **`TaskQ` stream hooks** — `pg_conn_factory` and `listen_conn`
  parameters for the LISTEN/NOTIFY transport in `TaskQ.stream()`, so
  pool-only / AAD deployments can stream without a DSN. `stream()` now
  uses `contextlib.aclosing` to ensure the inner generator's `finally`
  (conn close) runs promptly on early return.
- **`migrate.apply_pending_locked` hooks** — `conn` (caller-owned) and
  `conn_factory` (TaskQ-owned) parameters replace the DSN-only path.
- **Credential hot-reload (SIGHUP / interval / programmatic)** —
  hot-swaps every factory-backed PG pool, dedicated connection, and
  Redis client with freshly-built replacements (each factory fetches a
  fresh credential). Triggers: SIGHUP; `TASKQ_RELOAD_INTERVAL`
  (seconds, unset by default) for periodic reloads with no external
  signal — the only rotation path on Windows; and
  `WorkerDeps.request_reload()` / `reload_credentials(deps)` for
  embedders. Each factory call is bounded by
  `TASKQ_RELOAD_FACTORY_TIMEOUT` (default 30 s). The swap is atomic: the
  old pool stops serving new acquisitions immediately and is closed in
  the background with a bounded drain (default 5 s), then terminated —
  an in-flight actor that outlives the drain sees its next acquire fail
  and the job retries on the new pool. DI-injected `db: asyncpg.Pool`
  actors resolve the new pool (LOOP-scope cache refresh) and progress
  flushing follows the swap. A SIGHUP arriving mid-reload (success or
  failure) triggers exactly one follow-up reload; reloads are skipped
  while shutdown is in progress. Each resource reloads independently —
  one factory failure is logged and does not abort the rest; the
  `credentials-reloaded` log line's `failed` field reports any resource
  that didn't rotate. Caller-owned resources are not swapped.
- **NOTIFY listener resilience** — the reconnect loop rebuilds a dropped
  LISTEN connection through the user-supplied `notify_conn_factory` (or
  the DSN closure it was opened with) instead of a stale/absent DSN. A
  caller-owned `notify_conn` that drops disables the listener
  (poll-based dispatch fallback) instead of crashing the worker.
- **Ownership-contract enforcement** — caller-owned pools/connections/
  Redis clients are never closed by TaskQ (including shutdown paths). A
  caller-owned `leader_conn` with no `leader_conn_factory` and no
  `pg_dsn_direct` is a startup `ValueError` (no rebuild path).
  TaskQ-owned dedicated connections (DSN- or factory-built) get TCP
  keepalive.
- `taskq.worker` re-exports `WorkerConnections` and `reload_credentials`
  (lazy, alongside the existing `WorkerDeps` / `open_worker_deps`).
- `ErrorReporter` Protocol for vendor-neutral terminal failure routing (Sentry, Datadog, DLQ) with `NullErrorReporter` default and `taskq.error_reporter.failures` OTel counter
- `retry_classifier` hook on `@actor` for exception-instance-level retry classification (inspect attributes like HTTP status codes, return `RetryOverride` to refine kind/delay per occurrence)
- `RetryOverride` and `RetryClassifierHook` types exported from `taskq` top-level
- `on_success` hook on `@actor` for success callbacks (mirrors `on_retry_exhausted` with timeout guard)
- `start_to_close` per-attempt execution timeout with precedence chain: per-enqueue > `@actor(start_to_close=...)` > `TASKQ_DEFAULT_START_TO_CLOSE` worker fallback
- `KeyedReservationRef` for dynamic per-key (session/tenant) concurrency caps computed from job payload at dispatch time
- `name` and `identity_key` fields on `CronScheduleSpec` for per-property cron schedules and cron↔on-demand dedup
- `JobSortField` enum and `JobFilter.order_by` for "latest run by business key" queries
- `admin_actions_enabled` and `admin_ui_require_auth` security settings for admin UI
- `max_keyed_reservations` setting to guard against unbounded keyed reservation growth
- Consolidated testing guide (`docs/guides/testing.md`)
- **SSO / SAML auth for admin UI**
  - OIDC backend (`taskq[oidc]`): PKCE flow, JWKS validation, signed-cookie sessions
  - SAML backend (`taskq[saml]`): python3-saml, SP metadata, attribute extraction
  - Shared `AuthBundle`/`IdentityClaims` abstraction — both backends use the same
    session handling and group/role allowlist
  - `token_auth()` helper for machine-to-machine bearer-token auth
  - `TASKQ_SSO_BACKEND=none/oidc/saml` CLI integration for standalone `taskq ui serve`
  - Health/metrics endpoints wired into `taskq ui serve` with fail-closed
    `TASKQ_HEALTH_TOKEN`/`TASKQ_HEALTH_REQUIRE_TOKEN` pattern
  - `OIDCSettings`/`SAMLSettings` as separate DotEnvConfig classes with prefix scoping

### Fixed

- SQL injection in `batch.py` `BatchHandle.status()` and `wait_for_batch()` — `schema` parameter now validated against `_IDENT_RE` before SQL interpolation
- Fire-and-forget progress publish — `ctx.progress()` no longer blocks the actor on a synchronous Redis round-trip; publishes via background tasks with drain-on-shutdown
- Stale `[web]` extra references in README and CI — replaced with `[fastapi]`
- `ErrorReporter.report()` now has a timeout guard (`error_reporter_timeout`, default 3s) matching `on_retry_exhausted` convention
- `ErrorReporter.report()` argument order aligned with `OnRetryExhausted`: `(job, exception)` not `(error, job)`
- `retry_classifier` hook return value validated — non-`RetryOverride` returns caught and logged, not crash
- `retry_classifier` hook skipped for `non_retryable_exceptions` and `PayloadValidationError` — matches documented contract
- `on_retry_exhausted` now uses `inspect.isawaitable()` instead of `inspect.iscoroutine()` — handles non-coroutine Awaitables
- Rate-limit `refund()` for memory and Postgres log-style sliding window — was silent no-op, now properly frees slots
- Token-bucket `refund()` on Postgres backend — was silent no-op, now properly refunds tokens (capped at capacity) via `FOR UPDATE` on `rate_limit_buckets`
- `_di/solver.py` debug log now reports real `cache_hit` value instead of hardcoded `False`
- `worker/_leader_sweeps.py` logs warning on invalid schema and includes error detail in exception handlers
- `testing/pg.py` validates schema against `_IDENT_RE` before SQL interpolation
- `worker/notify.py` logs debug on NOTIFY payload parse failures
- Admin UI "run schedule now" endpoint checks `enabled` flag and has cooldown rate limiting
- Admin UI cron payload_factory error redirect uses generic error code instead of reflecting exception text
- Admin UI fails closed by default in non-dev environments when no `auth_dependency` is configured
- `humanize` moved from core to `[fastapi]` extra (was bloating core install)
- `starlette` and `prometheus_client` declared as direct dependencies (were transitive-reliance)
- Dependency upper bounds added to `asyncpg`, `redis`, `pydantic`, `fastapi`, `typer`, `dotenvmodel`, `uuid-utils`, `uvicorn`, `structlog`, `opentelemetry-instrumentation`, `prometheus-client`
- Worker exception handlers no longer swallow failure diagnostics. Timeout
  and generic-exception attempts log `job_timeout` / `job_exception`
  WARNING events carrying `error_class` / `error_message` /
  `error_traceback`; every terminal (non-retryable) failure across all
  five handlers emits exactly one `job_failed` ERROR event (`job_id`,
  `actor`, `attempt`, `cause`, `error_class`, plus handler context such as
  `snooze_count` / `consume_budget` / `bucket_name`) — one alertable event
  per dead job, and per-attempt diagnostics at WARNING so retryable
  attempts produce zero ERROR noise. Tracebacks are formatted from the
  explicit exception object rather than the ambient `sys.exception()`, so
  handler invocations outside an `except` block no longer record
  `'NoneType: None'`. The `terminal-write-failed` event now includes
  `job_error_traceback` and `infra_error_traceback`. Timeout spans
  (`lifecycle.scheduled` / `lifecycle.failed`) now report the concrete
  exception class instead of hardcoded `TimeoutError`, agreeing with the
  log fields. Snooze / RetryAfter / ReservationUnavailable terminal
  outcomes and the stranded-jobs leader sweep also log their failure
  details instead of continuing silently.

### Changed

- **dotenvmodel bumped 0.3.0 → 0.5.0.** `WorkerSettings` now uses dotenvmodel's native `post_load()` hook (added in 0.5.0) instead of a manual `_post_load` method called from `load()`/`load_from_dict()` overrides. The base `DotEnvConfig._load_fields` invokes `post_load` automatically on every load path — `load()`, `load_from_dict()`, and `reload()` — including under `validate=False`. The redundant `WorkerSettings.load`/`load_from_dict` overrides have been removed.
- **Breaking: cross-field invariant exceptions changed type.** `WorkerSettings.load()`/`load_from_dict()` cross-field invariants (`lock_lease >= 4 * heartbeat_interval`, grace-budget checks) previously raised `ValueError`; they now raise `ValidationError` (single failure) or `MultipleValidationErrors` (several at once). `ConstraintViolationError` (field validators) was already not a `ValueError`. **Callers that catch `ValueError` around `WorkerSettings.load*()` will no longer catch these** — catch `DotEnvModelError` (the common base) to cover both single and aggregate cases, or `ValidationError` when at most one invariant can fire. Field-level validation (`prune_retention_*`, `default_start_to_close`, `log_format`, etc.) already raised `ConstraintViolationError` and is unaffected.
- **`reload()` now enforces cross-field invariants and applies DSN fallback.** Previously `reload()` did not run `_post_load` (it was only called from the `load()`/`load_from_dict()` overrides), so a reload that produced invariant-violating values would silently succeed. This is now fixed by the native `post_load` hook.
- **`log_format` validation moved from `choices=` to a `validator` hook.** `choices=` is a built-in constraint that `load_from_dict(..., validate=False)` skips, so an invalid `TASKQ_LOG_FORMAT` could previously load silently under `validate=False`. The validator hook runs regardless of `validate=`, closing the hole. Error message changed from `log_format must be 'json' or 'console'` to `log_format must be one of ['console', 'json'], got <value>`.
- **Breaking: structured-log field rename in sub-enqueue failure events.**
  `sub_enqueue_re_enqueue_error` and `sub_enqueue_flush_error` now carry
  `error_class` + `error_message` instead of the single `message` field,
  matching the `error_class`/`error_message` convention used by every
  other error event (`job_timeout`, `job_exception`, `job_failed`,
  `rate_limit_release_failed`, `savepoint_rollback_failed`,
  `stranded_jobs_query_failed`, and the `failed_details` payload of
  `sub_enqueue_flush_failed`). Log pipelines querying `fields.message`
  on these two events must switch to `error_message`.

### Security

- SQL injection in `batch.py` public API (`BatchHandle.status()`, `wait_for_batch()`) — schema parameter was interpolated without validation
- Admin UI unauthenticated business-flow trigger — `POST /schedules/{id}/run` now requires `admin_actions_enabled=True` and has cooldown rate limiting
- Admin UI fail-closed defaults: `admin_ui_require_auth=True` raises `RuntimeError` in non-dev when no `auth_dependency`; `health_require_token=True` raises `RuntimeError` in non-dev when `health_token` is empty. Both have explicit opt-out env vars (`TASKQ_ADMIN_UI_REQUIRE_AUTH=false`, `TASKQ_HEALTH_REQUIRE_TOKEN=false`).
- Admin UI destructive actions (run-schedule, retry-job, cancel-job) gated behind `admin_actions_enabled` (default False). Run-schedule has per-process cooldown.

## 0.1.0 - 2026-07-08

### Added

- **Core Job System**
  - `@actor` decorator with typed `ActorRef` references
  - `TaskQ` facade for enqueueing and managing jobs
  - `JobsClient` for job queries, cancellation, and inspection
  - `JobHandle` for awaiting individual job results
  - Batch enqueue with `wait_for_batch` and `BatchHandle`

- **Worker System**
  - Multi-queue worker with configurable concurrency
  - Leader election for singleton job dispatch
  - Graceful shutdown with drain semantics
  - Heartbeat-based lease management
  - Workgroup orchestration for multi-replica deployments

- **Rate Limiting**
  - Sliding window (GCRA) algorithm
  - Token bucket algorithm
  - Composable rate limit groups
  - PostgreSQL and Redis backends

- **Scheduling**
  - Cron-based recurring schedules via `cron()`
  - Delayed job execution

- **Reliability**
  - Configurable retry policies with exponential backoff
  - Job cancellation with phase tracking
  - Idempotency keys and identity-based deduplication
  - Max pending and backpressure controls

- **Observability**
  - Vendor-neutral OpenTelemetry integration
  - Structured logging via structlog
  - Prometheus metrics exporter (optional extra)

- **Admin UI**
  - FastAPI-based web dashboard with htmx
  - Real-time SSE updates
  - Job inspection, queue management, worker monitoring

- **Progress Tracking**
  - Progress event streaming
  - Optional Redis fanout for real-time updates

- **Dependency Injection**
  - Scoped DI container with provider registry
  - Singleton and request scopes

- **Developer Experience**
  - `taskq` CLI (Typer) for migrations, health checks, admin UI, and workgroup management
  - Forward-only SQL migration runner
  - `taskq.testing` module with in-memory backend, fixtures, and assertions
  - Full type safety with py.typed marker

### Changed

- N/A (initial release)

### Security

- No known security issues
