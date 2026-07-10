# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
