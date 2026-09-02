# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed (breaking)

* **`KeyedRateLimitRef` and `KeyedReservationRef`: `payload_type` is now required and `key_fn` receives the validated Pydantic model, not the raw dict.** Every existing keyed-ref declaration must be updated:

  ```python
  # BEFORE (broken):
  KeyedRateLimitRef(
      base_name="api-per-tenant", key_fn=lambda p: p["tenant_id"], capacity=10, refill_per_second=1.0
  )

  # AFTER:
  KeyedRateLimitRef.typed(
      MyPayload,
      base_name="api-per-tenant",
      key_fn=lambda p: p.tenant_id,
      capacity=10,
      refill_per_second=1.0,
  )
  ```

  Use `.typed()` for compile-time type checking of `key_fn` against the payload model.

* **Dispatch-path malformed payloads now fail immediately as `PayloadValidationError` (non-retryable)** instead of being retried as a generic `ValidationError`. In-flight legacy rows with invalid payloads will fail on first dispatch instead of exhausting the retry budget.

* **Per-key budget reset hazard during deploys:** if defaults or validators change a key-deriving field's value vs the raw row, new concrete names materialize fresh full-capacity buckets alongside old ones (temporary over-admission window). Drain affected queues before deploying payload model changes that affect key derivation.

* **`taskq.validate_actor_payload` now resolves to the sanitized implementation, and its third parameter's keyword name is `actor`, not `actor_name`.** The public `taskq.validate_actor_payload` export previously resolved to a duplicate in `taskq.exceptions` that embedded the raw payload in its message (`Raw payload: {...}`) and attached pydantic errors with `include_input`. That message is persisted to the job row's `error_message` and rendered in the web admin, so attacker-controlled payload values could leak into both — this is an information-disclosure fix, not a refactor. The one remaining implementation lives in `taskq._validation` (`include_url=False`, `include_input=False`, no raw-payload embedding); `taskq.exceptions.validate_actor_payload` re-exports it lazily. **Call-site break:** the keyword `actor_name=` is now `actor=` (and is now optional, defaulting to `None`); the third *positional* argument is unaffected. Two further observable changes: the raised message is now `Payload validation failed for actor '<name>': <model title>` with no pydantic detail and no payload dump, and `validation_errors` entries no longer carry `input` or `url` keys — anything parsing `error_message` or reading those keys must be updated. The `raw_payload` parameter also now accepts an existing `BaseModel` as well as a `dict`. See [docs/guides/upgrading.md](docs/guides/upgrading.md).

* **Payload validation now runs before rate-limit acquisition, and `acquire_for_actor` receives the validated model instead of the raw row dict.** On the dispatch path nothing changes: `dispatch_one_job` already validates and passes `validated_payload`, so workers behave identically. The change is visible only to **direct callers of `consume_one_job`** that pass `validated_payload=None`. For those, an invalid payload now raises `PayloadValidationError` *before* any token is acquired; previously the consumer acquired first and burned a **non-refunded** token (`release_for_actor` sets `refund_on_release=False`) for an actor body that could never run. The error escapes to the caller, which owns the terminal write — as `dispatch_one_job`'s outer handler and the in-memory test runner already do. This also fixes a real defect in keyed refs: the registry re-validated the *raw* dict against the ref's `payload_type`, dropping the actor model's defaults, so an actor defaulting `tenant_id="unattributed"` against a ref model requiring `tenant_id` failed the job non-retryably on a payload that was perfectly valid for the actor. Same-model refs now hit the registry's `isinstance` fast path; a stricter cross-model ref re-validates the model's `model_dump()`, which carries the actor model's applied defaults and aliases.

* **`WorkerSettings` now rejects at load time several values it previously accepted.** A deployment whose configuration contains any of these stops starting on upgrade, with a settings-load error rather than the opaque mid-startup failure it used to produce. Audit before rolling out: `schema_name` longer than 63 characters (Postgres' `NAMEDATALEN` silently truncated it, while Redis channel templates interpolated the full string — the two stores quietly diverged); `workgroup_instance` that is not a valid UUID (previously a raw `ValueError` mid-registration); `worker_label` containing a NUL (previously an opaque asyncpg `22021 CharacterNotInRepertoireError` at startup); and any item of `queues` that does not match the canonical queue-name charset (letters, digits, `_`, `.`, `-`, first character a letter or `_`). Two new cross-field watchdog invariants also apply, but **only when `watchdog_enabled=True`**: `watchdog_loop_lag_budget + heartbeat_interval` must be `< lock_lease` (a stalled loop must die before its leases expire, or the leader sweep reclaims live jobs' locks mid-stall), and `watchdog_loop_lag_budget` must be `> watchdog_check_interval` (a budget at or below the sampling period trips on a healthy idle loop — measured: a 1.0 budget against the 1.0 s default check interval force-exits an idle worker on its first armed poll). These raise `ValidationError` / `MultipleValidationErrors`, not `ValueError` — see the dotenvmodel note below.

* **Breaking: the `state_change` structured-log event is now `state-change`.** This event shipped in v0.2.2, so log pipelines, saved searches, and alert rules matching `state_change` will silently stop matching. It is emitted from every backend that logs a job state transition, so one name now covers the Postgres and in-memory paths alike. (Three other event names were kebab-cased in the same pass — `batch_streaming_enqueued`, `pg_credential_refresh_failed`, `cancel_where_notify_failed` — but none of them ever shipped in a release, so no consumer can be matching on the old spellings.)


### Added

* `JobsClient.cancel_where(filter, reason)` — bulk cancel all jobs matching a
  `JobFilter` in a single set-based operation. Pending/scheduled jobs go straight
  to terminal `cancelled`; running jobs get cooperative cancel (`cancel_phase=1`).
  Returns `BulkCancelResult` with counts and affected IDs. Empty filters are
  rejected with `EmptyFilterError` unless `allow_empty_filter=True` is passed.
* `SubJobEnqueuer.enqueue()` now accepts `tags`, `inherit_tags`,
  `schedule_to_close`, `start_to_close`, and `heartbeat_timeout` parameters.
  Sub-jobs inherit the parent job's tags by default (`inherit_tags=True`); pass
  `inherit_tags=False` to suppress inheritance for a specific sub-job.
* `BulkCancelResult` and `EmptyFilterError` exported from `taskq` top-level.


- **Batch failure policies (`AbortBatchAfter`)** — #55. An opt-in
  `failure_policy` parameter on `enqueue_batch()` /
  `enqueue_batch_streaming()` creates a `batches` row and drives
  abort-on-consecutive-failure semantics via the
  `apply_batch_terminal_outcome` hook. When the threshold is reached the
  batch is aborted: pending/scheduled child jobs are cancelled and the
  batch row is set to `aborted`.
- **Batch finalizer (transactional enqueue with batch)** — #58. A
  `finalizer` parameter on `enqueue_batch()` /
  `enqueue_batch_streaming()` enqueues a finalizer job alongside the
  batch in the same transaction. The finalizer is NOT stamped with
  `batch_id` (deadlock prevention); `wait_for_batch` automatically
  excludes it from counts via the batch row's `finalizer_job_id`.
- **Batch discovery (`list_batches`, `BatchSummary`)** — #59.
  `JobsClient.list_batches(BatchFilter)` returns `BatchSummary` objects
  with live job-count aggregates. `BatchFilter` carries only
  batch-relevant fields (`queue`, `active`, `batch_id`, `limit`).
- **`enqueue_batch_streaming` for unbounded iterables** — accepts an
  `Iterable[EnqueueItem]` (including generators) and inserts in chunks
  of `chunk_size` (1–1000). All items share the same `batch_id`.
- **`wait_for_batch` with `expect_at_least`, `on_empty`,
  `exclude_job_id`** — `expect_at_least` raises `EmptyBatchError` when
  fewer than the expected number of jobs are present; `on_empty`
  controls behaviour when zero jobs and no `batches` row exist
  (`"error"` raises, `"ok"` returns empty status); `exclude_job_id`
  omits a specific job from counts (defaults to the batch row's
  `finalizer_job_id`).
- **Backend protocol batch methods (10 new methods)** —
  `enqueue_batch_atomic`, `create_batch`, `increment_batch_failures`,
  `reset_batch_failures`, `abort_batch`, `complete_batch`, `get_batch`,
  `list_batches`, `count_batch_non_terminal`, `prune_old_batches`.
- **Batches table migration (01.00.05_01)** — adds the `batches` table
  with columns for status tracking, failure counters, finalizer linkage,
  and batch-level metadata.
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


### Changed

* **Sub-jobs now inherit parent tags by default.** Every `ctx.jobs.enqueue()`
  call inside an actor body now propagates the parent job's tags to the sub-job,
  making sub-jobs findable by `JobFilter(tags=...)` and cancellable by
  `cancel_where`. Pass `inherit_tags=False` per-call to opt out. This is a
  behavior change for any code that relied on sub-job tags being empty —
  inherited tags make sub-jobs visible to tag-based filters and bulk cancels.


- **dotenvmodel bumped 0.3.0 → 0.5.0.** `WorkerSettings` now uses dotenvmodel's native `post_load()` hook (added in 0.5.0) instead of a manual `_post_load` method called from `load()`/`load_from_dict()` overrides. The base `DotEnvConfig._load_fields` invokes `post_load` automatically on every load path — `load()`, `load_from_dict()`, and `reload()` — including under `validate=False`. The redundant `WorkerSettings.load`/`load_from_dict` overrides have been removed.
- **Breaking: cross-field invariant exceptions changed type.** `WorkerSettings.load()`/`load_from_dict()` cross-field invariants (`lock_lease >= 4 * heartbeat_interval`, grace-budget checks) previously raised `ValueError`; they now raise `ValidationError` (single failure) or `MultipleValidationErrors` (several at once). `ConstraintViolationError` (field validators) was already not a `ValueError`. **Callers that catch `ValueError` around `WorkerSettings.load*()` will no longer catch these** — catch `DotEnvModelError` (the common base) to cover both single and aggregate cases, or `ValidationError` when at most one invariant can fire. Field-level validation (`prune_retention_*`, `default_start_to_close`, `log_format`, etc.) already raised `ConstraintViolationError` and is unaffected.
- **`reload()` now enforces cross-field invariants and applies DSN fallback.** Previously `reload()` did not run `_post_load` (it was only called from the `load()`/`load_from_dict()` overrides), so a reload that produced invariant-violating values would silently succeed. This is now fixed by the native `post_load` hook.
- **`log_format` validation moved from `choices=` to a `validator` hook.** `choices=` is a built-in constraint that `load_from_dict(..., validate=False)` skips, so an invalid `TASKQ_LOG_FORMAT` could previously load silently under `validate=False`. The validator hook runs regardless of `validate=`, closing the hole. Error message changed from `log_format must be 'json' or 'console'` to `log_format must be one of ['console', 'json'], got <value>`.
- **Breaking: `wait_for_batch` default `on_empty="error"` raises
  `EmptyBatchError` instead of silent return.** Previously, calling
  `wait_for_batch` on a batch_id with zero jobs and no `batches` row
  returned an empty `BatchCompletionStatus` silently. The default is now
  `on_empty="error"`, which raises `EmptyBatchError`. Pass
  `on_empty="ok"` to preserve the old silent-return behaviour.
- **Breaking: structured-log field rename in sub-enqueue failure events.**
  `sub_enqueue_re_enqueue_error` and `sub_enqueue_flush_error` now carry
  `error_class` + `error_message` instead of the single `message` field,
  matching the `error_class`/`error_message` convention used by every
  other error event (`job_timeout`, `job_exception`, `job_failed`,
  `rate_limit_release_failed`, `savepoint_rollback_failed`,
  `stranded_jobs_query_failed`, and the `failed_details` payload of
  `sub_enqueue_flush_failed`). Log pipelines querying `fields.message`
  on these two events must switch to `error_message`.
- **Breaking: `taskq.worker.actor_config` moved to `taskq.actor_config`.**
  The `ActorConfig` dataclass (released in v0.2.0–v0.2.2 at
  `taskq.worker.actor_config`) has moved to the top-level
  `taskq.actor_config` module. It is shared by the client, CLI, and admin
  UI, not worker-internal. The old import path raises `ImportError`. See
  [docs/guides/upgrading.md](docs/guides/upgrading.md) for the full
  migration mapping. The companion `actor_config_ops` module (listing,
  inspecting, tuning, and deregistering actors) has likewise moved from
  `taskq.worker.actor_config_ops` to `taskq.actor_config_ops`; it was
  never released under the `worker.*` path.
- **Breaking: dotenvmodel bumped to 1.x (`>=1.1.0,<2`), adopting its 1.0 defaults.** Environment-variable precedence flips: the process environment now beats `.env` files by default (previously `.env` values overwrote `os.environ`); restore the files-beat-env-vars behaviour with `DOTENV_OVERRIDE=true` or `TaskQSettings.load(override=True)`. `load()` no longer mutates `os.environ` — read `TASKQ_*` values from the settings instance, not the process environment, after a load. `TaskQSettings.load()` now forwards dotenvmodel's full parameter surface (`env`, `override`, `env_dir`, `read_dotfiles`, `read_environ`, `load_local`). Subclass string-field defaults containing `${VAR}` references are interpolated at load time (unset references resolve to `""`).
- **Breaking: time is unified on the database clock — the enqueue and rate-limit surfaces changed shape.** `EnqueueArgs.scheduled_at` is now nullable: "immediate" enqueue passes `None` and the server stamps it (no more client-side `now()` default), and `Backend` implementations that require a non-`None` datetime fail loudly. The raw `schedule_to_close` datetime form is deprecated in favour of `schedule_to_close_interval` (or declaring `retry.time_budget` on the actor — absolute datetimes cross clock domains and can misbehave under skew); every enqueue arm writes the deadline from one domain (server clock + interval). The rate-limit Redis Lua scripts derive `now` from `redis.call('TIME')` — the caller-supplied `now` ARGV is removed.
- **Every mixed-clock decision is now single-arbiter on the store's clock.** The application process and the database server keep separate clocks that can diverge or step (VM pause/resume, NTP drift); every place that mixed the two domains in one decision is anchored to the database clock: workgroup supervisor freshness is computed server-side (a skewed supervisor host can no longer kill healthy children); cron ticks read the server clock inside the leader transaction, with the catch-up cutoff and beyond-window recompute server-anchored (no fire-loops or silently skipped backlog under leader-clock skew); rate limiting runs on the store's clock (PG window predicates and GCRA/token-bucket epoch math are server-side; peeks measure against the store clock too); prune/archive cutoffs and enqueue-pinned result TTLs are stamped server-side; the batch COPY path is server-stamped via an in-transaction fixup (`status`, `created_at`, `scheduled_at`, `schedule_to_close`, `result_expires_at`), so dedup windows hold under skew.
- **`taskq[oidc]` no longer installs `httpx`; its `authlib` floor is now `>=1.8.0`.** authlib 1.8.0's `httpx_client` integration is httpx2-first (httpx is only a deprecated fallback), the direct OIDC calls (discovery, JWKS fetch) use `httpx2`, and nothing under `src/taskq` imports `httpx` — the extra's `httpx` entry was redundant (authlib 1.7.x, which imported `httpx` unconditionally, is excluded by the new floor).
- **`SubJobEnqueuer.enqueue_batch([])` now raises `ValueError` instead of returning `[]`.** Without a connection the fallback loop iterated zero items and returned an empty list silently, while `JobsClient.enqueue_batch` already raised pre-I/O and the streaming path raised on the first peek. An empty fan-out is now an error at every layer — guard the call site if your item list can legitimately be empty.
- **`taskq queues set-max-concurrent --max-concurrent` now requires `>= 1`; `0` is rejected.** The typer option's minimum moved from `0` to `1`, and the underlying `taskq.worker.queue_ops.set_queue_max_concurrent` now raises `ValueError` below `1` (previously below `0`). `0` was accepted by both layers and then died on the table's `CHECK (max_concurrent IS NULL OR max_concurrent >= 1)`, handing the operator a raw asyncpg `CheckViolationError` traceback — so this trades a crash for a clean error, but it is still a contract change for scripted callers. `NULL` (via `--clear`) remains the uncapped state; an emergency drain to `0` belongs to the per-actor `actor-config set --max-concurrent 0`, which still allows it.
- **`BatchFilter.limit` is now capped at 500 and raises `ValueError` above it.** `list_batches` renders a per-batch `LATERAL` job-count join per returned row and has no cursor pagination, so an unbounded limit scanned the whole `batches` table. The lower bound stays `>= 0` (`limit=0` remains "no rows") and the default is unchanged at 100. The cap is a validation error rather than a silent clamp, so a caller never receives a different page than the one it asked for.
- **Admin job-list filter inputs are now bounded and return 400 past their caps.** On `/jobs` (and `/jobs/count`, `/history`) the `status` list is deduplicated in first-occurrence order (values outside the closed status set are still a 400). The `/jobs` `tags` filter is stripped and deduplicated, rejected with a 400 above 16 items, and rejected with a 400 for any item longer than 255 characters (the enqueue-side tag length limit, so a longer filter term could never match a stored tag anyway). Deduplicated, otherwise-valid requests are unchanged; a client that previously sent repeated statuses or an oversized tag list now gets a 400 where it used to get a 200.
- **Queue names are now validated at the enqueue and actor-declaration chokepoints.** `JobsClient.enqueue()` (per-call `queue=` override and the actor-declared default alike) and `@actor(queue=...)` now run the backend's canonical `_validate_queue_name`, raising `ValueError` — at decoration time, which is import time in the common case. The `QueueName` annotation is inert at runtime (its `AfterValidator` only fires inside pydantic model validation), so a typo'd queue name previously sailed through and stranded every job on a queue no worker's `queue = ANY($1)` ever matched. **A typo that used to fail silently now fails loudly at import.**
- **The queue-name, tag, and keyed-key regexes are re-anchored `\A...\Z` instead of `^...$`.** Python's `$` also matches immediately before a trailing newline, so `"default\n"`, `"mytag\n"`, and `"key\n"` all satisfied the old patterns. Such values are now rejected. If any queue name, tag, or keyed rate-limit key in your system has a trailing newline — most plausibly from a shell `$(...)`, a file read, or an unstripped environment variable — it will start raising `ValueError`.
- **NUL bytes are rejected in caller-supplied text instead of surfacing as a database error.** `JobFilter` (`queue`, `actor`, `identity_key`, `tags`) and `ScheduleCreateArgs` (`actor`, `name`, `timezone`, `payload_factory`, `identity_key`) now raise `ValueError` in `__post_init__`; the admin UI's text filters (`actor`, `queue`, `search`, `identity_key`, `fairness_key`, `tags`) now return a clean 400. Previously these reached Postgres and came back as an opaque asyncpg `22021` — a 500 from the admin routes.
- **`ScheduleCreateArgs.dst_strategy` is now validated in `__post_init__`** and raises `ValueError` for a value outside the known set, which is newly exported as `taskq.cron.DST_STRATEGIES`. An unrecognized strategy previously constructed fine and took the default branch at cron-tick time.


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
- `TaskQ(redis_url=...)` validation: the URL routes through `load_from_dict`, so the `RedisDsn` field type coerces and validates it — an invalid URL now raises `TypeCoercionError` fail-fast at `open()` (previously a late `ValueError` from redis-py), and an empty or whitespace-only `redis_url` raises `ValueError` at construction instead of silently disabling Redis.
- The `.env`-not-found warning suppression is narrowed to exactly that one warning — a `logging.Filter` matched on message prefix, instead of raising the whole `dotenvmodel` logger to ERROR — so real misconfiguration warnings (e.g. an invalid `DOTENV_*` value) stay visible.
- Docs corrected: `configuration.md` claimed `TASKQ_ENVIRONMENT` selects `.env.{env}` files — `ENV` does; `TASKQ_ENVIRONMENT` is a deployment label that gates the unauthenticated-admin warning.
- Rate-limit `refund()` credited the *configured* store rather than the store
  that actually paid. With `backend="redis"` and `rate_limit_pg_fallback_enabled`
  (the default), an acquire during a Redis outage falls through to Postgres and
  consumes the token there, but `TokenBucket.refund` and `SlidingWindow.refund`
  both dispatched on the primitive's static `self._backend`, so the refund went
  to Redis. One failed job therefore cost twice: Postgres, which paid, was never
  repaid — and for a fixed-quota bucket (`refill_per_second == 0`) nothing ever
  puts that token back, so the quota is permanently smaller — while Redis was
  credited a token it never spent. Silently, because by refund time the outage is
  usually over. Both primitives now dispatch on `decision.backend`, the store the
  acquire actually used. For the GCRA sliding-window style, whose
  `previous_state` shape differs per backend, the mismatch also raised a
  `KeyError` straight out of the release path. `ConcurrencyReservation` was
  checked and does not have this shape — it has no Redis path at all.
  Deployments running Redis rate limits with the Postgres fallback enabled
  should expect quota accounting to change (correct itself) after upgrading.


### Security

- SQL injection in `batch.py` public API (`BatchHandle.status()`, `wait_for_batch()`) — schema parameter was interpolated without validation
- Admin UI unauthenticated business-flow trigger — `POST /schedules/{id}/run` now requires `admin_actions_enabled=True` and has cooldown rate limiting
- Admin UI fail-closed defaults: `admin_ui_require_auth=True` raises `RuntimeError` in non-dev when no `auth_dependency`; `health_require_token=True` raises `RuntimeError` in non-dev when `health_token` is empty. Both have explicit opt-out env vars (`TASKQ_ADMIN_UI_REQUIRE_AUTH=false`, `TASKQ_HEALTH_REQUIRE_TOKEN=false`).
- Admin UI destructive actions (run-schedule, retry-job, cancel-job) gated behind `admin_actions_enabled` (default False). Run-schedule has per-process cooldown.
- Keyed rate-limit `key_fn` errors no longer embed the payload. The `RateLimitRegistry` "key_fn returned an empty key" `ValueError` interpolated the whole payload (`for payload {payload!r}`); that exception propagates into the persisted `error_message` and the web admin through generic exception handling, and payload values are attacker-controlled. The message now names only the ref, matching the sanitization contract `PayloadValidationError` follows in `taskq._validation`.


### Internal

- Test containers are shared singletons: one Postgres and one Dragonfly container per pytest invocation, shared across all xdist workers (filelock refcount, stale-leftover sweep) with per-module database and per-test schema isolation preserved — full suite ~152 s vs the ~226–240 s baseline.
- Docker/testcontainers calls in tests run off the event loop (`asyncio.to_thread`) — docker-py's blocking HTTP round-trips no longer stall the event loop mid-test.
- Behavioral timing tests assert in a single clock domain (one statement reads the server clock and the row together), so application/database clock divergence cannot corrupt an assertion; liveness freshness is bounded by the missed-at-most-one-tick contract.


## [0.2.2](https://github.com/AZX-PBC-OSS/TaskQ/compare/v0.2.1...v0.2.2) (2026-07-22)


### Continuous Integration

* local self-contained publish workflow with attestations off ([#15](https://github.com/AZX-PBC-OSS/TaskQ/issues/15)) ([07fcfce](https://github.com/AZX-PBC-OSS/TaskQ/commit/07fcfced7d4cf8de26859d24b9282ba16a0a25f8))

## [0.2.1](https://github.com/AZX-PBC-OSS/TaskQ/compare/v0.2.0...v0.2.1) (2026-07-22)


### Continuous Integration

* fix reusable-workflow publish — conditional attestations, manual republish dispatch ([#12](https://github.com/AZX-PBC-OSS/TaskQ/issues/12)) ([8798fdd](https://github.com/AZX-PBC-OSS/TaskQ/commit/8798fddb7b6005b15879c0121055726aa465d26d))

## [0.2.0](https://github.com/AZX-PBC-OSS/TaskQ/compare/v0.1.0...v0.2.0) (2026-07-22)


### Features

* managed-identity connections, credential hot-reload, BYO pools ([df9d7c3](https://github.com/AZX-PBC-OSS/TaskQ/commit/df9d7c35ad00f267a6cffc0460a0a0a2cd0ec922))
* managed-identity connections, credential hot-reload, BYO pools ([a754fd7](https://github.com/AZX-PBC-OSS/TaskQ/commit/a754fd730e21f939b2ad2e6e1acd0ebca78c1eb5))


### Bug Fixes

* handle ENOTSOCK in stale socket cleanup, add session backstop fixture ([dc87254](https://github.com/AZX-PBC-OSS/TaskQ/commit/dc8725424fe1c788234d51e9d73dc38b0b92facf))
* log traceback on generic job exceptions ([63d18ca](https://github.com/AZX-PBC-OSS/TaskQ/commit/63d18caafffbcfc9b7fd390cde16a8b2f083b701))
* PR review correctness fixes, reload hardening, isolated test infra ([5cb6483](https://github.com/AZX-PBC-OSS/TaskQ/commit/5cb64837a28a514c4f2c13ee520af6aa2c5681c8))
* stop swallowing exceptions in worker exception handlers ([4ff0065](https://github.com/AZX-PBC-OSS/TaskQ/commit/4ff0065f242a45a34ee27f9325640114124c0540))
* stop swallowing exceptions in worker exception handlers ([fc7786b](https://github.com/AZX-PBC-OSS/TaskQ/commit/fc7786b3ba6565f6cb4c17879dbf05f71689120d))
* stringify job ids ([8dbf369](https://github.com/AZX-PBC-OSS/TaskQ/commit/8dbf369353fd649997dcf65013b927cd9b263396))


### Documentation

* improve examples, add real-world actors, deployment/troubleshooting/tutorial guides ([1d02b34](https://github.com/AZX-PBC-OSS/TaskQ/commit/1d02b34743014a1edf5574f961853a766761e637))


### Continuous Integration

* add release-please for automated release PRs, tags, and PyPI publish ([#10](https://github.com/AZX-PBC-OSS/TaskQ/issues/10)) ([ab86d7d](https://github.com/AZX-PBC-OSS/TaskQ/commit/ab86d7d371faf550aad8fdceb5f95b9d5da37b48))
* only deploy docs on push to main, not on PRs ([afaffc7](https://github.com/AZX-PBC-OSS/TaskQ/commit/afaffc79c7690db6c2947f61a0e71cf7778bc3d9))

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
