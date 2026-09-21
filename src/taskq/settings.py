"""TaskQ configuration via :mod:`dotenvmodel`.

Environment variables are namespaced with the ``TASKQ_`` prefix and loaded
through dotenvmodel's cascading ``.env`` discovery (``.env`` →
``.env.local`` → ``.env.{env}`` → ``.env.{env}.local``).

The library exposes a single :class:`TaskQSettings` class. Workers and
the client both load it via :meth:`TaskQSettings.load` at startup. To
extend with vendor-specific fields (e.g., ``OTEL_EXPORTER_OTLP_ENDPOINT``
overrides), subclass :class:`TaskQSettings` in the consuming application
and pass that subclass instead.
"""

import logging
import math
import os
import re
from datetime import timedelta
from pathlib import Path
from typing import Self
from uuid import UUID

from dotenvmodel import DotEnvConfig, Field, ValidationError, ValidatorContext
from dotenvmodel.types import PostgresDsn, RedisDsn, SecretStr

from taskq._close import worst_case_teardown_tail
from taskq._json import check_no_nul_str
from taskq.backend._protocol import (
    # Cycle-safe by import direction: nothing in _protocol's own chain
    # imports taskq.settings, and taskq/__init__ always finishes loading
    # backend._protocol (via taskq.actor) before anything loads settings.
    _validate_queue_name,  # pyright: ignore[reportPrivateUsage]  # Why: the canonical queue-name validator; the enqueue and actor chokepoints run the same one, so the charset cannot drift between them.
)
from taskq.connections import (
    DEFAULT_MAX_CACHED_STATEMENT_LIFETIME,
    DEFAULT_STATEMENT_CACHE_SIZE,
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining
    DEFAULT_EVENT_RETENTION_BATCH_SIZE,
    DEFAULT_EVENT_RETENTION_PERIOD,
    DEFAULT_EVENT_WRITER_BATCH_SIZE,
    DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
    DEFAULT_KEYED_ROW_RECLAIM_BATCH_SIZE,
    DEFAULT_KEYED_ROW_RECLAIM_PERIOD,
    DEFAULT_MAX_KEYED_RESERVATIONS,
    DEFAULT_MAX_RETRY_BACKOFF,
    DEFAULT_PRUNE_BATCH_SIZE,
    DEFAULT_PRUNE_RETENTION,
    IDEMPOTENCY_KEY_BYTES_CEILING,
    MAX_IDEMPOTENCY_KEY_BYTES,
    MAX_RESULT_BYTES,
    RECLAIM_EVENT_VISIBILITY_DELAY,
    RELEASE_EXIT_TAIL_SLACK_SECS,
    TERMINAL_WRITE_BUDGET_SECS,
    WATCHDOG_METRICS_FLUSH_TIMEOUT_SECS,
    check_channels_fit,
)

__all__ = ["OIDCSettings", "SAMLSettings", "TaskQSettings", "WorkerSettings"]


class OIDCSettings(DotEnvConfig):
    """OIDC SSO configuration (loaded from ``TASKQ_OIDC_*`` env vars)."""

    env_prefix = "TASKQ_OIDC_"

    issuer: str = Field(
        default="",
        description="OIDC discovery issuer URL "
        "(e.g. https://login.microsoftonline.com/{tenant}/v2.0).",
    )
    client_id: str = Field(default="", description="OAuth2 client ID registered at the IdP.")
    # Why SecretStr (dotenvmodel's native mechanism): a settings
    # repr reaches logs, debuggers and crash tracebacks; SecretStr masks
    # itself there and in every error path, loads straight from the env var
    # (a raw str is coerced on load and on a str default), and unwraps only
    # at the explicit get_secret_value() boundary. client_id stays plain ,
    # a public OAuth2 identifier, not a credential.
    client_secret: SecretStr = Field(default="", description="OAuth2 client secret.")
    redirect_uri: str = Field(
        default="",
        description="Must match the app registration's configured redirect URI.",
    )
    session_secret: SecretStr = Field(
        default="",
        description="Signing key for session cookies; "
        "use >=32 bytes of random data. Rotate to invalidate all sessions.",
    )
    session_max_age_seconds: int = Field(
        default=28800,
        ge=60,
        description="Session lifetime (s). Default 8h.",
    )
    scope: str = Field(
        default="openid profile email",
        description="OIDC scopes. Add 'Group.Read.All' for the "
        "Entra overage group_resolver (Graph API /me/memberOf).",
    )
    group_claim: str | None = Field(
        default=None,
        description="ID token claim name for groups "
        "(e.g. 'groups', 'roles'). None = authentication-only authorization.",
    )
    allowed_groups: str = Field(
        default="",
        description="Comma-separated group allowlist.",
    )

    @property
    def allowed_groups_set(self) -> frozenset[str]:
        return _parse_groups(self.allowed_groups)


class SAMLSettings(DotEnvConfig):
    """SAML SSO configuration (loaded from ``TASKQ_SAML_*`` env vars)."""

    env_prefix = "TASKQ_SAML_"

    entity_id: str = Field(default="", description="SP entity ID.")
    acs_url: str = Field(
        default="",
        description="Assertion Consumer Service URL.",
    )
    idp_entity_id: str = Field(default="", description="IdP entity ID.")
    idp_sso_url: str = Field(default="", description="IdP SSO endpoint.")
    idp_x509_cert: str = Field(
        default="",
        description="IdP signing certificate (PEM).",
    )
    sp_x509_cert: str | None = Field(
        default=None,
        description="SP cert (signed requests / encrypted assertions).",
    )
    # Why SecretStr: the SP's private key is the secret half of the keypair
    # (its public half sp_x509_cert and the IdP's idp_x509_cert stay plain ,
    # published certificates, not credentials); session_secret signs session
    # cookies. Same dotenvmodel-native masking rationale as OIDCSettings.
    sp_private_key: SecretStr | None = Field(
        default=None,
        description="SP private key (PEM).",
    )
    session_secret: SecretStr = Field(
        default="",
        description="Signing key for session cookies.",
    )
    session_max_age_seconds: int = Field(
        default=28800,
        ge=60,
        description="Session lifetime (s). Default 8h.",
    )
    group_attribute: str | None = Field(
        default=None,
        description="SAML attribute name for groups.",
    )
    allowed_groups: str = Field(
        default="",
        description="Comma-separated group allowlist.",
    )
    allow_cookieless_fallback: bool = Field(
        default=False,
        description="TASKQ_SAML_ALLOW_COOKIELESS_FALLBACK. Opt in to the SAML "
        "cookie-less ACS fallback: accept a callback with no usable "
        "correlation cookie when its validated InResponseTo names a pending "
        "AuthnRequest this process issued. Serves browsers that block the "
        "cross-site cookie; nothing ties the response to the browser "
        "posting it, so a captured signed response can be planted on a "
        "cookie-less victim (login CSRF) -- opt in only if that tradeoff is "
        "acceptable. Default off; see docs/guides/sso.md.",
    )

    @property
    def allowed_groups_set(self) -> frozenset[str]:
        return _parse_groups(self.allowed_groups)


_VALID_SSO_BACKENDS = frozenset({"none", "oidc", "saml"})


def _sso_backend_validator(value: str, ctx: ValidatorContext) -> str:
    normalized = value.lower()
    if normalized not in _VALID_SSO_BACKENDS:
        raise ValueError(
            f"{ctx.field_name} must be one of {sorted(_VALID_SSO_BACKENDS)}, got {value!r}"
        )
    return normalized


_VALID_FRAME_ANCESTORS = frozenset({"none", "self"})


def _frame_ancestors_validator(value: str, ctx: ValidatorContext) -> str:
    """Reject anything that is not a closed framing policy.

    Why fail rather than fall back to the default: a typo that silently became
    'no header' would take the clickjacking defence off in exactly the
    deployment whose operator believed they had configured it.
    """
    normalized = value.strip().lower().strip("'")
    if normalized not in _VALID_FRAME_ANCESTORS:
        raise ValueError(
            f"{ctx.field_name} must be one of {sorted(_VALID_FRAME_ANCESTORS)}, got {value!r}"
        )
    return normalized


def _schema_name_validator(value: str, ctx: ValidatorContext) -> str:
    """Validate `schema_name` against the canonical identifier regex.

    A validator hook rather than `regex=` deliberately. `regex=` is a
    dotenvmodel BUILT-IN constraint, and built-in constraints are skipped under
    `load_from_dict(..., validate=False)`, while validator hooks always run.
    `schema_name` is the one setting that reaches raw SQL as an interpolated
    identifier, so it is the last field that should be skippable.

    Not currently reachable in production -- `validate=False` appears only in
    test fixtures, and every interpolation site independently re-checks
    `_IDENT_RE`, which is genuine defence in depth. This closes the landmine
    before some future config-reload path steps on it. The same class was
    already fixed for `log_format` for the same reason.
    """
    if not _IDENT_RE.match(value):
        raise ValueError(
            f"{ctx.field_name} must be a valid SQL identifier "
            f"([A-Za-z_][A-Za-z0-9_]*), got {value!r}"
        )
    if len(value) > 63:
        # NAMEDATALEN is 64 including the terminator, so Postgres silently
        # truncates longer identifiers, while Redis channel templates
        # interpolate the full string, quietly diverging between stores.
        # A length cap belongs in this hook, not `max_length=`, because
        # built-in constraints are skipped under validate=False (see above).
        # Chars == bytes here: _IDENT_RE already admitted only ASCII.
        raise ValueError(
            f"{ctx.field_name} must be at most 63 characters (Postgres "
            f"NAMEDATALEN truncates longer identifiers), got {len(value)} characters"
        )
    # Every NOTIFY channel is derived from the schema; one that overflows
    # the identifier limit is a listener that silently hears nothing, so
    # the derivation is exercised here, at load, where it can refuse.
    try:
        check_channels_fit(value)
    except ValueError as exc:
        raise ValueError(f"{ctx.field_name}: {exc}") from exc
    return value


class _NoEnvFilesWarningFilter(logging.Filter):
    """Drop only dotenvmodel's "No .env files found in <dir>" WARNING.

    Why a logger-level filter works: every dotenvmodel 1.x module logs
    through the single ``"dotenvmodel"`` logger (``LOGGER_NAME`` in
    ``dotenvmodel/_constants.py``; ``loading.py`` hardcodes the same
    string), there are no child loggers, so one filter sees all of its
    records. Why prefix matching rather than raising the logger level:
    the level approach swallows *every* dotenvmodel warning, hiding real
    misconfiguration (e.g. an invalid ``DOTENV_OVERRIDE`` value falling
    back to default precedence with zero signal); this drops exactly the
    one known-noisy warning and leaves the rest visible.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        # getMessage() rather than record.message: the message attribute is
        # only populated during handler emission, after filtering runs.
        return not record.getMessage().startswith("No .env files found")


class TaskQSettings(DotEnvConfig):
    """Top-level TaskQ runtime configuration."""

    env_prefix = "TASKQ_"

    pg_dsn: PostgresDsn = Field(
        default=PostgresDsn("postgresql://taskq:taskq@localhost:5432/taskq"),
        description="Direct (non-PgBouncer) DSN. LISTEN/NOTIFY and advisory locks need a session.",
    )
    pg_is_pooled: bool = Field(
        default=False,
        description="TASKQ_PG_IS_POOLED. Declare that the DSN(s) TaskQ builds pools "
        "from route through a transaction-mode pooler (PgBouncer pool_mode=transaction, "
        "RDS Proxy, Supavisor). asyncpg cannot detect a pooler from the wire protocol - "
        "PgBouncer speaks plain Postgres - so the operator declares the topology. When "
        "True every pool TaskQ builds disables the prepared-statement cache "
        "(statement_cache_size=0, max_cached_statement_lifetime=0, overriding "
        "TASKQ_STATEMENT_CACHE_SIZE / TASKQ_MAX_CACHED_STATEMENT_LIFETIME) so a prepared "
        "statement can never outlive the server connection a pooler remaps underneath it, "
        "and the worker treats pooler-remap statement errors (SQLSTATE 26000 "
        "unnamed_prepared_statement, 42P05 duplicate_prepared_statement) as transient "
        "instead of loud surprises. Applies only to pools TaskQ builds: bring-your-own "
        "pools (WorkerConnections factories, a caller-supplied pool) must set the same "
        "create_pool kwargs themselves. Leave False when every DSN reaches Postgres "
        "directly.",
    )
    schema_name: str = Field(
        default="taskq",
        validator=_schema_name_validator,
        description="Postgres schema for all TaskQ tables.",
    )
    redis_url: RedisDsn | None = Field(
        default=None,
        description="Optional Redis URL. Required for real-time progress fanout.",
    )
    environment: str | None = Field(
        default=None,
        description="TASKQ_ENVIRONMENT. Deployment environment label. The "
        "unauthenticated-admin WARNING ('admin-ui-no-auth') fires in EVERY "
        "environment whenever the admin UI is served without auth_dependency. "
        "'dev' and 'development' additionally skip the fail-closed "
        "RuntimeError (the WARNING then notes the absence is the dev "
        "exemption); any other value (or None/empty) fails closed when "
        "admin_ui_require_auth is True.",
    )
    admin_max_sse_connections: int = Field(
        default=50,
        ge=1,
        description="TASKQ_ADMIN_MAX_SSE_CONNECTIONS. Maximum concurrent SSE "
        "connections the admin UI will serve. Used to size the connection-limit "
        "semaphore.",
    )
    progress_max_sse_connections: int = Field(
        default=50,
        ge=1,
        description="TASKQ_PROGRESS_MAX_SSE_CONNECTIONS. Maximum concurrent "
        "per-job progress SSE streams this process will serve. Each holds a "
        "Redis pubsub subscription and an asyncio task for as long as the "
        "client stays connected, so an uncapped endpoint is a resource-"
        "exhaustion surface on the app hosting the pipeline.",
    )
    progress_require_auth: bool = Field(
        default=True,
        description="TASKQ_PROGRESS_REQUIRE_AUTH. When True (the default), "
        "taskq.web.progress.create_router raises RuntimeError if "
        "auth_dependency is None in a non-dev environment, failing closed. "
        "Set to False to suppress the error and allow unauthenticated "
        "per-job progress/state endpoints in non-dev (not recommended - only "
        "for deployments that authenticate at the ingress).",
    )
    admin_host: str = Field(
        default="0.0.0.0",  # noqa: S104  # Why: default bind address for the admin UI server; production deployments override via TASKQ_ADMIN_HOST env var.
        description="TASKQ_ADMIN_HOST. Bind address for ``taskq ui serve``.",
    )
    admin_port: int = Field(
        default=8080,
        ge=1,
        le=65535,
        description="TASKQ_ADMIN_PORT. Bind port for ``taskq ui serve``.",
    )
    admin_url: str = Field(
        default="http://localhost:8080",
        description="TASKQ_ADMIN_URL. Public base URL of the admin UI as seen "
        "from a browser. Used by the example trigger app to construct redirect "
        "URLs after enqueueing. In a shared-container deployment this is the "
        "external address of the admin process (e.g. http://localhost:8001). "
        "Override when admin and trigger app are on different hosts or ports.",
    )
    admin_ui_polling_interval_seconds: float = Field(
        default=2.0,
        ge=0.1,
        description="TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS. How often the admin UI "
        "polls PG in polling/degraded mode. Injected as poll_interval_ms "
        "into every template.",
    )
    admin_worker_liveness_seconds: int = Field(
        default=30,
        ge=1,
        description="TASKQ_ADMIN_WORKER_LIVENESS_SECONDS. How recently a worker "
        "must have written last_seen_at to count as alive: it drives the admin "
        "UI's 'queue has pending jobs but no alive worker' banner and the "
        "leader's watchdog_healthy verdict, and on the worker side the leader's "
        "taskq.queue.live_workers gauge and the stranded-jobs detector's "
        "unserved-queue arm. Must comfortably exceed "
        "TASKQ_HEARTBEAT_INTERVAL (default 10 s), so the default 30 s is three "
        "beats; a deployment that lengthens the heartbeat, or whose PG is "
        "cross-region, has to raise this or every healthy worker reads as dead. "
        "Measured by Postgres against clock_timestamp(), never by the admin "
        "process's own clock.",
    )
    admin_ui_allow_rate_limit_reset: bool = Field(
        default=False,
        description="TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET. When True, the admin UI "
        "shows a reset button on the rate-limits page and serves the "
        "POST /rate-limits/{bucket_name}/reset endpoint. Default False "
        "for safety - prevents accidental resets in production.",
    )
    admin_ui_require_auth: bool = Field(
        default=True,
        description="TASKQ_ADMIN_UI_REQUIRE_AUTH. When True (the default), "
        "create_router raises RuntimeError if auth_dependency is None in a "
        "non-dev environment, failing closed. Set to False to suppress the "
        "error and allow an unauthenticated admin UI in non-dev (not "
        "recommended - only for air-gapped or localhost-only deployments).",
    )
    admin_ui_frame_ancestors: str = Field(
        default="none",
        validator=_frame_ancestors_validator,
        description="TASKQ_ADMIN_UI_FRAME_ANCESTORS. Who may frame admin pages: "
        "'none' (the default, nobody) or 'self' (the admin UI's own origin, for "
        "a host app that embeds the admin UI in its own dashboard). Emitted as "
        "both 'Content-Security-Policy: frame-ancestors ...' and the legacy "
        "'X-Frame-Options' (DENY / SAMEORIGIN). CSRF is no defence against UI "
        "redress: the framed page is the real, authenticated, same-origin page, "
        "so a tricked click carries a valid token.",
    )
    admin_ui_secure_cookies: bool = Field(
        default=True,
        description="TASKQ_ADMIN_UI_SECURE_COOKIES. Sets the 'Secure' flag on the "
        "admin UI's CSRF cookie. A configured value, not one inferred from "
        "request.url.scheme: behind a TLS-terminating edge (Azure Application "
        "Gateway, App Service) the app sees plain http, so an inferred flag is "
        "silently dropped on a connection the browser reached over HTTPS. Set "
        "False only for local http dev, where a Secure cookie is rejected by "
        "the browser and the admin UI stops working.",
    )
    admin_acquire_timeout: float = Field(
        default=5.0,
        gt=0,
        description="TASKQ_ADMIN_ACQUIRE_TIMEOUT (seconds). Bounds every wait an "
        "admin UI or progress request makes for a backend resource before "
        "its own query runs: a Postgres pool checkout and a Redis read. A "
        "pool with every connection wedged, or a black-holed broker, answers "
        "the request with 503 (Retry-After: 2) after this long instead of "
        "hanging it - and every other request behind it - until the client "
        "gives up. The query itself is bounded by the pool's command_timeout.",
    )
    admin_actions_enabled: bool = Field(
        default=False,
        description="TASKQ_ADMIN_ACTIONS_ENABLED. When True, the admin UI permits "
        "state-changing actions: run schedule now, enable/disable/skip a "
        "schedule, retry job, cancel job. "
        "Default False - prevents on-demand triggering of registered business "
        "logic, and silent suppression of scheduled work, via the admin UI "
        "without explicit opt-in. Separate from "
        "auth_dependency, which controls read access to all admin routes.",
    )

    # -- Managed identities -----------------------------------------------
    pg_credential_provider: str | None = Field(
        default=None,
        description="TASKQ_PG_CREDENTIAL_PROVIDER. Module:attr reference to a "
        "PgCredentialProvider (e.g. myapp.auth:make_provider) - an instance, a "
        "zero-arg factory returning one, or the provider class. Every Postgres "
        "pool and dedicated connection is then built through it, so SIGHUP / "
        "TASKQ_RELOAD_INTERVAL rotate real credentials. The CLI options "
        "--pg-credential-provider (taskq worker / migrate / ui serve) override "
        "it. The ref is resolved, not validated, at load time: the import lives "
        "in the CLI so a bad ref exits 1 with a pointed message instead of a "
        "settings traceback. See docs/guides/managed-identities.md.",
    )
    redis_credential_provider: str | None = Field(
        default=None,
        description="TASKQ_REDIS_CREDENTIAL_PROVIDER. Module:attr reference to a "
        "RedisCredentialProvider, in the same shapes as pg_credential_provider. "
        "Requires TASKQ_REDIS_URL. Overridden by --redis-credential-provider.",
    )
    reload_interval: float | None = Field(
        default=None,
        gt=0,
        description="TASKQ_RELOAD_INTERVAL (seconds). Cadence of the credential "
        "hot-reload (the same path as SIGHUP) on the worker and on `taskq ui "
        "serve`: every provider-backed pool and connection is rebuilt on a "
        "fresh credential with no external signal required - the rotation "
        "path for platforms without SIGHUP (e.g. Windows) and for hands-off "
        "scheduled rotation (e.g. ~720s for AWS IAM's 15-minute tokens). "
        "Unset, the cadence is derived from the lease the provider grants "
        "when it reports one (a Vault dynamic credential is rebuilt at half "
        "its lease TTL - see taskq.auth.ReloadSchedule); a username-bearing "
        "provider that reports no lease then warns at startup, and only "
        "SIGHUP / deps.request_reload() rotate it. Only factory-backed "
        "resources are rebuilt; DSN/static credentials are unaffected.",
    )

    # -- SSO / SAML -------------------------------------------------------
    sso_backend: str = Field(
        default="none",
        validator=_sso_backend_validator,
        description="TASKQ_SSO_BACKEND. Selects the SSO backend for the admin UI: "
        "'none' (default, unauthenticated/BYO-auth), 'oidc' (taskq[oidc]), "
        "or 'saml' (taskq[saml]). See docs/guides/sso.md.",
    )
    # Why SecretStr: the bearer credential for the health/metrics routes ,
    # same dotenvmodel-native masking rationale as the SSO secrets. Two
    # gotchas pinned by the cli's unwrap: a SecretStr is always truthy (even
    # the empty default), and an unset env var loads the field as None ,
    # hence the Optional annotation and the None-safe set-check. DSN fields
    # stay their BaseDsn types (their own repr already masks passwords).
    health_token: SecretStr | None = Field(
        default="",
        description="TASKQ_HEALTH_TOKEN. Bearer token for machine-to-machine "
        "access to health/metrics endpoints. When set, health and metrics "
        "routes require a matching 'Authorization: Bearer <token>' header. "
        "Leave empty for unauthenticated cluster-internal access - but see "
        "health_require_token, which fails closed on an empty token outside dev.",
    )
    health_require_token: bool = Field(
        default=True,
        description="TASKQ_HEALTH_REQUIRE_TOKEN. When True (the default), "
        "taskq ui serve raises RuntimeError if health_token is empty in a "
        "non-dev environment, failing closed. Set to False to suppress the "
        "error and allow unauthenticated health/metrics endpoints in non-dev "
        "(e.g. when relying on network policy / cluster-internal-only access "
        "instead of a bearer token. Many k8s liveness/readiness "
        "probes don't send auth headers by default, so enabling the token "
        "may require updating the probe config too).",
    )
    migrate_on_start: bool = Field(
        default=False,
        description="TASKQ_MIGRATE_ON_START. When True, apply pending migrations "
        "before the admin UI accepts its first request. Aborts startup "
        "if migrations fail. Consumed ONLY by `taskq ui serve` -- the worker "
        "ignores it (and warns when it is set), because N worker replicas "
        "racing to migrate is the concurrent-migration hazard migrations are "
        "supposed to avoid. Migrate from a pre-deploy job or init container.",
    )
    example_host: str = Field(
        default="0.0.0.0",  # noqa: S104  # Why: default bind address for the example trigger app; production deployments override via TASKQ_EXAMPLE_HOST env var.
        description="TASKQ_EXAMPLE_HOST. Bind address for the example trigger "
        "app (uvicorn). Only consumed by the example app; ignored by the "
        "worker and admin UI.",
    )
    example_port: int = Field(
        default=8000,
        ge=1,
        le=65535,
        description="TASKQ_EXAMPLE_PORT. Bind port for the example trigger "
        "app (uvicorn). Only consumed by the example app; ignored by the "
        "worker and admin UI.",
    )

    # -- Idempotency ------------------------------------------------------
    idempotency_key_max_bytes: int = Field(
        default=MAX_IDEMPOTENCY_KEY_BYTES,
        ge=1,
        le=IDEMPOTENCY_KEY_BYTES_CEILING,
        description="TASKQ_IDEMPOTENCY_KEY_MAX_BYTES. Maximum UTF-8 byte "
        "length of idempotency_key, and of idempotency_scope, each. Bytes "
        "rather than characters because the real bound is the composite "
        "unique index jobs_idempotency_scope_key_uniq: a btree v4 entry "
        "cannot exceed 2704 bytes, counted encoded. The ceiling keeps "
        "scope + key + index-tuple overhead under that, so no value here "
        "can turn a valid enqueue into a raw Postgres 'index row size ... "
        "exceeds btree version 4 maximum'. Raise it when keys are derived "
        "from URLs, composite business keys or opaque vendor cursors.",
    )

    # -- Postgres statement cache (asyncpg) -------------------------------
    # Defaults come from taskq.connections's module constants, the same
    # values every TaskQ-built pool passes explicitly at its construction
    # site (worker role pools, the per-slot transaction pool, the TaskQ
    # client pool, the admin UI pool). One source of truth: a tuning change
    # to a constant moves the settings default with it. taskq.connections
    # imports nothing from this module at runtime, so the dependency stays
    # one-directional.
    statement_cache_size: int = Field(
        default=DEFAULT_STATEMENT_CACHE_SIZE,
        ge=0,
        description="TASKQ_STATEMENT_CACHE_SIZE. Size of the per-connection "
        "prepared-statement LRU asyncpg keeps on every pool TaskQ builds. "
        "asyncpg's default of 100 thrashes on TaskQ's read paths, "
        "list_jobs alone renders 384+ filter-combination SQL variants (a "
        "measured 90-96% steady-state miss rate, each miss re-paying the "
        "Parse/Describe round trips; see benchmarks/ab_stmt_cache.py and "
        "docs/guides/ops.md). 512 covers the variant space with headroom. "
        "0 disables the statement cache. Applies only to pools TaskQ "
        "builds: bring-your-own pools (WorkerConnections factories, a "
        "caller-supplied pool) must pass the same create_pool kwargs "
        "themselves, TaskQ cannot resize a pool it did not build.",
    )
    max_cached_statement_lifetime: int = Field(
        default=DEFAULT_MAX_CACHED_STATEMENT_LIFETIME,
        ge=0,
        description="TASKQ_MAX_CACHED_STATEMENT_LIFETIME (seconds). How long "
        "a prepared statement may stay in asyncpg's per-connection cache on "
        "every pool TaskQ builds. asyncpg's default of 300 s re-prepares "
        "statements on workers that outlive it; 3600 s (1 h) keeps the "
        "textually-stable write hot loop (dispatch, enqueue, terminal "
        "updates, sweeps) prepared across ticks without pinning prepared "
        "plans for the process lifetime. 0 caches statements indefinitely. "
        "Same TaskQ-built-pools-only scope as statement_cache_size.",
    )

    # -- Enqueue advisory-lock budgets ------------------------------------
    # Defaults are the values of taskq.backend._enqueue's
    # DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS / DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS
    # / DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS (5 s each), written as literals,
    # not imported, because that module binds the asyncpg driver at import
    # time and this module is imported by the driver-free testing boundary
    # (taskq.testing.settings). The PostgresBackend enqueue wrappers read
    # these fields at the lock use sites (the dispatch_oversample plumbing
    # pattern); a deployment that sets none of them keeps the exact
    # pre-knob ceilings the module constants supplied.
    #
    # Declared here rather than on WorkerSettings because enqueue is a
    # CLIENT path: a producer process builds TaskQSettings and never a
    # WorkerSettings, so budgets that lived on the subclass were
    # unreachable from the side that actually takes these locks. The
    # client's own per-query pool bound follows these knobs rather than
    # the reverse: TaskQ sizes the pools it builds from them
    # (client._taskq's _CLIENT_POOL_COMMAND_TIMEOUT_SECS floor, re-derived
    # upward by connections.lock_budget_command_timeout_secs when a budget
    # is widened past its default here) and then delivers each budget
    # clamped to 80% of that bound (connections.bounded_lock_budget_ms),
    # so the server-side lock_timeout always fires before the pool's
    # client-side timer and the refusal is the typed one.
    max_pending_lock_timeout_ms: float = Field(
        default=5000.0,
        description="TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS (milliseconds). Bounded "
        "wait for the max_pending advisory lock on the single-enqueue path "
        "(the count-then-insert serialization per capped actor). Exhaustion "
        "raises MaxPendingLockTimeoutError, the same typed backpressure "
        "treatment as a cap rejection, and denials consume retry budget, so "
        "widen this during an outage that slows lock holders rather than "
        "letting the fixed ceiling convert slow holders into refused "
        "enqueues. Widening past the default re-derives the per-query bound "
        "of every pool the TaskQ client builds itself, so the wider budget "
        "is delivered end to end; at or below the default the client path "
        "delivers the budget clamped to 80% of that pool bound, a share of "
        "the 10 s shipped bound exceeds the 5 s default budget, so the "
        "defaults are delivered in full, and the margin is what the "
        "server-side lock_timeout needs to fire "
        "before the pool's own timer. 0 or less waits indefinitely "
        "server-side (the lock_timeout GUC convention shared with the "
        "sibling budgets), a pool TaskQ builds still applies its per-query "
        "bound, so set a large finite value there instead.",
    )
    unique_for_lock_timeout_ms: float = Field(
        default=5000.0,
        description="TASKQ_UNIQUE_FOR_LOCK_TIMEOUT_MS (milliseconds). Bounded "
        "wait for the unique_for single-flight advisory lock on the "
        "single-enqueue path (the identity preflight-then-insert "
        "serialization). Exhaustion raises UniqueForLockTimeoutError with "
        "retry-yields-dedup guidance; the correct contention outcome is "
        "usually the dedup return, so a unique_for caller may want a longer "
        "wait than the max_pending budget before giving up on the answer. "
        "Separate knob from max_pending_lock_timeout_ms because the two "
        "budgets bound different semantics (identity dedup vs capacity "
        "admission). Widening past the default re-derives the per-query "
        "bound of every pool the TaskQ client builds itself (see "
        "TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS for the delivery rule); at or "
        "below the default the client path delivers the budget clamped to "
        "80% of that pool bound. 0 or less waits indefinitely server-side "
        "(the lock_timeout GUC convention shared with the sibling budgets) "
        ", a pool TaskQ builds still applies its per-query bound, so set a "
        "large finite value there instead.",
    )
    idempotency_lock_timeout_ms: float = Field(
        default=5000.0,
        description="TASKQ_IDEMPOTENCY_LOCK_TIMEOUT_MS (milliseconds). Bounded "
        "wait for the idempotency token INSERT's speculative-lock conflict "
        "on the single-enqueue path, another transaction's UNCOMMITTED row "
        "with the same (idempotency_scope, idempotency_key) pair. On a "
        "transactional consumer the holder is the actor's own open "
        "transaction (unbounded by default), so this budget bounds the "
        "VICTIM; exhaustion raises IdempotencyKeyLockTimeoutError, meaning "
        "the dedup answer could not be determined in time, retry the same "
        "enqueue, which typically dedupes against the now-visible winner. "
        "Widening past the default re-derives the per-query bound of every "
        "pool the TaskQ client builds itself (see "
        "TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS for the delivery rule); at or "
        "below the default the client path delivers the budget clamped to "
        "80% of that pool bound. 0 or less waits indefinitely server-side "
        "(the lock_timeout GUC convention shared with the sibling budgets) "
        ", a pool TaskQ builds still applies its per-query bound, so set a "
        "large finite value there instead.",
    )

    @classmethod
    def load(
        cls,
        env: str | None = None,
        *,
        override: bool | None = None,
        env_dir: Path | str | None = None,
        read_dotfiles: bool | None = None,
        read_environ: bool | None = None,
        load_local: bool | None = None,
    ) -> Self:
        """Load settings via dotenvmodel's cascading ``.env`` discovery.

        All parameters are forwarded to ``DotEnvConfig.load`` unchanged
        (resolution: explicit argument > ``DOTENV_*`` env var > default).
        ``override=None`` keeps dotenvmodel's default precedence, the
        process environment beats ``.env`` files; pass ``override=True``
        or set ``DOTENV_OVERRIDE=true`` to make ``.env`` files win instead.
        ``read_dotfiles=False`` / ``read_environ=False`` disable the
        ``.env`` cascade / the process environment respectively, per
        dotenvmodel's documented symmetry.

        dotenvmodel logs a WARNING ("No .env files found in <cwd>") on
        every call when no ``.env`` file is present - noisy on every CLI
        invocation in projects that configure purely via real environment
        variables. ``read_dotfiles=False`` is not the answer: it silences
        the warning but disables the ``.env`` cascade entirely, a
        documented core TaskQ feature. This override instead installs a
        :class:`_NoEnvFilesWarningFilter` on the ``dotenvmodel`` logger
        for the duration of the call, dropping only that one warning;
        every other dotenvmodel warning (e.g. an invalid ``DOTENV_*``
        knob value) remains visible.

        ``WorkerSettings.load`` inherits this override via MRO, so worker
        startup gets the same quiet load.
        """
        dotenv_logger = logging.getLogger("dotenvmodel")
        no_env_files = _NoEnvFilesWarningFilter()
        dotenv_logger.addFilter(no_env_files)
        try:
            return super().load(
                env=env,
                override=override,
                env_dir=env_dir,
                read_dotfiles=read_dotfiles,
                read_environ=read_environ,
                load_local=load_local,
            )
        finally:
            dotenv_logger.removeFilter(no_env_files)

    @classmethod
    def resolve_cascade_value(cls, env_var_name: str) -> str | None:
        """One variable's value through :meth:`load`'s own layer resolution,
        without loading (or validating) the model.

        Resolution is EXACTLY ``load()``'s, per dotenvmodel's own tier and
        precedence rules: every behavior knob resolves through
        ``resolve_load_params`` (explicit ``DOTENV_*`` env var > default),
        the dotfile cascade is read with ``read_env_files`` (never written
        into ``os.environ``), ``read_dotfiles=False`` skips the file layer
        entirely, ``read_environ=False`` excludes the process environment
        as a value source, and the winner is process-env-vs-dotfile per
        the resolved ``override`` (default: the process environment wins).
        The per-field winner selection replicates dotenvmodel's private
        ``config._resolve_raw_value``: the function is not exported, so
        its six-line policy is restated here and pinned by
        ``tests/test_client_cli_first_use_bounded.py``'s precedence tests
        in both directions.

        Why this exists: ``load()`` validates EVERY field, so a caller
        that needs one value (the client's schema default, its lock-budget
        overlay) would fail on a malformed setting it never uses.
        This method resolves the value; validation stays the caller's,
        scoped to the fields it actually consumes.

        The "No .env files found" warning is filtered for the duration of
        the cascade read, exactly as :meth:`load` filters it.
        """
        from dotenvmodel.loading import read_env_files, resolve_load_params

        params = resolve_load_params()
        dotenv_logger = logging.getLogger("dotenvmodel")
        no_env_files = _NoEnvFilesWarningFilter()
        dotenv_logger.addFilter(no_env_files)
        try:
            layer = (
                read_env_files(
                    env=params.env,
                    env_dir=params.env_dir,
                    load_local=params.load_local,
                    read_environ=params.read_environ,
                )
                if params.read_dotfiles
                else None
            )
        finally:
            dotenv_logger.removeFilter(no_env_files)

        os_value = os.environ.get(env_var_name) if params.read_environ else None
        file_value = layer.values.get(env_var_name) if layer is not None else None
        if params.override:
            return file_value if file_value is not None else os_value
        return os_value if os_value is not None else file_value

    @property
    def oidc(self) -> OIDCSettings:
        """Lazily loaded OIDC sub-config (``TASKQ_OIDC_*`` env vars).

        Backed by dotenvmodel's ``cached()`` singleton: the environment is
        read on first access and the same instance returned thereafter.

        Reload (e.g. on SIGHUP): call ``OIDCSettings.cached().reload()`` to
        re-read the environment and mutate the shared instance in place -
        every holder observes the new values - or
        ``OIDCSettings.reset_cached()`` to force the next access to
        re-load. Tests that change ``TASKQ_OIDC_*`` mid-process must do
        the same (or use ``cached_override()``).
        """
        return OIDCSettings.cached()

    @property
    def saml(self) -> SAMLSettings:
        """Lazily loaded SAML sub-config (``TASKQ_SAML_*`` env vars).

        See :attr:`oidc` for the singleton/caching semantics and the
        SIGHUP-style reload recipe (``SAMLSettings.cached().reload()`` /
        ``SAMLSettings.reset_cached()``).
        """
        return SAMLSettings.cached()

    @property
    def is_dev_environment(self) -> bool:
        """Whether this process is labeled a development environment.

        The dev label is the single carve-out from the fail-closed auth
        gates (admin UI, health/metrics token, progress router):
        ``TASKQ_ENVIRONMENT`` set to ``dev`` or ``development`` lets those
        surfaces start without auth so local development needs no token
        machinery. Any other value - including unset - is not dev, so the
        gates fail closed.
        """
        return self.environment in {"dev", "development"}


def _parse_groups(raw: str) -> frozenset[str]:
    return frozenset(g.strip() for g in raw.split(",") if g.strip())


def _non_negative_timedelta(value: timedelta, ctx: ValidatorContext) -> timedelta:
    if value < timedelta(0):
        raise ValueError(f"{ctx.field_name} must not be negative, got {value}")
    return value


def _positive_timedelta(value: timedelta, ctx: ValidatorContext) -> timedelta:
    if value <= timedelta(0):
        raise ValueError(f"{ctx.field_name} must be > 0, got {value}")
    return value


def _positive_finite_float(value: float, ctx: ValidatorContext) -> float:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{ctx.field_name} must be > 0 and finite, got {value}")
    return value


_VALID_LOG_FORMATS = frozenset({"json", "console"})


def _log_format_validator(value: str, ctx: ValidatorContext) -> str:
    # A validator hook (not choices=) so the check also runs under
    # load_from_dict(..., validate=False) - choices= is a built-in constraint
    # that validate=False skips, which would let an invalid LOG_FORMAT load
    # silently. See dotenvmodel docs: validator hooks run regardless of validate.
    if value not in _VALID_LOG_FORMATS:
        raise ValueError(
            f"{ctx.field_name} must be one of {sorted(_VALID_LOG_FORMATS)}, got {value!r}"
        )
    return value


_VALID_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
_LOG_LEVEL_CHOICES = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def _log_level_validator(value: str, ctx: ValidatorContext) -> str:
    normalized = value.upper()
    if normalized not in _VALID_LOG_LEVELS:
        raise ValueError(f"{ctx.field_name} must be one of {_LOG_LEVEL_CHOICES}, got {value!r}")
    return normalized


_HH_MM_PATTERN = re.compile(r"^(\d{2}):(\d{2})$")


def _hh_mm_validator(value: str, ctx: ValidatorContext) -> str:
    m = _HH_MM_PATTERN.match(value)
    if m is None:
        raise ValueError(f'{ctx.field_name} must be HH:MM format (e.g. "03:00"), got {value!r}')
    hours = int(m.group(1))
    minutes = int(m.group(2))
    if not (0 <= hours <= 23 and 0 <= minutes <= 59):
        raise ValueError(f'{ctx.field_name} must be HH:MM format (e.g. "03:00"), got {value!r}')
    return value


def _cron_expr_validator(value: str | None, ctx: ValidatorContext) -> str | None:
    if value is None or value == "":
        return value
    # Lazy import: croniter (+ dateutil) costs ~16ms at import time and is
    # only needed when a cron expression is actually configured, not for
    # ``import taskq`` (taskq.cron defers it for the same reason).
    from croniter import croniter

    if not croniter.is_valid(value):
        raise ValueError(f"{ctx.field_name} must be a valid cron expression, got {value!r}")
    return value


def _workgroup_instance_validator(value: str, ctx: ValidatorContext) -> str:
    """UUID-validate ``workgroup_instance`` at load time.

    The worker calls ``UUID(workgroup_instance)`` at registration
    (worker/run.py); without this hook a malformed value surfaces as a raw
    ``ValueError`` mid-registration instead of a clean settings-load error.
    ``None`` (and the empty string, which dotenvmodel coerces to ``None``
    for ``str | None`` fields) never reaches the hook.
    """
    try:
        UUID(value)
    except ValueError as exc:
        raise ValueError(f"{ctx.field_name} must be a valid UUID, got {value!r}") from exc
    return value


def _worker_label_validator(value: str, ctx: ValidatorContext) -> str:
    """Reject a NUL in ``worker_label`` at load time.

    The label is bound directly as a text parameter in the worker
    registration INSERT; a NUL reaches Postgres as an opaque asyncpg 22021
    (``CharacterNotInRepertoireError``) at startup.
    """
    check_no_nul_str(value, what=ctx.field_name)
    return value


def _queue_names_validator(value: list[str], ctx: ValidatorContext) -> list[str]:
    """Validate each ``queues`` item against the canonical queue-name charset.

    Queue names flow into the registration INSERT's ``text[]`` parameter and
    must satisfy the same rule the backend enforces for enqueue-time queue
    names. Delegates to ``backend/_protocol.py``'s ``_validate_queue_name``
    rather than re-testing the regex, so this chokepoint cannot drift from
    the enqueue and actor ones. A NUL is outside that charset, so the rule
    covers it too.
    """
    for i, item in enumerate(value):
        try:
            _validate_queue_name(item)
        except ValueError as exc:
            raise ValueError(
                f"{ctx.field_name}[{i}] must be a valid queue name (letters, "
                f"digits, '_', '.', '-'; no ':'), got {item!r}"
            ) from exc
    return value


class WorkerSettings(TaskQSettings):
    """Worker-specific configuration with three-pool sizing and dual-DSN support.

       Extends :class:`TaskQSettings` with pool-size knobs, dual-DSN fields, and
       the validated ``lock_lease >= (max_heartbeat_failures + 1) *
       (heartbeat_interval + 2 * heartbeat_command_timeout)`` invariant: the
       lease must outlive the worst coherent failed-beat cascade to the
       heartbeat's isolate decision, with the per-tick command budget
    making each failed beat's gap bound true by enforcement.
    """

    # -- DSNs -----------------------------------------------------------
    pg_dsn_direct: PostgresDsn | None = Field(
        default=None,
        description="TASKQ_PG_DSN_DIRECT; falls back to pg_dsn when absent. "
        "Bypasses PgBouncer - used by dispatcher_pool, heartbeat_pool, "
        "notify_conn, and leader_conn.",
    )
    pg_dsn_pooled: PostgresDsn | None = Field(
        default=None,
        description="TASKQ_PG_DSN_POOLED; falls back to pg_dsn when absent. "
        "May route through PgBouncer transaction mode - used by "
        "worker_pool only.",
    )

    # -- Pool sizes -----------------------------------------------------
    dispatcher_pool_size: int = Field(
        default=4,
        ge=1,
        description="TASKQ_DISPATCHER_POOL_SIZE. Max connections for the "
        "dispatcher pool. Bypasses PgBouncer.",
    )
    dispatcher_command_timeout: float = Field(
        default=5.0,
        ge=1.0,
        description="TASKQ_DISPATCHER_COMMAND_TIMEOUT (seconds). Per-query "
        "timeout for the dispatcher pool and the TaskQ-built leader "
        "connections (election, cron, monitor), and the single deadline "
        "wrapped around each period-1 leader-loop iteration (scheduled_wake, "
        "cron): a stalled PG errors the iteration instead of hanging the "
        "loop past its staleness budget. Checked at load time when the "
        "watchdog is enabled: timeout + the 1.0s leader-loop period must be "
        "< max(period x watchdog_tick_grace_factor, watchdog_stale_floor) "
        "for the period-1 leader loops (scheduled_wake, cron), so a "
        "timeout-capped iteration can never false-trip the stale-loop "
        "detector on a healthy worker. The producer loop is not checked "
        "(its multi-statement dispatch_batch is not wrapped in a single "
        "asyncio.timeout). For the dispatcher POOL this configured value is "
        "the FLOOR, not the applied bound: the admission-path rate-limit "
        "acquires run on that pool, so TaskQ re-derives the pool's "
        "command_timeout upward from a widened token_bucket_lock_timeout_ms "
        "or sliding_window_lock_timeout_ms (max(configured, widest budget / "
        "0.8), connections.lock_budget_command_timeout_secs) the same way "
        "the client pool follows the enqueue lock budgets, a widened "
        "admission budget is honored end to end instead of being silently "
        "truncated by the pool's own client-side timer. The leader/notify "
        "dedicated connections keep the configured value: no admission "
        "acquire runs on them.",
    )
    dispatch_oversample: int = Field(
        default=2,
        ge=1,
        le=1000,
        description="TASKQ_DISPATCH_OVERSAMPLE. Multiplier for per-actor candidate "
        "gathering in the dispatch SQL. Each LATERAL reads residual x oversample "
        "candidates. Higher values absorb more identity collisions and "
        "multi-producer contention. Default 2 (tolerates 50% dupe identities). "
        "Set 1 when no identity_key is used and single-producer. "
        "The window also bounds how far one dispatch round can slide past rows "
        "locked by concurrent dispatchers: oversample dispatchers polling the "
        "same (actor, queue) can hold the whole window at once, so size it at "
        "or above the number of dispatchers that routinely poll the same "
        "actor+queue. A round that finds its whole window locked expands it "
        "geometrically (up to 8x) while claimable rows remain, which covers "
        "transient oversubscription, the setting governs the steady state "
        "so the common case never pays the expansion round trip.",
    )
    dispatch_scope_by_home_queue: bool = Field(
        default=False,
        description="TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE. Deprecated no-op, "
        "accepted so configurations that set it keep loading: dispatch is "
        "assignment-routed now (the jobs row carries the routing decision "
        "the old per-actor-capacity scoping approximated), so the flag has "
        "nothing left to apply. The worker logs a deprecated-setting "
        "warning at startup when it is set; remove it from the "
        "environment.",
    )
    # -- Admission row-lock budgets ---------------------------------------
    # Defaults are the values of the rate-limit package's
    # DEFAULT_TOKEN_BUCKET_LOCK_TIMEOUT_MS /
    # DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS (5 s each), written as
    # literals, not imported, because of dependency direction, not driver
    # binding (unlike the enqueue budgets above: both ratelimit modules
    # keep their asyncpg import under TYPE_CHECKING). Those modules
    # CONSUME this settings object, their PG acquire and refund paths
    # read these fields off it, and importing anything under
    # taskq.ratelimit runs the package's DI provider, which imports this
    # module at runtime, so a runtime import here would close a
    # settings → ratelimit → settings cycle. Drift between the literals
    # and the constants is pinned by
    # test_lock_budget_settings_default_to_the_shipped_constants
    # (tests/test_ratelimit_pg_row_lock_bounded.py). A deployment that
    # sets neither keeps the exact pre-knob ceilings.
    token_bucket_lock_timeout_ms: float = Field(
        default=5000.0,
        description="TASKQ_TOKEN_BUCKET_LOCK_TIMEOUT_MS (milliseconds). Bounded "
        "wait for the rate_limit_buckets row lock on the token-bucket "
        "Postgres acquire and refund. With the Postgres rate-limit fallback "
        "enabled a Redis outage funnels every admission through this lock, so "
        "the bound is what stops one black-holed holder stalling a bucket's "
        "admission. Exhaustion is an admission DENIAL, not a failure: the "
        "acquire fails closed and the denial's retry hint is one more budget, "
        "so shortening this tightens the re-check interval rather than "
        "refusing work. These acquires run on the dispatcher pool, whose "
        "client-side command_timeout would otherwise truncate a budget wider "
        "than its 5.0s floor before the server-side lock_timeout could fire: "
        "widening this past the default re-derives the TaskQ-built dispatcher "
        "pool's per-query bound upward (budget / 0.8), so the wider budget is "
        "delivered end to end, the same reconciliation the client pool "
        "applies to the enqueue lock budgets. A caller-supplied dispatcher "
        "pool keeps its own timeouts; size it above the budgets you set. 0 or "
        "less waits indefinitely server-side (the lock_timeout GUC convention "
        "shared with the sibling budgets), a TaskQ-built pool still applies "
        "its per-query bound, so set a large finite value there instead.",
    )
    sliding_window_lock_timeout_ms: float = Field(
        default=5000.0,
        description="TASKQ_SLIDING_WINDOW_LOCK_TIMEOUT_MS (milliseconds). "
        "Bounded wait for the sliding window's admission lock on Postgres, "
        "the per-bucket advisory lock on the log style and the "
        "rate_limit_buckets row lock on the GCRA style. Separate knob from "
        "token_bucket_lock_timeout_ms because the two limiter shapes hold "
        "their locks across different critical sections and a deployment may "
        "run only one of them. Exhaustion fails closed as a denial whose "
        "retry hint is one more budget. These acquires run on the dispatcher "
        "pool, whose client-side command_timeout would otherwise truncate a "
        "budget wider than its 5.0s floor before the server-side lock_timeout "
        "could fire: widening this past the default re-derives the "
        "TaskQ-built dispatcher pool's per-query bound upward (budget / 0.8), "
        "so the wider budget is delivered end to end, the same "
        "reconciliation the client pool applies to the enqueue lock budgets. "
        "A caller-supplied dispatcher pool keeps its own timeouts; size it "
        "above the budgets you set. 0 or less waits indefinitely server-side "
        "(the lock_timeout GUC convention shared with the sibling budgets), "
        "a TaskQ-built pool still applies its per-query bound, so set a large "
        "finite value there instead.",
    )
    heartbeat_pool_size: int = Field(
        default=4,
        ge=1,
        description="TASKQ_HEARTBEAT_POOL_SIZE. Max connections for the "
        "heartbeat pool. Bypasses PgBouncer.",
    )
    heartbeat_command_timeout: float = Field(
        default=2.0,
        gt=0.0,
        description="TASKQ_HEARTBEAT_COMMAND_TIMEOUT (seconds). Per-query "
        "timeout for the heartbeat pool, and, since the single-command-budget fix round, "
        "the SINGLE command budget the heartbeat tick wraps around its "
        "whole command sequence (BEGIN, the liveness write, the lease "
        "renewals, the still-held probe, the cancel hook's statements, "
        "COMMIT, each statement's own per-query timeout stays as the "
        "inner backstop). A tick whose statements legitimately need more "
        "than one of these in total now fails fast instead of dragging "
        "the beat out past the lease model's gap bound: raise this for a "
        "loaded or cross-region Postgres, and the lease-renewal "
        "threshold's safety floor absorbs the raised value automatically "
        "(see taskq.worker.heartbeat._lease_renewal_threshold). "
        "max_heartbeat_failures consecutive timeouts self-terminate the "
        "worker. Must be > 0: asyncpg reads 0 as 'no timeout', which "
        "turns a stalled beat into a hang.",
    )
    # worker_pool max_size is derived: int(max_concurrency * 1.5)

    # -- Timing ----------------------------------------------------------
    max_concurrency: int = Field(
        default=8,
        ge=1,
        description="TASKQ_MAX_CONCURRENCY. Upper bound on concurrent jobs. "
        "worker_pool max_size = int(max_concurrency * 1.5). When a "
        "LOOP-scope asyncpg.Connection is registered, this also sizes the "
        "worker's per-slot transaction pool (max_concurrency + 1 "
        "connections, fully warmed at boot), so a change requires a "
        "worker restart and moves the direct-connection budget, see "
        "docs/guides/deployment.md. Boot-only: no reload path re-reads "
        "settings.",
    )
    heartbeat_interval: float = Field(
        default=10.0,
        ge=0.5,
        description="TASKQ_HEARTBEAT_INTERVAL (seconds). Period between heartbeat ticks.",
    )
    lock_lease: float = Field(
        default=60.0,
        ge=1.0,
        description="TASKQ_LOCK_LEASE (seconds). Time before a held lock is "
        "reclaimed by the recovery sweep. "
        "Must be >= (max_heartbeat_failures + 1) * (heartbeat_interval + "
        "2 * heartbeat_command_timeout): the lease must outlive the worst "
        "coherent failed-beat cascade to the heartbeat's isolate decision "
        "(at the defaults, 4 * (10 + 2 * 2) = 56).",
    )
    leader_lease: float = Field(
        default=40.0,
        ge=1.0,
        description="TASKQ_LEADER_LEASE (seconds). How long the maintenance "
        "leader's lease is trusted without a renewal; another pod takes "
        "leadership once it lapses. Renewed every heartbeat_interval, and "
        "never held to less than 4 of them.",
    )
    max_heartbeat_failures: int = Field(
        default=3,
        ge=1,
        description="TASKQ_MAX_HEARTBEAT_FAILURES. Consecutive heartbeat "
        "failures before the worker self-terminates. Deliberate fail-fast: "
        "at the defaults (3 failures, 2 s command timeout) roughly six "
        "seconds of Postgres unavailability ends every worker at once, and "
        "the orchestrator restarts them into a recovered database while "
        "crash reclaim re-pends their leases, expect a restart herd on a "
        "Postgres failover, sized by your replica count.",
    )

    # ── Leader sweep intervals ─────────────────────────────────
    sweep_interval: float = Field(
        default=30.0,
        ge=1.0,
        description="TASKQ_SWEEP_INTERVAL (seconds). Period between leader "
        "sweep loop iterations, reclaim_expired_locks, "
        "sweep_expired_results, cleanup_stale_workers, and idle keyed-ref "
        "eviction. Lower values reduce recovery latency for crashed workers "
        "at the cost of more frequent PG queries.",
    )
    event_writer_batch_size: int = Field(
        default=DEFAULT_EVENT_WRITER_BATCH_SIZE,
        ge=1,
        le=10_000,
        description="TASKQ_EVENT_WRITER_BATCH_SIZE. Rows per committed batch "
        "for every writer of job_events rows (the expired-lock, deadline and "
        "scheduled-to-pending sweeps, bulk cancel, actor deregistration). "
        "Keeps each batch transaction inside the "
        "reclaim_event_visibility_delay margin; the server-side "
        "statement_timeout remains the enforcement if a batch exceeds it. "
        "The loop drains the remainder across further batches/calls.",
    )
    event_writer_statement_timeout_ms: float = Field(
        default=DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS,
        ge=50.0,
        description="TASKQ_EVENT_WRITER_STATEMENT_TIMEOUT_MS (milliseconds). "
        "Server-side statement_timeout applied to each event-writer batch "
        "transaction via SET LOCAL. Defaults to 7/8 of the 2 s "
        "reclaim_event_visibility_delay margin: a batch that cannot finish "
        "inside the watermark margin is aborted by the server rather than "
        "silently corrupting reclaim-event delivery.",
    )
    event_writer_reduced_batch_divisor: int = Field(
        default=4,
        ge=2,
        le=1000,
        description="TASKQ_EVENT_WRITER_REDUCED_BATCH_DIVISOR. Divisor for "
        "the reduced event-writer batch tier: once a worker's sweeps trip "
        "the batch-size breaker, batches shrink to "
        "max(1, event_writer_batch_size / this). Only a degradation ceiling "
        ", raising it makes the degraded tier closer to the normal one.",
    )
    sweep_breaker_failure_threshold: int = Field(
        default=3,
        ge=1,
        description="TASKQ_SWEEP_BREAKER_FAILURE_THRESHOLD. Consecutive "
        "sweep-batch cancellations (within sweep_breaker_window_secs) before "
        "the batch-size breaker latches to the reduced tier for the rest of "
        "the process lifetime. Any success between failures resets the "
        "consecutive count; a latched breaker does not unlatch.",
    )
    sweep_breaker_window_secs: float = Field(
        default=600.0,
        ge=1.0,
        description="TASKQ_SWEEP_BREAKER_WINDOW_SECS (seconds). Rolling "
        "window the sweep breaker counts consecutive failures within.",
    )
    sweep_drain_batches: int = Field(
        default=8,
        ge=1,
        le=1000,
        description="TASKQ_SWEEP_DRAIN_BATCHES. Maximum event-writer batches "
        "the leader's sweep loop executes per sweep per tick before leaving "
        "the remainder to the next tick. Bounded so one iteration cannot "
        "monopolise the loop; every batch commits, so a stopped drain keeps "
        "its progress.",
    )
    event_retention_period: timedelta = Field(
        default=DEFAULT_EVENT_RETENTION_PERIOD,
        validator=_non_negative_timedelta,
        description="TASKQ_EVENT_RETENTION_PERIOD. The age at which "
        "job_events rows are deleted regardless of parent-job status. "
        "timedelta(0) DISABLES the sweep, a deliberate inversion of the "
        "prune family's zero-means-archive-immediately: for a brand-new "
        "deletion loop the safe misconfiguration is off. The "
        "crash-reclaim outbox slice (kind='state_change' AND "
        "detail->>'reason'='lock_expired') is carved out of this window so "
        "an unread reclaim event survives it, but the carve-out is bounded: "
        "the same sweep deletes it at 100x this period "
        "(RECLAIM_OUTBOX_RETENTION_MULTIPLIER), so a short retention period "
        "bounds how far behind a lagging watch_reclaims consumer may run "
        "before it silently misses events. Negative "
        "values raise at settings load.",
    )
    event_retention_batch_size: int = Field(
        default=DEFAULT_EVENT_RETENTION_BATCH_SIZE,
        ge=1,
        description="TASKQ_EVENT_RETENTION_BATCH_SIZE. job_events rows "
        "deleted per leader sweep tick, one committed batch.",
    )
    keyed_row_reclaim_period: timedelta = Field(
        default=DEFAULT_KEYED_ROW_RECLAIM_PERIOD,
        validator=_non_negative_timedelta,
        description="TASKQ_KEYED_ROW_RECLAIM_PERIOD. The idle age at which "
        "fleet-reclaimable keyed rows, keyed reservation_slots rows and "
        "PG-state-backed keyed rate_limit_buckets rows, marked by the "
        "keyed column, are deleted by the maintenance leader's "
        "sweep_idle_keyed_rows, one bounded committed batch per tick per "
        "table. Why a fleet sweep exists: keyed rows orphan when the "
        "worker that materialised them dies, because the in-process "
        "reclamation machinery (registry eviction + the pending-reclaim "
        "drain) dies with the process; the rows' own last_used_at stamp "
        "(refreshed by the acquire/release/upsert statements that already "
        "touch them) is the fleet-wide signal. Static buckets and "
        "redis-backend keyed rows are never deleted by this sweep at any "
        "setting. timedelta(0) DISABLES the sweep, the same "
        "zero-means-off sentinel event_retention_period uses, so a "
        "brand-new deletion loop's safe misconfiguration is off. "
        "Negative values raise at settings load.",
    )
    keyed_row_reclaim_batch_size: int = Field(
        default=DEFAULT_KEYED_ROW_RECLAIM_BATCH_SIZE,
        ge=1,
        description="TASKQ_KEYED_ROW_RECLAIM_BATCH_SIZE. BUCKETS (rows, "
        "for rate_limit_buckets) the fleet reclaim sweep deletes per "
        "committed batch per tick, the constant-size bound that keeps "
        "one tick's DELETE independent of the dead-worker backlog it is "
        "recovering from, the same doctrine as "
        "event_retention_batch_size.",
    )
    queue_depth_interval: float = Field(
        default=15.0,
        ge=1.0,
        description="TASKQ_QUEUE_DEPTH_INTERVAL (seconds). Period between "
        "queue-depth metrics sampling iterations.",
    )
    reservation_slots_interval: float = Field(
        default=15.0,
        ge=1.0,
        description="TASKQ_RESERVATION_SLOTS_INTERVAL (seconds). Period "
        "between reservation-slot metrics sampling iterations.",
    )
    stranded_jobs_interval: float = Field(
        default=60.0,
        ge=1.0,
        description="TASKQ_STRANDED_JOBS_INTERVAL (seconds). Period between "
        "stranded-jobs (pending jobs whose actor has no actor_config) "
        "warning checks.",
    )

    # ── Cancellation and cleanup grace periods ───────────
    termination_grace_period: float = Field(
        default=85.0,
        ge=5.0,
        description="TASKQ_TERMINATION_GRACE_PERIOD (seconds). Total wall-clock "
        "budget from SIGTERM to forced exit; the shutdown watchdog counts "
        "it down from the first shutdown signal. Must satisfy "
        "cancellation_grace + cleanup_grace < termination_grace - 5, and "
        "should cover the modelled worst case cancellation_grace + "
        "cleanup_grace + the ~42s bounded-close teardown tail (see "
        "WorkerSettings.worst_case_shutdown_seconds), the default does: "
        "30 + 10 + 42 = 82s. The ~87s sibling-crash path (nine closes, "
        "including the conditional per-slot pool) exceeds the default by "
        "2s on per-slot workers, that path is the documented caveat the "
        "model understates; raise this setting when per-slot workers need "
        "a tight crash budget. A value below the worst case still "
        "loads but logs shutdown-budget-exceeds-termination-grace at "
        "startup. Size the pod/container grace "
        "(terminationGracePeriodSeconds / stop_grace_period) from the "
        "same worst case, not from this setting alone.",
    )
    cancellation_grace_period: float = Field(
        default=30.0,
        ge=0.0,
        description="TASKQ_CANCELLATION_GRACE_PERIOD (seconds). Cooperative cancel phase duration.",
    )
    cleanup_grace_period: float = Field(
        default=10.0,
        ge=0.0,
        description="TASKQ_CLEANUP_GRACE_PERIOD (seconds). Force-cancel cleanup grace.",
    )
    reclaim_event_visibility_delay: float = Field(
        default=RECLAIM_EVENT_VISIBILITY_DELAY.total_seconds(),
        ge=0.0,
        description="TASKQ_RECLAIM_EVENT_VISIBILITY_DELAY (seconds). Trailing-watermark "
        "margin poll_reclaim_events()/TaskQ.watch_reclaims() apply before returning a "
        "job_events row, so an out-of-commit-order sibling with a lower event_id has "
        "time to appear first (see docs/architecture.md's crash-reclaim section). "
        "Correctness assumes every job_events writer transaction commits within this "
        "margin of its INSERT; raise it if sweeps run under heavy lock contention or "
        "against very large batches, lower it if latency matters more and writes are "
        "known to be fast. A writer that exceeds the margin can cause a silently "
        "missed event - this is a real, not merely theoretical, risk under misconfiguration.",
    )

    # -- Retry backoff ceiling -------------------------------------------
    max_retry_backoff: timedelta = Field(
        default=DEFAULT_MAX_RETRY_BACKOFF,
        description=(
            "TASKQ_MAX_RETRY_BACKOFF (interval). Global ceiling on retry backoff "
            "per attempt - caps the per-actor RetryPolicy.cap so a misconfigured "
            "actor (e.g. cap=timedelta(days=365)) cannot strand jobs for an "
            "unreasonably long time. Default 24 h: conservative, aligns with a "
            "standard on-call rotation period"
        ),
    )

    default_start_to_close: timedelta | None = Field(
        default=None,
        validator=_positive_timedelta,
        description=(
            "TASKQ_DEFAULT_START_TO_CLOSE (interval). Worker-side fallback "
            "per-attempt execution timeout, applied only when a job has no "
            "start_to_close of its own (neither passed at enqueue time nor "
            "declared as an @actor(start_to_close=...) default). None (the "
            "default) means unbounded - matches existing behaviour, opt-in "
            "only. Set this to give every actor on this worker a safety-net "
            "wall-clock budget per attempt, preventing a hung or "
            "infinite-looping actor from occupying a coroutine slot forever, "
            "without having to configure start_to_close on every individual "
            "actor. Precedence (highest wins): per-enqueue start_to_close > "
            "@actor(start_to_close=...) > this setting. This does not affect "
            "schedule_to_close, which is a separate, unrelated deadline for "
            "the job's *overall* retry budget across all attempts - "
            "start_to_close bounds a single attempt's wall-clock time."
        ),
    )

    # -- Rate limit ------------------------------------------------
    rate_limit_pg_fallback_enabled: bool = Field(
        default=True,
        description="TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED. When False, Redis "
        "errors propagate instead of triggering PG fallback.",
    )
    max_keyed_reservations: int = Field(
        default=DEFAULT_MAX_KEYED_RESERVATIONS,
        ge=1,
        description="TASKQ_MAX_KEYED_RESERVATIONS. Guardrail on the number of "
        "distinct keyed-reservation entries tracked in memory. When the limit "
        "is reached, new keyed reservations raise ReservationUnavailable. "
        "Tune to your workload's expected key cardinality: the guardrail is "
        "PER PROCESS, so adding replicas does not raise the effective "
        "tenant-key fleet capacity, a tenant fleet above the limit gets "
        "denials on every replica at once.",
    )
    max_keyed_rate_limits: int = Field(
        default=10000,
        ge=1,
        description="TASKQ_MAX_KEYED_RATE_LIMITS. Guardrail on the number of "
        "distinct keyed-rate-limit entries tracked in memory. When the limit "
        "is reached, new keyed rate limits raise ReservationUnavailable. "
        "Independent from max_keyed_reservations, which governs keyed "
        "reservations only. Tune to your workload's expected key cardinality; "
        "like max_keyed_reservations the guardrail is per process and does "
        "not scale with the replica count.",
    )

    # -- Prometheus standalone metrics server ------------------
    metrics_port: int | None = Field(
        default=None,
        ge=1,
        le=65535,
        description="TASKQ_METRICS_PORT. TCP port for the worker's standalone "
        "Prometheus scrape listener, bound on TASKQ_METRICS_HOST (falling back "
        "to TASKQ_HEALTH_HOST). Unset (the "
        "default) means no listener, so setting a port is the opt-in, the same "
        "shape as TASKQ_HEALTH_PORT. Needs the [prometheus] extra and "
        "TASKQ_OTEL_AUTOCONFIGURE=true: the `taskq worker` CLI then adds a "
        "PrometheusMetricReader to the SDK meter provider it installs, so "
        "every taskq_* / messaging_* series this worker records, the "
        "leader-sampled gauges and the dispatch/consume counters the admin "
        "process never sees, is served at http://<host>:<port>/metrics. "
        "See observability.md, Serving the metrics.",
    )

    # -- Health server ------------------------------------------
    health_enabled: bool = Field(
        default=True,
        description="TASKQ_HEALTH_ENABLED. Enable the health server: both the "
        "Unix socket and the optional TCP listener (health_port). False "
        "disables both, and a health_port set alongside it is not honoured, "
        "so probes against that port fail.",
    )
    health_socket_path: str = Field(
        default="/tmp/taskq_health.sock",  # noqa: S108  # Why: default. Production deployments override via env var (typically /run/taskq.sock under tmpfs).
        description="TASKQ_HEALTH_SOCKET_PATH. Unix socket path for the health "
        "server, serving /live, /ready, /metrics and the opt-in /tasks "
        "endpoint. Give each co-located process a unique path: a path whose "
        "live peer holds it fails to bind, and that collision is a WARNING "
        "with the boot continuing (a live peer owning the path is a rolling-"
        "restart shape, not a failure); see health_port for the TCP arm's "
        "different contract.",
    )
    health_pg_ping_timeout: float = Field(
        default=0.2,
        ge=0.0,
        description="TASKQ_HEALTH_PG_PING_TIMEOUT. Seconds to wait for "
        "dispatcher_pool.acquire() in the readiness PG ping. "
        "Default 200ms .",
    )
    health_tasks_enabled: bool = Field(
        default=False,
        description="TASKQ_HEALTH_TASKS_ENABLED. Expose the privileged "
        "/tasks asyncio stack-dump endpoint on the Unix health socket. "
        "Off by default: the dump reveals code structure, file paths, and "
        "task names (never locals or payload values). Enabling it also "
        "tightens the socket to owner-only (no group/other access). Unix socket only, "
        "never mounted on the admin UI surface.",
    )
    health_host: str = Field(
        default="0.0.0.0",  # noqa: S104  # Why: a container probe reaches the replica over the pod network, so a loopback bind would be unprobeable. Only ever bound when health_port or metrics_port is explicitly set.
        description="TASKQ_HEALTH_HOST. Bind address for the optional TCP health "
        "listener and the optional Prometheus scrape listener. Only used when "
        "health_port or metrics_port is set. Defaults to all interfaces "
        "because Azure Container Apps and Kubernetes probe and scrape the replica "
        "over the pod network; narrow it to 127.0.0.1 when a local sidecar is the "
        "only prober.",
    )
    metrics_host: str | None = Field(
        default=None,
        description="TASKQ_METRICS_HOST. Bind address for the optional Prometheus "
        "scrape listener, overriding TASKQ_HEALTH_HOST for that listener alone, so "
        "the scrape and the probes can sit on different interfaces (a loopback "
        "sidecar scraper next to a pod-network probe is the shape that needs "
        "this). Unset falls back to TASKQ_HEALTH_HOST. Only used when "
        "metrics_port is set.",
    )
    health_port: int | None = Field(
        default=None,
        ge=0,
        le=65535,
        description="TASKQ_HEALTH_PORT. TCP port for the HTTP health listener serving "
        "/live and /ready. Unset (the default) means no TCP listener at all, so setting a "
        "port is the opt-in. Required on Azure Container Apps, whose probes support only "
        "httpGet/tcpSocket and cannot reach a Unix socket (there is no exec probe type). "
        "The Unix socket keeps working either way. If the port cannot be bound the worker "
        "fails to start with HealthTcpBindError rather than run with probes silently dead: "
        "the orchestrator routes this replica's probes here, and a tcpSocket probe against "
        "a port some other process holds would pass while "
        "probing the wrong process). The unix socket's collision is "
        "deliberately softer: a live peer owns the path, so the boot warns "
        "and continues. 0 binds an ephemeral "
        "port (tests only).",
    )
    health_request_timeout: float = Field(
        default=2.0,
        gt=0.0,
        description="TASKQ_HEALTH_REQUEST_TIMEOUT. Seconds allowed for a probe to send its "
        "whole request line and headers before the connection is dropped unanswered. "
        "Bounds a drip-feed client that would otherwise hold a connection open forever by "
        "staying just inside a per-line timeout. Keep it at or below the shortest probe "
        "timeoutSeconds you configure.",
    )
    health_max_header_bytes: int = Field(
        default=16 * 1024,
        gt=0,
        description="TASKQ_HEALTH_MAX_HEADER_BYTES. Cap on a probe request's accumulated "
        "request line plus headers. Pairs with health_request_timeout to bound a peer that "
        "sends many small lines fast enough to stay inside the deadline. 16 KiB is far "
        "above any real probe request, which carries a path and a handful of headers.",
    )
    health_readiness_check_timeout: float = Field(
        default=5.0,
        gt=0.0,
        description="TASKQ_HEALTH_READINESS_CHECK_TIMEOUT. Seconds each check registered "
        "via taskq.worker.health.register_readiness_check may take before it counts as a "
        "readiness failure. Defaults to 5s, matching the Azure Container Apps default "
        "readiness probe timeoutSeconds, so a wedged check fails the probe rather than "
        "outliving it.",
    )

    # ── In-worker watchdog (hang/deadlock detection) ────────────
    watchdog_enabled: bool = Field(
        default=True,
        description="TASKQ_WATCHDOG_ENABLED. Master switch for the in-worker "
        "watchdog detectors (shutdown deadline, stale loop ticks, sibling "
        "contract, event-loop lag). A detector trip dumps the asyncio task "
        "stacks and force-exits non-zero so the supervisor restarts the "
        "worker instead of leaving it wedged.",
    )
    watchdog_loop_lag_budget: float = Field(
        default=30.0,
        gt=0.0,
        description="TASKQ_WATCHDOG_LOOP_LAG_BUDGET (seconds). How long the "
        "event loop may go without scheduling before the lag watchdog trips. "
        "Deliberately far beyond any legitimate pause (GC, a slow tick) "
        "because the trip is terminal. Tier 2 of the lag detector; see "
        "watchdog_loop_lag_warn_budget for the non-terminal tier 1.",
    )
    watchdog_loop_lag_warn_budget: float = Field(
        default=5.0,
        gt=0.0,
        description="TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET (seconds). Non-terminal "
        "tier-1 event-loop lag threshold: faulthandler thread dump + metric "
        "+ deferred asyncio task-stack dump. Never exits; the terminal tier "
        "is watchdog_loop_lag_budget.",
    )
    watchdog_loop_lag_startup_grace: float = Field(
        default=30.0,
        ge=0.0,
        description="TASKQ_WATCHDOG_LOOP_LAG_STARTUP_GRACE (seconds). Grace "
        "before the lag watchdog arms, covering import-heavy startup, DI "
        "bootstrap, and first dispatch. Anchored to thread start; the lag "
        "detector also arms early once the first loop liveness tick lands.",
    )
    watchdog_tick_grace_factor: float = Field(
        default=5.0,
        gt=0.0,
        description="TASKQ_WATCHDOG_TICK_GRACE_FACTOR. Multiplier on a "
        "loop's iteration period before its liveness tick is declared "
        "stale (floor 10s). Generous on purpose: a terminal detector must "
        "never fire on a merely loaded host.",
    )
    watchdog_dump_interval: float = Field(
        default=5.0,
        gt=0.0,
        description="TASKQ_WATCHDOG_DUMP_INTERVAL (seconds). Interval "
        "between straggler logs (names + await sites of still-alive "
        "siblings) while a shutdown is in progress.",
    )
    watchdog_dump_after_fraction: float = Field(
        default=0.5,
        gt=0.0,
        lt=1.0,
        description="TASKQ_WATCHDOG_DUMP_AFTER_FRACTION. Fraction of the "
        "shutdown deadline that must be consumed before straggler dumps "
        "begin (0.5 = only in the back half of the budget). A drain inside "
        "its front half is within expectations and stays quiet; one "
        "countdown-start record is always logged so the window is never "
        "blind. Must be < 1: at 1.0 the deadline trip would always fire "
        "first, silently disabling the dumps.",
    )
    watchdog_stale_floor: float = Field(
        default=10.0,
        gt=0.0,
        description="TASKQ_WATCHDOG_STALE_FLOOR (seconds). Minimum "
        "staleness budget for any loop (period x grace_factor, floored at "
        "this value). Guards tiny intervals against false trips under "
        "host starvation, a terminal detector must never fire on load.",
    )
    watchdog_check_interval: float = Field(
        default=1.0,
        gt=0.0,
        description="TASKQ_WATCHDOG_CHECK_INTERVAL (seconds). Poll cadence "
        "for the stale-tick sweep and the loop-lag watchdog thread.",
    )

    # -- Polling and NOTIFY listener ------------------------
    poll_interval: float = Field(
        default=1.0,
        gt=0,
        description="TASKQ_POLL_INTERVAL (seconds). Producer loop fallback "
        "polling cadence when the NOTIFY listener is unavailable.",
    )
    notify_health_check_interval: float = Field(
        default=5.0,
        gt=0,
        description="TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL (seconds). How often "
        "_health_check_loop issues SELECT 1 on notify_conn. "
        "Detection latency before reconnect is at most this interval.",
    )
    notify_reconnect_backoff_initial: float = Field(
        default=1.0,
        gt=0,
        description="TASKQ_NOTIFY_RECONNECT_BACKOFF_INITIAL (seconds). "
        "Initial exponential backoff delay before the first reconnect "
        "retry. Cap is 30 s (factor 2 per attempt). "
        "Backoff sequence: 1, 2, 4, 8, 16, 30.",
    )
    notify_listener_setup_timeout: float = Field(
        default=10.0,
        validator=_positive_finite_float,
        description="TASKQ_NOTIFY_LISTENER_SETUP_TIMEOUT (seconds). Bounds "
        "each ``LISTEN`` execute and ``add_listener`` call during NOTIFY "
        "listener setup and reconnect - a half-open PG connection that "
        "accepts TCP (or completes the reconnect factory handshake) but "
        "stalls on the LISTEN execute or registration would otherwise "
        "wedge the notify loop forever. On timeout the connection is "
        "closed (bounded) and the reconnect retry loop is entered (or "
        "the initial setup raises).",
    )
    notify_enabled: bool = Field(
        default=True,
        description="TASKQ_NOTIFY_ENABLED. When True, the worker uses "
        "LISTEN/NOTIFY for near-zero-latency dispatch wakeups with poll "
        "interval as fallback. When False, the worker uses poll-only dispatch.",
    )
    notify_poll_interval: float = Field(
        default=5.0,
        ge=0.5,
        description="TASKQ_NOTIFY_POLL_INTERVAL (seconds). Fallback poll "
        "cadence when NOTIFY is enabled (rarely reached - NOTIFY handles "
        "the common case). Use poll_interval when NOTIFY is disabled.",
    )

    # -- Credential hot-reload --------------------------------------------
    # reload_interval lives on TaskQSettings: the worker and `taskq ui serve`
    # both rebuild provider-backed pools on it.
    reload_factory_timeout: float = Field(
        default=30.0,
        gt=0,
        description="TASKQ_RELOAD_FACTORY_TIMEOUT (seconds). Bounds each "
        "individual factory call - during a credential hot-reload "
        "(reload_credentials), at bootstrap when the worker opens its "
        "per-slot transaction pool (a fully-warmed open means one "
        "connection - and on a managed-identity deployment one "
        "credential fetch - per consumer slot), on the notify "
        "listener's health-check reconnect (reconnect_notify_conn), and "
        "at the DI scope bootstraps' first use of user-registered "
        "factories (the 'database pools, HTTP clients' provider class, "
        "resolved through ScopeContainer.get_or_create before any "
        "watchdog is armed). A hung token endpoint - or a black-holed "
        "DI factory - is marked failed for that resource - or "
        "logged as a reconnect attempt and retried - instead of wedging "
        "the reload coordinator, worker boot, or the reconnect loop.",
    )

    # -- Queue selection --------------------------------------------------
    queues: list[str] = Field(
        default_factory=lambda: ["default"],
        validator=_queue_names_validator,
        description="TASKQ_QUEUES. Comma-separated list of queue names "
        "this worker will consume from.",
    )

    worker_label: str | None = Field(
        default=None,
        validator=_worker_label_validator,
        description="TASKQ_WORKER_LABEL. Human-readable label stored in the "
        "workers table for correlation with workgroup supervisors and external "
        "monitoring. When omitted the column is NULL; hostname and pid columns "
        "provide identification.",
    )
    workgroup_instance: str | None = Field(
        default=None,
        validator=_workgroup_instance_validator,
        description="TASKQ_WORKGROUP_INSTANCE. UUIDv7 identifying the workgroup "
        "orchestrator that launched this worker. Used for cross-process correlation.",
    )

    # -- Pool lifecycle --------------------------------------------------
    pool_max_inactive_lifetime: float = Field(
        default=300.0,
        ge=0.0,
        description="TASKQ_POOL_MAX_INACTIVE_LIFETIME (seconds). asyncpg "
        "max_inactive_connection_lifetime - closes connections idle "
        "longer than this threshold. Set to 3600.0 to match a typical "
        "SQLAlchemy pool_recycle=3600 setting when running alongside "
        "an SQLAlchemy-based service. Applied to dispatcher_pool, "
        "heartbeat_pool, worker_pool, and the conditional per-slot "
        "transaction pool.",
    )

    # -- Observability --------------------------------------------
    otel_enabled: bool = Field(
        default=True,
        description="TASKQ_OTEL_ENABLED. When False, the library suppresses all span "
        "and metric creation but operations still succeed .",
    )
    otel_autoconfigure: bool = Field(
        default=True,
        description="TASKQ_OTEL_AUTOCONFIGURE. When True (the default), the `taskq "
        "worker` CLI installs SDK tracer and meter providers from the standard "
        "OTel environment variables (OTEL_EXPORTER_OTLP_ENDPOINT, "
        "OTEL_TRACES_EXPORTER, OTEL_METRICS_EXPORTER, OTEL_LOGS_EXPORTER) and "
        "from TASKQ_METRICS_PORT, through the same configurator "
        "opentelemetry-instrument uses, whenever the [otel] extra is installed "
        "and no provider is set yet. Set False when the embedding application "
        "or a vendor distro configures the SDK itself and the worker must not "
        "touch the global providers. Honours OTEL_SDK_DISABLED either way.",
    )
    exception_message_max_chars: int = Field(
        default=2000,
        ge=100,
        description="TASKQ_EXCEPTION_MESSAGE_MAX_CHARS. Bound on exception "
        "message text on spans and logs, after scrubbing. Matches the admin "
        "UI's traceback bound so there is one number for how much error text "
        "is kept, not two. Truncation appends the dropped character count, so "
        "an operator can see text was cut and raise this. Raise it when an "
        "actor formats large context into its messages; the stack trace is a "
        "separate field and is not bounded by this.",
    )
    exception_redaction_enabled: bool = Field(
        default=True,
        description="TASKQ_EXCEPTION_REDACTION_ENABLED. When True (the default), "
        "Postgres 'DETAIL:' lines are dropped from exception text before it "
        "reaches spans and logs, because they quote caller-supplied row values "
        "(idempotency_key, identity_key, fairness_key routinely hold tenant or "
        "subject identifiers). Set to False for advanced debugging to ship the "
        "raw text, including those row values, to every configured telemetry "
        "backend; the worker logs a startup WARNING while it is off. URI "
        "credential masking (scheme://user:***@host) is NOT affected by this "
        "setting and is always applied - no debugging case justifies sending a "
        "password to a telemetry vendor.",
    )
    worker_group: str = Field(
        default="default",
        description="TASKQ_WORKER_GROUP. Consumer group name emitted as "
        "messaging.consumer.group.name on CONSUMER spans .",
    )
    log_format: str = Field(
        default="json",
        validator=_log_format_validator,
        description="TASKQ_LOG_FORMAT. json|console. Selects JSONRenderer or ConsoleRenderer "
        "in setup_logging.",
    )
    log_level: str = Field(
        default="INFO",
        validator=_log_level_validator,
        description="TASKQ_LOG_LEVEL. Root logger level.",
    )

    # -- Pruning schedule --------------------------------------------
    prune_schedule_utc: str = Field(
        default="03:00",
        validator=_hh_mm_validator,
        description="TASKQ_PRUNE_SCHEDULE_UTC. HH:MM (UTC) for the daily prune "
        "run. Ignored when prune_cron_expr is set.",
    )
    prune_cron_expr: str | None = Field(
        default=None,
        validator=_cron_expr_validator,
        description="TASKQ_PRUNE_CRON_EXPR. Full 5-field cron expression. When "
        "set, takes precedence over prune_schedule_utc.",
    )
    prune_batch_size: int = Field(
        default=DEFAULT_PRUNE_BATCH_SIZE,
        ge=1,
        description="TASKQ_PRUNE_BATCH_SIZE. Rows to delete per batch.",
    )

    # -- Per-status prune retention --------------------------------
    prune_retention_period: timedelta = Field(
        default=DEFAULT_PRUNE_RETENTION,
        validator=_non_negative_timedelta,
        description="TASKQ_PRUNE_RETENTION_PERIOD. Reserved as the global "
        "fallback retention for terminal statuses without a per-status "
        "knob. Currently INERT: the prune sweep reads the four per-status "
        "fields below, and together they cover every terminal status "
        "(succeeded, failed, cancelled, crashed, abandoned), so no status "
        "ever falls back here, setting this value changes nothing today. "
        "Size the per-status fields instead; see "
        "TASKQ_PRUNE_RETENTION_SUCCEEDED. Negative values raise "
        "ConstraintViolationError at settings load.",
    )
    prune_retention_succeeded: timedelta = Field(
        default=timedelta(days=30),
        validator=_non_negative_timedelta,
        description="TASKQ_PRUNE_RETENTION_SUCCEEDED. How long a succeeded "
        "job stays in the hot jobs table before the daily prune sweep "
        "moves it to jobs_archive (where archive_retention_period then "
        "governs hard-deletion, 365 d by default, so history is not lost "
        "at prune time). Sizing is a hot-table trade: succeeded rows are "
        "usually the bulk of terminal volume, and every day of retention "
        "keeps roughly a day's terminal throughput in the hot table the "
        "admin /jobs list reads (at 100k jobs/day the default 30 d holds "
        "~3M rows, see the storage-planning note in configuration.md). "
        "Lower it for high-volume actors whose "
        "successes nobody audits (the per-actor metadata retention_days "
        "override shortens it further for one actor); raise it when "
        "operators routinely inspect successful runs older than a month "
        "without querying the archive. timedelta(0) archives succeeded "
        "jobs at the next sweep, the prune family's zero-means-now "
        "polarity, deliberately opposite to the sweep family's "
        "zero-means-off (see the 0 convention in configuration.md). "
        "Negative values raise ConstraintViolationError at settings load.",
    )
    prune_retention_failed: timedelta = Field(
        default=timedelta(days=90),
        validator=_non_negative_timedelta,
        description="TASKQ_PRUNE_RETENTION_FAILED. How long a failed job "
        "stays in the hot jobs table before the daily prune sweep moves it "
        "to jobs_archive. Failed rows are the first incident-audit trail, "
        "they carry error_class, error_message and the attempt history, "
        "so the default keeps them hot three times longer than succeeded "
        "rows (90 d vs 30 d). Size to how far back your on-call reads "
        "failures in the fast surfaces (admin /jobs) before the archive is "
        "acceptable; lower it only if failure volume makes the hot table's "
        "size the bigger incident risk. timedelta(0) archives failed jobs "
        "at the next sweep (zero-means-now, see the 0 convention in "
        "configuration.md). Negative values raise ConstraintViolationError "
        "at settings load.",
    )
    prune_retention_cancelled: timedelta = Field(
        default=timedelta(days=30),
        validator=_non_negative_timedelta,
        description="TASKQ_PRUNE_RETENTION_CANCELLED. How long a cancelled "
        "job stays in the hot jobs table before the daily prune sweep "
        "moves it to jobs_archive. Cancelled rows are operator- or "
        "deadline-initiated and rarely revisited after the fact, so the "
        "default follows succeeded (30 d); raise it if cancellations are "
        "part of your audit story, lower it toward 0 for bulk-cancel "
        "workloads whose rows are pure churn. timedelta(0) archives "
        "cancelled jobs at the next sweep (zero-means-now, see the 0 "
        "convention in configuration.md). Negative values raise "
        "ConstraintViolationError at settings load.",
    )
    prune_retention_abandoned: timedelta = Field(
        default=timedelta(days=90),
        validator=_non_negative_timedelta,
        description="TASKQ_PRUNE_RETENTION_ABANDONED. How long an abandoned "
        "job stays in the hot jobs table before the daily prune sweep "
        "moves it to jobs_archive. Also used for crashed jobs (no separate "
        "prune_retention_crashed field): both statuses mean the job "
        "outlived its execution budget or its worker, and both are the "
        "rows you reach for when reconstructing a fleet-level incident, so "
        "they share the longer 90 d default with failed. Same sizing trade "
        "and zero-means-now polarity as the sibling fields, see "
        "TASKQ_PRUNE_RETENTION_SUCCEEDED. Negative values raise "
        "ConstraintViolationError at settings load.",
    )

    # -- Archive retention & expiry schedule ----------------------
    archive_retention_period: timedelta = Field(
        default=timedelta(days=365),
        validator=_non_negative_timedelta,
        description="TASKQ_ARCHIVE_RETENTION_PERIOD. How long archived jobs are "
        "retained in jobs_archive before hard-deletion. Default 1 year. "
        "timedelta(0) hard-deletes an archived row at the next "
        "archive-expiry sweep, the row's expire_at is stamped "
        "archive-time plus this period, so a zero period expires it on "
        "arrival; the prune family's zero-means-now polarity, not the "
        "deletion-sweep family's zero-means-off (see the 0 convention in "
        "configuration.md). Negative values raise ConstraintViolationError.",
    )
    archive_expiry_schedule_utc: str = Field(
        default="04:00",
        validator=_hh_mm_validator,
        description="TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC. HH:MM (UTC) for the "
        "daily archive expiry sweep. Default 04:00, 1 hour after the "
        "prune sweep.",
    )
    archive_expiry_cron_expr: str | None = Field(
        default=None,
        validator=_cron_expr_validator,
        description="TASKQ_ARCHIVE_EXPIRY_CRON_EXPR. Full 5-field cron "
        "expression. When set, takes precedence over "
        "archive_expiry_schedule_utc.",
    )

    # -- Actor config drift handling ---------------------------------------
    force_update_actor_config: bool = Field(
        default=False,
        description=(
            "When True, sync_actor_config silently overwrites a stored "
            "actor_config row whose metadata differs from the registered "
            "value. When False (the default), metadata drift raises "
            "ActorConfigDriftList and the worker refuses to start. The "
            "queue assignment and the capacity fields (max_concurrent, "
            "max_pending, result_ttl) are unaffected by this flag: once a "
            "row exists, the stored value is always authoritative and is "
            "never overwritten by the registered @actor(...) literal, "
            "regardless of force. Move an actor between queues with "
            "`taskq actor-config move-queue`; tune a stored capacity value "
            "with `taskq actor-config set`. Env var: "
            "TASKQ_FORCE_UPDATE_ACTOR_CONFIG."
        ),
    )

    # -- Progress fanout --------------------------------------------
    progress_coalesce_interval: float = Field(
        default=0.5,
        ge=0.1,
        description="TASKQ_PROGRESS_COALESCE_INTERVAL (seconds). How long the "
        "periodic flush loop waits between writing coalesced progress state "
        "to Postgres. Redis publishes are not throttled by this setting - "
        "each ctx.progress() call publishes immediately (fire-and-forget). "
        "Lower values increase PG write frequency; minimum 0.1 s.",
    )
    progress_data_max_bytes: int = Field(
        default=16384,
        ge=1024,
        le=1048576,
        description="TASKQ_PROGRESS_DATA_MAX_BYTES. Maximum serialised byte "
        "length of the ``data`` dict in a single progress call. Payloads "
        "exceeding this limit raise ProgressTooLarge . "
        "Range: 1 KiB - 1 MiB; default 16 KiB.",
    )
    progress_publish_global: bool = Field(
        default=True,
        description="TASKQ_PROGRESS_PUBLISH_GLOBAL. When True (the default), "
        "progress events are additionally published to a schema-wide global "
        "fanout channel (in addition to the per-job channel). When False, "
        "events are only published to the per-job Redis channel. "
        "Does not affect Postgres flushing.",
    )

    # -- Job results ------------------------------------------------
    result_max_bytes: int = Field(
        default=MAX_RESULT_BYTES,
        ge=1024,
        le=1048576,
        description="TASKQ_RESULT_MAX_BYTES. Maximum serialised byte length of "
        "a job's terminal result dict. A larger result raises ResultTooLarge, "
        "which is non-retryable, the actor already ran, so a re-run returns "
        "the same oversized value. Range: 1 KiB - 1 MiB (the same ceiling as "
        "progress_data_max_bytes, so the durable payload can be configured as "
        "large as the transient one); default 64 KiB. Raise it only with the "
        "row size in mind: unlike progress data, the result is stored for the "
        "job's result_ttl.",
    )

    # -- Cron scheduler --------------------------------------------
    cron_catch_up_window: timedelta = Field(
        default=timedelta(hours=1),
        validator=_non_negative_timedelta,
        description="TASKQ_CRON_CATCH_UP_WINDOW. Missed firings within this "
        "window are caught up sequentially; older misses are skipped.",
    )
    cron_auto_disable_threshold: int = Field(
        default=3,
        ge=1,
        description="TASKQ_CRON_AUTO_DISABLE_THRESHOLD. Consecutive failures "
        "before a schedule is auto-disabled.",
    )
    cron_tick_limit: int = Field(
        default=DEFAULT_EVENT_WRITER_BATCH_SIZE,
        ge=1,
        le=10_000,
        description="TASKQ_CRON_TICK_LIMIT. Maximum schedules one cron tick "
        "selects, plans and fires. A catch-up burst larger than this drains "
        "across successive one-second ticks instead of one oversized "
        "transaction; the remainder stays due and untouched until its tick. "
        "Only the leader plans ticks, so this guardrail does not scale with "
        "the replica count: raise it when one tick's share of schedules "
        "genuinely exceeds it, or spread schedules off the second boundary.",
    )
    cron_payload_factory_timeout: float = Field(
        default=5.0,
        validator=_positive_finite_float,
        description="TASKQ_CRON_PAYLOAD_FACTORY_TIMEOUT. Per-call deadline "
        "for a cron schedule's payload factory (both the off-loop call and "
        "the coroutine a factory returns). Default 5.0s. The tick clamps it "
        "to stay strictly inside what is left of the leader's whole-tick "
        "deadline (dispatcher_command_timeout), so a factory that runs and "
        "outlives its granted deadline takes the named per-schedule failure "
        "this setting exists to record, never the whole-tick cancellation; "
        "a value at or above that deadline is therefore an upper bound, not "
        "the effective one. A factory is called only when the leftover can "
        "fund at least min(this value, a quarter of the tick's funded "
        "budget), a smaller leftover funds no call and the schedule is "
        "deferred (next_fire_at advances one tick cadence) rather than "
        "struck: the factory never ran, so there is no evidence against the "
        "schedule, only the schedule whose factory consumed the budget is "
        "failing. That also makes this setting the fairness lever when one "
        "slow-but-successful factory monopolizes the tick budget every tick "
        "(its peers defer indefinitely, watch taskq.cron.budget_deferrals "
        "and the cron-fire-budget-deferred log): set it BELOW the "
        "monopolizing factory's real duration and that factory takes the "
        "strike-and-auto-disable path instead, freeing its peers; raising "
        "dispatcher_command_timeout widens the funded budget the same "
        "resolution needs.",
    )

    # ── Until-idle drain mode ────────────────────────────────────────────
    idle_settle_window: float = Field(
        default=2.0,
        ge=0.0,
        description=(
            "TASKQ_IDLE_SETTLE_WINDOW (seconds). Time the drain monitor "
            "waits after queues appear empty before declaring drained. "
            "Only used when --until-idle is active."
        ),
    )
    idle_poll_interval: float = Field(
        default=1.0,
        ge=0.1,
        description=(
            "TASKQ_IDLE_POLL_INTERVAL (seconds). How often the drain "
            "monitor checks queue depth. Only used when --until-idle is active."
        ),
    )
    idle_max_runtime: float | None = Field(
        default=None,
        gt=0,
        description=(
            "TASKQ_IDLE_MAX_RUNTIME (seconds). Maximum wall-clock time "
            "for until-idle mode. When exceeded, exit code 4. None = no limit. "
            "Only used when --until-idle is active."
        ),
    )

    @property
    def resolved_leader_lease(self) -> float:
        """The maintenance lease actually honoured, in seconds.

        The lease is renewed once per heartbeat interval, so one shorter
        than four of them would demote a leader whose renewal is merely a
        tick behind, leadership would churn on every slow tick. Raising
        ``heartbeat_interval`` alone must not produce that, and must not
        refuse to boot either: a fleet that can no longer be told what to
        do is worse than one running a longer lease than it asked for. The
        configured value is therefore a floor that the same four-beat slack
        the jobs' lock leases carry can raise, never a ceiling.
        """
        return max(self.leader_lease, 4 * self.heartbeat_interval)

    @property
    def resolved_pg_dsn_direct(self) -> PostgresDsn:
        """Direct DSN guaranteed non-``None`` after :meth:`post_load`.

        Why a property: ``pg_dsn_direct: PostgresDsn | None`` carries the
        environment-shape that distinguishes "user did not set
        ``TASKQ_PG_DSN_DIRECT``" (``None``, fallback to ``pg_dsn``) from
        "user set it explicitly". Once :meth:`post_load` has applied the
        fallback, the field is always non-``None`` - but pyright cannot
        prove that across method boundaries. This property re-asserts the
        invariant at every call site, eliminating the need for ``assert``
        or ``cast`` at call sites that read the DSN.

        Raises :class:`RuntimeError` if accessed before :meth:`post_load`
        ran (signals a programming error: ``WorkerSettings()`` constructor
        must always go through :meth:`load` / :meth:`load_from_dict`).
        """
        if self.pg_dsn_direct is None:
            raise RuntimeError(
                "pg_dsn_direct accessed before post_load(); "
                "construct WorkerSettings via load()/load_from_dict()",
            )
        return self.pg_dsn_direct

    @property
    def resolved_pg_dsn_pooled(self) -> PostgresDsn:
        """Pooled DSN guaranteed non-``None`` after :meth:`post_load`.

        See :attr:`resolved_pg_dsn_direct` for the rationale.
        """
        if self.pg_dsn_pooled is None:
            raise RuntimeError(
                "pg_dsn_pooled accessed before post_load(); "
                "construct WorkerSettings via load()/load_from_dict()",
            )
        return self.pg_dsn_pooled

    def post_load(self) -> list[ValidationError] | None:
        """Apply DSN fallback and validate cross-field invariants after loading.

        Runs automatically on every load path (``load()``,
        ``load_from_dict()``, ``reload()``, and nested config loading),
        including under ``validate=False`` - consistent with the per-field
        ``validator`` hooks (transformation is part of loading, not
        validation). No ``WorkerSettings.load`` / ``load_from_dict``
        override is needed; the base ``DotEnvConfig._load_fields`` invokes
        this hook itself.

        Returns ``list[ValidationError]`` so failures integrate with
        dotenvmodel's uniform error hierarchy: a single returned error is
        raised unchanged (its exact type preserved), several aggregate
        into ``MultipleValidationErrors``. Catch ``DotEnvModelError`` (the
        common base) to cover both single and aggregate cases -
        ``MultipleValidationErrors`` is a ``DotEnvModelError`` but not a
        ``ValidationError``, so ``except ValidationError`` alone misses the
        multi-invariant case. ``ValidationError`` suffices only when at
        most one invariant can fire (e.g. a single field constraint).
        """
        errors: list[ValidationError] = []

        # DSN fallback: if split DSNs were not provided, resolve to pg_dsn.
        # After this, pg_dsn_direct and pg_dsn_pooled are always non-None.
        if self.pg_dsn_direct is None:
            self.pg_dsn_direct = self.pg_dsn
        if self.pg_dsn_pooled is None:
            self.pg_dsn_pooled = self.pg_dsn

        # lock_lease invariant: the lease must outlive the worst coherent
        # failed-beat cascade. A heartbeat tick's beat-to-beat gap is
        # bounded, with the per-tick command budget/ enforced,
        # by heartbeat_interval (the pool acquire's own timeout) + ONE
        # heartbeat_command_timeout (the tick's whole command sequence) + ONE
        # heartbeat_command_timeout (the bounded rollback-or-close teardown);
        # the isolate decision lands on the (max_heartbeat_failures + 1)-th
        # consecutive failed beat, so the lease must cover
        # (max_heartbeat_failures + 1) of those gaps. This is exactly the
        # safety floor taskq.worker.heartbeat._lease_renewal_threshold sizes
        # its renewal gate against, so keeping lock_lease above it guarantees
        # that gate's floor can never exceed the lease it guards. The bare
        # 4 * heartbeat_interval rule this check replaced ignored both
        # command-timeout terms and let the lease lapse before the isolate
        # decision under contention. Tightening note: this refuses
        # configs that loaded before, a lease between the old 4x edge and
        # the cascade floor must come up (or the command timeouts come down);
        # see docs/guides/upgrading.md.
        isolate_beats = self.max_heartbeat_failures + 1
        worst_beat_gap = self.heartbeat_interval + 2 * self.heartbeat_command_timeout
        cascade_floor = isolate_beats * worst_beat_gap
        if self.lock_lease < cascade_floor:
            errors.append(
                ValidationError(
                    field_name="lock_lease",
                    value=self.lock_lease,
                    error_msg=(
                        f"lock_lease ({self.lock_lease}) must cover the worst coherent "
                        f"failed-beat cascade: (max_heartbeat_failures + 1) * "
                        f"(heartbeat_interval + 2 * heartbeat_command_timeout) = "
                        f"{isolate_beats} * ({self.heartbeat_interval} + 2 * "
                        f"{self.heartbeat_command_timeout}) = {cascade_floor}"
                    ),
                )
            )

        # Park-tail vs heartbeat-exit: NO hard error here, deliberately:
        # see WorkerSettings.release_park_lease_cap. The release park is
        # capped by the lease in the consumer itself, which makes the
        # single-RELEASING-write-failure exposure structurally impossible
        # for every config that loads; a cross-field rejection here would
        # refuse configs that are safe under the cap (and would break
        # every fast test fixture that zeroes the graces against the
        # default budget: post_load runs even with validate=False). The
        # operator-facing surface is the warning tier in
        # _emit_startup_warnings, and the residual double-write-failure
        # bound is release_disown_lease_floor.

        # Cancellation + cleanup grace must fit within termination_grace_period.
        # termination_grace_period may be added by a subclass; the getattr guard
        # tolerates its absence when this base validation runs first.
        termination_grace = getattr(self, "termination_grace_period", None)
        if (
            termination_grace is not None
            and self.cancellation_grace_period + self.cleanup_grace_period
            >= termination_grace - 5.0
        ):
            errors.append(
                ValidationError(
                    field_name="cancellation_grace_period",
                    value=self.cancellation_grace_period,
                    error_msg=(
                        f"cancellation_grace_period ({self.cancellation_grace_period}) + "
                        f"cleanup_grace_period ({self.cleanup_grace_period}) must be < "
                        f"termination_grace_period - 5.0 ({termination_grace - 5.0})"
                    ),
                )
            )

        # Cancellation grace + cleanup grace must be less than lock_lease.
        if self.cancellation_grace_period + self.cleanup_grace_period >= self.lock_lease:
            errors.append(
                ValidationError(
                    field_name="cancellation_grace_period",
                    value=self.cancellation_grace_period,
                    error_msg=(
                        f"cancellation_grace_period ({self.cancellation_grace_period}) + "
                        f"cleanup_grace_period ({self.cleanup_grace_period}) must be < "
                        f"lock_lease ({self.lock_lease})"
                    ),
                )
            )

        # Bounded-loop staleness invariant: the period-1 leader loops
        # (scheduled_wake, cron) are wrapped in asyncio.timeout, so their
        # worst-case tick gap is timeout + period. That gap must fit the
        # loop's own budget max(period * watchdog_tick_grace_factor,
        # watchdog_stale_floor) or detector 2 force-exits a healthy worker
        # mid-degradation (measured: timeout 10.0 against budget 10.0
        # produced an 11s tick gap and a trip at age 10.008s). Only checked
        # when the watchdog is armed: with watchdog_enabled=False detector 2
        # is never spawned, and a stale tick only costs a transient NotReady,
        # which is not worth blocking boot over.
        #
        # The producer loop is deliberately NOT checked here: it is not
        # wrapped in asyncio.timeout (dispatch_batch is a multi-statement
        # transaction, BEGIN + dispatch CTE + INSERTs + COMMIT, plus a
        # queue-mode resolve on cache miss (the worker-side TTL cache in
        # taskq.backend._dispatch.QueueModeCache), each bounded
        # separately by the pool's command_timeout), so the timeout +
        # period model does not hold. The actual worst-case
        # gap is k * timeout + period for k statements, which the invariant
        # cannot express without knowing k at settings-load time.
        if self.watchdog_enabled:
            loop_label = "leader loops"
            period = 1.0
            budget = max(period * self.watchdog_tick_grace_factor, self.watchdog_stale_floor)
            if budget <= period + 1.0:
                # 1.0 = dispatcher_command_timeout's own ge= minimum: no
                # legal timeout can satisfy the gap, so the budget side
                # is what the operator must change.
                errors.append(
                    ValidationError(
                        field_name="watchdog_stale_floor",
                        value=self.watchdog_stale_floor,
                        error_msg=(
                            f"the {loop_label} staleness budget max({period} x "
                            f"watchdog_tick_grace_factor, watchdog_stale_floor) "
                            f"({budget}) must exceed dispatcher_command_timeout's "
                            f"1.0s minimum + the {period}s loop period"
                        ),
                    )
                )
            elif self.dispatcher_command_timeout + period >= budget:
                errors.append(
                    ValidationError(
                        field_name="dispatcher_command_timeout",
                        value=self.dispatcher_command_timeout,
                        error_msg=(
                            f"dispatcher_command_timeout ({self.dispatcher_command_timeout}) "
                            f"+ {period}s {loop_label} period must be < the loop's "
                            f"staleness budget max(period x watchdog_tick_grace_factor, "
                            f"watchdog_stale_floor) ({budget})"
                        ),
                    )
                )

        # Lag-watchdog lease invariant: a stalled event loop must die (the
        # terminal lag watchdog trips at watchdog_loop_lag_budget) before
        # its leases can expire (lock_lease), otherwise the leader sweep
        # reclaims LIVE jobs' locks mid-stall and the worker wakes from the
        # stall to find its work reassigned. The heartbeat_interval term is
        # the worst-case age the last beat can carry when the stall starts,
        # so the trip is guaranteed to land inside the lease. Only checked
        # when the watchdog is armed: with watchdog_enabled=False no
        # terminal lag detector exists, and stall-vs-lease ordering is a
        # deployment concern, not a load-time guarantee (same gating as the
        # bounded-loop invariant above).
        if self.watchdog_enabled and (
            self.watchdog_loop_lag_budget + self.heartbeat_interval >= self.lock_lease
        ):
            errors.append(
                ValidationError(
                    field_name="watchdog_loop_lag_budget",
                    value=self.watchdog_loop_lag_budget,
                    error_msg=(
                        f"watchdog_loop_lag_budget ({self.watchdog_loop_lag_budget}) + "
                        f"heartbeat_interval ({self.heartbeat_interval}) must be < "
                        f"lock_lease ({self.lock_lease}): a stalled event loop must die "
                        f"(the terminal lag watchdog) before its leases expire, or the "
                        f"leader sweep reclaims LIVE jobs' locks mid-stall. Keep the lag "
                        f"budget comfortably inside lock_lease, both knobs must move "
                        f"together."
                    ),
                )
            )

        # Lag budget vs check interval coherence: the lag detector samples
        # the loop once per watchdog_check_interval and schedules the beat
        # it measures from the same poll, so a healthy loop's observed lag
        # is ~check_interval by construction. A budget at or below the
        # sampling period therefore trips on health, not stalls (measured:
        # budget 1.0 against the 1.0s default check interval force-exits an
        # idle worker on its first armed poll). Same watchdog gating as the
        # lease invariant above.
        if self.watchdog_enabled and (
            self.watchdog_loop_lag_budget <= self.watchdog_check_interval
        ):
            errors.append(
                ValidationError(
                    field_name="watchdog_loop_lag_budget",
                    value=self.watchdog_loop_lag_budget,
                    error_msg=(
                        f"watchdog_loop_lag_budget ({self.watchdog_loop_lag_budget}) "
                        f"must be > watchdog_check_interval "
                        f"({self.watchdog_check_interval}): the detector samples the "
                        f"loop once per check interval, so a budget at or below its "
                        f"own sampling period trips on a healthy loop's beat cadence. "
                        f"Raise the budget (keeping it inside lock_lease) or lower "
                        f"watchdog_check_interval."
                    ),
                )
            )

        # Tier-1 vs tier-2 ordering: the warn budget must be able to fire
        # before the terminal budget. A warn budget at or above the
        # terminal budget silently disables tier 1, the worker is
        # force-exited with no prior lag warning, exactly the
        # silent-disable failure the (0, 1) bound on
        # watchdog_dump_after_fraction exists to prevent ("at 1.0 the
        # deadline trip always fires first"). A budget pair the validator
        # cannot distinguish from a healthy one is a misconfiguration the
        # operator only meets at the os._exit. Same watchdog gating as
        # the lease invariant above.
        if self.watchdog_enabled and (
            self.watchdog_loop_lag_warn_budget >= self.watchdog_loop_lag_budget
        ):
            errors.append(
                ValidationError(
                    field_name="watchdog_loop_lag_warn_budget",
                    value=self.watchdog_loop_lag_warn_budget,
                    error_msg=(
                        f"watchdog_loop_lag_warn_budget "
                        f"({self.watchdog_loop_lag_warn_budget}) must be < "
                        f"watchdog_loop_lag_budget ({self.watchdog_loop_lag_budget}): "
                        f"a warn budget at or above the terminal budget can never "
                        f"fire first, so the terminal lag trip force-exits with zero "
                        f"prior warning. Raise watchdog_loop_lag_budget (keeping it "
                        f"inside lock_lease) or lower the warn budget."
                    ),
                )
            )

        return errors or None

    @property
    def worker_pool_size(self) -> int:
        """Derived pool size for worker_pool: int(max_concurrency * 1.5)."""
        return int(self.max_concurrency * 1.5)

    @property
    def worst_case_shutdown_seconds(self) -> float:
        """Modelled worst-case wall clock from SIGTERM to process exit.

        The shutdown phase graces plus the bounded-close tail that unwinds
        after them (see :func:`taskq._close.worst_case_teardown_tail`).

        This is deliberately NOT enforced by ``post_load``. The cross-field
        validator there rejects a config outright, and a budget below this
        dead-backend worst case is a legitimate operator choice (a tight
        dev deployment that would rather be SIGKILLed mid-unwind than wait
        out a hung close). The shipped default covers the model, raising
        the validator to reject sub-worst-case budgets would take that
        choice away. The number is surfaced as a startup warning instead,
        and ``docs/guides/deployment.md`` documents the pod-grace formula.
        """
        return (
            self.cancellation_grace_period + self.cleanup_grace_period + worst_case_teardown_tail()
        )

    @property
    def release_exit_tail_seconds(self) -> float:
        """Seconds past the termination deadline the process can still be
        alive: the tail every release hold pads its remaining share with.

        The deadline trip is not instantaneous: the watchdog checks the
        deadline once per ``watchdog_dump_interval`` (the check is clipped
        to the deadline, so real lag is loop jitter and this term is
        margin), and ``trip()`` then renders task stacks and joins the
        bounded metrics flush before ``os._exit``. The stack render and
        critical log write have no bound of their own: the
        ``RELEASE_EXIT_TAIL_SLACK_SECS`` heuristic covers them, and the
        residual (a full stderr pipe under a slow docker logging driver)
        is documented in docs/guides/workers.md. Kept as a settings
        property because the lease arithmetic that guards the disown path
        (see ``release_disown_lease_floor``) needs the same number the
        worker-layer hold pads with: one source, no drift.
        """
        return (
            self.watchdog_dump_interval
            + WATCHDOG_METRICS_FLUSH_TIMEOUT_SECS
            + RELEASE_EXIT_TAIL_SLACK_SECS
        )

    @property
    def release_park_lease_cap(self) -> float:
        """The lease-derived ceiling on the release-until-exited park.

        The consumer's shutdown arm parks a still-running sync actor (or
        transactional unwind) so a provable exit can earn the immediate
        ``pending`` release, but the heartbeat, the row's only lease
        renewer, stops when the orchestrator sets ``shutdown_event``
        (roughly ``cancellation_grace + cleanup_grace`` in). A park that
        ran to the termination budget on a lease shorter than that budget
        would let the row's lease expire mid-park, and the leader's
        reclaim sweep (no carve-out for SHUTDOWN-origin rows:
        ``cancel_phase`` stays 0) would re-pend a row whose actor thread
        is still executing: the double-run, reachable from a single
        infra-failed RELEASING write. The consumer therefore caps the
        park at::

            lock_lease - heartbeat_interval - TERMINAL_WRITE_BUDGET_SECS

        which is exactly the bound that makes the parked consumer's
        release write land before the earliest reclaim (last heartbeat,
        up to one interval stale, plus the lease) for ANY config: the
        write starts by ``cancel + cap`` and spends at most the terminal
        write's budget, and ``cancel + (lock_lease - heartbeat -
        write_budget) + write_budget <= (cancel + cleanup_grace -
        heartbeat) + lock_lease`` holds identically. Safety by
        construction, not by configuration, which is why there is no
        cross-field rejection in ``post_load`` for this shape: the cap
        makes every loadable config safe, and refusing e.g.
        ``120/30/10/60`` outright would reject a config the cap already
        protects. The cost of a binding cap is latency, not safety: a
        capped park releases the row earlier with a longer hold, and the
        ``release-park-lease-capped`` startup warning names the configs
        where that trade is being made.
        """
        return self.lock_lease - self.heartbeat_interval - TERMINAL_WRITE_BUDGET_SECS

    @property
    def release_park_budget_bound(self) -> float:
        """The budget-derived park bound at the graces: ``termination -
        cancellation - cleanup - TERMINAL_WRITE_BUDGET_SECS``.

        The park's other ceiling (the remaining termination budget minus
        the release write's own budget, evaluated at the cancel). The
        lease cap binds first whenever
        ``release_park_lease_cap < release_park_budget_bound``: i.e.
        whenever ``lock_lease < termination - cancellation - cleanup +
        heartbeat``, and that is the inequality the
        ``release-park-lease-capped`` startup warning surfaces with its
        arithmetic. At the shipped defaults (60 vs 55) the budget bound
        binds and the park runs its full remaining budget.
        """
        return (
            self.termination_grace_period
            - self.cancellation_grace_period
            - self.cleanup_grace_period
            - TERMINAL_WRITE_BUDGET_SECS
        )

    @property
    def release_disown_lease_floor(self) -> float:
        """The ``lock_lease`` the disown path needs (warning floor; the
        shipped default is 63 vs lock_lease 60 and marginally fails).

        When BOTH release writers fail their writes (the RELEASING phase's
        and the consumer's: the consumer's exhaustion disowns the row),
        the row stays ``running`` behind a lease the heartbeat has already
        stopped renewing, and the leader's reclaim sweep becomes the only
        exit: at the earliest ``last heartbeat + lock_lease``. For the
        reclaim to stay behind the process's true exit (the deadline trip
        plus the exit tail, which is where an outlived actor thread dies),
        the lease must cover:

        ``lock_lease >= termination - cancellation - cleanup + heartbeat
        + release_exit_tail_seconds``

        At the shipped defaults that is 63 against ``lock_lease`` 60: a
        residue of ~3s that requires the double write failure AND a sweep
        tick landing inside it. Surfaced as a startup warning, not a hard
        fail: the shipped default would not load otherwise, and whether to
        spend 3 more seconds of lease on a double-failure residue is a
        maintainer/operator call (see
        ``_emit_startup_warnings`` in taskq.worker._bootstrap).
        """
        return (
            self.termination_grace_period
            - self.cancellation_grace_period
            - self.cleanup_grace_period
            + self.heartbeat_interval
            + self.release_exit_tail_seconds
        )

    @property
    def shutdown_budget_is_sufficient(self) -> bool:
        """Whether ``termination_grace_period`` covers the modelled worst case."""
        return self.worst_case_shutdown_seconds <= self.termination_grace_period
