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

# NOTE: dotenvmodel resolves field types from ``__annotations__`` directly
# rather than via ``typing.get_type_hints``. Adding ``from __future__ import
# annotations`` to this module turns those types into forward-ref strings
# and breaks the typed-DSN/SecretStr coercion. Keep annotations evaluated.

from datetime import timedelta
from pathlib import Path
from typing import Self

from dotenvmodel import DotEnvConfig, Field, ValidationError, ValidatorContext
from dotenvmodel.types import PostgresDsn, RedisDsn

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
    client_secret: str = Field(default="", description="OAuth2 client secret.")
    redirect_uri: str = Field(
        default="",
        description="Must match the app registration's configured redirect URI.",
    )
    session_secret: str = Field(
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
    sp_private_key: str | None = Field(
        default=None,
        description="SP private key (PEM).",
    )
    session_secret: str = Field(
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

    @property
    def allowed_groups_set(self) -> frozenset[str]:
        return _parse_groups(self.allowed_groups)


class TaskQSettings(DotEnvConfig):
    """Top-level TaskQ runtime configuration."""

    env_prefix = "TASKQ_"

    pg_dsn: PostgresDsn = Field(
        default=PostgresDsn("postgresql://taskq:taskq@localhost:5432/taskq"),
        description="Direct (non-PgBouncer) DSN. LISTEN/NOTIFY and advisory locks need a session.",
    )
    schema_name: str = Field(
        default="taskq",
        regex=r"^[A-Za-z_][A-Za-z0-9_]*$",
        description="Postgres schema for all TaskQ tables.",
    )
    redis_url: RedisDsn | None = Field(
        default=None,
        description="Optional Redis URL. Required for real-time progress fanout.",
    )
    environment: str | None = Field(
        default=None,
        description="TASKQ_ENVIRONMENT. Deployment environment label. "
        "Values 'dev' and 'development' suppress the unauthenticated-admin "
        "WARNING; any other value (or None/empty) triggers it.",
    )
    admin_max_sse_connections: int = Field(
        default=50,
        ge=1,
        description="TASKQ_ADMIN_MAX_SSE_CONNECTIONS. Maximum concurrent SSE "
        "connections the admin UI will serve. Used to size the connection-limit "
        "semaphore.",
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
    admin_ui_allow_rate_limit_reset: bool = Field(
        default=False,
        description="TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET. When True, the admin UI "
        "shows a reset button on the rate-limits page and serves the "
        "POST /rate-limits/{bucket_name}/reset endpoint. Default False "
        "for safety — prevents accidental resets in production.",
    )
    admin_ui_require_auth: bool = Field(
        default=True,
        description="TASKQ_ADMIN_UI_REQUIRE_AUTH. When True (the default), "
        "create_router raises RuntimeError if auth_dependency is None in a "
        "non-dev environment, failing closed. Set to False to suppress the "
        "error and allow an unauthenticated admin UI in non-dev (not "
        "recommended — only for air-gapped or localhost-only deployments).",
    )
    admin_actions_enabled: bool = Field(
        default=False,
        description="TASKQ_ADMIN_ACTIONS_ENABLED. When True, the admin UI permits "
        "destructive actions (run schedule now, retry job, cancel job). "
        "Default False — prevents on-demand triggering of registered business "
        "logic via the admin UI without explicit opt-in. Separate from "
        "auth_dependency, which controls read access to all admin routes.",
    )

    # ── SSO / SAML ───────────────────────────────────────────────────────
    sso_backend: str = Field(
        default="none",
        description="TASKQ_SSO_BACKEND. Selects the SSO backend for the admin UI: "
        "'none' (default, unauthenticated/BYO-auth), 'oidc' (taskq[oidc]), "
        "or 'saml' (taskq[saml]). See docs/guides/sso.md.",
    )
    health_token: str = Field(
        default="",
        description="TASKQ_HEALTH_TOKEN. Bearer token for machine-to-machine "
        "access to health/metrics endpoints. When set, health and metrics "
        "routes require a matching 'Authorization: Bearer <token>' header. "
        "Leave empty for unauthenticated cluster-internal access — but see "
        "health_require_token, which fails closed on an empty token outside dev.",
    )
    health_require_token: bool = Field(
        default=True,
        description="TASKQ_HEALTH_REQUIRE_TOKEN. When True (the default), "
        "taskq ui serve raises RuntimeError if health_token is empty in a "
        "non-dev environment, failing closed. Set to False to suppress the "
        "error and allow unauthenticated health/metrics endpoints in non-dev "
        "(e.g. when relying on network policy / cluster-internal-only access "
        "instead of a bearer token — note that many k8s liveness/readiness "
        "probes don't send auth headers by default, so enabling the token "
        "may require updating the probe config too).",
    )
    migrate_on_start: bool = Field(
        default=False,
        description="TASKQ_MIGRATE_ON_START. When True, apply pending migrations "
        "before the admin UI accepts its first request. Aborts startup "
        "if migrations fail.",
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

    @classmethod
    def load(
        cls, env: str | None = None, *, override: bool = True, env_dir: Path | None = None
    ) -> Self:
        """Load settings via dotenvmodel's cascading ``.env`` discovery.

        dotenvmodel logs a WARNING ("No .env files found in <cwd>") on
        every call when no ``.env`` file is present — noisy on every CLI
        invocation in projects that configure purely via real environment
        variables. dotenvmodel's ``load()`` exposes no quiet/verbosity
        parameter (checked via ``inspect.signature``), so this temporarily
        raises the ``dotenvmodel`` stdlib logger to ERROR for the duration
        of the call and restores the previous level afterward — the same
        level-based mechanism dotenvmodel's own ``logging_config`` helpers
        use, just without installing an extra handler.

        ``WorkerSettings.load`` calls ``super().load(...)``, so it goes
        through this same suppression via MRO.
        """
        import logging

        dotenv_logger = logging.getLogger("dotenvmodel")
        previous_level = dotenv_logger.level
        dotenv_logger.setLevel(logging.ERROR)
        try:
            return super().load(env=env, override=override, env_dir=env_dir)
        finally:
            dotenv_logger.setLevel(previous_level)

    @property
    def oidc(self) -> OIDCSettings:
        """Lazily loaded OIDC sub-config (``TASKQ_OIDC_*`` env vars)."""
        return OIDCSettings.load()

    @property
    def saml(self) -> SAMLSettings:
        """Lazily loaded SAML sub-config (``TASKQ_SAML_*`` env vars)."""
        return SAMLSettings.load()


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


_VALID_LOG_FORMATS = frozenset({"json", "console"})


def _log_format_validator(value: str, ctx: ValidatorContext) -> str:
    # A validator hook (not choices=) so the check also runs under
    # load_from_dict(..., validate=False) — choices= is a built-in constraint
    # that validate=False skips, which would let an invalid LOG_FORMAT load
    # silently. See dotenvmodel docs: validator hooks run regardless of validate.
    if value not in _VALID_LOG_FORMATS:
        raise ValueError(
            f"{ctx.field_name} must be one of {sorted(_VALID_LOG_FORMATS)}, got {value!r}"
        )
    return value


class WorkerSettings(TaskQSettings):
    """Worker-specific configuration with three-pool sizing and dual-DSN support.

    Extends :class:`TaskQSettings` with pool-size knobs, dual-DSN fields, and
    the validated ``lock_lease >= 4 * heartbeat_interval`` invariant.
    """

    # ── DSNs ───────────────────────────────────────────────────────────
    pg_dsn_direct: PostgresDsn | None = Field(
        default=None,
        description="TASKQ_PG_DSN_DIRECT; falls back to pg_dsn when absent. "
        "Bypasses PgBouncer — used by dispatcher_pool, heartbeat_pool, "
        "notify_conn, and leader_conn.",
    )
    pg_dsn_pooled: PostgresDsn | None = Field(
        default=None,
        description="TASKQ_PG_DSN_POOLED; falls back to pg_dsn when absent. "
        "May route through PgBouncer transaction mode — used by "
        "worker_pool only.",
    )

    # ── Pool sizes ─────────────────────────────────────────────────────
    dispatcher_pool_size: int = Field(
        default=4,
        ge=1,
        description="TASKQ_DISPATCHER_POOL_SIZE. Max connections for the "
        "dispatcher pool. Bypasses PgBouncer.",
    )
    dispatch_oversample: int = Field(
        default=2,
        ge=1,
        le=1000,
        description="TASKQ_DISPATCH_OVERSAMPLE. Multiplier for per-actor candidate "
        "gathering in the dispatch SQL. Each LATERAL reads residual x oversample "
        "candidates. Higher values absorb more identity collisions and "
        "multi-producer contention. Default 2 (tolerates 50% dupe identities). "
        "Set 1 when no identity_key is used and single-producer.",
    )
    dispatch_scope_by_home_queue: bool = Field(
        default=False,
        description="TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE. When True, restrict "
        "per_actor_capacity to actors whose home queue (actor_config.queue) "
        "the worker subscribes to. Lowers per-cycle probe count at the cost "
        "of not dispatching enqueue(queue=...) override jobs whose actor's "
        "home queue is not subscribed. Default False (override-safe).",
    )
    heartbeat_pool_size: int = Field(
        default=4,
        ge=1,
        description="TASKQ_HEARTBEAT_POOL_SIZE. Max connections for the "
        "heartbeat pool. Bypasses PgBouncer.",
    )
    # worker_pool max_size is derived: int(max_concurrency * 1.5)

    # ── Timing ──────────────────────────────────────────────────────────
    max_concurrency: int = Field(
        default=8,
        ge=1,
        description="TASKQ_MAX_CONCURRENCY. Upper bound on concurrent jobs. "
        "worker_pool max_size = int(max_concurrency * 1.5).",
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
        "Must be >= 4 * heartbeat_interval.",
    )
    max_heartbeat_failures: int = Field(
        default=3,
        ge=1,
        description="TASKQ_MAX_HEARTBEAT_FAILURES. Consecutive heartbeat "
        "failures before the worker self-terminates.",
    )

    # ── Cancellation and cleanup grace periods ───────────
    termination_grace_period: float = Field(
        default=60.0,
        ge=5.0,
        description="TASKQ_TERMINATION_GRACE_PERIOD (seconds). Total wall-clock "
        "budget from SIGTERM to forced exit. Must satisfy "
        "cancellation_grace + cleanup_grace < termination_grace - 5.",
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

    # ── Retry backoff ceiling ───────────────────────────────────────────
    max_retry_backoff: timedelta = Field(
        default=timedelta(hours=24),
        description=(
            "TASKQ_MAX_RETRY_BACKOFF (interval). Global ceiling on retry backoff "
            "per attempt — caps the per-actor RetryPolicy.cap so a misconfigured "
            "actor (e.g. cap=timedelta(days=365)) cannot strand jobs for an "
            "unreasonably long time. Default 24 h: conservative, matches one "
            "standard on-call rotation, and mirrors Dramatiq's DEFAULT_MAX_BACKOFF "
            "philosophy "
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
            "default) means unbounded — matches existing behaviour, opt-in "
            "only. Set this to give every actor on this worker a safety-net "
            "wall-clock budget per attempt, preventing a hung or "
            "infinite-looping actor from occupying a coroutine slot forever, "
            "without having to configure start_to_close on every individual "
            "actor. Precedence (highest wins): per-enqueue start_to_close > "
            "@actor(start_to_close=...) > this setting. This does not affect "
            "schedule_to_close, which is a separate, unrelated deadline for "
            "the job's *overall* retry budget across all attempts — "
            "start_to_close bounds a single attempt's wall-clock time."
        ),
    )

    # ── Rate limit ────────────────────────────────────────────────
    rate_limit_pg_fallback_enabled: bool = Field(
        default=True,
        description="TASKQ_RATE_LIMIT_PG_FALLBACK_ENABLED. When False, Redis "
        "errors propagate instead of triggering PG fallback.",
    )
    max_keyed_reservations: int = Field(
        default=10000,
        ge=1,
        description="TASKQ_MAX_KEYED_RESERVATIONS. Guardrail on the number of "
        "distinct keyed-reservation entries tracked in memory. When the limit "
        "is reached, new keyed reservations raise ReservationUnavailable. "
        "Tune to your workload's expected key cardinality.",
    )

    # ── Prometheus standalone metrics server ──────────────────
    metrics_port: int = Field(
        default=9090,
        ge=1,
        le=65535,
        description="TASKQ_METRICS_PORT. Bind port for the standalone "
        "Prometheus metrics server (taskq health metrics --port). "
        "The in-process FastAPI mount ignores this field.",
    )

    # ── Health server ──────────────────────────────────────────
    health_enabled: bool = Field(
        default=True,
        description="TASKQ_HEALTH_ENABLED. Enable the Unix-socket health server.",
    )
    health_socket_path: str = Field(
        default="/tmp/taskq_health.sock",  # noqa: S108  # Why: default. Production deployments override via env var (typically /run/taskq.sock under tmpfs).
        description="TASKQ_HEALTH_SOCKET_PATH. Unix socket path for the health server.",
    )
    health_pg_ping_timeout: float = Field(
        default=0.2,
        ge=0.0,
        description="TASKQ_HEALTH_PG_PING_TIMEOUT. Seconds to wait for "
        "dispatcher_pool.acquire() in the readiness PG ping. "
        "Default 200ms .",
    )

    # ── Polling and NOTIFY listener ────────────────────────
    poll_interval: float = Field(
        default=1.0,
        description="TASKQ_POLL_INTERVAL (seconds). Producer loop fallback "
        "polling cadence when the NOTIFY listener is unavailable.",
    )
    notify_health_check_interval: float = Field(
        default=5.0,
        description="TASKQ_NOTIFY_HEALTH_CHECK_INTERVAL (seconds). How often "
        "_health_check_loop issues SELECT 1 on notify_conn. "
        "Detection latency before reconnect is at most this interval.",
    )
    notify_reconnect_backoff_initial: float = Field(
        default=1.0,
        description="TASKQ_NOTIFY_RECONNECT_BACKOFF_INITIAL (seconds). "
        "Initial exponential backoff delay before the first reconnect "
        "retry. Cap is 30 s (factor 2 per attempt). "
        "Backoff sequence: 1, 2, 4, 8, 16, 30.",
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
        "cadence when NOTIFY is enabled (rarely reached — NOTIFY handles "
        "the common case). Use poll_interval when NOTIFY is disabled.",
    )

    # ── Credential hot-reload ────────────────────────────────────────────
    reload_interval: float | None = Field(
        default=None,
        gt=0,
        description="TASKQ_RELOAD_INTERVAL (seconds). When set, the worker "
        "periodically triggers a credential hot-reload (the same path as "
        "SIGHUP) with no external signal required — the rotation path for "
        "platforms without SIGHUP (e.g. Windows) and for hands-off "
        "scheduled rotation (e.g. ~720s for AWS IAM's 15-minute tokens). "
        "None disables the timer; SIGHUP and deps.request_reload() still "
        "work. Only factory-backed resources are rebuilt; DSN/static "
        "credentials are unaffected.",
    )
    reload_factory_timeout: float = Field(
        default=30.0,
        gt=0,
        description="TASKQ_RELOAD_FACTORY_TIMEOUT (seconds). Bounds each "
        "individual factory call during a credential hot-reload — a hung "
        "token endpoint is marked failed for that resource instead of "
        "wedging the reload coordinator (and all future SIGHUPs).",
    )

    # ── Queue selection ──────────────────────────────────────────────────
    queues: list[str] = Field(
        default_factory=lambda: ["default"],
        description="TASKQ_QUEUES. Comma-separated list of queue names "
        "this worker will consume from.",
    )

    worker_label: str | None = Field(
        default=None,
        description="TASKQ_WORKER_LABEL. Human-readable label stored in the "
        "workers table for correlation with workgroup supervisors and external "
        "monitoring. When omitted the column is NULL; hostname and pid columns "
        "provide identification.",
    )
    workgroup_instance: str | None = Field(
        default=None,
        description="TASKQ_WORKGROUP_INSTANCE. UUIDv7 identifying the workgroup "
        "orchestrator that launched this worker. Used for cross-process correlation.",
    )

    # ── Pool lifecycle ──────────────────────────────────────────────────
    pool_max_inactive_lifetime: float = Field(
        default=300.0,
        ge=0.0,
        description="TASKQ_POOL_MAX_INACTIVE_LIFETIME (seconds). asyncpg "
        "max_inactive_connection_lifetime — closes connections idle "
        "longer than this threshold. Set to 3600.0 to match a typical "
        "SQLAlchemy pool_recycle=3600 setting when running alongside "
        "an SQLAlchemy-based service. Applied to dispatcher_pool, "
        "heartbeat_pool, and worker_pool.",
    )

    # ── Observability ────────────────────────────────────────────
    otel_enabled: bool = Field(
        default=True,
        description="TASKQ_OTEL_ENABLED. When False, the library suppresses all span "
        "and metric creation but operations still succeed .",
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
        description="TASKQ_LOG_LEVEL. Root logger level.",
    )

    # ── Pruning schedule ────────────────────────────────────────────
    prune_schedule_utc: str = Field(
        default="03:00",
        description="TASKQ_PRUNE_SCHEDULE_UTC. HH:MM (UTC) for the daily prune "
        "run. Ignored when prune_cron_expr is set.",
    )
    prune_cron_expr: str | None = Field(
        default=None,
        description="TASKQ_PRUNE_CRON_EXPR. Full 5-field cron expression. When "
        "set, takes precedence over prune_schedule_utc.",
    )
    prune_batch_size: int = Field(
        default=10000,
        ge=1,
        description="TASKQ_PRUNE_BATCH_SIZE. Rows to delete per batch.",
    )

    # ── Per-status prune retention ────────────────────────────────
    prune_retention_period: timedelta = Field(
        default=timedelta(days=30),
        validator=_non_negative_timedelta,
        description="TASKQ_PRUNE_RETENTION_PERIOD. Global fallback retention. "
        "timedelta(0) means archive all terminal jobs immediately (valid). "
        "Negative values raise ConstraintViolationError at settings load.",
    )
    prune_retention_succeeded: timedelta = Field(
        default=timedelta(days=30),
        validator=_non_negative_timedelta,
        description="TASKQ_PRUNE_RETENTION_SUCCEEDED.",
    )
    prune_retention_failed: timedelta = Field(
        default=timedelta(days=90),
        validator=_non_negative_timedelta,
        description="TASKQ_PRUNE_RETENTION_FAILED.",
    )
    prune_retention_cancelled: timedelta = Field(
        default=timedelta(days=30),
        validator=_non_negative_timedelta,
        description="TASKQ_PRUNE_RETENTION_CANCELLED.",
    )
    prune_retention_abandoned: timedelta = Field(
        default=timedelta(days=90),
        validator=_non_negative_timedelta,
        description="TASKQ_PRUNE_RETENTION_ABANDONED. Also used for crashed "
        "jobs (no separate prune_retention_crashed field).",
    )

    # ── Archive retention & expiry schedule ──────────────────────
    archive_retention_period: timedelta = Field(
        default=timedelta(days=365),
        validator=_non_negative_timedelta,
        description="TASKQ_ARCHIVE_RETENTION_PERIOD. How long archived jobs are "
        "retained in jobs_archive before hard-deletion. Default 1 year. "
        "timedelta(0) is valid. Negative values raise ConstraintViolationError.",
    )
    archive_expiry_schedule_utc: str = Field(
        default="04:00",
        description="TASKQ_ARCHIVE_EXPIRY_SCHEDULE_UTC. HH:MM (UTC) for the "
        "daily archive expiry sweep. Default 04:00, 1 hour after the "
        "prune sweep.",
    )
    archive_expiry_cron_expr: str | None = Field(
        default=None,
        description="TASKQ_ARCHIVE_EXPIRY_CRON_EXPR. Full 5-field cron "
        "expression. When set, takes precedence over "
        "archive_expiry_schedule_utc.",
    )

    # ── Actor config drift handling ───────────────────────────────────────
    force_update_actor_config: bool = Field(
        default=False,
        description=(
            "When True, sync_actor_config silently overwrites stored "
            "actor_config rows that differ from the registered values. "
            "When False (the default), drift raises ActorConfigDriftList "
            "and the worker refuses to start. Set to True for one deploy "
            "to deliberately change a stored max_concurrent / queue / "
            "metadata, then unset. Env var: TASKQ_FORCE_UPDATE_ACTOR_CONFIG."
        ),
    )

    # ── Progress fanout ────────────────────────────────────────────
    progress_coalesce_interval: float = Field(
        default=0.5,
        ge=0.1,
        description="TASKQ_PROGRESS_COALESCE_INTERVAL (seconds). How long the "
        "periodic flush loop waits between writing coalesced progress state "
        "to Postgres. Redis publishes are not throttled by this setting — "
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

    # ── Cron scheduler ────────────────────────────────────────────
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

    @property
    def resolved_pg_dsn_direct(self) -> PostgresDsn:
        """Direct DSN guaranteed non-``None`` after :meth:`post_load`.

        Why a property: ``pg_dsn_direct: PostgresDsn | None`` carries the
        environment-shape that distinguishes "user did not set
        ``TASKQ_PG_DSN_DIRECT``" (``None``, fallback to ``pg_dsn``) from
        "user set it explicitly". Once :meth:`post_load` has applied the
        fallback, the field is always non-``None`` — but pyright cannot
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
        including under ``validate=False`` — consistent with the per-field
        ``validator`` hooks (transformation is part of loading, not
        validation). No ``WorkerSettings.load`` / ``load_from_dict``
        override is needed; the base ``DotEnvConfig._load_fields`` invokes
        this hook itself.

        Returns ``list[ValidationError]`` so failures integrate with
        dotenvmodel's uniform error hierarchy: a single returned error is
        raised unchanged (its exact type preserved), several aggregate
        into ``MultipleValidationErrors``. Catch ``DotEnvModelError`` (the
        common base) to cover both single and aggregate cases —
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

        # lock_lease invariant: "Tolerates 3 missed heartbeats before reclamation."
        if self.lock_lease < 4 * self.heartbeat_interval:
            errors.append(
                ValidationError(
                    field_name="lock_lease",
                    value=self.lock_lease,
                    error_msg=(
                        f"lock_lease ({self.lock_lease}) must be >= 4 * heartbeat_interval "
                        f"({4 * self.heartbeat_interval})"
                    ),
                )
            )

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

        return errors or None

    @property
    def worker_pool_size(self) -> int:
        """Derived pool size for worker_pool: int(max_concurrency * 1.5)."""
        return int(self.max_concurrency * 1.5)
