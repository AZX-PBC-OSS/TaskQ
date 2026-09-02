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
import re
from datetime import timedelta
from pathlib import Path
from typing import Self
from uuid import UUID

from croniter import croniter
from dotenvmodel import DotEnvConfig, Field, ValidationError, ValidatorContext
from dotenvmodel.types import PostgresDsn, RedisDsn

from taskq._close import worst_case_teardown_tail
from taskq._json import check_no_nul_str
from taskq.backend._protocol import (
    # Cycle-safe by import direction: nothing in _protocol's own chain
    # imports taskq.settings, and taskq/__init__ always finishes loading
    # backend._protocol (via taskq.actor) before anything loads settings.
    _QUEUE_NAME_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical queue-name charset rather than redefining it
)
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it
    RECLAIM_EVENT_VISIBILITY_DELAY,
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
        # truncates longer identifiers — while Redis channel templates
        # interpolate the full string, quietly diverging between stores.
        # A length cap belongs in this hook, not `max_length=`, because
        # built-in constraints are skipped under validate=False (see above).
        # Chars == bytes here: _IDENT_RE already admitted only ASCII.
        raise ValueError(
            f"{ctx.field_name} must be at most 63 characters (Postgres "
            f"NAMEDATALEN truncates longer identifiers), got {len(value)} characters"
        )
    return value


class _NoEnvFilesWarningFilter(logging.Filter):
    """Drop only dotenvmodel's "No .env files found in <dir>" WARNING.

    Why a logger-level filter works: every dotenvmodel 1.x module logs
    through the single ``"dotenvmodel"`` logger (``LOGGER_NAME`` in
    ``dotenvmodel/_constants.py``; ``loading.py`` hardcodes the same
    string) — there are no child loggers, so one filter sees all of its
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

    # -- SSO / SAML -------------------------------------------------------
    sso_backend: str = Field(
        default="none",
        validator=_sso_backend_validator,
        description="TASKQ_SSO_BACKEND. Selects the SSO backend for the admin UI: "
        "'none' (default, unauthenticated/BYO-auth), 'oidc' (taskq[oidc]), "
        "or 'saml' (taskq[saml]). See docs/guides/sso.md.",
    )
    health_token: str = Field(
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
        "instead of a bearer token - note that many k8s liveness/readiness "
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
        ``override=None`` keeps dotenvmodel's default precedence — the
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
    names (``backend/_protocol.py``'s ``_QUEUE_NAME_RE``). A NUL is outside
    that charset, so the rule covers it too.
    """
    for i, item in enumerate(value):
        if not _QUEUE_NAME_RE.match(item):
            raise ValueError(
                f"{ctx.field_name}[{i}] must be a valid queue name (letters, "
                f"digits, '_', '.', '-'; first char a letter or '_'), got {item!r}"
            )
    return value


class WorkerSettings(TaskQSettings):
    """Worker-specific configuration with three-pool sizing and dual-DSN support.

    Extends :class:`TaskQSettings` with pool-size knobs, dual-DSN fields, and
    the validated ``lock_lease >= 4 * heartbeat_interval`` invariant.
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
        "asyncio.timeout).",
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

    # -- Timing ----------------------------------------------------------
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

    # ── Leader sweep intervals ─────────────────────────────────
    sweep_interval: float = Field(
        default=30.0,
        ge=1.0,
        description="TASKQ_SWEEP_INTERVAL (seconds). Period between leader "
        "sweep loop iterations — reclaim_expired_locks, "
        "sweep_expired_results, cleanup_stale_workers, and idle keyed-ref "
        "eviction. Lower values reduce recovery latency for crashed workers "
        "at the cost of more frequent PG queries.",
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
        default=timedelta(hours=24),
        description=(
            "TASKQ_MAX_RETRY_BACKOFF (interval). Global ceiling on retry backoff "
            "per attempt - caps the per-actor RetryPolicy.cap so a misconfigured "
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
        default=10000,
        ge=1,
        description="TASKQ_MAX_KEYED_RESERVATIONS. Guardrail on the number of "
        "distinct keyed-reservation entries tracked in memory. When the limit "
        "is reached, new keyed reservations raise ReservationUnavailable. "
        "Tune to your workload's expected key cardinality.",
    )
    max_keyed_rate_limits: int = Field(
        default=10000,
        ge=1,
        description="TASKQ_MAX_KEYED_RATE_LIMITS. Guardrail on the number of "
        "distinct keyed-rate-limit entries tracked in memory. When the limit "
        "is reached, new keyed rate limits raise ReservationUnavailable. "
        "Independent from max_keyed_reservations, which governs keyed "
        "reservations only. Tune to your workload's expected key cardinality.",
    )

    # -- Prometheus standalone metrics server ------------------
    metrics_port: int = Field(
        default=9090,
        ge=1,
        le=65535,
        description="TASKQ_METRICS_PORT. Bind port for the standalone "
        "Prometheus metrics server (taskq health metrics --port). "
        "The in-process FastAPI mount ignores this field.",
    )

    # -- Health server ------------------------------------------
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
    health_tasks_enabled: bool = Field(
        default=False,
        description="TASKQ_HEALTH_TASKS_ENABLED. Expose the privileged "
        "/tasks asyncio stack-dump endpoint on the Unix health socket. "
        "Off by default: the dump reveals code structure, file paths, and "
        "task names (never locals or payload values). Enabling it also "
        "tightens the socket to owner-only (no group/other access). Unix socket only — "
        "never mounted on the admin UI surface.",
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
        "host starvation — a terminal detector must never fire on load.",
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
        "each ``add_listener`` call during NOTIFY listener setup and "
        "reconnect - a half-open PG connection that accepts TCP but stalls "
        "on the LISTEN handshake would otherwise wedge the notify loop "
        "forever. On timeout the connection is closed (bounded) and the "
        "reconnect retry loop is entered (or the initial setup raises).",
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
    reload_interval: float | None = Field(
        default=None,
        gt=0,
        description="TASKQ_RELOAD_INTERVAL (seconds). When set, the worker "
        "periodically triggers a credential hot-reload (the same path as "
        "SIGHUP) with no external signal required - the rotation path for "
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
        "individual factory call during a credential hot-reload - a hung "
        "token endpoint is marked failed for that resource instead of "
        "wedging the reload coordinator (and all future SIGHUPs).",
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
        "heartbeat_pool, and worker_pool.",
    )

    # -- Observability --------------------------------------------
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
        default=10000,
        ge=1,
        description="TASKQ_PRUNE_BATCH_SIZE. Rows to delete per batch.",
    )

    # -- Per-status prune retention --------------------------------
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

    # -- Archive retention & expiry schedule ----------------------
    archive_retention_period: timedelta = Field(
        default=timedelta(days=365),
        validator=_non_negative_timedelta,
        description="TASKQ_ARCHIVE_RETENTION_PERIOD. How long archived jobs are "
        "retained in jobs_archive before hard-deletion. Default 1 year. "
        "timedelta(0) is valid. Negative values raise ConstraintViolationError.",
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
            "actor_config row whose queue or metadata differ from the "
            "registered values. When False (the default), that structural "
            "drift raises ActorConfigDriftList and the worker refuses to "
            "start. Capacity fields (max_concurrent, max_pending, "
            "result_ttl) are unaffected by this flag: once a row exists, "
            "the stored value is always authoritative and is never "
            "overwritten by the registered @actor(...) literal, "
            "regardless of force. Use `taskq actor-config set` to change "
            "a stored capacity value. Env var: TASKQ_FORCE_UPDATE_ACTOR_CONFIG."
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
        # transaction — BEGIN + resolve_queue_modes + dispatch CTE + INSERTs
        # + COMMIT, each bounded separately by the pool's command_timeout),
        # so the timeout + period model does not hold. The actual worst-case
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
                        f"budget comfortably inside lock_lease — both knobs must move "
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
        validator there rejects a config outright, and TaskQ's own defaults
        (60 / 30 / 10) produce a modelled worst case above
        ``termination_grace_period`` -- so validating it would refuse to
        start every deployment running the defaults, turning a
        sizing warning into a fleet-wide outage on upgrade. The number is
        surfaced as a startup warning instead, and
        ``docs/guides/deployment.md`` documents the pod-grace formula.
        """
        return (
            self.cancellation_grace_period + self.cleanup_grace_period + worst_case_teardown_tail()
        )

    @property
    def shutdown_budget_is_sufficient(self) -> bool:
        """Whether ``termination_grace_period`` covers the modelled worst case."""
        return self.worst_case_shutdown_seconds <= self.termination_grace_period
