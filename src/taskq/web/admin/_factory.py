"""Admin UI router factory: Jinja2 setup, auth hook, route registration.

Importing this module requires the ``taskq[fastapi]`` optional extra.
"""

import asyncio
import hmac
import importlib
import pkgutil
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import asyncpg
import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse, Response
from fastapi.routing import APIRoute
from jinja2 import Environment, PackageLoader
from starlette.middleware.gzip import GZipMiddleware as _GZipMiddleware
from starlette.types import Receive, Scope, Send

from taskq.backend._protocol import Backend
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: canonical identifier regex; reusing the shared validation pattern rather than redefining it.
)
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.registry import registry as _rl_singleton
from taskq.settings import TaskQSettings
from taskq.web.admin import _static

logger = structlog.get_logger("taskq.web.admin")


# ── GZip middleware (static assets only, not HTML) ──────────────────────


class GZipStaticOnly(_GZipMiddleware):
    """GZip only static assets (/static/*), not HTML or JSON responses."""

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            path: str = scope.get("path", "")
            if "/static/" not in path:
                await self.app(scope, receive, send)
                return
        await super().__call__(scope, receive, send)


# ── Jinja2 filter: humanized relative timestamps ────────────────────────
#
# Every timestamp the admin UI renders was written by the database clock
# (`clock_timestamp()`), so the age of a row belongs to the database's clock
# domain. Answering "how stale is this?" by subtracting from this process's
# `datetime.now()` makes every rendered age wrong by exactly the app-to-database
# skew — NTP drift, a paused VM, a container clock, WSL2's documented stepping —
# and that question is asked during an incident, when the wrong answer costs the
# most. The rest of this branch removes mixed-domain arithmetic by pushing it
# into SQL; a Jinja filter cannot issue a query, so it uses the next best thing:
# a periodically measured offset between the two clocks, so the subtraction is
# performed in the database's domain even though it runs in Python.
#
# Better still, a query can do the arithmetic server-side — the
# `EXTRACT(EPOCH FROM clock_timestamp() - col)` shape `_LEADER_SQL` uses for
# `watchdog_healthy` — so no clock participates at all. That is the preferred
# form for new queries; it renders the number itself, not through this filter.

_CLOCK_OFFSET_TTL: float = 30.0


@dataclass
class _DbClockOffset:
    """Measured ``database_now - app_now``, in seconds."""

    seconds: float = 0.0
    expires_at: float = 0.0


_db_clock_offset = _DbClockOffset()


async def refresh_db_clock_offset(pool: asyncpg.Pool) -> None:
    """Re-measure the app-to-database clock offset, at most once per TTL.

    Installed as a router-level dependency so every admin request keeps the
    offset fresh for the (synchronous) Jinja filter. Failures are swallowed
    and the previous offset kept: a clock probe must never take down a page,
    and a slightly stale offset is still far closer to the truth than
    ignoring skew entirely.
    """
    now = time.monotonic()
    if now < _db_clock_offset.expires_at:
        return
    try:
        before = datetime.now(UTC)
        async with pool.acquire() as conn:
            db_now: datetime = await conn.fetchval("SELECT clock_timestamp()")
        after = datetime.now(UTC)
    except Exception:
        # Back off for a full TTL rather than probing on every request.
        _db_clock_offset.expires_at = now + _CLOCK_OFFSET_TTL
        return
    # Why the midpoint: the round trip happens between the two local reads, so
    # the server's instant is best compared against the middle of that window
    # rather than either end — the same correction NTP applies. On a local
    # database this is sub-millisecond, but it costs nothing and keeps the
    # measurement honest over a slow link.
    app_now = before + (after - before) / 2
    _db_clock_offset.seconds = (db_now - app_now).total_seconds()
    _db_clock_offset.expires_at = now + _CLOCK_OFFSET_TTL


def _db_now() -> datetime:
    """This process's best estimate of the database's current instant."""
    return datetime.now(UTC) + timedelta(seconds=_db_clock_offset.seconds)


def _time_ago(ts: Any) -> str:
    """Return a human-readable relative time string via humanize (e.g. '2 minutes ago').

    Takes a database-written timestamp, which is aged against the database
    clock rather than this process's. Anything that is not a timestamp is
    stringified unchanged.
    """
    if ts is None or ts == "":
        return "—"
    try:
        import humanize

        if isinstance(ts, str):
            dt = datetime.fromisoformat(ts)
        elif isinstance(ts, datetime):
            dt = ts
        else:
            return str(ts)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)

        return humanize.naturaltime(_db_now() - dt)
    except Exception:
        return str(ts) if ts else "—"


def _iso_attr(ts: Any) -> str:
    """Return ISO timestamp for tooltip title attribute."""
    if ts is None:
        return ""
    if isinstance(ts, datetime):
        return ts.isoformat()
    if isinstance(ts, str):
        return ts
    return str(ts)


# ------------------------------------------------------------------
# Redis health cache
#
# Accessed only from asyncio coroutines on a single event loop —
# no mutex is needed (asyncio is cooperative, not preemptive).
# A harmless double-ping can occur when two coroutines both see a
# stale cache simultaneously; last-writer-wins, both results valid.
# ------------------------------------------------------------------

_CACHE_TTL: float = 5.0


@dataclass
class _RedisHealthCache:
    ok: bool = False
    expires_at: float = field(default=0.0)


_redis_health_cache = _RedisHealthCache()


async def get_realtime_mode(
    redis_client: Any | None,
) -> tuple[str, str]:
    """Return ``(realtime_mode, mode_label)`` using a 5 s server-side cache.

    realtime_mode ∈ {"realtime", "polling", "polling-degraded"}.
    Cache is module-level — one entry covers all admin UI routes
    on the same process.
    """
    if redis_client is None:
        return "polling", "polling mode"
    now = asyncio.get_running_loop().time()
    if now < _redis_health_cache.expires_at:
        ok = _redis_health_cache.ok
    else:
        try:
            await asyncio.wait_for(redis_client.ping(), timeout=0.5)
            ok = True
        except Exception:
            ok = False
        _redis_health_cache.ok = ok
        _redis_health_cache.expires_at = now + _CACHE_TTL
    if ok:
        return "realtime", "real-time mode"
    return "polling-degraded", "polling mode (Redis unavailable)"


_STATIC_DIR: Path = Path(__file__).resolve().parent.parent / "static"


def get_pg_pool(request: Request) -> asyncpg.Pool:
    """Dependency: yields the asyncpg pool from ``app.state``."""
    pool: asyncpg.Pool = request.app.state.pg_pool
    return pool


async def _refresh_clock_offset(pool: asyncpg.Pool = Depends(get_pg_pool)) -> None:
    """Router-level dependency: keep the app-to-database clock offset fresh."""
    await refresh_db_clock_offset(pool)


def get_backend(request: Request) -> Backend | None:
    """Dependency: yields the Backend from ``app.state`` if configured."""
    return getattr(request.app.state, "backend", None)


def get_rl_registry(request: Request) -> RateLimitRegistry:
    """Dependency: yields the RateLimitRegistry from ``app.state``.

    ``setup_admin_state`` always sets the key (bundle instance or
    singleton). The ``getattr`` fallback keeps hand-assembled
    ``app.state`` setups (which never set the key) working exactly as
    today: the module singleton. Internal — deliberately not exported.
    """
    rl: RateLimitRegistry | None = getattr(request.app.state, "rate_limit_registry", None)
    return rl if rl is not None else _rl_singleton


def get_schema(request: Request) -> str:
    """Dependency: yields the schema name from ``app.state``.

    Re-validates against :data:`_IDENT_RE` as defence-in-depth — the schema
    was validated at ``create_router`` construction time, but this ensures a
    runtime mutation of ``app.state.schema`` (e.g. by a misconfigured test
    fixture) cannot reach SQL interpolation.
    """
    s: str = request.app.state.schema
    if not _IDENT_RE.match(s):
        raise HTTPException(status_code=500, detail="invalid schema configuration")
    return s


def get_redis_client(request: Request) -> Any | None:
    """Dependency: yields the redis client from ``app.state``."""
    client: Any | None = request.app.state.redis_client
    return client


def get_templates(request: Request) -> Environment:
    """Dependency: yields the Jinja2 Environment from ``app.state``."""
    env: Environment = request.app.state.templates
    return env


def get_settings(request: Request) -> TaskQSettings:
    """Dependency: yields the TaskQSettings from ``app.state``."""
    s: TaskQSettings = request.app.state.settings
    return s


async def get_realtime_ctx(
    redis_client: Any = Depends(get_redis_client),
) -> tuple[str, str]:
    """Dependency: returns (realtime_mode, mode_label) for template rendering."""
    return await get_realtime_mode(redis_client)


def get_base_path(request: Request) -> str:
    """Dependency: yields the admin UI base path from ``app.state``."""
    s: str = request.app.state.base_path
    return s


_CSRF_COOKIE_NAME: str = "taskq_csrf_token"


def get_csrf_token(request: Request) -> str:
    """Dependency: returns the CSRF token.

    Prefers the token set by ``_CsrfRoute`` via ``request.state``
    so the form hidden field and the cookie always carry the same value.
    Falls back to the cookie (present from a prior GET), then generates
    a fresh token.
    """
    token = getattr(request.state, "_csrf_token", None)
    if token is not None:
        return token
    return request.cookies.get(_CSRF_COOKIE_NAME) or secrets.token_hex(32)


async def validate_csrf(request: Request) -> None:
    """Dependency: validates the synchronizer-token CSRF on POST requests."""
    cookie_token = request.cookies.get(_CSRF_COOKIE_NAME)
    if cookie_token is None:
        raise HTTPException(status_code=403, detail="CSRF token missing from cookies")
    form = await request.form()
    form_token = form.get("csrf_token")
    if not isinstance(form_token, str):
        raise HTTPException(status_code=403, detail="CSRF token missing from form")
    if not hmac.compare_digest(cookie_token, form_token):
        raise HTTPException(status_code=403, detail="CSRF token mismatch")


class _CsrfRoute(APIRoute):
    """Custom APIRoute that sets the CSRF cookie and the security headers.

    Uses the *synchronizer-token* pattern: the cookie is ``HttpOnly`` (JS
    cannot read it), and the server embeds the same token value in a hidden
    form field via the ``get_csrf_token`` dependency.  On POST, the server
    compares the two values using ``validate_csrf``.

    * ``httponly=True``  — prevents XSS-driven token theft
    * ``secure``         — ``secure_cookies``, a *configured* value; see below
    * ``samesite=strict`` — cookie never sent on cross-site requests

    Why ``secure`` is configured rather than derived from
    ``request.url.scheme``: behind a TLS-terminating edge (Azure Application
    Gateway, App Service) TLS ends at the gateway and the app sees plain
    ``http``, so the derived flag silently evaporated on exactly the
    deployments that most need it — while the session cookie, which already
    used a configured flag, kept it. The two cookies disagreeing about the
    same connection is the bug. Deployments that want the scheme to be
    observed accurately should run uvicorn with ``--proxy-headers`` so
    ``X-Forwarded-Proto`` is honoured; the warning below fires when the
    configured value and the observed scheme disagree.

    Subclass via :func:`_csrf_route_class` to bind the configured values;
    the class attributes here are the safe defaults.
    """

    secure_cookies: bool = True
    frame_ancestors: str = "none"
    scheme_mismatch_warned: bool = False

    def _apply_framing_headers(self, response: Response) -> None:
        """Refuse to be framed, on every admin response.

        Both headers, deliberately. ``frame-ancestors`` is the current
        standard and obsoletes ``X-Frame-Options``, but OWASP's clickjacking
        guidance is explicit that the mechanisms are independent and that more
        than one should be deployed where possible; XFO is what a browser too
        old for CSP Level 2 still honours, and a browser that supports both
        ignores XFO, so there is no conflict to resolve.

        The CSP carries ``frame-ancestors`` and nothing else on purpose. The
        admin templates use inline ``<script>`` blocks and inline ``style=``
        attributes, so any ``default-src``/``script-src`` policy tight enough
        to be worth having would break the UI, and one loose enough not to
        (``'unsafe-inline'``) would buy nothing. Tightening that is a
        template change, not a header change.

        Applied to every response, not only ``text/html``: a content-type
        test is one refactor away from a page that quietly loses the header,
        and the directive is inert on a JSON or static response.
        """
        ancestors = self.frame_ancestors
        response.headers["Content-Security-Policy"] = f"frame-ancestors '{ancestors}'"
        response.headers["X-Frame-Options"] = "DENY" if ancestors == "none" else "SAMEORIGIN"

    def _warn_on_scheme_mismatch(self, request: Request) -> None:
        """Warn once when the configured cookie policy contradicts the wire.

        Mirrors the loud ``admin-ui-no-auth`` warning: a misconfiguration that
        weakens the deployment should not be silent. Once per router, not per
        request — a per-request warning is a log flood that gets filtered out,
        which is the same as being silent.
        """
        if self.scheme_mismatch_warned or self.secure_cookies:
            return
        if request.url.scheme != "https":
            return
        type(self).scheme_mismatch_warned = True
        logger.warning(
            "admin-ui-cookie-scheme-mismatch",
            scheme=request.url.scheme,
            secure_cookies=self.secure_cookies,
            detail=(
                "TASKQ_ADMIN_UI_SECURE_COOKIES is false but this request "
                "arrived over HTTPS, so the admin UI's CSRF cookie is being "
                "issued without the Secure flag on a TLS connection: any "
                "plaintext request to the same host leaks it. Set "
                "TASKQ_ADMIN_UI_SECURE_COOKIES=true (the default) unless this "
                "is local http-only development. Note the inverse case needs "
                "no change: behind a TLS-terminating gateway the app sees "
                "http even though the browser used https, which is why this "
                "flag is configured rather than inferred - run uvicorn with "
                "--proxy-headers if you also want the observed scheme to be "
                "accurate."
            ),
        )

    def get_route_handler(self) -> Callable[..., Any]:
        original_handler = super().get_route_handler()

        async def csrf_aware_handler(request: Request) -> Response:
            if request.method == "GET":
                token = getattr(request.state, "_csrf_token", None)
                if token is None:
                    token = request.cookies.get(_CSRF_COOKIE_NAME) or secrets.token_hex(32)
                    request.state._csrf_token = token
            response = await original_handler(request)
            # All admin responses should not be cached by browsers/proxies.
            # Static files are exempt (served via _static.py with their own headers).
            ct = response.headers.get("content-type", "")
            if "text/html" in ct or "application/json" in ct:
                response.headers["Cache-Control"] = "no-cache"
            self._apply_framing_headers(response)
            self._warn_on_scheme_mismatch(request)
            if request.method == "GET":
                token = getattr(request.state, "_csrf_token", None)
                if token is None:
                    token = request.cookies.get(_CSRF_COOKIE_NAME) or secrets.token_hex(32)
                response.set_cookie(
                    _CSRF_COOKIE_NAME,
                    token,
                    httponly=True,
                    secure=self.secure_cookies,
                    samesite="strict",
                )
            return response

        return csrf_aware_handler


def _csrf_route_class(settings: TaskQSettings) -> type[_CsrfRoute]:
    """Bind the configured cookie/framing policy into a route class.

    A closure over settings rather than a read of ``request.app.state``: the
    admin router is mounted into a *host* application whose state the router
    does not own, and a security header that depends on the host remembering
    to call ``setup_admin_state`` is a header that can go missing.
    """

    class _ConfiguredCsrfRoute(_CsrfRoute):
        secure_cookies = settings.admin_ui_secure_cookies
        frame_ancestors = settings.admin_ui_frame_ancestors

    return _ConfiguredCsrfRoute


class _AppLike(Protocol):
    @property
    def state(self) -> Any: ...


@dataclass
class AdminBundle:
    """Returned by ``create_router()``; contains the router and all app.state values.

    Pass this to ``setup_admin_state(app, bundle)`` in your lifespan before
    the first request, then mount ``bundle.router`` via ``app.include_router``.

    ``rate_limit_registry`` scopes the registry the rate-limit/reservation
    pages read; ``None`` resolves to the module singleton (the default —
    same-process behavior is unchanged).
    """

    router: APIRouter
    templates: Environment
    pg_pool: asyncpg.Pool
    schema: str
    redis_client: Any | None
    settings: TaskQSettings
    base_path: str
    backend: Backend | None = None
    rate_limit_registry: RateLimitRegistry | None = None


def setup_admin_state(app: _AppLike, bundle: AdminBundle) -> None:
    """Populate ``app.state`` from *bundle* so route handler dependencies resolve.

    Call this in your FastAPI lifespan after creating the bundle and before
    the first request arrives.
    """
    app.state.pg_pool = bundle.pg_pool
    app.state.schema = bundle.schema
    app.state.redis_client = bundle.redis_client
    app.state.templates = bundle.templates
    app.state.settings = bundle.settings
    app.state.base_path = bundle.base_path
    app.state.backend = bundle.backend
    app.state.rate_limit_registry = (
        bundle.rate_limit_registry if bundle.rate_limit_registry is not None else _rl_singleton
    )


def create_router(
    pg_pool: asyncpg.Pool,
    *,
    schema: str = "taskq",
    redis_client: Any
    | None = None,  # Why: redis is an optional dependency (taskq[redis]); only runtime use is `is not None` boolean check — erasure boundary documented per erasure-boundary policy
    auth_dependency: Callable[..., Any] | None = None,
    base_path: str = "",
    backend: Backend | None = None,
    rate_limit_registry: RateLimitRegistry | None = None,
) -> AdminBundle:
    """Create the admin UI FastAPI router.

    Route handlers access shared resources (pool, schema, redis, settings,
    templates) via ``Depends(get_pg_pool)`` etc., which read from
    ``request.app.state``.  Call ``setup_admin_state(app, bundle)`` in your
    lifespan to populate those keys, then mount ``bundle.router`` at your
    chosen prefix via ``app.include_router``.

    ``base_path`` must match the prefix passed to ``include_router`` (e.g.
    ``"/admin"``).  It is injected as a Jinja2 global so templates can build
    prefix-safe URLs with ``{{ base_path }}/queues`` etc.

    ``rate_limit_registry`` is an optional owned :class:`RateLimitRegistry`
    the admin pages read configured primitives from (e.g. the API-process
    instance in a multi-process deployment).  Default ``None`` resolves to
    the module singleton — same-process behavior is unchanged.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    settings = TaskQSettings.load()

    env = Environment(
        autoescape=True,
        loader=PackageLoader("taskq.web", "templates"),
    )
    env.globals["base_path"] = base_path  # pyright: ignore[reportArgumentType]  # Why: Jinja2 Environment.globals accepts arbitrary values for template globals; str is valid.
    env.globals["poll_interval_ms"] = int(settings.admin_ui_polling_interval_seconds * 1000)  # pyright: ignore[reportArgumentType]  # Why: same as above; int is a valid template global.
    env.filters["time_ago"] = _time_ago
    env.filters["iso_attr"] = _iso_attr

    # Why router-level: the relative-time filter is synchronous and cannot
    # query, so the app-to-database clock offset it needs has to be refreshed
    # by something that can. Every admin route serves timestamps, so the
    # dependency belongs on the router rather than being repeated per page --
    # and repeating it per page is how one page would end up telling a
    # different story about staleness than the next. It is cached for
    # _CLOCK_OFFSET_TTL, so this is one extra query per 30 s, not per request.
    router_dependencies: list[Any] = [Depends(_refresh_clock_offset)]
    if auth_dependency is not None:
        router_dependencies.insert(0, Depends(auth_dependency))
    router_kwargs: dict[str, Any] = {
        "route_class": _csrf_route_class(settings),
        "dependencies": router_dependencies,
    }

    router = APIRouter(**router_kwargs)

    if auth_dependency is None:
        is_dev_env = settings.environment in {"dev", "development"}
        if not is_dev_env and settings.admin_ui_require_auth:
            raise RuntimeError(
                "admin UI requires auth_dependency in non-dev environments "
                "(set TASKQ_ADMIN_UI_REQUIRE_AUTH=false to disable)"
            )
        # Why this warning sits outside the environment test that governs the
        # RuntimeError above: a dev-labeled process is the only configuration
        # that actually serves an unauthenticated admin UI, so it is the one
        # that most needs a log line. Keeping the warning inside the non-dev
        # branch meant the silent case was the dangerous one.
        suppressed_by = (
            "TASKQ_ENVIRONMENT is a dev environment, so the fail-closed startup check did not run"
            if is_dev_env
            else "TASKQ_ADMIN_UI_REQUIRE_AUTH is false, so the fail-closed "
            "startup check was suppressed"
        )
        logger.warning(
            "admin-ui-no-auth",
            environment=settings.environment,
            detail=(
                "admin UI is being served with no authentication: "
                f"{suppressed_by}. Every admin route is reachable by anyone "
                "who can reach this port. This is unsafe if the process is "
                "actually serving production traffic: a production "
                "deployment mislabeled as dev disables this check and the "
                "health/metrics token check (TASKQ_HEALTH_REQUIRE_TOKEN) at "
                "the same time. Pass auth_dependency to create_router, or "
                "set TASKQ_ENVIRONMENT to the real environment so startup "
                "fails closed."
            ),
        )

    @router.get("/")
    async def index() -> RedirectResponse:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        return RedirectResponse(url="queues", status_code=302)

    _static.register(router, _STATIC_DIR)

    _discover_and_register(router)

    # ── Progress SSE / poll-state routes ────────────────────────────────
    # The admin UI's realtime.js connects to these endpoints for live
    # progress streaming.  Mount at /jobs so the paths become
    #   /jobs/api/job/{job_id}/progress/stream   (SSE)
    #   /jobs/api/job/{job_id}/state             (poll-state JSON)
    from taskq.web.progress import create_router as _create_progress_router

    progress_router = _create_progress_router(
        pg_pool,
        redis_client,
        schema=schema,
        auth_dependency=auth_dependency,
    )
    router.include_router(progress_router, prefix="/jobs")

    return AdminBundle(
        router=router,
        templates=env,
        pg_pool=pg_pool,
        schema=schema,
        redis_client=redis_client,
        settings=settings,
        base_path=base_path,
        backend=backend,
        rate_limit_registry=rate_limit_registry,
    )


def _discover_and_register(
    router: APIRouter,
) -> None:
    """Iterate sibling submodules and call their ``register()`` if present.

    Pages add a ``register()`` function to their own submodule — they
    never edit this file.  This follows the "decompose by composition, not
    accumulation" principle.
    """
    import taskq.web.admin as pkg

    for module_info in pkgutil.iter_modules(pkg.__path__):
        if module_info.name.startswith("_"):
            continue
        mod = importlib.import_module(f"taskq.web.admin.{module_info.name}")
        register_fn: Any = getattr(mod, "register", None)
        if callable(register_fn):
            register_fn(router)
