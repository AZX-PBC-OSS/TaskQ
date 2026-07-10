"""Admin UI router factory: Jinja2 setup, auth hook, route registration.

Importing this module requires the ``taskq[fastapi]`` optional extra.
"""

import asyncio
import hmac
import importlib
import pkgutil
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
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


def _time_ago(ts: Any) -> str:
    """Return a human-readable relative time string via humanize (e.g. '2 minutes ago')."""
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

        return humanize.naturaltime(datetime.now(UTC) - dt)
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


def get_backend(request: Request) -> Backend | None:
    """Dependency: yields the Backend from ``app.state`` if configured."""
    return getattr(request.app.state, "backend", None)


def get_schema(request: Request) -> str:
    """Dependency: yields the schema name from ``app.state``."""
    s: str = request.app.state.schema
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
    """Custom APIRoute that sets the CSRF cookie on every GET response.

    Uses the *synchronizer-token* pattern: the cookie is ``HttpOnly`` (JS
    cannot read it), and the server embeds the same token value in a hidden
    form field via the ``get_csrf_token`` dependency.  On POST, the server
    compares the two values using ``validate_csrf``.

    * ``httponly=True``  — prevents XSS-driven token theft
    * ``secure``         — True over HTTPS, False over HTTP (dev-compatible)
    * ``samesite=strict`` — cookie never sent on cross-site requests
    """

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
            if request.method == "GET":
                token = getattr(request.state, "_csrf_token", None)
                if token is None:
                    token = request.cookies.get(_CSRF_COOKIE_NAME) or secrets.token_hex(32)
                response.set_cookie(
                    _CSRF_COOKIE_NAME,
                    token,
                    httponly=True,
                    secure=request.url.scheme == "https",
                    samesite="strict",
                )
            return response

        return csrf_aware_handler


class _AppLike(Protocol):
    @property
    def state(self) -> Any: ...


@dataclass
class AdminBundle:
    """Returned by ``create_router()``; contains the router and all app.state values.

    Pass this to ``setup_admin_state(app, bundle)`` in your lifespan before
    the first request, then mount ``bundle.router`` via ``app.include_router``.
    """

    router: APIRouter
    templates: Environment
    pg_pool: asyncpg.Pool
    schema: str
    redis_client: Any | None
    settings: TaskQSettings
    base_path: str
    backend: Backend | None = None


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


def create_router(
    pg_pool: asyncpg.Pool,
    *,
    schema: str = "taskq",
    redis_client: Any
    | None = None,  # Why: redis is an optional dependency (taskq[redis]); only runtime use is `is not None` boolean check — erasure boundary documented per erasure-boundary policy
    auth_dependency: Callable[..., Any] | None = None,
    base_path: str = "",
    backend: Backend | None = None,
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

    router_kwargs: dict[str, Any] = {"route_class": _CsrfRoute}
    if auth_dependency is not None:
        router_kwargs["dependencies"] = [Depends(auth_dependency)]

    router = APIRouter(**router_kwargs)

    if auth_dependency is None and settings.environment not in {"dev", "development"}:
        if settings.admin_ui_require_auth:
            raise RuntimeError(
                "admin UI requires auth_dependency in non-dev environments "
                "(set TASKQ_ADMIN_UI_REQUIRE_AUTH=false to disable)"
            )
        logger.warning(
            "admin-ui-no-auth",
            environment=settings.environment,
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
