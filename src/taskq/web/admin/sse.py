"""SSE endpoint for the admin UI.

Wires PG LISTEN/NOTIFY to SSE so the admin UI receives real-time
state_change events. Falls back to keepalive-only when no PG pool
or schema is available. Connection count is bounded per-topic by an
asyncio.Semaphore sized from settings.admin_max_sse_connections.

Importing this module requires the ``taskq[fastapi]`` optional extra.
"""

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import asyncpg
import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from taskq.constants import events_channel
from taskq.settings import TaskQSettings
from taskq.web._sse_limit import SESSION_RECHECK_TIMEOUT_SECS
from taskq.web.admin._factory import (
    get_pg_pool,
    get_realtime_mode,
    get_redis_client,
    get_schema,
    get_session_verifier,
    get_settings,
)
from taskq.web.admin._listen import listen_with_reconnect

logger = structlog.get_logger("taskq.web.admin.sse")

_TOPIC_SEMAPHORES: dict[tuple[str, int], asyncio.Semaphore] = {}

_KEEPALIVE_INTERVAL: float = 30.0

_RECONNECT_BACKOFF_INITIAL: float = 1.0

_RECONNECT_BACKOFF_MAX: float = 30.0

_SSE_HEADERS: dict[str, str] = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
}


def _get_semaphore(topic: str, max_connections: int) -> asyncio.Semaphore:
    # The limit is part of the key, the same mechanism
    # taskq.web._sse_limit applies: the map is module-global and outlives
    # any one mount, so keying by topic alone let the FIRST mount's limit
    # silently govern every later mount of the same topic at a different
    # ``admin_max_sse_connections`` (the test suite mounts the admin
    # router repeatedly with different settings). A mount that disagrees
    # on the limit gets its own budget; mounts that agree share one.
    scoped = (topic, max_connections)
    if scoped not in _TOPIC_SEMAPHORES:
        _TOPIC_SEMAPHORES[scoped] = asyncio.Semaphore(max_connections)
    return _TOPIC_SEMAPHORES[scoped]


async def _sse_generator(
    semaphore: asyncio.Semaphore,
    resolve_pool: Callable[[], asyncpg.Pool | None],
    schema: str | None,
    session_verifier: Callable[[], Awaitable[bool]] | None = None,
) -> AsyncGenerator[str, None]:
    """Stream admin state_change events (or keepalives) as SSE strings.

    ``session_verifier`` is the request-scoped re-check for post-revocation
    auth (#316): it runs before the first frame and once per loop iteration --
    before every yielded event and at every keepalive tick -- and a failure
    ends the stream, the finally releasing the topic's semaphore slot. Any
    exception out of the verifier is treated as revocation (fail closed), and
    so is a check that outlives ``SESSION_RECHECK_TIMEOUT_SECS``: a hung
    host verifier must not freeze the generator inside its own keepalive
    path.
    """

    _recheck_count = {"n": 0}

    async def _session_still_valid() -> bool:
        if session_verifier is None:
            return True
        first_check = _recheck_count["n"] == 0
        try:
            _recheck_count["n"] += 1
            return bool(
                await asyncio.wait_for(
                    session_verifier(),
                    timeout=SESSION_RECHECK_TIMEOUT_SECS,
                )
            )
        except TimeoutError:
            # A verifier that outlives its bound is an unknown session
            # state, not a pass: fail closed, and say WHY -- a hung verifier
            # (wedged IdP introspection call) wedging the stream is a
            # different incident from a revoked session.
            logger.warning(
                "admin-sse-session-recheck-timeout",
                topic=schema,
                timeout_secs=SESSION_RECHECK_TIMEOUT_SECS,
                stream_phase="initial" if first_check else "streaming",
            )
            return False
        except Exception:
            # Fail closed: an unknown session state must not keep an
            # admin stream open.
            return False

    try:
        if not await _session_still_valid():
            logger.warning("admin-sse-session-revoked", topic=schema, stream_phase="initial")
            return

        yield 'event: status\ndata: {"status":"awaiting_progress_backend"}\n\n'

        pool = resolve_pool()
        use_pg = pool is not None and schema is not None

        if use_pg:
            channel = events_channel(schema)  # type: ignore[arg-type]  # Why: use_pg guard ensures schema is str at runtime

            def _live_pool() -> asyncpg.Pool:
                # Re-resolved on every LISTEN (re)connect: a credential
                # rotation replaces the admin pool under a running stream.
                current = resolve_pool()
                assert current is not None, "the admin pool was unset under a live SSE stream"
                return current

            async for payload in listen_with_reconnect(
                _live_pool,
                channel,
                keepalive_interval=_KEEPALIVE_INTERVAL,
                backoff_initial=_RECONNECT_BACKOFF_INITIAL,
                backoff_max=_RECONNECT_BACKOFF_MAX,
            ):
                # Once per iteration: bounds post-revocation staleness at one
                # keepalive interval (payload None) and gates every event.
                if not await _session_still_valid():
                    logger.warning(
                        "admin-sse-session-revoked", topic=schema, stream_phase="streaming"
                    )
                    return
                if payload is None:
                    yield ": keepalive\n\n"
                else:
                    yield f"event: state_change\ndata: {payload}\n\n"
        else:
            while True:
                if not await _session_still_valid():
                    logger.warning(
                        "admin-sse-session-revoked", topic=schema, stream_phase="streaming"
                    )
                    return
                await asyncio.sleep(_KEEPALIVE_INTERVAL)
                yield ": keepalive\n\n"
    finally:
        semaphore.release()


def register(router: APIRouter) -> None:
    """Attach the ``GET /sse/mode`` probe and the ``GET /sse/{topic}`` SSE endpoint.

    ``/sse/mode`` is registered FIRST: FastAPI matches routes in registration
    order, so the probe must exist before the ``{topic}`` catch-all or the
    topic route claims the path (and answers 400 for an unknown topic).
    """

    @router.get("/sse/mode")
    async def sse_mode(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        redis_client: Any | None = Depends(get_redis_client),
    ) -> JSONResponse:
        """The page-render Redis verdict, re-checked through the same 5 s cache.

        The admin UI's job-detail script polls this every 30 s so a page that
        rendered in polling-degraded mode upgrades itself when Redis returns,
        without a manual reload. This is the only reachable reachability probe
        a mounted admin router has: the worker health router's
        ``/jobs/health/ready`` answers a different process's deps and is not
        registered here, so the client used to 404 it forever.
        """
        mode, _label = await get_realtime_mode(redis_client)
        return JSONResponse({"realtime": mode == "realtime"})

    @router.get("/sse/{topic}")
    async def sse_endpoint(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        topic: str,
        request: Request,
        settings: TaskQSettings = Depends(get_settings),
        schema: str | None = Depends(get_schema),
        session_verifier: Callable[[Request], Awaitable[bool]] | None = Depends(
            get_session_verifier
        ),
    ) -> StreamingResponse:
        _valid_topics = frozenset({"queues", "jobs", "workers", "history"})
        if topic not in _valid_topics:
            raise HTTPException(status_code=400, detail=f"unknown SSE topic: {topic!r}")
        semaphore = _get_semaphore(topic, settings.admin_max_sse_connections)
        try:
            await asyncio.wait_for(semaphore.acquire(), timeout=0.001)
        except TimeoutError:
            raise HTTPException(
                status_code=429,
                detail="too many SSE connections for this topic",
            ) from None
        # Bind the request into the re-check once: the verifier reads the same
        # session cookie the request arrived with, and a session revoked
        # mid-stream fails the re-check even though those bytes are unchanged
        # (#316). None when the host wired no verifier: the stream then
        # authenticates once, the pre-#316 behavior.
        _session_verifier: Callable[[], Awaitable[bool]] | None = (
            (lambda: session_verifier(request)) if session_verifier is not None else None
        )
        gen = _sse_generator(semaphore, lambda: get_pg_pool(request), schema, _session_verifier)
        return StreamingResponse(
            content=gen,
            media_type="text/event-stream; charset=utf-8",
            headers=_SSE_HEADERS,
        )
