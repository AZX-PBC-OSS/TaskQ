"""SSE endpoint for the admin UI.

Wires PG LISTEN/NOTIFY to SSE so the admin UI receives real-time
state_change events. Falls back to keepalive-only when no PG pool
or schema is available. Connection count is bounded per-topic by an
asyncio.Semaphore sized from settings.admin_max_sse_connections.

Importing this module requires the ``taskq[fastapi]`` optional extra.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable
from typing import Any

import asyncpg
import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from taskq._close import CLOSE_TIMEOUT_SECS
from taskq.constants import events_channel
from taskq.settings import TaskQSettings
from taskq.web.admin._factory import (
    get_pg_pool,
    get_realtime_mode,
    get_redis_client,
    get_schema,
    get_settings,
)
from taskq.web.admin._listen import listen_with_reconnect

logger = structlog.get_logger("taskq.web.admin.sse")

_TOPIC_SEMAPHORES: dict[str, asyncio.Semaphore] = {}

_KEEPALIVE_INTERVAL: float = 30.0

_RECONNECT_BACKOFF_INITIAL: float = 1.0

_RECONNECT_BACKOFF_MAX: float = 30.0

_SSE_HEADERS: dict[str, str] = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",
}


def _get_semaphore(topic: str, max_connections: int) -> asyncio.Semaphore:
    if topic not in _TOPIC_SEMAPHORES:
        _TOPIC_SEMAPHORES[topic] = asyncio.Semaphore(max_connections)
    return _TOPIC_SEMAPHORES[topic]


async def _sse_generator(
    semaphore: asyncio.Semaphore,
    resolve_pool: Callable[[], asyncpg.Pool | None],
    schema: str | None,
) -> AsyncGenerator[str, None]:
    try:
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

            feed = listen_with_reconnect(
                _live_pool,
                channel,
                keepalive_interval=_KEEPALIVE_INTERVAL,
                backoff_initial=_RECONNECT_BACKOFF_INITIAL,
                backoff_max=_RECONNECT_BACKOFF_MAX,
            )
            try:
                async for payload in feed:
                    if payload is None:
                        yield ": keepalive\n\n"
                    else:
                        yield f"event: state_change\ndata: {payload}\n\n"
            finally:
                # Deterministic, bounded release of the LISTEN connection.
                # An ``async for`` never closes its iterator, and every
                # exit that is not the feed's own exhaustion - a client
                # disconnect closes THIS generator, never the inner one -
                # abandons *feed* mid-iteration. Without this close, the
                # feed's finally (remove_listener, UNLISTEN,
                # pool.release) runs only when the GC finalizes the
                # abandoned generator: an unbounded delay pinning a
                # session-scoped LISTEN connection against the pool's
                # cap. Bounded by the same close timeout every
                # TaskQ-initiated close uses (read at call time, so tests
                # can shrink it); a close that outlives the bound gives
                # up loudly-suppressed, no worse than the GC-driven
                # status quo it replaces.
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(feed.aclose(), timeout=CLOSE_TIMEOUT_SECS)
        else:
            while True:
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
        gen = _sse_generator(semaphore, lambda: get_pg_pool(request), schema)
        return StreamingResponse(
            content=gen,
            media_type="text/event-stream; charset=utf-8",
            headers=_SSE_HEADERS,
        )
