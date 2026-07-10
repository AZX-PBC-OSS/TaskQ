"""SSE endpoint for the admin UI.

Wires PG LISTEN/NOTIFY to SSE so the admin UI receives real-time
state_change events. Falls back to keepalive-only when no PG pool
or schema is available. Connection count is bounded per-topic by an
asyncio.Semaphore sized from settings.admin_max_sse_connections.

Importing this module requires the ``taskq[fastapi]`` optional extra.
"""

import asyncio
from collections.abc import AsyncGenerator

import asyncpg
import structlog
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from taskq.constants import events_channel
from taskq.settings import TaskQSettings
from taskq.web.admin._factory import get_pg_pool, get_schema, get_settings
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
    pool: asyncpg.Pool | None,
    schema: str | None,
    topic: str,
) -> AsyncGenerator[str, None]:
    try:
        yield 'event: status\ndata: {"status":"awaiting_progress_backend"}\n\n'

        use_pg = pool is not None and schema is not None

        if use_pg:
            channel = events_channel(schema)  # type: ignore[arg-type]  # Why: use_pg guard ensures schema is str at runtime
            async for payload in listen_with_reconnect(
                pool,  # type: ignore[arg-type]  # Why: use_pg guard ensures pool is not None at runtime
                channel,
                keepalive_interval=_KEEPALIVE_INTERVAL,
                backoff_initial=_RECONNECT_BACKOFF_INITIAL,
                backoff_max=_RECONNECT_BACKOFF_MAX,
            ):
                if payload is None:
                    yield ": keepalive\n\n"
                else:
                    yield f"event: state_change\ndata: {payload}\n\n"
        else:
            while True:
                await asyncio.sleep(_KEEPALIVE_INTERVAL)
                yield ": keepalive\n\n"
    finally:
        semaphore.release()


def register(router: APIRouter) -> None:
    """Attach the ``GET /sse/{topic}`` SSE endpoint to *router*."""

    @router.get("/sse/{topic}")
    async def sse_endpoint(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        topic: str,
        settings: TaskQSettings = Depends(get_settings),
        pool: asyncpg.Pool | None = Depends(get_pg_pool),
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
        gen = _sse_generator(semaphore, pool, schema, topic)
        return StreamingResponse(
            content=gen,
            media_type="text/event-stream; charset=utf-8",
            headers=_SSE_HEADERS,
        )
