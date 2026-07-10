"""FastAPI router: SSE progress bridge and poll-state endpoint.

Bridges Redis pub/sub progress events to Server-Sent Events for browsers and
API clients.  Mount at ``prefix="/jobs"`` to produce the canonical URLs:

    GET /jobs/api/job/{job_id}/progress/stream  — SSE stream
    GET /jobs/api/job/{job_id}/state            — poll-state (JSON)

Importing this module requires the ``taskq[fastapi]`` optional extra (which
includes ``sse-starlette``).

Design notes
------------
- Subscribe-before-query: the Redis channel is subscribed BEFORE the PG
  snapshot read to eliminate the race window where an event published between
  the PG read and the subscribe would be silently lost.
- The PG connection is released immediately after the initial row fetch; no
  PG connection is held during streaming.
- The Redis subscribe and PG query happen in the handler body (before
  ``EventSourceResponse`` is created) so that 404/503 errors produce proper
  HTTP status codes rather than appearing inside an already-started SSE stream.
- Keepalive comments are emitted every ``sse_heartbeat_interval`` seconds via
  a ``get_message(timeout=...)`` polling loop (avoids blocking ``listen()``
  which has no per-message timeout support).
- On client disconnect, ``try/finally`` in the generator calls
  ``pubsub.unsubscribe()`` and ``pubsub.aclose()`` to prevent stale Redis
  subscriptions.
"""

import contextlib
from collections.abc import AsyncGenerator, Callable
from datetime import timedelta
from typing import Any, cast
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from sse_starlette.event import ServerSentEvent
from sse_starlette.sse import EventSourceResponse

from taskq import _json
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it
    progress_channel,
)
from taskq.progress._events import ProgressEvent

logger = structlog.get_logger("taskq.web.progress")

# ------------------------------------------------------------------
# Wire-format constants
# ------------------------------------------------------------------

# Cache-Control: no-cache keeps the SSE semantics (revalidation allowed)
# rather than no-store (no caching at all).  The EventSourceResponse default
# is no-store; we override it here.  X-Accel-Buffering and Connection are set
# automatically by EventSourceResponse but Cache-Control must be overridden.
_SSE_HEADERS: dict[str, str] = {
    "Cache-Control": "no-cache",
}

_SSE_SEPARATOR = "\n"

# Returned when Redis is not configured or unreachable at subscribe time.
_REDIS_503_BODY: dict[str, str] = {"error": "redis_not_configured"}

# SQL for the progress snapshot read (both initial connect and reconnect).
_PROGRESS_SQL = 'SELECT progress_state, progress_seq, status FROM "{schema}".jobs WHERE id = $1'


# ------------------------------------------------------------------
# Internal helpers
# ------------------------------------------------------------------


def _make_sse_event(*, event: str, seq: int, data: str) -> ServerSentEvent:
    """Return a ``ServerSentEvent`` for a progress/terminal payload."""
    return ServerSentEvent(
        data=data,
        event=event,
        id=str(seq),
        sep=_SSE_SEPARATOR,
    )


def _make_done_event() -> ServerSentEvent:
    """Return the ``event: done`` closing signal (no id, no data)."""
    return ServerSentEvent(event="done", sep=_SSE_SEPARATOR)


def _make_keepalive() -> ServerSentEvent:
    """Return a ``: keepalive`` SSE comment."""
    return ServerSentEvent(comment="keepalive", sep=_SSE_SEPARATOR)


def _serialize_progress_state(progress_state: Any) -> str:
    """Serialize the PG ``progress_state`` jsonb value to an SSE data string."""
    if progress_state is None:
        return "{}"
    if isinstance(progress_state, (str, bytes)):
        # asyncpg may decode jsonb as a str; pass through as-is
        return progress_state if isinstance(progress_state, str) else progress_state.decode()
    # asyncpg returns a dict for jsonb; re-serialize with our json module
    return _json.dumps_str(progress_state)


def _resolve_last_event_id(
    request: Request,
    query_param: int | None,
) -> int | None:
    """Resolve ``last_event_id`` from ``Last-Event-ID`` header (priority) or query param.

    Per WHATWG SSE spec §9.2.1: the ``Last-Event-ID`` header is sent
    automatically by the browser ``EventSource`` on reconnect; the query
    parameter is a curl/debugging convenience.  Header wins when both present.
    """
    header_val = request.headers.get("Last-Event-ID")
    if header_val is not None:
        try:
            return int(header_val)
        except ValueError:
            return None
    return query_param


# ------------------------------------------------------------------
# SSE event generator
# ------------------------------------------------------------------


async def _event_generator(
    pubsub: Any,
    channel: str,
    job_id: UUID,
    is_terminal: bool,
    progress_seq: int,
    progress_data: str,
    resolved_last_event_id: int | None,
    heartbeat_secs: float,
) -> AsyncGenerator[ServerSentEvent, None]:
    """Yield SSE events from Redis pub/sub after the initial PG snapshot.

    This is the core streaming loop extracted from ``progress_stream`` so that
    unit tests can exercise the real production generator directly rather than
    reimplementing it.
    """
    try:
        last_emitted_seq: int
        if resolved_last_event_id is None:
            event_type = "terminal" if is_terminal else "progress"
            yield _make_sse_event(event=event_type, seq=progress_seq, data=progress_data)
            last_emitted_seq = progress_seq
            if is_terminal:
                yield _make_done_event()
                return
        else:
            if progress_seq > resolved_last_event_id:
                event_type = "terminal" if is_terminal else "progress"
                yield _make_sse_event(
                    event=event_type,
                    seq=progress_seq,
                    data=progress_data,
                )
                last_emitted_seq = progress_seq
                if is_terminal:
                    yield _make_done_event()
                    return
            else:
                last_emitted_seq = max(0, resolved_last_event_id)

        while True:
            raw_msg = await pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=heartbeat_secs,
            )

            if raw_msg is None:
                yield _make_keepalive()
                continue

            raw_data: Any = raw_msg.get("data")
            if raw_data is None:
                continue

            raw_str: str
            try:
                raw_str = (
                    raw_data.decode("utf-8")
                    if isinstance(raw_data, (bytes, bytearray))
                    else str(raw_data)
                )
                event = ProgressEvent.model_validate_json(raw_str)
            except Exception:  # Why: malformed/non-ProgressEvent messages on the shared channel must be discarded silently; the channel is not exclusively owned by this library.
                logger.debug(
                    "sse-redis-malformed-message",
                    job_id=str(job_id),
                    channel=channel,
                )
                continue

            # filter duplicates.
            if event.seq <= last_emitted_seq:
                continue

            last_emitted_seq = event.seq
            event_json = event.model_dump_json(exclude_none=True)

            if event.terminal:
                # terminal event — emit payload then done, close.
                yield _make_sse_event(
                    event="terminal",
                    seq=event.seq,
                    data=event_json,
                )
                yield _make_done_event()
                return

            yield _make_sse_event(
                event="progress",
                seq=event.seq,
                data=event_json,
            )

    finally:
        # always release the Redis subscription; errors here
        # must not mask the primary exception.
        with contextlib.suppress(Exception):
            await pubsub.unsubscribe(channel)
        with contextlib.suppress(Exception):
            await pubsub.aclose()


# ------------------------------------------------------------------
# Public factory
# ------------------------------------------------------------------


def create_router(
    pg_pool: asyncpg.Pool,
    redis_client: Any,  # redis.asyncio.Redis | None — typed Any at erasure boundary; redis is an optional dep
    *,
    schema: str = "taskq",
    auth_dependency: Callable[..., Any] | None = None,
    sse_heartbeat_interval: timedelta = timedelta(seconds=15),
) -> APIRouter:
    """Return a FastAPI ``APIRouter`` exposing the SSE progress bridge.

    Mount at ``prefix="/jobs"`` to produce the canonical paths::

        GET /jobs/api/job/{job_id}/progress/stream
        GET /jobs/api/job/{job_id}/state

    Parameters
    ----------
    pg_pool:
        asyncpg connection pool for snapshot reads and poll-state queries.
    redis_client:
        ``redis.asyncio.Redis`` instance, or ``None`` when Redis is not
        configured.  If ``None``, the SSE endpoint returns HTTP 503.
    schema:
        PostgreSQL schema name (default ``"taskq"``).
    auth_dependency:
        Optional FastAPI dependency callable; if provided it is injected via
        ``Depends()`` on all routes (same pattern as
        ``taskq.web.admin.create_router``).
    sse_heartbeat_interval:
        Cadence for ``': keepalive'`` SSE comments (default 15 s).
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    router_kwargs: dict[str, Any] = {"tags": ["progress"]}
    if auth_dependency is not None:
        router_kwargs["dependencies"] = [Depends(auth_dependency)]

    router = APIRouter(**router_kwargs)

    _schema = schema
    _redis_client = redis_client
    _pg_pool = pg_pool
    _heartbeat_secs = sse_heartbeat_interval.total_seconds()
    _progress_sql = _PROGRESS_SQL.format(schema=_schema)

    # ----------------------------------------------------------------
    # SSE endpoint
    # ----------------------------------------------------------------

    @router.get(
        "/api/job/{job_id}/progress/stream",
        response_class=EventSourceResponse,
    )
    async def progress_stream(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        job_id: UUID,
        request: Request,
        last_event_id: int | None = None,
    ) -> EventSourceResponse:
        """Stream progress events for a job via SSE.

        On initial connection (no ``last_event_id`` / ``Last-Event-ID``
        header): emits the current PG snapshot, then streams Redis events.

        On reconnect (``last_event_id`` present): subscribes Redis FIRST,
        emits one catch-up event from PG if ``progress_seq > last_event_id``,
        then resumes streaming.

        HTTP 404 — job not found.
        HTTP 503 — Redis not configured or unavailable.
        """
        if _redis_client is None:
            return JSONResponse(  # pyright: ignore[reportReturnType]  # Why: FastAPI accepts any Response subclass here; JSONResponse is returned for the 503 before SSE upgrade.
                status_code=503,
                content=_REDIS_503_BODY,
                headers={"Retry-After": "2"},
            )

        resolved_last_event_id = _resolve_last_event_id(request, last_event_id)
        channel = progress_channel(_schema, job_id)

        # ------------------------------------------------------------------
        # Phase 1: subscribe-before-query.
        #
        # Both the Redis subscribe and the PG query run in the handler body
        # (before EventSourceResponse is constructed) so that 404/503 errors
        # are returned as proper HTTP status codes rather than appearing mid-
        # stream after a 200 has already been sent.
        # ------------------------------------------------------------------

        pubsub = _redis_client.pubsub()
        try:
            await pubsub.subscribe(channel)
        except Exception as exc:
            logger.warning(
                "sse-redis-subscribe-failed",
                job_id=str(job_id),
                channel=channel,
                error=str(exc),
            )
            with contextlib.suppress(Exception):
                await pubsub.aclose()
            return JSONResponse(  # pyright: ignore[reportReturnType]  # Why: FastAPI accepts any Response subclass here; JSONResponse is returned for the 503 before SSE upgrade.
                status_code=503,
                content=_REDIS_503_BODY,
                headers={"Retry-After": "2"},
            )

        # short-lived PG connection — released before any SSE
        # byte is written.
        try:
            async with _pg_pool.acquire() as conn:
                row = await conn.fetchrow(_progress_sql, job_id)
        except Exception:
            # Cleanup must not mask the original exception from the PG query.
            with contextlib.suppress(Exception):
                await pubsub.unsubscribe(channel)
            with contextlib.suppress(Exception):
                await pubsub.aclose()
            raise

        if row is None:
            with contextlib.suppress(Exception):
                await pubsub.unsubscribe(channel)
            with contextlib.suppress(Exception):
                await pubsub.aclose()
            raise HTTPException(status_code=404, detail="job not found")

        # Extract snapshot data from PG row.
        raw_progress_state: Any = row["progress_state"]
        progress_seq: int = row["progress_seq"]
        status: str = row["status"]
        is_terminal = status in TERMINAL_STATUSES
        progress_data = _serialize_progress_state(raw_progress_state)

        # ------------------------------------------------------------------
        # Phase 2: build and return EventSourceResponse.
        #
        # The generator owns pubsub from here; the try/finally inside
        # _event_generator ensures cleanup even on client disconnect
        # (CancelledError).
        # ------------------------------------------------------------------

        return EventSourceResponse(
            content=_event_generator(
                pubsub=pubsub,
                channel=channel,
                job_id=job_id,
                is_terminal=is_terminal,
                progress_seq=progress_seq,
                progress_data=progress_data,
                resolved_last_event_id=resolved_last_event_id,
                heartbeat_secs=_heartbeat_secs,
            ),
            headers=_SSE_HEADERS,
            # Effectively disable sse-starlette's built-in ping; we emit our own
            # keepalive comments via the get_message timeout loop.  ping=0
            # causes a tight loop (anyio.sleep(0) returns immediately), so we
            # use a 24-hour interval that will never fire in practice.
            ping=86_400,
            sep=_SSE_SEPARATOR,
        )

    # ----------------------------------------------------------------
    # Poll-state endpoint
    # ----------------------------------------------------------------

    @router.get("/api/job/{job_id}/state")
    async def job_state(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        job_id: UUID,
    ) -> JSONResponse:
        """Return the current progress state for a job (polling fallback).

        Response body::

            {"status": <str>, "progress_state": <dict | null>, "progress_seq": <int>}

        HTTP 404 — job not found.
        """
        async with _pg_pool.acquire() as conn:
            row = await conn.fetchrow(_progress_sql, job_id)

        if row is None:
            raise HTTPException(status_code=404, detail="job not found")

        raw_ps: Any = row["progress_state"]
        progress_state: dict[str, object] | None
        if raw_ps is None:
            progress_state = None
        elif isinstance(raw_ps, dict):
            progress_state = cast("dict[str, object]", raw_ps)
        else:
            # asyncpg may return a str for jsonb; parse it.
            parsed: Any = _json.loads(raw_ps)
            progress_state = cast("dict[str, object]", parsed) if isinstance(parsed, dict) else None

        return JSONResponse(
            content={
                "status": row["status"],
                "progress_state": progress_state,
                "progress_seq": row["progress_seq"],
            }
        )

    return router
