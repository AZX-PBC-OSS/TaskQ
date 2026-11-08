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

from taskq._close import CLOSE_TIMEOUT_SECS
from taskq._shield import shield_with_retrieval
from taskq.constants import events_channel
from taskq.obs import (
    record_sse_connection_closed,
    record_sse_connection_opened,
    record_sse_rejection,
)
from taskq.settings import TaskQSettings
from taskq.web._sse_limit import session_gate
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
    # any one mount, so keying by topic alone let the first mount's limit
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
    topic: str = "",
) -> AsyncGenerator[str, None]:
    """Stream admin state_change events (or keepalives) as SSE strings.

    ``session_verifier`` is the request-scoped re-check for post-revocation
    auth : it runs before the first frame and once per loop iteration --
    before every yielded event and at every keepalive tick -- and a failure
    ends the stream, the finally releasing the topic's semaphore slot. Any
    exception out of the verifier is treated as revocation (fail closed), and
    so is a check that outlives ``SESSION_RECHECK_TIMEOUT_SECS``: a hung
    host verifier must not freeze the generator inside its own keepalive
    path.
    """

    _session_still_valid = session_gate(
        session_verifier,
        logger,
        timeout_warning="admin-sse-session-recheck-timeout",
        timeout_fields={"topic": schema},
    )

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

            feed = listen_with_reconnect(
                _live_pool,
                channel,
                keepalive_interval=_KEEPALIVE_INTERVAL,
                backoff_initial=_RECONNECT_BACKOFF_INITIAL,
                backoff_max=_RECONNECT_BACKOFF_MAX,
            )
            try:
                async for payload in feed:
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
            finally:
                # Deterministic, bounded release of the LISTEN connection.
                # An ``async for`` never closes its iterator, and every
                # exit that is not the feed's own exhaustion - a client
                # disconnect closes this generator, never the inner one -
                # abandons *feed* mid-iteration. Without this close, the
                # feed's finally (remove_listener, unlisten,
                # pool.release) runs only when the GC finalizes the
                # abandoned generator: an unbounded delay pinning a
                # session-scoped LISTEN connection against the pool's
                # cap. Bounded by the same close timeout every
                # TaskQ-initiated close uses (read at call time, so tests
                # can shrink it); a close that outlives the bound gives
                # up loudly-suppressed, no worse than the GC-driven
                # status quo it replaces.
                #
                # The close must also survive the very cancellation that
                # interrupts the stream. wait_for cancels the task it is
                # waiting on when the wait itself is cancelled, and a
                # cancelled stream re-delivers cancellation at every
                # checkpoint (an anyio cancel scope does; that is the
                # shape every ASGI server streams under). Unshielded, the
                # second delivery kills the close mid-flight, the feed's
                # finally dies before pool.release, and the connection
                # strands against the pool cap for the life of the
                # process: a generator abandoned inside its own finally
                # is terminated and can never be closed again. shield
                # detaches the close into its own task that runs to
                # completion - the release happens even when this exit is
                # cancelled - while the CancelledError shield re-raises
                # here is deliberately left uncaught (an except below
                # re-raises it) so the stream still ends cancelled.
                # shield_with_retrieval, not plain shield: the detached
                # close's outcome must be retrieved, not lost as asyncio
                # "Task exception was never retrieved" noise - a close
                # that outlives its bound while detached raises
                # TimeoutError into nobody (see taskq._shield).
                #
                # A close that outlives the bound gives up loudly (the
                # wait_for TimeoutError is logged below, and the feed's
                # own cleanup is cancellation-tolerant and finishes the
                # release detached - see _listen_pump.release_listen_connection),
                # no worse than the GC-driven status quo it replaces.
                try:
                    await shield_with_retrieval(
                        asyncio.wait_for(feed.aclose(), timeout=CLOSE_TIMEOUT_SECS)
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # The close failed on this (observed) path - a
                    # suppressed-in-the-feed infra error surfaced by the
                    # closer. Silent suppression would hide a release that
                    # never happened; the detached path is covered by
                    # shield_with_retrieval above.
                    logger.warning(
                        "admin-sse-listen-close-failed",
                        topic=schema,
                        error_type=type(exc).__name__,
                        error=str(exc),
                    )
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
        # The connections gauge moves where the slot moves: this
        # finally is the only release site the topic's semaphore has
        # (the 429 path never constructed this generator), so the level
        # counts a stream for exactly the span its slot was held.
        record_sse_connection_closed("admin", topic)


async def _holds_generator(
    semaphore: asyncio.Semaphore,
    resolve_pool: Callable[[], asyncpg.Pool | None],
    schema: str | None,
    session_verifier: Callable[[], Awaitable[bool]] | None = None,
    topic: str = "holds",
) -> AsyncGenerator[str, None]:
    """Stream the HITL broadcast as SSE ``hold`` frames — the
    TYPED listener's events (``HoldCreated`` / ``HoldResolved`` /
    ``HoldExpired`` / ``Backfilled``) straight to the browser, the open
    holds BACKFILLED at subscribe (a page render sees the pending
    approvals without a poll). The ``Backfilled`` reconcile snapshot
    rides every (re)backfill: a hold resolved or expired during an
    outage never announces its own death — the browser board drops any
    card the snapshot disowns. The resolve/deliver door is UNCHANGED —
    this is the read face only. The session re-check gates every
    frame exactly as the state_change feed's does; the exit runs one bounded,
    cancellation-shielded teardown whose listener stop sits in the frame
    close's inner finally (a client disconnect closes THIS generator; the
    frames unsubscribe, the pump stops, and that pump stop is what returns
    the dedicated LISTEN connection — the release is only as sustainable as
    the stop)."""
    from taskq.workflows.api._hitl_listen import (
        HitlListener,  # the import law (the import law): lazy, never at module scope
    )

    _session_still_valid = session_gate(
        session_verifier,
        logger,
        timeout_warning="admin-sse-session-recheck-timeout",
        timeout_fields={"topic": schema},
    )
    try:
        if not await _session_still_valid():
            logger.warning("admin-sse-session-revoked", topic=schema, stream_phase="initial")
            return
        pool = resolve_pool()
        if pool is None or schema is None:
            # The polling-degraded mode: keepalives only (the state feed's
            # own shape — a page that rendered degraded upgrades on
            # reload).
            while True:
                if not await _session_still_valid():
                    logger.warning(
                        "admin-sse-session-revoked", topic=schema, stream_phase="streaming"
                    )
                    return
                await asyncio.sleep(_KEEPALIVE_INTERVAL)
                yield ": keepalive\n\n"
            return
        listener = HitlListener(pool, schema=schema, keepalive_interval=_KEEPALIVE_INTERVAL)
        frames = listener.frames()
        try:
            await listener.start()
            async for frame in frames:
                if not await _session_still_valid():
                    logger.warning(
                        "admin-sse-session-revoked", topic=schema, stream_phase="streaming"
                    )
                    return
                if frame is None:
                    yield ": keepalive\n\n"
                else:
                    yield f"event: hold\ndata: {frame.model_dump_json()}\n\n"
        finally:
            # One bounded teardown owns both halves of this consumer's
            # exit: the frames close runs first (it unsubscribes this
            # consumer's queue from the fan-out), and the listener stop
            # runs in the close's inner finally (it cancels and reaps
            # the pump task — the pump stop is what returns the
            # dedicated LISTEN connection). Stop therefore runs on
            # every exit that reaches this teardown — a successful
            # close, the logged ordinary close failure, the close's
            # own timeout, or a BaseException out of the close,
            # cancellation included. The whole coroutine is awaited
            # exactly once through shield_with_retrieval: a
            # cancellation delivered here detaches the teardown — the
            # detached teardown and the stop's nested shielded release
            # cleanup may then outlive this caller, including at the
            # stop bound — while the cancellation still propagates out
            # of this generator and the detached outcome is retrieved
            # (and logged when it failed). The two bounds are the same
            # CLOSE_TIMEOUT_SECS, sequential and independent; they
            # bound TaskQ's cooperative waits only — neither universal
            # completion nor a universal wall-clock connection-release
            # guarantee (a cleanup that detached under the shield runs
            # until the backend operations themselves return). A
            # frames-close timeout can also defer the inner iterator's
            # own unsubscribe finalization; a stop that returns through
            # its reap phase still ends every subscriber by clearing
            # the fan-out registry, even when the inner frames close
            # timed out.
            async def teardown() -> None:
                try:
                    await asyncio.wait_for(frames.aclose(), timeout=CLOSE_TIMEOUT_SECS)
                except Exception as exc:
                    logger.warning(
                        "admin-sse-holds-close-failed",
                        topic=schema,
                        error_type=type(exc).__name__,
                        error=str(exc),
                    )
                finally:
                    await asyncio.wait_for(listener.stop(), timeout=CLOSE_TIMEOUT_SECS)

            await shield_with_retrieval(teardown())
    finally:
        semaphore.release()
        record_sse_connection_closed("admin", topic)


def register(router: APIRouter) -> None:
    """Attach the ``GET /sse/mode`` probe and the ``GET /sse/{topic}`` SSE endpoint.

    ``/sse/mode`` is registered first: FastAPI matches routes in registration
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
        registered here, so the admin page needs this same-process probe.
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
        _valid_topics = frozenset({"queues", "jobs", "workers", "history", "holds"})
        if topic not in _valid_topics:
            raise HTTPException(status_code=400, detail=f"unknown SSE topic: {topic!r}")
        semaphore = _get_semaphore(topic, settings.admin_max_sse_connections)
        try:
            await asyncio.wait_for(semaphore.acquire(), timeout=0.001)
        except TimeoutError:
            # The 429 site (the admin surface's half of the SSE-health
            # pair): the cap's refusals were invisible in metrics — a
            # client-side error only. topic is the closed vocabulary the
            # 400 guard above validated, so the label set stays bounded.
            record_sse_rejection("admin", topic)
            raise HTTPException(
                status_code=429,
                detail="too many SSE connections for this topic",
            ) from None
        record_sse_connection_opened("admin", topic)
        # Bind the request into the re-check once: the verifier reads the same
        # session cookie the request arrived with, and a session revoked
        # mid-stream fails the re-check even though those bytes are unchanged
        # . None when the host wired no verifier: the stream then
        # authenticates once at subscribe only, with no mid-stream re-check.
        _session_verifier: Callable[[], Awaitable[bool]] | None = (
            (lambda: session_verifier(request)) if session_verifier is not None else None
        )
        if topic == "holds":
            # the typed listener's events — backfilled at subscribe, then the
            # live NOTIFYs.
            gen = _holds_generator(
                semaphore,
                lambda: get_pg_pool(request),
                schema,
                _session_verifier,
                topic=topic,
            )
        else:
            gen = _sse_generator(
                semaphore,
                lambda: get_pg_pool(request),
                schema,
                _session_verifier,
                topic=topic,
            )
        return StreamingResponse(
            content=gen,
            media_type="text/event-stream; charset=utf-8",
            headers=_SSE_HEADERS,
        )
