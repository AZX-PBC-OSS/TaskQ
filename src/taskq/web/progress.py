"""FastAPI router: SSE progress bridge and poll-state endpoint.

Bridges Redis pub/sub progress events to Server-Sent Events for browsers and
API clients.  Mount at ``prefix="/jobs"`` to produce the canonical URLs:

    GET /jobs/api/job/{job_id}/progress/stream , SSE stream
    GET /jobs/api/job/{job_id}/state           , poll-state (JSON)

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
- Each broker read carries an app-level deadline: the delegated ``timeout=``
  bounds only the read itself, while a reconnect inside redis-py's
  ``parse_response`` (broker dropped mid-read) is bounded by nothing, so the
  loop wraps every read in ``asyncio.wait_for`` and a read that outlives the
  deadline ends the stream, exactly as broker death does.
- On client disconnect, ``try/finally`` in the generator calls
  ``pubsub.unsubscribe()`` and ``pubsub.aclose()`` to prevent stale Redis
  subscriptions.
- Session re-check (#316): the router-level ``Depends(auth_dependency)`` runs
  once at request acceptance; a stream that authenticated once would otherwise
  keep delivering frames long after its session was revoked. The generator
  therefore re-invokes an optional ``session_verifier`` -- the re-check the
  auth dependency itself exposes (``create_auth_dependency`` attaches one) --
  before every yielded event and at every keepalive tick, and ends the stream
  on failure. The finally releases the SSE slot and the Redis subscription.
  Bounded staleness: at most one keepalive interval between the revocation and
  the stream ending; the effective keepalive interval is capped at
  ``_SSE_HEARTBEAT_CAP_SECS`` so the bound holds no matter how a host
  configures ``sse_heartbeat_interval`` (the clamp is logged at startup).
  The re-check itself is bounded too (``SESSION_RECHECK_TIMEOUT_SECS``): a
  verifier that hangs is fail-closed revocation, never a frozen generator.
  The check is cookie-only -- no session store, no DB round trip -- so
  server-side revocation of a still-validly-signed cookie is impossible by
  construction (docs/guides/sso.md states the same limits).
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Awaitable, Callable
from datetime import timedelta
from typing import Any, cast
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from sse_starlette.event import ServerSentEvent
from sse_starlette.sse import EventSourceResponse

from taskq import _json
from taskq._close import CLOSE_TIMEOUT_SECS, close_redis_bounded
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.client._taskq import orjson_response_class
from taskq.constants import (
    _IDENT_RE,  # pyright: ignore[reportPrivateUsage]  # Why: reusing the canonical identifier regex rather than redefining it
    progress_channel,
)
from taskq.obs import get_meter
from taskq.settings import TaskQSettings
from taskq.web._pool import BoundedPool
from taskq.web._sse_limit import SESSION_RECHECK_TIMEOUT_SECS, acquire_sse_slot

logger = structlog.get_logger("taskq.web.progress")

_meter = get_meter()

_sse_malformed_counter = _meter.create_counter(
    name="taskq.sse.malformed_messages",
    description=(
        "Total Redis pub/sub messages discarded by the SSE progress bridge "
        "because they did not parse to the ProgressEvent envelope. The "
        "channel is not exclusively owned by this library so a drop is "
        "the correct outcome, but it must be visible: without the counter "
        "a publisher that stopped emitting the envelope is "
        "indistinguishable from a quiet channel."
    ),
    unit="1",
)

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

# Grace added to the delegated heartbeat timeout to form the app-level
# deadline on each broker read in the streaming loop. get_message's own
# ``timeout=`` bounds only the read; redis-py's PubSub.parse_response
# re-enters the connection when the broker dropped (a reconnect with no
# connect timeout of its own), so a wedged/tarpitted broker can stall one
# read far past the heartbeat cadence while the subscription, the asyncio
# task, and the SSE slot stay pinned. Every TaskQ-initiated wait on a
# possibly-dead broker is bounded by this codebase (the admin health ping's
# 0.5 s wait_for in admin/_factory.py, every close via close_redis_bounded);
# the read gets the same treatment, one heartbeat window for the read
# itself, plus the same grace the health ping allows.
_BROKER_READ_GRACE_SECS: float = 0.5

# Ceiling for a wire envelope's ``seq``: the durable cursor is the jobs
# table's ``progress_seq int`` column (migration 01.00.00_01), so the int4
# domain bounds every seq the healthy pipeline can issue. The SSE
# generator discards out-of-domain envelopes as malformed rather than
# advancing its dedup cursor into a range no future event can reach.
_MAX_PROGRESS_SEQ: int = 2**31 - 1

# Cap on the effective keepalive/re-check cadence of a live stream. The
# keepalive tick is what bounds post-revocation staleness (#316): the
# generator re-checks the session once per tick, so a host configuring
# ``sse_heartbeat_interval`` to hours would silently move the revocation
# bound to hours. Whatever the configured interval, a quiet stream re-checks
# at least every 60 seconds; the same cap governs sse-starlette's fallback
# ping below.
_SSE_HEARTBEAT_CAP_SECS: float = 60.0

# JSON responses render through orjson (taskq._json), never stdlib json ,
# byte-identical bodies to starlette's stdlib JSONResponse for these payloads.
_OrjsonJSONResponse: "type[JSONResponse]" = orjson_response_class()

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

    Every id this stream issues is a non-negative integer sequence number,
    so a header that is not one cannot have come from it - a hand-rolled
    client or a proxy rewriting headers - and is rejected with a 400
    naming the header. Reading it as "no cursor" instead would replay the
    stream from the snapshot and silently shadow a valid query parameter.
    """
    header_val = request.headers.get("Last-Event-ID")
    if header_val is not None:
        try:
            resolved = int(header_val)
        except ValueError:
            resolved = -1
        if resolved < 0:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Last-Event-ID must be a non-negative integer sequence number issued by "
                    f"this stream, got {header_val[:64]!r}"
                ),
            )
        return resolved
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
    sse_slot_semaphore: asyncio.Semaphore | None = None,
    session_verifier: Callable[[], Awaitable[bool]] | None = None,
) -> AsyncGenerator[ServerSentEvent, None]:
    """Yield SSE events from Redis pub/sub after the initial PG snapshot.

    This is the core streaming loop extracted from ``progress_stream`` so that
    unit tests can exercise the real production generator directly rather than
    reimplementing it.

    ``session_verifier`` is a request-scoped re-check (zero-argument here; the
    route binds the request) returning whether the session that opened the
    stream is still valid. When given, it runs before the initial snapshot and
    once per streaming-loop iteration -- before every yielded event and at
    every keepalive tick -- and a failure ends the stream: the finally below
    releases the SSE slot and unsubscribes/closes the Redis subscription. Any
    exception out of the verifier is treated as revocation (fail closed); the
    browser's EventSource reconnects and is refused at the door.
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
            # state, not a pass: fail closed, but say WHY -- a hung verifier
            # (wedged IdP introspection call) wedging the stream is a
            # different incident from a revoked session, and the revocation
            # warning below would misname it.
            logger.warning(
                "sse-session-recheck-timeout",
                job_id=str(job_id),
                channel=channel,
                timeout_secs=SESSION_RECHECK_TIMEOUT_SECS,
                stream_phase="initial" if first_check else "streaming",
            )
            return False
        except Exception:
            # Fail closed: an unknown session state must not keep a
            # privileged stream open. Logged below via the shared
            # revocation path's caller.
            return False

    try:
        if not await _session_still_valid():
            logger.warning(
                "sse-session-revoked",
                job_id=str(job_id),
                channel=channel,
                stream_phase="initial",
            )
            return

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
            try:
                raw_msg = await asyncio.wait_for(
                    pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=heartbeat_secs,
                    ),
                    timeout=heartbeat_secs + _BROKER_READ_GRACE_SECS,
                )
            except TimeoutError:
                # Fail-visible, matching broker death mid-stream: the stream
                # ends (the browser EventSource reconnects) and the finally
                # below releases the subscription and the SSE slot. A read
                # that outlives the deadline is reported, never silently
                # retried into another unbounded wait.
                logger.warning(
                    "sse-redis-read-timeout",
                    job_id=str(job_id),
                    channel=channel,
                    read_bound_secs=heartbeat_secs + _BROKER_READ_GRACE_SECS,
                )
                raise

            # Session re-check (#316): once per streaming-loop iteration --
            # this point is both the keepalive tick and "before each yielded
            # event" -- and the read above suspends at most one heartbeat, so
            # a revoked session ends the stream within one keepalive
            # interval, never delivering another frame.
            if not await _session_still_valid():
                logger.warning(
                    "sse-session-revoked",
                    job_id=str(job_id),
                    channel=channel,
                    stream_phase="streaming",
                )
                return

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
                # Performance: the published payload is already exactly
                # ``ProgressEvent.model_dump_json(exclude_none=True)`` bytes
                # (progress/_publish.py), and this generator reads only
                # ``seq`` and ``terminal``, so a pydantic validate→dump
                # round-trip per message per client (O(clients x events))
                # re-derives bytes the channel already carries. Parse the
                # envelope once with orjson, validate the required keys
                # cheaply, and emit the raw string verbatim.
                parsed: object = _json.loads(raw_str)
                if not isinstance(parsed, dict):
                    raise ValueError("envelope must be a JSON object")
                envelope = cast("dict[str, object]", parsed)
                seq = envelope["seq"]
                if not isinstance(seq, int) or isinstance(seq, bool):
                    raise ValueError("seq must be an integer")
                if not (0 <= seq <= _MAX_PROGRESS_SEQ):
                    # The seq cursor is the durable row's int4
                    # ``progress_seq`` (migration 01.00.00_01), so no
                    # honest event can carry a value outside the int4
                    # domain. A lie that passes the type check with a
                    # huge value would advance ``last_emitted_seq`` past
                    # every future event and starve this stream into a
                    # blackhole; out-of-domain is malformed, discarded
                    # like any other malformed message.
                    raise ValueError("seq outside the int4 cursor domain")
                if envelope["job_id"] != str(job_id):
                    # The crossed-wire check: the channel is per-job, but
                    # a lying proxy can deliver ANOTHER job's envelope on
                    # it. Without this compare the foreign event would be
                    # forwarded onto this stream (its seq could even gate
                    # this job's future events). Off-wire deliverables
                    # are malformed here, discarded like any other.
                    raise ValueError("envelope is for a different job")
                terminal = envelope.get("terminal", False)
                if not isinstance(terminal, bool):
                    raise ValueError("terminal must be a boolean")
                if envelope.get("kind") not in ("progress", "state_change"):
                    raise ValueError("kind must be a progress/state_change literal")
                # ProgressEvent declares these fields with no default, so a
                # message missing any of them failed full model validation
                # before; the cheap membership check keeps that drop behaviour.
                # Their VALUES are not type-checked here: published wire
                # always satisfies the model, and foreign junk that mimics
                # the envelope shape is forwarded as inert JSON.
                for _required in ("job_id", "actor", "ts", "status"):
                    if _required not in envelope:
                        raise ValueError(f"missing {_required!r}")
            except Exception:  # Why: malformed/non-ProgressEvent messages on the shared channel must be discarded with their counter, never crash the stream; the channel is not exclusively owned by this library.
                _sse_malformed_counter.add(1)
                logger.debug(
                    "sse-redis-malformed-message",
                    job_id=str(job_id),
                    channel=channel,
                )
                continue

            # filter duplicates.
            if seq <= last_emitted_seq:
                continue

            last_emitted_seq = seq

            if terminal:
                # terminal event, emit payload then done, close.
                yield _make_sse_event(
                    event="terminal",
                    seq=seq,
                    data=raw_str,
                )
                yield _make_done_event()
                return

            yield _make_sse_event(
                event="progress",
                seq=seq,
                data=raw_str,
            )

    finally:
        # Released here, not in the route handler: the slot is held for the
        # LIFE of the stream, and a client disconnect arrives as CancelledError
        # thrown into this generator. Releasing in the handler would free the
        # slot the instant the response was constructed, making the cap a no-op.
        if sse_slot_semaphore is not None:
            sse_slot_semaphore.release()
        # always release the Redis subscription; errors here
        # must not mask the primary exception.
        with contextlib.suppress(Exception):
            await pubsub.unsubscribe(channel)
        # Why bounded: keeps "every TaskQ-initiated close is bounded" true ,
        # the helper never raises, so the redundant suppress is dropped and a
        # hung broker cannot wedge the stream finalizer. Module-global read
        # at call time: tests monkeypatch CLOSE_TIMEOUT_SECS to shrink it.
        await close_redis_bounded(pubsub, "web-progress", CLOSE_TIMEOUT_SECS)


# ------------------------------------------------------------------
# Public factory
# ------------------------------------------------------------------


def create_router(
    pg_pool: asyncpg.Pool,
    redis_client: Any,  # redis.asyncio.Redis | None, typed Any at erasure boundary; redis is an optional dep
    *,
    schema: str = "taskq",
    auth_dependency: Callable[..., Any] | None = None,
    sse_heartbeat_interval: timedelta = timedelta(seconds=15),
    max_sse_connections: int | None = None,
    resolve_pg_pool: Callable[[Request], asyncpg.Pool] | None = None,
    resolve_redis_client: Callable[[Request], Any] | None = None,
    session_verifier: Callable[[Request], Awaitable[bool]] | None = None,
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
        ``taskq.web.admin.create_router``). Outside a dev environment
        (``TASKQ_ENVIRONMENT`` not ``dev``/``development``) the factory
        raises ``RuntimeError`` when it is omitted, unless
        ``TASKQ_PROGRESS_REQUIRE_AUTH=false`` suppresses the check; serving
        without auth always logs a warning.
    sse_heartbeat_interval:
        Cadence for ``': keepalive'`` SSE comments (default 15 s).
    max_sse_connections:
        Maximum concurrent progress streams this process will serve; further
        connections get HTTP 429. Defaults to
        ``TASKQ_PROGRESS_MAX_SSE_CONNECTIONS`` (50). Each stream holds a Redis
        pubsub subscription and an asyncio task for as long as the client stays
        connected, so an uncapped endpoint lets any principal who can reach the
        route exhaust Redis connections, event-loop tasks and file descriptors
        on the app hosting the pipeline. The admin ``/sse/{topic}`` endpoint has
        had such a cap; this one did not.
    resolve_pg_pool / resolve_redis_client:
        Per-request resolvers for the pool and the Redis client, used
        instead of *pg_pool* / *redis_client* when given. A host whose pool
        is replaced while it runs - ``taskq ui serve`` rebuilds it on a
        credential rotation - resolves the live one from its own state on
        every request rather than serving from the pool this router was
        constructed with, which that rotation has closed. Each is a
        FastAPI dependency: it may declare ``request: Request``.
    session_verifier:
        Optional async re-check for long-lived SSE streams
        (#316): ``Callable[[Request], Awaitable[bool]]`` returning whether the
        session that opened the stream is still valid. Live streams re-invoke
        it before every yielded event and at every keepalive tick and end on
        failure, so a session revoked mid-stream (secret rotation, expiry, an
        allowlist change) stops receiving frames within one keepalive
        interval, capped at 60 s. Each invocation is bounded by
        ``SESSION_RECHECK_TIMEOUT_SECS`` (5 s): a verifier that hangs or
        outlives the bound is treated as failure -- fail closed -- so a wedged
        host verifier cannot freeze the generator inside its keepalive path.
        When omitted, the router derives it from the ``session_verifier``
        attribute the taskq auth dependencies (``create_auth_dependency``,
        ``token_auth``) attach to the callable they return. A host supplying
        its own ``auth_dependency`` MUST pass ``session_verifier`` explicitly
        to keep the re-check: without the attribute the router logs a
        one-per-router ``progress-stream-no-session-verifier`` warning and
        the streams authenticate once, exactly as the router-level ``Depends``
        did before #316.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")

    settings = TaskQSettings.load()

    if auth_dependency is None:
        if not settings.is_dev_environment and settings.progress_require_auth:
            raise RuntimeError(
                "progress router requires auth_dependency in non-dev environments "
                "(set TASKQ_PROGRESS_REQUIRE_AUTH=false to disable)"
            )
        # Why this warning sits outside the environment test that governs the
        # RuntimeError above: a dev-labeled process is the only configuration
        # that actually serves an unauthenticated progress router, so it is
        # the one that most needs a log line. Keeping the warning inside the
        # non-dev branch meant the silent case was the dangerous one.
        suppressed_by = (
            "TASKQ_ENVIRONMENT is a dev environment, so the fail-closed startup check did not run"
            if settings.is_dev_environment
            else "TASKQ_PROGRESS_REQUIRE_AUTH is false, so the fail-closed "
            "startup check was suppressed"
        )
        logger.warning(
            "progress-router-no-auth",
            environment=settings.environment,
            detail=(
                "the progress router is being served with no authentication: "
                f"{suppressed_by}. The SSE stream and per-job state endpoints "
                "are reachable by anyone who can reach this port, and each "
                "stream holds a Redis pubsub subscription and an asyncio task "
                "for as long as the client stays connected. Pass "
                "auth_dependency to create_router, or set TASKQ_ENVIRONMENT "
                "to the real environment so startup fails closed."
            ),
        )

    router_kwargs: dict[str, Any] = {"tags": ["progress"]}
    if auth_dependency is not None:
        router_kwargs["dependencies"] = [Depends(auth_dependency)]

    router = APIRouter(**router_kwargs)

    # Session re-check derivation (#316): prefer the explicit parameter, fall
    # back to the attribute the taskq auth dependencies attach to the callable
    # they return. An auth dependency without a re-check cannot be safely
    # re-invoked by us (it may need FastAPI dependency injection), so the
    # honest fallback is a loud warning, not a silent best-effort call.
    if session_verifier is None and auth_dependency is not None:
        derived: Any = getattr(auth_dependency, "session_verifier", None)
        session_verifier = cast("Callable[[Request], Awaitable[bool]] | None", derived)
    if auth_dependency is not None and session_verifier is None:
        logger.warning(
            "progress-stream-no-session-verifier",
            detail=(
                "auth_dependency exposes no session_verifier re-check, so SSE "
                "streams authenticate once at subscribe and will keep "
                "delivering frames after a session is revoked. Pass a "
                "session_verifier to create_router, or build the dependency "
                "with taskq's create_auth_dependency/token_auth, which attach "
                "one."
            ),
        )

    _schema = schema
    _heartbeat_secs = sse_heartbeat_interval.total_seconds()
    # Why the cap: the keepalive tick is the revocation bound (#316) -- the
    # generator re-checks the session once per tick -- so a host-configured
    # interval of hours would silently move that bound to hours. The loop and
    # the fallback ping below are both capped at _SSE_HEARTBEAT_CAP_SECS.
    _effective_heartbeat_secs = min(_heartbeat_secs, _SSE_HEARTBEAT_CAP_SECS)
    if _heartbeat_secs > _SSE_HEARTBEAT_CAP_SECS:
        # The clamp itself must not be silent: a host that asked for an hour
        # between keepalives otherwise discovers the 60 s floor only by
        # reading the source. Once per router (this runs in create_router).
        logger.warning(
            "sse-heartbeat-interval-clamped",
            configured_seconds=_heartbeat_secs,
            effective_seconds=_effective_heartbeat_secs,
            detail=(
                "sse_heartbeat_interval exceeds the "
                f"{_SSE_HEARTBEAT_CAP_SECS:g} s cap and is clamped: the "
                "keepalive tick is also the session re-check cadence "
                "(#316), so a longer interval would silently loosen the "
                "bound on how long a revoked session keeps receiving "
                "frames."
            ),
        )
    _acquire_timeout = settings.admin_acquire_timeout

    # The degraded-mode log fires once per router, not per request: the
    # first stream a misconfigured portal refuses names the condition,
    # every later 503 is the same fact (a per-request warning is a log
    # flood that gets filtered out, which is the same as being silent).
    _degraded_mode_logged = False

    def _constructed_pool() -> asyncpg.Pool:
        return pg_pool

    def _constructed_redis() -> Any:
        return redis_client

    _resolve_pool: Callable[..., Any] = (
        resolve_pg_pool if resolve_pg_pool is not None else _constructed_pool
    )

    def _get_pool(pool: asyncpg.Pool = Depends(_resolve_pool)) -> BoundedPool:
        # Every checkout below is bounded (TASKQ_ADMIN_ACQUIRE_TIMEOUT): a
        # pool with nothing to give answers 503 instead of hanging the
        # request and every request behind it.
        return BoundedPool(pool, acquire_timeout=_acquire_timeout, role="progress")

    _get_redis: Callable[..., Any] = (
        resolve_redis_client if resolve_redis_client is not None else _constructed_redis
    )
    if max_sse_connections is None:
        max_sse_connections = settings.progress_max_sse_connections
    _max_sse = max_sse_connections
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
        pg_pool: BoundedPool = Depends(_get_pool),
        redis_client: Any = Depends(_get_redis),
    ) -> Response:
        """Stream progress events for a job via SSE.

        On initial connection (no ``last_event_id`` / ``Last-Event-ID``
        header): emits the current PG snapshot, then streams Redis events.

        On reconnect (``last_event_id`` present): subscribes Redis FIRST,
        emits one catch-up event from PG if ``progress_seq > last_event_id``,
        then resumes streaming.

        HTTP 404, job not found.
        HTTP 503, Redis not configured or unavailable.
        """
        nonlocal _degraded_mode_logged
        if redis_client is None:
            # One-time degraded-mode log (the operator-side companion of
            # the factory's ``admin-ui-no-redis-client`` startup warning):
            # this 503 is what puts the browser into polling mode, so the
            # log names it. Once per router; see the flag above.
            if not _degraded_mode_logged:
                _degraded_mode_logged = True
                logger.warning(
                    "progress-stream-polling-degraded",
                    detail=(
                        "the admin portal answered a progress stream with "
                        "no Redis client (503 redis_not_configured): the "
                        "dashboard falls back to polling the per-job state "
                        "endpoint. Set TASKQ_REDIS_URL and pass the client "
                        "to create_router to restore live SSE progress"
                    ),
                )
            return _OrjsonJSONResponse(  # pyright: ignore[reportReturnType]  # Why: FastAPI accepts any Response subclass here; the JSON response is returned for the 503 before SSE upgrade.
                status_code=503,
                content=_REDIS_503_BODY,
                headers={"Retry-After": "2"},
            )

        # Why here: after the cheap 503 guard (so an unconfigured Redis does
        # not consume a slot) and before any pubsub subscription is created,
        # so a rejected connection allocates nothing.
        sse_slot_semaphore = await acquire_sse_slot("progress-stream", _max_sse)
        # The slot is held for the LIFE of the stream, so ownership transfers
        # to the generator on the success path only. Every early exit below
        # (503 subscribe failure, PG error, 404 job not found) has to give it
        # back -- otherwise a run of requests for missing jobs would exhaust
        # the cap without a single stream ever opening. Idempotent so the
        # generator's own release can never double-release.
        _slot_released = False

        def _release_slot() -> None:
            nonlocal _slot_released
            if not _slot_released:
                _slot_released = True
                sse_slot_semaphore.release()

        try:
            return await _serve_progress_stream(
                job_id=job_id,
                request=request,
                last_event_id=last_event_id,
                sse_slot_semaphore=sse_slot_semaphore,
                release_slot=_release_slot,
                pg_pool=pg_pool,
                redis_client=redis_client,
                session_verifier=session_verifier,
            )
        except BaseException:
            _release_slot()
            raise

    async def _serve_progress_stream(  # pyright: ignore[reportUnusedFunction]  # Why: called by progress_stream above; not a route.
        *,
        job_id: UUID,
        request: Request,
        last_event_id: int | None,
        sse_slot_semaphore: asyncio.Semaphore,
        release_slot: Callable[[], None],
        pg_pool: BoundedPool,
        redis_client: Any,
        session_verifier: Callable[[Request], Awaitable[bool]] | None,
    ) -> Response:
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

        pubsub = redis_client.pubsub()
        try:
            await pubsub.subscribe(channel)
        except Exception as exc:
            logger.warning(
                "sse-redis-subscribe-failed",
                job_id=str(job_id),
                channel=channel,
                error=str(exc),
            )
            # Why bounded: same close contract as the generator finally ,
            # helper never raises (suppress dropped), hung broker cannot
            # wedge the 503 path.
            await close_redis_bounded(pubsub, "web-progress", CLOSE_TIMEOUT_SECS)
            release_slot()
            return _OrjsonJSONResponse(
                status_code=503,
                content=_REDIS_503_BODY,
                headers={"Retry-After": "2"},
            )

        # short-lived PG connection, released before any SSE
        # byte is written.
        try:
            async with pg_pool.acquire() as conn:
                row = await conn.fetchrow(_progress_sql, job_id)
        except Exception:
            # Cleanup must not mask the original exception from the PG query.
            with contextlib.suppress(Exception):
                await pubsub.unsubscribe(channel)
            # Why bounded: helper never raises (suppress dropped), so the
            # original PG error always propagates even with a dead broker.
            await close_redis_bounded(pubsub, "web-progress", CLOSE_TIMEOUT_SECS)
            raise

        if row is None:
            with contextlib.suppress(Exception):
                await pubsub.unsubscribe(channel)
            # Why bounded: helper never raises (suppress dropped), so the 404
            # is raised even with a dead broker.
            await close_redis_bounded(pubsub, "web-progress", CLOSE_TIMEOUT_SECS)
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

        # Bind the request into the re-check once: the verifier reads the
        # same cookie (or bearer token) the request arrived with, and a
        # session revoked mid-stream -- secret rotation, expiry, an allowlist
        # change -- fails the re-check even though those bytes are unchanged.
        _session_verifier: Callable[[], Awaitable[bool]] | None = (
            (lambda: session_verifier(request)) if session_verifier is not None else None
        )

        return EventSourceResponse(
            content=_event_generator(
                pubsub=pubsub,
                channel=channel,
                job_id=job_id,
                is_terminal=is_terminal,
                progress_seq=progress_seq,
                progress_data=progress_data,
                resolved_last_event_id=resolved_last_event_id,
                sse_slot_semaphore=sse_slot_semaphore,
                heartbeat_secs=_effective_heartbeat_secs,
                session_verifier=_session_verifier,
            ),
            headers=_SSE_HEADERS,
            # We emit our own keepalive comments via the get_message timeout
            # loop; sse-starlette's ping is only a fallback. Bounded at twice
            # the effective (capped) heartbeat: the old 24-hour value was a
            # no-lifetime-cap stream -- exactly the property the #316 fix
            # removes -- while ping=0 causes a tight loop (anyio.sleep(0)
            # returns immediately).
            ping=_effective_heartbeat_secs * 2,
            sep=_SSE_SEPARATOR,
        )

    # ----------------------------------------------------------------
    # Poll-state endpoint
    # ----------------------------------------------------------------

    @router.get("/api/job/{job_id}/state")
    async def job_state(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator.
        job_id: UUID,
        request: Request,
        pg_pool: BoundedPool = Depends(_get_pool),
    ) -> Response:
        """Return the current progress state for a job (polling fallback).

        Polling is a first-class mode of the admin UI, so the poll is
        conditional: a client that already rendered the current state
        sends ``If-None-Match`` with the progress sequence it has (the
        seq doubles as the ETag), and an unchanged tick is answered
        ``304`` with no body at all - the poll cadence never re-downloads
        data the client already has.

        Response body::

            {"status": <str>, "progress_state": <dict | null>, "progress_seq": <int>}

        HTTP 304, ``If-None-Match`` matches the current progress_seq: the
        client's rendered state is current, nothing to download.
        HTTP 404, job not found.
        """
        async with pg_pool.acquire() as conn:
            row = await conn.fetchrow(_progress_sql, job_id)

        if row is None:
            raise HTTPException(status_code=404, detail="job not found")

        # The progress sequence is a monotonically increasing write cursor
        # on the progress bytes this endpoint returns, so it IS the ETag
        # for the progress state: a seq the client already rendered means
        # every progress field (including the fingerprint the client
        # derives from it) is unchanged.
        #
        # The terminal rows are the exception: the terminal writes' seq
        # expression is GREATEST(progress_seq, $buffer_seq) and the
        # consumer passes the seq its progress buffer just flushed,
        # exactly the row's current cursor, so the status flips terminal
        # and the cursor stands still. A 304 there would hide the
        # terminal transition behind an unchanged cursor, and the poll
        # client (realtime.js stops only on a DOWNLOADED terminal status)
        # would render running forever. A terminal row therefore always
        # answers 200: the terminal body is the LAST body a poll client
        # needs, the one download that ends the poll loop, so the cost is
        # one bounded download per job and the seq-matching 304 keeps
        # serving the steady non-terminal state.
        etag = f'"{row["progress_seq"]}"'
        is_terminal = row["status"] in TERMINAL_STATUSES
        if not is_terminal and request.headers.get("if-none-match") == etag:
            return Response(status_code=304, headers={"ETag": etag})

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

        return _OrjsonJSONResponse(
            content={
                "status": row["status"],
                "progress_state": progress_state,
                "progress_seq": row["progress_seq"],
            },
            headers={"ETag": etag},
        )

    return router
