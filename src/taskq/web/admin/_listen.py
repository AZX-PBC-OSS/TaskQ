"""Shared async generator for PG LISTEN/NOTIFY with automatic reconnection.

The body is the shared LISTEN pump (:mod:`taskq.workflows.api._listen_pump`)
— the same LISTEN → BACKFILL → TAIL reconnect skeleton, the bounded raw
queue, and the SHIELDED teardown the two workflow listeners ride. This
module is the admin feed's thin adapter over it: one channel, payload
strings (no channel carried — the feed listens one channel and the
frames' consumers know which), and the pool resolved through the
caller's callable.
"""

from collections.abc import AsyncGenerator, Callable

import asyncpg
import structlog

_log = structlog.get_logger("taskq.web.admin.listen")


async def listen_with_reconnect(
    resolve_pool: Callable[[], asyncpg.Pool],
    channel: str,
    *,
    keepalive_interval: float = 30.0,
    backoff_initial: float = 1.0,
    backoff_max: float = 30.0,
    acquire_timeout: float = 5.0,
) -> AsyncGenerator[str | None, None]:
    """Yield NOTIFY payloads from *channel* with automatic reconnection.

    Yields each payload string as it arrives.  Yields ``None`` as a keepalive
    signal when no payload arrives within *keepalive_interval*.  The caller
    should break out of the loop (e.g. on client disconnect) to stop
    listening; the generator cleans up the connection in its ``finally``.

    *resolve_pool* is called on every (re)connect rather than a pool being
    captured once: the admin pool is replaced by a credential rotation
    (``taskq ui serve``), and a stream that kept reconnecting to the pool
    it started on would be reconnecting to a closed one for the rest of
    its life. The rotation terminates the old pool's connections, which
    is why a closed session is checked for on every keepalive - a
    terminated LISTEN connection raises nothing on its own; the queue
    just goes quiet.
    """
    # The import law (the admin's): taskq.workflows loads lazily, never
    # at module scope.
    from taskq.workflows.api._listen_pump import pump

    def _log_reconnect(exc: Exception, backoff: float) -> None:
        # Every failure here - a lost session, a pool that cannot
        # connect, a bouncer rejecting the session-scoped LISTEN - leaves
        # the feed on keepalives only until a reconnect succeeds. The
        # consumer sees a healthy-looking stream that never delivers, so
        # the reconnect is what carries the cause to the operator.
        _log.warning(
            "listen-reconnect",
            channel=channel,
            error_type=type(exc).__name__,
            error=str(exc),
            backoff=backoff,
        )

    inner = pump(
        resolve_pool,
        (channel,),
        None,
        keepalive=keepalive_interval,
        backoff_initial=backoff_initial,
        backoff_max=backoff_max,
        acquire_timeout=acquire_timeout,
        overflow_log=_log,
        closed_message="LISTEN connection was closed underneath the stream",
        on_reconnect=_log_reconnect,
    )
    # The adapter's own finally closes the inner pump: a bare ``async
    # for`` abandons the inner generator to the GC finalizer when this
    # generator is closed, and the dedicated LISTEN connection's release
    # would wait on garbage collection instead of the close (the
    # deterministic-release contract the SSE teardown pins hold).
    try:
        async for item in inner:
            if item is None:
                yield None
            else:
                _chan, payload = item
                yield payload
    finally:
        await inner.aclose()
