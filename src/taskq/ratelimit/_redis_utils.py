"""Shared helpers for Redis rate-limit primitives: PG fallback, script caching,
and the store-clock read."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import structlog

from taskq.ratelimit.decision import RateLimitDecision

if TYPE_CHECKING:
    import redis.asyncio as redis_async
    from redis.commands.core import AsyncScript

    from taskq.settings import WorkerSettings

logger = structlog.get_logger("taskq.ratelimit._redis_utils")

__all__ = [
    "ensure_redis_script",
    "redis_time_seconds",
    "with_pg_fallback",
]


async def redis_time_seconds(redis_client: "redis_async.Redis") -> float:
    """Read the store's clock via ``TIME``, the domain the Lua scripts stamp.

    Used by the non-script peek paths so their elapsed/refill estimates run
    in the same clock domain as the admission state they read.
    """
    t = await redis_client.time()  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py time() return type is untyped in the stub; returns (seconds, microseconds)
    return float(t[0]) + float(t[1]) / 1_000_000  # pyright: ignore[reportUnknownArgumentType, reportIndex]  # Why: t is an untyped sequence of two ints from the redis-py stub; validated at runtime


async def with_pg_fallback(
    redis_call: Awaitable[RateLimitDecision],
    pg_call: Callable[[], Awaitable[RateLimitDecision]],
    *,
    bucket_name: str,
    settings: "WorkerSettings | None",
    style: str | None = None,
) -> RateLimitDecision:
    """Try a Redis acquire; on ConnectionError/TimeoutError and the
    ResponseError siblings that mean "this server cannot serve right now",
    fall back to PG.

    The extra siblings are pinned exactly (checked against redis-py 8.1.0's
    exception hierarchy): :class:`redis.ReadOnlyError` (a replica promoted
    mid-flight answers writes with READONLY) and
    :class:`redis.OutOfMemoryError` (a maxmemory breach) are both direct
    ``ResponseError`` subclasses and both mean the store cannot serve - the
    same outage class as a connection failure. They must be named
    individually rather than catching the parent ``ResponseError``:
    :class:`redis.exceptions.NoScriptError` is also a ``ResponseError``
    sibling, and redis-py handles it client-side (``Script.__call__``
    re-EVALs after a fresh SCRIPT LOAD), so it never signals a store outage.

    The WARNING log is emitted **before** delegating to the PG path so that
    if the PG path also emits an INFO denial log, the WARNING precedes the
    INFO in the captured stream.

    Raises :class:`ImportError` if the ``[redis]`` extra is not installed
    and the caller somehow reaches this path (should not happen when
    ``register_redis_pool`` is used, which silently skips when redis is
    absent).
    """
    try:
        import redis as _redis_mod
        from redis.exceptions import OutOfMemoryError as _RedisOutOfMemoryError
        from redis.exceptions import ReadOnlyError as _RedisReadOnlyError
    except ImportError as exc:
        raise ImportError(
            "taskq[redis] is required to use a Redis-backed rate limiter. "
            "Install it with: pip install 'taskq[redis]'"
        ) from exc

    try:
        return await redis_call
    except (
        _redis_mod.ConnectionError,
        _redis_mod.TimeoutError,
        _RedisReadOnlyError,
        _RedisOutOfMemoryError,
    ) as exc:
        if settings is None or not settings.rate_limit_pg_fallback_enabled:
            raise
        log_kwargs: dict[str, object] = {
            "bucket_name": bucket_name,
            "backend": "redis",
            "fallback": "postgres",
            "error": str(exc),
        }
        if style is not None:
            log_kwargs["style"] = style
        logger.warning("rate-limit-redis-fallback", **log_kwargs)
        return await pg_call()


async def ensure_redis_script(
    get: Callable[[], "AsyncScript | None"],
    set: Callable[["AsyncScript"], None],
    register: Callable[[], "AsyncScript"],
    lock: asyncio.Lock,
) -> "AsyncScript":
    """Lazily register a Lua script exactly once per client, using
    double-checked locking.

    A registered ``AsyncScript`` is bound to the client instance it was
    registered on, and a client is replaceable (the worker reload path
    rebuilds ``deps.redis_client``; a loop restart re-resolves the DI
    value).  The cache the caller's *get*/*set* closures manage MUST
    therefore be keyed by client identity: *get* returns the cached
    script only when it was registered on the client in use now, and
    *set* records that binding.  A cache keyed on nothing reuses a dead
    client's script against a live client forever, so a store that
    healed through a client swap never heals.
    """
    existing = get()
    if existing is not None:
        return existing
    async with lock:
        existing = get()
        if existing is not None:
            return existing
        script = register()
        set(script)
        return script
