"""Shared helpers for Redis rate-limit primitives: PG fallback and script caching."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import structlog

from taskq.ratelimit.decision import RateLimitDecision

if TYPE_CHECKING:
    from redis.commands.core import AsyncScript

    from taskq.settings import WorkerSettings

logger = structlog.get_logger("taskq.ratelimit._redis_utils")


async def with_pg_fallback(
    redis_call: Awaitable[RateLimitDecision],
    pg_call: Callable[[], Awaitable[RateLimitDecision]],
    *,
    bucket_name: str,
    settings: "WorkerSettings | None",
    style: str | None = None,
) -> RateLimitDecision:
    """Try a Redis acquire; on ConnectionError/TimeoutError, fall back to PG.

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
    except ImportError as exc:
        raise ImportError(
            "taskq[redis] is required to use a Redis-backed rate limiter. "
            "Install it with: pip install 'taskq[redis]'"
        ) from exc

    try:
        return await redis_call
    except (_redis_mod.ConnectionError, _redis_mod.TimeoutError) as exc:
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
    """Lazily register a Lua script exactly once using double-checked locking."""
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
