"""Redis implementations for sliding-window rate limiter.

All Redis-path methods (acquire, peek, reset, refund for both log and
GCRA styles, plus Lua-script caching helpers) live here as module-level
functions taking ``self: SlidingWindow`` as the first parameter.
"""

from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

from taskq.ratelimit._decision_log import log_decision
from taskq.ratelimit._redis_utils import ensure_redis_script, with_pg_fallback
from taskq.ratelimit._scripts import (
    GCRA_REFUND_SCRIPT,
    SLIDING_WINDOW_GCRA_SCRIPT,
    SLIDING_WINDOW_LOG_SCRIPT,
)
from taskq.ratelimit.decision import RateLimitDecision, RateLimitState

if TYPE_CHECKING:
    import asyncpg
    import redis.asyncio as redis_async
    from redis.commands.core import AsyncScript

    from taskq.backend.clock import Clock
    from taskq.ratelimit.sliding_window import SlidingWindow
    from taskq.settings import WorkerSettings

__all__ = [
    "_acquire_redis_gcra",
    "_acquire_redis_gcra_wrapped",
    "_acquire_redis_log",
    "_acquire_redis_log_wrapped",
    "_peek_redis_gcra",
    "_peek_redis_log",
    "_refund_redis_gcra",
    "_refund_redis_log",
    "_reset_redis_gcra",
    "_reset_redis_log",
]


async def _ensure_log_script(
    self: "SlidingWindow", redis_client: "redis_async.Redis"
) -> "AsyncScript":
    return await ensure_redis_script(
        lambda: self._redis_log_script,
        lambda s: setattr(self, "_redis_log_script", s),
        lambda: redis_client.register_script(SLIDING_WINDOW_LOG_SCRIPT),
        self._script_lock,
    )


async def _ensure_gcra_script(
    self: "SlidingWindow", redis_client: "redis_async.Redis"
) -> "AsyncScript":
    return await ensure_redis_script(
        lambda: self._redis_gcra_script,
        lambda s: setattr(self, "_redis_gcra_script", s),
        lambda: redis_client.register_script(SLIDING_WINDOW_GCRA_SCRIPT),
        self._script_lock,
    )


async def _ensure_gcra_refund_script(
    self: "SlidingWindow", redis_client: "redis_async.Redis"
) -> "AsyncScript":
    return await ensure_redis_script(
        lambda: self._redis_gcra_refund_script,
        lambda s: setattr(self, "_redis_gcra_refund_script", s),
        lambda: redis_client.register_script(GCRA_REFUND_SCRIPT),
        self._script_lock,
    )


async def _acquire_redis_log(
    self: "SlidingWindow",
    now_ms: int,
    request_id: UUID | None,
    redis_client: "redis_async.Redis | None",
    clock: "Clock",
    settings: "WorkerSettings | None",
) -> RateLimitDecision:
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    if request_id is None:
        raise RuntimeError("request_id required for log-style acquire")

    script = await _ensure_log_script(self, redis_client)

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw:{{{self._name}}}"

    window_ms = int(self._window.total_seconds() * 1000)
    ttl_ms = int(self._ttl.total_seconds() * 1000)

    argv: list[int | str] = [
        now_ms,
        window_ms,
        self._limit,
        str(request_id),
        ttl_ms,
    ]

    raw: list[object] = await script(keys=[key], args=argv)  # pyright: ignore[reportAssignmentType, reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py AsyncScript.__call__ has no return-type annotation — pyright cannot model the return shape; the three-element list structure is guaranteed by the Lua script contract

    allowed = int(raw[0]) == 1  # pyright: ignore[reportArgumentType]  # Why: raw[0] is int | bytes from Redis; int() accepts both at runtime
    count = int(raw[1])  # pyright: ignore[reportArgumentType]  # Why: raw[1] is int | bytes from Redis; int() accepts both
    retry_after_ms = int(raw[2])  # pyright: ignore[reportArgumentType]  # Why: raw[2] is int | bytes from Redis; int() accepts both

    result = RateLimitDecision(
        allowed=allowed,
        remaining=float(self._limit - count) if allowed else 0.0,
        retry_after=timedelta(0) if allowed else timedelta(milliseconds=retry_after_ms),
        bucket_name=self._name,
        backend="redis",
        request_id=str(request_id),
    )

    log_decision(result, style=self._style)
    return result


async def _acquire_redis_log_wrapped(
    self: "SlidingWindow",
    now_ms: int,
    request_id: UUID | None,
    redis_client: "redis_async.Redis | None",
    pg_pool: "asyncpg.Pool | None",
    clock: "Clock",
    settings: "WorkerSettings | None",
) -> RateLimitDecision:
    """Redis log-style path with optional PG fallback on ConnectionError/TimeoutError."""
    from taskq.ratelimit._sliding_window_pg import _acquire_pg_log

    return await with_pg_fallback(
        _acquire_redis_log(self, now_ms, request_id, redis_client, clock, settings),
        lambda: _acquire_pg_log(self, pg_pool, clock, settings, request_id),
        bucket_name=self._name,
        settings=settings,
        style="log",
    )


async def _acquire_redis_gcra(
    self: "SlidingWindow",
    now_ms: int,
    redis_client: "redis_async.Redis | None",
    clock: "Clock",
    settings: "WorkerSettings | None",
) -> RateLimitDecision:
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    script = await _ensure_gcra_script(self, redis_client)

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw_gcra:{{{self._name}}}"

    window_ms = int(self._window.total_seconds() * 1000)
    emission_interval_ms = window_ms / self._limit
    delay_tolerance_ms = window_ms
    quantity_ms = emission_interval_ms
    ttl_ms = int(self._ttl.total_seconds() * 1000)

    argv: list[float | int] = [
        emission_interval_ms,
        delay_tolerance_ms,
        quantity_ms,
        ttl_ms,
        now_ms,
    ]

    raw: list[object] = await script(keys=[key], args=argv)  # pyright: ignore[reportAssignmentType, reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py AsyncScript.__call__ has no return-type annotation — pyright cannot model the return shape; the three-element list structure is guaranteed by the Lua script contract

    allowed = int(raw[0]) == 1  # pyright: ignore[reportArgumentType]  # Why: raw[0] is int | bytes from Redis; int() accepts both at runtime
    retry_after_ms = int(raw[1])  # pyright: ignore[reportArgumentType]  # Why: raw[1] is int | bytes from Redis; int() accepts both
    remaining_estimate = int(raw[2])  # pyright: ignore[reportArgumentType]  # Why: raw[2] is int | bytes from Redis; int() accepts both

    previous_state: dict[str, object] | None = None
    if allowed and len(raw) >= 5:
        pre_acquire_tat_str = raw[3]  # pyright: ignore[reportArgumentType]  # Why: raw[3] is bytes | str from Redis (Lua tostring); runtime type is correct
        post_acquire_tat_str = raw[4]  # pyright: ignore[reportArgumentType]  # Why: raw[4] is bytes | str from Redis (Lua tostring); runtime type is correct
        pre_str = (
            pre_acquire_tat_str.decode()
            if isinstance(pre_acquire_tat_str, bytes)
            else str(pre_acquire_tat_str)
        )
        post_str = (
            post_acquire_tat_str.decode()
            if isinstance(post_acquire_tat_str, bytes)
            else str(post_acquire_tat_str)
        )
        previous_state = {
            "pre_acquire_tat_str": pre_str,
            "post_acquire_tat_str": post_str,
            "ttl_ms": ttl_ms,
        }

    result = RateLimitDecision(
        allowed=allowed,
        remaining=float(remaining_estimate) if allowed else 0.0,
        retry_after=timedelta(0) if allowed else timedelta(milliseconds=retry_after_ms),
        bucket_name=self._name,
        backend="redis",
        previous_state=previous_state,
    )

    log_decision(result, style=self._style)
    return result


async def _acquire_redis_gcra_wrapped(
    self: "SlidingWindow",
    now_ms: int,
    redis_client: "redis_async.Redis | None",
    pg_pool: "asyncpg.Pool | None",
    clock: "Clock",
    settings: "WorkerSettings | None",
) -> RateLimitDecision:
    """Redis GCRA path with optional PG fallback on ConnectionError/TimeoutError."""
    from taskq.ratelimit._sliding_window_pg import _acquire_pg_gcra

    return await with_pg_fallback(
        _acquire_redis_gcra(self, now_ms, redis_client, clock, settings),
        lambda: _acquire_pg_gcra(self, pg_pool, clock, settings),
        bucket_name=self._name,
        settings=settings,
        style="gcra",
    )


async def _peek_redis_log(
    self: "SlidingWindow",
    now_ms: int,
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> RateLimitState:
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw:{{{self._name}}}"
    window_ms = int(self._window.total_seconds() * 1000)

    count = int(await redis_client.zcard(key))  # pyright: ignore[reportUnknownMemberType, reportUnknownArgumentType]  # Why: redis-py zcard return type is untyped in the stub
    is_exhausted = count >= self._limit
    retry_after: timedelta | None = None

    if is_exhausted and count > 0:
        oldest = await redis_client.zrange(key, 0, 0, withscores=True)  # pyright: ignore[reportUnknownMemberType]  # Why: redis-py zrange return type is untyped in the stub
        if oldest:
            oldest_entry = oldest[0]
            oldest_score = (
                float(oldest_entry[1])
                if isinstance(oldest_entry, (list, tuple))
                else float(oldest_entry)
            )  # pyright: ignore[reportUnknownArgumentType]  # Why: redis-py zrange return type is untyped in the stub; isinstance narrowing is sufficient at runtime.
            retry_ms = int(oldest_score) + window_ms - now_ms
            retry_after = timedelta(milliseconds=max(1, retry_ms))

    return RateLimitState(
        bucket_name=self._name,
        backend="redis",
        is_exhausted=is_exhausted,
        remaining=float(max(0, self._limit - count)),
        retry_after=retry_after,
        limit=self._limit,
        window=self._window,
        style="log",
    )


async def _peek_redis_gcra(
    self: "SlidingWindow",
    now_ms: int,
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> RateLimitState:
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw_gcra:{{{self._name}}}"
    window_ms = int(self._window.total_seconds() * 1000)
    emission_interval_ms = window_ms / self._limit
    delay_tolerance_ms = window_ms

    tat_raw = await redis_client.get(key)  # pyright: ignore[reportUnknownMemberType]  # Why: redis-py get return type is untyped in the stub
    tat = float(tat_raw) if tat_raw else float(now_ms)
    tat = max(tat, float(now_ms))

    remaining = float(max(0, int((delay_tolerance_ms - (tat - now_ms)) / emission_interval_ms)))
    is_exhausted = remaining <= 0
    retry_after: timedelta | None = None
    if is_exhausted:
        new_tat = tat + emission_interval_ms
        allow_at = new_tat - delay_tolerance_ms
        retry_after = timedelta(milliseconds=max(1, round(allow_at - now_ms)))

    return RateLimitState(
        bucket_name=self._name,
        backend="redis",
        is_exhausted=is_exhausted,
        remaining=remaining,
        retry_after=retry_after,
        limit=self._limit,
        window=self._window,
        style="gcra",
    )


async def _reset_redis_log(
    self: "SlidingWindow",
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> None:
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw:{{{self._name}}}"
    await redis_client.delete(key)  # pyright: ignore[reportUnknownMemberType]  # Why: redis-py delete return type is untyped in the stub


async def _reset_redis_gcra(
    self: "SlidingWindow",
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> None:
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend")

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw_gcra:{{{self._name}}}"
    await redis_client.delete(key)  # pyright: ignore[reportUnknownMemberType]  # Why: redis-py delete return type is untyped in the stub


async def _refund_redis_log(
    self: "SlidingWindow",
    decision: RateLimitDecision,
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> None:
    if decision.request_id is None:
        raise ValueError(
            "log-style refund requires decision.request_id for ZREM; "
            "got None — was this decision from a non-log acquire path?"
        )
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis backend refund")
    if settings is None:
        raise RuntimeError("settings not injected for redis backend refund")

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw:{{{self._name}}}"

    await redis_client.zrem(key, decision.request_id)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py zrem() return type is untyped in the stub


async def _refund_redis_gcra(
    self: "SlidingWindow",
    decision: RateLimitDecision,
    redis_client: "redis_async.Redis | None",
    settings: "WorkerSettings | None",
) -> None:
    if decision.previous_state is None:
        return
    if redis_client is None:
        raise RuntimeError("redis_client not injected for redis gcra refund")
    if settings is None:
        raise RuntimeError("settings not injected for redis gcra refund")

    script = await _ensure_gcra_refund_script(self, redis_client)

    schema_name = settings.schema_name
    key = f"taskq:{schema_name}:sw_gcra:{{{self._name}}}"

    pre_acquire_tat_str = str(decision.previous_state["pre_acquire_tat_str"])  # type: ignore[arg-type]  # Why: dict[str, object] value is str at runtime; type narrowing not possible from generic dict
    post_acquire_tat_str = str(decision.previous_state["post_acquire_tat_str"])  # type: ignore[arg-type]  # Why: dict[str, object] value is str at runtime; type narrowing not possible from generic dict
    ttl_ms = int(decision.previous_state["ttl_ms"])  # type: ignore[arg-type]  # Why: dict[str, object] value is int at runtime; type narrowing not possible from generic dict

    argv: list[int | str] = [pre_acquire_tat_str, post_acquire_tat_str, ttl_ms]
    await script(keys=[key], args=argv)  # pyright: ignore[reportUnknownMemberType, reportUnknownVariableType]  # Why: redis-py AsyncScript.__call__ has no return-type annotation; refund return value is not consumed
