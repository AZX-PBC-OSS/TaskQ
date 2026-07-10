"""Redis pool and RateLimitRegistry DI providers for the rate-limit subsystem.

Exposes the LOOP-scoped async-generator factory and an idempotent bootstrap
helper that registers it with a :class:`ProviderRegistry`.  The factory
participates in the DI dep-edge graph automatically because its
``settings: WorkerSettings`` parameter is introspected by
``_collect_dep_edges``.

Also registers the module-level :data:`RateLimitRegistry` singleton as a
LOOP-scope value so the consumer can resolve it at dispatch time.  The
DI-registered instance and the module singleton are the same object —
callers that import the singleton directly see the same state as
DI-resolved consumers.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings


async def get_redis_pool(
    settings: WorkerSettings,
) -> AsyncIterator[Any]:
    """Yield a Redis client for the worker loop lifetime.

    The return type is ``AsyncIterator[Any]`` rather than
    ``AsyncIterator[redis.asyncio.Redis]`` so the DI system can introspect
    the annotation without requiring the ``[redis]`` extra at runtime.
    The actual yielded value is a ``redis.asyncio.Redis`` instance.

    Raises :class:`RuntimeError` when ``settings.redis_url`` is ``None``
    (Redis not configured but a Redis-backed rate limiter was registered).
    Raises :class:`ImportError` when the ``[redis]`` extra is not installed.
    """
    if settings.redis_url is None:
        raise RuntimeError(
            "Redis not configured but a Redis-backed rate limiter "
            "(TokenBucket/SlidingWindow) was registered"
        )
    import redis.asyncio as redis_async

    client = redis_async.from_url(
        str(settings.redis_url),
        decode_responses=False,  # Why: raw bytes are safer for binary payloads and cluster-safety across shards
    )
    try:
        yield client
    finally:
        await client.aclose()


def register_redis_pool(registry: ProviderRegistry) -> None:
    """Idempotent registration of the LOOP-scoped Redis pool factory.

    Calls ``registry.register_factory(redis.asyncio.Redis, Scope.LOOP,
    get_redis_pool)`` only when ``registry.has_provider(redis.asyncio.Redis)``
    is ``False``, so user-supplied registrations take precedence.

    Silently skips registration when the ``[redis]`` extra is not installed.
    """
    try:
        import redis.asyncio as redis_async
    except ImportError:
        return

    if registry.has_provider(redis_async.Redis):
        return
    registry.register_factory(redis_async.Redis, Scope.LOOP, get_redis_pool)


def register_rate_limit_registry(
    di_registry: ProviderRegistry,
    rl_registry: RateLimitRegistry,
) -> None:
    """Idempotent registration of the LOOP-scope RateLimitRegistry singleton.

    Registers the given :class:`RateLimitRegistry` as a ``Scope.LOOP`` value
    so it is available at dispatch time via DI resolution.  The same object
    is also importable as :data:`taskq.ratelimit.registry.registry` — both
    paths observe identical state.
    """
    if di_registry.has_provider(RateLimitRegistry):
        return
    di_registry.register_value(RateLimitRegistry, Scope.LOOP, rl_registry)
