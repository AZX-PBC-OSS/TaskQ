"""Unit tests for the Redis pool DI provider (,).

register_redis_pool is idempotent.
get_redis_pool raises RuntimeError when redis_url is None.
"""

import contextlib

import pytest
import redis.asyncio as redis_async

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq.ratelimit._provider import get_redis_pool, register_redis_pool
from taskq.settings import WorkerSettings

pytestmark = pytest.mark.redis

# ── register_redis_pool is idempotent ──────────────────────────


def test_register_redis_pool_registers_once() -> None:
    registry = ProviderRegistry()
    register_redis_pool(registry)

    assert registry.has_provider(redis_async.Redis)
    entry = registry.get(redis_async.Redis)
    assert entry.scope == Scope.LOOP


def test_register_redis_pool_idempotent_noop() -> None:
    registry = ProviderRegistry()
    register_redis_pool(registry)
    register_redis_pool(registry)

    entry = registry.get(redis_async.Redis)
    assert entry.scope == Scope.LOOP


def test_register_redis_pool_dep_edges_include_worker_settings() -> None:
    registry = ProviderRegistry()
    register_redis_pool(registry)

    dep_types = {edge[1] for edge in registry._dep_edges}  # pyright: ignore[reportPrivateUsage] # Why: dep_edges is the only way to verify the factory's dependency graph; no public accessor exists
    assert WorkerSettings in dep_types


def test_register_redis_pool_validate_succeeds() -> None:
    registry = ProviderRegistry()
    registry.register_value(
        WorkerSettings,
        Scope.PROCESS,
        WorkerSettings.load_from_dict({"pg_dsn": "postgresql://u:p@h/d"}),
    )
    register_redis_pool(registry)
    registry.validate()


# ── get_redis_pool raises RuntimeError when redis_url is None ──


async def test_get_redis_pool_raises_when_redis_url_none() -> None:
    settings = WorkerSettings.load_from_dict({"pg_dsn": "postgresql://u:p@h/d"})
    assert settings.redis_url is None

    with pytest.raises(RuntimeError, match="Redis not configured"):
        gen = get_redis_pool(settings)
        try:
            await gen.__anext__()
        except RuntimeError:
            raise
        finally:
            with contextlib.suppress(RuntimeError):
                await gen.aclose()  # pyright: ignore[reportAttributeAccessIssue] # Why: AsyncIterator[Redis] from an async generator has aclose() at runtime; pyright cannot model async-generator cleanup protocol


async def test_get_redis_pool_error_message_mentions_both_primitives() -> None:
    settings = WorkerSettings.load_from_dict({"pg_dsn": "postgresql://u:p@h/d"})
    assert settings.redis_url is None

    with pytest.raises(RuntimeError) as exc_info:
        gen = get_redis_pool(settings)
        try:
            await gen.__anext__()
        except RuntimeError:
            raise
        finally:
            with contextlib.suppress(RuntimeError):
                await gen.aclose()  # pyright: ignore[reportAttributeAccessIssue] # Why: AsyncIterator[Redis] from an async generator has aclose() at runtime; pyright cannot model async-generator cleanup protocol

    msg = exc_info.value.args[0]
    assert "TokenBucket" in msg
    assert "SlidingWindow" in msg
