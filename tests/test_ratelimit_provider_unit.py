"""Unit tests for the Redis pool DI provider (,).

register_redis_pool is idempotent.
get_redis_pool raises RuntimeError when redis_url is None.
get_redis_pool bounds its teardown redis close (review N4).
"""

import asyncio
import contextlib

import pytest
import redis.asyncio as redis_async
import structlog.testing

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


# ── Bounded redis teardown close (review N4) ──────────────────


class _HungRedis:
    """Docker-free fake Redis whose aclose() hangs on a gate (dead broker)."""

    def __init__(self) -> None:
        self.aclose_calls = 0
        self.aclose_wait = asyncio.Event()  # never set — aclose() hangs forever

    async def aclose(self) -> None:
        self.aclose_calls += 1
        await self.aclose_wait.wait()


async def test_get_redis_pool_bounds_teardown_close(monkeypatch: pytest.MonkeyPatch) -> None:
    """A hung redis aclose() at provider teardown is bounded: the LOOP-scope
    shutdown logs ``redis-teardown-close-timeout`` (label=ratelimit) and
    completes instead of adding an unaccounted ~5s tail to worker teardown
    (review N4). Docker-free: ``redis_async.from_url`` is patched to a fake
    whose aclose() never returns."""
    import taskq.ratelimit._provider as provider_mod

    # Why raising=False: pre-fix the module has no CLOSE_TIMEOUT_SECS seam,
    # so the RED state must demonstrate the teardown wedge (outer timeout),
    # not an AttributeError from the shrink.
    monkeypatch.setattr(provider_mod, "CLOSE_TIMEOUT_SECS", 0.05, raising=False)
    fake = _HungRedis()
    monkeypatch.setattr(redis_async, "from_url", lambda *a, **kw: fake)

    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": "redis://localhost:6379/0"}
    )

    # Why the outer timeout: pre-fix the provider's finally awaited
    # client.aclose() unbounded, so the RED state wedges here instead of
    # failing fast.
    with structlog.testing.capture_logs() as captured:
        async with asyncio.timeout(5):
            async for _ in get_redis_pool(settings):
                pass

    assert fake.aclose_calls == 1
    timeout_events = [e for e in captured if e.get("event") == "redis-teardown-close-timeout"]
    assert len(timeout_events) == 1, f"expected 1 redis timeout event, got {captured!r}"
    assert timeout_events[0].get("label") == "ratelimit", (
        f"expected label=ratelimit on the timeout event, got {timeout_events[0]!r}"
    )
