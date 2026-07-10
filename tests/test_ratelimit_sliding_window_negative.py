"""Negative tests for SlidingWindow constructor validation (2/3).

These tests exercise the runtime ``ValueError`` guards that complement
pyright's static Literal narrowing. Parametrised over ``style`` where
applicable per the test plan.
"""

from datetime import timedelta
from typing import Any, cast

import pytest

from taskq.ratelimit import SlidingWindow

# ── limit=0 raises ValueError (both styles) ───────────────────


@pytest.mark.parametrize("style", ["log", "gcra"])
def test_limit_zero_raises(style: str) -> None:
    """limit=0 raises ValueError at construction."""
    with pytest.raises(ValueError, match="limit must be >= 1"):
        SlidingWindow(
            name="test",
            limit=0,
            window=timedelta(seconds=60),
            backend="memory",
            style=style,
        )


@pytest.mark.parametrize("style", ["log", "gcra"])
def test_limit_negative_raises(style: str) -> None:
    """limit < 0 raises ValueError at construction."""
    with pytest.raises(ValueError, match="limit must be >= 1"):
        SlidingWindow(
            name="test",
            limit=-1,
            window=timedelta(seconds=60),
            backend="memory",
            style=style,
        )


# ── window=timedelta(0) raises ValueError (both styles) ───────


@pytest.mark.parametrize("style", ["log", "gcra"])
def test_window_zero_raises(style: str) -> None:
    """window=timedelta(0) raises ValueError at construction."""
    with pytest.raises(ValueError, match="window must be > timedelta"):
        SlidingWindow(
            name="test",
            limit=10,
            window=timedelta(0),
            backend="memory",
            style=style,
        )


@pytest.mark.parametrize("style", ["log", "gcra"])
def test_window_negative_raises(style: str) -> None:
    """window < timedelta(0) raises ValueError at construction."""
    with pytest.raises(ValueError, match="window must be > timedelta"):
        SlidingWindow(
            name="test",
            limit=10,
            window=timedelta(seconds=-1),
            backend="memory",
            style=style,
        )


# ── style="invalid" raises ValueError ────────────────────────


def test_style_invalid_raises() -> None:
    """style='invalid' raises ValueError at construction."""
    with pytest.raises(ValueError, match="style must be 'log' or 'gcra'"):
        SlidingWindow(
            name="test",
            limit=10,
            window=timedelta(seconds=60),
            backend="memory",
            style=cast(Any, "invalid"),
        )


def test_style_numeric_raises() -> None:
    """style=42 raises ValueError at construction."""
    with pytest.raises(ValueError, match="style must be 'log' or 'gcra'"):
        SlidingWindow(
            name="test",
            limit=10,
            window=timedelta(seconds=60),
            backend="memory",
            style=cast(Any, 42),
        )


# ── Redis backend guard clauses: None redis_client / settings ───────────


def _make_settings() -> Any:
    from taskq.settings import WorkerSettings

    return WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "schema_name": "test_schema"},
    )


async def test_peek_redis_log_raises_without_redis_client() -> None:
    """_peek_redis_log raises RuntimeError when redis_client is None."""
    from taskq.ratelimit._sliding_window_redis import _peek_redis_log

    sw = SlidingWindow("test", limit=5, window=timedelta(seconds=10), backend="redis", style="log")
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await _peek_redis_log(sw, now_ms=0, redis_client=None, settings=None)


async def test_peek_redis_log_raises_without_settings() -> None:
    """_peek_redis_log raises RuntimeError when settings is None."""
    from taskq.ratelimit._sliding_window_redis import _peek_redis_log

    class _FakeRedis:
        async def zcard(self, key: object) -> int:
            return 0

    sw = SlidingWindow("test", limit=5, window=timedelta(seconds=10), backend="redis", style="log")
    with pytest.raises(RuntimeError, match="settings not injected"):
        await _peek_redis_log(sw, now_ms=0, redis_client=_FakeRedis(), settings=None)


async def test_peek_redis_gcra_raises_without_redis_client() -> None:
    """_peek_redis_gcra raises RuntimeError when redis_client is None."""
    from taskq.ratelimit._sliding_window_redis import _peek_redis_gcra

    sw = SlidingWindow("test", limit=5, window=timedelta(seconds=10), backend="redis", style="gcra")
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await _peek_redis_gcra(sw, now_ms=0, redis_client=None, settings=None)


async def test_reset_redis_log_raises_without_redis_client() -> None:
    """_reset_redis_log raises RuntimeError when redis_client is None."""
    from taskq.ratelimit._sliding_window_redis import _reset_redis_log

    sw = SlidingWindow("test", limit=5, window=timedelta(seconds=10), backend="redis", style="log")
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await _reset_redis_log(sw, redis_client=None, settings=None)


async def test_reset_redis_gcra_raises_without_redis_client() -> None:
    """_reset_redis_gcra raises RuntimeError when redis_client is None."""
    from taskq.ratelimit._sliding_window_redis import _reset_redis_gcra

    sw = SlidingWindow("test", limit=5, window=timedelta(seconds=10), backend="redis", style="gcra")
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await _reset_redis_gcra(sw, redis_client=None, settings=None)


async def test_refund_redis_log_raises_without_redis_client() -> None:
    """_refund_redis_log raises RuntimeError when redis_client is None."""
    from taskq.ratelimit._sliding_window_redis import _refund_redis_log
    from taskq.ratelimit.decision import RateLimitDecision

    sw = SlidingWindow("test", limit=5, window=timedelta(seconds=10), backend="redis", style="log")
    decision = RateLimitDecision(
        allowed=True,
        remaining=1.0,
        retry_after=timedelta(0),
        bucket_name="test",
        backend="redis",
        request_id="req1",
    )
    with pytest.raises(RuntimeError, match="redis_client not injected"):
        await _refund_redis_log(sw, decision, redis_client=None, settings=None)


async def test_refund_redis_gcra_returns_early_without_previous_state() -> None:
    """_refund_redis_gcra returns immediately when previous_state is None."""
    from taskq.ratelimit._sliding_window_redis import _refund_redis_gcra
    from taskq.ratelimit.decision import RateLimitDecision

    sw = SlidingWindow("test", limit=5, window=timedelta(seconds=10), backend="redis", style="gcra")
    decision = RateLimitDecision(
        allowed=True,
        remaining=1.0,
        retry_after=timedelta(0),
        bucket_name="test",
        backend="redis",
        previous_state=None,
    )
    # Should not raise — early return
    await _refund_redis_gcra(sw, decision, redis_client=None, settings=None)


# ── Redis backend: peek and reset with fake Redis ───────────────────────


async def test_peek_redis_log_exhausted() -> None:
    """_peek_redis_log returns is_exhausted=True and retry_after when at capacity."""

    class _FakeRedis:
        async def zcard(self, key: object) -> int:
            return 10

        async def zrange(
            self, key: object, start: int, end: int, withscores: bool = True
        ) -> list[tuple[bytes, float]]:
            return [(b"req1", 1000.0)]

    from taskq.ratelimit._sliding_window_redis import _peek_redis_log

    sw = SlidingWindow("test", limit=5, window=timedelta(seconds=10), backend="redis", style="log")
    state = await _peek_redis_log(
        sw, now_ms=5000, redis_client=_FakeRedis(), settings=_make_settings()
    )
    assert state.is_exhausted is True
    assert state.retry_after is not None


async def test_peek_redis_gcra_exhausted() -> None:
    """_peek_redis_gcra returns is_exhausted=True when TAT is far in the future."""

    class _FakeRedis:
        async def get(self, key: object) -> str | None:
            return "20000.0"

    from taskq.ratelimit._sliding_window_redis import _peek_redis_gcra

    sw = SlidingWindow("test", limit=5, window=timedelta(seconds=10), backend="redis", style="gcra")
    state = await _peek_redis_gcra(
        sw, now_ms=5000, redis_client=_FakeRedis(), settings=_make_settings()
    )
    assert state.is_exhausted is True
    assert state.retry_after is not None


async def test_reset_redis_log_deletes_key() -> None:
    """_reset_redis_log deletes the Redis key."""

    class _FakeRedis:
        def __init__(self) -> None:
            self.deleted: list[object] = []

        async def delete(self, key: object) -> int:
            self.deleted.append(key)
            return 1

    from taskq.ratelimit._sliding_window_redis import _reset_redis_log

    sw = SlidingWindow("test", limit=5, window=timedelta(seconds=10), backend="redis", style="log")
    redis = _FakeRedis()
    await _reset_redis_log(sw, redis_client=redis, settings=_make_settings())
    assert len(redis.deleted) == 1


async def test_reset_redis_gcra_deletes_key() -> None:
    """_reset_redis_gcra deletes the Redis key."""

    class _FakeRedis:
        def __init__(self) -> None:
            self.deleted: list[object] = []

        async def delete(self, key: object) -> int:
            self.deleted.append(key)
            return 1

    from taskq.ratelimit._sliding_window_redis import _reset_redis_gcra

    sw = SlidingWindow("test", limit=5, window=timedelta(seconds=10), backend="redis", style="gcra")
    redis = _FakeRedis()
    await _reset_redis_gcra(sw, redis_client=redis, settings=_make_settings())
    assert len(redis.deleted) == 1


# ── PG backend guard clauses: None pg_pool / settings ───────────────────


async def test_peek_pg_log_raises_without_pg_pool() -> None:
    """_peek_pg_log raises RuntimeError when pg_pool is None."""
    from taskq.ratelimit._sliding_window_pg import _peek_pg_log

    sw = SlidingWindow(
        "test", limit=5, window=timedelta(seconds=10), backend="postgres", style="log"
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await _peek_pg_log(sw, now_ms=0, pg_pool=None, clock=None, settings=None)


async def test_peek_pg_gcra_raises_without_pg_pool() -> None:
    """_peek_pg_gcra raises RuntimeError when pg_pool is None."""
    from taskq.ratelimit._sliding_window_pg import _peek_pg_gcra

    sw = SlidingWindow(
        "test", limit=5, window=timedelta(seconds=10), backend="postgres", style="gcra"
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await _peek_pg_gcra(sw, now_ms=0, pg_pool=None, clock=None, settings=None)


async def test_reset_pg_log_raises_without_pg_pool() -> None:
    """_reset_pg_log raises RuntimeError when pg_pool is None."""
    from taskq.ratelimit._sliding_window_pg import _reset_pg_log

    sw = SlidingWindow(
        "test", limit=5, window=timedelta(seconds=10), backend="postgres", style="log"
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await _reset_pg_log(sw, pg_pool=None, settings=None)


async def test_reset_pg_gcra_raises_without_pg_pool() -> None:
    """_reset_pg_gcra raises RuntimeError when pg_pool is None."""
    from taskq.ratelimit._sliding_window_pg import _reset_pg_gcra

    sw = SlidingWindow(
        "test", limit=5, window=timedelta(seconds=10), backend="postgres", style="gcra"
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await _reset_pg_gcra(sw, pg_pool=None, settings=None)


async def test_acquire_pg_log_raises_without_pg_pool() -> None:
    """_acquire_pg_log raises RuntimeError when pg_pool is None."""
    from uuid import uuid4

    from taskq.ratelimit._sliding_window_pg import _acquire_pg_log

    sw = SlidingWindow(
        "test", limit=5, window=timedelta(seconds=10), backend="postgres", style="log"
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await _acquire_pg_log(sw, pg_pool=None, clock=None, settings=None, request_id=uuid4())


async def test_acquire_pg_gcra_raises_without_pg_pool() -> None:
    """_acquire_pg_gcra raises RuntimeError when pg_pool is None."""
    from taskq.ratelimit._sliding_window_pg import _acquire_pg_gcra

    sw = SlidingWindow(
        "test", limit=5, window=timedelta(seconds=10), backend="postgres", style="gcra"
    )
    with pytest.raises(RuntimeError, match="pg_pool not injected"):
        await _acquire_pg_gcra(sw, pg_pool=None, clock=None, settings=None)
