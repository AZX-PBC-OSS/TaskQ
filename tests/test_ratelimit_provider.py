"""Integration tests for the Redis pool DI provider.

get_redis_pool yields a usable Redis client; PING returns True.
"""

import asyncio

import pytest

from taskq.ratelimit._provider import get_redis_pool
from taskq.settings import WorkerSettings

pytestmark = [pytest.mark.integration, pytest.mark.redis]


# ── get_redis_pool yields a usable client ──────────────────────


async def test_get_redis_pool_yields_pingable_client(redis_url: str) -> None:
    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": redis_url},
    )

    async for client in get_redis_pool(settings):
        # The contract under test: the factory yields a WORKING client -
        # config correctness, not broker latency. The ping rides the
        # product's own production socket budget (deliberately not
        # opted out - get_redis_pool is the worker-loop's real client),
        # and the SHARED co-tenanted broker's stall band measurably
        # exceeds 5s under -n 2 load (the same weather class the
        # test-built clients opt out of): a single ping is a lottery.
        # Bounded retries prove pingability while tolerating the stall;
        # a broker down for all three is a real failure.
        last_error: Exception | None = None
        for _attempt in range(3):
            try:
                result: bool = await client.ping()  # pyright: ignore[reportGeneralTypeIssues, reportUnknownVariableType] # Why: redis-py shares sync/async stubs; ping() returns Awaitable[bool] at runtime but pyright sees bool
                assert result is True
                last_error = None
                break
            except TimeoutError as exc:  # the 5s production budget against a stalled broker
                last_error = exc
                await asyncio.sleep(0.5)
        assert last_error is None, (
            f"the factory's client never pinged clean in 3 attempts: {last_error!r}"
        )
