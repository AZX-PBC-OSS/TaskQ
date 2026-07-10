"""Integration tests for the Redis pool DI provider.

get_redis_pool yields a usable Redis client; PING returns True.
"""

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
        result: bool = await client.ping()  # pyright: ignore[reportGeneralTypeIssues, reportUnknownVariableType] # Why: redis-py shares sync/async stubs; ping() returns Awaitable[bool] at runtime but pyright sees bool
        assert result is True
