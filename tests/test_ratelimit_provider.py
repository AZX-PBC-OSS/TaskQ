"""Integration tests for the Redis pool DI provider.

get_redis_pool yields a usable Redis client; PING returns True.
"""

import asyncio

import pytest

from taskq.ratelimit._provider import get_redis_pool
from taskq.settings import WorkerSettings

pytestmark = [pytest.mark.integration, pytest.mark.redis]


# ── get_redis_pool yields a usable client ──────────────────────


async def test_get_redis_pool_yields_pingable_client(private_redis_url: str) -> None:
    # Why lazy: module-level redis imports break collection on extras that do
    # not install it (the suite's import-purity boundary rule).
    import redis.exceptions

    settings = WorkerSettings.load_from_dict(
        {"pg_dsn": "postgresql://u:p@h/d", "redis_url": private_redis_url},
    )

    async for client in get_redis_pool(settings):
        # The contract under test: the factory yields a WORKING client -
        # config correctness, not broker latency. The ping rides the
        # product's own production socket budget (deliberately not
        # opted out - get_redis_pool is the worker-loop's real client),
        # so the broker under the ping must be one whose latency the
        # test OWNS: a PRIVATE per-test Dragonfly (``private_redis_url``,
        # ~1s boot, no co-tenants). On the SHARED pair a single ping is a
        # lottery - the leg's co-tenancy stall band (historically
        # test_attack478_conserve's ``CLIENT PAUSE 6000 ALL`` issued on
        # the shared broker, plus ordinary -n 2 container churn)
        # measurably pushed every 5s-budget ping past its read deadline
        # when a band landed on the ping (measured 2026-09-27 on a
        # v1.39.0 Dragonfly replica: consecutive 5s-budget pings turned
        # into TimeoutError-at-read, the exact CI signature). The DURABLE
        # cure is the private broker, not a bigger window - every
        # budget-stretch cure found the next exposed ping in the next
        # leg (this pin, then test_worker_deps's). The retry window is
        # RETAINED as belt-and-braces for the private container's first
        # handshake under runner load; it is DERIVED (the dd4572ff
        # doctrine, not a guess) from the measured co-tenancy stretch -
        # 20x (test_cancel_storm's _STORM_DEADLINE_SECS derives from the
        # same band), applied to the ping's 5s production budget => ~100s
        # ceiling; 19 attempts x (5s budget + 0.5s gap) covers it and
        # pays nothing on a healthy broker (first ping wins).
        #
        # The catch below is the load-bearing part and it once was the
        # defect: redis-py does NOT raise the builtin TimeoutError on a
        # timed-out read - its read path converts the raw asyncio timeout
        # into ``redis.exceptions.TimeoutError``, which subclasses
        # ``RedisError`` (-> ``Exception``), NOT ``builtins.TimeoutError``
        # (redis-py 8.1.0 MRO; connection.py's ``raise TimeoutError(f
        # "Timeout reading from {host_error}")``). A bare
        # ``except TimeoutError`` resolved to the BUILTIN, so every
        # redis-py timeout escaped the catch on attempt one and this
        # derived window never ran a single retry: CI's 2026-09-27 leg
        # red was ONE >=5s stall (the traceback walks
        # on_connect_check_health -> read_response straight out of the
        # loop), not an exhausted ~104s - the leg's whole wall was
        # 81.32s. Both classes are caught: redis-py's converted class on
        # the wire paths, the builtin (== asyncio's, py3.11+) where a raw
        # asyncio timeout surfaces unwrapped.
        last_error: Exception | None = None
        for _attempt in range(19):
            try:
                result: bool = await client.ping()  # pyright: ignore[reportGeneralTypeIssues, reportUnknownVariableType] # Why: redis-py shares sync/async stubs; ping() returns Awaitable[bool] at runtime but pyright sees bool
                assert result is True
                last_error = None
                break
            except (redis.exceptions.TimeoutError, TimeoutError) as exc:
                # the 5s production budget against a stalled broker
                last_error = exc
                await asyncio.sleep(0.5)
        assert last_error is None, (
            f"the factory's client never pinged clean in the derived window "
            f"(19 attempts x (5s budget + 0.5s gap) ~= 104s ceiling): {last_error!r}"
        )
