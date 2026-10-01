"""Dependency-guard preamble shared by the ratelimit store delegates.

Every Redis and PG delegate (the token bucket's methods and both
sliding-window styles' module functions) opens with the same two-guard
block: the store client and the settings must both be injected or the
delegate cannot answer. The guards here preserve the exception-type
split every caller and wording pin relies on:

* a missing **PG pool** raises the typed
  :class:`taskq.exceptions.RateLimitDependencyUnavailable`, the
  fail-closed denial contract the acquire boundary maps to a denial
  outcome (see that class's docstring);
* every other missing dependency (the Redis client, the settings) is a
  plain :class:`RuntimeError`, never the typed denial.

Both helpers return the injected dependency paired with the settings'
schema name, the value every delegate extracts immediately after the
guards.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from taskq.exceptions import RateLimitDependencyUnavailable

if TYPE_CHECKING:
    import asyncpg
    import redis.asyncio as redis_async

    from taskq.settings import WorkerSettings

__all__ = ["_require_pg", "_require_redis"]


def _require_pg(
    pg_pool: asyncpg.Pool | None,
    settings: WorkerSettings | None,
    role: str,
) -> tuple[asyncpg.Pool, str]:
    """Guard the PG store dependency, return ``(pool, schema_name)``.

    ``role`` is the message suffix pinning which delegate failed (``"postgres
    backend"``, ``"postgres gcra refund"``, ...), preserved byte-identical.
    """
    if pg_pool is None:
        raise RateLimitDependencyUnavailable(f"pg_pool not injected for {role}")
    if settings is None:
        raise RuntimeError(f"settings not injected for {role}")
    return pg_pool, settings.schema_name


def _require_redis(
    redis_client: redis_async.Redis | None,
    settings: WorkerSettings | None,
    role: str,
) -> tuple[redis_async.Redis, str]:
    """Guard the Redis client dependency, return ``(client, schema_name)``.

    Both guards raise plain :class:`RuntimeError`: only the PG store's
    absence is the typed fail-closed denial. ``role`` is the message
    suffix (``"redis backend"``, ``"redis gcra refund"``, ...),
    preserved byte-identical.
    """
    if redis_client is None:
        raise RuntimeError(f"redis_client not injected for {role}")
    if settings is None:
        raise RuntimeError(f"settings not injected for {role}")
    return redis_client, settings.schema_name
