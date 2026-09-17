"""A pool whose every checkout is bounded, for request handlers.

A request handler that awaited ``pool.acquire()`` with no bound hung for
as long as the pool had no connection to give - a pool whose connections
are all wedged behind a black-holed Postgres, or all held by streams - and
every request behind it hung the same way, with nothing reported. Every
handler in the admin UI and the progress router checks out through
:class:`BoundedPool` instead.
"""

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

import asyncpg
import structlog
from fastapi import HTTPException

if TYPE_CHECKING:
    from asyncpg.pool import PoolConnectionProxy

logger = structlog.get_logger("taskq.web.pool")

__all__ = ["BoundedPool"]


class BoundedPool:
    """*pool* with every checkout bounded by *acquire_timeout*.

    :meth:`acquire` waits at most ``acquire_timeout`` seconds (the
    ``TASKQ_ADMIN_ACQUIRE_TIMEOUT`` setting) and answers a checkout that
    does not arrive in time with 503 and ``Retry-After``, logged as
    ``pool-acquire-timeout`` with the pool's occupancy so the operator can
    tell exhaustion from a dead database. The raw pool stays reachable as
    :attr:`pool` for library calls that manage their own checkouts.
    *role* names the router in the log line.
    """

    __slots__ = ("acquire_timeout", "pool", "role")

    def __init__(self, pool: asyncpg.Pool, *, acquire_timeout: float, role: str) -> None:
        self.pool = pool
        self.acquire_timeout = acquire_timeout
        self.role = role

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator["PoolConnectionProxy"]:
        acquired = False
        try:
            async with self.pool.acquire(timeout=self.acquire_timeout) as conn:
                acquired = True
                yield conn
        except TimeoutError:
            if acquired:
                # A timeout raised by the handler's own work inside the
                # checkout is the handler's to report, not a checkout that
                # never arrived.
                raise
            logger.warning(
                "pool-acquire-timeout",
                role=self.role,
                acquire_timeout=self.acquire_timeout,
                pool_size=self.pool.get_size(),
                pool_idle=self.pool.get_idle_size(),
            )
            raise HTTPException(
                status_code=503,
                detail=(
                    f"no database connection became available within "
                    f"{self.acquire_timeout:g}s (TASKQ_ADMIN_ACQUIRE_TIMEOUT); the pool "
                    "is exhausted or Postgres is not answering"
                ),
                headers={"Retry-After": "2"},
            ) from None
