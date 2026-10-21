"""The process-shared pool, opened lazily INSIDE the running loop.

The deps instance is built at import time (the binding's law), but a
pool belongs to an event loop — and the worker's boot loop is not the
worker's run loop. So the deps hand out a lazy holder: the first body
that needs the pool opens it inside the loop that is actually running.
"""

from __future__ import annotations

import asyncio

import asyncpg


class LazyPool:
    """Get-or-open THE process pool (one pool per dsn per loop lifetime)."""

    def __init__(self, dsn: str, *, max_size: int = 20) -> None:
        self._dsn = dsn
        self._max_size = max_size
        self._pool: asyncpg.Pool | None = None
        self._lock = asyncio.Lock()

    async def get(self) -> asyncpg.Pool:
        if self._pool is None or self._pool.is_closing():
            # Single-flight: concurrent first-touching bodies must not
            # race two pools into existence.
            async with self._lock:
                if self._pool is None or self._pool.is_closing():
                    self._pool = await asyncpg.create_pool(
                        self._dsn, min_size=1, max_size=self._max_size
                    )
        assert self._pool is not None
        return self._pool

    async def close(self) -> None:
        if self._pool is not None and not self._pool.is_closing():
            await self._pool.close()
