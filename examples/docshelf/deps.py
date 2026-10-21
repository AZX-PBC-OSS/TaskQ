"""The deps seam's binding — ONE instance, bound ONCE at the app's door.

The bodies never reach through ``ctx`` for services and never touch a
module global: a body's signature DECLARES ``deps: DocShelfDeps`` and
the runner hands the app's bound instance in. The pool holder is LAZY:
the app module imports in every process (the worker's, the tests', the
docs fences') — import never opens a connection and never demands the
DSN; the first body that needs the pool resolves the env IN ITS OWN
PROCESS (a worker's TASKQ_PG_DSN is the worker's).
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field

import asyncpg

from examples.docshelf.config import DocShelfConfig
from examples.docshelf.mockapi import MockExtractionAPI


@dataclass(slots=True)
class DocShelfDeps:
    """The ONE deps instance: the config, the mock API, the pool holder."""

    cfg: DocShelfConfig = field(default_factory=DocShelfConfig.load)
    api: MockExtractionAPI = field(init=False)
    _pool: asyncpg.Pool | None = field(default=None, init=False, repr=False)
    _schema: str | None = field(default=None, init=False, repr=False)
    _lock: asyncio.Lock = field(default=None, init=False, repr=False)  # type: ignore[assignment]  # Why: built lazily (a Lock wants a loop).

    def __post_init__(self) -> None:
        self.api = MockExtractionAPI(self.cfg.dynamics)
        self._lock = asyncio.Lock()

    def _env(self, name: str, what: str) -> str:
        value = os.environ.get(name, "")
        if not value:
            raise RuntimeError(
                f"{name} is not set — DocShelf's bodies drive a real Postgres; "
                f"export the {what} (see the guide's prerequisites)"
            )
        return value

    async def pool(self) -> asyncpg.Pool:
        """The process's pool, connected on first use (lazy — import must
        never open a connection; the env resolves in the RUNNING process)."""
        if self._pool is None:
            async with self._lock:
                if self._pool is None:
                    dsn = self._env("TASKQ_PG_DSN", "DSN")
                    schema = os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
                    self._pool = await asyncpg.create_pool(dsn, min_size=1, max_size=16)
                    self._schema = schema
        assert self._pool is not None
        return self._pool

    @property
    def schema(self) -> str:
        """The schema the pool was opened against (set at first connect)."""
        if self._schema is None:
            # The caller wants the schema before any body connected — the
            # env is the same answer (the schema is per-process config).
            return os.environ.get("TASKQ_SCHEMA_NAME", "taskq")
        return self._schema

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
