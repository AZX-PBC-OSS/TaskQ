"""DI registry for the e2e worker container — ``pg_pool`` (LOOP) + fake HTTP client (TRANSIENT).

Importable standalone (no sibling imports): only the in-container
``e2e.worker_entry`` calls :func:`build_registry`; the test process never
wires providers. ``actors.py`` imports :class:`FakeHttpClient` from here
via a relative import so the DI type annotation resolves under both
package roots.
"""

from collections.abc import AsyncIterator

import asyncpg

from taskq.di import ProviderRegistry, Scope
from taskq.settings import WorkerSettings


class FakeHttpClient:
    """Deterministic fake HTTP client — never touches a socket.

    Registered at TRANSIENT scope, so each actor invocation receives a
    fresh instance. ``call_count`` / ``calls`` record invocations for
    behavior assertions (surfaced to tests via ``e2e_effects`` rows).
    """

    def __init__(self) -> None:
        self.call_count = 0
        self.calls: list[str] = []

    async def get(self, path: str) -> dict[str, object]:
        """Return a deterministic static response; no I/O of any kind."""
        self.call_count += 1
        self.calls.append(path)
        return {"path": path, "status": 200, "body": {"tier": "premium", "source": "e2e-fake"}}


async def _pg_pool_factory() -> AsyncIterator[asyncpg.Pool]:
    """LOOP-scope asyncpg pool to the module-schema PG (network-alias DSN from env)."""
    pool = await asyncpg.create_pool(str(WorkerSettings.load().pg_dsn), min_size=1, max_size=4)
    yield pool
    await pool.close()


def _fake_http_factory() -> FakeHttpClient:
    """TRANSIENT-scope factory — a fresh fake client per invocation."""
    return FakeHttpClient()


def build_registry() -> ProviderRegistry:
    """Build and return the e2e DI registry.

    Called by ``worker_entry`` and passed to ``worker_main(di_registry=...)``.
    Do NOT call ``validate()`` here — the worker does that during bootstrap.
    Registering ``asyncpg.Pool`` overrides the worker's default pool
    registration (see ``taskq.worker._bootstrap``).
    """
    registry = ProviderRegistry()
    registry.register_factory(asyncpg.Pool, Scope.LOOP, _pg_pool_factory)
    registry.register_factory(FakeHttpClient, Scope.TRANSIENT, _fake_http_factory)
    return registry
