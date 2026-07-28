"""Integration test for ``make_pg_pool_factory(init=...)`` (issue #31).

Against real Postgres: an ``init`` hook forwarded through the factory runs
once per physical connection — on the initial connection, on connections
opened by pool growth, and on replacements opened after
``max_inactive_connection_lifetime`` recycling. The hook registers a real
per-connection ``set_type_codec`` (the same mechanism
``pgvector.asyncpg.register_vector`` uses) on a custom enum type, so the
test exercises actual codec registration rather than mere invocation.
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from taskq.auth import PgCredential, make_pg_pool_factory

pytestmark = pytest.mark.integration


class _StaticPgProvider:
    """PgCredentialProvider returning the test container's static credential."""

    def __init__(self, password: str) -> None:
        self._password = password
        self.calls = 0

    async def get_pg_credential(self) -> PgCredential:
        self.calls += 1
        return PgCredential(password=self._password)


async def test_init_hook_registers_codec_on_every_physical_connection(pg_dsn: str) -> None:
    admin = await asyncpg.connect(pg_dsn)
    try:
        await admin.execute("DROP TYPE IF EXISTS init_hook_flavor CASCADE")
        await admin.execute("CREATE TYPE init_hook_flavor AS ENUM ('vanilla', 'chocolate')")
    finally:
        await admin.close()

    initialized_pids: list[int] = []

    async def init(conn: asyncpg.Connection) -> None:
        # The same mechanism pgvector.asyncpg.register_vector uses: a
        # text-format codec registered by type name, per connection.
        await conn.set_type_codec(
            "init_hook_flavor",
            encoder=lambda v: v,
            decoder=lambda v: f"flavor:{v}",
            format="text",
        )
        initialized_pids.append(await conn.fetchval("SELECT pg_backend_pid()"))

    provider = _StaticPgProvider(password="taskq")  # pg_container fixture's static credentials
    # sslmode=disable: the factory injects sslmode=require only when the DSN
    # carries no explicit sslmode, and the test container serves no TLS.
    factory = make_pg_pool_factory(
        f"{pg_dsn}?sslmode=disable",
        provider,
        min_size=1,
        max_size=2,
        max_inactive_connection_lifetime=0.5,
        init=init,
    )

    pool = await factory()
    try:
        # Pool growth: two concurrent acquires need two physical connections,
        # and each must have run init for the codec to resolve the enum.
        async with pool.acquire() as c1, pool.acquire() as c2:
            pid1 = await c1.fetchval("SELECT pg_backend_pid()")
            pid2 = await c2.fetchval("SELECT pg_backend_pid()")
            assert pid1 != pid2
            assert await c1.fetchval("SELECT 'vanilla'::init_hook_flavor") == "flavor:vanilla"
            assert await c2.fetchval("SELECT 'chocolate'::init_hook_flavor") == "flavor:chocolate"
        assert {pid1, pid2} <= set(initialized_pids)

        # Recycling: both idle connections are terminated after 0.5s, so the
        # next acquire is a NEW physical connection — init must re-run on it.
        await asyncio.sleep(1.0)
        async with pool.acquire() as c3:
            pid3 = await c3.fetchval("SELECT pg_backend_pid()")
            assert pid3 not in {pid1, pid2}
            assert await c3.fetchval("SELECT 'vanilla'::init_hook_flavor") == "flavor:vanilla"
        assert pid3 in initialized_pids

        # Credential fetch stays per-factory-invocation even though init ran
        # per physical connection: one fetch for >=3 physical connections.
        assert provider.calls == 1
    finally:
        await pool.close()
