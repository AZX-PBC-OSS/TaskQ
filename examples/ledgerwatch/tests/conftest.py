"""LedgerWatch's drill fixtures: the estate, the fleet, the receipts."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio

import taskq.migrate

#: The env is stamped (and the app imported) at support's import time —
#: the deps binding, the worker fleet and every trigger agree on the
#: estate.
from examples.ledgerwatch.app import (
    app as lw_app,  # noqa: F401  # pyright: ignore[reportUnusedImport]  # Why: the import IS the estate's registration for the in-process runner doors.
)
from examples.ledgerwatch.tests.support import (
    SCHEMA,
    TEST_DSN,
    Fleet,
)


@pytest.fixture(scope="session")
def lw_schema() -> str:
    """The drill estate's schema name (one per pytest invocation)."""
    return SCHEMA


@pytest.fixture()
def schema(estate: str) -> str:
    """The estate's name (the tests' SQL reads ride it)."""
    return estate


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def estate(lw_schema: str) -> AsyncIterator[str]:
    """Migrations + the app's tables, once per invocation; the schema is
    dropped after (the disposable estate)."""
    conn = await asyncpg.connect(TEST_DSN)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{lw_schema}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{lw_schema}"')
    finally:
        await conn.close()
    await taskq.migrate.apply_pending_locked(TEST_DSN, schema=lw_schema, phase="pre")
    await taskq.migrate.apply_pending_locked(TEST_DSN, schema=lw_schema, phase="post")
    from examples.ledgerwatch.store import bootstrap_conn

    conn = await asyncpg.connect(TEST_DSN)
    try:
        await bootstrap_conn(conn, lw_schema)
    finally:
        await conn.close()
    yield lw_schema
    if os.environ.get("LEDGERWATCH_KEEP_SCHEMA"):
        return  # the post-mortem lane: keep the estate for row-level inspection
    conn = await asyncpg.connect(TEST_DSN)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{lw_schema}" CASCADE')
    finally:
        await conn.close()


@pytest_asyncio.fixture(loop_scope="module")
async def pool(estate: str) -> AsyncIterator[asyncpg.Pool]:
    """The drill process's own pool (loop-bound — one per test module)."""
    pool = await asyncpg.create_pool(TEST_DSN, min_size=1, max_size=20)
    yield pool
    await pool.close()


@pytest_asyncio.fixture(loop_scope="module")
async def fleet(
    pool: asyncpg.Pool, estate: str, request: pytest.FixtureRequest
) -> AsyncIterator[Fleet]:
    """One REAL worker subprocess, ready (heartbeating), profile from
    the module's ``FLEET_ENV`` (the drills' recorded env)."""
    _ = pool  # the readiness poll rides its own connections; the pool pins the module's loop
    overrides: dict[str, str] = dict(getattr(request.module, "FLEET_ENV", {}))
    f = Fleet(estate)
    await f.start(1, **overrides)
    yield f
    await f.stop_graceful()
