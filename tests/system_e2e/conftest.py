"""The system-e2e tier's fixtures: the effects ledger and the client.

The tier builds on the MAIN suite's container fixtures (``pg_dsn`` for a
module-scoped database on the shared Postgres container,
``module_pg_schema`` for a migrated per-module schema, the redis fixtures
for the broker workloads) and adds only what is specific to simulating
the system: the body-run effects table every scenario's exactly-once
invariant reconciles against, and the production client opened on the
real DSN.

Workers are real subprocesses (see :mod:`tests.system_e2e._harness`), so
unlike tests/e2e there are no containers to build and no opt-in image
machinery - the tier's own opt-in flag (``--system-e2e``, root
conftest.py) gates collection, and Docker is required only for the
containers the fixtures below already own.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

import pytest_asyncio

from taskq import TaskQ

if TYPE_CHECKING:
    import asyncpg

    from taskq.testing.fixtures import ModulePgSchema

_EFFECTS_DDL = """
CREATE TABLE IF NOT EXISTS "{schema}".sys_effects (
    job_id  UUID NOT NULL,
    attempt INT NOT NULL,
    actor   TEXT NOT NULL,
    kind    TEXT NOT NULL,
    at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
"""


@pytest_asyncio.fixture
async def sys_ledger(
    pg_dsn: str, module_pg_schema: ModulePgSchema
) -> AsyncIterator[asyncpg.Connection]:
    """A raw connection with the module's effects table created.

    The table is a per-TEST clean slate: setup is IF NOT EXISTS (a
    crashed prior run's leftovers cannot poison the population) and
    teardown DROPs it, so no effects row can outlive the test that
    owned its jobs. Scenario bodies use the connection for seeding,
    chaos SQL and invariant reads; per-scenario tags keep the DELETEs
    bounded.
    """
    import asyncpg as _asyncpg

    conn = await _asyncpg.connect(pg_dsn)
    await conn.execute(_EFFECTS_DDL.format(schema=module_pg_schema.schema_name))
    yield conn
    await conn.execute(f'DROP TABLE IF EXISTS "{module_pg_schema.schema_name}".sys_effects')
    await conn.close()


@pytest_asyncio.fixture
async def sys_client(pg_dsn: str, module_pg_schema: ModulePgSchema) -> AsyncIterator[TaskQ]:
    """The production client on the module's DSN and schema: the real
    HTTP-free surface an operator's process would hold. Scenarios that
    need a SECOND concurrent client open their own (a rolling deploy has
    two generations of everything, clients included)."""
    async with TaskQ(dsn=pg_dsn, schema=module_pg_schema.schema_name) as client:
        yield client
