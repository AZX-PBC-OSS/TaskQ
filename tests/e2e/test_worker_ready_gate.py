"""The e2e readiness gate's own controls.

``wait_for_worker_ready`` is the one gate every e2e worker fixture
yields through; its contract (its docstring) is that only a worker whose
heartbeat loop is ALIVE satisfies it - a worker that crashes
mid-bootstrap (after ``register_worker`` but before its first tick)
never does. The registration INSERT itself stamps
``last_seen_at = clock_timestamp()``, which is strictly greater than
``started_at = now()`` (the transaction's start) - so a gate keyed on
``last_seen_at > started_at`` admits the registration ROW, not a
heartbeat TICK, and a fixture can yield while the boot is still between
``register_worker`` and its later bootstrap steps (the diagnosed
mechanism of test_cron's "worker startup did not register the cron
schedule" reds). The gate therefore requires a SECOND BEAT: the row's
own ``last_seen_at`` strictly increasing across two gate samples -
something only a live heartbeat loop produces."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import pytest

from taskq.backend._sql import WORKER_REGISTRATION_SQL_TEMPLATE
from taskq._ids import new_uuid

from ._assertions import wait_for_worker_ready

if TYPE_CHECKING:
    import asyncpg

    from .conftest import E2ESchema

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


async def _seed_registration_row(pool: asyncpg.Pool, schema: str) -> object:
    """One workers row in the boot registration's EXACT shipped shape -
    written by the shipped template, no heartbeat loop behind it."""
    worker_id = new_uuid()
    sql = WORKER_REGISTRATION_SQL_TEMPLATE.format(schema=schema)
    await pool.execute(
        sql, worker_id, "gate-control-host", 424242, ["e2e"], "gate-control", None, "{}"
    )
    return worker_id


async def test_the_worker_ready_gate_refuses_a_never_ticking_registration_row(
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
) -> None:
    """A registration-shaped row with NO heartbeat loop behind it must
    never satisfy the gate: it times out. (Pre-fix, the gate passed on
    the row itself - the mid-bootstrap admission the contract forbids.)"""
    schema = e2e_schema.schema_name
    worker_id = await _seed_registration_row(e2e_pg_pool, schema)
    try:
        with pytest.raises(TimeoutError, match="second post-registration beat"):
            await wait_for_worker_ready(
                e2e_pg_pool,
                schema,
                timeout=2.0,
                known_workers=frozenset(),
            )
    finally:
        await e2e_pg_pool.execute(f'DELETE FROM "{schema}".workers WHERE id = $1', worker_id)


async def test_the_worker_ready_gate_admits_a_genuinely_beating_worker(
    e2e_pg_pool: asyncpg.Pool,
    e2e_schema: E2ESchema,
) -> None:
    """The positive witness: a worker whose own ``last_seen_at`` moves
    across the gate's samples satisfies the gate promptly - the beat
    rule's admission direction (the fix must not time out everything)."""
    schema = e2e_schema.schema_name
    worker_id = await _seed_registration_row(e2e_pg_pool, schema)

    async def _beat_twice() -> None:
        for _ in range(2):
            await asyncio.sleep(0.4)
            await e2e_pg_pool.execute(
                f'UPDATE "{schema}".workers SET last_seen_at = clock_timestamp() WHERE id = $1',
                worker_id,
            )

    try:
        beater = asyncio.create_task(_beat_twice())
        await wait_for_worker_ready(
            e2e_pg_pool,
            schema,
            timeout=10.0,
            known_workers=frozenset(),
        )
        await beater
    finally:
        await e2e_pg_pool.execute(f'DELETE FROM "{schema}".workers WHERE id = $1', worker_id)
