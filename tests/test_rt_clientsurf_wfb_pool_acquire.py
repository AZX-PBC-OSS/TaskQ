"""Red-team: wait_for_batch's pool acquire has no bound (PG/client path).

The blocking form of :func:`taskq.batch.wait_for_batch`
(``snooze_via_exception=False`` — "Use this form from scripts and
integration tests", src/taskq/batch.py:469-473) polls through::

    async def _fetch() -> BatchCompletionStatus:
        if isinstance(db, _asyncpg.Pool):
            async with db.acquire() as conn:   # <- no timeout
                return await _fetch_and_decide(conn)

(src/taskq/batch.py:576-580). asyncpg's ``Pool.acquire`` has NO default
timeout, so on an exhausted pool every poll — the first one and every
snooze-loop iteration after it — parks forever.

The project already treats this exact shape as a defect elsewhere:
``JobsClient._schedule_seed_now`` wraps its pool acquire in
``asyncio.wait_for(..., DEFAULT_CAPACITY_READ_TIMEOUT)`` with the
rationale "asyncpg acquire has no default timeout, so an unbounded
acquire wedges every create_schedule/update_schedule on an exhausted
pool" (src/taskq/client/_jobs.py:1509-1517), and the workgroup health
check passes ``pg_pool.acquire(timeout=2.0)`` for the same reason
(src/taskq/worker/workgroup.py:433). The batch-blocking path is the one
unbounded acquire left on a caller-facing wait surface, and the snooze
loop is documented for scripts — the callers least equipped to notice a
wedge.

RED test asserts the DESIRED observable: a poll that cannot acquire a
connection within a bounded window raises TimeoutError (the project's
report idiom) instead of hanging. The observation window is bounded so
a red never hangs the suite; a free-pool GREEN control proves the SQL
path itself is healthy and the only failing variable is the acquire.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from taskq._ids import new_base62, new_uuid
from taskq.batch import wait_for_batch
from taskq.migrate import apply_pending

pytestmark = pytest.mark.integration

_OBSERVE_SECS = 4.0
"""Bounded observation window: long enough to prove the call did not
return or raise within any plausible acquire bound (the project's own
acquire bounds are 2 s), short enough to never hang the suite."""


@pytest_asyncio.fixture
async def tcs_schema(pg_dsn: str) -> AsyncIterator[str]:
    """A fresh random schema, migrated, dropped afterwards."""
    schema = f"tcs_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()
    try:
        yield schema
    finally:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await conn.close()


async def test_blocking_wait_for_batch_pool_acquire_is_bounded(
    pg_dsn: str, tcs_schema: str
) -> None:
    """RED: when the pool cannot yield a connection, wait_for_batch
    (blocking mode) must raise TimeoutError within a bounded window —
    not park forever inside db.acquire().

    Current behavior: src/taskq/batch.py:578 ``async with db.acquire()``
    carries no timeout, so with the pool's only connection held the very
    first poll of the blocking loop never starts and the caller's
    "convenience" wait wedges indefinitely — the same hazard class the
    project fixed at src/taskq/client/_jobs.py:1529-1537 by bounding the
    identical acquire shape."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        held = await pool.acquire()  # the pool's only connection, held for the whole call
        try:
            batch_id = new_uuid()
            task = asyncio.create_task(
                wait_for_batch(
                    pool,
                    batch_id,
                    schema=tcs_schema,
                    snooze_via_exception=False,
                    on_empty="ok",
                )
            )
            _done, pending = await asyncio.wait({task}, timeout=_OBSERVE_SECS)

            if task in pending:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                pytest.fail(
                    "CONTRACT: wait_for_batch (blocking mode) must raise TimeoutError "
                    "when its pool cannot yield a connection within a bounded window — "
                    "the project rule is 'every wait on something outside the process "
                    "is bounded, and exceeding the bound is reported', and the project "
                    "already bounds this exact shape elsewhere (acquire timeout at "
                    "src/taskq/client/_jobs.py:1529-1537, pg_pool.acquire(timeout=2.0) "
                    "at src/taskq/worker/workgroup.py:433). Instead the call was still "
                    "parked inside db.acquire() after "
                    f"{_OBSERVE_SECS}s: src/taskq/batch.py:578 'async with "
                    "db.acquire()' carries no timeout, so an exhausted pool wedges "
                    "every poll of the blocking snooze loop forever."
                )

            exc = task.exception()
            assert isinstance(exc, TimeoutError), (
                "expected a clear TimeoutError reporting the un-acquirable pool "
                f"within {_OBSERVE_SECS}s, got "
                f"{type(exc).__name__ if exc is not None else 'no exception'}"
            )
        finally:
            await held.close()
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(pool.close(), timeout=5.0)


async def test_blocking_wait_for_batch_returns_on_a_free_pool(pg_dsn: str, tcs_schema: str) -> None:
    """GREEN control: the same call on a FREE pool completes promptly with
    the empty status (on_empty='ok'), proving the SQL templates and the
    loop are healthy and the only failing variable in the RED test is
    the unbounded acquire."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=1)
    try:
        batch_id: UUID = new_uuid()
        status = await asyncio.wait_for(
            wait_for_batch(
                pool,
                batch_id,
                schema=tcs_schema,
                snooze_via_exception=False,
                on_empty="ok",
            ),
            timeout=10.0,
        )
        assert status.total == 0 and status.pending == 0, (
            f"control: an unknown batch_id with on_empty='ok' must report the "
            f"empty status, got total={status.total} pending={status.pending}"
        )
    finally:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(pool.close(), timeout=5.0)
