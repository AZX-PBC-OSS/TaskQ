# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Green pin: insert-insert ABBA between two multi-key enqueue transactions resolves.

Two connections inserting the same two idempotency keys in opposite
order inside open transactions form the canonical unique-index deadlock:
each transaction's INSERT ... ON CONFLICT (idempotency_scope,
idempotency_key) DO NOTHING (backend/_sql_templates.py) waits on the
other's uncommitted speculative token. With ``deadlock_timeout`` shrunk,
Postgres must detect the cycle and abort exactly one side with 40P01
``asyncpg.DeadlockDetectedError``.

Pinned dispositions per path:

* client (``_enqueue_on_conn`` used directly on a caller-owned open
  transaction): surfaces the RAW driver error - not swallowed, not
  converted to a TaskQ error - so the caller owns its retry policy;
* cron / bulk-cancel: classify 40P01 as transient and retry - pinned by
  membership in ``worker/_transient.py``'s TRANSIENT_PG_ERRORS (the cron
  loops' retry classifier) and by the existing retry-loop pins
  tests/test_rt_cancel_deadlock.py and tests/test_cancel_where_pg.py
  (``backend/_cancel_bulk.py`` retries a deadlocked batch), so this file
  pins the missing leg: the real resolution and the client surfacing.
"""

import asyncio
import contextlib

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.backend._enqueue import _enqueue_on_conn
from taskq.backend._protocol import JobRow
from taskq.backend._sql_templates import render as render_sql
from taskq.backend.clock import SystemClock
from taskq.migrate import apply_pending
from taskq.testing.jobs import make_enqueue_args
from taskq.worker._transient import TRANSIENT_PG_ERRORS

pytestmark = pytest.mark.integration


async def test_insert_insert_abba_deadlock_resolves_exactly_one_side_40p01(pg_dsn: str) -> None:
    """GREEN PIN: the ABBA cycle resolves - exactly one side raises the raw
    asyncpg.DeadlockDetectedError out of the enqueue path, its transaction
    rolls back, and the winner's transaction commits both keys.

    Contract being pinned (correct handling, green today): Postgres
    deadlock detection (deadlock_timeout shrunk to 50 ms) breaks the
    speculative-token cycle with exactly one victim; the enqueue path
    surfaces the driver error unchanged for the client caller; the
    victim's rows vanish (rollback), the winner's rows persist (commit).
    A regression that swallows, converts, or double-aborts the deadlock
    breaks this pin.
    """
    schema = f"tlck_{new_base62()}".lower()
    conn_a: asyncpg.Connection | None = None
    conn_b: asyncpg.Connection | None = None
    try:
        conn_a = await asyncpg.connect(pg_dsn)
        await apply_pending(conn_a, schema=schema)
        conn_b = await asyncpg.connect(pg_dsn)
        sql = render_sql(schema)
        clock = SystemClock()
        await conn_a.execute("SET deadlock_timeout = '50ms'")
        await conn_b.execute("SET deadlock_timeout = '50ms'")
        k_a = f"abba-a-{new_base62()}".lower()
        k_b = f"abba-b-{new_base62()}".lower()

        tx_a = conn_a.transaction()
        tx_b = conn_b.transaction()
        await tx_a.__aenter__()
        await tx_b.__aenter__()
        winner_task: asyncio.Task[JobRow] | None = None
        victim_task: asyncio.Task[JobRow] | None = None
        victim_exc: BaseException | None = None
        row_a1 = await _enqueue_on_conn(
            conn_a, sql, schema, clock, make_enqueue_args(idempotency_key=k_a)
        )
        row_b1 = await _enqueue_on_conn(
            conn_b, sql, schema, clock, make_enqueue_args(idempotency_key=k_b)
        )
        task_a2 = asyncio.create_task(
            _enqueue_on_conn(conn_a, sql, schema, clock, make_enqueue_args(idempotency_key=k_b))
        )
        await asyncio.sleep(0.1)
        task_b1 = asyncio.create_task(
            _enqueue_on_conn(conn_b, sql, schema, clock, make_enqueue_args(idempotency_key=k_a))
        )
        tx_of = {task_a2: tx_a, task_b1: tx_b}
        victim_tx_exited = False
        try:
            done, _pending = await asyncio.wait(
                {task_a2, task_b1}, timeout=10.0, return_when=asyncio.FIRST_COMPLETED
            )
            assert done, (
                "Contract: the ABBA cycle must produce a 40P01 victim within the "
                "bounded wait - neither contested insert resolved, so deadlock "
                "detection never fired"
            )
            first = next(iter(done))
            other = task_b1 if first is task_a2 else task_a2
            first_exc = first.exception()
            if isinstance(first_exc, asyncpg.DeadlockDetectedError):
                victim_task, victim_exc = first, first_exc
                winner_task = other
                await tx_of[victim_task].__aexit__(type(victim_exc), victim_exc, None)
                victim_tx_exited = True
            elif first_exc is None:
                winner_task, victim_exc = first, None
                victim_task = other
            else:
                pytest.fail(
                    "Contract: the first ABBA resolution must be the deadlock victim "
                    f"or the winner's row, not another error ({first_exc!r})"
                )
            if victim_exc is None:
                try:
                    await asyncio.wait_for(asyncio.shield(victim_task), timeout=5.0)
                except asyncpg.DeadlockDetectedError as exc:
                    victim_exc = exc
            assert isinstance(victim_exc, asyncpg.DeadlockDetectedError), (
                "Contract: exactly one side must raise the RAW asyncpg."
                "DeadlockDetectedError out of _enqueue_on_conn - the client "
                "disposition surfaces the driver error unconverted so the caller "
                f"owns its retry; got {victim_exc!r}"
            )
            winner_row = await asyncio.wait_for(asyncio.shield(winner_task), timeout=5.0)
            assert isinstance(winner_row, JobRow), (
                f"Contract: the winner's contested enqueue must complete; got {winner_row!r}"
            )
            if not victim_tx_exited:
                await tx_of[victim_task].__aexit__(type(victim_exc), victim_exc, None)
                victim_tx_exited = True
            await tx_of[winner_task].__aexit__(None, None, None)

            winner_first_row = row_a1 if winner_task is task_a2 else row_b1
            victim_first_row = row_b1 if winner_task is task_a2 else row_a1
            winner_conn = conn_a if winner_task is task_a2 else conn_b
            victim_conn = conn_b if winner_task is task_a2 else conn_a
            assert (
                await winner_conn.fetchval(
                    f'SELECT id FROM "{schema}".jobs WHERE id = $1', winner_first_row.id
                )
                is not None
            ), "Contract: the winner's own first insert must persist after its commit"
            assert (
                await winner_conn.fetchval(
                    f'SELECT id FROM "{schema}".jobs WHERE id = $1', winner_row.id
                )
                is not None
            ), (
                "Contract: the winner's contested insert (released by the victim's rollback) must persist"
            )
            assert winner_row.id != victim_first_row.id, (
                "Contract: the contested key's surviving row must be the WINNER's "
                "fresh insert, not the victim's rolled-back row"
            )
            assert (
                await victim_conn.fetchval(
                    f'SELECT id FROM "{schema}".jobs WHERE id = $1', victim_first_row.id
                )
                is None
            ), "Contract: the victim's own first insert must be gone after its rollback"
        finally:
            if not victim_tx_exited and victim_task is not None and victim_exc is not None:
                with contextlib.suppress(Exception):
                    await tx_of[victim_task].__aexit__(type(victim_exc), victim_exc, None)
        assert asyncpg.DeadlockDetectedError in TRANSIENT_PG_ERRORS, (
            "Contract: 40P01 must stay classified transient - the cron loops retry on "
            "TRANSIENT_PG_ERRORS and bulk-cancel's batch retry (pinned by "
            "tests/test_rt_cancel_deadlock.py and tests/test_cancel_where_pg.py) "
            "depends on the same family"
        )
    finally:
        if conn_b is not None:
            with contextlib.suppress(Exception):
                await conn_b.close()
        if conn_a is not None:
            with contextlib.suppress(Exception):
                await conn_a.close()
        with contextlib.suppress(Exception):
            conn = await asyncpg.connect(pg_dsn)
            try:
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            finally:
                with contextlib.suppress(Exception):
                    await conn.close()
