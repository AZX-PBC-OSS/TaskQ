# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Green pin: a successful migration's GUC state on a caller-owned connection.

``migrate.migration_advisory_lock`` (the lock context
``apply_pending_locked`` runs under) widens the caller connection's
``statement_timeout`` to unlimited unconditionally after acquiring the
lock (``SET statement_timeout = 0``) and NEVER restores it - the
docstring argues this for exit in general ("the widened state is
deliberately NOT restored on exit"), not only the failure path.
``lock_timeout``, in contrast, IS reset to unlimited (``SET
lock_timeout = 0``) before the DDL runs.

This file pins that documented tradeoff on the SUCCESS path: judged per
the audit's instruction, the docstring's argument ("the unlock must not
trade a GUC restore for a failed migration run") is written against exit
in general, so the success-path non-restore is documented behavior, not
an unargued defect. A caller-owned connection that successfully migrated
keeps ``statement_timeout = 0`` (its prior session value is clobbered)
while its ``lock_timeout`` is reset. A future change that restores the
caller's GUCs on success must update this pin deliberately.
"""

import contextlib

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.migrate import apply_pending_locked

pytestmark = pytest.mark.integration


async def test_successful_migration_leaves_documented_guc_state_on_caller_conn(
    pg_dsn: str,
) -> None:
    """GREEN PIN (documented tradeoff): after a SUCCESSFUL
    apply_pending_locked on a caller-owned connection that carried a
    session statement_timeout, the connection keeps statement_timeout = 0
    (never restored) while lock_timeout is reset to 0.

    Contract being pinned: migrate.py's migration_advisory_lock widens
    statement_timeout to 0 for the apply phase and deliberately does not
    restore it on exit ("the widened state is deliberately NOT restored
    on exit") - including the success path, per the docstring's own
    argument - while lock_timeout is explicitly reset ("Reset before the
    DDL so a legitimately long migration step is not killed by the wait
    bound"). Today both GUCs read '0' after success; the caller's prior
    '5s' statement_timeout is gone. This pins the documented asymmetry so
    a deliberate restore-on-success must consciously update it.
    """
    schema = f"tlck_{new_base62()}".lower()
    conn: asyncpg.Connection | None = None
    try:
        conn = await asyncpg.connect(pg_dsn)
        await conn.execute("SET statement_timeout = '5s'")
        applied = await apply_pending_locked(conn=conn, schema=schema, lock_timeout=5.0)
        assert applied, (
            "fixture fidelity: the fresh random schema had pending migrations, so the "
            "SUCCESS path ran (an empty apply would pin nothing)"
        )
        statement_timeout = await conn.fetchval("SELECT current_setting('statement_timeout')")
        assert statement_timeout == "0", (
            "Contract (documented tradeoff, pinned): a successful migration leaves "
            "the caller-owned connection's statement_timeout at '0' - the widened "
            "state is deliberately NOT restored on exit (migrate.py docstring), so "
            "the caller's prior '5s' session bound is gone. Today's behavior; a "
            "future restore-on-success must update this pin deliberately. Got "
            f"{statement_timeout!r}"
        )
        lock_timeout = await conn.fetchval("SELECT current_setting('lock_timeout')")
        assert lock_timeout == "0", (
            "Contract (documented reset): the wait bound IS reset before the DDL - "
            "lock_timeout must read '0' after a successful run, not the acquire-time "
            f"bound; got {lock_timeout!r}"
        )
    finally:
        if conn is not None:
            with contextlib.suppress(Exception):
                await conn.close()
        with contextlib.suppress(Exception):
            drop_conn = await asyncpg.connect(pg_dsn)
            try:
                await drop_conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            finally:
                with contextlib.suppress(Exception):
                    await drop_conn.close()
