# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team: cross-process crash recovery of the migration runner.

A migrator that dies mid-apply (container killed, DDL failure) must not
strand the schema: the advisory-lock protocol must let the NEXT migrator
re-acquire the lock and complete the job. For a non-transactional migration
the failed run's earlier statements are already committed and unrecorded, so
recovery depends on the documented idempotency contract — the second
migrator must re-execute the file, tolerate the debris, and only then record
it. Pinned here across two real connections through the real
``apply_pending_locked`` protocol (the single-connection re-run shape is
already pinned by tests/test_migrate_no_transaction.py; the lock-serialize
shape by tests/test_migrate_lock_integration.py).
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq import migrate as migrate_mod
from taskq._ids import new_base62
from taskq.migrate import Migration, apply_pending, apply_pending_locked, list_applied

pytestmark = pytest.mark.integration

_M1_SQL = 'CREATE TABLE "{schema}".crash_m1 (id int)'
_M2_SQL = (
    "-- taskq:no-transaction\n"
    'CREATE TABLE IF NOT EXISTS "{schema}".crash_m2 (id int);\n'
    "DO $$ BEGIN "
    'IF EXISTS (SELECT 1 FROM "{schema}".gate WHERE blocked) THEN '
    "RAISE EXCEPTION 'gated: transient condition'; "
    "END IF; END $$;"
)


async def test_second_migrator_completes_after_first_fails_mid_no_transaction_file(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Contract: a crashed migrator's partial, unrecorded effects must not
    strand the schema — the next migrator under the same lock protocol
    re-executes the file against the debris and completes it.

    The ``gate`` table stands in for a transient external condition (a
    blocking lock, an INVALID-index remnant, a wedged dependency): it fails
    the winner mid-file after its first statement committed, then clears,
    and the loser must finish the job and record it exactly once.
    """
    schema = f"tmg_{new_base62()}".lower()
    setup = await asyncpg.connect(pg_dsn)
    winner = await asyncpg.connect(pg_dsn)
    loser = await asyncpg.connect(pg_dsn)
    try:
        await setup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(setup, schema=schema)
        await setup.execute(f'CREATE TABLE "{schema}".gate (blocked boolean NOT NULL)')
        await setup.execute(f'INSERT INTO "{schema}".gate VALUES (true)')

        m1 = Migration(
            version="90.01.00_01",
            phase="pre",
            description="crash_step_one",
            filename="90.01.00_01_pre_crash_step_one.sql",
            sql_template=_M1_SQL,
        )
        m2 = Migration(
            version="90.02.00_01",
            phase="pre",
            description="crash_step_two",
            filename="90.02.00_01_pre_crash_step_two.sql",
            sql_template=_M2_SQL,
            use_transaction=False,
        )
        monkeypatch.setattr(migrate_mod, "discover", lambda: [m1, m2])

        with pytest.raises(SystemExit) as excinfo:
            await apply_pending_locked(conn=winner, schema=schema, lock_timeout=10.0)
        assert m2.filename in str(excinfo.value), (
            "the crash report must name the file that failed mid-apply"
        )
        assert "NOT recorded" in str(excinfo.value), (
            "the crash report must state the failed no-transaction file was not "
            "recorded — the re-run contract the next migrator relies on"
        )
        ledger_after_crash = await list_applied(winner, schema)
        assert m1.key in ledger_after_crash, (
            "the transactional step before the crash stays recorded"
        )
        assert m2.key not in ledger_after_crash, (
            "the crashed no-transaction file must not be recorded"
        )
        crash_m2_exists = await winner.fetchval(
            f"SELECT to_regclass('\"{schema}\".crash_m2') IS NOT NULL"
        )
        assert crash_m2_exists is True, (
            "the no-transaction file's first statement must have committed — the "
            "debris the next migrator has to tolerate"
        )

        await setup.execute(f'DELETE FROM "{schema}".gate')

        applied = await apply_pending_locked(conn=loser, schema=schema, lock_timeout=10.0)
        assert [m.key for m in applied] == [m2.key], (
            "the second migrator must re-execute exactly the crashed file — "
            "neither skipping it (stranded schema) nor re-applying the recorded one"
        )
        ledger_after_recovery = await list_applied(loser, schema)
        assert m1.key in ledger_after_recovery and m2.key in ledger_after_recovery, (
            "after recovery both steps must be recorded exactly once"
        )
        recorded_rows = await loser.fetchval(
            f'SELECT count(*) FROM "{schema}".schema_migrations WHERE version = $1',
            m2.key,
        )
        assert recorded_rows == 1, "the recovered file must be recorded exactly once"
    finally:
        for c in (setup, winner, loser):
            await c.close()
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()
