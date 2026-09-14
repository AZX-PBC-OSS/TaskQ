# Why: schema is a fixed test identifier, not user input; every value is $-bound.
# (No file-level S608 noqa: none of this file's statements match the S608 SELECT-interpolation pattern.)
"""Red-team pin (PG): the leader probes cannot see a read-only server — writes can.

The leader's liveness probes are pure reads — ``SELECT 1`` on ``deps.leader_conn``
(election loop) and on the monitor conn (watchdog loop, leader.py ``fetchval``
probe site) — which SUCCEED on a read-only server, while every leader WRITE (the
election upsert into maintenance_leader, the cron tick's transaction, the sweeps)
fails with SQLSTATE 25006 ``asyncpg.ReadOnlySQLTransactionError``.  The watchdog
therefore reports a healthy leader on a server where the leader cannot do any
leader work — the asymmetry the watchdog is structurally blind to.

This file pins the FACT on a real PG session flipped read-only via
``set_config('default_transaction_read_only', 'on', ...)`` — the session-level
equivalent of a failover's read-only window — so the classification defect it
feeds (25006 missing from TRANSIENT_PG_ERRORS, pinned in
``tests/test_rt_leader_read_only_classification.py``) has its precondition proven:
probe green, advisory-lock acquire green, write 25006.
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.constants import schema_lock_name
from taskq.migrate import apply_pending

pytestmark = pytest.mark.integration


async def test_probes_and_locks_succeed_where_leader_writes_fail_read_only(
    pg_dsn: str,
) -> None:
    schema = f"tlrt_{new_base62()}".lower()

    setup = await asyncpg.connect(pg_dsn)
    try:
        await setup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(setup, schema=schema)
    finally:
        await setup.close()

    conn = await asyncpg.connect(pg_dsn)
    try:
        # The session-level equivalent of a failover's read-only window.
        await conn.execute("SELECT set_config('default_transaction_read_only', 'on', false)")

        # The watchdog / election probe shape: a pure read — SUCCEEDS.
        probed = await conn.fetchval("SELECT 1")
        assert probed == 1, (
            "precondition: the pure-read probe must succeed on a read-only session — "
            "if even SELECT 1 failed, the watchdog would see the condition and this "
            "asymmetry would not exist"
        )

        # The election lock shape: the session-scoped advisory-lock acquire is NOT
        # a database write — it SUCCEEDS on a read-only server, so an election can
        # even WIN the lock on a server where the upsert that follows must fail.
        got_lock = await conn.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))",
            schema_lock_name("maintenance_leader", schema),
        )
        assert got_lock is True, (
            "the election lock acquire (pg_try_advisory_lock) is not a database write "
            "and must succeed on a read-only session — if PG forbade it, elections "
            "would fail loudly on read-only and the asymmetry would be half as deep"
        )

        # Every leader WRITE shape: fails 25006 on the very same session.
        with pytest.raises(asyncpg.ReadOnlySQLTransactionError) as excinfo:
            await conn.execute(f'CREATE TABLE "{schema}"."rt_write_probe" (i int)')
        assert excinfo.value.sqlstate == "25006", (
            "the write failure must carry SQLSTATE 25006 (read_only_sql_transaction) — "
            f"got {excinfo.value.sqlstate!r}"
        )

        # The asymmetry itself: the probe is still green after the write failed.
        probed_again = await conn.fetchval("SELECT 1")
        assert probed_again == 1, (
            "the probe must still succeed after writes began failing — this is the "
            "exact state the watchdog cannot distinguish from a healthy leader: "
            "SELECT 1 green, every leader write 25006, and the worker's survival "
            "riding entirely on TRANSIENT_PG_ERRORS knowing the shape (it does not — "
            "see tests/test_rt_leader_read_only_classification.py)"
        )
    finally:
        await conn.close()

    # A fresh session is read-write: clean the schema up.
    cleanup = await asyncpg.connect(pg_dsn)
    try:
        await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await cleanup.close()
