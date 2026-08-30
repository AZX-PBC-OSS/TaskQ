"""Migration locking: reachable from the CLI, and bounded when contended.

Two defects with one root cause in `migrate.py`'s locking:

* `taskq migrate up` -- the command the README names as *the* deploy step and
  calls idempotent -- called the UNLOCKED `apply_pending`. The lock-protected
  wrapper existed in the same module but was wired only into `ui serve
  --migrate`. Concurrently against a virgin schema, the loser hits the
  pre-initial migration's bare `CREATE TABLE` (no `IF NOT EXISTS`) and
  crash-loops on `DuplicateTableError`.
* The acquire itself was `pg_advisory_lock`, which BLOCKS with no client-side
  bound and no log line. A replica arriving while another holds the lock
  mid-DDL waited indefinitely, blew past its startup probe, was killed,
  restarted, and blocked again -- restart churn stacked on the lock window.
  (The paired unlock three lines below was already bounded, with a comment
  explaining exactly why an unbounded wait is dangerous.)
"""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from taskq import migrate as migrate_mod

pytestmark = pytest.mark.integration


async def test_lock_contention_raises_instead_of_blocking_forever(pg_dsn: str) -> None:
    """A second migrator must fail fast and say why.

    Pre-fix this call blocked until the holder released -- forever, in the
    wedged-holder case this bound exists for.
    """
    holder = await asyncpg.connect(pg_dsn)
    try:
        await holder.execute(
            "SELECT pg_advisory_lock($1)", migrate_mod._MIGRATION_LOCK_KEY
        )  # Why: pinning the exact key the migrator uses.

        async with asyncio.timeout(20):
            with pytest.raises(SystemExit) as excinfo:
                await migrate_mod.apply_pending_locked(
                    pg_dsn, schema="lock_contention_test", lock_timeout=1.0
                )

        msg = str(excinfo.value)
        assert "another process is applying migrations" in msg
        # Must not be misreported as a broken migration.
        assert "migration failed, aborting startup" not in msg
    finally:
        await holder.execute("SELECT pg_advisory_unlock($1)", migrate_mod._MIGRATION_LOCK_KEY)
        await holder.close()


async def test_lock_is_released_so_the_next_migrator_proceeds(pg_dsn: str) -> None:
    """The bound must not leak the lock on the failure path."""
    holder = await asyncpg.connect(pg_dsn)
    try:
        await holder.execute("SELECT pg_advisory_lock($1)", migrate_mod._MIGRATION_LOCK_KEY)
        with pytest.raises(SystemExit):
            await migrate_mod.apply_pending_locked(
                pg_dsn, schema="lock_release_test", lock_timeout=1.0
            )
    finally:
        await holder.execute("SELECT pg_advisory_unlock($1)", migrate_mod._MIGRATION_LOCK_KEY)
        await holder.close()

    # Uncontended now: the same call must succeed and actually migrate.
    applied = await migrate_mod.apply_pending_locked(pg_dsn, schema="lock_release_test")
    assert applied, "migrations should have been applied once the lock was free"

    probe = await asyncpg.connect(pg_dsn)
    try:
        exists = await probe.fetchval(
            "SELECT to_regclass('\"lock_release_test\".jobs') IS NOT NULL"
        )
        assert exists is True
        await probe.execute('DROP SCHEMA IF EXISTS "lock_release_test" CASCADE')
    finally:
        await probe.close()


async def test_concurrent_migrations_serialize_rather_than_racing(pg_dsn: str) -> None:
    """Two migrators against a virgin schema: one applies, neither crashes.

    This is the scenario a container platform produces by itself -- two
    replicas, or a retried deploy job. Unlocked, the loser died on
    DuplicateTableError from a bare CREATE TABLE.
    """
    schema = "lock_race_test"
    try:
        results = await asyncio.gather(
            migrate_mod.apply_pending_locked(pg_dsn, schema=schema, lock_timeout=60.0),
            migrate_mod.apply_pending_locked(pg_dsn, schema=schema, lock_timeout=60.0),
            return_exceptions=True,
        )
        for r in results:
            assert not isinstance(r, BaseException), f"a concurrent migrator failed: {r!r}"

        applied_counts = sorted(len(r) for r in results)  # type: ignore[arg-type]  # Why: guarded non-exception above.
        # Exactly one does the work; the other finds nothing pending.
        assert applied_counts[0] == 0, "both migrators applied migrations -- they raced"
        assert applied_counts[1] > 0
    finally:
        cleanup = await asyncpg.connect(pg_dsn)
        try:
            await cleanup.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await cleanup.close()
