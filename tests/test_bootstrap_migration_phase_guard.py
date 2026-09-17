"""Boot-guard regression pins for the phased-rollout currency rule.

``_refuse_boot_on_pending_migrations`` (``src/taskq/worker/_bootstrap.py``)
treats only ``pre``-phase migrations as a currency verdict. Every
discovered migration is keyed ``f"{version}:{phase}"``
(``taskq.migrate.Migration.key``), and the documented phased-rollout
procedure that
``01.00.09_01_post_drop_actor_fairness_dispatch_index.sql``'s own header
instructs operators to follow leaves the fleet in a deliberate middle
state: apply the ``pre`` migration, roll the new worker release out
across the fleet, and only once every worker is confirmed on the new
release, apply the ``post`` migration. Counting a pending ``post``-phase
migration as "schema behind code" would refuse boot for exactly the state
the procedure requires, deadlocking every rolling deploy; counting a
pending ``pre``-phase one as bootable would let a worker start against a
schema missing structures the code already reads.

``taskq.migrate.apply_pending`` models the same distinction (a ``phase``
filter, and a refusal to apply ``post`` before its ``pre`` counterpart --
see ``migrate.py``'s pre/post ordering guard).

The first test applies pending migrations restricted to ``phase="pre"`` --
precisely the state the ``01.00.09_01`` header instructs operators to
leave the fleet in during the rollout window -- and asserts boot succeeds.
The second leaves a ``pre``-phase migration pending and asserts the guard
still refuses boot.
"""

import re

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.migrate import apply_pending, discover
from taskq.settings import WorkerSettings
from taskq.worker._bootstrap import WorkerDeps, _refuse_boot_on_pending_migrations

pytestmark = pytest.mark.integration

_SCHEMA_LABEL = f"tbmpg_{new_base62()}".lower()


def _settings_for(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict({"pg_dsn": pg_dsn, "schema_name": schema})


async def test_post_phase_migration_pending_matches_documented_rollout_state(
    pg_dsn: str,
) -> None:
    """Sanity check: the fixture used below is faithful to the real repo state.

    01.00.09_01 must actually exist as a pre/post pair, or the scenario
    this test exercises is not the one the migration file's header
    documents.
    """
    keys = {m.key for m in discover()}
    assert "01.00.09_01:pre" in keys
    assert "01.00.09_01:post" in keys


async def test_worker_boots_during_documented_pre_applied_post_pending_window(
    pg_dsn: str,
) -> None:
    """Boot succeeds while only the pre-phase migration has been applied --
    exactly the state the 01.00.09_01 post-migration header instructs
    operators to leave the fleet in during a phased rollout.

    Per the documented pre/roll/post procedure, a worker on the new
    release running against a schema with the pre-phase migration applied
    and the post-phase migration intentionally not yet applied is not a
    deployment mistake, so the guard must not raise.
    """
    schema = _SCHEMA_LABEL

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()

    conn = await asyncpg.connect(pg_dsn)
    try:
        # This is the exact documented step 1 of the rollout: apply only
        # the pre-phase migrations (`taskq migrate up --phase pre`).
        # 01.00.09_01:post is deliberately left unapplied -- the header
        # of that file instructs operators not to apply it until every
        # worker in the fleet is confirmed on the release that shipped
        # the pre-phase migration.
        await apply_pending(conn, schema=schema, phase="pre")
    finally:
        await conn.close()

    settings = _settings_for(pg_dsn, schema)
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        deps = WorkerDeps(  # type: ignore[call-arg]
            settings=settings,
            dispatcher_pool=pool,
            heartbeat_pool=pool,
            worker_pool=pool,
            notify_conn=None,
            leader_conn=None,
        )
        try:
            # This is step 2 of the documented rollout: start the new
            # worker release fleet-wide, BEFORE the post migration is
            # applied. Per the documented procedure this boot step must
            # succeed.
            await _refuse_boot_on_pending_migrations(deps, settings)
        except RuntimeError as exc:
            pytest.fail(
                "worker boot refused during the documented pre-applied/"
                f"post-pending rollout window: {exc}"
            )
    finally:
        await pool.close()
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await conn.close()


async def test_worker_refuses_boot_while_a_pre_phase_migration_is_still_pending(
    pg_dsn: str,
) -> None:
    """The fix for the post-phase false positive must not weaken the guard
    for its real purpose: a schema still missing a pre-phase migration is
    an actual deployment-ordering mistake, and boot must still refuse.

    This applies every migration up to and including 01.00.08_01 (leaving
    01.00.09_01's pre phase, and everything after it, unapplied) and
    asserts the guard still raises -- the phase filter that lets a
    pending post-phase migration through must not also let a pending
    pre-phase migration through.
    """
    schema = f"{_SCHEMA_LABEL}_pre"

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()

    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema, target="01.00.08_01")
    finally:
        await conn.close()

    settings = _settings_for(pg_dsn, schema)
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=2)
    try:
        deps = WorkerDeps(  # type: ignore[call-arg]
            settings=settings,
            dispatcher_pool=pool,
            heartbeat_pool=pool,
            worker_pool=pool,
            notify_conn=None,
            leader_conn=None,
        )
        with pytest.raises(RuntimeError, match=re.escape("01.00.09_01:pre")):
            await _refuse_boot_on_pending_migrations(deps, settings)
    finally:
        await pool.close()
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        finally:
            await conn.close()
