# ruff: noqa: S608  # Why: every schema name is a fixed test identifier, values are $-bound.
"""Upgrade-path attack: version-to-version migration chains.

Every production user of an unreleased-intermediate schema walks the same
path this module exercises: a database migrated up to some historical
checkpoint (the last migration of an older main build), then the pending
chain applied through the current tree, then real work on the upgraded
schema. One test per recent checkpoint (the last migration of versions
01.00.14 through 01.00.18), asserting at every step:

* the chain applies cleanly with no ``INVALID`` index debris,
* the ledger stays consistent (every bundled key recorded, checksums
  match the bundled files, ``use_transaction`` truthful),
* rows created BEFORE the upgrade round-trip through it: a legacy
  pending job is claimable post-upgrade (``claim_epoch`` 0 → 1 on the
  first post-migration claim), terminal-writable with the presented
  epoch, fenced against a stale epoch, cancellable, and the terminal
  row the old build left behind archives cleanly (``claim_epoch``
  carried into ``jobs_archive``),
* fresh work (enqueue / dispatch / terminal / cancel) works on the
  upgraded schema through the real backend API.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq._ids import new_base62, new_uuid
from taskq.backend._protocol import JobFilter
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import (
    apply_pending,
    checksum_drifts,
    discover,
    list_applied,
    list_invalid_indexes,
)
from taskq.settings import WorkerSettings
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.pg import create_pending_job, seed_actors
from taskq.testing.settings import make_integration_settings_dict
from taskq.worker._leader_shared import prune_terminal_jobs
from taskq.worker.deps import open_worker_deps

pytestmark = pytest.mark.integration

#: The last migration of each recent historical version. A database that
#: stopped here is exactly "a main build of that era, fully migrated".
CHECKPOINTS: tuple[str, ...] = (
    "01.00.14_01",
    "01.00.15_01",
    "01.00.16_01",
    "01.00.17_01",
    "01.00.18_01",
)


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


@pytest.fixture
async def upgrade_conn(pg_dsn: str) -> AsyncIterator[asyncpg.Connection]:
    conn = await asyncpg.connect(pg_dsn)
    try:
        yield conn
    finally:
        await conn.close()


async def _assert_ledger_consistent(conn: asyncpg.Connection, schema: str) -> None:
    """Every bundled migration recorded, checksums match, flags truthful."""
    all_migrations = discover()
    applied = await list_applied(conn, schema)
    assert applied == {m.key for m in all_migrations}, (
        "ledger does not record exactly the bundled migration set after the upgrade"
    )
    assert await checksum_drifts(conn, schema=schema) == {}, (
        "post-upgrade ledger checksums drift from the bundled files"
    )
    rows = await conn.fetch(
        f'SELECT version, checksum, use_transaction FROM "{schema}".schema_migrations'
    )
    recorded = {r["version"]: r for r in rows}
    for m in all_migrations:
        r = recorded[m.key]
        assert r["checksum"] == m.checksum(schema), f"{m.key}: ledger checksum mismatch"
        assert r["use_transaction"] is m.use_transaction, f"{m.key}: use_transaction mismatch"


async def _backend_smoke(pg_dsn: str, schema: str) -> None:
    """Enqueue → dispatch → succeed, and cancel_where, on the upgraded schema."""
    settings = WorkerSettings.load_from_dict(make_integration_settings_dict(pg_dsn))
    settings.schema_name = schema
    stack = AsyncExitStack()
    deps = await stack.enter_async_context(open_worker_deps(settings))
    try:
        backend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=deps.settings.cancellation_grace_period),
            cleanup_grace_period=timedelta(seconds=deps.settings.cleanup_grace_period),
        )
        row = await backend.enqueue(make_enqueue_args(actor="test_actor", priority=10))
        assert row.status == "pending"
        worker_id = new_uuid()
        dispatched = await backend.dispatch_batch(
            worker_id=worker_id,
            queues=["default"],
            limit=1,
            lock_lease=timedelta(seconds=30),
        )
        assert len(dispatched) == 1
        assert dispatched[0].claim_epoch == 1, (
            "a fresh row's first claim must stamp claim_epoch 1 (pre-migration "
            "rows read 0; no claim can ever stamp 0 again)"
        )
        ok = await backend.mark_succeeded(
            dispatched[0].id,
            worker_id,
            {"ok": True},
            claim_epoch=dispatched[0].claim_epoch,
            attempt=dispatched[0].attempt,
        )
        assert ok is True, "the terminal write fenced on the presented epoch must land"

        # Bulk cancel (the JobFilter surface, renamed predicate included)
        # on a fresh pending row.
        cancel_row = await backend.enqueue(make_enqueue_args(actor="test_actor"))
        result = await backend.cancel_where(
            JobFilter(actor="test_actor", status="pending"), "upgrade-smoke"
        )
        assert result.cancelled_directly >= 1
        listed = await backend.list_jobs(JobFilter(actor="test_actor", status="cancelled"))
        assert any(j.id == cancel_row.id for j in listed)
    finally:
        await stack.aclose()


async def _legacy_rows_round_trip(conn: asyncpg.Connection, pg_dsn: str, schema: str) -> None:
    """Rows the OLD build left behind survive the upgrade functionally.

    The core v2v invariant of 01.00.18_02: a pre-migration row reads
    ``claim_epoch`` 0, which no post-migration claim can ever stamp, so
    the first claim of an upgraded legacy row bumps to 1 and every later
    epoch-bearing fence must accept the presented value and refuse a
    stale one.
    """
    # scheduled_at in the past by a margin: the claim CTE compares against
    # the DATABASE clock, and the two clocks can diverge by fractions of a
    # second (see conftest's clock-divergence probe).
    past = datetime.now(UTC) - timedelta(seconds=10)
    legacy_pending = await create_pending_job(conn, schema, scheduled_at=past)
    legacy_terminal = await create_pending_job(conn, schema, status="succeeded")
    await conn.execute(
        f'UPDATE "{schema}".jobs SET finished_at = $2 WHERE id = $1',
        legacy_terminal,
        datetime.now(UTC) - timedelta(hours=2),
    )

    epoch = await conn.fetchval(
        f'SELECT claim_epoch FROM "{schema}".jobs WHERE id = $1', legacy_pending
    )
    assert epoch == 0, "a pre-upgrade row must read the migration default epoch 0"

    settings = WorkerSettings.load_from_dict(make_integration_settings_dict(pg_dsn))
    settings.schema_name = schema
    stack = AsyncExitStack()
    deps = await stack.enter_async_context(open_worker_deps(settings))
    try:
        backend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(seconds=deps.settings.cancellation_grace_period),
            cleanup_grace_period=timedelta(seconds=deps.settings.cleanup_grace_period),
        )
        worker_id = new_uuid()
        dispatched = await backend.dispatch_batch(
            worker_id=worker_id,
            queues=["default"],
            limit=5,
            lock_lease=timedelta(seconds=30),
        )
        assert legacy_pending in [d.id for d in dispatched], (
            "the legacy pending row must be claimable by a post-upgrade worker"
        )
        mine = next(d for d in dispatched if d.id == legacy_pending)
        assert mine.claim_epoch == 1, "the first post-upgrade claim stamps epoch 1"

        ok = await backend.mark_succeeded(
            mine.id,
            worker_id,
            {"legacy": True},
            claim_epoch=mine.claim_epoch,
            attempt=mine.attempt,
        )
        assert ok is True

        # The stale-epoch write (the value a pre-fence reader would hold:
        # 0, the migration default) must be fenced out.
        staled = await backend.mark_succeeded(
            mine.id,
            worker_id,
            {"stale": True},
            claim_epoch=0,
            attempt=mine.attempt,
        )
        assert staled is False, "epoch 0 is the one epoch no post-migration claim stamps"
    finally:
        await stack.aclose()

    # The terminal row the OLD build left behind archives cleanly: the
    # prune carries claim_epoch 0 into jobs_archive (the column the
    # archive's widened uniqueness also gained).
    pruned = await prune_terminal_jobs(
        conn,
        retention_per_status={"succeeded": timedelta(hours=1)},
        archive_retention=timedelta(days=30),
        schema=schema,
    )
    assert pruned.total_deleted >= 1
    arch_epoch = await conn.fetchval(
        f'SELECT claim_epoch FROM "{schema}".jobs_archive WHERE id = $1', legacy_terminal
    )
    assert arch_epoch == 0, "the archive must carry the legacy row's epoch"


async def _checkpoint_upgrade(pg_dsn: str, conn: asyncpg.Connection, checkpoint: str) -> None:
    schema = f"atk_up_{new_base62()}".lower()
    try:
        applied_to_cp = await apply_pending(conn, schema=schema, target=checkpoint)
        assert applied_to_cp[-1].version == checkpoint
        # The dispatch claim routes through actor_config; seed the actor
        # the smoke enqueues with (the old build's registry rows survive
        # the upgrade untouched - ON CONFLICT DO NOTHING).
        await seed_actors(conn, schema)
        assert await checksum_drifts(conn, schema=schema) == {}
        pre_keys = await list_applied(conn, schema)

        upgraded = await apply_pending(conn, schema=schema)
        assert [m.key for m in upgraded], f"no pending chain above {checkpoint}"
        assert all(m.key not in pre_keys for m in upgraded)

        assert await list_invalid_indexes(conn, schema) == [], (
            f"upgrading past {checkpoint} left INVALID indexes"
        )
        await _assert_ledger_consistent(conn, schema)
        await _legacy_rows_round_trip(conn, pg_dsn, schema)
        await _backend_smoke(pg_dsn, schema)
    finally:
        await _drop_schema(conn, schema)


@pytest.mark.parametrize("checkpoint", CHECKPOINTS)
async def test_upgrade_from_checkpoint_through_current(
    pg_dsn: str, upgrade_conn: asyncpg.Connection, checkpoint: str
) -> None:
    await _checkpoint_upgrade(pg_dsn, upgrade_conn, checkpoint)


async def test_fully_current_schema_has_no_pending(pg_dsn: str) -> None:
    """The newest checkpoint's chain is the current tree: applying again is a no-op."""
    conn = await asyncpg.connect(pg_dsn)
    schema = f"atk_cur_{new_base62()}".lower()
    try:
        applied = await apply_pending(conn, schema=schema)
        assert applied, "a fresh schema must have a pending chain"
        again = await apply_pending(conn, schema=schema)
        assert again == []
        await _assert_ledger_consistent(conn, schema)
    finally:
        await _drop_schema(conn, schema)
        await conn.close()


async def test_pre_edit_ledger_refuses_then_recovers(pg_dsn: str) -> None:
    """A ledger recorded by an OLD main build (whose bundled file was later
    prose-edited) refuses the upgrade fail-closed, and the documented
    remedy recovers it.

    01.00.14_01's comments were edited after its introduction
    (aca69ff6 → the prose pass), so a database migrated from a build of
    that era carries a checksum the current file no longer renders. The
    runner must refuse before applying anything (ChecksumDriftError),
    apply nothing, and --allow_checksum_drift must let the chain finish
    with a consistent end state.
    """
    conn = await asyncpg.connect(pg_dsn)
    schema = f"atk_drift_{new_base62()}".lower()
    try:
        # Build the checkpoint ledger, then corrupt ONE applied checksum
        # to the shape a pre-edit ledger carries (a different sha-256 for
        # the same key: the file's rendered text was edited afterwards).
        await apply_pending(conn, schema=schema, target="01.00.14_01")
        await conn.execute(
            f"UPDATE \"{schema}\".schema_migrations SET checksum = '{'0' * 64}' "
            "WHERE version = '01.00.14_01:pre'"
        )

        from taskq.migrate import ChecksumDriftError

        with pytest.raises(ChecksumDriftError) as excinfo:
            await apply_pending(conn, schema=schema)
        assert any(d.key == "01.00.14_01:pre" for d in excinfo.value.drifts)
        # The refusal applied nothing: the ledger still stops at the checkpoint.
        assert "01.00.15_01:pre" not in await list_applied(conn, schema)

        recovered = await apply_pending(conn, schema=schema, allow_checksum_drift=True)
        assert [m.key for m in recovered]
        # The end state is consistent EXCEPT the documented drift keep:
        # the ledger retains the stored (pre-edit) checksum and the drift
        # stays visible, every OTHER key matches the bundled files.
        drifts = await checksum_drifts(conn, schema=schema)
        assert set(drifts) == {"01.00.14_01:pre"}
        all_migrations = discover()
        applied = await list_applied(conn, schema)
        assert applied == {m.key for m in all_migrations}
        assert await list_invalid_indexes(conn, schema) == []
    finally:
        await _drop_schema(conn, schema)
        await conn.close()
