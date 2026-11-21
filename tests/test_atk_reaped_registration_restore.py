# ruff: noqa: S608  # Why: every interpolated identifier is this module's own generated schema name, never operator input; all values are $-bound.
"""The election seam's repaired registration is the row boot would have written.

The reaped-row repair runs from two seams (``heartbeat_loop``'s zero-row
liveness write and ``MaintenanceLeader._election_loop``'s FK recovery).
Both restore through one shared builder, and the harm a divergent payload
does is silent: a restored row is a *fresh* row, so nothing the reaped row
held survives it. The dispatch claim's execution fence reads
``(w.metadata->>'workflow_execution')::boolean`` straight off that row
(``taskq.backend._dispatch_sql``'s ``wf_exec_capable`` CTE, whose
unregistered edge COALESCEs to ``false``). A restore that stamped the
capability away would therefore not fail anything - the worker would simply
never be handed another workflow row, and its flow rows would park
fail-closed forever, under a green heartbeat.

The heartbeat seam is pinned against the real database in
``tests/test_heartbeat_integration.py``; the election seam is pinned only
against doubles, and those doubles answer the repair with a command tag
without ever reading what was bound. This test runs the election alone -
which is the incident's own ordering, the survivor's heartbeat stalled while
its campaign fires - and asserts the row Postgres actually ended up with:
the capability, the capacity/notify keys, and the identity columns boot
stamps. A bare-identity repair passes every "the leader recovered" check and
fails here.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import socket
from datetime import timedelta
from uuid import UUID

import asyncpg
import pytest
import structlog

from taskq._ids import new_base62, new_uuid
from taskq.backend._records import jsonb_param
from taskq.backend._sql import build_heartbeat_sql
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.assertions import wait_for_condition
from taskq.testing.pg import create_worker
from taskq.worker._leader_shared import cleanup_stale_workers
from taskq.worker._registration import reregister_worker
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.leader import MaintenanceLeader
from taskq.workflows._capability import WORKER_BOOT_WORKFLOW_SCHEMA

pytestmark = pytest.mark.integration

_HEARTBEAT_INTERVAL_S = 0.5
_MAX_HEARTBEAT_FAILURES = 3
#: The tier's own derivation: 0.5 * (3 + 3) = 3 s.
_GRACE = timedelta(seconds=_HEARTBEAT_INTERVAL_S * (_MAX_HEARTBEAT_FAILURES + 3))


def _settings(pg_dsn: str, schema: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema.lower(),
            "TASKQ_HEARTBEAT_INTERVAL": str(_HEARTBEAT_INTERVAL_S),
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.5",
            "TASKQ_MAX_HEARTBEAT_FAILURES": str(_MAX_HEARTBEAT_FAILURES),
            "TASKQ_LOCK_LEASE": "8.0",
            "TASKQ_QUEUES": "default,flows",
            "TASKQ_WORKER_LABEL": "labelled-by-the-supervisor",
            # The workgroup correlation is a UUID the supervisor stamps; the
            # restore must carry it, or the worker vanishes from its
            # workgroup's health view while remaining fully operational.
            "TASKQ_WORKGROUP_INSTANCE": "018f6a3c-1d2e-7c4a-9b5d-0e1f2a3b4c5d",
            "TASKQ_WATCHDOG_ENABLED": "false",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
        }
    )


async def _reap(dsn_direct: str, schema: str, worker_id: UUID, reaper_id: UUID) -> int:
    """Run the production stale sweep against the aged worker row.

    One transaction, so the row cannot be refreshed between the UPDATE that
    makes it stale and the DELETE that reaps it - the race the sweep runs in
    production, made deterministic.
    """
    conn = await asyncpg.connect(dsn_direct)
    try:
        async with conn.transaction():
            await conn.execute(
                f'UPDATE "{schema}".workers '  # Why: generated identifier, never input; values are bound.
                "SET last_seen_at = clock_timestamp() - $1::interval WHERE id = $2",
                _GRACE + timedelta(seconds=1),
                worker_id,
            )
            return await cleanup_stale_workers(
                conn, worker_id=reaper_id, staleness=_GRACE, schema=schema
            )
    finally:
        await conn.close()


async def test_the_election_seam_restores_the_full_boot_registration_payload(
    pg_dsn: str,
) -> None:
    schema = f"test_atk_restore_{new_base62()}".lower()
    settings = _settings(pg_dsn, schema)

    admin = await asyncpg.connect(pg_dsn)
    try:
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(admin, schema=schema)
    finally:
        await admin.close()

    assert settings.pg_dsn is not None
    assert settings.pg_dsn_direct is not None
    stack = contextlib.AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    obs_conn = await asyncpg.connect(settings.pg_dsn_direct)
    stack.push_async_callback(obs_conn.close)
    shutdown = asyncio.Event()
    election: asyncio.Task[None] | None = None
    try:
        backend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=timedelta(0),
            cleanup_grace_period=timedelta(0),
        )
        worker_id, reaper_id = new_uuid(), new_uuid()
        async with deps.dispatcher_pool.acquire() as conn:
            await create_worker(conn, schema, worker_id)
        await create_worker(obs_conn, schema, reaper_id)

        # The boot's assignment (_bootstrap._main, the only production writer
        # of this field) supplies the capability the restore must carry; a
        # capable boot is what makes a downgraded repair visible.
        deps.workflow_execution_capable = True

        removed = await _reap(str(settings.pg_dsn_direct), schema, worker_id, reaper_id)
        assert removed == 1, (
            "the trigger did not reproduce: the stale sweep removed "
            f"{removed} rows, expected the one reaped worker"
        )
        still_there = await obs_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".workers WHERE id = $1',
            worker_id,
        )
        assert int(still_there) == 0, (
            "the row survived the reap, so nothing below would prove the election seam restored it"
        )

        # No heartbeat loop at all: the election is the only writer that can
        # bring the row back, exactly the ordering the incident produced when
        # the surviving worker's ticks had stalled past the grace.
        leader = MaintenanceLeader(deps, worker_id, backend, clock=SystemClock())
        with structlog.testing.capture_logs() as captured:
            election = asyncio.create_task(leader.run(shutdown), name="leader-atk-restore")
            await wait_for_condition(
                lambda: deps.is_leader.is_set() or shutdown.is_set(),
                description="the reaped worker's campaign to recover its row and win",
                timeout=30.0,
            )
            died = shutdown.is_set() and not deps.is_leader.is_set()
            kinds = sorted(
                {
                    str(event.get("kind"))
                    for event in captured
                    if event.get("kind") not in (None, "election_wake_ignored_stopping")
                }
            )
        assert not died, (
            "the election could not recover the reaped registration and stood "
            "the process down instead: the FK arm's restore did not leave a row "
            f"the retried elect could reference. events={kinds}"
        )

        row = await obs_conn.fetchrow(
            f"SELECT hostname, pid, queues, worker_label, workgroup_instance, "
            f'last_seen_at, metadata FROM "{schema}".workers WHERE id = $1',
            worker_id,
        )
        assert row is not None, (
            "the worker won the election but its workers row is still missing; "
            "the maintenance_leader row cannot legitimately reference it"
        )

        metadata: dict[str, object] = json.loads(row["metadata"])
        assert metadata.get("workflow_execution") is True, (
            "the election seam restored the reaped worker as vanilla: the "
            "restored row does not carry the capability the boot stamped, so "
            f"the claim fence parks every workflow row fail-closed. metadata={metadata}"
        )
        assert metadata.get("workflow_execution_schema") == WORKER_BOOT_WORKFLOW_SCHEMA, (
            "the restored row's schema level does not match the level this "
            f"build implements ({WORKER_BOOT_WORKFLOW_SCHEMA}); metadata={metadata}"
        )
        assert metadata.get("max_concurrency") == settings.max_concurrency, (
            f"the restored row lost the capacity boot stamped: {metadata}"
        )
        assert metadata.get("notify_enabled") == getattr(settings, "notify_enabled", False)

        # The identity columns are what a workgroup supervisor and the admin
        # surface correlate on; a restore that bound a narrower parameter list
        # leaves the row unattributable.
        assert list(row["queues"]) == list(settings.queues), (
            f"the restored row's queues are {row['queues']!r}, boot's are {settings.queues!r}"
        )
        assert row["worker_label"] == settings.worker_label
        assert str(row["workgroup_instance"]) == settings.workgroup_instance
        assert row["hostname"] == socket.gethostname()
        assert row["pid"] == os.getpid(), (
            "the restored row's pid is not this process's: the repair did "
            f"not re-stamp the identity boot wrote ({row['pid']!r})"
        )

        # The dispatch claim's own read, not a key existence probe: the value
        # the fence sees for this worker after the repair.
        capable = await obs_conn.fetchval(
            f"SELECT COALESCE((SELECT (w.metadata->>'workflow_execution')::boolean "
            f'FROM "{schema}".workers w WHERE w.id = $1), false)',
            worker_id,
        )
        assert capable is True, (
            "the repaired row is not admitted by the dispatch fence's "
            "capability read, so a restored workflow-capable worker would "
            "never be handed another flow row"
        )

        restored_events = [
            event for event in captured if event.get("kind") == "worker_registration_restored"
        ]
        assert len(restored_events) == 1, (
            "expected exactly one worker-registration-restored warning for the "
            f"election seam, got {restored_events!r}"
        )
        assert restored_events[0]["log_level"] == "warning"
        assert restored_events[0]["source"] == "leader_election"

        # The restored row is born fresh: the same sweep that reaped it must
        # not take it straight back out (a repair that stamped a stale
        # last_seen_at would loop the reap/restore churn under the operator's
        # rate alert).
        again = await cleanup_stale_workers(
            obs_conn, worker_id=reaper_id, staleness=_GRACE, schema=schema
        )
        assert again == 0, (
            f"the sweep re-reaped {again} just-restored row(s): the repair was born stale"
        )
    finally:
        shutdown.set()
        if election is not None:
            with contextlib.suppress(asyncio.CancelledError, ExceptionGroup):
                await election
        await stack.aclose()


async def test_the_two_repair_seams_collide_on_one_missing_row_without_clobbering(
    pg_dsn: str,
) -> None:
    """Both repair writers can reach the same missing row at once; the loser
    must no-op, not raise and not overwrite the winner.

    Hypothesis attacked: ``WORKER_REREGISTRATION_SQL_TEMPLATE``'s ``ON
    CONFLICT (id) DO NOTHING`` is the only thing that makes two independent
    repairs of one worker id safe. The heartbeat's repair runs inside the
    tick's transaction (uncommitted until the commit), while the election
    seam's repair runs autocommit on ``leader_conn`` - so the second writer's
    INSERT genuinely blocks on the first one's uncommitted unique-index entry
    and only then resolves. Under a plain INSERT that collision is an
    ``asyncpg.UniqueViolationError`` (a non-transient error that fail-closes
    the election arm); under ``DO UPDATE`` the second - narrower, tick-less -
    payload clobbers the winner's row. Both are silent in the doubles, which
    answer a canned tag without ever reaching Postgres.

    Reachable case: a worker whose row the stale sweep reaped while its
    heartbeat was stalled. Its next tick restores inside the tick's
    transaction at the same moment its campaign recovers from the
    ``maintenance_leader`` FK violation - one process owns both loops, and
    the fleet-liveness incident is exactly the window where both fire.
    """
    schema = f"test_atk_collide_{new_base62()}".lower()
    settings = _settings(pg_dsn, schema)

    admin = await asyncpg.connect(pg_dsn)
    try:
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(admin, schema=schema)
    finally:
        await admin.close()

    worker_id = new_uuid()
    tick_payload: dict[str, object] = {"loop_stalls": {"send_email": {"gil_held": 2}}}
    # Two writers, two connections: the heartbeat seam's transaction and the
    # election seam's autocommit path.
    heartbeat_conn = await asyncpg.connect(pg_dsn)
    election_conn = await asyncpg.connect(pg_dsn)
    second: asyncio.Task[str] | None = None
    try:
        row_before = await heartbeat_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".workers WHERE id = $1', worker_id
        )
        assert int(row_before) == 0, "trigger precondition: the row must be missing"

        tx = heartbeat_conn.transaction()
        await tx.start()
        first_tag = await reregister_worker(
            heartbeat_conn,
            worker_id,
            settings,
            tick_metadata=tick_payload,
        )
        assert first_tag == "INSERT 0 1", (
            f"the heartbeat seam's repair did not insert the missing row: got {first_tag!r}"
        )

        election_pid = await election_conn.fetchval("SELECT pg_backend_pid()")
        second = asyncio.create_task(
            reregister_worker(election_conn, worker_id, settings),
            name="election-seam-repair",
        )

        async def blocked_or_finished() -> bool:
            """Server-side fact, not a sleep: the second INSERT is waiting on a
            lock, or it already resolved (which means it never contended)."""
            if second.done():
                return True
            # Pid plus datname scope: the exact pid names one session, and
            # the datname scope is the suite-hygiene gate's rule for every
            # pg_stat_activity read on the shared cluster (see
            # tests/test_suite_hygiene.py's section comment).
            waiting = await heartbeat_conn.fetchval(
                "SELECT wait_event_type FROM pg_stat_activity "
                "WHERE pid = $1 AND datname = current_database()",
                election_pid,
            )
            return str(waiting) == "Lock"

        await wait_for_condition(
            blocked_or_finished,
            description=(
                "the election seam's repair to block on the heartbeat transaction's "
                "uncommitted insert (a server-side lock wait on the winner's row)"
            ),
            timeout=10.0,
        )
        if second.done():
            # It resolved without waiting, so it never contended: surface what
            # it actually got (a plain INSERT's UniqueViolationError here)
            # instead of a harness message that hides the real failure.
            resolved = second.result()
            raise AssertionError(
                "the election seam's repair resolved against a row still "
                "uncommitted on another connection instead of waiting for it: "
                f"it did not contend for the same unique index entry (got {resolved!r})"
            )

        await tx.commit()
        loser_tag = await asyncio.wait_for(second, timeout=10.0)
        assert loser_tag == "INSERT 0 0", (
            "the second repair did not no-op against the winner's row: a "
            "non-zero tag means it inserted (or updated) a row that already "
            f"existed - got {loser_tag!r}"
        )

        row = await heartbeat_conn.fetchrow(
            f'SELECT last_seen_at, metadata FROM "{schema}".workers WHERE id = $1',
            worker_id,
        )
        assert row is not None
        metadata: dict[str, object] = json.loads(row["metadata"])
        assert "loop_stalls" in metadata, (
            "the losing repair clobbered the winner's row: the heartbeat's "
            f"tick payload is gone from the metadata ({metadata})"
        )
        assert metadata.get("workflow_execution") is False, (
            f"the surviving row lost the registration half of the payload: metadata={metadata}"
        )
        count = await heartbeat_conn.fetchval(
            f'SELECT count(*) FROM "{schema}".workers WHERE id = $1', worker_id
        )
        assert int(count) == 1, f"the collision left {count} rows for one worker id"
    finally:
        if second is not None and not second.done():
            second.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await second
        await election_conn.close()
        await heartbeat_conn.close()


async def test_the_tick_after_a_restore_keeps_the_restored_capability(
    pg_dsn: str,
) -> None:
    """The restore's capability keys must survive the *next* heartbeat tick.

    Hypothesis attacked: the repair's metadata survives only if the ordinary
    liveness write merges rather than replaces -
    ``UPDATE ... SET last_seen_at = clock_timestamp(), metadata = metadata || $2``
    (``UPDATE_WORKER_LIVENESS_SQL_TEMPLATE``). If that write ever becomes
    ``metadata = $2``, a capable worker's restored row is silently downgraded
    one tick after the repair: the workflow claim fence reads
    ``metadata->>'workflow_execution'`` as data, so its flow rows park
    fail-closed forever while every health signal is green. No existing test
    exercises the tick boundary: the change's real-DB pin stops at the first
    tick whose repair landed, and the doubles never evaluate the merge.

    Reachable case: exactly the population this change is for - a capable
    worker whose row the stale sweep reaped, whose tick restores it, and
    whose loop keeps ticking afterwards (every tick from then on).
    """
    schema = f"test_atk_tick_merge_{new_base62()}".lower()
    settings = _settings(pg_dsn, schema)

    admin = await asyncpg.connect(pg_dsn)
    try:
        await admin.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(admin, schema=schema)
    finally:
        await admin.close()

    conn = await asyncpg.connect(pg_dsn)
    try:
        worker_id = new_uuid()
        # The repair the heartbeat's zero-row branch runs (capable worker).
        tag = await reregister_worker(
            conn,
            worker_id,
            settings,
            workflow_execution=True,
            tick_metadata={"loop_stalls": {"send_email": {"gil_held": 1}}},
        )
        assert tag == "INSERT 0 1"

        # The very next tick's liveness write, rendered and bound exactly as
        # heartbeat_loop issues it.
        liveness_sql = build_heartbeat_sql(schema)[0]
        await conn.execute(
            liveness_sql,
            worker_id,
            jsonb_param({"loop_stalls": {"send_email": {"gil_held": 2}}}),
        )

        row = await conn.fetchrow(
            f'SELECT metadata FROM "{schema}".workers WHERE id = $1', worker_id
        )
        assert row is not None
        metadata: dict[str, object] = json.loads(row["metadata"])
        assert metadata.get("workflow_execution") is True, (
            "the tick after the repair dropped the restored workflow capability: "
            "the liveness write replaced the metadata instead of merging it, so a "
            f"restored capable worker silently parks every flow row. metadata={metadata}"
        )
        assert metadata.get("workflow_execution_schema") == WORKER_BOOT_WORKFLOW_SCHEMA
        assert metadata.get("max_concurrency") == settings.max_concurrency
        # The merge did happen: this tick's own key won the merge.
        assert metadata.get("loop_stalls") == {"send_email": {"gil_held": 2}}, (
            f"the tick's own metadata did not merge into the restored row: {metadata}"
        )
        capable = await conn.fetchval(
            "SELECT COALESCE((SELECT (w.metadata->>'workflow_execution')::boolean "
            f'FROM "{schema}".workers w WHERE w.id = $1), false)',
            worker_id,
        )
        assert capable is True, (
            "the dispatch fence's capability read is false one tick after the "
            f"repair: metadata={metadata}"
        )
    finally:
        await conn.close()
