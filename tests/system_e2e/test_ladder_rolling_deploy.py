"""Lifecycle 5: a rolling deploy x the cancel ladder's full-poll walk.

Two generations overlap (the deploy's live window) while the operator's
cancel requests arm on rows both generations hold, and one generation
exits while the fleet's ladders are live - with rows held mid-grace, rows
fenced out of their outcome writes (the unheld class the walk owns), and
the second generation's ladder still between phases.

The composition layers the rolling-deploy
shape (generation A drains while generation B serves) over the
ladder's NEW seams - the full-poll walk (held AND unheld rows) and its
controller-side sighting map.

System invariants: every flagged row ends terminal, owned by exactly one
writer - the holder's ladder (the abandon's 'abandoned'), the exiting
generation's shutdown honouring the operator's request ('cancelled'), or
the next leader's reclaim sweep cancel branch ('cancelled' - the only
owner a row whose holder died mid-ladder has left); the cancel audit
columns survive the honouring; the conservation counter balances across
BOTH tables; and the composition actually fired (the walk's abandon
landed, work crossed the deploy boundary, the archive moved).
"""

# ruff: noqa: S608  # Why: schema is a fixture identifier validated at settings load; every value is $-bound.

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import timedelta
from typing import TYPE_CHECKING

import asyncpg
import pytest

from taskq.backend._sweeps import sweep_expired_locks
from taskq.backend.statemachine import TERMINAL_STATUSES
from taskq.worker._leader_shared import prune_terminal_jobs
from tests.system_e2e._harness import (
    WorkerProc,
    graceful_stop,
    reap,
    spawn_worker,
    wait_worker_ready,
)
from tests.system_e2e._invariants import (
    assert_balanced,
    assert_effects_balance,
    delete_tagged,
)
from tests.system_e2e.actors import FlakyPayload, SysPayload, sys_fast, sys_flaky, sys_slow

if TYPE_CHECKING:
    from taskq import TaskQ
    from taskq.testing.fixtures import ModulePgSchema

pytestmark = [pytest.mark.integration, pytest.mark.system]

_TAG = "sys-s5"


async def _arm_cancels(conn: asyncpg.Connection, schema: str, reason: str) -> int:
    """Arm the operator's request on everything active, via the client's
    own write path (the request-carrying writer: phase 1 + requested_at)."""
    rows = await conn.fetch(
        f'SELECT id FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] '
        "AND status IN ('running', 'pending', 'scheduled')",
        _TAG,
    )
    armed = 0
    for row in rows:
        with contextlib.suppress(Exception):
            await conn.execute(
                f'UPDATE "{schema}".jobs SET cancel_phase = 1, '
                "cancel_requested_at = clock_timestamp() WHERE id = $1 AND "
                "cancel_requested_at IS NULL",
                row["id"],
            )
            armed += 1
    _ = reason
    return armed


@pytest.mark.timeout(300)
async def test_rolling_deploy_mid_ladder_exit_conserves_every_flagged_row(
    pg_dsn: str,
    module_pg_schema: ModulePgSchema,
    sys_client: TaskQ,
    sys_ledger: asyncpg.Connection,
) -> None:
    schema = module_pg_schema.schema_name
    conn = sys_ledger
    worker_a: WorkerProc | None = None
    worker_b: WorkerProc | None = None
    try:
        # Generation A boots and fills its slots: slow bodies (the ladder
        # walks them held: the abandon owns them) and flaky bodies (the
        # retry write a flag fences out: the walk owns them unheld).
        worker_a = spawn_worker(pg_dsn, schema, tag="s5-a")
        wait_worker_ready(worker_a)

        for _ in range(2):
            await sys_client.enqueue(sys_slow, SysPayload(sleep=6.0), tags=[_TAG])
        for _ in range(3):
            await sys_client.enqueue(sys_flaky, FlakyPayload(fail_until_attempt=2), tags=[_TAG])
        for _ in range(2):
            await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG])

        deadline = time.monotonic() + 30.0
        running = 0
        while time.monotonic() < deadline:
            running = await conn.fetchval(
                f'SELECT count(*)::int FROM "{schema}".jobs '
                "WHERE tags @> ARRAY[$1::text] AND status = 'running'",
                _TAG,
            )
            if running >= 2:
                break
            await asyncio.sleep(0.05)
        assert running >= 2, f"generation A never filled its slots: {running}"

        # The operator arms cancels on EVERYTHING the fleet holds: the
        # slow (held - the ladder's abandon owns them), the flaky (their
        # next retry write fences out - the walk owns them unheld), the
        # fast (whichever way their writes land). A's walk runs its
        # graces here: escalate at +1s, abandon at +2s, per row.
        armed = await _arm_cancels(conn, schema, "deploy-window")
        assert armed >= 4, f"the arming never covered the fleet: {armed}"
        await asyncio.sleep(5.0)

        # The walk's signature must already be on the ledger: an
        # 'abandoned' row whose error_class is the abandon's own origin.
        abandoned_early = await conn.fetchval(
            f'SELECT count(*)::int FROM "{schema}".jobs WHERE tags @> ARRAY[$1::text] '
            "AND status = 'abandoned' AND error_class = 'CancelAbandoned'",
            _TAG,
        )
        assert abandoned_early >= 1, (
            "generation A's ladder never landed an abandon: the walk never fired"
        )

        # Generation B boots into the live window: the deploy's overlap.
        worker_b = spawn_worker(pg_dsn, schema, tag="s5-b")
        wait_worker_ready(worker_b)

        # B serves through the boundary: new work the deploy admits,
        # flagged the same way, walked by B's ladder (held and unheld).
        for _ in range(2):
            await sys_client.enqueue(sys_fast, SysPayload(), tags=[_TAG])
        await asyncio.sleep(1.0)  # B claims them; its ladder arms mid-grace
        await _arm_cancels(conn, schema, "deploy-window-b")

        # A exits while the FLEET's ladders are live (B's rows are
        # between phases; A's own are terminal or in A's shutdown's
        # hands). Whatever A's shutdown honours or leaves, B and the
        # sweeps own the survivors.
        rc = graceful_stop(worker_a, timeout=60.0)
        worker_a = None  # exited; nothing left to reap
        _ = rc  # A's exit shape is the drain's business, not this pin's

        # The settle: B's fleet must carry every row to terminal. The
        # sweeps here are the settle's own backstop (the leader loop's
        # shape, driven by hand so a leader-less window cannot stall the
        # pin), never the assertion's subject.
        settle_by = time.monotonic() + 90.0
        while time.monotonic() < settle_by:
            await sweep_expired_locks(
                conn, timedelta(seconds=1), timedelta(seconds=1), schema=schema
            )
            pending = await conn.fetchval(
                f'SELECT count(*)::int FROM "{schema}".jobs '
                "WHERE tags @> ARRAY[$1::text] AND status NOT IN "
                "('succeeded', 'failed', 'crashed', 'cancelled', 'abandoned')",
                _TAG,
            )
            if pending == 0:
                break
            await asyncio.sleep(0.5)

        counts = await assert_balanced(conn, schema, _TAG)
        _ = counts

        # Audit truthfulness of the honoured requests: every cancel-family
        # terminal row carries the operator's columns or an origin marker,
        # and the walk's abandon kept its own signature somewhere in the
        # population (the arming half ran at least twice; the honouring
        # half must show it).
        flagged = await conn.fetch(
            "SELECT status::text AS status, error_class AS error_class, "
            "cancel_requested_at AS requested_at FROM ("
            f'SELECT status, error_class, cancel_requested_at FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status IN ('cancelled', 'abandoned') "
            "UNION ALL "
            f'SELECT status, error_class, cancel_requested_at FROM "{schema}".jobs_archive '
            "WHERE tags @> ARRAY[$1::text] AND status IN ('cancelled', 'abandoned')"
            ") c",
            _TAG,
        )
        assert len(flagged) >= 4, (
            f"too few cancel-family terminals for the two arming waves: {len(flagged)}"
        )
        unmarked = [
            dict(r)
            for r in flagged
            if r["requested_at"] is None
            and r["error_class"] not in ("CancelledBeforeStart", "CancelledCooperatively")
        ]
        assert not unmarked, (
            f"{len(unmarked)} cancel-family rows carry neither the operator's "
            f"request columns nor an origin marker: {unmarked}"
        )
        abandoned = await conn.fetchval(
            f'SELECT count(*)::int FROM (SELECT 1 FROM "{schema}".jobs '
            "WHERE tags @> ARRAY[$1::text] AND status = 'abandoned' "
            'UNION ALL SELECT 1 FROM "' + schema + '".jobs_archive '
            "WHERE tags @> ARRAY[$1::text] AND status = 'abandoned') a",
            _TAG,
        )
        assert abandoned >= 1, "the ladder's abandon did not survive the deploy"

        # The deploy moved work across the boundary (B admitted and
        # finished the post-overlap enqueues), and the effects ledger
        # reconciles across both generations.
        await prune_terminal_jobs(
            conn,
            retention_per_status={
                "succeeded": timedelta(0),
                **{s: timedelta(days=3650) for s in TERMINAL_STATUSES if s != "succeeded"},
            },
            archive_retention=timedelta(days=3650),
            batch_size=10,
            schema=schema,
        )
        await assert_effects_balance(conn, schema, _TAG)
    finally:
        if worker_a is not None:
            reap(worker_a)
        if worker_b is not None:
            reap(worker_b)
        await delete_tagged(conn, schema, _TAG)
