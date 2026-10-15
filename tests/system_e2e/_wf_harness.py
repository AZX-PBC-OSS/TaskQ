"""The workflow marches' fleet helpers (T15): spawn, wait, read, close.

The vanilla harness's shape (``_harness.py`` — spawn/readiness/signal/
reap) with the WORKFLOW capability: every pod boots the
``tests.system_e2e._wf_entry`` entry, whose app import makes the boot
stamp ``workflow_execution: true`` and project the flows' (actor, queue)
cohorts — the dispatch fence then hands the pod workflow rows.

The readers return the ROWS (the ledger is truth, everything else is a
cache): a flow's status is derived, never trusted from one column; the
invariants close every scenario (``_invariants.assert_balanced``) and
the join's exactly-once pin reads the ``wf_join_fire`` table directly.

Every wait bound is DERIVED (the harness's stated contract) and printed
next to its MEASURED value — the module-level ``measured`` dict is the
scenario's own record, and the tests assert against
``derived + margin`` shapes, never bare constants.
"""

# ruff: noqa: S608  # Why: every query's schema identifier comes from the settings boundary the caller validated; every value is $-bound.

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from typing import Any

import asyncpg
import pytest_asyncio

from tests.system_e2e._harness import (
    SWEEP_INTERVAL_S,
    TIER_LOAD_STRETCH,
    WorkerProc,
    spawn_joined_worker,
)
from tests.system_e2e._wf_app import MARCH_WORKER_QUEUES

#: The workflow-capable entry: subprocesses run THIS module (its app
#: import is the capability stamp).
WF_ENTRY = "tests.system_e2e._wf_entry"

#: The leader lease the march fleets boot with (the rolling-fleet file's
#: measured rationale: a loaded leader iteration pays every sweep's
#: dispatcher budget before it renews — under that bound leadership
#: churns forever). The march bounds derive from this name.
MARCH_LEADER_LEASE_S = 20.0

#: THE MARCH FLEET'S LOCK LEASE (the heartbeat budget's co-invariant):
#: the deploy-matrix's migration cell prices a 3s ACCESS-EXCLUSIVE
#: window, so the fleet's heartbeat failure budget (10 failures ≈ 10.5s
#: of tolerance — the ladder heals THROUGH the window instead of the
#: fail-fast isolation eating the pod) demands a lease that covers its
#: own worst coherent failed-beat cascade (the settings validator's
#: floor: 0.5 + 11 x 1.0 = 11.5s). The march bounds derive from THIS
#: lease — never the vanilla harness's 8s.
MARCH_LOCK_LEASE_S = 15.0

#: A march's settle bound: the flow's OWN body times (the marches' bodies
#: are sub-second each) x the co-tenancy stretch, plus one leader lease
#: (the dispatch cadence the fleet's leader owns) plus the sweep interval
#: — the same derivation shape the rolling-fleet file's bounds carry.
MARCH_SETTLE_BOUND_S = (
    30.0 + MARCH_LEADER_LEASE_S + SWEEP_INTERVAL_S + MARCH_LOCK_LEASE_S
) * TIER_LOAD_STRETCH

#: The reclaim cell's bound: the lapsed lease's re-pend (THE MARCH lease +
#: the sweep) + the new pod's claim (one poll) + the body's OWN re-run (the
#: ``reclaim_target`` long node sleeps 30s — the re-claimed attempt pays
#: it again) — all stretched.
RECLAIM_BODY_S = 30.0  # the long node's sleep (the killed attempt's remainder)
RECLAIM_BOUND_S = (MARCH_LOCK_LEASE_S + SWEEP_INTERVAL_S + 5.0 + RECLAIM_BODY_S) * TIER_LOAD_STRETCH


async def spawn_wf_fleet(
    conn: asyncpg.Connection,
    pg_dsn: str,
    schema: str,
    names: list[str],
    *,
    extra_env: dict[str, str] | None = None,
    log_sink: str | None = None,
) -> dict[str, WorkerProc]:
    """Boot the named pods to the JOINED-fleet standard, workflow-capable."""
    env = {
        "TASKQ_LEADER_LEASE": str(MARCH_LEADER_LEASE_S),
        "TASKQ_LOCK_LEASE": str(MARCH_LOCK_LEASE_S),
        # THE HEARTBEAT BUDGET SIZES TO THE DEPLOY WINDOW (the operator's
        # own law, docs/guides/deployment.md): the fail-fast isolation
        # (3 consecutive failures ≈ 6s of unavailability) is the RIGHT
        # default for a dead database and the WRONG one for a deploy's
        # ACCESS-EXCLUSIVE migration window — an operator deploying into
        # a live fleet raises the budget above the window so the ladder
        # heals through it (the matrix's migration cell prices a 3s
        # hold; 10 failures ≈ 10.5s of tolerance clears it with margin).
        "TASKQ_MAX_HEARTBEAT_FAILURES": "10",
        # BOTH queues: the vanilla actors' system_e2e + the flows'
        # framework-default (the join nodes ride the default queue).
        "TASKQ_QUEUES": MARCH_WORKER_QUEUES,
    }
    if extra_env:
        env.update(extra_env)
    fleet: dict[str, WorkerProc] = {}
    for name in names:
        fleet[name] = await spawn_joined_worker(
            conn,
            pg_dsn,
            schema,
            tag=f"wf-{name}",
            extra_env=env,
            entry=WF_ENTRY,
            log_sink=f"{log_sink}-{name}" if log_sink else None,
        )
    return fleet


async def flow_root(conn: asyncpg.Connection, schema: str, flow_id: str) -> dict[str, Any]:
    """The flow root's row (the derived status is the sweep's verdict)."""
    row = await conn.fetchrow(
        f'SELECT id, status::text AS status, metadata FROM "{schema}".jobs WHERE id = $1',
        flow_id,
    )
    assert row is not None, f"flow root {flow_id} vanished"
    return dict(row)


async def flow_nodes(conn: asyncpg.Connection, schema: str, flow_id: str) -> list[dict[str, Any]]:
    """Every node row of the run (the graph is rows)."""
    rows = await conn.fetch(
        f"""SELECT id::text, status::text AS status, step_key, map_index, attempt,
                   deps_pending, metadata, error_class, code_version
            FROM "{schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
            ORDER BY id""",
        flow_id,
    )
    return [dict(r) for r in rows]


async def wait_flow_terminal(
    conn: asyncpg.Connection, schema: str, flow_id: str, *, bound_s: float = MARCH_SETTLE_BOUND_S
) -> tuple[str, float]:
    """Wait for the run's terminal verdict, DERIVED from the rows.

    Returns ``(status, measured_s)`` — the measured value printed next to
    the bound is the harness's own contract; the assertion lives in the
    caller (``measured <= bound``), so a failure names both. The verdict
    is the rows-only reconstruction (the §17.5 derivation's shipped read),
    never one column's cache.
    """
    from taskq.backend._protocol import JobId
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows._status import reconstruct_workflow_status

    wsql = WorkflowSql.build(schema)
    start = time.monotonic()
    while time.monotonic() - start < bound_s:
        status = await reconstruct_workflow_status(conn, wsql, JobId(flow_id))
        if status in ("complete", "failed", "cancelled"):
            return (status, time.monotonic() - start)
        await asyncio.sleep(0.25)
    raise TimeoutError(
        f"flow {flow_id} did not terminalize within {bound_s}s (the march settle bound)"
    )


async def tag_run_rows(pool: asyncpg.Pool, schema: str, flow_id: str, tag: str) -> None:
    """Stamp *tag* on every row of the run NOW (the invariants'
    population filter). Called immediately before each
    ``assert_balanced``: rows born since the last stamp (map forks,
    emitted chains) join the population — the tag is a census, not a
    birthright."""
    async with pool.acquire() as conn:
        await conn.execute(
            f"""UPDATE "{schema}".jobs
                SET tags = tags || ARRAY[$2::text]
                WHERE (metadata->>'flow_id')::uuid = $1::uuid
                  AND NOT tags @> ARRAY[$2::text]""",
            flow_id,
            tag,
        )


async def join_fires(conn: asyncpg.Connection, schema: str, flow_id: str) -> list[dict[str, Any]]:
    """The join-fire ledger's rows for the run (exactly-once's table)."""
    rows = await conn.fetch(
        f"""SELECT jf.step_key, count(*)::int AS fires
            FROM "{schema}".wf_join_fire jf
            WHERE jf.flow_id = $1::uuid
            GROUP BY jf.step_key""",
        flow_id,
    )
    return [dict(r) for r in rows]


async def hold_rows(conn: asyncpg.Connection, schema: str, run_id: str) -> list[dict[str, Any]]:
    """The run's signal rows (the holds' truth)."""
    rows = await conn.fetch(
        f"""SELECT id::text, signal_name, status::text AS status, hold_epoch, node_key
            FROM "{schema}".wf_signals
            WHERE workflow_id = $1::uuid
            ORDER BY id""",
        run_id,
    )
    return [dict(r) for r in rows]


async def audit_rows(conn: asyncpg.Connection, schema: str, target_id: str) -> list[dict[str, Any]]:
    """The admin-audit rows addressed at one target (the operator's trail)."""
    rows = await conn.fetch(
        f"""SELECT action, principal_subject, target_type, target_id
            FROM "{schema}".admin_audit
            WHERE target_id = $1
            ORDER BY id""",
        target_id,
    )
    return [dict(r) for r in rows]


@pytest_asyncio.fixture
async def wf_pool(pg_dsn: str, module_pg_schema: Any) -> AsyncIterator[asyncpg.Pool]:
    """A plain pool on the module's DSN: the march process's own client."""
    pool = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=4)
    yield pool
    await pool.close()
