"""Fixtures + seed helpers for the workflow pin suites, registered from the
root conftest.

These fixtures live here instead of ``tests/workflows/conftest.py`` for the
same reason ``tests/web_admin/_fixtures.py`` does: pytest 9.1.1 drops a
nested conftest's fixtures for a file revisited non-adjacently in the
argument list (pytest-dev/pytest#14971). The workflow pins span four files
(the engine's finalize/sweep/fork families, the schema's structural +
band families, the ledger family), so the seed helpers and the red-log
sink live here ONCE — a fixture duplicated across two files is a seam owed
now, and this is the seam's home.

Every helper drives the REAL engine surfaces (``taskq.workflows``); the
seed shapes are the flow-row/join-row/running-node representations the
engine's own statements read back.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql
from taskq.workflows.engine import render_workflow_sql

MEASUREMENTS = Path(__file__).parent.parent / ".measurements"

#: The terminal-status SQL set — the statement-side literal the engine's
#: guards spell (the twin of statemachine.TERMINAL_STATUSES); the pins'
#: convicted variants re-spell it.
TERMINAL_SQL = "('succeeded','failed','cancelled','crashed','abandoned')"


class RedLog:
    """The red-output sink: a file that gets READ (BUILD-PROTOCOL §2).
    Every pin's convicted variant appends its observed dragon here; the
    fixture flushes on teardown."""

    def __init__(self, filename: str) -> None:
        self._filename = filename
        self.entries: list[dict[str, str]] = []

    def red(self, pin: str, mutation: str, observed: Any) -> None:
        self.entries.append(
            {"pin": pin, "mutation": mutation, "red": json.dumps(observed, default=str)}
        )

    def flush(self) -> None:
        MEASUREMENTS.mkdir(exist_ok=True)
        (MEASUREMENTS / self._filename).write_text(json.dumps(self.entries, indent=2))


@pytest.fixture
def engine_redlog() -> Iterator[RedLog]:
    """The red sink for the engine pins (one file, read by the gate)."""
    log = RedLog("pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def ledger_redlog() -> Iterator[RedLog]:
    """The red sink for the ledger pins."""
    log = RedLog("ledger-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def propagation_redlog() -> Iterator[RedLog]:
    """The red sink for the T06 propagation pins (the phase-2 family)."""
    log = RedLog("t06-propagation-reds.json")
    yield log
    log.flush()


#: The G7 always-on assertion's mapping: the §17.5 derivation's workflow
#: status → the flow ROOT row's job_status (the root's legal vocabulary).
#: blocked/running/pending runs are LIVE runs (the root stays running —
#: or its pre-start pending); complete runs report succeeded.
G7_DERIVED_TO_ROOT: dict[str, str] = {
    "complete": "succeeded",
    "failed": "failed",
    "cancelled": "cancelled",
    "blocked": "running",
    "running": "running",
    "pending": "pending",
}


@pytest.fixture
async def wf_g7_status_truth(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> AsyncIterator[None]:
    """G7 (T08): the ALWAYS-ON metamorphic assertion — at test end, the
    REPORTED workflow status (the flow root row's own status, the
    linearization point the engine's maintenance leg writes) equals the
    status RECONSTRUCTED FROM ROWS ALONE (the §17.5 derivation over the
    node rows + the ledger — D4's two-source rule). Registered for every
    workflow integration test via the collection hook (tests/conftest.py
    adds it to the wf-family files); catches status-drift continuously,
    not just in the dedicated pin's scenarios. A deliberately-lying
    fixture status (a root hand-written to a state the rows cannot
    derive) reds the suite (the drill pin proves the teeth)."""
    yield
    await g7_check(wf_conn, wf_schema, wf_sql)


async def g7_check(wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql) -> None:
    """The G7 assertion's body (one home — the fixture and the teeth-drill
    pin both run THIS, never a re-spelled copy)."""
    from taskq.workflows._status import reconstruct_workflow_status

    flows = await wf_conn.fetch(
        f"SELECT id, status FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__'"
    )
    for flow in flows:
        reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(flow["id"]))
        expected = G7_DERIVED_TO_ROOT.get(reconstructed, "running")
        assert flow["status"] == expected, (
            f"the reported status drifted from the rows: flow {flow['id']} "
            f"reports {flow['status']!r} but the rows reconstruct "
            f"{reconstructed!r} (expected the root {expected!r}) — the "
            "status cache has arrived (G7)"
        )


@pytest.fixture
async def wf_conn(clean_pg_conn: asyncpg.Connection) -> asyncpg.Connection:
    """The per-test clean connection on the module's migrated schema."""
    return clean_pg_conn


@pytest.fixture
def wf_schema(module_pg_schema: Any) -> str:
    return module_pg_schema.schema_name


@pytest.fixture
def wf_sql(module_pg_schema: Any) -> WorkflowSql:
    """The workflow statement bundle rendered for the module's schema."""
    return render_workflow_sql(module_pg_schema.schema_name)


class FlowStandIn:
    """The registered-definition stand-in (T09's API formalizes it; the
    run-key claim needs the placement fields only)."""

    def __init__(self, name: str = "ledger-flow") -> None:
        self.name = name
        self.actor = "wf"
        self.queue = "default"
        self.max_attempts = 3
        self.retry_kind = "transient"
        self.payload: dict[str, object] = {"flow": name}
        self.trace_id: str | None = None


async def seed_flow(
    conn: asyncpg.Connection, schema: str, *, status: str = "running", workflow: str | None = None
) -> JobId:
    """The flow-run row: a jobs row, step_key = the entry marker, the run
    scope. Its status IS the run's status (the linearization point).
    ``workflow`` stamps the root's metadata with the workflow's registered
    name — the sweep's fire arm resolves a healed join's reducer body from
    that REGISTERED DEFINITION (the durable, cross-process leg; the
    process-local memo is a cache). The stamp is what ``insert_flow_run``
    writes for a named flow; the seed passes it through so the pins drive
    the shipped shape."""
    flow_id = new_uuid()
    metadata: dict[str, object] = {"flow_id": str(flow_id)}
    if workflow:
        metadata["workflow"] = workflow
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', $2, "
        "'__flow__', $4::jsonb, 'workflow-run', $3)",
        flow_id,
        status,
        f"flow:{flow_id}",
        json.dumps(metadata),
    )
    return JobId(flow_id)


async def seed_join(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    step_key: str = "join",
    deps: int = 1,
    scheduled_in: float | None = None,
    consumers: list[dict[str, object]] | None = None,
) -> JobId:
    """A join-wait row: pending + deps_pending = deps + blocking_reason
    join. ``scheduled_in`` seconds in the future makes it a HELD row (P3
    rule 1's representation: the signal deadline is the only live timer)."""
    join_id = new_uuid()
    # VALUES expressions cannot reference the row's own columns -- the due
    # case is now() minus an hour, not the column minus an hour.
    scheduled_sql = (
        "now() - interval '1 hour'"
        if scheduled_in is None
        else f"now() + interval '{scheduled_in} seconds'"
    )
    meta: dict[str, object] = {"flow_id": str(flow_id), "blocking_reason": "join"}
    if consumers:
        meta["consumers"] = consumers
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, deps_pending, metadata, scheduled_at) "
        f"VALUES ($1, 'wf', 'default', '{{}}', 3, 'transient', 'pending', $2, $3, "
        f"$4::jsonb, {scheduled_sql})",
        join_id,
        step_key,
        deps,
        json.dumps(meta),
    )
    return JobId(join_id)


async def seed_edge(
    conn: asyncpg.Connection, schema: str, child_id: JobId, parent_id: JobId, flow_id: JobId
) -> None:
    """One edge-ledger row (the join counter's ONLY truth source)."""
    await conn.execute(
        f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id) VALUES ($1, $2, $3)',
        child_id,
        parent_id,
        flow_id,
    )


async def seed_running_node(
    conn: asyncpg.Connection, schema: str, flow_id: JobId, *, step_key: str = "a"
) -> JobId:
    """A running node: the finalize fence's admitted shape (its claim view:
    a fresh worker, attempt 1, epoch 0)."""
    node_id = new_uuid()
    worker_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, locked_by_worker, lock_expires_at, "
        "claim_epoch, step_key, metadata) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 1, $2, "
        "now() + interval '90 seconds', 0, $3, $4)",
        node_id,
        worker_id,
        step_key,
        json.dumps({"flow_id": str(flow_id)}),
    )
    return JobId(node_id)


async def claim_view(
    conn: asyncpg.Connection, schema: str, node_id: JobId
) -> tuple[JobId, int, int]:
    """The node's CURRENT claim view (worker, attempt, epoch) — what its
    own finalize presents to the fence."""
    rec = await conn.fetchrow(
        f'SELECT locked_by_worker, attempt, claim_epoch FROM "{schema}".jobs WHERE id = $1',
        node_id,
    )
    assert rec is not None and rec["locked_by_worker"] is not None
    return JobId(rec["locked_by_worker"]), rec["attempt"], rec["claim_epoch"]


async def node_state(conn: asyncpg.Connection, schema: str, node_id: JobId) -> dict[str, Any]:
    """The node row's engine-relevant columns (metadata jsonb arrives as
    str on un-coded connections — decoded here)."""
    rec = await conn.fetchrow(
        f'SELECT status, deps_pending, metadata, result FROM "{schema}".jobs WHERE id = $1',
        node_id,
    )
    assert rec is not None
    return {
        "status": rec["status"],
        "deps_pending": rec["deps_pending"],
        "metadata": rec["metadata"]
        if isinstance(rec["metadata"], dict)
        else json.loads(rec["metadata"] or "{}"),
        "result": rec["result"],
    }


async def fire_count(conn: asyncpg.Connection, schema: str, join_id: JobId) -> int:
    """How many fire rows the join has (exactly one = exactly-once)."""
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".wf_join_fire WHERE join_job_id = $1',
            join_id,
        )
    )
