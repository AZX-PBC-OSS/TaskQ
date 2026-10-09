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
import os
import time
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any
from uuid import (
    uuid4,  # noqa: TID251  # Why: the redlog RUN ID is deliberately NOT a persisted id — no B-tree, no ordering; randomness is the point (attribution token).
)

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
    fixture flushes on teardown.

    THE PRESERVATION LAW (attack-3's hygiene finding, the fixer's own
    lane): the evidence sinks are APPEND-ONLY and RUN-SCOPED — flush()
    appends ONE JSONL record (``{"run": …, "entries": [...]}``) and
    never rewrites the file. The convicted defect: ``write_text``
    rewrote the WHOLE sink per run, so a PARTIAL run (one pin file
    re-run in isolation) replaced the corpus with ONLY that subset's
    entries — a full run's evidence silently deleted, the recorded band
    numbers falsified by whichever subset ran last. Append-only: a
    partial run adds its own run-scoped record; history is never
    truncated. The run id (pid + a token + the timestamp) makes each
    record attributable."""

    def __init__(self, filename: str) -> None:
        self._filename = filename
        self._run_id = f"{time.strftime('%Y%m%dT%H%M%S')}-{os.getpid()}-{uuid4().hex[:8]}"
        self.entries: list[dict[str, str]] = []

    def red(self, pin: str, mutation: str, observed: Any) -> None:
        self.entries.append(
            {"pin": pin, "mutation": mutation, "red": json.dumps(observed, default=str)}
        )

    def flush(self) -> None:
        MEASUREMENTS.mkdir(exist_ok=True)
        record = {
            "run": self._run_id,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "entries": self.entries,
        }
        with (MEASUREMENTS / self._filename).open("a") as sink:
            sink.write(json.dumps(record) + "\n")


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
def hitl_redlog() -> Iterator[RedLog]:
    """The red sink for the T10 HITL pins (the convicted variants)."""
    log = RedLog("t10-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def loop_redlog() -> Iterator[RedLog]:
    """The red sink for the T19 loop pins (the mutation drills' reds)."""
    log = RedLog("t19-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def propagation_redlog() -> Iterator[RedLog]:
    """The red sink for the T06 propagation pins (the phase-2 family)."""
    log = RedLog("t06-propagation-reds.json")
    yield log
    log.flush()


@pytest.fixture
def t20_redlog() -> Iterator[RedLog]:
    """The red sink for the T20 streaming pins (the convicted variants)."""
    log = RedLog("t20-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def progress_redlog() -> Iterator[RedLog]:
    """The red sink for the T21 progress pins (the convicted variants)."""
    log = RedLog("t21-pin-reds.json")
    yield log
    log.flush()


@pytest.fixture
def createseam_redlog() -> Iterator[RedLog]:
    """The red sink for the create-seam pins (the create's atomicity, the
    run-key claim's honesty, the root-marker fence, the reap belt, the
    packaged run)."""
    log = RedLog("createseam-pin-reds.json")
    yield log
    log.flush()


#: The G7 always-on assertion's mapping: the §17.5 derivation's workflow
#: status → the flow ROOT row's job_status (the root's legal vocabulary).
#: The mapping is the TERMINAL states' expectation; the LAW is stated in
#: :func:`g7_check` (the root row is a cache, and each terminal root has
#: exactly one writer whose semantics decide what it may claim).
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
    pin both run THIS, never a re-spelled copy).

    THE LAW (the cache's semantics, stated from each terminal root's
    writer — the phase-2 attack's H4 round made this check ALWAYS-ON for
    real, so the law must red lies and never red the flips' windows):

    * the root row is a CACHE of the derivation;
    * a root claiming 'succeeded' is written by ONE writer only — the
      maintenance leg's complete branch (all rows terminal, none failed,
      none cancelled) — so the rows MUST derive 'complete'. A
      prematurely-complete root over rows that derive anything else is
      THE status-cache lie this check exists to catch (the teeth pin's
      own lie is this shape);
    * a root claiming 'failed' may never contradict a COMPLETED run (the
      cascade and the maintenance leg both write it from a non-absorbed
      failure on the rows; the rows cannot un-fail);
    * a root claiming 'cancelled' is the cancel arm's linearization
      point — the rows may lag it in every direction (a straggler child
      terminal-failing after the flip derives 'failed'; the flip
      stands) — nothing to assert against;
    * a LIVE root ('running'/'pending') may lag the rows' terminal
      verdict by one sweep pass — the maintenance leg's lag window (the
      H1 cure heals it in one pass; the H1 pin carries that teeth).
    """
    from taskq.workflows._status import reconstruct_workflow_status

    flows = await wf_conn.fetch(
        f"SELECT id, status FROM \"{wf_schema}\".jobs WHERE step_key = '__flow__'"
    )
    for flow in flows:
        reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, JobId(flow["id"]))
        status = flow["status"]
        if status == "succeeded":
            assert reconstructed == "complete", (
                f"the reported status drifted from the rows: flow {flow['id']} "
                f"reports 'succeeded' but the rows reconstruct "
                f"{reconstructed!r} — the status cache has arrived (G7): a "
                "prematurely-complete root is written by nothing but a "
                "cache"
            )
        elif status == "failed":
            assert reconstructed != "complete", (
                f"the reported status drifted from the rows: flow {flow['id']} "
                f"reports 'failed' but the rows reconstruct 'complete' — "
                "the wrong verdict on a completed run (G7)"
            )


@pytest.fixture
async def wf_conn(clean_pg_conn: asyncpg.Connection) -> asyncpg.Connection:
    """The per-test clean connection on the module's migrated schema."""
    return clean_pg_conn


@pytest.fixture
def wf_schema(module_pg_schema: Any) -> str:
    return module_pg_schema.schema_name


@pytest.fixture
async def wf_pool(module_pg_schema: Any) -> AsyncIterator[asyncpg.Pool]:
    """The RUNNER's pool (T09): the flow runner acquires from a pool (its
    claims + finalizes); the module's DSN builds it — one pool per test,
    closed at teardown. The loop pins (T19) share it (a fixture
    duplicated across two files is a seam owed NOW — this is the
    seam's home, beside the other wf fixtures)."""
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    yield pool
    await pool.close()


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
