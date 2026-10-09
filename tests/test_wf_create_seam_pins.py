"""THE CREATE-SEAM PINS (the migrations/run-creation front's convictions,
cured): the create's atomicity, the run-key claim's honesty, the
root-marker dispatch fence, the nodeless-root reap belt, and the packaged
one-call run.

THE CONVICTIONS THE PINS HOLD RED FOREVER:

1. **THE CREATE ATOMICITY** — the pre-cure create was N+M+2 auto-committed
   statements: the root insert COMMITTED ALONE and the static nodes +
   ROOT_START rode a second acquire with no ``conn.transaction()``. The
   kill window committed the ORPHAN ROOT (pending, ZERO nodes): a root
   ``WORKFLOW_ROOT_MAINTAIN_SQL`` cannot derive (its rollup INNER-JOINS
   the node rows — a nodeless root never derives, never terminalizes,
   never prunes), and one that SQUATS THE RUN KEY (the retry's arbiter
   conflict answers ``created=False`` and the nodes never landed —
   ``drive()`` ran to ``max_ticks``). The cure is the fork-atomicity law
   at creation granularity: root + nodes + edges + ROOT_START are ONE
   transaction (the orphan is UNREPRESENTABLE), and the census-grace
   reap arm is the belt (any orphan that could ever exist — a future
   statement-order regression's debris — is reaped ``failed`` past the
   grace).
2. **THE CLAIM CHURN** — the fleet dispatch's candidate fence carries the
   ROOT-MARKER leg (the root row is a CACHE, never work); the pin drills
   the leg out of the rendered statement and observes the churn (a
   capable worker claims the pending root). The shipped statement greens
   it.
3. **THE RUN-KEY FAILURE LIE** — a FAILED run + the same run_key returned
   the failed run's bare id with NOTHING re-fired and no way to tell
   "already running" from "already failed" without a second query. The
   claim surface is HONEST: :meth:`FlowRunner.create_flow` returns the
   typed :class:`RunClaim` — ``created | existing-running |
   existing-terminal`` — and the existing-terminal claim is the
   REFUSED-TO-REUSE verdict stated loudly (the re-run is the caller's
   documented choice: a new key, never a silent 202).
4. **THE PACKAGED RUN** — ``taskq.workflows.run(flow, pool, schema,
   input=…, key=…)`` exists and works end to end (the one-call surface
   the consumer report demanded): create + drive + the honest claim + the
   decoded result. The docs teach THIS surface, not the phantom API.
"""

# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId
from taskq.workflows import (
    FlowRunner,
    StepContext,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows.engine import render_workflow_sql
from taskq.workflows.ledger import insert_flow_run
from tests._wf_fixtures import FlowStandIn, RedLog

# ── the pinned graph's bodies (module-level: the annotations must
#    resolve — the compile's hint resolution reads the defining globals) ──


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


async def _prepare(ctx: StepContext, params: Ingest) -> Report:
    return Report(ref=params.doc_id)


async def _tail(ctx: StepContext, t: Report) -> dict[str, str]:
    return {"tail": t.ref}


_FLOW_NAME = "create_seam_flow"


def _create_seam_app() -> tuple[WorkflowApp, Any]:
    """The pinned graph: two static nodes, no maps (the create's N+M+2
    shape exactly)."""
    app = WorkflowApp()

    @app.workflow(_FLOW_NAME)
    def create_seam_flow() -> object:
        a = step(_prepare, Ingest(doc_id="d1"), key="a")
        return build(step(_tail, a, key="tail"))

    return app, app.get(_FLOW_NAME)


async def census(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> int:
    """The run's NODE-row count (the orphan's census: a nodeless root
    counts ZERO)."""
    return int(
        await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 "
            "AND metadata ? 'flow_id' AND step_key <> '__flow__'",
            flow_id,
        )
    )


async def _root_status(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> str | None:
    return await conn.fetchval(f'SELECT status::text FROM "{schema}".jobs WHERE id = $1', flow_id)


@pytest.mark.integration
async def test_pin_create_is_one_transaction_the_kill_commits_nothing(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    createseam_redlog: RedLog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PIN 1a — THE CREATE ATOMICITY: the root insert + the static nodes +
    edges + ROOT_START are ONE transaction. THE DRILL: the node pass
    RAISES mid-create (the kill after the root insert) — the shipped tx
    rolls the root back and NOTHING commits; the convicted N+M+2 shape
    (each statement auto-committed) left the ORPHAN ROOT behind: pending,
    zero nodes, undevelopable by the maintenance derivation (the rollup
    INNER-JOINS the node rows), never pruned, and squatting the run key."""
    _app, compiled = _create_seam_app()
    runner = FlowRunner(compiled, wf_pool, wf_schema)

    async def _killed_nodes(conn: object, flow_id: JobId, input: object) -> None:
        raise RuntimeError("the kill: the process dies after the root insert")

    monkeypatch.setattr(runner, "_insert_static_nodes", _killed_nodes)
    with pytest.raises(RuntimeError, match="the kill"):
        await runner.create_flow(input=Ingest(doc_id="d1"), run_key="seam:kill")

    orphan = await wf_conn.fetchrow(
        f'SELECT id, status::text AS status FROM "{wf_schema}".jobs '
        "WHERE step_key = '__flow__' AND idempotency_key = 'seam:kill'"
    )
    if orphan is not None:
        createseam_redlog.red(
            "pin_create_is_one_transaction",
            "the create's statements are auto-committed (no conn.transaction()) "
            "— the root insert committed ALONE",
            {
                "orphan_root": str(orphan["id"]),
                "status": orphan["status"],
                "nodes": await census(wf_conn, wf_schema, JobId(orphan["id"])),
            },
        )
    assert orphan is None, (
        f"the kill after the root insert left ORPHAN ROOT {orphan} — the "
        "root insert committed ALONE (the N+M+2 shape): the create is not "
        "one transaction"
    )


@pytest.mark.integration
async def test_pin_kill_retry_completes_the_run(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    createseam_redlog: RedLog,
) -> None:
    """PIN 1b — THE RETRY COMPLETES: the legacy debris (a root committed
    without its nodes — the pre-cure head's exact product, crafted here
    through the shipped root claim) + the retry create_flow(run_key=…)
    → the nodes LAND, the root runs, the flow reaches terminal. The
    convicted retry was ``created=False → early return``: the census
    stayed 0 forever and ``drive()`` ran to ``max_ticks`` (the wedged
    run)."""
    _app, compiled = _create_seam_app()
    runner = FlowRunner(compiled, wf_pool, wf_schema)

    # The DEBRIS: the root row committed alone (the kill window's product).
    debris = await insert_flow_run(
        wf_conn,
        render_workflow_sql(wf_schema),
        entry=FlowStandIn(_FLOW_NAME),
        run_key="seam:retry",
    )
    assert debris.created

    claim = await runner.create_flow(input=Ingest(doc_id="d1"), run_key="seam:retry")
    nodes = await census(wf_conn, wf_schema, claim.flow_id)
    if nodes == 0:
        createseam_redlog.red(
            "pin_kill_retry_completes",
            "created=False → early return: the nodes never inserted "
            "(the run-key squatter — the retry wedged nodeless forever)",
            {"flow_id": str(claim.flow_id), "nodes": nodes},
        )
    assert nodes > 0, (
        "the retry against the orphan root inserted NOTHING — the run-key "
        "squatter (created=False) early-returned and the run is wedged "
        f"nodeless forever (census={nodes})"
    )
    assert await _root_status(wf_conn, wf_schema, claim.flow_id) == "running", (
        "the completed run's root must be running (ROOT_START rode the tx)"
    )
    verdict = await FlowRunner(compiled, wf_pool, wf_schema).drive(claim.flow_id)
    assert verdict == "terminal", (
        f"the completed run must drive to terminal — {verdict} is the wedge"
    )


@pytest.mark.integration
async def test_pin_dispatch_never_claims_the_flow_root(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    createseam_redlog: RedLog,
) -> None:
    """PIN 2 — THE ROOT-MARKER LEG: the fleet dispatch's candidate fence
    excludes the flow-root rows (step_key '__flow__' is a CACHE, never
    work). A pending root + a workflow-CAPABLE worker on the root's queue
    claims NOTHING. THE DRILL: the exclusion leg dropped from the rendered
    statement → the CHURN observed (the capable worker claims the root
    row — the claim-snooze-claim loop's fuel). The shipped statement
    greens it."""
    # The app constructs (the registry registers — the projection's input).
    _app, _compiled_flow = _create_seam_app()

    # The pending root on the flow's (actor, queue) cohort — crafted
    # through the shipped root claim (no nodes: the debris shape).
    root = await insert_flow_run(
        wf_conn,
        render_workflow_sql(wf_schema),
        entry=FlowStandIn(_FLOW_NAME),
        run_key="seam:churn",
    )
    assert root.created

    # The capable worker + the projected cohort (the boot's own surfaces).
    worker_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'createseam', 1, $2::text[], $3::jsonb)",
        worker_id,
        ["default"],
        json.dumps({"workflow_execution": True}),
    )
    from taskq.workflows import _worker_execution as seam

    _proj = seam.project_workflow_actor_configs()
    for config in _proj:
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1, $2) '
            "ON CONFLICT (actor) DO NOTHING",
            config.actor,
            config.queue,
        )

    # THE SHIPPED STATEMENT: the pending root is never claimed.
    claimed = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=10,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    assert claimed == [], (
        f"the fleet dispatch claimed {[str(r['id']) for r in claimed]} — the "
        "flow ROOT row is a cache, never work: the ROOT-MARKER leg dropped"
    )

    # THE DRILL: the exclusion leg dropped from the rendered statement —
    # the churn manifests on the SAME population (the pending root row).
    shipped_sql = DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema)
    mutated = shipped_sql.replace("step_key <> '__flow__'\n", "step_key IS NOT NULL\n")
    assert mutated != shipped_sql, "the mutation drill did not arm"
    churned = await dispatch_batch(
        wf_conn,
        sql=mutated,
        queues=["default"],
        limit_n=10,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    createseam_redlog.red(
        "pin_dispatch_never_claims_the_flow_root",
        "the ROOT-MARKER leg dropped from the candidate fence — the capable "
        "worker claims the pending root row (the churn)",
        {
            "claimed": [str(r["id"]) for r in churned],
            "step_keys": [r["step_key"] for r in churned],
        },
    )
    assert any(r["step_key"] == "__flow__" for r in churned), (
        "the mutation drill claimed no root row — the drill did not "
        "manifest the churn; the pin has no teeth"
    )


@pytest.mark.integration
async def test_pin_run_key_claim_is_honest_terminal_is_refused_loudly(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    createseam_redlog: RedLog,
) -> None:
    """PIN 3 — THE CLAIM SURFACE HONEST: create_flow returns the TYPED
    RunClaim — ``created`` / ``existing-running`` / ``existing-terminal``
    — never a bare id. THE CONVICTED LIE: a FAILED run + the same key
    returned the failed run's id (a silent 202 by another name): nothing
    re-fired, and 'already running' was indistinguishable from 'already
    failed' without a second query. The typed claim states the
    REFUSED-TO-REUSE verdict loudly: the caller re-runs by the documented
    path (a NEW key — a terminal run's key is never silently reused)."""
    _app, compiled = _create_seam_app()
    runner = FlowRunner(compiled, wf_pool, wf_schema)

    fresh = await runner.create_flow(input=Ingest(doc_id="d1"), run_key="seam:honest")
    if not hasattr(fresh, "kind"):
        createseam_redlog.red(
            "pin_run_key_claim_is_honest",
            "create_flow returned a bare JobId — the RunClaim.status "
            "DISCARDED (the caller cannot tell 'already running' from "
            "'already failed' without a second query)",
            {"returned": repr(fresh)},
        )
    assert hasattr(fresh, "kind"), (
        f"create_flow returned {fresh!r} — the bare id lie: the typed "
        "RunClaim (created | existing-running | existing-terminal) is the "
        "honest surface"
    )
    assert fresh.kind == "created", f"the fresh create must be 'created', got {fresh.kind!r}"
    assert fresh.created and fresh.status == "running"

    running = await runner.create_flow(input=Ingest(doc_id="d1"), run_key="seam:honest")
    assert running.kind == "existing-running", (
        f"the live run's replay must be 'existing-running', got {running.kind!r}"
    )
    assert running.flow_id == fresh.flow_id and not running.created

    # The run FAILS (the operator's terminal-fail stands in for any
    # terminal verdict): the SAME key must answer existing-terminal +
    # the prior run's id — LOUDLY, never a silent no-op.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'failed', finished_at = now() WHERE id = $1",
        fresh.flow_id,
    )
    failed = await runner.create_flow(input=Ingest(doc_id="d1"), run_key="seam:honest")
    assert failed.kind == "existing-terminal", (
        f"the FAILED run's replay must be 'existing-terminal', got "
        f"{getattr(failed, 'kind', repr(failed))!r} — a terminal run's key "
        "is never silently reused"
    )
    assert failed.status == "failed"
    assert failed.flow_id == fresh.flow_id, "the claim carries the PRIOR run's id"
    assert not failed.created
    # NOTHING re-fired: the terminal run's rows are the prior run's own.
    assert await census(wf_conn, wf_schema, failed.flow_id) > 0


@pytest.mark.integration
async def test_pin_nodeless_root_reap_belt(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    createseam_redlog: RedLog,
) -> None:
    """PIN 4 — THE CENSUS-GRACE REAP ARM (the belt): a NODELESS root
    (pending or running — the orphan shapes) past the grace is reaped
    ``failed`` with the LOUD error class; a healthy run (nodes present)
    is never touched; a nodeless root WITHIN the grace is never touched
    (the grace is the belt's own conservatism — a future statement-order
    regression's in-flight window is not reaped mid-create)."""
    from taskq.workflows._sweep import NODELESS_ROOT_REAP_GRACE_S, reap_nodeless_roots

    wsql = render_workflow_sql(wf_schema)
    _app, compiled = _create_seam_app()

    # THE ORPHAN: a nodeless RUNNING root, past the grace (created_at
    # back-dated by twice the grace).
    orphan = await insert_flow_run(
        wf_conn, wsql, entry=FlowStandIn(_FLOW_NAME), run_key="seam:orphan"
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', "
        "created_at = now() - make_interval(secs => $2) WHERE id = $1",
        orphan.flow_id,
        NODELESS_ROOT_REAP_GRACE_S * 2,
    )
    # THE YOUNG ORPHAN: nodeless, within the grace.
    young = await insert_flow_run(
        wf_conn, wsql, entry=FlowStandIn(_FLOW_NAME), run_key="seam:young"
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running' WHERE id = $1",
        young.flow_id,
    )
    # THE HEALTHY RUN: nodeless predicates never reap it — it has node rows.
    healthy = await insert_flow_run(
        wf_conn, wsql, entry=FlowStandIn(_FLOW_NAME), run_key="seam:healthy"
    )
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running' WHERE id = $1",
        healthy.flow_id,
    )
    await FlowRunner(compiled, wf_pool, wf_schema)._insert_static_nodes(
        wf_conn, healthy.flow_id, Ingest(doc_id="d1")
    )
    assert await census(wf_conn, wf_schema, healthy.flow_id) > 0

    reaped = await reap_nodeless_roots(wf_pool, wsql)
    if reaped == 0:
        createseam_redlog.red(
            "pin_nodeless_root_reap_belt",
            "the belt reaped nothing — the nodeless orphan root survives "
            "forever (the maintain derivation's INNER-JOIN blind spot)",
            {"reaped": reaped},
        )
    assert reaped >= 1, "the belt reaped nothing — the orphan survived"

    orphan_row = await wf_conn.fetchrow(
        f'SELECT status::text AS status, error_class FROM "{wf_schema}".jobs WHERE id = $1',
        orphan.flow_id,
    )
    assert orphan_row is not None and orphan_row["status"] == "failed", (
        f"the past-grace nodeless root must be reaped 'failed', got {orphan_row}"
    )
    assert orphan_row["error_class"] == "NodelessRunReaped", (
        "the reap is LOUD: the error class names the belt's verdict"
    )
    assert await _root_status(wf_conn, wf_schema, young.flow_id) == "running", (
        "the within-grace nodeless root must be untouched"
    )
    assert await _root_status(wf_conn, wf_schema, healthy.flow_id) == "running", (
        "the healthy run must be untouched"
    )


@pytest.mark.integration
async def test_pin_packaged_run_one_call_end_to_end(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    createseam_redlog: RedLog,
) -> None:
    """PIN 5 — THE PACKAGED RUN (the consumer report's ergonomic door):
    ``taskq.workflows.run(flow, pool, schema, input=…, key=…)`` exists and
    works END TO END — the typed claim + the drive + the decoded result.
    The re-run against the terminal run answers the HONEST claim (the
    demo's 409/202 distinction rides it). The phantom API the pre-cure
    docs taught (``workflows.run(flow, input, key=…)``) did not exist at
    all — this pin holds the REAL surface."""
    import taskq.workflows as wf

    if not hasattr(wf, "run"):
        createseam_redlog.red(
            "pin_packaged_run_one_call",
            "taskq.workflows.run does not exist — the docs taught a phantom "
            "API (workflows.run(flow, input, key=…))",
            {"surface": "taskq.workflows.run", "present": False},
        )
    _app, compiled = _create_seam_app()

    result = await wf.run(compiled, wf_pool, wf_schema, input=Ingest(doc_id="d1"))
    assert result.claim.kind == "created"
    assert result.outcome == "terminal"
    assert result.result == {"tail": "d1"}, f"the decoded result: {result.result!r}"

    # A keyed run, driven to terminal, then the replay: the HONEST claim.
    keyed = await wf.run(
        compiled, wf_pool, wf_schema, input=Ingest(doc_id="d1"), key="seam:package"
    )
    assert keyed.claim.kind in ("created", "existing-terminal")
    replay = await wf.run(
        compiled, wf_pool, wf_schema, input=Ingest(doc_id="d1"), key="seam:package"
    )
    if not hasattr(replay.claim, "kind") or replay.claim.kind != "existing-terminal":
        createseam_redlog.red(
            "pin_packaged_run_one_call",
            "the terminal run's replay did not answer the typed "
            "existing-terminal claim (the silent 202)",
            {"claim": repr(replay.claim)},
        )
    assert replay.claim.kind == "existing-terminal", (
        f"the terminal run's replay must be 'existing-terminal', got "
        f"{getattr(replay.claim, 'kind', repr(replay.claim))!r}"
    )
