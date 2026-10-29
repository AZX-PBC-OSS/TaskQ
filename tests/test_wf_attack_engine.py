"""ATTACK PINS — THE CRASHED-TERMINAL WEDGE + THE RUN-CREATION SEAM
(engine half).

Provenance: the hostile review of the consolidated head (af1b8779) —
FINDING PACK 1 (the round's most severe engine finding; attacker
evidence in schema ``att_t20``) and the migrations front's F-CREATE-1 /
F-CREATE-3. The pins assert the SAFE behavior; each carries its
unmarked red run in the pack's RECEIPTS.md.

THE WEDGE (pack 1, convicted LIVE at af1b8779): a run whose nodes are
``{succeeded|failed, crashed}`` wedges ``running`` FOREVER. The chain:

* the jobs-level reclaim (``backend._sweeps._SWEEP_1_BODY``) reads
  ``status = 'running'`` rows ONLY, and writes ``crashed`` exactly when
  the attempt budget is EXHAUSTED (``_RECLAIM_HAS_BUDGET_SQL``:
  ``attempt < max_attempts`` fails → the ELSE arm) — so every crashed
  row is terminal at the jobs layer, revisited by no arm;
* the workflow derivation's rule 1
  (``_status.derive_workflow_status``: any ``crashed`` → ``running``)
  and the maintenance leg's ``has_unresolved``
  (``_sql_status.WORKFLOW_ROOT_MAINTAIN_SQL``: ``crashed`` counts as
  unresolved work) both count ``crashed`` as "the reclaim's input — the
  run is still live (the reclaim re-runs it)" — true of a RECLAIMABLE
  crash, false of every row the reclaim actually writes;
* so ``reconstruct_workflow_status`` derives ``running`` for the
  corpse, the maintenance leg never finalizes the root, the G7
  always-on assertion (reported == reconstructed) stays GREEN on a
  dead run, and retention's liveness guard holds every row forever.

The named verdict (the lane's semantics read off ``_status.py``'s own
table): a budget-exhausted crashed node is an UNABSORBED, IRRECOVERABLE
failure — the table's rule 2 (``counts_failed``) shape — so the wedge
derives ``failed``, never ``running``, and never ``complete``
(rule 4's all-succeeded/failed/skipped row cannot claim a node whose
work can never report). The pins assert NOT-running + ``failed``.

F-CREATE-1 (LANDED): ``FlowRunner.create_flow`` is N+M+2 auto-committed
statements — ``_insert_root`` commits the root row ALONE on its own
pool acquire; ``_insert_static_nodes`` + ``ROOT_START`` ride a SECOND
acquire with NO ``conn.transaction()``. Kill after the root insert →
an orphan root (``pending``, zero nodes) the derivation can never see
(``WORKFLOW_ROOT_MAINTAIN_SQL`` inner-joins the node rows — the orphan
produces no ``per_flow`` row), never terminalizes, never prunes.

F-CREATE-3 (LANDED): the pending root row (``actor='wf'``,
``step_key='__flow__'``, ``metadata.flow_id`` = its own id) matches the
dispatch candidate predicates — ``backend/_dispatch_sql.py`` carries NO
``__flow__`` exclusion (grep-verified: the only ``step_key`` legs in
the claim path are the ``IS NULL`` short-circuit and the
capability/flow-terminal fence, which the pending root PASSES for a
``workflow_execution``-capable worker: the flow it names is itself,
and ``pending`` is not terminal). The claimed root reaches the
intercept, ``_resolve_body('__flow__', None)`` raises the plain
``WorkflowRunError`` — NOT the ``WorkflowBodyUnresolvableError`` the
intercept catches — and the row churns.
"""

# ruff: noqa: S608  # Why: every f-string SQL below interpolates only the module fixture's own throwaway schema identifier (validated against the fixtures' _IDENT_RE) or renders the engine's own named constants with a named mutation; all values are $n-bound.
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
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._status import reconstruct_workflow_status
from taskq.workflows._sweep import reap_phantom_ledger, sweep_join_rederive
from taskq.workflows.ledger import insert_flow_run
from tests._wf_fixtures import FlowStandIn, seed_flow

pytestmark = pytest.mark.integration

#: The jobs-table terminal vocabulary (the pin's SAFE side: a surfaced
#: run is one of these, never 'running').
TERMINAL = ("succeeded", "failed", "cancelled", "crashed", "abandoned")


async def _seed_wedge(wf_conn: asyncpg.Connection, wf_schema: str) -> JobId:
    """THE WEDGE SHAPE, seeded in the post-reclaim state: the flow root
    ``running``; one node ``crashed`` at the budget ceiling
    (``attempt == max_attempts`` — the row the reclaim's ELSE arm
    writes, terminal at the jobs layer, revisited by no arm) with its
    phantom ``running`` ledger row (the reaper fences 'running' ledger
    rows on TERMINAL flows only — the wedge feeds itself); one
    ``succeeded`` sibling (the sharpest form — everything else
    SUCCEEDED and the run still wedges)."""
    flow_id = await seed_flow(wf_conn, wf_schema, status="running")
    crashed = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, step_key, metadata, error_class, error_message, "
        "finished_at) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'crashed', 3, 'boom', "
        "$2::jsonb, 'WorkerCrashed', "
        "'lock expired before worker reported terminal state', clock_timestamp())",
        crashed,
        json.dumps({"flow_id": str(flow_id)}),
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
        "attempt, status) VALUES ($1, $2, $3, 'boom', 3, 'running')",
        new_uuid(),
        flow_id,
        crashed,
    )
    sibling = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, step_key, metadata, finished_at) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'succeeded', 1, 'ok', "
        "$2::jsonb, clock_timestamp())",
        sibling,
        json.dumps({"flow_id": str(flow_id)}),
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_step_ledger (id, flow_id, job_id, step_key, '
        "attempt, status) VALUES ($1, $2, $3, 'ok', 1, 'succeeded')",
        new_uuid(),
        flow_id,
        sibling,
    )
    return flow_id


# ── Pack 1, pin (a): THE DERIVATION MUST TERMINALIZE THE CORPSE ────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [pack-1(a)]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING pack-1(a) @ af1b8779: a run whose nodes are …
async def test_wedge_derivation_terminalizes_the_dead_run(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    """PACK 1(a): the derivation must terminalize a run whose work is
    all terminal-or-irrecoverable. The wedge shape {crashed at
    attempt == max_attempts, succeeded} derives ``failed`` — the table's
    rule-2 verdict for an unabsorbed failure — NEVER ``running``.

    The unmarked red (RECEIPTS.md): ``reconstruct_workflow_status``
    returns ``'running'`` for the corpse — and the G7 always-on
    assertion agrees with the root's ``running`` cache, so the dead run
    is hidden, not surfaced."""
    flow_id = await _seed_wedge(wf_conn, wf_schema)
    reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, flow_id)
    assert reconstructed == "failed", (
        f"the wedge derives {reconstructed!r}: a budget-exhausted crashed node "
        "(attempt == max_attempts — the reclaim's own terminal arm wrote it) "
        "is an unabsorbed IRRECOVERABLE failure, and a run whose work is all "
        "terminal-or-irrecoverable must derive 'failed', never 'running'"
    )


# ── Pack 1, pin (b): THE WEDGE IS SURFACED, NOT HIDDEN (the G7 face) ───


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [pack-1(b)]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING pack-1(b) @ af1b8779: the maintenance leg's …
async def test_wedge_root_is_surfaced_failed_within_maintenance_grace(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql, wf_pool: asyncpg.Pool
) -> None:
    """PACK 1(b), the G7 guard: after the cure the wedge is IMPOSSIBLE —
    the maintenance arms (``sweep_join_rederive``'s root-maintain leg +
    ``reap_phantom_ledger``) terminalize the wedged root within the
    sweep's grace, so the reported status and the rows-alone
    reconstruction agree ON A TERMINAL VERDICT (the drift-check's
    agreement then means what it says). The unmarked red (RECEIPTS.md):
    three full passes leave the root ``running`` and the reconstruction
    ``running`` — the always-on assertion green on a dead run."""
    flow_id = await _seed_wedge(wf_conn, wf_schema)
    for _ in range(3):
        await sweep_join_rederive(wf_pool, wf_sql)
        await reap_phantom_ledger(wf_pool, wf_sql)
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "failed", (
        f"the wedged run is hidden, not surfaced: the root is {root!r} after 3 "
        "maintenance passes — the budget-exhausted crashed node is reclaim's "
        "OUTPUT (terminal at the jobs layer), never its input; the root must "
        "terminalize 'failed' so the operator (and retention) can see the corpse"
    )
    # The drift-check's agreement is honest ONLY on the terminal verdict.
    reconstructed = await reconstruct_workflow_status(wf_conn, wf_sql, flow_id)
    assert reconstructed == "failed", (
        f"reported {root!r} but the rows reconstruct {reconstructed!r} — "
        "reported == reconstructed must hold ON THE TERMINAL VERDICT"
    )


# ── F-CREATE-1: THE INTERRUPTED CREATE LEAVES NO LIVE ORPHAN ───────────


class _CreateIn(BaseModel):
    doc_id: str


async def _create_body(ctx: StepContext, params: _CreateIn) -> Any:
    return {"doc_id": params.doc_id}


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-CREATE-1]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING F-CREATE-1 @ af1b8779: create_flow commits the …
async def test_interrupted_create_leaves_no_live_orphan(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F-CREATE-1: drive the REAL ``create_flow`` with the second leg
    killed (``_insert_static_nodes`` raises — the crash-after-root-insert
    window). The SAFE law, either cure direction:

    * ONE TRANSACTION — the root insert rolls back with the failure: NO
      root row exists afterwards; or
    * A NAMED ARM'S GRACE — the orphan is terminalized/reaped by the
      sweep within 3 maintenance passes.

    Today's shape fails both: the root row sits ``pending`` with zero
    nodes forever (the red in RECEIPTS.md)."""
    app = WorkflowApp()

    @app.workflow("create_seam_orphan")
    def _wf() -> Promise[object]:
        return build(step(_create_body, _CreateIn(doc_id="d1"), key="only"))

    runner = FlowRunner(app.get("create_seam_orphan"), wf_pool, wf_schema)

    async def _killed(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("killed mid-create — the process died after the root insert")

    monkeypatch.setattr(runner, "_insert_static_nodes", _killed)
    with pytest.raises(RuntimeError, match="killed mid-create"):
        await runner.create_flow(run_key="create1:orphan")

    root_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE step_key = '
        "'__flow__' AND idempotency_key = 'create1:orphan'"
    )
    if root_status is not None:
        # The orphan exists (today's shape: the root commit survived the
        # kill). The grace: the named arms must resolve it.
        for _ in range(3):
            await sweep_join_rederive(wf_pool, wf_sql)
            await reap_phantom_ledger(wf_pool, wf_sql)
        root_status = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".jobs WHERE step_key = '
            "'__flow__' AND idempotency_key = 'create1:orphan'"
        )
    assert root_status is None or root_status in TERMINAL, (
        f"the interrupted create left a LIVE orphan: the root is {root_status!r} "
        "with zero nodes — invisible to the maintenance leg's inner join, "
        "non-terminal after 3 passes, unprunable forever. One transaction, or "
        "a reaping arm within the grace: pick one."
    )


# ── F-CREATE-3: THE CLAIM PATH NEVER HANDS OUT THE __flow__ ROW ────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-CREATE-3]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING F-CREATE-3 @ af1b8779: the dispatch fence …
async def test_dispatch_never_claims_the_flow_root_row(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    """F-CREATE-3, at the SQL level (the doctrine's verify-first): the
    pending root row — the mid-create window's shape AND every run's
    steady pre-``ROOT_START`` instant — must be UNCLAIMABLE even by a
    registered, ``workflow_execution``-capable worker serving the root's
    own (actor, queue) cohort. The ``__flow__`` row is the run's
    linearization point, never a body-bearing node; handing it out buys
    the WorkflowRunError churn the intercept never signed for."""
    claim = await insert_flow_run(
        wf_conn, wf_sql, entry=FlowStandIn(name="root-claim-flow"), run_key="create3:root"
    )
    assert claim.created
    # The cohort + the CAPABLE worker (the F3 projection's stamps, seeded
    # the way test_pin_2's fence pin seeds them).
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".actor_config (actor, queue) '
        "VALUES ('wf', 'default') ON CONFLICT (actor) DO NOTHING"
    )
    worker_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'wf-pin', 1, '{default}', $2::jsonb)",
        worker_id,
        json.dumps({"workflow_execution": True}),
    )
    dispatched = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    claimed = {str(r["id"]) for r in dispatched}
    assert str(claim.flow_id) not in claimed, (
        f"the claim path handed out the __flow__ root row {claim.flow_id} to a "
        f"capable worker (claimed={sorted(claimed)}) — the fence admits any "
        "step_key row whose named flow is non-terminal, and the pending root "
        "names ITSELF; the root is the linearization point, never executable work"
    )
