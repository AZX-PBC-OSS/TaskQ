"""THE FLEET EXECUTOR'S OWN TESTS (the evidence-integrity cure 1a: the
scoped-coverage gate's least-tested seam is its most important new one).

``taskq/workflows/_worker_execution.py`` is THE queue-IS-the-runtime seam:
the app registry (the capability predicate's input), the boot projection
(F3 — the cohorts the fleet's dispatch LATERAL reads), and the execution
door (a claimed row EXECUTES on the claiming worker, exactly once). The
execution-cure's probes (``.measurements/execution-probe/*``) proved the
seam on a live deployment; these pins hold it on every battery run:

* **THE REGISTRY** — weak membership, the reset seam, the capability
  predicate's exact law;
* **THE PROJECTION** — the cohorts a compiled graph can stamp, the
  one-queue-per-actor refusal, the deterministic order, the
  ``workflow_actor`` metadata stamp (the estate sees the population);
* **THE DOOR** — the claimed/intercepted/executed-once/queue-routed path
  against a live PG: the fleet's ``dispatch_batch`` claims the row
  (queue-routed through the projected cohort, capability-fenced),
  ``execute_flow_job`` resolves the body from the REGISTERED definition,
  runs the runner's own machinery, and the body lands EXACTLY ONCE; and
  every unresolvable shape (no flow link, a vanished row, no step key,
  an unstamped root, an uncompiled name) refuses LOUD
  (``WorkflowBodyUnresolvableError``) — the parking semantics, never a
  silent wedge.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId, JobRow
from taskq.backend._records import _job_row_from_record
from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows import FlowRunner, Promise, WorkflowApp, build, step
from taskq.workflows import _worker_execution as seam

if TYPE_CHECKING:
    from taskq.workflows.api._app import CompiledWorkflow


def _claimed_row(record: asyncpg.Record) -> JobRow:
    """The claimed record decoded through the backend's OWN read model
    (the same decode the fleet's dispatch loop performs)."""
    return _job_row_from_record(record)


class Ingest(BaseModel):
    doc_id: str


@dataclass(frozen=True, slots=True)
class EnrichDeps:
    """The deps seam's demo shape (the app's declared Deps)."""

    marker: str = "bound-once"


class Report(BaseModel):
    ref: str


# ── module-level bodies (the compile's hint resolution reads globals) ────

_EXECUTIONS: list[str] = []


async def _observed_body(ctx: Any, params: Ingest) -> Report:
    """The door's victim body: records ITS OWN execution (the
    executed-exactly-once observation is the body's append, read back
    after the door returns)."""
    _EXECUTIONS.append(str(params.doc_id))
    return Report(ref=params.doc_id)


# ── the registry + the capability predicate (pure) ───────────────────────


def test_the_app_registry_is_weak_and_resettable() -> None:
    """A registered app is observable (``iter_imported_apps`` /
    ``workflow_execution_capable``); the reset seam clears it — the
    conftest's autouse isolation drives exactly this."""
    seam.reset_app_registry_for_tests()
    try:
        app = WorkflowApp()
        seam.register_app(app)
        assert app in seam.iter_imported_apps()
        assert seam.workflow_execution_capable() is True
        seam.reset_app_registry_for_tests()
        assert seam.iter_imported_apps() == []
        assert seam.workflow_execution_capable() is False, (
            "a process that never imported an app CANNOT execute flow bodies"
        )
    finally:
        seam.reset_app_registry_for_tests()


def test_the_compiled_cache_round_trips_and_misses_loud() -> None:
    """``record_compiled`` is the door's D1 face; a name that was never
    compiled misses with ``KeyError`` — the door converts it to the
    LOUD unresolvable error, never a silent wedge."""
    marker = object()
    seam.record_compiled("seam_probe_flow", marker)
    try:
        assert seam.get_compiled_workflow("seam_probe_flow") is marker
    finally:
        seam._compiled.pop("seam_probe_flow", None)
    with pytest.raises(KeyError):
        seam.get_compiled_workflow("seam_probe_never_compiled")


# ── the boot projection (F3's call site) ─────────────────────────────────


def test_the_runner_door_carries_the_apps_deps_binding(wf_pool: asyncpg.Pool) -> None:
    """THE DEPS SEAM'S WORKER DOOR (the DI capability): the compile
    carries the app's bound instance (``WorkflowApp(deps=…)``) and
    ``_runner_for`` — the worker-hosted execution door's runner factory —
    hands THE instance to its runner, the SAME binding the vanilla door
    and the packaged run read. No re-mint, no registry probe, no
    getattr-string: the instance rides the compiled object."""
    seam.reset_app_registry_for_tests()
    try:
        deps = EnrichDeps(marker="worker-door")
        app = WorkflowApp(deps=deps)

        @app.workflow("seam_deps_flow")
        def seam_deps_flow() -> Promise[object]:
            node = step(
                _observed_body, Ingest(doc_id="d"), key="solo", actor="wf-seam", queue="q-seam"
            )
            return build(node)

        app.get("seam_deps_flow")  # the door's compile + registration
        worker_id = JobId(new_uuid())
        runner = seam._runner_for("seam_deps_flow", wf_pool, "deps_probe_schema", worker_id)
        assert runner._deps is deps
    finally:
        seam.reset_app_registry_for_tests()


def test_the_projection_projects_the_declared_cohorts() -> None:
    """Every imported app's workflows compile HERE (the D1 registry
    populates as a side effect) and the graphs' (actor, queue) cohorts
    project as sorted ``ActorConfig`` rows stamped ``workflow_actor`` —
    the dispatch capacity LATERAL's visible population."""
    seam.reset_app_registry_for_tests()
    try:
        app = WorkflowApp()

        @app.workflow("seam_projection_flow")
        def seam_projection_flow() -> Promise[object]:
            node = step(
                _observed_body, Ingest(doc_id="d"), key="solo", actor="wf-seam", queue="q-seam"
            )
            return build(node)

        configs = seam.project_workflow_actor_configs()
        seam_cohorts = [(c.actor, c.queue) for c in configs if c.actor == "wf-seam"]
        assert seam_cohorts == [("wf-seam", "q-seam")], seam_cohorts
        for config in configs:
            if config.actor == "wf-seam":
                assert config.metadata.get("workflow_actor") is True, (
                    "the projection's rows carry the workflow_actor stamp — "
                    "the estate sees the population as PROJECTED (F3)"
                )
        # The projection's compile side effect: the D1 registry holds the
        # definition in THIS process (the door's body resolution). The
        # compile is DETERMINISTIC, not memoized: a re-derive is equal,
        # not identical (the registry's own idempotency law).
        compiled = cast("CompiledWorkflow", seam.get_compiled_workflow("seam_projection_flow"))
        assert compiled.name == "seam_projection_flow"
        assert app.get("seam_projection_flow").nodes.keys() == compiled.nodes.keys()
    finally:
        seam.reset_app_registry_for_tests()


def test_the_projection_refuses_one_actor_over_two_queues() -> None:
    """THE ONE-QUEUE LAW: one actor name declared over TWO queues is the
    refused defect (``actor_config`` is keyed by actor) — the split
    placement is expressed with DISTINCT actor names per queue. TWO
    granularities (the F2-3 isolation's law): a workflow whose OWN
    cohorts conflict skips THAT WORKFLOW LOUDLY (the healthy apps boot);
    the conflict ACROSS workflows/apps is the raise (skipping one side
    silently would make its rows unclaimable — the invisible cohort by
    another door)."""
    seam.reset_app_registry_for_tests()
    try:
        app = WorkflowApp()

        @app.workflow("seam_conflict_flow")
        def seam_conflict_flow() -> Promise[object]:
            node = step(_observed_body, Ingest(doc_id="d"), key="solo", actor="wf-split")
            second = step(_observed_body, node, key="two", actor="wf-split", queue="gpu")
            return build(second)

        # THE WORKFLOW'S OWN CONFLICT: the skip-loudly arm (F2-3) — the
        # boot survives, the defect is NAMED, the broken workflow's
        # cohorts project NOTHING (the isolation pin owns the log's
        # shape; here the seam's contract: the projection does NOT
        # raise on the workflow's own conflict).
        configs = seam.project_workflow_actor_configs()
        assert all(c.actor != "wf-split" for c in configs), (
            "the conflicting workflow's cohorts projected NOTHING"
        )

        # THE CROSS-WORKFLOW CONFLICT: the raise — two HEALTHY
        # declarations fighting over one cohort name is the drift the
        # guards refuse, never a silent skip.
        healthy = WorkflowApp()

        @healthy.workflow("seam_conflict_other")
        def seam_conflict_other() -> Promise[object]:
            third = step(
                _observed_body, Ingest(doc_id="d"), key="one", actor="wf-split", queue="q1"
            )
            return build(third)

        second_app = WorkflowApp()

        @second_app.workflow("seam_conflict_rival")
        def seam_conflict_rival() -> Promise[object]:
            fourth = step(
                _observed_body, Ingest(doc_id="d"), key="one", actor="wf-split", queue="q2"
            )
            return build(fourth)

        with pytest.raises(seam.WorkflowActorQueueConflictError) as excinfo:
            seam.project_workflow_actor_configs()
        assert "wf-split" in str(excinfo.value)
        assert "across workflows/apps" in str(excinfo.value)
    finally:
        seam.reset_app_registry_for_tests()


def test_the_projection_is_deterministically_ordered() -> None:
    """Two cohorts project in SORTED order (the drift comparison and the
    event stream read the projection — the readable order is the law)."""
    seam.reset_app_registry_for_tests()
    try:
        app = WorkflowApp()

        @app.workflow("seam_order_flow_b")
        def seam_order_flow_b() -> Promise[object]:
            return build(
                step(_observed_body, Ingest(doc_id="d"), key="s", actor="wf-b", queue="qb")
            )

        @app.workflow("seam_order_flow_a")
        def seam_order_flow_a() -> Promise[object]:
            return build(
                step(_observed_body, Ingest(doc_id="d"), key="s", actor="wf-a", queue="qa")
            )

        cohorts = [(c.actor, c.queue) for c in seam.project_workflow_actor_configs()]
        assert cohorts == sorted(cohorts), cohorts
        assert ("wf-a", "qa") in cohorts and ("wf-b", "qb") in cohorts
    finally:
        seam.reset_app_registry_for_tests()


# ── the execution door (live PG — the fleet path) ────────────────────────


async def _capable_worker_row(conn: asyncpg.Connection, schema: str, worker_id: UUID) -> None:
    """The dispatch fence's data leg: a registered worker whose boot ran
    the F3 projection (``workflow_execution`` stamped)."""
    await conn.execute(
        f'INSERT INTO "{schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'seam-pin', 1, '{default}', $2::jsonb)",
        worker_id,
        json.dumps({"workflow_execution": True}),
    )


async def _project_cohorts_into(conn: asyncpg.Connection, schema: str) -> None:
    """The boot's sync surface, as the worker's boot performs it: the
    projection's cohorts land in ``actor_config``."""
    for config in seam.project_workflow_actor_configs():
        await conn.execute(
            f'INSERT INTO "{schema}".actor_config (actor, queue) VALUES ($1, $2) '
            "ON CONFLICT (actor) DO NOTHING",
            config.actor,
            config.queue,
        )


@pytest.mark.integration
async def test_the_door_executes_a_claimed_row_exactly_once_queue_routed(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """THE FLEET PATH, end to end: the app registered → the projection
    stamps the cohorts → ``create_flow`` writes the root + the node row →
    ``dispatch_batch`` CLAIMS the row (queue-routed through the projected
    cohort, capability-fenced) → ``execute_flow_job`` resolves the body
    from the REGISTERED definition and runs the runner's own machinery →
    the body executed EXACTLY ONCE, the node terminalized."""
    seam.reset_app_registry_for_tests()
    _EXECUTIONS.clear()
    try:
        app = WorkflowApp()

        @app.workflow("seam_door_flow")
        def seam_door_flow() -> Promise[object]:
            return build(step(_observed_body, Ingest(doc_id="claimed"), key="solo"))

        await _project_cohorts_into(wf_conn, wf_schema)
        worker_id = new_uuid()
        await _capable_worker_row(wf_conn, wf_schema, worker_id)

        runner = FlowRunner(app.get("seam_door_flow"), module_pg_pool, wf_schema)
        flow_id = (await runner.create_flow(input={"doc_id": "claimed"})).flow_id

        # THE QUEUE ROUTING: the node row sits on the projected cohort's
        # queue and the fleet's dispatch claims it for the CAPABLE worker.
        dispatched = await dispatch_batch(
            wf_conn,
            sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
            queues=["default"],
            limit_n=5,
            worker_id=worker_id,
            lock_lease=timedelta(seconds=30),
        )
        assert len(dispatched) == 1, f"the flow row was not queue-routed/claimed: {dispatched}"
        record = dispatched[0]
        assert record["step_key"] == "solo"

        job = _claimed_row(record)
        assert job.metadata.get("flow_id") is not None
        outcome = await seam.execute_flow_job(
            pool=module_pg_pool,
            schema=wf_schema,
            worker_id=JobId(worker_id),
            job=job,
        )
        # THE DOOR'S RECORD (the fleet-truth law): the execution returns
        # the FlowExecution record — the outcome label AND the REAL
        # identities the door resolved on the way; the assert reads the
        # Record, never a bare string.
        assert outcome.outcome == "succeeded", outcome
        assert outcome.step_key == "solo" and outcome.workflow_name == "seam_door_flow"
        assert _EXECUTIONS == ["claimed"], (
            f"the body ran {len(_EXECUTIONS)}x — the door's exactly-once law broke"
        )
        node = await wf_conn.fetchrow(
            f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', record["id"]
        )
        assert node is not None and node["status"] == "succeeded", node
        # The runner's OWN ledger machinery ran (the same tx1/tx2, not a
        # second execution semantics): one claim row, the flow terminal.
        claims = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1', flow_id
        )
        assert claims == 1, claims
        # The ledger claim is TERMINAL (the runner's own finalize ran —
        # tx1/tx2, not a second execution semantics). The root row is a
        # CACHE, never the decider: its terminal verdict belongs to the
        # maintenance derivation, not to this door.
        ledger_status = await wf_conn.fetchval(
            f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1', flow_id
        )
        assert ledger_status == "succeeded", ledger_status
    finally:
        seam.reset_app_registry_for_tests()
        _EXECUTIONS.clear()


def _running_row(job_id: JobId, metadata: dict[str, object], *, actor: str = "wf") -> JobRow:
    """A claimed-row read model: running, attempt 1, epoch 0 — the shape
    the fleet's dispatch hands the intercept."""
    from datetime import UTC, datetime

    now = datetime.now(tz=UTC)
    return JobRow(
        id=job_id,
        actor=actor,
        queue="default",
        payload={},
        payload_schema_ver=1,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        created_at=now,
        scheduled_at=now,
        metadata=metadata,
    )


@pytest.mark.integration
async def test_the_door_refuses_a_row_without_a_flow_link(
    wf_schema: str, module_pg_pool: asyncpg.Pool
) -> None:
    """The door is for workflow rows ONLY: a claimed row whose metadata
    carries no ``flow_id`` refuses LOUD (the intercept never routes one
    here; a hand-routed one names itself)."""
    with pytest.raises(seam.WorkflowBodyUnresolvableError, match="no flow_id"):
        await seam.execute_flow_job(
            pool=module_pg_pool,
            schema=wf_schema,
            worker_id=JobId(new_uuid()),
            job=_running_row(JobId(new_uuid()), metadata={}),
        )


@pytest.mark.integration
async def test_the_door_refuses_a_vanished_row(
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool
) -> None:
    """A claimed row gone from the table (a crash-window delete) refuses
    LOUD — the bounded read answers nothing, the door never invents a
    body to run."""
    ghost_flow = await wf_conn.fetchval(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', '__flow__', "
        "$2::jsonb) RETURNING id",
        new_uuid(),
        json.dumps({"flow_id": str(new_uuid()), "workflow": "seam_ghost"}),
    )
    with pytest.raises(seam.WorkflowBodyUnresolvableError, match="gone or carries no step_key"):
        await seam.execute_flow_job(
            pool=module_pg_pool,
            schema=wf_schema,
            worker_id=JobId(new_uuid()),
            job=_running_row(JobId(new_uuid()), metadata={"flow_id": str(ghost_flow)}),
        )


@pytest.mark.integration
async def test_the_door_refuses_a_row_without_a_step_key(
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool
) -> None:
    """The claimed-row read model needs the step identity; a row with no
    ``step_key`` (the flow root itself, routed by defect) refuses LOUD."""
    root_id = await wf_conn.fetchval(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', NULL, "
        "$2::jsonb) RETURNING id",
        new_uuid(),
        json.dumps({"flow_id": str(new_uuid()), "workflow": "seam_root"}),
    )
    with pytest.raises(seam.WorkflowBodyUnresolvableError, match="gone or carries no step_key"):
        await seam.execute_flow_job(
            pool=module_pg_pool,
            schema=wf_schema,
            worker_id=JobId(new_uuid()),
            job=_running_row(JobId(root_id), metadata={"flow_id": str(new_uuid())}, actor="flow"),
        )


@pytest.mark.integration
async def test_the_door_refuses_a_flow_root_without_a_workflow_stamp(
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool
) -> None:
    """The stamp is the resolution's durable leg: a flow root predating
    the stamp (``metadata.workflow`` absent) leaves the body
    unresolvable — LOUD, budget-free parking (the actor-not-found
    semantics), never a silent wedge."""
    flow_id = new_uuid()
    node_id = await wf_conn.fetchval(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, deps_pending) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 'solo', "
        "$2::jsonb, 0) RETURNING id",
        new_uuid(),
        json.dumps({"flow_id": str(flow_id)}),
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', '__flow__', "
        "$2::jsonb)",
        flow_id,
        json.dumps({"flow_id": str(flow_id)}),  # NO 'workflow' stamp
    )
    with pytest.raises(seam.WorkflowBodyUnresolvableError, match="stamps no workflow name"):
        await seam.execute_flow_job(
            pool=module_pg_pool,
            schema=wf_schema,
            worker_id=JobId(new_uuid()),
            job=_running_row(JobId(node_id), metadata={"flow_id": str(flow_id)}),
        )


@pytest.mark.integration
async def test_the_door_refuses_an_uncompiled_workflow_name(
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool
) -> None:
    """A row stamped with a workflow THIS process never compiled names a
    deployment defect (the definitions module imported nowhere) — the
    door refuses LOUD with the deployment-defect message."""
    flow_id = new_uuid()
    node_id = await wf_conn.fetchval(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, deps_pending) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 'solo', "
        "$2::jsonb, 0) RETURNING id",
        new_uuid(),
        json.dumps({"flow_id": str(flow_id)}),
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata) "
        "VALUES ($1, 'flow', 'default', '{}', 3, 'transient', 'running', '__flow__', "
        "$2::jsonb)",
        flow_id,
        json.dumps({"flow_id": str(flow_id), "workflow": "seam_never_compiled"}),
    )
    with pytest.raises(seam.WorkflowBodyUnresolvableError, match="not compiled in this process"):
        await seam.execute_flow_job(
            pool=module_pg_pool,
            schema=wf_schema,
            worker_id=JobId(new_uuid()),
            job=_running_row(JobId(node_id), metadata={"flow_id": str(flow_id)}),
        )


# ── the intercept's seams (worker/run.py's lazy resolution) ──────────────


def test_the_intercepts_lazy_seam_resolves_and_maps_the_outcomes() -> None:
    """The worker's intercept resolves the seam LAZILY (the §16.1 law:
    ``import taskq`` never imports the package) and maps the execution
    tail's outcome labels onto the consumed-messages vocabulary."""
    from taskq.worker.run import _workflow_execution_seam

    resolved = _workflow_execution_seam()
    assert resolved is not None
    assert resolved.__name__ == "taskq.workflows._worker_execution"
    # Resolved once: the cache answers identically (process-stable).
    assert _workflow_execution_seam() is resolved

    from taskq.worker.run import _FLOW_OUTCOME_TO_CONSUMED

    # Every label the door can return maps (the vocabulary has no gap).
    for label in ("succeeded", "laddered", "held"):
        assert label in _FLOW_OUTCOME_TO_CONSUMED, label
