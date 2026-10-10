"""THE EXECUTION MACHINERY'S DIRECT TESTS (F2-1): the Python half of the
production-execution cure — the door (:func:`~taskq.workflows.
_worker_execution.execute_flow_job`), the fleet-claimed step
(:meth:`FlowRunner.run_fleet_claimed_step`), the boot projection
(:func:`project_workflow_actor_configs`), and the intercept's engine
room (:func:`taskq.worker.run._dispatch_flow_job`) — every arm driven,
the receipts the assertions themselves.

The SQL fence's pin (the claim's capability leg) lives in
``test_wf_sweep_pins.py`` (pin 2) + the EXPLAIN bound pin
(``test_sweepaudit_dispatch_bound.py``); THIS family owns everything
Python: the resolution errors x the shapes, the parking path, the
cancel-absorption arm, the per-workflow projection isolation, the
epoch threading's fence (the wrong epoch fences the terminal out), and
THE LOOP SURVIVES (a transient from the door must never kill the
consumer loop — the F2-2 red, kept dead by the pin).
"""

from __future__ import annotations

import asyncio
import enum
import gc
import json
import types
import weakref
from datetime import UTC, datetime
from typing import Any
from unittest.mock import patch

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.backend._protocol import JobId, JobRow
from taskq.backend.clock import Clock
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.workflows import (
    DONE,
    Chain,
    FlowRunner,
    Route,
    Step,
    WorkflowApp,
    build,
    chain_source,
    step,
)
from taskq.workflows._worker_execution import (
    FlowExecution,
    WorkflowActorQueueConflictError,
    WorkflowBodyUnresolvableError,
    execute_flow_job,
    iter_imported_apps,
    project_workflow_actor_configs,
    workflow_execution_capable,
)
from tests._di_scopes import bootstrap_scopes, make_scopes
from tests.test_worker_drain import _backend_stub, _make_job_row, _settings, _stub_deps

pytestmark = pytest.mark.integration

CAPABLE_WORKER = JobId(new_uuid())


class ExecIngest(BaseModel):
    doc_id: str


class ExecReport(BaseModel):
    ref: str


RAN: list[str] = []


async def _exec_body(ctx: Any, params: ExecIngest) -> ExecReport:
    """The door's body: records the run, returns the typed result."""
    RAN.append(str(ctx.job_id))
    return ExecReport(ref=params.doc_id)


def _exec_app(workflow_name: str) -> WorkflowApp:
    """One healthy app: one step, the placement on the default pair.
    The build function is COMPILED here (``app.get`` — the compile
    records the compiled graph + registers the bodies, the same pass the
    boot projection runs)."""
    app = WorkflowApp()

    @app.workflow(workflow_name)
    def _workflow() -> object:
        return build(step(_exec_body, ExecIngest(doc_id="d1"), key="only"))

    app.get(workflow_name)
    return app


# ── 1. the boot projection: the cohorts, the isolation, the refusals ────


def _project_with(apps: list[Any]) -> list[Any]:
    """The projection with its app-registry view narrowed to *apps* (the
    projection reads the process-global WeakSet through
    ``iter_imported_apps``; the cohort tests narrow it for
    determinism — the real WeakSet's register/collect behavior is
    pinned separately below)."""
    with patch("taskq.workflows._worker_execution.iter_imported_apps", return_value=apps):
        return project_workflow_actor_configs()


def test_projection_projects_the_healthy_cohorts() -> None:
    """The healthy app's cohorts project: the (actor, queue) pairs the
    compiled graphs stamp, the projection's metadata stamp riding every
    row (the estate's surfaces see the projected population)."""
    app = _exec_app("wf-exec-projection")
    configs = _project_with([app])
    by_actor = {c.actor: c for c in configs}
    assert by_actor["wf"].queue == "default"
    assert by_actor["wf"].metadata == {"workflow_actor": True}


def test_projection_the_split_placement_cohorts() -> None:
    """The split placement's two cohorts: the source node's pair and the
    chain's gpu-named pair, ONE row each, both projected."""

    class ScreenOutcome(enum.Enum):
        CLEAN = "clean"
        FLAGGED = "flagged"

    async def _screen(ctx: Any, item: dict[str, object]) -> ScreenOutcome:
        return ScreenOutcome.CLEAN  # pragma: no cover - the compile only needs the body

    app = WorkflowApp()
    chain = Chain(
        name="wf-exec-split-chain",
        start="screen",
        steps={
            "screen": Step(
                body=_screen,
                outcomes=ScreenOutcome,
                route=Route({ScreenOutcome.CLEAN: DONE, ScreenOutcome.FLAGGED: DONE}),
            ),
        },
        actor="wf-exec-gpu",
        queue="gpu",
    )

    async def _split_source(ctx: Any) -> None:
        """The chain source's paged generator: the corpus rides the
        CLOSURE, not a params arg — a source body taking a param with no
        wired source is the E10-arity refusal (the wiring's own promise),
        so the split fixture's source takes ONLY the context."""

    @app.workflow("wf-exec-split")
    def _workflow() -> object:
        src = chain_source(chain, _split_source, key="doc_source")
        return build(src)

    configs = _project_with([app])
    pairs = {(c.actor, c.queue) for c in configs}
    assert ("wf", "default") in pairs, pairs
    assert ("wf-exec-gpu", "gpu") in pairs, pairs


def test_projection_broken_build_fn_skips_loudly_the_healthy_boots() -> None:
    """F2-3's pin: ONE broken build fn in ANY imported app must not
    refuse the worker's whole boot — the broken workflow skips LOUDLY
    (the ERROR line names the app, the workflow, the remedy) and the
    healthy app's cohorts project unchanged."""
    import structlog.testing

    broken = WorkflowApp()

    @broken.workflow("wf-exec-broken")
    def _broken_build() -> object:
        raise RuntimeError("the broken build fn (the F2-3 fixture)")

    healthy = _exec_app("wf-exec-healthy")
    with (
        structlog.testing.capture_logs() as logs,
        patch(
            "taskq.workflows._worker_execution.iter_imported_apps",
            return_value=[broken, healthy],
        ),
    ):
        configs = project_workflow_actor_configs()  # must NOT raise
    by_actor = {c.actor: c for c in configs}
    assert "wf" in by_actor, (
        f"the healthy app's cohorts must project beside the broken app; got {by_actor}"
    )
    # THE LOUD LOG: the skip is an ERROR-line event naming the app, the
    # workflow, the remedy (a silent skip would be the invisible cohort
    # by another door).
    skips = [e for e in logs if e.get("event") == "workflow-projection-skipped"]
    assert skips, "the skip is LOUD: an ERROR line names the broken workflow"
    assert any(e.get("workflow") == "wf-exec-broken" for e in skips)
    assert any(e.get("error_class") == "RuntimeError" for e in skips)


def test_projection_the_workflows_own_conflict_skips_the_workflow_loudly() -> None:
    """One workflow declaring its own actor over TWO queues: that
    workflow skips LOUDLY; the app's OTHER workflow still projects."""
    conflicting = WorkflowApp()

    @conflicting.workflow("wf-exec-conflicted")
    def _conflicted() -> object:
        a = step(_exec_body, ExecIngest(doc_id="d"), key="a", actor="wf", queue="default")
        b = step(_exec_body, a, key="b", actor="wf", queue="gpu")
        return build(b)

    healthy = _exec_app("wf-exec-beside-the-conflict")
    configs = _project_with([conflicting, healthy])  # must NOT raise
    by_actor = {c.actor: c for c in configs}
    assert "wf" in by_actor, f"the healthy workflow must still project; got {by_actor}"


def test_projection_cross_app_conflict_refuses() -> None:
    """Two HEALTHY workflows projecting one actor name onto different
    queues: the drift the estate's guards refuse — the boot refusal,
    never a silent skip (a skipped side's rows would be the invisible
    cohort by another door)."""
    app_default = _exec_app("wf-exec-xa")

    app_gpu = WorkflowApp()

    @app_gpu.workflow("wf-exec-xb")
    def _wb() -> object:
        return build(step(_exec_body, ExecIngest(doc_id="d"), key="only", actor="wf", queue="gpu"))

    with pytest.raises(WorkflowActorQueueConflictError, match="across workflows/apps"):
        _project_with([app_default, app_gpu])


def test_the_app_registry_observes_without_pinning() -> None:
    """The registry is a WeakSet: the app registers at construction, the
    capability marker answers from the LIVE apps, and a collected app
    leaves both (the registry observes the imported apps, never pins
    them)."""

    def _construct_and_drop() -> weakref.ref[Any]:
        app = WorkflowApp()  # the constructor registers it
        assert any(a is app for a in iter_imported_apps())
        return weakref.ref(app)

    ref = _construct_and_drop()
    gc.collect()
    assert ref() is None, "the registry must not pin the app (a WeakSet, not a list)"

    live_app = WorkflowApp()
    with patch("taskq.workflows._worker_execution._apps", weakref.WeakSet([live_app])):
        assert workflow_execution_capable() is True, (
            "at least one imported app = the process can resolve bodies"
        )
    with patch("taskq.workflows._worker_execution._apps", weakref.WeakSet()):
        assert workflow_execution_capable() is False, (
            "no imported app = not capable (the fence's data leg never lies)"
        )


# ── 2. the door: the resolution errors x the shapes + the fences ────────


def _job_row(
    job_id: JobId,
    *,
    metadata: dict[str, object],
    attempt: int = 1,
    claim_epoch: int = 1,
    payload_override: dict[str, object] | None = None,
) -> JobRow:
    """A claimed workflow row's dispatch read-model (the decode's own
    shape: NO step_key — the door's bounded read is the identity's
    source). ``payload_override`` is the row's real payload (the
    wiring's wf_args ride it) — the door's arg resolution reads the
    JobRow's payload, which is the decode's verbatim copy."""
    return JobRow(
        id=job_id,
        actor="wf",
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload=payload_override if payload_override is not None else {},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=attempt,
        max_attempts=3,
        retry_kind="transient",
        created_at=datetime(2025, 1, 1, tzinfo=UTC),
        scheduled_at=datetime(2025, 1, 1, tzinfo=UTC),
        metadata=metadata,
        claim_epoch=claim_epoch,
    )


async def _seed_flow_with_step(
    conn: asyncpg.Connection, schema: str, *, workflow: str | None, step_key: str = "only"
) -> tuple[JobId, JobId]:
    """The flow root (its metadata carrying the workflow stamp when
    named) + one claimed child row (running, the attempt incremented,
    the epoch bumped — the fleet claim's own shape)."""
    flow_id = new_uuid()
    root_meta: dict[str, object] = {"flow_id": str(flow_id)}
    if workflow:
        root_meta["workflow"] = workflow
    node_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.,
        "retry_kind, status, step_key, metadata, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', "
        "'__flow__', $2::jsonb, 'workflow-run', $3)",
        flow_id,
        json.dumps(root_meta),
        f"workflow-run:{flow_id}",
    )
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.,
        "retry_kind, status, attempt, claim_epoch, step_key, metadata, scheduled_at) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', 'running', 1, 1, "
        "$2, $3::jsonb, now() - interval '1 hour')",
        node_id,
        step_key,
        json.dumps({"flow_id": str(flow_id)}),
    )
    return JobId(flow_id), JobId(node_id)


async def test_the_door_executes_the_fleet_claimed_row(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: Any
) -> None:
    """THE DOOR'S GREEN (the machinery's own receipt): a claimed row
    resolves its body from the registered definition (D1), executes it
    ONCE, and the finalize lands — the row terminal-succeeds, the ledger
    says 'succeeded'."""
    del wf_conn
    app = _exec_app("wf-exec-door-green")
    runner = FlowRunner(app.get("wf-exec-door-green"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    node = await wf_pool.fetchrow(
        f"SELECT id, payload FROM \"{wf_schema}\".jobs WHERE step_key = 'only' "  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.,
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert node is not None
    # THE FLEET CLAIM: running, attempt 1, epoch 1 (dispatch_batch's own
    # shape).
    await wf_pool.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 1, "  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.,
        "claim_epoch = 1, locked_by_worker = $2, "
        "lock_expires_at = now() + interval '60 seconds' WHERE id = $1",
        node["id"],
        CAPABLE_WORKER,
    )

    RAN.clear()
    execution = await execute_flow_job(
        pool=wf_pool,
        schema=wf_schema,
        worker_id=CAPABLE_WORKER,
        job=_job_row(
            JobId(node["id"]),
            metadata={"flow_id": str(flow_id)},
            # THE ROW'S OWN PAYLOAD (the wiring's wf_args ride it — the
            # dispatch decode carries it verbatim; the door's arg
            # resolution reads it).
            payload_override=json.loads(node["payload"])
            if isinstance(node["payload"], str)
            else node["payload"],
        ),
    )
    assert isinstance(execution, FlowExecution)
    assert execution.outcome == "succeeded"
    assert execution.step_key == "only"
    assert execution.workflow_name == "wf-exec-door-green"
    assert execution.flow_id == str(flow_id)
    assert [str(node["id"])] == RAN, f"the body ran EXACTLY ONCE; ran={RAN}"
    row = await wf_pool.fetchrow(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.,
        node["id"],
    )
    assert row is not None and row["status"] == "succeeded"
    ledger = await wf_pool.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE job_id = $1 AND attempt = 1',  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.
        node["id"],
    )
    assert ledger == "succeeded", f"the ledger carries the attempt's own terminal; got {ledger}"


async def test_the_door_fence_the_wrong_epoch_never_lands(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: Any
) -> None:
    """THE EPOCH THREADING's receipt: a terminal write fenced on an epoch
    the row does not carry (a STALE read — the row was re-claimed between
    this worker's dispatch read and its finalize) updates NOTHING: the
    row stays running, the ledger records 'fenced' — the epoch parameter
    is load-bearing, not decoration."""
    del wf_conn
    app = _exec_app("wf-exec-door-epoch")
    runner = FlowRunner(app.get("wf-exec-door-epoch"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    node = await wf_pool.fetchrow(
        f"SELECT id, payload FROM \"{wf_schema}\".jobs WHERE step_key = 'only' "  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.,
        "AND (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    assert node is not None
    await wf_pool.execute(
        f"UPDATE \"{wf_schema}\".jobs SET status = 'running', attempt = 1, "  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.,
        "claim_epoch = 7, locked_by_worker = $2, "
        "lock_expires_at = now() + interval '60 seconds' WHERE id = $1",
        node["id"],
        CAPABLE_WORKER,
    )
    execution = await execute_flow_job(
        pool=wf_pool,
        schema=wf_schema,
        worker_id=CAPABLE_WORKER,
        job=_job_row(
            JobId(node["id"]),
            metadata={"flow_id": str(flow_id)},
            claim_epoch=6,  # STALE: the row moved on (re-claimed) under us
            payload_override=json.loads(node["payload"])
            if isinstance(node["payload"], str)
            else node["payload"],
        ),
    )
    assert (
        execution.outcome == "succeeded"
    )  # the tail's label (the record's echo — the ROW is the truth)
    row = await wf_pool.fetchrow(
        f'SELECT status, attempt, claim_epoch FROM "{wf_schema}".jobs WHERE id = $1',  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.
        node["id"],
    )
    assert row is not None, "the row exists"
    assert row["status"] == "running", (
        f"the fence must hold: the terminal write keyed on the STALE epoch "
        f"updates nothing; the row is status={row['status']} "
        f"attempt={row['attempt']} epoch={row['claim_epoch']}"
    )
    ledger = await wf_pool.fetchval(
        f'SELECT status FROM "{wf_schema}".wf_step_ledger WHERE job_id = $1 AND attempt = 1',  # noqa: S608  # Why: the schema identifier is the fixture's own (validated against _IDENT_RE); asyncpg cannot bind identifiers as parameters.
        node["id"],
    )
    assert ledger == "fenced", f"the fenced finalize is ON THE RECORD; got {ledger}"


async def test_the_door_resolution_errors(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: Any
) -> None:
    """The resolution errors x the shapes: every unresolvable shape
    raises the SAME typed error (the parking's shape) — no flow_id in
    the metadata; the row gone; the root stamping no workflow name; the
    stamped name never compiled here; the step key foreign to the
    registered definition."""
    # (a) no flow_id in the metadata. (wf_conn stays LIVE: the later
    # shapes seed rows through it.)
    with pytest.raises(WorkflowBodyUnresolvableError, match="no flow_id"):
        await execute_flow_job(
            pool=wf_pool,
            schema=wf_schema,
            worker_id=CAPABLE_WORKER,
            job=_job_row(JobId(new_uuid()), metadata={}),
        )
    # (b) the claimed row does not exist.
    with pytest.raises(WorkflowBodyUnresolvableError, match="gone or carries no step_key"):
        await execute_flow_job(
            pool=wf_pool,
            schema=wf_schema,
            worker_id=CAPABLE_WORKER,
            job=_job_row(JobId(new_uuid()), metadata={"flow_id": str(new_uuid())}),
        )
    # (c) the root stamps no workflow name.
    flow_id, node_id = await _seed_flow_with_step(wf_conn, wf_schema, workflow=None)
    with pytest.raises(WorkflowBodyUnresolvableError, match="stamps no workflow name"):
        await execute_flow_job(
            pool=wf_pool,
            schema=wf_schema,
            worker_id=CAPABLE_WORKER,
            job=_job_row(node_id, metadata={"flow_id": str(flow_id)}),
        )
    # (d) the stamped name is not compiled in this process.
    flow_id, node_id = await _seed_flow_with_step(
        wf_conn, wf_schema, workflow="wf-exec-never-imported"
    )
    with pytest.raises(WorkflowBodyUnresolvableError, match="never-imported"):
        await execute_flow_job(
            pool=wf_pool,
            schema=wf_schema,
            worker_id=CAPABLE_WORKER,
            job=_job_row(node_id, metadata={"flow_id": str(flow_id)}),
        )
    # (e) the step key is foreign to the registered definition.
    _exec_app("wf-exec-known")  # compiles + registers ONLY the 'only' step
    flow_id, node_id = await _seed_flow_with_step(
        wf_conn, wf_schema, workflow="wf-exec-known", step_key="foreign-step"
    )
    with pytest.raises(WorkflowBodyUnresolvableError, match="foreign-step"):
        await execute_flow_job(
            pool=wf_pool,
            schema=wf_schema,
            worker_id=CAPABLE_WORKER,
            job=_job_row(node_id, metadata={"flow_id": str(flow_id)}),
        )


# ── 3. the intercept's engine room: the cancel-absorption arm ───────────


def _seam_stub(execute: Any) -> types.ModuleType:
    """A stand-in seam module: the door replaced, the error class the
    REAL one (the intercept's except reads it off the seam)."""
    from taskq.workflows import _worker_execution as real

    stub = types.ModuleType("wf-exec-seam-stub")
    stub.execute_flow_job = execute  # type: ignore[attr-defined]
    stub.WorkflowBodyUnresolvableError = real.WorkflowBodyUnresolvableError  # type: ignore[attr-defined]
    return stub


async def test_the_intercept_absorbs_the_childs_cancellation() -> None:
    """THE CANCEL-ABSORPTION ARM: the child cancelled alone (an operator
    cancel's escalation, the loop alive) — the helper ABSORBS it (returns
    'cancelled'), never raises; the registry entry is gone."""
    from taskq.worker.cancel import ActiveJobRegistry
    from taskq.worker.run import _dispatch_flow_job
    from taskq.worker.shutdown import ShutdownPhase

    started = asyncio.Event()

    async def _sleeping_door(**kwargs: object) -> FlowExecution:
        started.set()
        await asyncio.sleep(30)
        return FlowExecution(  # pragma: no cover - the cancel lands first
            outcome="succeeded", step_key="s", workflow_name="w", flow_id="f"
        )

    settings = _settings()
    deps = _stub_deps(settings)
    active = ActiveJobRegistry()
    object.__setattr__(deps, "active_jobs", active)
    object.__setattr__(deps, "producer_stop_event", asyncio.Event())
    object.__setattr__(deps, "shutdown_phase", ShutdownPhase.NONE)

    job = _make_job_row("wf")
    job.metadata.update({"flow_id": str(new_uuid())})
    helper = asyncio.create_task(
        _dispatch_flow_job(
            deps=deps,
            job=job,
            worker_id=new_uuid(),
            enqueuer=SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=None),
            flow_seam=_seam_stub(_sleeping_door),
        )
    )
    await started.wait()
    entry = active.get(job.id)
    assert entry is not None, "the attempt is registered while the body runs"
    entry.task.cancel()
    outcome = await asyncio.wait_for(helper, timeout=5)
    assert outcome == "cancelled", f"the absorption returns the cancelled label; got {outcome!r}"
    assert active.get(job.id) is None, "the registry entry is deregistered on the absorption"
    await asyncio.sleep(0)  # let the cancelled child finish unwinding


# ── 4. THE LOOP SURVIVES (F2-2's pin) ───────────────────────────────────


async def _run_loop_over(
    jobs: list[JobRow],
    *,
    door: Any,
) -> tuple[Any, list[JobRow], list[tuple[object, dict[str, object] | None]]]:
    """Drive the REAL di_consumer_loop over *jobs* with the door stubbed
    to *door* (the seam patched); the FIRST plain row (no flow_id) is
    the loop's survival witness — its dispatch is the proof the loop
    moved on. Returns (deps, the witness's dispatch calls, the snooze
    writes)."""
    from taskq.worker.run import di_consumer_loop

    registry = ProviderRegistry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC)))
    registry.validate()

    process_scope, thread_scope, loop_scope = make_scopes(registry)
    await bootstrap_scopes(registry, process_scope, thread_scope, loop_scope, settings)

    @actor(name="wf")
    async def _wf_placeholder(payload: BaseModel, ctx: JobContext[BaseModel]) -> None: ...

    dispatch_calls: list[JobRow] = []
    stop_event = asyncio.Event()

    async def _witness_dispatch(**kwargs: object) -> str:
        job = kwargs.get("job")
        assert isinstance(job, JobRow)
        dispatch_calls.append(job)
        stop_event.set()
        return "succeeded"

    backend = _backend_stub()
    snoozed: list[tuple[object, dict[str, object] | None]] = []

    async def _mark_snoozed(
        job_id: object,
        worker_id: object,
        delay: object,
        metadata_update: dict[str, object] | None = None,
        **kwargs: object,
    ) -> str:
        snoozed.append((job_id, metadata_update))
        return "scheduled"

    backend.mark_snoozed = _mark_snoozed  # type: ignore[method-assign]  # Why: the test double records the parking write's shape; the spec'd stub's own method set is the vanilla legs', the parking arm is the flow intercept's.

    deps = _stub_deps(settings)

    local_queue: asyncio.Queue[JobRow] = asyncio.Queue()
    for job in jobs:
        await local_queue.put(job)

    seam = _seam_stub(door)
    with (
        patch("taskq.worker.run._workflow_execution_seam", return_value=seam),
        patch("taskq.worker.run.dispatch_one_job", side_effect=_witness_dispatch),
    ):
        loop_task = asyncio.create_task(
            di_consumer_loop(
                deps,
                local_queue,
                stop_event,
                backend=backend,
                worker_id=new_uuid(),
                registry=registry,
                process_scope=process_scope,
                thread_scope=thread_scope,
                loop_scope=loop_scope,
                actor_registry={"wf": _wf_placeholder},
                enqueuer=SubJobEnqueuer(
                    loop_scope_resolved=None, worker_pool=None, backend=backend
                ),
            )
        )
        await asyncio.wait_for(loop_task, timeout=10)

    await loop_scope.shutdown()
    await thread_scope.shutdown()
    await process_scope.shutdown()
    return deps, dispatch_calls, snoozed


def _flow_then_plain() -> tuple[JobRow, JobRow]:
    """The two-job harness rows: a flow row (the intercept's) then a
    plain row (the witness's — same registered actor, no flow_id, so the
    vanilla dispatch path takes it)."""
    flow_job = _make_job_row("wf")
    flow_job.metadata.update({"flow_id": str(new_uuid())})
    plain_job = _make_job_row("wf")
    return flow_job, plain_job


def test_the_loop_survives_a_transient_from_the_door() -> None:
    """F2-2's pin: the door raising a TRANSIENT (a pool/asyncpg error)
    must NOT kill the consumer loop — the loop absorbs (the vanilla leg's
    own shape: counted, the claim resolved), moves on, and the NEXT job
    processes (the witness dispatch fires)."""

    async def _raising_door(**kwargs: object) -> FlowExecution:
        raise ConnectionError("the door's pool died (the F2-2 fixture)")

    flow_job, plain_job = _flow_then_plain()
    deps, dispatch_calls, _snoozed = asyncio.run(
        _run_loop_over([flow_job, plain_job], door=_raising_door)
    )
    assert len(dispatch_calls) == 1, (
        f"THE LOOP MUST SURVIVE: the next job's dispatch must fire after "
        f"the door's transient; dispatched={len(dispatch_calls)}"
    )
    assert deps.drain_failures >= 1, "the transient is counted (the vanilla leg's own arm)"


def test_the_loop_survives_and_parks_the_unresolvable_row() -> None:
    """The parking path, AT THE LOOP: an unresolvable row snoozes
    budget-free with the released_reason stamp (the stranded-jobs
    detector's witness) and the loop moves on to the next job."""

    async def _unresolvable_door(**kwargs: object) -> FlowExecution:
        raise WorkflowBodyUnresolvableError(
            "workflow 'never-imported' is not compiled in this process"
        )

    flow_job, plain_job = _flow_then_plain()
    deps, dispatch_calls, snoozed = asyncio.run(
        _run_loop_over([flow_job, plain_job], door=_unresolvable_door)
    )
    assert len(dispatch_calls) == 1, "the loop survives the parking arm"
    assert len(snoozed) == 1, f"the unresolvable row parks exactly once; snoozed={snoozed}"
    assert snoozed[0][1] == {"released_reason": "workflow-body-unresolvable"}, snoozed[0]
    assert deps.drain_failures == 0, "the parking is budget-free (a defect row, not a failure)"


def test_the_loop_survives_and_disowns_the_slot_acquire_failure() -> None:
    """The door's pool-acquire failure (the SlotPoolAcquireError arm):
    the row is DISOWNED (the heartbeat stops renewing; the reclaim sweep
    owns the recovery) and the loop survives to the next job."""
    from taskq.worker.dispatch import SlotPoolAcquireError

    async def _slot_dead_door(**kwargs: object) -> FlowExecution:
        raise SlotPoolAcquireError(acquire_timeout=1.0)

    flow_job, plain_job = _flow_then_plain()
    deps, dispatch_calls, _snoozed = asyncio.run(
        _run_loop_over([flow_job, plain_job], door=_slot_dead_door)
    )
    assert len(dispatch_calls) == 1, "the loop survives the disown arm"
    assert flow_job.id in deps.disowned_jobs, (
        f"the row is disowned (nothing left to move it); disowned={deps.disowned_jobs}"
    )
