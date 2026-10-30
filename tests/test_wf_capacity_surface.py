"""THE CAPACITY-SURFACE PINS: the workflow actor's declared capacity
terms (``max_concurrent`` / ``max_pending`` / ``reservations`` /
``result_ttl``) — the declaration→projection leg the engine's per-actor
claim arithmetic already reads.

Five seams, one law (one actor, one capacity):

* the decorator's validation refusals (the vanilla ``@actor``'s own
  non-negative-int-or-None law, at the declaration site);
* the handle-terms flow (the ``@app.actor`` handle's terms ride the
  wiring into the node's decl as ADMISSION DEFAULTS — the explicit step
  params override, never the reverse);
* the projection's ``ActorConfig`` rows carrying the declared caps (the
  seed-only row the operator owns thereafter);
* the capacity-conflict refusals (one actor name, two different caps —
  within one workflow: skip LOUDLY; across apps: the boot REFUSES);
* the reservations' collection (the ensure-slots pass's input: dedup by
  name, deterministic order, the differing-config refusal).

The queue-trap door's boot gate (an unconsumed projected wf queue
refuses the boot) is pinned beside them — the same file, the same
surface.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from taskq.actor_config import ActorConfig
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.settings import WorkerSettings
from taskq.workflows import (
    Promise,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    loop,
    map_source,
    route,
    sink,
    step,
)
from taskq.workflows._worker_execution import project_workflow_actor_configs


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


# ── (a) the decorator's validation refusals ─────────────────────────────


def test_negative_max_concurrent_refused_at_declaration() -> None:
    """A negative ``max_concurrent`` is the vanilla ``@actor``'s own
    refusal, raised at the DECLARATION site (before anything registers —
    the app's queue universe is not yet touched)."""
    app = WorkflowApp()
    with pytest.raises(ValueError, match="max_concurrent"):

        @app.actor(max_concurrent=-1)
        async def bad(
            ctx: StepContext, params: Ingest
        ) -> Report:  # pragma: no cover - the refusal is the pin
            return Report(ref=params.doc_id)


def test_bool_capacity_refused_at_declaration() -> None:
    """``True``/``False`` are ints in Python — a bool passed as a cap is
    a bug wearing an int's clothes; the declaration refuses it."""
    app = WorkflowApp()
    with pytest.raises(ValueError, match="max_concurrent"):

        @app.actor(max_concurrent=True)
        async def bad(
            ctx: StepContext, params: Ingest
        ) -> Report:  # pragma: no cover - the refusal is the pin
            return Report(ref=params.doc_id)


# ── (b) the handle-terms flow ────────────────────────────────────────────


def test_handle_terms_flow_into_the_node_decl() -> None:
    """An ``@app.actor(max_concurrent=…, reservations=[…])`` handle wired
    via ``step()`` carries ITS terms into the node's decl as the
    ADMISSION DEFAULTS — the projection's raw material."""
    app = WorkflowApp()
    guard = ConcurrencyReservation(name="pdfium-guard", slots=1, lease=30.0)

    @app.actor(max_concurrent=1, reservations=[guard])
    async def guarded(
        ctx: StepContext, params: Ingest
    ) -> Report:  # pragma: no cover - the runner drives real bodies
        return Report(ref=params.doc_id)

    @app.workflow("guarded-flow")
    def guarded_flow() -> Promise[object]:
        return build(step(guarded, Ingest(doc_id="d"), key="guard"))

    node = app.get("guarded-flow").nodes["guard"]
    assert node.max_concurrent == 1
    assert node.reservations == (guard,)


def test_explicit_step_params_override_the_handle_terms() -> None:
    """The explicit step params OVERRIDE the handle's terms, never the
    reverse; an unoverridden term still flows (the handle's reservations
    ride when only ``max_concurrent`` is replaced)."""
    app = WorkflowApp()
    guard = ConcurrencyReservation(name="pdfium-guard", slots=1, lease=30.0)
    other = ConcurrencyReservation(name="other-guard", slots=2, lease=30.0)

    @app.actor(max_concurrent=1, reservations=[guard], result_ttl=60.0)
    async def guarded(
        ctx: StepContext, params: Ingest
    ) -> Report:  # pragma: no cover - the runner drives real bodies
        return Report(ref=params.doc_id)

    @app.workflow("overridden-flow")
    def overridden_flow() -> Promise[object]:
        return build(
            step(
                guarded,
                Ingest(doc_id="d"),
                key="guard",
                max_concurrent=4,
                reservations=[other],
            )
        )

    node = app.get("overridden-flow").nodes["guard"]
    assert node.max_concurrent == 4
    assert node.reservations == (other,)
    assert node.result_ttl == 60.0


def test_handle_term_conflict_message_names_reservation_buckets_not_instances() -> None:
    """The wiring conflict's message renders the two sides' BUCKET NAMES
    (the identity the whole capacity law speaks — name-keyed, H-A3), never
    the raw ``<ConcurrencyReservation object at 0x…>`` dump the default
    repr gives: the operator reading the refusal must see WHICH buckets
    disagree, and a pointer cannot be acted on."""
    from taskq.workflows.api._graph import WorkflowBuildError

    app = WorkflowApp()
    node_guard = ConcurrencyReservation(name="node-guard", slots=1, lease=30.0)
    handle_guard = ConcurrencyReservation(name="handle-guard", slots=2, lease=30.0)

    @app.actor(reservations=[handle_guard])
    async def map_body(
        ctx: StepContext, item: Ingest
    ) -> Report:  # pragma: no cover - the wiring refuses before any run
        return Report(ref=item.doc_id)

    @app.workflow("conflict-copy-flow")
    def conflict_copy_flow() -> Promise[object]:
        source = step(_plain_body, Ingest(doc_id="d"), key="src", reservations=[node_guard])
        return build(map_source(source, map_body))

    with pytest.raises(WorkflowBuildError, match="reservations") as excinfo:
        app.get("conflict-copy-flow")
    message = str(excinfo.value)
    assert "node-guard" in message and "handle-guard" in message, message
    assert "object at 0x" not in message, (
        f"the conflict message dumps reservation instances, not names: {message}"
    )


# ── (c) the projection's ActorConfig rows carry the declared caps ───────


def test_the_collect_reads_the_recorded_graphs_not_a_recompile() -> None:
    """H-D1's teeth: the projection records the compiled graphs (the
    ``record_compiled`` cache); the reservations' collect READS the
    recording — the build fn is NOT re-invoked (the compile+validate
    doubling: the boot paid every workflow's build fn twice, ~92us/node
    — 918ms at 10k nodes, paid again by the collect). The contract the
    collect keeps: the declared reservations still come back."""
    from taskq.workflows._worker_execution import collect_workflow_reservations

    app = WorkflowApp()
    guard = ConcurrencyReservation(name="counted-guard", slots=1, lease=30.0)
    calls = {"n": 0}

    def _counting_build() -> Promise[object]:
        calls["n"] += 1
        first = step(_plain_body, Ingest(doc_id="d"), key="solo", reservations=[guard])
        return build(first)

    app.workflow("counted-flow")(_counting_build)

    project_workflow_actor_configs()
    after_projection = calls["n"]
    assert after_projection >= 1, "the projection never compiled the workflow"

    reservations, _unknown = collect_workflow_reservations()
    assert calls["n"] == after_projection, (
        f"the collect re-invoked the build fn ({after_projection} -> {calls['n']}) — "
        "the compile doubling H-D1 convicted (the collect must read the "
        "recorded graphs, the sibling rate-limit collect's own shape)"
    )
    assert any(r.name == "counted-guard" for r in reservations), (
        "the collect stopped returning the declared reservations"
    )


def test_actor_carrier_carries_the_caps() -> None:
    """The handle's F3 carrier (``actor_config``) — the REAL projection's
    ONE vocabulary (H-A12), the same shape the config sync + the guards
    consume — carries the declared caps (the reservations do NOT ride
    it: no field; the ensure-slots pass owns their materialization)."""
    app = WorkflowApp()
    decorate = app.actor(max_concurrent=2, result_ttl=60.0, queue="gpu")
    handle = decorate(_plain_body)
    config = handle.actor_config()
    assert config == {
        "actor": "_plain_body",
        "queue": "gpu",
        "max_concurrent": 2,
        "result_ttl": 60.0,
        "metadata": {"workflow_actor": True},
        "capacity_owner": "operator",
    }


def test_actor_carrier_carries_the_code_owner() -> None:
    """C15: the F3 carrier carries the handle's capacity-owner
    declaration (the projection's vocabulary grew with the ActorConfig
    field) — "code" rides the carrier the same way the caps do."""
    app = WorkflowApp()
    handle = app.actor(max_concurrent=2, capacity_owner="code")(_plain_body)
    config = handle.actor_config()
    assert config["capacity_owner"] == "code"
    assert config["max_concurrent"] == 2


def test_projection_rows_carry_the_declared_caps() -> None:
    """The boot projection's ``ActorConfig`` rows carry the declared
    terms onto the cohort the STEP names (the wiring's placement law:
    the handle's queue is not the node's)."""
    app = WorkflowApp()

    @app.actor(max_concurrent=2, result_ttl=60.0)
    async def capped(
        ctx: StepContext, params: Ingest
    ) -> Report:  # pragma: no cover - the runner drives real bodies
        return Report(ref=params.doc_id)

    @app.workflow("capped-flow")
    def capped_flow() -> Promise[object]:
        return build(step(capped, Ingest(doc_id="d"), key="cap", actor="wf-cap", queue="gpu"))

    row = next(c for c in project_workflow_actor_configs() if c.actor == "wf-cap")
    assert row.max_concurrent == 2
    assert (
        row.max_pending is None
    )  # H-A6: the dead knob is off the surface — the row seeds it UNSET
    assert row.result_ttl == 60.0
    assert row.queue == "gpu"
    assert row.metadata == {"workflow_actor": True}


def test_projection_rows_default_the_unset_caps() -> None:
    """A cohort whose nodes declare no terms projects the all-None row —
    the seed-only undeclared shape (the positional-read regression's
    twin: a cap-less actor must never reach for a terms entry that is
    not there)."""
    app = WorkflowApp()

    @app.workflow("plain-flow")
    def plain_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="plain", actor="wf-plain"))

    row = next(c for c in project_workflow_actor_configs() if c.actor == "wf-plain")
    assert row.max_concurrent is None
    assert row.max_pending is None
    assert row.result_ttl is None


async def _plain_body(ctx: StepContext, params: Ingest) -> Report:
    """The uncapped body (the default-cohort probe)."""
    return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies


async def _report_body(ctx: StepContext, data: Report) -> Report:
    """The downstream body (consumes a parent's ``Report`` — the E5
    compat rule's own shape for the chained wirings)."""
    return data  # pragma: no cover - the runner drives real bodies


# ── (d) the capacity-conflict refusals ───────────────────────────────────


def test_within_workflow_capacity_conflict_refused() -> None:
    """One workflow, one actor name, two different caps — the graph's
    own conflict, refused where the cohorts are computed (the projection
    raises before any row is computed)."""
    from taskq.workflows._worker_execution import (
        WorkflowActorCapacityConflictError,
        _workflow_cohorts,
    )

    app = WorkflowApp()

    @app.workflow("conflict-flow")
    def conflict_flow() -> Promise[object]:
        first = step(_plain_body, Ingest(doc_id="d"), key="one", actor="wf-x", max_concurrent=1)
        second = step(_report_body, first, key="two", actor="wf-x", max_concurrent=2)
        return build(second)

    with pytest.raises(WorkflowActorCapacityConflictError, match="wf-x"):
        _workflow_cohorts(app.get("conflict-flow"))


def test_across_apps_capacity_conflict_refuses_the_boot() -> None:
    """Two HEALTHY apps declaring one actor name with different caps —
    the drift the guards refuse (skipping one side would project the
    OTHER's terms, the segfault guard failing open): the boot RAISES,
    the queue conflict's own shape."""
    from taskq.workflows._worker_execution import WorkflowActorCapacityConflictError

    app_a = WorkflowApp()

    @app_a.workflow("flow-a")
    def flow_a() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="a", actor="wf-x", max_concurrent=1))

    app_b = WorkflowApp()

    @app_b.workflow("flow-b")
    def flow_b() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="b", actor="wf-x", max_concurrent=2))

    with pytest.raises(WorkflowActorCapacityConflictError, match="wf-x"):
        project_workflow_actor_configs()


def test_same_terms_across_apps_merge() -> None:
    """The refusal is for DIFFERENT terms: the same declared caps on two
    apps' workflows of one actor name are the SAME declaration — the
    projection merges them into one row (the idempotent shape)."""
    app_a = WorkflowApp()

    @app_a.workflow("flow-a")
    def flow_a() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="a", actor="wf-x", max_concurrent=1))

    app_b = WorkflowApp()

    @app_b.workflow("flow-b")
    def flow_b() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="b", actor="wf-x", max_concurrent=1))

    rows = [c for c in project_workflow_actor_configs() if c.actor == "wf-x"]
    assert len(rows) == 1
    assert rows[0].max_concurrent == 1


# ── (e) the reservations' collection ─────────────────────────────────────


def test_collect_workflow_reservations_dedups_by_name_in_deterministic_order() -> None:
    """The collect walks the imported apps' compiled graphs' node decls:
    the same instance declared on several nodes returns ONCE (dedup by
    name) and the return is NAME-SORTED — byte-stable regardless of the
    app registry's iteration order."""
    from taskq.workflows._worker_execution import collect_workflow_reservations

    app = WorkflowApp()
    guard_b = ConcurrencyReservation(name="b-guard", slots=1, lease=30.0)
    guard_a = ConcurrencyReservation(name="a-guard", slots=2, lease=30.0)

    @app.actor(reservations=[guard_b])
    async def first(
        ctx: StepContext, params: Ingest
    ) -> Report:  # pragma: no cover - the runner drives real bodies
        return Report(ref=params.doc_id)

    @app.actor(reservations=[guard_a])
    async def second(
        ctx: StepContext, data: Report
    ) -> Report:  # pragma: no cover - the runner drives real bodies
        return data

    @app.workflow("res-flow")
    def res_flow() -> Promise[object]:
        one = step(first, Ingest(doc_id="d"), key="one")
        two = step(second, one, key="two")
        return build(two)

    # THE COLLECT READS THE RECORDED GRAPHS (H-D1): project first — the
    # boot's own order (the collect never re-invokes the build fn).
    project_workflow_actor_configs()
    assert collect_workflow_reservations() == ([guard_a, guard_b], [])


def test_collect_workflow_reservations_shared_handle_collects_once() -> None:
    """One reservation declared at TWO guarded wiring sites (two handles,
    the module-level singleton): the collect returns it once — the
    dedup's whole point."""
    from taskq.workflows._worker_execution import collect_workflow_reservations

    app = WorkflowApp()
    guard = ConcurrencyReservation(name="pdfium-guard", slots=1, lease=30.0)

    @app.actor(reservations=[guard])
    async def source(
        ctx: StepContext, params: Ingest
    ) -> Report:  # pragma: no cover - the runner drives real bodies
        return Report(ref=params.doc_id)

    @app.actor(reservations=[guard])
    async def sink_body(
        ctx: StepContext, data: Report
    ) -> Report:  # pragma: no cover - the runner drives real bodies
        return data

    @app.workflow("shared-flow")
    def shared_flow() -> Promise[object]:
        one = step(source, Ingest(doc_id="d"), key="one")
        two = step(sink_body, one, key="two")
        return build(two)

    project_workflow_actor_configs()  # the boot's own order (H-D1: the collect reads the recording)
    assert collect_workflow_reservations() == ([guard], [])


def test_collect_workflow_reservations_refuses_differing_configs() -> None:
    """One reservation NAME, two different configs — the registry's own
    duplicate-name law, pre-empted at the collect: ValueError, both
    configs named (never a silent first-wins shadow)."""
    from taskq.workflows._worker_execution import collect_workflow_reservations

    app = WorkflowApp()
    guard = ConcurrencyReservation(name="pdfium-guard", slots=1, lease=30.0)
    wider = ConcurrencyReservation(name="pdfium-guard", slots=8, lease=30.0)

    @app.actor(reservations=[guard])
    async def source(
        ctx: StepContext, params: Ingest
    ) -> Report:  # pragma: no cover - the runner drives real bodies
        return Report(ref=params.doc_id)

    @app.actor(reservations=[wider])
    async def sink_body(
        ctx: StepContext, data: Report
    ) -> Report:  # pragma: no cover - the runner drives real bodies
        return data

    @app.workflow("clash-flow")
    def clash_flow() -> Promise[object]:
        one = step(source, Ingest(doc_id="d"), key="one")
        two = step(sink_body, one, key="two")
        return build(two)

    project_workflow_actor_configs()  # the boot's own order (H-D1: the collect reads the recording)
    with pytest.raises(ValueError, match="pdfium-guard"):
        collect_workflow_reservations()


def test_collect_workflow_reservations_ignores_non_reservation_entries() -> None:
    """The decls' reservation slots carry INSTANCES for the collect; a
    non-reservation entry (an undeclared-shape placeholder) is not the
    collect's subject — it rides the surface untouched, uncollected."""
    from taskq.workflows._worker_execution import collect_workflow_reservations

    app = WorkflowApp()

    @app.workflow("mixed-flow")
    def mixed_flow() -> Promise[object]:
        return build(
            step(
                _plain_body,
                Ingest(doc_id="d"),
                key="one",
                actor="wf-m",
                reservations=("a-str-name",),
            )
        )

    assert collect_workflow_reservations() == ([], [])


# ── the queue-trap door (the boot gate) ──────────────────────────────────


def test_unconsumed_workflow_queue_refuses_the_boot() -> None:
    """A projected cohort's queue outside the worker's consumed set
    refuses the boot LOUDLY under the STRICT gate (the vanilla twin's
    own opt-in law — H-A8): the silent strand (rows pending forever, no
    error, anywhere) named, the strict knob + the remedy spelled."""
    from taskq.worker._bootstrap import _fail_fast_on_unconsumed_workflow_queues
    from taskq.workflows._worker_execution import WorkflowQueueUnconsumedError

    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES": "etl", "TASKQ_QUEUES_STRICT": "1"})
    cohorts = [
        ActorConfig(
            actor="wf-cap", max_concurrent=2, queue="default", metadata={"workflow_actor": True}
        )
    ]
    with pytest.raises(WorkflowQueueUnconsumedError) as excinfo:
        _fail_fast_on_unconsumed_workflow_queues(settings, cohorts)
    message = str(excinfo.value)
    assert "default" in message, message
    assert "etl" in message, "the consumed set is named"
    assert "TASKQ_QUEUES_STRICT" in message, "the strict gate's own name is on the record"
    assert "TASKQ_QUEUES" in message, "the remedy names the knob"


def test_consumed_workflow_queue_boots() -> None:
    """The green twin: a cohort on a consumed queue raises nothing."""
    from taskq.worker._bootstrap import _fail_fast_on_unconsumed_workflow_queues

    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES": "etl,gpu"})
    cohorts = [
        ActorConfig(
            actor="wf-cap", max_concurrent=2, queue="gpu", metadata={"workflow_actor": True}
        )
    ]
    _fail_fast_on_unconsumed_workflow_queues(settings, cohorts)


def test_empty_consumed_set_leaves_the_gate_silent() -> None:
    """A worker consuming nothing is the ``worker-consumes-no-queues``
    warning's own subject — the cohort gate does not double-signal on
    data it cannot judge."""
    from taskq.worker._bootstrap import _fail_fast_on_unconsumed_workflow_queues

    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES": ""})
    cohorts = [
        ActorConfig(
            actor="wf-cap", max_concurrent=2, queue="default", metadata={"workflow_actor": True}
        )
    ]
    _fail_fast_on_unconsumed_workflow_queues(settings, cohorts)


# ── the strict gate's SECOND FACE: the app-declared strictness ───────────


def test_app_declared_strict_queues_refuse_the_boot() -> None:
    """THE APP-DECLARED FACE of the strict gate: a WorkflowApp declaring
    ``queues_strict=True`` carries the contract in CODE — the boot
    REFUSES with NO env set at all. The fleet that needs this face
    structurally cannot use the env var (its workgroup children each
    subscribe to a queue subset; fleet-wide env coordination is
    impossible), but every child already imports the app — the
    declaration rides the app registry the projection itself walks
    (the OR-law's app-side arm)."""
    from taskq.worker._bootstrap import _fail_fast_on_unconsumed_workflow_queues
    from taskq.workflows._worker_execution import WorkflowQueueUnconsumedError

    app = WorkflowApp(queues_strict=True)  # the declaration — no env anywhere
    assert app.queues_strict is True
    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES": "etl"})  # env face silent
    cohorts = [
        ActorConfig(
            actor="wf-declared",
            max_concurrent=1,
            queue="remote",
            metadata={"workflow_actor": True},
        )
    ]
    with pytest.raises(WorkflowQueueUnconsumedError) as excinfo:
        _fail_fast_on_unconsumed_workflow_queues(settings, cohorts)
    message = str(excinfo.value)
    assert "remote" in message, message
    assert "queues_strict" in message, "the declaration face is named in the gate"
    assert "TASKQ_QUEUES_STRICT" in message, "the env face is named in the gate"


def test_app_without_declaration_keeps_the_warning_arm() -> None:
    """THE DEFAULT ARM, unchanged (the additive-only law): a registered
    app with NO declaration and no env keeps the ONE aggregated warning;
    the boot proceeds. The declaration's absence is today's behavior —
    even with an app in the registry (the getattr default's face)."""
    import structlog.testing

    from taskq.worker._bootstrap import _fail_fast_on_unconsumed_workflow_queues

    app = WorkflowApp()  # registered, NOT strict — the default
    assert app.queues_strict is False
    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES": "etl"})
    cohorts = [
        ActorConfig(
            actor="wf-undeclared",
            max_concurrent=1,
            queue="remote",
            metadata={"workflow_actor": True},
        )
    ]
    with structlog.testing.capture_logs() as logs:
        _fail_fast_on_unconsumed_workflow_queues(settings, cohorts)  # no raise
    warnings = [e for e in logs if e.get("event") == "workflow-cohorts-on-unconsumed-queues"]
    assert len(warnings) == 1, warnings
    assert warnings[0]["log_level"] == "warning", warnings[0]


def test_env_only_strict_still_refuses_with_an_undeclared_app() -> None:
    """THE ENV FACE, preserved (today's behavior): ``TASKQ_QUEUES_STRICT``
    refuses the boot even when a registered app did NOT declare strict —
    the env can only ADD strictness, and the env-only fleet loses
    nothing."""
    from taskq.worker._bootstrap import _fail_fast_on_unconsumed_workflow_queues
    from taskq.workflows._worker_execution import WorkflowQueueUnconsumedError

    # The ref IS the keep-alive: the app registry is a WeakSet — an
    # unheld app drops out, and the pin stops testing its subject.
    _app = WorkflowApp()  # registered, NOT strict
    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES": "etl", "TASKQ_QUEUES_STRICT": "1"})
    cohorts = [
        ActorConfig(
            actor="wf-env-only",
            max_concurrent=1,
            queue="remote",
            metadata={"workflow_actor": True},
        )
    ]
    with pytest.raises(WorkflowQueueUnconsumedError):
        _fail_fast_on_unconsumed_workflow_queues(settings, cohorts)


def test_env_and_declaration_together_refuse_the_boot() -> None:
    """BOTH faces set — the OR-law's intersection is the SAME refusal
    (strict is strict; no face can soften the other)."""
    from taskq.worker._bootstrap import _fail_fast_on_unconsumed_workflow_queues
    from taskq.workflows._worker_execution import WorkflowQueueUnconsumedError

    _app = WorkflowApp(queues_strict=True)  # keep-alive: the app registry is a WeakSet
    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES": "etl", "TASKQ_QUEUES_STRICT": "1"})
    cohorts = [
        ActorConfig(
            actor="wf-both",
            max_concurrent=1,
            queue="remote",
            metadata={"workflow_actor": True},
        )
    ]
    with pytest.raises(WorkflowQueueUnconsumedError):
        _fail_fast_on_unconsumed_workflow_queues(settings, cohorts)


def test_consumed_cohorts_boot_under_the_declaration() -> None:
    """A worker consuming ALL its projected cohorts is unaffected — even
    app-declared strict: the gate judges the cohort-vs-consumed delta;
    strictness only chooses the arm when the delta is non-empty. The
    healthy boot is SILENT (no refusal, no warning)."""
    import structlog.testing

    from taskq.worker._bootstrap import _fail_fast_on_unconsumed_workflow_queues

    _app = WorkflowApp(queues_strict=True)  # keep-alive: the app registry is a WeakSet
    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES": "etl,gpu"})
    cohorts = [
        ActorConfig(
            actor="wf-own", max_concurrent=1, queue="gpu", metadata={"workflow_actor": True}
        ),
        ActorConfig(
            actor="wf-other", max_concurrent=1, queue="etl", metadata={"workflow_actor": True}
        ),
    ]
    with structlog.testing.capture_logs() as logs:
        _fail_fast_on_unconsumed_workflow_queues(settings, cohorts)  # no raise
    assert logs == [], "the healthy boot is silent"


def test_consumed_cohorts_boot_under_env_strict() -> None:
    """The same green path under the ENV face: strict env + all cohorts
    consumed → the boot proceeds, silent (today's behavior preserved)."""
    import structlog.testing

    from taskq.worker._bootstrap import _fail_fast_on_unconsumed_workflow_queues

    _app = WorkflowApp()  # no declaration — the env is the only strict face
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "etl,gpu", "TASKQ_QUEUES_STRICT": "1"}
    )
    cohorts = [
        ActorConfig(
            actor="wf-own", max_concurrent=1, queue="gpu", metadata={"workflow_actor": True}
        ),
        ActorConfig(
            actor="wf-other", max_concurrent=1, queue="etl", metadata={"workflow_actor": True}
        ),
    ]
    with structlog.testing.capture_logs() as logs:
        _fail_fast_on_unconsumed_workflow_queues(settings, cohorts)  # no raise
    assert logs == [], "the healthy boot is silent"


def test_heterogeneous_fleet_child_keeps_the_warning_arm() -> None:
    """THE HETEROGENEOUS FLEET's per-worker law: a child whose OWN
    cohorts are consumed, while a SIBLING worker serves another queue,
    stays on the WARNING arm when nothing declares strict — the check is
    per-worker over THIS worker's projected cohorts, and the sibling's
    consumption is the warning's own caveat, never a refusal here."""
    import structlog.testing

    from taskq.worker._bootstrap import _fail_fast_on_unconsumed_workflow_queues

    # The ref IS the keep-alive: the app registry is a WeakSet — an
    # unheld app drops out, and the pin stops testing its subject.
    _app = WorkflowApp()  # no declaration, no env — the sibling case is the warning arm's subject
    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES": "etl"})
    cohorts = [
        ActorConfig(
            actor="wf-own", max_concurrent=1, queue="etl", metadata={"workflow_actor": True}
        ),
        ActorConfig(
            actor="wf-sibling",
            max_concurrent=1,
            queue="remote",
            metadata={"workflow_actor": True},
        ),
    ]
    with structlog.testing.capture_logs() as logs:
        _fail_fast_on_unconsumed_workflow_queues(settings, cohorts)  # no raise
    warnings = [e for e in logs if e.get("event") == "workflow-cohorts-on-unconsumed-queues"]
    assert len(warnings) == 1, warnings
    assert warnings[0]["queues"] == ["remote"], warnings[0]  # only the sibling's queue named
    assert warnings[0]["worker_queues"] == ["etl"], warnings[0]
    assert warnings[0]["cohorts"] == {"wf-sibling": "remote"}, warnings[0]


# ── (f) the round-1 convictions' pins (§H-A) ────────────────────────────


def test_shared_reservation_boots_clean_on_a_non_default_schema() -> None:
    """H-A2: ONE module-level reservation, declared once, passed to BOTH
    a vanilla ``@actor`` and a wf step — the boot registers it twice
    (the vanilla pass AND the wf pass's rebind at the SAME registry) and
    must stay CLEAN on a non-default schema: ONE rebind doctrine (the
    vanilla pass rebinds too, the same ``rebind_schema`` at the same
    phase). The pre-fix split (the vanilla pass registered the RAW
    declaration, the wf pass the rebind) died in the registry's
    ``_same_config`` schema comparison — the boot REFUSED."""
    from datetime import timedelta

    from pydantic import BaseModel, TypeAdapter

    from taskq.actor import ActorRef
    from taskq.retry import RetryPolicy
    from taskq.worker._bootstrap import _bootstrap_guards_and_registries
    from taskq.workflows._worker_execution import collect_workflow_reservations

    class _VanillaPayload(BaseModel):
        v: str = "x"

    class _VanillaResult(BaseModel):
        ok: bool = True

    shared = ConcurrencyReservation(
        "shared-guard", slots=1, lease=timedelta(seconds=30)
    )  # the module-level declaration — the settings-default schema
    live_schema = "deploy_schema_shared_guard"

    async def _handler(payload: _VanillaPayload) -> _VanillaResult:  # pragma: no cover
        return _VanillaResult()

    vanilla_ref = ActorRef(
        name="vanilla-sharer",
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_VanillaPayload,
        result_adapter=TypeAdapter(_VanillaResult),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
        reservations=[shared],
    )

    app = WorkflowApp()

    @app.actor(reservations=[shared])
    async def wf_sharer(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies

    @app.workflow("shared-reservation-flow")
    def shared_reservation_flow() -> Promise[object]:
        return build(step(wf_sharer, Ingest(doc_id="d"), key="one"))

    settings = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "default", "TASKQ_SCHEMA_NAME": live_schema}
    )
    # THE VANILLA PASS (the real bootstrap function).
    _di_registry, resolved = _bootstrap_guards_and_registries(
        settings, {"vanilla-sharer": vanilla_ref}, None, None, None
    )
    # THE WF PASS's registration shape (the bootstrap's own two lines):
    # the collect + the rebind + the register — the SAME registry.
    wf_collected, _wf_unknown = collect_workflow_reservations()
    for wf_res in wf_collected:
        resolved.register(wf_res.rebind_schema(settings.schema_name))
    # Clean: ONE registration, bound to the DEPLOYMENT's schema (the
    # ensure-slots pass's own filter reads it).
    live = resolved.get_reservation("shared-guard")
    assert live.schema == live_schema
    assert live.name == "shared-guard"


def test_collect_dedup_ignores_schema_but_heeds_keyed_and_lock_lease() -> None:
    """H-A9: the collect's dedup equality is the identity that SURVIVES
    the rebind — ``(name, slots, lease, keyed, lock_lease)``, the schema
    EXCLUDED (the boot's rebind owns it: two same-name same-config
    differing-schema declarations are ONE registration). The differing
    ``keyed``/``lock_lease`` refusals are the next pins' subject (one
    app per pin — the app registry is process-global)."""
    from datetime import timedelta

    from taskq.workflows._worker_execution import collect_workflow_reservations

    # The SAME-CONFIG differing-schema pair: ONE registration.
    plain_a = ConcurrencyReservation(
        "rebind-guard", slots=2, lease=timedelta(seconds=30), schema="taskq"
    )
    plain_b = ConcurrencyReservation(
        "rebind-guard", slots=2, lease=timedelta(seconds=30), schema="other_schema"
    )
    app = WorkflowApp()

    @app.actor(reservations=[plain_a])
    async def first(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies

    @app.actor(reservations=[plain_b])
    async def second(ctx: StepContext, data: Report) -> Report:
        return data  # pragma: no cover - the runner drives real bodies

    @app.workflow("dedup-schema-twin-flow")
    def dedup_schema_twin_flow() -> Promise[object]:
        one = step(first, Ingest(doc_id="d"), key="one")
        two = step(second, one, key="two")
        return build(two)

    project_workflow_actor_configs()  # the boot's own order (H-D1: the collect reads the recording)
    collected, _unknown = collect_workflow_reservations()
    assert [r.name for r in collected] == ["rebind-guard"], collected


def test_collect_dedup_refuses_differing_keyed_and_lock_lease() -> None:
    """H-A9's teeth: the dedup's identity HEEDS ``keyed`` — two same-name
    declarations differing on it are the refused defect (ValueError,
    both configs named). The pre-fix dedup ignored it (and compared the
    rebind-away schema)."""
    from datetime import timedelta

    from taskq.workflows._worker_execution import collect_workflow_reservations

    keyed = ConcurrencyReservation("keyed-guard", slots=1, lease=timedelta(seconds=30), keyed=True)
    static = ConcurrencyReservation(
        "keyed-guard", slots=1, lease=timedelta(seconds=30), keyed=False
    )
    app = WorkflowApp()

    @app.actor(reservations=[keyed])
    async def first(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies

    @app.actor(reservations=[static])
    async def second(ctx: StepContext, data: Report) -> Report:
        return data  # pragma: no cover - the runner drives real bodies

    @app.workflow("dedup-keyed-flow")
    def dedup_keyed_flow() -> Promise[object]:
        one = step(first, Ingest(doc_id="d"), key="one")
        two = step(second, one, key="two")
        return build(two)

    project_workflow_actor_configs()  # the boot's own order (H-D1: the collect reads the recording)
    with pytest.raises(ValueError, match="keyed-guard") as excinfo:
        collect_workflow_reservations()
    assert "keyed" in str(excinfo.value), "the differing field is named"


def test_collect_dedup_refuses_differing_lock_lease() -> None:
    """H-A9's teeth, the lock_lease arm: two same-name declarations
    differing ONLY on ``lock_lease`` are the refused defect — the lease
    the heartbeat extends by is part of the reservation's identity."""
    from datetime import timedelta

    from taskq.workflows._worker_execution import collect_workflow_reservations

    leased = ConcurrencyReservation(
        "lease-guard", slots=1, lease=timedelta(seconds=30), lock_lease=timedelta(seconds=10)
    )
    unleased = ConcurrencyReservation("lease-guard", slots=1, lease=timedelta(seconds=30))
    app = WorkflowApp()

    @app.actor(reservations=[leased])
    async def first(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies

    @app.actor(reservations=[unleased])
    async def second(ctx: StepContext, data: Report) -> Report:
        return data  # pragma: no cover - the runner drives real bodies

    @app.workflow("dedup-lock-lease-flow")
    def dedup_lock_lease_flow() -> Promise[object]:
        one = step(first, Ingest(doc_id="d"), key="one")
        two = step(second, one, key="two")
        return build(two)

    project_workflow_actor_configs()  # the boot's own order (H-D1: the collect reads the recording)
    with pytest.raises(ValueError, match="lease-guard") as excinfo:
        collect_workflow_reservations()
    assert "lock_lease" in str(excinfo.value), "the differing field is named"


def test_keyed_reservation_ref_refused_at_the_wiring() -> None:
    """H-A10: a ``KeyedReservationRef`` in ``reservations=`` is the LOUD
    door at the wiring (``rate_limit_names``' own pattern — the keyed
    shape names no concrete bucket, and a row stamped with a name it
    cannot carry is the admitted-never-limited lie). Never silently
    dropped."""
    from datetime import timedelta

    from taskq.ratelimit.refs import KeyedReservationRef
    from taskq.workflows.api._graph import WorkflowBuildError

    ref = KeyedReservationRef.typed(
        Ingest,
        base_name="tenant-guard",
        key_fn=lambda p: p.doc_id,
        slots=1,
        lease=timedelta(seconds=30),
    )
    app = WorkflowApp()

    @app.workflow("keyed-reservation-ref-flow")
    def keyed_ref_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="one", reservations=[ref]))

    # The wiring verbs run at COMPILE (the build fn is the recorder's
    # subject) — the door fires when the graph is compiled.
    with pytest.raises(WorkflowBuildError, match="KeyedReservationRef"):
        app.get("keyed-reservation-ref-flow")


def test_keyed_reservation_ref_refused_at_the_decorator() -> None:
    """H-A10's decorator arm: ``@app.actor(reservations=[ref])`` refuses
    the keyed shape at the declaration site — the same loud door, the
    other entry."""
    from datetime import timedelta

    from taskq.ratelimit.refs import KeyedReservationRef
    from taskq.workflows.api._graph import WorkflowBuildError

    ref = KeyedReservationRef.typed(
        Ingest,
        base_name="tenant-guard-decorator",
        key_fn=lambda p: p.doc_id,
        slots=1,
        lease=timedelta(seconds=30),
    )
    app = WorkflowApp()
    with pytest.raises(WorkflowBuildError, match="KeyedReservationRef"):

        @app.actor(reservations=[ref])
        async def guarded(ctx: StepContext, params: Ingest) -> Report:
            return Report(ref=params.doc_id)  # pragma: no cover


def test_max_pending_is_off_the_wf_surface() -> None:
    """H-A6 (the honest scope): ``max_pending`` is REMOVED from the wf
    surface — the engine's inserts bypass the enqueue admission and the
    claim SQL cannot bound a backlog (claiming is the only drain: a
    claim-side gate would wedge the backlog forever), so the declared
    knob was a DEAD one. A removed parameter is the surface's truth:
    passing it is the loud TypeError, never a silent accept."""
    app = WorkflowApp()
    with pytest.raises(TypeError, match="max_pending"):
        # Why the ignore: the REMOVED parameter is the pin's subject —
        # the checker's own refusal and the runtime TypeError agree.
        step(_plain_body, Ingest(doc_id="d"), key="one", max_pending=5)  # pyright: ignore[reportCallIssue]
    with pytest.raises(TypeError, match="max_pending"):
        app.actor(max_pending=5)  # pyright: ignore[reportCallIssue]


def test_actor_carrier_matches_the_projection_vocabulary() -> None:
    """H-A12: the handle's F3 carrier speaks the REAL projection's ONE
    vocabulary — the ``ActorConfig`` field set (``actor`` named by the
    handle's own declared name, the ``workflow_actor`` metadata mark),
    never a second, workflow-namespaced scheme beside it."""
    app = WorkflowApp()
    handle = app.actor(max_concurrent=2, result_ttl=60.0, queue="gpu")(_plain_body)
    config = handle.actor_config()
    assert config == {
        "actor": "_plain_body",
        "queue": "gpu",
        "max_concurrent": 2,
        "result_ttl": 60.0,
        "metadata": {"workflow_actor": True},
        "capacity_owner": "operator",
    }


def test_equal_terms_different_reservation_instances_do_not_conflict() -> None:
    """H-A3: the one-cap law compares the ROW-CARRIED fields only — two
    nodes, one actor name, equal ``max_concurrent``/``result_ttl``,
    DIFFERENT reservation instances of the SAME (name, slots, lease):
    NO conflict (the reservation's identity is the collect's own
    name-keyed law — one registration). The pre-fix law compared the
    INSTANCES (no ``__eq__``): equal configs were false conflicts."""
    from taskq.workflows._worker_execution import (
        collect_workflow_reservations,
        project_workflow_actor_configs,
    )

    app = WorkflowApp()
    twin_one = ConcurrencyReservation("twin-guard", slots=1, lease=30.0)
    twin_two = ConcurrencyReservation("twin-guard", slots=1, lease=30.0)
    assert twin_one is not twin_two

    @app.actor(max_concurrent=1, reservations=[twin_one], result_ttl=60.0)
    async def first(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies

    @app.actor(max_concurrent=1, reservations=[twin_two], result_ttl=60.0)
    async def second(ctx: StepContext, data: Report) -> Report:
        return data  # pragma: no cover - the runner drives real bodies

    @app.workflow("twin-instances-flow")
    def twin_instances_flow() -> Promise[object]:
        one = step(first, Ingest(doc_id="d"), key="one", actor="wf-twin")
        two = step(second, one, key="two", actor="wf-twin")
        return build(two)

    rows = [c for c in project_workflow_actor_configs() if c.actor == "wf-twin"]
    assert len(rows) == 1, "equal terms, different instances — ONE registration"
    assert rows[0].max_concurrent == 1
    # The reservations' identity is the collect's own name-keyed law.
    assert [r.name for r in collect_workflow_reservations()[0]] == ["twin-guard"]


def test_differing_row_fields_conflict_names_the_fields() -> None:
    """H-A3's teeth: differing ROW fields still conflict, and the error
    NAMES the differing fields (never a values-only tuple dump)."""
    from taskq.workflows._worker_execution import (
        WorkflowActorCapacityConflictError,
        _workflow_cohorts,
    )

    app = WorkflowApp()

    @app.workflow("field-names-flow")
    def field_names_flow() -> Promise[object]:
        first = step(_plain_body, Ingest(doc_id="d"), key="one", actor="wf-fn", max_concurrent=1)
        second = step(_report_body, first, key="two", actor="wf-fn", result_ttl=60.0)
        return build(second)

    with pytest.raises(WorkflowActorCapacityConflictError, match="max_concurrent") as excinfo:
        _workflow_cohorts(app.get("field-names-flow"))
    assert "result_ttl" in str(excinfo.value), "the differing field is named, both sides"


def test_within_workflow_capacity_conflict_refuses_the_boot() -> None:
    """H-A7: the within-workflow capacity conflict is NEVER skippable —
    the skip stranded the workflow's rows forever (no cohort projected,
    nothing claims them) and blinded the queue-trap door. The
    across-apps arm's semantics apply within one workflow TOO: the boot
    REFUSES. (A raising build fn stays skip-loudly — the isolation arm
    keeps the OTHER failure classes.)"""
    from taskq.workflows._worker_execution import WorkflowActorCapacityConflictError

    app = WorkflowApp()

    @app.workflow("conflict-refuses-flow")
    def conflict_refuses_flow() -> Promise[object]:
        first = step(_plain_body, Ingest(doc_id="d"), key="one", actor="wf-cr", max_concurrent=1)
        second = step(_report_body, first, key="two", actor="wf-cr", max_concurrent=2)
        return build(second)

    with pytest.raises(WorkflowActorCapacityConflictError, match="wf-cr"):
        project_workflow_actor_configs()


def test_a_raising_build_fn_still_skips_loudly() -> None:
    """H-A7's isolation arm, kept: a RAISING build fn (a capacity
    conflict's sibling — any non-capacity failure class) skips THAT
    workflow loudly and the healthy workflows still project. Only the
    CAPACITY conflict refuses the boot."""
    app = WorkflowApp()

    @app.workflow("raising-build-flow")
    def raising_build_flow() -> Promise[object]:
        raise RuntimeError("the build fn's own defect")

    @app.workflow("healthy-flow")
    def healthy_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d"), key="ok", actor="wf-healthy"))

    rows = [c for c in project_workflow_actor_configs() if c.actor == "wf-healthy"]
    assert len(rows) == 1, "the healthy workflow still projects"


def test_non_strict_foreign_cohort_warns_not_refuses() -> None:
    """H-A8: the queue-consumed refusal is the STRICT gate's own arm —
    the vanilla twin's opt-in law. NON-strict + a foreign cohort (a
    heterogeneous fleet's sibling worker owns the queue) is the WARNING
    arm (the aggregated event, the remedy named), never the refusal."""
    import structlog.testing

    from taskq.worker._bootstrap import (
        _fail_fast_on_unconsumed_workflow_queues,
    )

    settings = WorkerSettings.load_from_dict({"TASKQ_QUEUES": "etl"})  # non-strict (the default)
    cohorts = [
        ActorConfig(
            actor="wf-foreign", max_concurrent=1, queue="remote", metadata={"workflow_actor": True}
        )
    ]
    with structlog.testing.capture_logs() as logs:
        _fail_fast_on_unconsumed_workflow_queues(settings, cohorts)  # no raise
    warnings = [e for e in logs if e.get("event") == "workflow-cohorts-on-unconsumed-queues"]
    assert len(warnings) == 1, warnings
    assert warnings[0]["log_level"] == "warning"
    assert warnings[0]["queues"] == ["remote"], warnings[0]
    assert warnings[0]["cohorts"] == {"wf-foreign": "remote"}, warnings[0]
    assert "TASKQ_QUEUES" in str(warnings[0].get("remedy", "")), warnings[0]


# ── (g) H-A4: the silent cap-drop by the placement override ────────────


class ImageItem(BaseModel):
    kind_id: str


class AudioItem(BaseModel):
    kind_id: str


async def _route_source(ctx: StepContext, params: Ingest) -> list[ImageItem | AudioItem]:
    """The typed route's source body (the union's list)."""
    return [ImageItem(kind_id="i")]  # pragma: no cover - the runner drives real bodies


async def _audio_arm(ctx: StepContext, item: AudioItem) -> Report:
    return Report(ref=item.kind_id)  # pragma: no cover - the runner drives real bodies


def test_route_arm_actor_override_refuses_a_capped_body() -> None:
    """H-A4's refusal: a CAPPED handle wired as a route arm whose
    placement OVERRIDES the actor (``RouteArm(actor=…)``) — the arm's
    cohort would project NULL (uncapped): the handle's capacity terms
    dropped by the override, the guard failing open by rename. The
    wiring REFUSES, naming the dropped terms and the two cures (declare
    the terms on the override, or keep the handle's actor)."""
    from taskq.workflows.api._graph import RouteArm, WorkflowBuildError

    app = WorkflowApp()
    guard = ConcurrencyReservation("route-arm-guard", slots=1, lease=30.0)

    @app.actor(max_concurrent=1, reservations=[guard], result_ttl=30.0)
    async def capped_image_arm(ctx: StepContext, item: ImageItem) -> Report:
        return Report(ref=item.kind_id)  # pragma: no cover - the runner drives real bodies

    @app.workflow("route-override-refusal-flow")
    def route_override_refusal_flow() -> Promise[object]:
        source = step(_route_source, Ingest(doc_id="d"), key="src")
        return build(
            route(
                source,
                {
                    ImageItem: RouteArm(capped_image_arm, actor="wf-image"),  # the override
                    AudioItem: _audio_arm,
                },
            )
        )

    with pytest.raises(WorkflowBuildError, match="capacity terms would be dropped") as excinfo:
        app.get("route-override-refusal-flow")
    message = str(excinfo.value)
    assert "max_concurrent" in message and "reservations" in message and "result_ttl" in message, (
        "the dropped terms are named"
    )
    assert "wf-image" in message, "the override's own actor is named"


def test_route_arm_declares_its_own_terms() -> None:
    """H-A4's cure: the arm's OWN declaration (the RouteArm passthrough)
    covers the capped handle's terms — no refusal, and the arm's terms
    RIDE: the projection carries them onto the arm's effective cohort
    and the reservations collect (the ensure-slots pass's input)."""
    from taskq.workflows._worker_execution import (
        collect_workflow_reservations,
        project_workflow_actor_configs,
    )
    from taskq.workflows.api._graph import RouteArm

    app = WorkflowApp()
    arm_guard = ConcurrencyReservation("arm-own-guard", slots=2, lease=30.0)

    @app.actor(max_concurrent=1, reservations=[arm_guard], result_ttl=30.0)
    async def capped_image_arm(ctx: StepContext, item: ImageItem) -> Report:
        return Report(ref=item.kind_id)  # pragma: no cover - the runner drives real bodies

    @app.workflow("route-arm-own-terms-flow")
    def route_arm_own_terms_flow() -> Promise[object]:
        source = step(_route_source, Ingest(doc_id="d"), key="src")
        return build(
            route(
                source,
                {
                    ImageItem: RouteArm(
                        capped_image_arm,
                        actor="wf-image",
                        max_concurrent=2,
                        reservations=[arm_guard],
                        result_ttl=60.0,
                    ),
                    AudioItem: _audio_arm,
                },
            )
        )

    rows = {c.actor: c for c in project_workflow_actor_configs()}
    arm_row = rows["wf-image"]
    assert arm_row.queue == "default"
    assert arm_row.max_concurrent == 2, "the ARM's own declaration rides"
    assert arm_row.result_ttl == 60.0
    assert arm_guard.name in [r.name for r in collect_workflow_reservations()[0]], (
        "the arm's own reservation collects"
    )


def test_map_queue_only_override_is_not_refused() -> None:
    """H-A4's same-actor arm: a capped handle wired as the MAP body —
    the map's placement keeps the source node's actor (the map spells no
    actor override), so the terms are NOT dropped and NOT refused: they
    flow into the node's unset term slots (the handle-terms flow's own
    law) and ride the shared cohort."""
    from taskq.workflows._worker_execution import project_workflow_actor_configs

    app = WorkflowApp()
    map_guard = ConcurrencyReservation("map-body-guard", slots=1, lease=30.0)

    @app.actor(max_concurrent=1, reservations=[map_guard], result_ttl=45.0)
    async def capped_map_item(ctx: StepContext, item: ImageItem) -> Report:
        return Report(ref=item.kind_id)  # pragma: no cover - the runner drives real bodies

    @app.workflow("map-same-actor-flow")
    def map_same_actor_flow() -> Promise[object]:
        source = step(_route_source, Ingest(doc_id="d"), key="src")
        mapped = map_source(source, capped_map_item)  # the SAME cohort — no override
        return build(mapped)

    compiled = app.get("map-same-actor-flow")
    node = compiled.nodes["src"]
    assert node.max_concurrent == 1, "the map body's terms flowed in (never dropped)"
    assert node.reservations == (map_guard,)
    assert node.result_ttl == 45.0
    rows = {c.actor: c for c in project_workflow_actor_configs()}
    assert rows["wf"].max_concurrent == 1 and rows["wf"].result_ttl == 45.0


def test_chain_step_capped_body_refused() -> None:
    """H-A4's chain arm: a capped handle as a CHAIN step body — the
    chain's rows ride the CHAIN's own actor (the placement override the
    chain spells), and the chain surface has no terms passthrough: the
    handle's capacity terms would be dropped. The wiring REFUSES."""
    from taskq.workflows.api._graph import WorkflowBuildError
    from taskq.workflows.chain import Chain, Step

    app = WorkflowApp()

    @app.actor(max_concurrent=1)
    async def capped_chain_body(ctx: StepContext, item: ImageItem) -> Report:
        return Report(ref=item.kind_id)  # pragma: no cover - the runner drives real bodies

    capped_chain = Chain(
        name="capped-chain",
        start="s1",
        steps={"s1": Step(body=capped_chain_body, outcomes=Report)},
    )

    @app.workflow("chain-capped-body-flow")
    def chain_capped_body_flow() -> Promise[object]:
        source = step(_route_source, Ingest(doc_id="d"), key="src")
        return build(chain_source(capped_chain, _route_source, source, key="chain-src"))

    with pytest.raises(WorkflowBuildError, match="capacity terms would be dropped"):
        app.get("chain-capped-body-flow")


# ── (h) H-A11: the loop escalation's cohort projects ────────────────────


def test_escalation_cohort_projects() -> None:
    """H-A11: an ``on_exhausted="escalate"`` loop's ESCALATION rows stamp
    the registered definition's own (actor, queue) — the
    ``WorkflowDef.actor``/``queue`` defaults ("workflow"/"default") — and
    the projection never walked that cohort: the escalation rows parked
    forever under the DEFAULT on_exhausted policy (no actor_config row →
    no dispatch capacity, the strict-queue worker never listening). The
    projection INCLUDES the escalation cohort."""
    from taskq.workflows import Done, Refine, loop

    app = WorkflowApp()

    async def iter_body(ctx: StepContext, carry: None) -> Done[Report] | Refine[None]:
        return Done(Report(ref="done"))  # pragma: no cover - the runner drives real bodies

    @app.workflow("escalation-cohort-flow")
    def escalation_cohort_flow() -> Promise[object]:
        return build(loop("retrying", iter_body, initial=None, max_iterations=3))

    app.get("escalation-cohort-flow")  # the registration door (the D1 write)
    actors = {c.actor for c in project_workflow_actor_configs()}
    assert "workflow" in actors, (
        f"the escalation cohort (the WorkflowDef's own placement) projects: {sorted(actors)}"
    )


async def test_a_workflow_reservation_rebinds_its_schema_at_registration() -> None:
    """THE SCHEMA-AT-IMPORT SEAM (the flows demo's conviction): a
    workflow-declared reservation is constructed at module import —
    before settings exist — so its schema is the declaration's default,
    not the deployment's. The bootstrap's registration must rebind a
    COPY (the declaration never mutates — the same module-level instance
    may be registered by many processes); the pre-fix shape filtered
    silently, the guard failing closed invisibly: registered, never
    slotted, every acquire denied forever."""
    from datetime import timedelta

    from taskq.ratelimit.reservation import ConcurrencyReservation
    from taskq.workflows import WorkflowApp

    res = ConcurrencyReservation("pdfium_extract", slots=1, lease=timedelta(minutes=20))
    assert res.schema == "taskq"  # the declaration's default — NOT the deployment's

    app = WorkflowApp()

    @app.actor(reservations=[res], max_concurrent=1)
    def guarded(ctx: object, item: str) -> str:
        return item

    live_schema = "deploy_schema_under_test"
    rebound = res.rebind_schema(live_schema)
    assert rebound.schema == live_schema
    assert rebound.name == res.name and rebound.slots == res.slots
    assert res.schema == "taskq", "the declaration object must never mutate"
    assert rebound is not res


# ══ ROUND-2 RED-TEAM CONVICTIONS (§H-A's round-2 subsection) ═════════════


# ── C3: the registry's duplicate-name law ────────────────────────────────


def test_the_registry_duplicate_name_law_heeds_lock_lease() -> None:
    """C3: the REGISTRY's duplicate-name law compares the identity that
    survives the rebind — ``(name, slots, lease, keyed, lock_lease)``,
    the schema EXCLUDED (the rebind owns it). Two registrations differing
    only in ``lock_lease`` are the REFUSED defect (the ValueError names
    the field); an identical re-register is the idempotent no-op."""
    from datetime import timedelta

    from taskq.ratelimit.registry import RateLimitRegistry

    registry = RateLimitRegistry()
    registry.register(
        ConcurrencyReservation("dup-law", slots=2, lease=30.0, lock_lease=timedelta(seconds=60))
    )
    # THE IDENTICAL RE-REGISTER: idempotent (the debug no-op).
    registry.register(
        ConcurrencyReservation("dup-law", slots=2, lease=30.0, lock_lease=timedelta(seconds=60))
    )
    # THE TRUE CONFIG DIFFERENCE: refused, the field NAMED.
    with pytest.raises(ValueError, match="lock_lease") as excinfo:
        registry.register(
            ConcurrencyReservation("dup-law", slots=2, lease=30.0, lock_lease=timedelta(seconds=90))
        )
    assert "dup-law" in str(excinfo.value), "the reservation's own name is on the record"


# ── C2: the explicit-registry door (the docs' own pattern) ───────────────


def test_the_explicit_registry_door_boots_clean_on_the_docs_pattern() -> None:
    """C2: the docs' blessed pattern — construct a ``RateLimitRegistry``,
    ``.register()`` the RAW import-time reservation on it, pass it to
    ``worker_main(..., rate_limit_registry=rl)`` — must BOOT CLEAN on a
    non-default schema. ONE doctrine: the registration passes (the
    vanilla pass AND the wf pass) register the REBOUND copy, and the
    pre-registered raw instance REBINDS at the register step — the
    resolved registry's entries are ALWAYS settings-schema-bound. The
    pre-fix shape died in ``_same_config``'s schema comparison (the raw
    instance's ``taskq`` default vs the deployment's ``acme``): the boot
    REFUSED — the docs' own pattern was a crash."""
    from datetime import timedelta

    from pydantic import BaseModel, TypeAdapter

    from taskq.actor import ActorRef
    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.retry import RetryPolicy
    from taskq.worker._bootstrap import _bootstrap_guards_and_registries
    from taskq.workflows._worker_execution import collect_workflow_reservations

    class _VanillaPayload(BaseModel):
        v: str = "x"

    class _VanillaResult(BaseModel):
        ok: bool = True

    # THE DOCS' PATTERN: the explicit registry, the RAW declaration
    # registered on it (the import-time default schema), the registry
    # passed to worker_main.
    explicit = RateLimitRegistry()
    raw = ConcurrencyReservation("stripe_concurrent", slots=8, lease=timedelta(seconds=30))
    explicit.register(raw)

    async def _handler(payload: _VanillaPayload) -> _VanillaResult:  # pragma: no cover
        return _VanillaResult()

    vanilla_ref = ActorRef(
        name="vanilla-sharer",
        queue="default",
        fn=_handler,
        wants_ctx=False,
        dependencies={},
        payload_type=_VanillaPayload,
        result_adapter=TypeAdapter(_VanillaResult),
        retry=RetryPolicy(),
        result_ttl=None,
        singleton=False,
        unique_for=None,
        max_pending=None,
        reservations=[raw],
    )

    app = WorkflowApp()

    @app.actor(reservations=[raw])
    async def wf_sharer(ctx: StepContext, params: Ingest) -> Report:
        return Report(ref=params.doc_id)  # pragma: no cover - the runner drives real bodies

    @app.workflow("explicit-registry-flow")
    def explicit_registry_flow() -> Promise[object]:
        return build(step(wf_sharer, Ingest(doc_id="d"), key="one"))

    settings = WorkerSettings.load_from_dict(
        {"TASKQ_QUEUES": "default", "TASKQ_SCHEMA_NAME": "acme"}
    )
    # THE VANILLA PASS (the real bootstrap function) against the EXPLICIT
    # registry — the pre-fix shape raised ValueError here.
    _di_registry, resolved = _bootstrap_guards_and_registries(
        settings, {"vanilla-sharer": vanilla_ref}, None, None, explicit
    )
    # THE WF PASS (the bootstrap's own shape): the projection (the
    # compile's recording — the collect reads it), then the collect + the
    # rebind + the register — the SAME registry, the raw instance ALREADY
    # in it.
    project_workflow_actor_configs()
    wf_reservations, _wf_unknown = collect_workflow_reservations()
    for wf_res in wf_reservations:
        resolved.register(wf_res.rebind_schema(settings.schema_name))
    # CLEAN: the boot's resolved entry is ALWAYS settings-schema-bound —
    # the raw pre-registration was rebound AT the register step.
    live = resolved.get_reservation("stripe_concurrent")
    assert live.schema == "acme", "the resolved registry's entries are settings-schema-bound"
    assert live.name == "stripe_concurrent" and live.slots == 8
    # THE DECLARATION OBJECT IS NEVER MUTATED (the rebind is a copy).
    assert raw.schema == "taskq"
    assert raw is not live


# ── C4: the str-name reservations' boot warning ──────────────────────────


def test_the_collect_gathers_plain_str_reservation_names() -> None:
    """C4: the collect gathers the plain-``str`` reservation names the
    rate-limits collect gathers its own (the second slot's return — the
    bootstrap's warning input). The wiring door passes the str (the row
    stamps the name — the claim-time fail-closed arm remains the
    enforcer); the boot's job is the WARNING, never a silent gather."""
    from taskq.workflows._worker_execution import collect_workflow_reservations
    from taskq.workflows.api._graph import reservation_names

    # THE ROW STILL STAMPS THE NAME (the wiring door's own passthrough —
    # the claim-time fail-closed arm remains the enforcer).
    assert reservation_names(("typo-guard",), what="the step's reservations") == ("typo-guard",)

    app = WorkflowApp()

    @app.workflow("str-res-flow")
    def str_res_flow() -> Promise[object]:
        return build(
            step(
                _plain_body,
                Ingest(doc_id="d"),
                key="one",
                actor="wf-str-res",
                reservations=("typo-guard",),
            )
        )

    project_workflow_actor_configs()  # the boot's own order (the collect reads the recording)
    reservations, unknown = collect_workflow_reservations()
    assert reservations == [], "a str names no instance to register"
    assert unknown == ["typo-guard"], "the str name is GATHERED (the warning's input)"


def test_the_bootstrap_warns_unbacked_workflow_reservation_names() -> None:
    """C4's WARNING: the bootstrap's reservation twin of
    ``workflow-rate-limit-name-unregistered`` — a str name NO registry
    entry backs is the WARNING (the rate-limit twin's own shape: the
    names + the remedy); a name the registry (a vanilla actor's own
    registration) backs stays SILENT."""
    import structlog
    import structlog.testing

    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.worker._bootstrap import _warn_unregistered_workflow_reservations

    registry = RateLimitRegistry()
    log = structlog.get_logger("wf-capacity-round2-pin")
    with structlog.testing.capture_logs() as logs:
        _warn_unregistered_workflow_reservations(log, ["typo-guard"], registry)
    warnings = [e for e in logs if e.get("event") == "workflow-reservation-name-unregistered"]
    assert len(warnings) == 1, warnings
    assert warnings[0]["log_level"] == "warning"
    assert warnings[0]["names"] == ["typo-guard"], warnings[0]
    assert "park fail-closed" in str(warnings[0].get("remedy", "")), warnings[0]

    # THE BACKED NAME: a vanilla actor's own registration (or the wf
    # collect's) put the name in the registry — no warning.
    registry.register(ConcurrencyReservation("typo-guard", slots=1, lease=30.0))
    with structlog.testing.capture_logs() as logs:
        _warn_unregistered_workflow_reservations(log, ["typo-guard"], registry)
    assert [e for e in logs if e.get("event") == "workflow-reservation-name-unregistered"] == [], (
        "a registry-backed name stays silent"
    )


# ── C6: the workflow placement (the definition's own rows' face) ─────────


async def _loop_iter_body(ctx: StepContext, carry: None) -> object:
    """The escalation pins' loop body (the runner drives real bodies)."""
    from taskq.workflows import Done

    return Done(Report(ref="done"))  # pragma: no cover - the runner drives real bodies


def test_the_workflow_placement_declares_the_escalation_cohort() -> None:
    """C6: ``@app.workflow(name, actor=…, queue=…)`` is THE PLACEMENT
    FACE for the definition's own rows — the escalation consumer rows
    (D1: ``escalation_bindings`` reads the definition) AND the loop
    node's fallback placement (the loop's rows fall back to the
    workflow's declared cohort instead of the hardcoded
    ``("wf", "default")``). The projection reads the WorkflowDef's
    placement: the ("wf-x", "heavy") cohort projects."""
    from taskq.workflows._worker_execution import project_workflow_actor_configs
    from taskq.workflows.api._loop import escalation_bindings
    from taskq.workflows.definitions import get_registry

    app = WorkflowApp()

    @app.workflow("placed-flow", actor="wf-x", queue="heavy")
    def placed_flow() -> Promise[object]:
        return build(loop("retrying", _loop_iter_body, initial=None, max_iterations=3))

    app.get("placed-flow")  # the registration door (the D1 write)

    # THE DEFINITION'S PLACEMENT (the escalation rows' own address).
    definition = get_registry().get("placed-flow")
    assert (definition.actor, definition.queue) == ("wf-x", "heavy")

    # THE LOOP'S FALLBACK PLACEMENT (the loop node's own rows).
    node = app.get("placed-flow").nodes["retrying"]
    assert (node.actor, node.queue) == ("wf-x", "heavy")

    # THE ESCALATION ROWS' BINDINGS (D1 — the enqueue's own face).
    bindings = escalation_bindings(
        "placed-flow", "retrying", flow_id=1, error_class=None, message=None
    )
    assert bindings is not None
    assert bindings["actor"] == "wf-x" and bindings["queue"] == "heavy"

    # THE PROJECTION reads the WorkflowDef's placement: the cohort
    # projects (the escalation rows' dispatch capacity exists).
    rows = {(c.actor, c.queue) for c in project_workflow_actor_configs()}
    assert ("wf-x", "heavy") in rows, f"the declared escalation cohort projects: {rows}"
    assert ("workflow", "default") not in rows, "the default cohort is gone for the adopter"


def test_the_workflow_placement_defaults_stay() -> None:
    """C6's non-adopter law: NO placement declared — the definition's
    placement stays the CURRENT values ("workflow"/"default"), the loop
    node's fallback stays ("wf", "default"): no behavior change."""
    from taskq.workflows.definitions import get_registry

    app = WorkflowApp()

    @app.workflow("unplaced-flow")
    def unplaced_flow() -> Promise[object]:
        return build(loop("retrying", _loop_iter_body, initial=None, max_iterations=3))

    app.get("unplaced-flow")
    definition = get_registry().get("unplaced-flow")
    assert (definition.actor, definition.queue) == ("workflow", "default")
    node = app.get("unplaced-flow").nodes["retrying"]
    assert (node.actor, node.queue) == ("wf", "default")


def test_the_workflow_placement_joins_the_one_queue_law() -> None:
    """C6's validation: the declared placement joins the SAME one-queue
    law the node cohorts keep — a node on the declared actor's name at a
    DIFFERENT queue is the split placement, convicted by the projection's
    own law (the workflow skips LOUDLY, the conflict naming the actor and
    both queues; nothing of it projects). The cure the error names:
    distinct actor names per queue."""
    import structlog
    import structlog.testing

    from taskq.workflows._worker_execution import project_workflow_actor_configs

    app = WorkflowApp()

    @app.workflow("split-placement-flow", actor="wf-split", queue="heavy")
    def split_placement_flow() -> Promise[object]:
        # THE ESCALATING LOOP registers the escalation body — the
        # definition's declared placement is LIVE (the escalation rows'
        # address); the node on the same actor at the DEFAULT queue is
        # the split placement the law refuses.
        sink(loop("retrying", _loop_iter_body, initial=None, max_iterations=3))
        one = step(_plain_body, Ingest(doc_id="d"), key="one", actor="wf-split")  # "default"
        return build(one)

    app.get("split-placement-flow")  # the registration door (the D1 write)
    with structlog.testing.capture_logs() as logs:
        rows = project_workflow_actor_configs()
    skipped = [e for e in logs if e.get("event") == "workflow-projection-skipped"]
    assert len(skipped) == 1, logs
    assert "WorkflowActorQueueConflictError" in str(skipped[0].get("error_class")), skipped[0]
    assert "wf-split" in str(skipped[0].get("error")), skipped[0]
    assert "heavy" in str(skipped[0].get("error")) and "default" in str(skipped[0].get("error")), (
        "BOTH queues are named (the split placement's own faces)"
    )
    # NOTHING of the split workflow projects — the rows would strand.
    assert all(c.actor != "wf-split" for c in rows), rows


def test_the_workflow_placement_refuses_non_string_values() -> None:
    """C6's declaration-site guard: a non-str (or empty) actor=/queue= is
    the refused declaration — the placement names a real actor and
    queue, never a silent anything."""
    app = WorkflowApp()
    with pytest.raises(ValueError, match="actor"):
        app.workflow("bad-actor-flow", actor=3)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="queue"):
        app.workflow("bad-queue-flow", queue="")  # type: ignore[arg-type]


# ── (r3) the collects' ONE base — the deleted workflow's ghost ───────────


def _declare_the_doomed_app() -> None:
    """Declare the doomed app in THIS frame: the projection records its
    compile (the D1 cache), and on return the app object itself dies
    (the registry is weak) — its graph survives in ``_compiled`` as the
    ORPHAN the round-3 conviction names (the cache has no eviction)."""
    from taskq.ratelimit.token_bucket import TokenBucket

    doomed = WorkflowApp()

    @doomed.workflow("ghost-wf")
    def ghost_wf() -> Promise[object]:
        return build(
            step(
                _plain_body,
                Ingest(doc_id="d"),
                key="g",
                rate_limits=[TokenBucket("ghost-bucket", 5, 1.0)],
                reservations=[ConcurrencyReservation(name="ghost-guard", slots=1, lease=30.0)],
            )
        )

    project_workflow_actor_configs()


def test_the_dropped_workflows_ghost_stops_registering_the_boot_stays_clean() -> None:
    """R3-THE SIBLINGS' ONE BASE: a workflow deleted from its app
    in-process (the reload/redeploy story) leaves its compiled graph in
    the ``_compiled`` cache FOREVER (no eviction) — the rate-limit
    collect's old walk of that dict registered the ORPHAN's buckets
    beside the live estate's, and a surviving workflow re-declaring the
    same bucket name with a different config raised the registry's
    conflict refusal at the unguarded register: THE GHOST REFUSED THE
    BOOT. The cure: BOTH collects walk the LIVE apps' workflow names
    (the reservations collect's own base) — the orphan's terms stop
    registering, and the same-bucket-different-config re-declaration
    boots clean."""
    import gc

    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.ratelimit.token_bucket import TokenBucket
    from taskq.workflows._worker_execution import (
        collect_workflow_rate_limits,
        collect_workflow_reservations,
    )

    _declare_the_doomed_app()
    gc.collect()  # the doomed app's refcount is zero — the WeakSet drops it

    # THE REDEPLOY: a LIVE app re-declares the same BUCKET name with a
    # different config (and the same reservation name, differently
    # sized) on a workflow that survived the reload.
    live_app = WorkflowApp()

    @live_app.workflow("live-wf")
    def live_wf() -> Promise[object]:
        return build(
            step(
                _plain_body,
                Ingest(doc_id="d"),
                key="g",
                rate_limits=[TokenBucket("ghost-bucket", 9, 1.0)],
                reservations=[ConcurrencyReservation(name="ghost-guard", slots=3, lease=30.0)],
            )
        )

    project_workflow_actor_configs()

    # THE RATE-LIMIT COLLECT: no ghost refusal — the LIVE config is what
    # registers (pre-fix red: the orphan's capacity-5 bucket registered
    # first, the live capacity-9 raised the conflict ValueError).
    registry = RateLimitRegistry()
    registered, _unknown = collect_workflow_rate_limits(registry)
    assert registered == ["ghost-bucket"], registered
    live_bucket = registry.get_rate_limit("ghost-bucket")
    assert isinstance(live_bucket, TokenBucket) and live_bucket.capacity == 9, (
        "the LIVE config registered — the orphan's terms never reached the registry"
    )

    # THE RESERVATIONS COLLECT: the ghost's guard is not gathered either
    # (the same base — the sibling cannot drift).
    reservations, _res_unknown = collect_workflow_reservations()
    assert [r.name for r in reservations] == ["ghost-guard"], reservations
    assert reservations[0].slots == 3, "the LIVE slots, never the ghost's"


def test_the_rate_limit_conflict_refusal_names_the_buckets_not_the_reprs() -> None:
    """R3-THE CONFLICT'S RENDER (the reservation face's own fix,
    mirrored): two LIVE workflows declaring one bucket name with
    different configs is the refused defect (the registry's own
    duplicate-name law) — but the refusal names the RESOLVED BUCKET and
    the differing config FIELDS, never the raw ``__slots__`` reprs the
    registry's message carries (``object at 0x…`` is an address, not a
    diagnosis)."""
    from taskq.ratelimit.registry import RateLimitRegistry
    from taskq.ratelimit.token_bucket import TokenBucket
    from taskq.workflows._worker_execution import collect_workflow_rate_limits

    app_a = WorkflowApp()

    @app_a.workflow("conflict-a")
    def conflict_a() -> Promise[object]:
        return build(
            step(
                _plain_body,
                Ingest(doc_id="d"),
                key="a",
                rate_limits=[TokenBucket("clash-bucket", 5, 1.0)],
            )
        )

    app_b = WorkflowApp()

    @app_b.workflow("conflict-b")
    def conflict_b() -> Promise[object]:
        return build(
            step(
                _plain_body,
                Ingest(doc_id="d"),
                key="b",
                rate_limits=[TokenBucket("clash-bucket", 9, 1.0)],
            )
        )

    project_workflow_actor_configs()
    with pytest.raises(ValueError, match="clash-bucket") as exc_info:
        collect_workflow_rate_limits(RateLimitRegistry())
    message = str(exc_info.value)
    assert "object at 0x" not in message, f"the refusal dumps instances, not names: {message}"
    assert "capacity" in message and "5" in message and "9" in message, (
        f"the differing config FIELDS are named: {message}"
    )


# ── (r3) the park stamp's one truth ──────────────────────────────────────


def test_the_park_stamp_clears_the_stale_denied_bucket() -> None:
    """R3-THE PARK'S ONE TRUTH: a row denied on bucket X (the denial's
    snooze stamped ``released_bucket``) that is THEN fail-closed-parked
    (an unknown bucket, a malformed metadata shape — the same row
    re-claimed) must not read BOTH stamps: the snooze statement's
    metadata merge is jsonb concatenation (``metadata || update``), so
    the park's shape CLEARS ``released_bucket`` (None — jsonb null
    overwrites) and the row reads one truth: parked, with the arm's own
    kind. Pre-fix red: the merged row still carried the denial's bucket
    beside ``flow-rate-limit-parked`` — the trail self-contradicted."""
    from datetime import timedelta

    from taskq.exceptions import ReservationUnavailable
    from taskq.worker.run import (  # pyright: ignore[reportPrivateUsage]  # Why: the stamp helper is the unit under test.
        _flow_denial_snooze_metadata,
    )

    denial = _flow_denial_snooze_metadata(
        ReservationUnavailable(
            bucket_name="guard-x",
            retry_after=timedelta(seconds=1),
            source="rate_limit",
        )
    )
    assert denial["released_reason"] == "flow-rate-limit-denied"
    assert denial["released_bucket"] == "guard-x"

    # THE ROW after the denial's snooze (the merge's left side), then
    # the re-claim's fail-closed PARK: the statement merges
    # ``metadata || update`` (postgres.py's mark_snoozed) — dict |
    # dict is that merge, in Python.
    row_metadata: dict[str, object] = dict(denial)
    park = _flow_denial_snooze_metadata(None, parked_kind="flow-reservation-unknown-bucket")
    merged = row_metadata | park
    assert merged["released_reason"] == "flow-rate-limit-parked", merged
    assert merged["released_bucket"] is None, (
        f"THE STALE STAMP: the park kept the denial's bucket — {merged}"
    )
    assert merged["parked"] == "flow-reservation-unknown-bucket", merged
