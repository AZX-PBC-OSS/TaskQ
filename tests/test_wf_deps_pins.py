"""THE DEPS SEAM'S PINS (the DI capability — SAI migration blocker #1).

The seam's law: bodies need dependencies WITHOUT reaching through ``ctx``
or module globals. A ``Deps`` dataclass is declared PER APP and bound ONE
instance at the door — ``WorkflowApp(deps=…)`` (the authoring door) or
``FlowRunner(…, deps=…)`` (the direct door; ``run(…, deps=…)`` inherits
the compiled's binding). A step body OPTS IN by declaring one parameter
beyond ``ctx`` + the wired sources — the SAME arity discipline E10 owns,
extended honestly: the extra parameter receives the app's bound deps
instance, and ``validate()``'s E12-deps-contract rule checks the
DECLARATION against the app's binding at build:

* a body declaring the deps shape where the app binds NONE is the E12
  refusal (the message names the fix — bind ``deps=`` or fix the arity);
* a body whose deps annotation the bound instance does not SATISFY is
  the E12 refusal (both type names in the message);
* a true arity mismatch (two params beyond the wiring, no deps shape)
  stays E10's.

The bound instance is THE instance: the step bodies, the map's item
children, the loop driver's bodies, and the hold-wake's re-execution all
receive the SAME object (bound once at the door, never re-minted per
claim). The runner never inspects it — ``object`` through the runner, the
TYPING proven at the decoration (E12's isinstance), no ``Any``, no
getattr-strings.

THE LANDMINE (named, test-scope): the InMemoryBackend has NO workflow
execution surface — the deps seam's test face is the REAL ``FlowRunner``
against the pg fixtures (the zero-workflow-surfaces fact stays named here
so a future in-memory workflow runner inherits the seam's law, not a
second mechanism).
"""

from __future__ import annotations

from dataclasses import dataclass

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.workflows import (
    Done,
    FlowRunner,
    GateDecl,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
    map_source,
    run,
    step,
)
from taskq.workflows.api._validate import WorkflowValidationError


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class Carry(BaseModel):
    n: int = 0


@dataclass(frozen=True, slots=True)
class EnrichDeps:
    """THE APP'S DECLARED DEPS (the seam's shape): one instance, bound at
    the door — the demo's enrich-queue client stand-in."""

    marker: str = "bound-once"


@dataclass(frozen=True, slots=True)
class OtherDeps:
    """A DIFFERENT deps type (the mismatch probe's subject)."""


SEEN: list[object] = []
"""The deps instances the bodies observed, in execution order."""


async def _items_source(ctx: StepContext, params: Ingest) -> list[str]:
    return ["a", "b"]


async def _deps_body(ctx: StepContext, params: Ingest, deps: EnrichDeps) -> Report:
    SEEN.append(deps)
    return Report(ref=f"{params.doc_id}:{deps.marker}")


async def _deps_item(ctx: StepContext, doc_id: str, deps: EnrichDeps) -> Report:
    SEEN.append(deps)
    return Report(ref=f"{doc_id}:{deps.marker}")


async def _deps_loop(
    ctx: StepContext, carry: Carry, deps: EnrichDeps
) -> Done[Report] | Refine[Carry]:
    SEEN.append(deps)
    if carry.n >= 1:
        return Done(Report(ref=f"loop:{deps.marker}"))
    return Refine(Carry(n=carry.n + 1))


# ── the happy path: the deps instance flows, typed, through the door ────


async def test_happy_path_di_the_bound_instance_flows(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A step body declaring ``(ctx, params, deps)`` receives the app's
    bound instance — THE instance (identity), never a copy, never a
    re-mint — and the run terminals with the body's typed result."""
    SEEN.clear()
    deps = EnrichDeps(marker="happy")
    app = WorkflowApp(deps=deps)

    @app.workflow("deps_happy_flow")
    def deps_happy_flow() -> Promise[Report]:
        a = step(_deps_body, Ingest(doc_id="d1"), key="a")
        return build(a)

    runner = FlowRunner(app.get("deps_happy_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert await runner.result(flow_id) == {"ref": "d1:happy"}
    assert [deps] == SEEN


async def test_map_child_di_the_same_instance(wf_schema: str, wf_pool: asyncpg.Pool) -> None:
    """The map's item children receive the SAME deps instance (the app's,
    bound once) — every child, every re-run."""
    SEEN.clear()
    deps = EnrichDeps(marker="map")
    app = WorkflowApp(deps=deps)

    @app.workflow("deps_map_flow")
    def deps_map_flow() -> Promise[object]:
        src = step(_items_source, Ingest(doc_id="d1"), key="src")
        mapped = map_source(src, _deps_item)
        del mapped
        return build(step(_deps_body, Ingest(doc_id="d1"), key="tail"))

    runner = FlowRunner(app.get("deps_map_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert len(SEEN) >= 3  # two children + the tail
    assert all(d is deps for d in SEEN)


async def test_loop_di_the_same_instance(wf_schema: str, wf_pool: asyncpg.Pool) -> None:
    """The loop driver's body receives the SAME deps instance — every
    iteration."""
    SEEN.clear()
    deps = EnrichDeps(marker="loop")
    app = WorkflowApp(deps=deps)

    @app.workflow("deps_loop_flow")
    def deps_loop_flow() -> Promise[object]:
        return build(loop("it", _deps_loop, initial=Carry(n=0), max_iterations=5))

    runner = FlowRunner(app.get("deps_loop_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert len(SEEN) == 2 and all(d is deps for d in SEEN)


async def test_hold_wake_di_the_same_instance(wf_schema: str, wf_pool: asyncpg.Pool) -> None:
    """The hold's resume RE-EXECUTES the body from the top — the wake's
    execution receives the SAME deps instance the first attempt saw."""

    class Approval(BaseModel):
        verdict: str

    from taskq.workflows.api._hitl import HitlClient

    SEEN.clear()
    deps = EnrichDeps(marker="hold")

    app = WorkflowApp(deps=deps)

    @app.workflow("deps_hold_flow")
    def deps_hold_flow() -> Promise[object]:
        async def review(ctx: StepContext, params: Ingest, d: EnrichDeps) -> object:
            SEEN.append(d)
            if len(SEEN) > 1:
                return Report(ref=f"woke:{d.marker}")
            return await ctx.wait_signal(Approval, timeout_s=120.0)

        return build(
            step(
                review,
                Ingest(doc_id="d1"),
                key="review",
                gates=(GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0),),
            )
        )

    runner = FlowRunner(app.get("deps_hold_flow"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, until="held") == "held"
    client = HitlClient(wf_pool, schema=wf_schema)
    holds = await client.list(flow_id)
    assert holds
    resolved = await client.resolve(holds[0].hold_id, {"verdict": "approve"})
    assert resolved.status == "delivered"
    # the wake re-executed the body with the SAME instance — and the
    # wake's Report terminalizes the run (drive to terminal).
    assert await runner.drive(flow_id) == "terminal"
    assert [deps, deps] == SEEN


# ── the E12 refusals (the build-time convictions) ────────────────────────


def test_e12_refuses_deps_declared_where_the_app_binds_none() -> None:
    """A body declaring the deps shape where the app binds NO deps is
    the E12 refusal AT BUILD — the message names the fix."""
    app = WorkflowApp()  # no deps bound

    @app.workflow("deps_orphan_flow")
    def deps_orphan_flow() -> Promise[object]:
        a = step(_deps_body, Ingest(doc_id="d1"), key="a")
        return build(a)

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract") as excinfo:
        app.get("deps_orphan_flow")
    assert "deps=" in str(excinfo.value)


def test_e12_refuses_the_type_mismatch() -> None:
    """A body whose deps annotation the bound instance does not satisfy
    is the E12 refusal — both type names in the message."""

    async def mismatched(ctx: StepContext, params: Ingest, deps: OtherDeps) -> Report:
        return Report(ref=params.doc_id)

    app = WorkflowApp(deps=EnrichDeps(marker="m"))

    @app.workflow("deps_mismatch_flow")
    def deps_mismatch_flow() -> Promise[object]:
        a = step(mismatched, Ingest(doc_id="d1"), key="a")
        return build(a)

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract") as excinfo:
        app.get("deps_mismatch_flow")
    message = str(excinfo.value)
    assert "OtherDeps" in message and "EnrichDeps" in message


def test_e10_still_owns_the_true_arity_mismatch() -> None:
    """Two params beyond the wiring is NOT a deps shape — E10's refusal
    stands (the deps contract is ONE extra parameter, the last; an
    unsatisfied ONE-param-extra declaration is E12's mismatch, not
    E10's)."""

    async def too_many(ctx: StepContext, params: Ingest, extra: str, more: str) -> Report:
        del extra, more
        return Report(ref=params.doc_id)

    app = WorkflowApp(deps=EnrichDeps(marker="m"))

    @app.workflow("deps_arity_flow")
    def deps_arity_flow() -> Promise[object]:
        a = step(too_many, Ingest(doc_id="d1"), key="a")
        return build(a)

    with pytest.raises(WorkflowValidationError, match="E10-arity"):
        app.get("deps_arity_flow")


def test_e12_owns_the_unsatisfied_single_extra_param() -> None:
    """ONE param beyond the wiring whose declared type the bound
    instance does not satisfy — E12's mismatch (the deps shape read),
    never E10's count."""

    async def too_many(ctx: StepContext, params: Ingest, extra: str) -> Report:
        del extra
        return Report(ref=params.doc_id)

    app = WorkflowApp(deps=EnrichDeps(marker="m"))

    @app.workflow("deps_shape_flow")
    def deps_shape_flow() -> Promise[object]:
        a = step(too_many, Ingest(doc_id="d1"), key="a")
        return build(a)

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract"):
        app.get("deps_shape_flow")


def test_e12_map_and_loop_children_refused() -> None:
    """The deps contract walks the map's item bodies and the loop bodies
    too: a child declaring deps where the app binds none is E12."""
    app = WorkflowApp()

    @app.workflow("deps_child_orphan_flow")
    def deps_child_orphan_flow() -> Promise[object]:
        src = step(_items_source, Ingest(doc_id="d1"), key="src")
        mapped = map_source(src, _deps_item)
        del mapped
        return build(src)

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract"):
        app.get("deps_child_orphan_flow")

    app2 = WorkflowApp()

    @app2.workflow("deps_loop_orphan_flow")
    def deps_loop_orphan_flow() -> Promise[object]:
        return build(loop("it", _deps_loop, initial=Carry(n=0), max_iterations=2))

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract"):
        app2.get("deps_loop_orphan_flow")


# ── the runner door + the packaged run door ──────────────────────────────


async def test_runner_door_binds_deps_and_overrides_the_binding(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """``FlowRunner(…, deps=…)`` binds the instance at the runner's own
    door: the explicit binding WINS (the body sees the RUNNER's
    instance), and the validation re-runs against the effective binding
    (a runner-side binding on a compile carrying none upgrades the E12
    view — the direct-compile door). The APP door's refusal stands: a
    compile from ``get()`` that binds no deps never reaches a runner."""
    SEEN.clear()
    app_deps = EnrichDeps(marker="app")
    runner_deps = EnrichDeps(marker="runner")

    app = WorkflowApp(deps=app_deps)

    @app.workflow("deps_runner_door_flow")
    def deps_runner_door_flow() -> Promise[object]:
        a = step(_deps_body, Ingest(doc_id="d1"), key="a")
        return build(a)

    # WITHOUT any binding at the app: the E12 refusal at the get() door
    # (covered by its own pin) — the compile never reaches a runner.
    bare = WorkflowApp()

    @bare.workflow("deps_runner_bare_flow")
    def deps_runner_bare_flow() -> Promise[object]:
        a = step(_deps_body, Ingest(doc_id="d1"), key="a")
        return build(a)

    with pytest.raises(WorkflowValidationError, match="E12-deps-contract"):
        bare.get("deps_runner_bare_flow")

    # WITH the runner's explicit binding: the door's instance wins.
    runner = FlowRunner(app.get("deps_runner_door_flow"), wf_pool, wf_schema, deps=runner_deps)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    assert [runner_deps] == SEEN
    del app_deps


async def test_packaged_run_door_rides_the_compiled_binding(
    wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """``run(flow, pool, schema)`` — the deps ride the COMPILED binding
    (the app's, bound once): no explicit pass, the body still sees the
    instance."""
    SEEN.clear()
    deps = EnrichDeps(marker="packaged")
    app = WorkflowApp(deps=deps)

    @app.workflow("deps_packaged_flow")
    def deps_packaged_flow() -> Promise[Report]:
        a = step(_deps_body, Ingest(doc_id="d1"), key="a")
        return build(a)

    result = await run(app.get("deps_packaged_flow"), wf_pool, wf_schema)
    assert result.outcome == "terminal"
    assert result.result == {"ref": "d1:packaged"}
    assert [deps] == SEEN
