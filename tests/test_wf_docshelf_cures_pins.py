# Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.
"""THE DOCSHELF ADOPTION BLOCKERS' CURES (the engine-cures lane) — the
adoption team's verdict was "Fix F1 + F4 + F2 and we would ship without
hesitation"; F4 (the connection-hold) and F1 (the ladder's inheritance)
re-probed against this branch are the HOLDMAP LANE's territory (the
verdicts ride this lane's report); the REMAINING cures are HERE, each
red-first (the pin convicted against the base head ``0e2f5cc09`` before
its cure landed — the red receipts are this lane's captures):

* **F2** — THE ROUTE ARMS' COHORTS NEVER PROJECTED AT BOOT: the arms
  fork at RUNTIME (the source's finalize stamps each arm's effective
  placement), and the boot projection's cohort walk read the DECLARED
  nodes only — an arm's rows stamped ``(actor, queue)`` pairs no
  ``ActorConfig`` carried: the rows STRANDED SILENTLY (no dispatch
  capacity, the strict-queue worker never listened) until the app
  provisioned the cohorts itself. THE CURE: the boot projection's walk
  extended to the arms' effective pairs
  (``_workflow_cohorts`` — the runner's ``_route_fork`` arithmetic, one
  vocabulary); a boot that strands silently is the defect.
* **F3** — THE DERIVED JOIN'S COHORT CONFLICT REPORTED AT VALIDATE:
  ``route()``'s derived join inherits the source's ACTOR and takes the
  route's ``queue=`` (default ``default``) — a source placed on a
  non-default queue split its actor over two queues, the conflict
  discovered ONLY at the worker's boot, ``validate()`` clean. THE CURE
  (``W6-actor-queue-cohort``, the W2 precedent's severity — the wiring
  warns, the boot projection's one-queue law stays the refusal door,
  because the split placement under one actor is also the taught
  heterogeneous shape): the ambush reported at the wiring — the actor,
  both queues, and the distinct-actor remedy named; the map's EXPLICIT
  ``queue=`` knob (the doc-ingest guide's Shape-4 wiring) stays silent.
* **F6** — THE FAN-IN BOUND'S ESCAPE EXPOSED ON THE MAP FACE: the
  1000-parent bound is real (the docshelf's ``FanInBoundExceededError``
  at 2335 chunks) and the child-driven escape was reachable only through
  the raw ``JoinSpec`` — the authoring verbs exposed nothing. THE CURE:
  ``child_driven=`` on ``map_source`` / ``route`` (one machinery, both
  spellings), threaded to the fork's ``JoinSpec`` — and the guide's
  fan-in section teaches it.
* **F9** — THE JOIN'S DECODE FACE, PINNED HONEST: the re-probe against
  this branch found the decode ALREADY HONEST at both join faces (the
  route barrier and the multi-parent fan-in — the declared models
  decoded, the docshelf's raw-dict face not reproducible through the
  shared tail with resolvable module-scope annotations). Per the
  doctrine, the honesty is HELD: this pin asserts the join body receives
  the DECLARED models — the silent type-mismatch is what's forbidden,
  and the pin convicts it the day any face regresses to raw dicts.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    map_source,
    route,
    step,
)
from taskq.workflows import _worker_execution as seam
from taskq.workflows.api._validate import validate_compiled

# ── the shared bodies (module scope — the annotations must resolve) ──────


class Image(BaseModel):
    v: int


class Audio(BaseModel):
    v: int


class Report(BaseModel):
    ref: str


async def _src_body(ctx: StepContext) -> list[Image | Audio]:
    return [Image(v=1), Audio(v=2)]


async def _arm_image(ctx: Any, item: Image) -> Report:
    return Report(ref="img")


async def _arm_audio(ctx: Any, item: Audio) -> Report:
    return Report(ref="aud")


# ── F2: the arms' cohorts project at boot ────────────────────────────────


def test_f2_the_route_arms_forked_cohorts_project_at_boot() -> None:
    """THE ARMS' ROWS ARE CLAIMABLE ROWS: a route whose arms declare
    their own placements (the per-arm queue, the arm's DISTINCT actor —
    the one-queue law's remedy) must see BOTH arms' forked cohorts in
    the boot projection, beside the source's own and the derived join's.
    The pre-cure walk read the declared nodes only: the arms' cohorts
    were absent — the arm rows stranded silently until the app
    provisioned the cohorts itself (the docshelf's workaround)."""
    seam.reset_app_registry_for_tests()
    try:
        app = WorkflowApp()

        @app.workflow("dsc_f2_arm_cohorts")
        def dsc_f2() -> Promise[object]:
            source = step(_src_body, key="src")
            routed = route(
                source,
                {
                    Image: RouteArm(body=_arm_image, actor="wf-cpu", queue="cpu"),
                    Audio: RouteArm(body=_arm_audio, actor="wf-gpu", queue="gpu"),
                },
            )
            return build(routed)

        configs = [(c.actor, c.queue) for c in seam.project_workflow_actor_configs()]
        # THE SOURCE'S OWN + THE DERIVED JOIN'S cohorts (the declared
        # nodes) — and THE ARMS' FORKED cohorts (the cure's subjects).
        assert ("wf", "default") in configs, (
            f"the source's own cohort vanished from the projection: {configs}"
        )
        assert ("wf-cpu", "cpu") in configs, (
            "F2: the cpu arm's forked cohort is ABSENT from the boot "
            f"projection — the arm's rows strand silently (the pre-cure "
            f"walk read the declared nodes only): {configs}"
        )
        assert ("wf-gpu", "gpu") in configs, (
            "F2: the gpu arm's forked cohort is ABSENT from the boot "
            f"projection — the arm's rows strand silently: {configs}"
        )
    finally:
        seam.reset_app_registry_for_tests()


def test_f2_the_conflicting_arm_shape_skips_loud_never_silent() -> None:
    """THE OTHER HONEST FACE (the pin's complement): an arm spelling a
    SECOND queue on the source's OWN actor (the one-queue-law breach)
    must go LOUD at boot — the workflow's cohorts project NOTHING and
    the defect is named (the projection-skipped arm) — never the silent
    stranding the pre-cure boot shipped. The projection does NOT raise
    for a workflow's OWN conflict (the isolation law — one broken build
    fn must not refuse the worker's whole boot); the READ is: no cohort
    of the broken workflow projects."""
    seam.reset_app_registry_for_tests()
    try:
        app = WorkflowApp()

        @app.workflow("dsc_f2_conflict")
        def dsc_f2_conflict() -> Promise[object]:
            source = step(_src_body, key="src")
            routed = route(
                source,
                {
                    Image: RouteArm(body=_arm_image, queue="gpu"),
                    Audio: RouteArm(body=_arm_audio, queue="gpu"),
                },
            )
            return build(routed)

        configs = [(c.actor, c.queue) for c in seam.project_workflow_actor_configs()]
        assert all(actor != "wf" for actor, _queue in configs), (
            "the conflicting workflow's cohorts projected NOTHING (the "
            f"skip-loud arm) — found {configs}"
        )
    finally:
        seam.reset_app_registry_for_tests()


# ── F3: the derived join's cohort conflict convicted at validate ─────────


def test_f3_the_derived_joins_cohort_conflict_is_convicted_at_validate() -> None:
    """THE CONFLICT AT THE WIRING, NOT AT THE WORKER'S 3AM: a route's
    source placed on a NON-DEFAULT queue — the derived join inherits the
    source's actor and takes the route's ``queue=`` (default
    ``default``): one actor over two queues, and the pre-cure
    ``validate()`` was CLEAN — the conflict surfaced only at the
    worker's boot (the projection's cohort refusal). THE CURE (W6): the
    same law is REPORTED at validate — the diagnostic names the actor,
    both queues, and the distinct-actor remedy (the W2 precedent's
    severity: the wiring warns, the boot projection's one-queue law
    stays the refusal door — the estate's own taught heterogeneous
    placement under one actor is the shape the dev-loop door honors)."""
    app = WorkflowApp()

    @app.workflow("dsc_f3_join_conflict")
    def dsc_f3() -> Promise[object]:
        source = step(_src_body, key="src", actor="wf-gpu", queue="gpu")
        routed = route(
            source,
            {Image: RouteArm(body=_arm_image), Audio: RouteArm(body=_arm_audio)},
        )
        return build(routed)

    diagnostics = validate_compiled(app.get("dsc_f3_join_conflict"))
    cohort = [d for d in diagnostics if d.rule == "W6-actor-queue-cohort"]
    assert cohort, (
        "F3: the derived join's cohort conflict is INVISIBLE at validate — "
        "the pre-cure law left the ambush to the worker's boot "
        f"(the diagnostics: {[d.rule for d in diagnostics]})"
    )
    message = cohort[0].message
    assert "'wf-gpu'" in message, message
    assert "gpu" in message and "default" in message, message


def test_f3_the_taught_map_knobs_split_stays_silent() -> None:
    """THE MAP'S EXPLICIT KNOB IS NOT THE AMBUSH: the plain map_source's
    ``queue=`` is the taught Shape-4 placement (the doc-ingest guide's
    own wiring — the item children on 'enrich' under the source's
    default-queue actor): an EXPLICIT author choice, never the derived
    join's un-spelled default. Its split stays silent at validate (the
    boot projection's one-queue law remains that face's door) — the
    zero-warning budget the examples pin holds."""
    app = WorkflowApp()

    @app.workflow("dsc_f3_map_knob")
    def dsc_f3_map() -> Promise[object]:
        source = step(_src_body, key="src")
        mapped = map_source(source, _arm_image, queue="enrich")
        return build(mapped)

    diagnostics = validate_compiled(app.get("dsc_f3_map_knob"))
    assert not any(d.rule == "W6-actor-queue-cohort" for d in diagnostics), (
        f"F3: the map's TAUGHT explicit queue split was reported — the "
        f"knob is the doc-ingest guide's own wiring: "
        f"{[d.message for d in diagnostics if d.rule == 'W6-actor-queue-cohort']}"
    )


def test_f3_the_taught_heterogeneous_shape_stays_clean_at_the_error_severity() -> None:
    """THE SEVERITY'S GUARD (the doc-ingest guide's own wiring): the
    heterogeneous placement under ONE actor (the map's children on
    'enrich', the downstream steps on 'cpu'/'io') is the estate's
    TAUGHT shape — the dev-loop door honors it, so the validate must
    never REFUSE it: the validate() raising face stays clean (the
    zero-warnings budget the examples pin), and the boot projection's
    one-queue law remains the fleet's refusal door."""
    app = WorkflowApp()

    @app.workflow("dsc_f3_taught_split")
    def dsc_f3_taught() -> Promise[object]:
        source = step(_src_body, key="src")
        mapped = map_source(source, _arm_image, queue="enrich")
        downstream = step(_f9_barrier_body, mapped, key="downstream", queue="cpu")
        return build(downstream)

    compiled_taught = app.get("dsc_f3_taught_split")
    diagnostics = validate_compiled(compiled_taught)
    cohort = [d for d in diagnostics if d.rule == "W6-actor-queue-cohort"]
    assert not cohort, (
        f"the taught heterogeneous wiring was reported (the map's explicit "
        f"knob is not the ambush): {[d.message for d in cohort]}"
    )
    compiled_taught.validate()  # the raising face: no error raises


# ── F6: the fan-in bound's escape exposed on the map face ────────────────


async def _big_map_child(ctx: Any, item: Image) -> Report:
    return Report(ref=f"i{item.v}")


async def _big_source(ctx: StepContext) -> list[Image]:
    return [Image(v=i) for i in range(1001)]  # one past the declared bound


def test_f6_the_child_driven_escape_is_exposed_on_the_map_face() -> None:
    """THE ESCAPE AT THE AUTHORING VERB: ``map_source(src, body,
    child_driven=True)`` (and route()'s own spelling — one machinery)
    compiles the CHILD-DRIVEN join shape — the compiled node carries the
    declaration, so the fork's ``JoinSpec`` admits a corpus past
    MAX_FAN_IN_PER_JOIN (the docshelf's 2335-chunk conviction) that the
    default shape REFUSES. The witness is the compile face (the declared
    field rides the node); the bound's own refusals are the fan-in
    pins' (test_wf_fanin_bound_pins)."""
    app = WorkflowApp()

    @app.workflow("dsc_f6_child_driven")
    def dsc_f6() -> Promise[object]:
        source = step(_big_source, key="big")
        mapped = map_source(source, _big_map_child, child_driven=True)
        return build(mapped)

    compiled = app.get("dsc_f6_child_driven")
    assert compiled.nodes["big"].map_child_driven is True, (
        "F6: the child-driven escape is NOT exposed on map_source — the "
        "param never landed on the verb (the escape reachable only through "
        "the raw JoinSpec, the authoring face silent)"
    )

    # THE DEFAULT STAYS THE BOUNDED SHAPE.
    app2 = WorkflowApp()

    @app2.workflow("dsc_f6_default")
    def dsc_f6_default() -> Promise[object]:
        source = step(_big_source, key="big")
        mapped = map_source(source, _big_map_child)
        return build(mapped)

    compiled2 = app2.get("dsc_f6_default")
    assert compiled2.nodes["big"].map_child_driven is False


def test_f6_the_child_driven_escape_is_exposed_on_route() -> None:
    """The route's own spelling (the dict form IS map_source — one
    machinery): ``route(source, arms, child_driven=True)`` carries the
    same declaration to the SAME join."""
    app = WorkflowApp()

    @app.workflow("dsc_f6_route_cd")
    def dsc_f6_route() -> Promise[object]:
        source = step(_src_body, key="src")
        routed = route(
            source,
            {Image: RouteArm(body=_arm_image), Audio: RouteArm(body=_arm_audio)},
            child_driven=True,
        )
        return build(routed)

    compiled = app.get("dsc_f6_route_cd")
    assert compiled.nodes["src"].map_child_driven is True, (
        "F6: the child-driven escape is NOT exposed on route() — the "
        "route's join admits no escape at the authoring face"
    )


# ── F9: the join's decode face, pinned honest ────────────────────────────

_F9_SEEN: dict[str, list[object]] = {}


async def _f9_source(ctx: StepContext) -> list[Image | Audio]:
    return [Image(v=1), Audio(v=2)]


async def _f9_join_body(ctx: Any, a: Image, b: Audio) -> dict[str, int]:
    _F9_SEEN["join"] = [a, b]
    return {"v": a.v + b.v}


async def _f9_barrier_body(ctx: Any, items: list[Report]) -> int:
    _F9_SEEN["barrier"] = list(items)
    return len(items)


async def test_f9_the_multi_parent_join_body_receives_the_declared_models(
    wf_pool: Any, wf_schema: str
) -> None:
    """THE DECODE FACE AT THE JOIN, HELD: a multi-parent fan-in's body —
    the step consuming SEVERAL promises — receives the parents' results
    DECODED INTO THE DECLARED MODELS (the same R3 face the rest of the
    machinery honors), never the jsonb round-trip's raw dicts. The
    docshelf team's F9 observed raw dicts at this face; the re-probe
    against this branch found the decode HONEST (the docshelf face not
    reproducible through the shared tail with resolvable annotations) —
    this pin HOLDS the honesty permanently: a face that regresses to
    raw dicts reds here, loudly."""
    _F9_SEEN.clear()
    app = WorkflowApp()

    @app.workflow("dsc_f9_join_decode")
    def dsc_f9() -> Promise[object]:
        pa = step(_mk_image, key="pa")
        pb = step(_mk_audio, key="pb")
        return build(step(_f9_join_body, pa, pb, key="joined"))

    runner = FlowRunner(app.get("dsc_f9_join_decode"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    seen = _F9_SEEN.get("join")
    assert seen is not None, "the join body never ran"
    types = [type(x).__name__ for x in seen]
    assert all(isinstance(x, Image | Audio) for x in seen), (
        f"F9: the multi-parent join's body received RAW values ({types}) — "
        "the declared models' decode face regressed (the silent "
        "type-mismatch is what's forbidden)"
    )
    assert isinstance(seen[0], Image) and isinstance(seen[1], Audio), (
        f"F9: the parents arrived SWAPPED or mistyped: {types}"
    )


async def test_f9_the_route_barrier_decodes_the_declared_sum(wf_pool: Any, wf_schema: str) -> None:
    """THE BARRIER'S DECODE (the docshelf's digest face): the route
    join's consumer — its ``list[Sum]`` param — receives the DECLARED
    models with attribute access, never dicts (the guide's own
    teaching, now held by a pin)."""
    _F9_SEEN.clear()
    app = WorkflowApp()

    @app.workflow("dsc_f9_barrier_decode")
    def dsc_f9_barrier() -> Promise[object]:
        source = step(_f9_source, key="source")
        routed = route(
            source,
            {Image: RouteArm(body=_arm_image), Audio: RouteArm(body=_arm_audio)},
        )
        return build(step(_f9_barrier_body, routed, key="barrier"))

    runner = FlowRunner(app.get("dsc_f9_barrier_decode"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    seen = _F9_SEEN.get("barrier")
    assert seen is not None, "the barrier body never ran"
    assert len(seen) == 2
    assert all(isinstance(x, Report) for x in seen), (
        f"F9: the route barrier's body received {[type(x).__name__ for x in seen]} — "
        "the declared sum's decode face regressed to raw dicts"
    )


async def _mk_image(ctx: StepContext) -> Image:
    return Image(v=7)


async def _mk_audio(ctx: StepContext) -> Audio:
    return Audio(v=9)
