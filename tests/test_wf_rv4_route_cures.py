"""THE RV4 CURE LANE'S PINS (the recertifier's seven-conviction pack).

The routing spine certified; seven live convictions + the estate red;
this file carries the pins — red-first (each pin ran against the base
head ``94996174`` and CONVICTED before its cure landed; the red receipts
live beside the pins in ``.measurements/rv4-*.txt``), every cure WITH
its pin.

* **F1** — the empty-corpus wedge (``_runner.py``'s ``_route_fork``
  returns ``None`` for a legitimately-empty corpus: the join never
  fires, the flow wedges or lies). The cure: the empty list IS the
  typed sum's honest value — the join fires with ``[]``.
* **F8** — the >1000 fan-in bound's build seam (the named constant +
  the E-rule refusal AT BUILD; ``validate_fork``'s raise INTO the
  ladder's try).
* **THE ARM ARITY** — E10/E12's walk extended to the ``map_arms``.
* **E15'S "RELATED" DOOR** — the arm-param contract's exactness.
"""

# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.

from __future__ import annotations

import pytest
from pydantic import BaseModel

from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    WorkflowBuildError,
    build,
    gather,
    map_source,
    route,
    sink,
    step,
)
from taskq.workflows.api._validate import WorkflowValidationError


class ImageItem(BaseModel):
    doc_id: str


class AudioItem(BaseModel):
    doc_id: str


class ImageResult(BaseModel):
    doc_id: str


class AudioResult(BaseModel):
    doc_id: str


async def process_image(ctx: object, item: ImageItem) -> ImageResult:
    return ImageResult(doc_id=item.doc_id)


async def process_audio(ctx: object, item: AudioItem) -> AudioResult:
    return AudioResult(doc_id=item.doc_id)


# ── F1: the empty-corpus wedge ──────────────────────────────────────────


async def empty_corpus_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
    """The legitimately-empty nightly corpus: zero documents chunked."""
    return []


async def test_f1_the_empty_corpus_run_terminalizes_result_is_the_empty_list(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """F1's e2e: a route source whose body returns [] — the run
    TERMINALIZES, ``result() == []`` (the empty list IS the typed sum's
    honest value), and ZERO rows stick non-terminal (the wedge's row
    receipt)."""

    app = WorkflowApp()

    @app.workflow("rv4_f1_empty_corpus")
    def rv4_f1_empty_corpus() -> Promise[object]:
        src = step(empty_corpus_source, key="media")
        routed = route(
            src,
            {
                ImageItem: RouteArm(body=process_image, queue="gpu"),
                AudioItem: RouteArm(body=process_audio, queue="io"),
            },
        )
        return build(routed)  # the route's join IS the terminal: result() IS the typed sum

    runner = FlowRunner(app.get("rv4_f1_empty_corpus"), module_pg_pool, wf_schema)  # pyright: ignore[reportArgumentType]  # Why: the object-typed fixtures.
    flow_id = (await runner.create_flow()).flow_id
    verdict = await runner.drive(flow_id)
    assert verdict == "terminal", (
        f"the empty corpus WEDGED (drive returned {verdict!r} — the "
        "max-ticks bound, the stuck-run shape)"
    )
    result = await runner.result(flow_id)
    assert result == [], f"the empty corpus's result() must be [], got {result!r}"
    assert result is not None, "result() is None on a succeeded route — the lying empty read"
    # THE ROWS ARE THE RECEIPT: zero stuck rows — every row of the run
    # is terminal.
    rows = await wf_conn.fetch(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the object-typed fixture.
        f'SELECT step_key, status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    stuck = [dict(r) for r in rows if r["status"] not in ("succeeded", "failed", "cancelled")]  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the object-typed fixture's Record members.
    assert not stuck, f"stuck rows on the empty-corpus run: {stuck}"


async def test_f1_the_empty_map_corpus_fires_the_join_with_the_empty_list(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """F1's map face: the plain map_source over an empty corpus — the
    join fires with [], the run terminalizes, the collect states []."""

    async def tiny_source(ctx: StepContext) -> list[ImageItem]:
        return []

    async def per_item(ctx: object, item: ImageItem) -> ImageResult:
        return ImageResult(doc_id=item.doc_id)

    async def summarize(ctx: object, items: list[ImageResult]) -> dict[str, int]:
        return {"n": len(items)}

    app = WorkflowApp()

    @app.workflow("rv4_f1_empty_map")
    def rv4_f1_empty_map() -> Promise[object]:
        src = step(tiny_source, key="tiny")
        mapped = map_source(src, per_item)
        summary = step(summarize, mapped, key="summary")
        sink(mapped)
        return build(summary)

    runner = FlowRunner(app.get("rv4_f1_empty_map"), module_pg_pool, wf_schema)  # pyright: ignore[reportArgumentType]  # Why: the object-typed fixtures.
    flow_id = (await runner.create_flow()).flow_id
    verdict = await runner.drive(flow_id)
    assert verdict == "terminal", f"the empty MAP wedged (drive returned {verdict!r})"
    result = await runner.result(flow_id)
    assert result is not None, "result() is None on a succeeded empty map"
    rows = await wf_conn.fetch(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the object-typed fixture.
        f'SELECT step_key, status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1",
        flow_id,
    )
    stuck = [dict(r) for r in rows if r["status"] not in ("succeeded", "failed", "cancelled")]  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the object-typed fixture's Record members.
    assert not stuck, f"stuck rows on the empty-map run: {stuck}"


# ── F8: the >1000 fan-in bound ──────────────────────────────────────────


def test_f8_the_fan_in_bound_is_a_named_constant() -> None:
    """The fan-in bound is a NAMED constant the build refusal reads —
    the same law as max_in_flight's fence-on default."""
    from taskq.workflows.definitions import MAX_FAN_IN_PER_JOIN

    assert isinstance(MAX_FAN_IN_PER_JOIN, int)
    assert MAX_FAN_IN_PER_JOIN == 1000


def test_f8_the_fan_in_refusal_at_build_names_the_bound_and_the_escape() -> None:
    """The >1000 fan-in refusal fires AT BUILD (the wiring verb's door —
    E-rule), the message names the bound AND the child_driven escape —
    the remedy reachable from the refusal's own text."""

    app = WorkflowApp()

    async def leaf(ctx: object) -> int:
        return 1

    @app.workflow("rv4_f8_fan_in")
    def rv4_f8_fan_in() -> Promise[object]:
        sources = [step(leaf, key=f"leaf_{i}") for i in range(1001)]
        joined = gather(sources)
        return build(joined)

    with pytest.raises(WorkflowValidationError) as exc_info:
        app.get("rv4_f8_fan_in")
    message = str(exc_info.value)
    assert "1000" in message, f"the refusal must name the bound: {message}"
    assert "child_driven" in message, f"the refusal must name the escape: {message}"


# ── the arm arity (E10/E12's walk extended to the map_arms) ─────────────


async def zero_param_arm(ctx: object) -> ImageResult:
    return ImageResult(doc_id="zero")


async def two_param_arm(ctx: object, item: ImageItem, extra: object) -> ImageResult:
    return ImageResult(doc_id=item.doc_id)


def test_the_arm_arity_convicts_at_build() -> None:
    """A zero-param / two-param arm is the BUILD refusal (E10/E12's walk
    over the map_arms) — never the raw mid-flow TypeError. The
    zero-param arm is E10's conviction (0 ≠ the wired 1); the two-param
    arm is the deps SHAPE, so E12 owns it (the app binds no deps)."""

    async def src(ctx: StepContext) -> list[ImageItem | AudioItem]:
        return []

    for body, label, rule in (
        (zero_param_arm, "zero-param", "E10-arity"),
        (two_param_arm, "two-param", "E12-deps-contract"),
    ):
        app = WorkflowApp()

        @app.workflow(f"rv4_arity_{label}")
        def wired(
            body: object = body,
        ) -> Promise[object]:  # the loop binding (B023): the arm's body bound at definition
            src_p = step(src, key="media")
            routed = route(
                src_p,
                {
                    ImageItem: RouteArm(body=body),  # type: ignore[dict-item]  # Why: the drill's union — the convicted arm shapes ride the type contract.
                    AudioItem: RouteArm(body=process_audio),
                },
            )
            return build(routed)

        with pytest.raises(WorkflowValidationError, match=rule):
            app.get(f"rv4_arity_{label}")


# ── E15's "related" door — the exact-union-member law ───────────────────


class DocBase(BaseModel):
    doc_id: str


class DocItem(DocBase):
    pass


def test_e15_a_superclass_arm_param_refuses_at_build() -> None:
    """An arm declared with the SUPERCLASS (``DocBase`` for a
    ``DocItem`` member) is the named build refusal — the subclass's
    fields would be dropped (``extra='ignore'``) at the runtime decode.
    The exact union member is the law."""

    async def superclass_arm(ctx: object, item: DocBase) -> ImageResult:  # pyright: ignore[reportUntypedBaseClass]  # Why: the pin's own subject — the superclass IS the convicted shape.
        return ImageResult(doc_id=item.doc_id)

    async def doc_src(ctx: StepContext) -> list[DocItem]:
        return []

    app = WorkflowApp()

    @app.workflow("rv4_e15_superclass")
    def rv4_e15_superclass() -> Promise[object]:
        src_p = step(doc_src, key="media")
        routed = route(src_p, {DocItem: RouteArm(body=superclass_arm)})
        return build(routed)

    with pytest.raises(WorkflowValidationError, match="DocBase"):
        app.get("rv4_e15_superclass")


def test_e15_an_unrelated_arm_param_refuses_at_build() -> None:
    """An arm declared with an UNRELATED model is the named build
    refusal (the E-rule, not the raw runtime death)."""

    async def unrelated_arm(ctx: object, item: AudioItem) -> ImageResult:
        return ImageResult(doc_id=item.doc_id)  # type: ignore[arg-type]  # Why: the pin's own subject — the unrelated model IS the convicted shape.

    async def doc_src(ctx: StepContext) -> list[DocItem]:
        return []

    app = WorkflowApp()

    @app.workflow("rv4_e15_unrelated")
    def rv4_e15_unrelated() -> Promise[object]:
        src_p = step(doc_src, key="media")
        routed = route(src_p, {DocItem: RouteArm(body=unrelated_arm)})
        return build(routed)

    with pytest.raises(WorkflowValidationError):
        app.get("rv4_e15_unrelated")


# ── the consumer discriminator law (E16) ────────────────────────────────


class PictureOut(BaseModel):
    """Field-identical with ClipOut at the required face — the guaranteed
    mispick's members."""

    ref: str


class ClipOut(BaseModel):
    ref: str


class ThinOut(BaseModel):
    ref: str


class RichOut(BaseModel):
    ref: str
    detail: str


async def _thin_source(ctx: StepContext) -> list[ThinOut | RichOut]:
    return []


async def _thin_consumer(ctx: object, items: list[ThinOut | RichOut]) -> int:
    return len(items)


async def _identical_consumer(ctx: object, items: list[PictureOut | ClipOut]) -> int:
    return len(items)


class DistinctA(BaseModel):
    ref: str


class DistinctB(BaseModel):
    note: str


async def _distinct_consumer(ctx: object, items: list[DistinctA | DistinctB]) -> int:
    return len(items)


def _rules_of(compiled: object) -> list[tuple[str, str]]:
    from taskq.workflows.api._validate import _run_rules

    return [(d.rule, d.severity) for d in _run_rules(compiled)]  # pyright: ignore[reportUnknownVariableType,reportUnknownMemberType]  # Why: the validator's probe seam returns the typed diagnostics; the walk below reads the rule/severity pairs.


def test_e16_field_identical_union_members_are_the_hard_refusal() -> None:
    """Field-identical union members at a CONSUMER param — the decode
    picks the first member for every payload (the guaranteed silent
    mispick): the hard E16 refusal at build."""

    app = WorkflowApp()

    @app.workflow("rv4_e16_identical")
    def rv4_e16_identical() -> Promise[object]:
        src_p = step(_thin_source, key="src")
        sink(src_p)
        return build(step(_identical_consumer, src_p, key="sum"))

    compiled = app._compile("rv4_e16_identical")
    rules = _rules_of(compiled)
    assert ("E16-union-discriminator", "error") in rules, rules


def test_e16_subset_union_members_warn_the_mispick_risk() -> None:
    """Subset-relared members (the richer payloads decode as the thinner
    first member): the E16 WARNING naming the mispick risk + the Literal
    fix."""

    app = WorkflowApp()

    @app.workflow("rv4_e16_subset")
    def rv4_e16_subset() -> Promise[object]:
        src_p = step(_thin_source, key="src")
        sink(src_p)
        return build(step(_thin_consumer, src_p, key="sum"))

    compiled = app._compile("rv4_e16_subset")
    rules = _rules_of(compiled)
    assert ("E16-union-discriminator", "warning") in rules, rules


def test_e16_distinct_union_members_are_clean() -> None:
    """Distinct members (each requires a field the other lacks): the
    decode falls through to the true member — no E16 diagnostic."""

    app = WorkflowApp()

    @app.workflow("rv4_e16_distinct")
    def rv4_e16_distinct() -> Promise[object]:
        src_p = step(_thin_source, key="src")
        sink(src_p)
        return build(step(_distinct_consumer, src_p, key="sum"))

    compiled = app._compile("rv4_e16_distinct")
    rules = _rules_of(compiled)
    assert not [r for r in rules if r[0] == "E16-union-discriminator"], rules


# ── the smaller faces ───────────────────────────────────────────────────


def test_the_same_qualname_twin_refuses_at_build() -> None:
    """Two distinct classes sharing one type tag (module.qualname) — the
    second arm SILENTLY overwrote the first (the same-qualname twin's
    overwrite): the tag-injectivity refusal at the wiring verb."""
    from taskq.workflows.chain import type_tag

    TwinA = type("Twin", (BaseModel,), {"__module__": "tests.test_wf_rv4_route_cures"})  # noqa: N806  # Why: the twin IS the subject — two distinct class objects, one tag.
    TwinB = type("Twin", (BaseModel,), {"__module__": "tests.test_wf_rv4_route_cures"})  # noqa: N806
    assert TwinA is not TwinB
    assert type_tag(TwinA) == type_tag(TwinB)

    async def twin_src(ctx: StepContext) -> list[ImageItem | AudioItem]:
        return []

    async def arm_a(ctx: object, item: TwinA) -> ImageResult:  # pyright: ignore[reportUntypedBaseClass]  # Why: the dynamic twin IS the pin's subject.
        return ImageResult(doc_id="a")

    async def arm_b(ctx: object, item: TwinB) -> AudioResult:  # pyright: ignore[reportUntypedBaseClass]  # Why: the same walk.
        return AudioResult(doc_id="b")

    app = WorkflowApp()

    @app.workflow("rv4_twin")
    def rv4_twin() -> Promise[object]:
        src_p = step(twin_src, key="media")
        routed = route(
            src_p,  # type: ignore[arg-type]  # Why: the twin union IS the pin's subject — the arms' keys collapse to one tag.
            {
                TwinA: RouteArm(body=arm_a),  # type: ignore[dict-item]  # Why: the drill's union — the twins ride the return annotation's resolution.
                TwinB: RouteArm(body=arm_b),  # type: ignore[dict-item]  # Why: the same walk.
            },
        )
        return build(routed)

    with pytest.raises(WorkflowBuildError, match="tag"):
        app.get("rv4_twin")


async def _waiting_arm(ctx: StepContext, item: ImageItem) -> ImageResult:
    """The arm-held HITL: the arm body WAITS on a signal with no declared
    gate — the hold was COMPILE-invisible (the pre-cure E14 walk skipped
    the arms)."""
    await ctx.wait_signal(
        (ImageResult,), timeout_s=30.0
    )  # pragma: no cover - the compile reads the source; the runner never runs this body
    return ImageResult(doc_id=item.doc_id)


def test_the_arm_held_hitl_is_compile_visible() -> None:
    """E14's walk OVER the map_arms: an arm body calling
    ``ctx.wait_signal`` with no declared gate produces the E14 warning
    NAMING the arm's child key (the per-arm gates' face) — the hold is
    no longer compile-invisible."""

    async def src(ctx: StepContext) -> list[ImageItem | AudioItem]:
        return []

    app = WorkflowApp()

    @app.workflow("rv4_arm_hitl")
    def rv4_arm_hitl() -> Promise[object]:
        src_p = step(src, key="media")
        routed = route(
            src_p,
            {
                ImageItem: RouteArm(body=_waiting_arm),
                AudioItem: RouteArm(body=process_audio),
            },
        )
        return build(routed)

    compiled = app._compile("rv4_arm_hitl")
    from taskq.workflows.api._validate import _run_rules

    diags = [d for d in _run_rules(compiled) if d.rule == "E14-gate-wiring"]
    assert any("media.item" in d.message for d in diags), [d.message for d in diags]


def test_the_typod_arm_queue_warns_w2() -> None:
    """A RouteArm spelling a queue no actor declares (the typo) — the W2
    warning NAMES the arm's child key and the typo'd queue (the
    pre-cure walk read node.queue only; the arm's children dispatched
    onto a queue no worker may listen on, clean at build)."""

    async def src(ctx: StepContext) -> list[ImageItem | AudioItem]:
        return []

    app = (
        WorkflowApp()
    )  # no actors: the declared universe is {"default"} — both arm queues are outside it

    @app.workflow("rv4_typo_queue")
    def rv4_typo_queue() -> Promise[object]:
        src_p = step(src, key="media")
        routed = route(
            src_p,
            {
                ImageItem: RouteArm(body=process_image, queue="gp"),  # the TYPO
                AudioItem: RouteArm(body=process_audio, queue="io"),
            },
        )
        return build(routed)

    compiled = app._compile("rv4_typo_queue")
    from taskq.workflows.api._validate import _run_rules

    diags = [d for d in _run_rules(compiled) if d.rule == "W2-unknown-queue"]
    assert any("'gp'" in d.message and "media.item" in d.message for d in diags), [
        d.message for d in diags
    ]


def test_the_mermaid_arms_render() -> None:
    """The route's arms render ON the source's label (the branches the
    compile carries — the pre-cure render drew the route source as the
    plain rectangle, the route's branches invisible), and the
    fork-carrying node wears the map-source shape."""

    async def src(ctx: StepContext) -> list[ImageItem | AudioItem]:
        return []

    app = WorkflowApp()

    @app.workflow("rv4_mermaid")
    def rv4_mermaid() -> Promise[object]:
        src_p = step(src, key="media")
        routed = route(
            src_p,
            {
                ImageItem: RouteArm(body=process_image, queue="gpu"),
                AudioItem: RouteArm(body=process_audio, queue="io"),
            },
        )
        return build(routed)

    text = app.get("rv4_mermaid").mermaid()
    assert "media([" in text, text  # the map-source stadium, not the plain rectangle
    assert "ImageItem→process_image" in text, text
    assert "AudioItem→process_audio" in text, text


def test_the_double_attach_lie_is_fixed() -> None:
    """A map over a ROUTED source is the double-attach refusal whose text
    names the ROUTE (the pre-cure check read map_item only — the map
    attached ON TOP silently, clobbering the route's placement fields;
    when it did refuse, the text said 'already carries a map' — a lie
    about the attachment it refused)."""

    async def src(ctx: StepContext) -> list[ImageItem | AudioItem]:
        return []

    async def per_item(ctx: object, item: ImageItem) -> ImageResult:
        return ImageResult(doc_id=item.doc_id)

    app = WorkflowApp()

    @app.workflow("rv4_double_attach")
    def rv4_double_attach() -> Promise[object]:
        src_p = step(src, key="media")
        _routed = route(
            src_p,
            {
                ImageItem: RouteArm(body=process_image),
                AudioItem: RouteArm(body=process_audio),
            },
        )
        # THE DOUBLE ATTACH: a MAP over the SAME source the route rides
        mapped = map_source(src_p, per_item)
        return build(mapped)

    with pytest.raises(WorkflowBuildError, match="typed route"):
        app.get("rv4_double_attach")


def test_the_cannot_resolve_message_is_truthful() -> None:
    """A source whose return annotation the compile CANNOT RESOLVE (the
    function-scope model) refuses with the 'CANNOT RESOLVE' message —
    the pre-cure text said 'does not declare', a lie about a body that
    DID declare."""

    class Local(BaseModel):  # the function-scope model — the annotation cannot resolve
        doc_id: str

    async def local_src(ctx: StepContext) -> list[Local]:  # pyright: ignore[reportUntypedBaseClass]  # Why: the unresolvable annotation IS the pin's subject.
        return []

    async def local_arm(ctx: object, item: ImageItem) -> ImageResult:
        return ImageResult(doc_id="x")

    app = WorkflowApp()

    @app.workflow("rv4_unresolvable")
    def rv4_unresolvable() -> Promise[object]:
        src_p = step(local_src, key="media")
        return build(route(src_p, {ImageItem: RouteArm(body=local_arm)}))

    with pytest.raises(WorkflowBuildError, match="CANNOT RESOLVE"):
        app.get("rv4_unresolvable")
