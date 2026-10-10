# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.
"""T27's HOSTILE-REVIEW pins (the rv4 internals round, feat/taskqflow @
fab8dbe6) — the typed route's convicted defects, pinned asserting the
SAFE behavior.

Provenance: the rv4 internals review's live convictions, each reproduced
against this head BEFORE pinning (the pre-pin repro shapes + outputs are
in the pin pack's RECEIPTS.md). Per the doctrine, a LIVE finding is
pinned under ``pytest.mark.xfail(strict=True, reason="LIVE FINDING
rv4-<id>: …")`` asserting the SAFE behavior — the cure flips the pin
XPASS-strict (a red that says: remove the marker WITH the cure). The one
GREEN guard (rv4-9's undeclared leg) pins the behavior that is already
correct and must survive the cure.

The findings (each convicted live; the receipts carry the verbatim reds):

* F-RV4-1 (SEVERE) — the EMPTY-LIST route: ``_route_fork`` returns
  ``None`` over ``[]`` (api/_runner.py:1212-1213). With a CONSUMER of
  the join the join row never spawns, the consumer's reserved dep never
  releases, the run wedges ``running`` FOREVER; with the join as the
  TERMINAL the run terminalizes SUCCEEDED with ``result()`` == ``None``
  — the succeeded-having-routed-nothing shape through the empty-corpus
  door. THE PIN'S SEMANTICS (the map's empty-join shape is the named
  precedent): an empty source list → the join FIRES exactly once with
  ``[]`` → the consumer receives ``[]`` → the flow terminalizes
  succeeded → ``result()`` is the consumer's empty-list answer.
* F-RV4-2 (SEVERE) — the >1000 FAN-IN DRIVER CRASH: ``_route_fork``
  carries no ``MAX_FAN_IN_PER_JOIN`` check; ``validate_fork`` raises
  INSIDE the finalize tx, OUTSIDE the ladder's try
  (api/_runner.py:746) — a raw ``ValueError`` escapes ``drive()``, the
  source row wedges ``running``, and the fleet's reclaim re-crashes it
  forever. The error's own remedy (``JoinSpec(child_driven=True)``) is
  unreachable from the route API. THE PIN: a TYPED, LADDERED, NAMED
  refusal — the source terminal-FAILED with a named ``error_class``,
  the run failed loudly, a re-drive does not re-crash.
* F-RV4-3 — the ARM-ARITY HOLE: E15's comment claims "E10/E12's faces
  own the arity" — FALSE for arms (E10 walks ``node.args``, E12 walks
  ``map_item``/``loop_body``, neither walks ``map_arms``;
  _validate.py:664-666). A zero-param arm BUILDS clean and dies
  ``TypeError: arm_zero() takes 1 positional argument but 2 were given``
  at the child (observed live, ``error_class='TypeError'``); a
  two-param arm the same. THE PIN: arm arity is a BUILD refusal
  (an E-rule naming the arm + the shape).
* F-RV4-4 — the "RELATED" TRUNCATION: E15's related-check
  (_validate.py:672-678) admits SUPERCLASS params — ``item: DocBase``
  over the member ``TextDoc`` builds clean and the runtime SILENTLY
  TRUNCATES the subclass's fields (pydantic ``extra='ignore'``: the arm
  receives ``DocBase(doc_id=...)``, the ``text`` field GONE — observed
  live); ``item: BaseModel`` builds clean and dies raw
  ``PydanticUserError`` at the child. THE PIN (the direction named by
  the finding, flagged for the maintainer in PINMAP.md): the arm param
  must be EXACTLY the union member — a superclass/BaseModel param is a
  build refusal NAMING the truncation hazard.
* F-RV4-5 — the SAME-TAG TWIN COLLAPSE: ``type_tag`` =
  ``module.qualname`` is not injective — two ``create_model("Rv4Twin")``
  types share one tag; the second arm SILENTLY OVERWRITES the first in
  the normalized dict (_graph.py:663) and the tag-set totality compare
  passes both (observed: ``map_arms`` carries ONE entry, the first
  arm's body gone). THE PIN: duplicate type tags refuse at the verb
  with the collision named.
* F-RV4-6 — the CONSUMER'S SMART-UNION MISPICK: the join's consumer
  param ``list[A | B]`` revalidates through pydantic's smart union
  (_runner_codec.py:219) — two members with IDENTICAL fields decode the
  second member's values AS THE FIRST silently (observed live: the
  fork's ``media.item:...AudioItem`` row carried ``note="from-b"`` and
  the consumer received it as ``SumA``). THE PIN (direction flagged in
  PINMAP.md): a routed union whose members are field-identical (no
  discriminator) is a build-time diagnostic naming both members.
* F-RV4-7 — the ARM-HELD HITL COMPILE-BLINDNESS: E14's ``_node_bodies``
  walks body/loop_body/map_item but NOT ``map_arms``
  (_validate.py:815-826) — a ``wait_signal`` inside a route arm raises
  NO E14 signal (no warning, no Mermaid hold) AND a gate declared on
  the SOURCE convicts "declared-never-waited" though the arm waits
  (both observed live). THE PIN (the contract per the house's
  zero-false-positive doctrine): the E14 walk covers ``map_arms`` — an
  arm wait with NO declared gate is the WARNING face; the declared gate
  on the SOURCE covers the arms' waits (no diagnostic).
* F-RV4-8 (minor) — the ROUTE→MAP DOUBLE-ATTACH MESSAGE: map_source's
  check reads ``map_item`` only (_graph.py:572) — map-after-route
  refuses via the ACCIDENTAL "node 'media.join' is declared twice"
  instead of the designed "already carries a map or a route". THE PIN:
  the designed message fires on all four double-attach directions.
* F-RV4-9 (minor) — the UNRESOLVABLE-ANNOTATION MESSAGE: the verb
  hard-refuses "does not declare a list[...] return" when the source
  DID declare one the compile could not resolve (_graph.py:714-719;
  observed live with a factory-built model). THE PIN: the message
  distinguishes undeclared from unresolvable (the undeclared face's
  message is the GREEN guard).
* F-RV4-10 (minor) — W2 BLIND TO ARM QUEUES: a typo'd
  ``RouteArm(queue="gpuu-typo")`` yields zero diagnostics
  (_validate.py:1097-1120 reads ``node.queue`` only). THE PIN: the
  queue-vocabulary warning covers arm queues.
"""

from __future__ import annotations

from typing import Any, cast

import pytest
from pydantic import BaseModel, create_model

from taskq.workflows import (
    GateDecl,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    WorkflowBuildError,
    build,
    map_source,
    route,
    step,
)
from taskq.workflows.api._validate import WorkflowValidationError, validate_compiled
from taskq.workflows.definitions import MAX_FAN_IN_PER_JOIN
from tests._wf_fixtures import fire_count

# ── the demo's union (the T27 house corpus's shape) ─────────────────────


class ImageItem(BaseModel):
    doc_id: str
    uri: str


class AudioItem(BaseModel):
    doc_id: str
    uri: str


class ImageResult(BaseModel):
    doc_id: str
    ocr: str


class AudioResult(BaseModel):
    doc_id: str
    transcript: str


async def media_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
    """The non-empty route source (the static pins' wiring — build-only)."""
    return [ImageItem(doc_id="d1", uri="u"), AudioItem(doc_id="d2", uri="u")]


async def empty_route_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
    """F-RV4-1's source: the declared union, the EMPTY corpus."""
    return []


async def process_image(ctx: object, item: ImageItem) -> ImageResult:
    return ImageResult(doc_id=item.doc_id, ocr=f"ocr:{item.doc_id}")


async def process_audio(ctx: object, item: AudioItem) -> AudioResult:
    return AudioResult(doc_id=item.doc_id, transcript=f"tr:{item.doc_id}")


# ── F-RV4-1: the empty-list route ────────────────────────────────────────
#
# THE SAFE SEMANTICS (the map's empty-join shape is the named precedent —
# ``_map_fork``'s own docstring: "the map over nothing — the join fires
# empty"): the empty source list is a DEFINED outcome, never a wedge and
# never a None. NOTE (PINMAP): at this head the MAP face's empty fork
# crashes raw too (the docstring's shape is aspirational) — the route's
# cure and the map's cure share the empty-join semantics these pins assert.


@pytest.mark.integration
async def test_rv4_1a_the_empty_route_fires_the_join_with_the_empty_list(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """The CONSUMER shape: an empty source list → the join FIRES exactly
    once with ``[]`` → the consumer receives ``[]`` and answers the
    empty-sum answer → the run terminalizes SUCCEEDED. Never a wedge,
    never a vanished join."""
    from taskq.workflows import FlowRunner

    async def consume(ctx: object, items: list[object]) -> dict[str, int]:
        """The join's consumer: the empty list's answer is the typed zero."""
        return {"count": len(items)}

    app = WorkflowApp()

    @app.workflow("rv4_1a_empty_route_consumer")
    def rv4_1a_empty_route_consumer() -> Promise[object]:
        src = step(empty_route_source, key="media")
        routed = route(
            src,
            {
                ImageItem: RouteArm(body=process_image),
                AudioItem: RouteArm(body=process_audio),
            },
        )
        return build(step(consume, routed, key="consume"))

    runner = FlowRunner(app.get("rv4_1a_empty_route_consumer"), module_pg_pool, wf_schema)  # pyright: ignore[reportArgumentType]  # Why: the house fixtures are object-typed (the route pins' convention) — the runner's own params are the contract.
    flow_id = (await runner.create_flow()).flow_id
    verdict = await runner.drive(flow_id, tick=0.02, max_ticks=120)
    assert verdict == "terminal", (
        f"F-RV4-1a: the empty route WEDGED the run (drive returned {verdict!r}) — "
        "the join row never spawns and the consumer's reserved dep never releases"
    )
    # THE JOIN FIRED, exactly once, with the empty list.
    join_row = await wf_conn.fetchrow(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the object-typed fixture — the assert IS the runtime shape check.
        f'SELECT status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'media.join'",
        flow_id,
    )
    assert join_row is not None, "F-RV4-1a: the route's join row never spawned"
    assert join_row["status"] == "succeeded", dict(join_row)  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the object-typed fixture's Record members — the str() reads are the runtime shape check.
    join_id = await wf_conn.fetchval(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the same walk.
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'media.join'",
        flow_id,
    )
    fires = await fire_count(wf_conn, wf_schema, join_id)  # pyright: ignore[reportArgumentType]  # Why: the same walk.
    assert fires == 1, f"F-RV4-1a: the join fired {fires} times (exactly once is the law)"
    # THE CONSUMER RECEIVED [] — its answer is the empty-sum zero, ON result().
    result = await runner.result(flow_id)
    assert result == {"count": 0}, (
        f"F-RV4-1a: result() is {result!r} — the consumer's empty-list answer "
        "({'count': 0}) is the defined outcome, never None"
    )


@pytest.mark.integration
async def test_rv4_1b_the_empty_route_terminal_join_returns_the_empty_sum(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """The TERMINAL shape: the route's join as the flow's terminal — the
    empty source list fires the join with ``[]`` and ``result()`` IS the
    empty sum (``[]``), never ``None``, the run SUCCEEDED."""
    from taskq.workflows import FlowRunner

    app = WorkflowApp()

    @app.workflow("rv4_1b_empty_route_terminal")
    def rv4_1b_empty_route_terminal() -> Promise[object]:
        src = step(empty_route_source, key="media")
        return build(
            route(
                src,
                {
                    ImageItem: RouteArm(body=process_image),
                    AudioItem: RouteArm(body=process_audio),
                },
            )
        )

    runner = FlowRunner(app.get("rv4_1b_empty_route_terminal"), module_pg_pool, wf_schema)  # pyright: ignore[reportArgumentType]  # Why: the object-typed fixtures (the house convention).
    flow_id = (await runner.create_flow()).flow_id
    verdict = await runner.drive(flow_id, tick=0.02, max_ticks=120)
    assert verdict == "terminal", (
        f"F-RV4-1b: the empty route wedged the run (drive returned {verdict!r})"
    )
    join_row = await wf_conn.fetchrow(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the object-typed fixture.
        f'SELECT status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'media.join'",
        flow_id,
    )
    assert join_row is not None, (
        "F-RV4-1b: the join row never spawned — the run terminalized SUCCEEDED "
        "having routed NOTHING (result() is None below)"
    )
    assert join_row["status"] == "succeeded", dict(join_row)  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
    result = await runner.result(flow_id)
    assert result == [], (
        f"F-RV4-1b: result() is {result!r} — the empty sum is [], never None "
        "(None is the succeeded-having-routed-nothing lie)"
    )


# ── F-RV4-2: the >1000 fan-in driver crash ───────────────────────────────


async def oversized_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
    """F-RV4-2's source: MAX_FAN_IN_PER_JOIN+1 elements (the fork's join
    over them trips ``validate_fork`` INSIDE the finalize tx)."""
    return [ImageItem(doc_id=f"d{i}", uri="u") for i in range(MAX_FAN_IN_PER_JOIN + 1)]


@pytest.mark.integration
async def test_rv4_2_the_oversized_route_fan_in_is_a_laddered_named_refusal(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """A route over MAX_FAN_IN_PER_JOIN+1 elements is a TYPED, LADDERED,
    NAMED refusal: ``drive()`` does NOT raise a raw error through the
    driver, the source row terminal-FAILS with a named ``error_class``
    whose message names the fan-in bound, the run FAILS loudly, and a
    re-drive answers 'terminal' quietly (no re-crash loop)."""
    from taskq.workflows import FlowRunner

    async def consume(ctx: object, items: list[object]) -> dict[str, int]:
        return {"count": len(items)}

    app = WorkflowApp()

    @app.workflow("rv4_2_fan_in_refusal")
    def rv4_2_fan_in_refusal() -> Promise[object]:
        src = step(
            oversized_source, key="media", max_attempts=1
        )  # one attempt: the named refusal terminal-fails without burning retries
        routed = route(
            src,
            {
                ImageItem: RouteArm(body=process_image),
                AudioItem: RouteArm(body=process_audio),
            },
        )
        return build(step(consume, routed, key="consume"))

    runner = FlowRunner(app.get("rv4_2_fan_in_refusal"), module_pg_pool, wf_schema)  # pyright: ignore[reportArgumentType]  # Why: the object-typed fixtures.
    flow_id = (await runner.create_flow()).flow_id
    # THE RED LANDS HERE: drive() raises the raw ValueError out of the
    # finalize tx (untyped, unladdered) instead of returning 'terminal'
    # over a terminal-FAILED source.
    verdict = await runner.drive(flow_id, tick=0.02, max_ticks=120)
    assert verdict == "terminal", f"F-RV4-2: drive returned {verdict!r} over the refusal"
    source_row = await wf_conn.fetchrow(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the object-typed fixture.
        f'SELECT status, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'media'",
        flow_id,
    )
    assert source_row is not None, "the source row is gone"
    assert source_row["status"] == "failed", (  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
        f"F-RV4-2: the source is {source_row['status']!r} — the refusal must "  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
        "terminal-FAIL the source, never wedge it 'running'"
    )
    assert source_row["error_class"], (  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
        "F-RV4-2: the refusal must be NAMED (an error_class), not a raw escape"
    )
    assert str(MAX_FAN_IN_PER_JOIN) in str(  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
        source_row["error_message"]  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
    ), f"F-RV4-2: the refusal's message must name the bound: {source_row!r}"
    # THE RUN FAILS LOUDLY.
    root = await wf_conn.fetchrow(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the same walk.
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root is not None and root["status"] == "failed", dict(root) if root else None  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
    # NO RE-CRASH LOOP: a re-drive over the terminal run is a quiet
    # 'terminal' (the reclaim never re-executes the refusal).
    assert await runner.drive(flow_id, tick=0.02, max_ticks=20) == "terminal"


# ── F-RV4-3: the arm-arity hole ──────────────────────────────────────────


async def arm_zero(ctx: object) -> ImageResult:
    """THE ZERO-PARAM ARM: builds clean today (no rule walks map_arms'
    arity) and dies ``TypeError: arm_zero() takes 1 positional argument
    but 2 were given`` at the child — the ladder burning a wiring-time
    lie, observed live (error_class='TypeError', the run failed)."""
    return ImageResult(doc_id="z", ocr="z")


async def arm_two(ctx: object, item: ImageItem, extra: object) -> ImageResult:
    """THE TWO-PARAM ARM: one param beyond the item — the same hole's
    other face (the runtime invokes arms as ``body(ctx, item)``; the
    extra param is the ladder's TypeError waiting)."""
    return ImageResult(doc_id=item.doc_id, ocr="z")


def _assert_arm_arity_refusal(exc_info: pytest.ExceptionInfo[WorkflowValidationError]) -> None:
    """The refusal's contract (the rule SEAT is the maintainer's — E10's
    walk extended to arms, E12's deps-contract face, or E15 owning the
    arms' whole shape; PINMAP flags the choice): an E-rule fires, and the
    diagnostic NAMES the offending subject — the route child the arm
    drives (the source node's ``.item`` child key, the tag's seat). The
    arm function's own name is not the graph's subject — the rule firing
    on the right child is."""
    message = str(exc_info.value)
    assert any(
        rule in message for rule in ("E10-arity", "E12-deps-contract", "E15-route-totality")
    ), f"F-RV4-3: the refusal must be an E-rule owning the arm's arity — got: {message}"
    # THE LANDED NAMING (the merge's reconciliation): the refusal names
    # the offending route child — '<source>.item:<TypeTag>' (the runtime's
    # own address for the arm; the child key IS the arm's identity on the
    # rows).
    assert "media.item" in message, (
        f"F-RV4-3: the refusal must NAME the offending route child — got: {message}"
    )


def test_rv4_3_a_zero_param_arm_is_a_build_refusal() -> None:
    """The arm's declared params are the invocation's contract (the
    runner calls ``body(ctx, item)`` — exactly one item): a zero-param
    arm is a wiring-time lie the validator refuses, naming the arm."""
    app = WorkflowApp()

    @app.workflow("rv4_3_zero_arity_arm")
    def rv4_3_zero_arity_arm() -> Promise[object]:
        src = step(media_source, key="media")
        return build(
            route(
                src, {ImageItem: RouteArm(body=arm_zero), AudioItem: RouteArm(body=process_audio)}
            )
        )

    with pytest.raises(WorkflowValidationError) as exc_info:
        app.get("rv4_3_zero_arity_arm")
    _assert_arm_arity_refusal(exc_info)


def test_rv4_3_a_two_param_arm_is_a_build_refusal() -> None:
    """The hole's other face: one param BEYOND the item (the deps shape's
    count with no deps contract walked for arms) — the same build
    refusal, the arm named."""
    app = WorkflowApp()

    @app.workflow("rv4_3_two_arity_arm")
    def rv4_3_two_arity_arm() -> Promise[object]:
        src = step(media_source, key="media")
        return build(
            route(src, {ImageItem: RouteArm(body=arm_two), AudioItem: RouteArm(body=process_audio)})
        )

    with pytest.raises(WorkflowValidationError) as exc_info:
        app.get("rv4_3_two_arity_arm")
    _assert_arm_arity_refusal(exc_info)


# ── F-RV4-4: the "related" truncation ────────────────────────────────────


class DocBase(BaseModel):
    doc_id: str


class TextDoc(DocBase):
    text: str


class ImageDoc(DocBase):
    width: int


async def doc_source(ctx: StepContext) -> list[TextDoc | ImageDoc]:
    return [TextDoc(doc_id="d1", text="hello"), ImageDoc(doc_id="d2", width=3)]


async def base_arm(ctx: object, item: DocBase) -> dict[str, object]:
    """THE SUPERCLASS PARAM: admitted by E15's related-check today; at
    runtime the codec validates the TextDoc element INTO DocBase —
    pydantic's extra='ignore' drops ``text`` and the arm receives the
    truncated shell (observed live: type 'DocBase', has_text False)."""
    return {"doc_id": item.doc_id}


async def basemodel_arm(ctx: object, item: BaseModel) -> dict[str, object]:
    """THE BARE-BaseModel PARAM: admitted today; dies raw
    ``PydanticUserError`` at the child (observed live:
    error_class='PydanticUserError')."""
    return {"doc_id": "?"}


async def image_arm(ctx: object, item: ImageDoc) -> dict[str, object]:
    return {"doc_id": item.doc_id, "width": item.width}


def test_rv4_4_a_superclass_arm_param_is_a_build_refusal() -> None:
    """The arm param IS the decode's target: anything WIDER than the
    union member decodes the element with the member's fields DROPPED —
    the silent truncation the typed boundary exists to refuse. The
    refusal names the arm AND the truncation hazard."""
    app = WorkflowApp()

    @app.workflow("rv4_4_superclass_arm")
    def rv4_4_superclass_arm() -> Promise[object]:
        src = step(doc_source, key="docs")
        return build(
            route(src, {TextDoc: RouteArm(body=base_arm), ImageDoc: RouteArm(body=image_arm)})
        )

    with pytest.raises(WorkflowValidationError) as exc_info:
        app.get("rv4_4_superclass_arm")
    message = str(exc_info.value)
    # The contract: E15 fires, naming the offending arm AND the member
    # type it must be EXACT to — the hazard's sentence is wording, never
    # pinned.
    assert "E15-route-totality" in message, message
    assert "base_arm" in message, message
    assert "TextDoc" in message, (
        f"F-RV4-4: the refusal must NAME the exact member the arm betrays — got: {message}"
    )


def test_rv4_4_a_bare_basemodel_arm_param_is_a_build_refusal() -> None:
    """The extreme truncation: ``item: BaseModel`` decodes every element
    into the field-less shell (and crashes pydantic outright) — the same
    refusal face, the arm named."""
    app = WorkflowApp()

    @app.workflow("rv4_4_basemodel_arm")
    def rv4_4_basemodel_arm() -> Promise[object]:
        src = step(doc_source, key="docs")
        return build(
            route(src, {TextDoc: RouteArm(body=basemodel_arm), ImageDoc: RouteArm(body=image_arm)})
        )

    with pytest.raises(WorkflowValidationError) as exc_info:
        app.get("rv4_4_basemodel_arm")
    message = str(exc_info.value)
    assert "E15-route-totality" in message, message
    assert "basemodel_arm" in message or "BaseModel" in message, message


# ── F-RV4-5: the same-tag twin collapse ──────────────────────────────────

#: Two DISTINCT models, one ``module.qualname`` tag (the factory-built
#: model shape): the normalized ``map_arms`` dict keys by the tag, so the
#: second arm SILENTLY OVERWRITES the first (observed: one entry, the
#: first arm's body gone) and the tag-set totality compare passes both.
TwinA = create_model("Rv4Twin", doc_id=(str, ...))
TwinB = create_model("Rv4Twin", doc_id=(str, ...))


async def twin_source(ctx: StepContext) -> list[Any]:  # the union rides __annotations__ below
    return []


twin_source.__annotations__["return"] = list[TwinA | TwinB]  # type: ignore[valid-type]  # Why: the two twins ARE the drill's union — the annotation must hold BOTH distinct types (a string annotation could not name them apart; that is the finding's point).


async def twin_arm_a(ctx: object, item: Any) -> dict[str, str]:
    return {"arm": "a"}


async def twin_arm_b(ctx: object, item: Any) -> dict[str, str]:
    return {"arm": "b"}


twin_arm_a.__annotations__["item"] = TwinA
twin_arm_b.__annotations__["item"] = TwinB


def test_rv4_5_duplicate_type_tags_refuse_at_the_verb() -> None:
    """Two union members that share a type tag are UNREPRESENTABLE as
    route arms (one key, two bodies): the verb refuses LOUDLY, naming
    the colliding tag and the duplication — never the silent overwrite
    (the dropped arm's elements would route to the WRONG body at
    runtime, or die RouterNotTotal on the body's lie)."""
    app = WorkflowApp()

    @app.workflow("rv4_5_twin_collapse")
    def rv4_5_twin_collapse() -> Promise[object]:
        src = step(twin_source, key="twins")
        return build(
            route(src, {TwinA: RouteArm(body=twin_arm_a), TwinB: RouteArm(body=twin_arm_b)})
        )

    with pytest.raises(WorkflowBuildError) as exc_info:
        app.get("rv4_5_twin_collapse")
    message = str(exc_info.value)
    # The contract: the verb refuses, naming the COLLIDING TAG (the
    # subject the two members share) — the collision's sentence is
    # wording, never pinned.
    assert "Rv4Twin" in message, (
        f"F-RV4-5: the refusal must NAME the colliding tag — got: {message}"
    )


# ── F-RV4-6: the consumer's smart-union mispick ──────────────────────────


class Rv4SumA(BaseModel):
    """The first arm's return — field-IDENTICAL to the second's (no
    Literal discriminator, same field names + types)."""

    doc_id: str
    note: str


class Rv4SumB(BaseModel):
    doc_id: str
    note: str


async def sum_arm_a(ctx: object, item: ImageItem) -> Rv4SumA:
    return Rv4SumA(doc_id=item.doc_id, note="from-a")


async def sum_arm_b(ctx: object, item: AudioItem) -> Rv4SumB:
    return Rv4SumB(doc_id=item.doc_id, note="from-b")


async def consume_sums(ctx: object, items: list[Rv4SumA | Rv4SumB]) -> dict[str, int]:
    """The join's consumer over the IDENTICAL-FIELD union: pydantic's
    smart union decodes the second member's values AS THE FIRST silently
    (observed live: the AudioItem-armed child carried note='from-b' and
    the consumer received a Rv4SumA)."""
    return {"n": len(items)}


def test_rv4_6_field_identical_union_members_are_a_build_diagnostic() -> None:
    """A routed sum whose members are field-identical (no discriminator)
    cannot round-trip the consumer's decode honestly — the dispatch is
    exact-type at the fork but the consumer's decode needs the tag. The
    build (the verb's door OR the validator's re-proof — the seat is the
    maintainer's, PINMAP flags it) names BOTH members and the hazard."""
    app = WorkflowApp()

    @app.workflow("rv4_6_smart_union_hazard")
    def rv4_6_smart_union_hazard() -> Promise[object]:
        src = step(media_source, key="media")
        routed = route(
            src,
            {
                ImageItem: RouteArm(body=sum_arm_a),
                AudioItem: RouteArm(body=sum_arm_b),
            },
        )
        return build(step(consume_sums, routed, key="consume"))

    message = ""
    try:
        compiled = app.get("rv4_6_smart_union_hazard")
    except (WorkflowBuildError, WorkflowValidationError) as exc:
        message = str(exc)
    else:
        message = "\n".join(f"{d.rule}: {d.message}" for d in validate_compiled(compiled))
    assert "Rv4SumA" in message and "Rv4SumB" in message, (
        f"F-RV4-6: no build-time diagnostic named the field-identical members "
        f"— the consumer's silent mispick ships. Build surfaces said: {message!r}"
    )
    # (The hazard's sentence — "identical", "discriminator", the smart
    # union's prose — is wording, never pinned: the diagnostic naming
    # BOTH offending members is the contract.)


# ── F-RV4-7: the arm-held HITL compile-blindness ─────────────────────────


class Rv4Approval(BaseModel):
    ok: bool


async def waiting_image_arm(ctx: StepContext, item: ImageItem) -> ImageResult:
    """THE ARM-HELD WAIT: the route's child HOLDS on a human — invisible
    to every compile surface today (E14's walk skips map_arms)."""
    approval = await ctx.wait_signal(Rv4Approval, timeout_s=30.0)
    return ImageResult(doc_id=item.doc_id, ocr=f"answered:{type(approval).__name__}")


def test_rv4_7a_an_arm_wait_with_no_declared_gate_is_warned() -> None:
    """The E14 walk covers the arms: an arm body calling
    ``ctx.wait_signal`` with NO gate declared on the route's source is
    the waited-never-declared face — the WARNING (the T26 ruling's
    severity: the hold is row-real but compile-invisible), naming the
    route's source node."""
    app = WorkflowApp()

    @app.workflow("rv4_7a_arm_wait_no_gate")
    def rv4_7a_arm_wait_no_gate() -> Promise[object]:
        src = step(media_source, key="media")
        return build(
            route(
                src,
                {
                    ImageItem: RouteArm(body=waiting_image_arm),
                    AudioItem: RouteArm(body=process_audio),
                },
            )
        )

    compiled = app.get("rv4_7a_arm_wait_no_gate")
    e14 = [d for d in validate_compiled(compiled) if d.rule == "E14-gate-wiring"]
    assert any(d.severity == "warning" and "media" in d.message for d in e14), (
        f"F-RV4-7a: the arm's wait_signal raised no E14 warning (the arm-held "
        f"hold is compile-invisible) — E14 said: {[str(d) for d in e14]!r}"
    )


def test_rv4_7b_the_source_gate_covers_the_arms_waits() -> None:
    """The contract's other face: the route's arms are the source node's
    OWN bodies for the gate walk — a gate declared on the source COVERS
    the arms' waits (per-arm gate declarations do not exist; the source's
    seat is the arms' seat). No E14 diagnostic fires either direction."""
    app = WorkflowApp()

    @app.workflow("rv4_7b_arm_wait_gated_source")
    def rv4_7b_arm_wait_gated_source() -> Promise[object]:
        src = step(
            media_source,
            key="media",
            gates=(GateDecl(name="Rv4Approval", payload_models=(Rv4Approval,), timeout_s=30.0),),
        )
        return build(
            route(
                src,
                {
                    ImageItem: RouteArm(body=waiting_image_arm),
                    AudioItem: RouteArm(body=process_audio),
                },
            )
        )

    # THE RED LANDS HERE: app.get raises WorkflowValidationError (E14's
    # declared-never-waited ERROR) though the arm WAITS on the declared gate.
    compiled = app.get("rv4_7b_arm_wait_gated_source")
    e14 = [d for d in validate_compiled(compiled) if d.rule == "E14-gate-wiring"]
    assert not e14, (
        f"F-RV4-7b: the source's declared gate covers the arms' waits — E14 "
        f"falsely convicted: {[str(d) for d in e14]!r}"
    )


# ── F-RV4-8: the route→map double-attach message ─────────────────────────


def test_rv4_8_every_double_attach_direction_speaks_the_designed_message() -> None:
    """One fork per node — ALL FOUR double-attach directions refuse, and
    every refusal NAMES THE OFFENDING NODE (the doubled source, not its
    join child): the diagnostic's subject is the node that finalized
    twice, never the accidental join-key collision (which names a
    different node and no cause)."""

    def _route(src: Promise[object]) -> Promise[object]:
        return route(
            src,
            {
                ImageItem: RouteArm(body=process_image),
                AudioItem: RouteArm(body=process_audio),
            },
        )

    def _map(src: Promise[object]) -> Promise[object]:
        return cast("Promise[object]", map_source(src, process_image))

    def wire_route_after_route(src: Promise[object]) -> Promise[object]:
        _route(src)
        return _route(src)

    def wire_route_after_map(src: Promise[object]) -> Promise[object]:
        _map(src)
        return _route(src)

    def wire_map_after_route(src: Promise[object]) -> Promise[object]:
        _route(src)  # THE CONVICTED DIRECTION: map_source's check reads map_item only
        return _map(src)

    def wire_map_after_map(src: Promise[object]) -> Promise[object]:
        _map(src)
        return _map(src)

    wirers = {
        "route-after-route": wire_route_after_route,
        "route-after-map": wire_route_after_map,
        "map-after-route": wire_map_after_route,
        "map-after-map": wire_map_after_map,
    }
    for label, wire in wirers.items():
        app = WorkflowApp()

        @app.workflow(f"rv4_8_{label.replace('-', '_')}")
        def the_double_attach(wire=wire):  # type: ignore[no-untyped-def]  # Why: the leg's wirer rides the closure — the loop's parametrization seam.
            src = step(media_source, key="media")
            return build(wire(src))

        with pytest.raises(WorkflowBuildError) as exc_info:
            app.get(f"rv4_8_{label.replace('-', '_')}")
        message = str(exc_info.value)
        # The contract: the double-attach refusal fires on the DOUBLED
        # SOURCE node — never on its join child (the accidental door's
        # subject). Which sentence carries the cause is wording; which
        # node the rule names is the subject.
        assert "'media'" in message, (
            f"F-RV4-8 ({label}): the refusal must NAME the doubled source node — got: {message}"
        )
        assert "media.join" not in message, (
            f"F-RV4-8 ({label}): the join-key collision is the wrong door (the "
            f"refusal names the join, not the doubled source) — got: {message}"
        )


# ── F-RV4-9: the unresolvable-annotation message ─────────────────────────


def _make_unresolvable_source() -> tuple[Any, type[BaseModel]]:
    """A DECLARED-but-UNRESOLVABLE ``list[...]`` return (the factory-built
    model's shape): the body's code never references the model by value
    (no closure cell) and the name is no module global, so
    ``get_type_hints`` cannot resolve the DECLARED annotation —
    ``body_hints`` returns {} and the verb cannot read the union."""
    model = create_model("Rv4HiddenDoc", doc_id=(str, ...))

    async def factory_source(ctx: StepContext) -> "list[Rv4HiddenDoc]":  # noqa: F821, UP037  # Why: the quoted, never-resolvable name IS the drill — a declared annotation the compile cannot resolve (the factory-built model's shape); a resolvable spelling would defeat the probe.  # pyright: ignore[reportUndefinedVariable, reportUnknownParameterType]  # Why: the same drill — the undefined name is the probe's subject, not a defect.
        return cast("list[Rv4HiddenDoc]", [{"doc_id": "d1"}])  # noqa: F821  # Why: the same deliberate unresolved name — the body's code references no value (no closure cell), so the hint-resolution seam finds nothing.  # pyright: ignore[reportUndefinedVariable]  # Why: the same drill.

    return factory_source, model


async def hidden_arm(ctx: object, item: BaseModel) -> dict[str, str]:
    return {"doc_id": "x"}


def test_rv4_9_the_unresolvable_return_is_distinguished_from_undeclared() -> None:
    """The source DECLARED ``list[Rv4HiddenDoc]`` — the compile cannot
    RESOLVE it. The refusal must say so — the DECLARED-but-unresolvable
    face and the NO-annotation face are DIFFERENT refusals (the same
    door, the cause distinguished), and both name the offending source
    node. The distinction IS the contract; which sentence carries it is
    wording (the de-slop law: never pinned)."""
    source_body, hidden_model = _make_unresolvable_source()
    assert "return" in source_body.__annotations__, "the source DID declare a return"

    app = WorkflowApp()

    @app.workflow("rv4_9_unresolvable_return")
    def rv4_9_unresolvable_return() -> Promise[object]:
        src = step(source_body, key="docs")
        return build(route(src, {hidden_model: RouteArm(body=hidden_arm)}))

    with pytest.raises(WorkflowBuildError) as exc_info:
        app.get("rv4_9_unresolvable_return")
    unresolvable = str(exc_info.value)
    assert "docs" in unresolvable, (
        f"F-RV4-9: the refusal must NAME the offending source node — got: {unresolvable}"
    )

    # THE DISTINCTION, measured on the other face: a source with NO
    # return annotation is a DIFFERENT refusal — same door, different
    # cause (a collapse here is the lie in either direction: the
    # undeclared face pointing at a resolution fix, the unresolvable
    # face pointing at an annotation that exists).
    app2 = WorkflowApp()

    async def bare_source(ctx: StepContext):  # pyright: ignore[reportUnknownParameterType]  # Why: the deliberately UNANNOTATED body IS the contrast face (the undeclared refusal's subject).
        return []

    @app2.workflow("rv4_9_undeclared_return")
    def rv4_9_undeclared_return() -> Promise[object]:
        src = step(bare_source, key="docs")
        return build(route(src, {ImageItem: RouteArm(body=process_image)}))

    with pytest.raises(WorkflowBuildError) as undeclared_info:
        app2.get("rv4_9_undeclared_return")
    undeclared = str(undeclared_info.value)
    assert "docs" in undeclared, (
        f"F-RV4-9: the undeclared face must ALSO name the source node — got: {undeclared}"
    )
    assert undeclared != unresolvable, (
        f"F-RV4-9: the two faces COLLAPSED — a DECLARED-but-unresolvable return "
        f"and a NO-annotation return got the SAME refusal, one of them a lie "
        f"about the cause: {unresolvable!r}"
    )


# ── F-RV4-10: W2 blind to arm queues ─────────────────────────────────────


def test_rv4_10_the_queue_vocabulary_warning_covers_arm_queues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The route's children ride the ARMS' queues — a typo'd arm queue is
    the same dispatch-onto-nobody hazard W2 exists to warn on, at the
    same build face, naming the queue."""
    monkeypatch.setenv("TASKQ_QUEUES", "gpu,io")
    app = WorkflowApp()

    @app.workflow("rv4_10_arm_queue_vocabulary")
    def rv4_10_arm_queue_vocabulary() -> Promise[object]:
        src = step(media_source, key="media")
        return build(
            route(
                src,
                {
                    ImageItem: RouteArm(body=process_image, queue="gpuu-typo"),
                    AudioItem: RouteArm(body=process_audio, queue="io"),
                },
            )
        )

    compiled = app.get("rv4_10_arm_queue_vocabulary")
    w2 = [d for d in validate_compiled(compiled) if d.rule == "W2-unknown-queue"]
    assert any("gpuu-typo" in d.message for d in w2), (
        f"F-RV4-10: the typo'd ARM queue raised no W2 warning — the "
        f"queue-vocabulary walk is blind to arm queues: {[str(d) for d in w2]!r}"
    )
