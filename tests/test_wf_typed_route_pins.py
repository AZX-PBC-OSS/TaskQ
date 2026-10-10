"""T27's pins — THE TYPED ROUTE AT THE GRAPH LEVEL (``route(promise, arms)``).

THE CONVICTION (the reviewer's, lived on the routing-proof corpus): the
chain's route works but the chain is graph-INVISIBLE (no join-back, the
items untyped, one placement per chain, the enum middleman), and the graph
DSL's ``skip=``-predicates are STRINGLY — the live conviction: a "video"
tag skipped BOTH arms and the run terminalized SUCCEEDED having routed
NOTHING (the exact silent drop ``RouterNotTotal`` exists to prevent).

THE CURE (T27 — the ticket is ``dag-research/tickets/T27-typed-route.md``):
the union MEMBERS key a route dict at the graph level —

    route(source_promise, {ImageItem: RouteArm(body=process_image, queue="gpu"),
                           AudioItem: RouteArm(body=process_audio, queue="io")})

— the fork stamps each child's target by the element's runtime type (the
child rows ARE graph nodes → the join-back is BY CONSTRUCTION), the arm
bodies' declared param types are the decode's target (the mis-routed
element dies LOUDLY in the coercion, never silently), and the totality
fence stands at THREE doors: the wiring verb's refusal (missing/unknown
keys, named), the validator's E15-route-totality (the checker-independent
re-proof from the compiled graph), and the runtime ``RouterNotTotal`` (the
no-match element dies loudly — the skip-silent shape is DEAD).

Red-first: every pin below ran against the base head ``d0491207`` BEFORE
the surface existed (the lazy imports raise ``ImportError`` — the red
receipt: ``.measurements/t27-typed-route-reds.txt``); the greens are the
built code's evidence. Captured: ``.measurements/t27-*.txt``.
"""

# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

import pytest
from pydantic import BaseModel

from taskq.workflows import Promise, StepContext

if TYPE_CHECKING:
    from taskq.workflows import WorkflowApp

# ── the demo's union (the images/audio shape: the reviewer's scenario) ──


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


#: THE GHOST (the reviewer's "video" tag): a third element type outside the
#: declared union — the runtime door's subject.
class VideoItem(BaseModel):
    doc_id: str


# ── the bodies ──────────────────────────────────────────────────────────


async def media_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
    """The route's source: the union PRODUCTION (the element itself — the
    type IS the tag)."""
    return [
        ImageItem(doc_id="d1", uri="s3://b/d1.png"),
        AudioItem(doc_id="d2", uri="s3://b/d2.wav"),
        ImageItem(doc_id="d3", uri="s3://b/d3.png"),
        AudioItem(doc_id="d4", uri="s3://b/d4.wav"),
    ]


async def process_image(ctx: object, item: ImageItem) -> ImageResult:
    """THE IMAGE ARM — the narrowed arm (the param declares its model;
    the decode's target IS this annotation — R3)."""
    return ImageResult(doc_id=item.doc_id, ocr=f"ocr:{item.doc_id}")


async def process_audio(ctx: object, item: AudioItem) -> AudioResult:
    """THE AUDIO ARM — the other narrowed arm."""
    return AudioResult(doc_id=item.doc_id, transcript=f"tr:{item.doc_id}")


async def lying_media_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
    """THE BODY THAT LIED (the reviewer's live conviction, inverted): the
    declared union is ImageItem | AudioItem but a "video" element rides
    the list — at the pre-cure heads this element skipped BOTH arms (the
    stringly skip face) and the run terminalized SUCCEEDED having routed
    NOTHING. The cast IS the drill (the runtime door exists for the body
    that lies about its type — the checker cannot see a cast)."""
    ghost: object = VideoItem(doc_id="vid-999")
    return [
        ImageItem(doc_id="d1", uri="s3://b/d1.png"),
        cast("ImageItem | AudioItem", ghost),  # the "video" tag — the live conviction's element
        AudioItem(doc_id="d2", uri="s3://b/d2.wav"),
    ]


def wire_the_scenario(
    app: WorkflowApp,
    name: str,
    source_body: Callable[..., Awaitable[object]],
) -> None:
    """Wire the reviewer's exact scenario on *app*: the source step → the
    typed route (the two placements) → the typed-sum join (the terminal).
    The lazy imports ARE the red-first door: at the base head they raise
    ``ImportError`` and every pin xfails."""
    from taskq.workflows import RouteArm, build, route, step

    @app.workflow(name)
    def the_scenario() -> Promise[object]:
        src = step(source_body, key="media")
        routed = route(
            src,
            {
                ImageItem: RouteArm(body=process_image, queue="gpu"),
                AudioItem: RouteArm(body=process_audio, queue="io"),
            },
        )
        return build(routed)


# ── R1: the totality fence, door 1 — the wiring verb's refusal ──────────


def test_r1_a_non_total_route_refuses_at_the_wiring() -> None:
    """A route missing a union member is REFUSED at the wiring — the
    element it drops would route NOTHING (the silent drop the route
    exists to refuse); the refusal NAMES the member."""
    from taskq.workflows import RouteArm, WorkflowBuildError, build, route, step

    app = WorkflowApp()

    @app.workflow("r1_not_total")
    def r1_not_total() -> Promise[object]:
        src = step(media_source, key="media")
        return build(route(src, {ImageItem: RouteArm(body=process_image)}))  # AudioItem DROPPED

    with pytest.raises(WorkflowBuildError, match="AudioItem"):
        app.get("r1_not_total")


def test_r1_an_unknown_arm_key_refuses_at_the_wiring() -> None:
    """A route keyed by a member OUTSIDE the union is the same refusal's
    other face — the unknown member is NAMED."""
    from taskq.workflows import RouteArm, WorkflowBuildError, build, route, step

    class Foreign(BaseModel):
        x: int = 0

    app = WorkflowApp()

    @app.workflow("r1_unknown_key")
    def r1_unknown_key() -> Promise[object]:
        src = step(media_source, key="media")
        return build(
            route(
                src,
                {
                    ImageItem: RouteArm(body=process_image, queue="gpu"),
                    AudioItem: RouteArm(body=process_audio, queue="io"),
                    Foreign: RouteArm(body=process_image),
                },
            )
        )

    with pytest.raises(WorkflowBuildError, match="Foreign"):
        app.get("r1_unknown_key")


# ── R1: the totality fence, door 2 — the validator's E15 re-proof ───────


def test_r1_e15_route_totality_reds_the_validator() -> None:
    """THE VALIDATOR'S OWN DOOR (the checker-independent fence): E15 —
    the next free rule id — re-proves the route's totality from the
    COMPILED graph (public, mutable data — E3's precedent: the rule owns
    the shape injected into it). Both directions convict: a MISSING
    member and an UNKNOWN member, each named."""
    from taskq.workflows import RouteArm, WorkflowApp, route, step
    from taskq.workflows.api._validate import WorkflowValidationError, validate_compiled

    app = WorkflowApp()

    @app.workflow("r1_e15_probe")
    def r1_e15_probe() -> Promise[object]:
        src = step(media_source, key="media")
        return build(route(src, {ImageItem: RouteArm(body=process_image), AudioItem: RouteArm(body=process_audio)}))

    class Foreign(BaseModel):
        x: int = 0

    # The probe seam: compile WITHOUT the registration door (the
    # validator's pins' subject IS the invalid graph).
    compiled = app._compile("r1_e15_probe")

    # Leg 1 — the MISSING member (AudioItem dropped from the injected graph).
    media_node = compiled.nodes["media"]
    media_node.map_arms = {
        f"{ImageItem.__module__}.{ImageItem.__qualname__}": RouteArm(body=process_image),
    }
    # validate_compiled's raise is the AGGREGATED report (the rule name
    # rides the message); the individual diagnostic's rule is E15.
    with pytest.raises(WorkflowValidationError) as missing_info:
        validate_compiled(compiled)
    assert "E15-route-totality" in str(missing_info.value), str(missing_info.value)

    # Leg 2 — the UNKNOWN member (a Foreign key injected).
    media_node.map_arms = {
        f"{ImageItem.__module__}.{ImageItem.__qualname__}": RouteArm(body=process_image),
        f"{AudioItem.__module__}.{AudioItem.__qualname__}": RouteArm(body=process_audio),
        f"{Foreign.__module__}.{Foreign.__qualname__}": RouteArm(body=process_image),
    }
    with pytest.raises(WorkflowValidationError) as unknown_info:
        validate_compiled(compiled)
    assert "E15-route-totality" in str(unknown_info.value), str(unknown_info.value)


# ── R1: the totality fence, door 3 — the runtime loud death ─────────────


@pytest.mark.integration
async def test_r1_the_video_element_dies_loud_at_runtime(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """THE LIVE CONVICTION INVERTED: the same "video" scenario — a
    third-type element where the route expects the declared union — now
    DIES LOUDLY: the source row terminal-FAILED with
    ``error_class='RouterNotTotal'``, NO child routed (the skip-silent
    shape is dead), and the run FAILED — never succeeded-having-routed-
    nothing."""
    from taskq.workflows import FlowRunner, WorkflowApp, RouteArm, build, route, step

    app = WorkflowApp()

    @app.workflow("r1_video_ghost")
    def r1_video_ghost() -> Promise[object]:
        src = step(lying_media_source, key="media")
        return build(route(src, {ImageItem: RouteArm(body=process_image), AudioItem: RouteArm(body=process_audio)}))

    runner = FlowRunner(app.get("r1_video_ghost"), module_pg_pool, wf_schema)  # pyright: ignore[reportArgumentType]  # Why: the house fixtures are object-typed (the refutation pins' convention) — the runner's own params are the contract.
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)

    source_row = await wf_conn.fetchrow(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the object-typed fixture — the assert IS the runtime shape check.
        f'SELECT status, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'media'",
        flow_id,
    )
    assert source_row is not None, "the source row is gone"
    assert source_row["status"] == "failed", dict(source_row)
    assert source_row["error_class"] == "RouterNotTotal", dict(source_row)
    assert "VideoItem" in str(source_row["error_message"]), dict(source_row)
    # NOTHING routed — the skip-silent shape is dead.
    children = await wf_conn.fetch(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the same fixture walk.
        f'SELECT step_key FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'media.item%'",
        flow_id,
    )
    assert not children, f"the ghost routed {[str(r['step_key']) for r in children]} — a silent drop survived"
    # The run FAILED — never succeeded-having-routed-nothing.
    root = await wf_conn.fetchrow(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the same fixture walk.
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root is not None and root["status"] == "failed", dict(root) if root else None


# ── R2: the join-back — the routed results are join-addressable ─────────


@pytest.mark.integration
async def test_r2_the_routed_results_join_addressable(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """R2: the child rows ARE graph nodes — every routed child feeds the
    derived ``<src>.join`` (BY CONSTRUCTION), the join fires with the
    arms' returns packed (THE TYPED SUM), and the flow's ``result()``
    carries it."""
    from taskq.workflows import FlowRunner, WorkflowApp

    app = WorkflowApp()
    wire_the_scenario(app, "r2_join_back", media_source)

    runner = FlowRunner(app.get("r2_join_back"), module_pg_pool, wf_schema)  # pyright: ignore[reportArgumentType]  # Why: the object-typed fixtures (the house convention).
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    # THE JOIN ROW FIRED with the typed sum packed (the arms' returns).
    join_row = await wf_conn.fetchrow(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the object-typed fixture.
        f'SELECT status, result FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'media.join'",
        flow_id,
    )
    assert join_row is not None, "the route's join-back is missing — the children are not graph nodes"
    assert join_row["status"] == "succeeded", dict(join_row)
    result = await runner.result(flow_id)
    assert isinstance(result, list), result
    images = [r for r in result if isinstance(r, dict) and "ocr" in r]
    audios = [r for r in result if isinstance(r, dict) and "transcript" in r]
    assert len(images) == 2, result
    assert len(audios) == 2, result
    # The typed sum: the arms' OWN returns, self-discriminating.
    assert {str(r["doc_id"]) for r in images} == {"d1", "d3"}, images
    assert {str(r["doc_id"]) for r in audios} == {"d2", "d4"}, audios


# ── R3: the decoded typed models ─────────────────────────────────────────


@pytest.mark.integration
async def test_r3_the_arm_bodies_receive_their_declared_models(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """R3: the arm bodies' params are the TYPED MODELS — the runner's
    codec validates the jsonb element into the arm's declared annotation
    (the chain's dict face, the convicted gap, never exists here): the
    arms' own attribute access proves the element arrived AS its type,
    and the rows carry the arms' own returns."""
    from taskq.workflows import FlowRunner, WorkflowApp

    app = WorkflowApp()
    wire_the_scenario(app, "r3_decoded_arms", media_source)

    runner = FlowRunner(app.get("r3_decoded_arms"), module_pg_pool, wf_schema)  # pyright: ignore[reportArgumentType]  # Why: the object-typed fixtures.
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    rows = await wf_conn.fetch(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the object-typed fixture.
        f'SELECT step_key, status, result FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'media.item%' "
        "ORDER BY step_key, map_index",
        flow_id,
    )
    assert len(rows) == 4, [dict(r) for r in rows]
    # The arms' OWN returns are on the rows (attribute access worked —
    # the element arrived AS the declared model in BOTH arms).
    for r in rows:
        assert r["status"] == "succeeded", dict(r)
        assert "ocr" in str(r["result"]) or "transcript" in str(r["result"]), dict(r)


def test_r3_a_duck_typed_arm_param_refuses_at_build() -> None:
    """THE DUCK-SHAPED HOLE (E5's own conviction shape, at the route's
    arms): an arm body whose param is a plain dict (or unannotated/Any)
    consumes the element UNVALIDATED — the route is the typed boundary
    and the duck arm is the hole it exists to close. E15 refuses at
    build, naming the arm."""
    from taskq.workflows import RouteArm, WorkflowApp, route, step
    from taskq.workflows.api._validate import WorkflowValidationError

    async def duck_arm(ctx: object, item: dict[str, object]) -> dict[str, object]:  # the duck
        return {"doc_id": str(item.get("doc_id"))}

    app = WorkflowApp()

    @app.workflow("r3_duck_arm")
    def r3_duck_arm() -> Promise[object]:
        src = step(media_source, key="media")
        return build(
            route(src, {ImageItem: RouteArm(body=duck_arm), AudioItem: RouteArm(body=process_audio)})
        )

    with pytest.raises(WorkflowValidationError) as exc_info:
        app.get("r3_duck_arm")
    assert getattr(exc_info.value, "rule", "") == "E15-route-totality", str(exc_info.value)
    assert "duck_arm" in str(exc_info.value), str(exc_info.value)


def test_r3_an_unrelated_arm_param_refuses_at_build() -> None:
    """The arm-param contract's other face: an arm declaring an UNRELATED
    model (the image arm claiming the audio result's shape) is the
    wiring promising data the arm cannot accept — E15 refuses at build,
    both type names in the message."""
    from taskq.workflows import RouteArm, WorkflowApp, route, step
    from taskq.workflows.api._validate import WorkflowValidationError

    async def wrong_arm(ctx: object, item: AudioResult) -> dict[str, object]:
        return {"doc_id": item.doc_id}

    app = WorkflowApp()

    @app.workflow("r3_wrong_arm")
    def r3_wrong_arm() -> Promise[object]:
        src = step(media_source, key="media")
        return build(
            route(src, {ImageItem: RouteArm(body=wrong_arm), AudioItem: RouteArm(body=process_audio)})
        )

    with pytest.raises(WorkflowValidationError) as exc_info:
        app.get("r3_wrong_arm")
    assert getattr(exc_info.value, "rule", "") == "E15-route-totality", str(exc_info.value)
    assert "AudioResult" in str(exc_info.value), str(exc_info.value)


# ── R4: the per-arm placement — the rows assert it ───────────────────────


@pytest.mark.integration
async def test_r4_the_child_rows_stamp_their_arms_placement(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """R4: process_image's children on the gpu queue, process_audio's on
    the io queue — the CHILD ROWS' own queue/actor columns carry their
    arm's stamp (the fork's per-child placement), and each row's
    step_key NAMES its arm (the ledger receipt is direct)."""
    from taskq.workflows import FlowRunner, WorkflowApp

    app = WorkflowApp()
    wire_the_scenario(app, "r4_placements", media_source)

    runner = FlowRunner(app.get("r4_placements"), module_pg_pool, wf_schema)  # pyright: ignore[reportArgumentType]  # Why: the object-typed fixtures.
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    rows = await wf_conn.fetch(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the object-typed fixture.
        f'SELECT step_key, map_index, queue, actor FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'media.item%'",
        flow_id,
    )
    assert len(rows) == 4, [dict(r) for r in rows]
    image_tag = f"{ImageItem.__module__}.{ImageItem.__qualname__}"
    audio_tag = f"{AudioItem.__module__}.{AudioItem.__qualname__}"
    # THE ROWS ASSERT IT: the gpu queue carries exactly the image arm's
    # rows (elements 0 and 2), the io queue exactly the audio arm's
    # (elements 1 and 3); each row's step_key NAMES its arm and its
    # map_index is the element's index (the arbiter's discriminator).
    image_rows = sorted(
        int(r["map_index"])  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the object-typed fixture's Record members — the int() IS the runtime shape check.
        for r in rows
        if str(r["step_key"]) == f"media.item:{image_tag}"  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
    )
    audio_rows = sorted(
        int(r["map_index"])  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
        for r in rows
        if str(r["step_key"]) == f"media.item:{audio_tag}"  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
    )
    assert image_rows == [0, 2], [dict(r) for r in rows]
    assert audio_rows == [1, 3], [dict(r) for r in rows]
    assert {str(r["queue"]) for r in rows if "ImageItem" in str(r["step_key"])} == {"gpu"}, [  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
        dict(r) for r in rows
    ]
    assert {str(r["queue"]) for r in rows if "AudioItem" in str(r["step_key"])} == {"io"}, [  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
        dict(r) for r in rows
    ]


# ── the e2e: the reviewer's exact scenario ───────────────────────────────


@pytest.mark.integration
async def test_e2e_the_reviewers_scenario(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """THE WHOLE SCENARIO, LIVED: the images/audio map → the typed route
    (the union's members as keys) → the two placements (gpu/io) → the
    typed-sum join → the downstream consumer's summary ON result(). The
    rows are the receipt."""
    from taskq.workflows import FlowRunner, WorkflowApp, RouteArm, build, route, sink, step

    async def summarize(ctx: object, items: list[ImageResult | AudioResult]) -> dict[str, int]:
        """The join's downstream consumer: the typed sum DECODED (the
        flat list of the arms' models — attribute access works)."""
        images = sum(1 for i in items if isinstance(i, ImageResult))
        audios = sum(1 for i in items if isinstance(i, AudioResult))
        return {"images": images, "audios": audios}

    app = WorkflowApp()

    @app.workflow("r27_e2e_scenario")
    def r27_e2e_scenario() -> Promise[object]:
        src = step(media_source, key="media")
        routed = route(
            src,
            {
                ImageItem: RouteArm(body=process_image, queue="gpu"),
                AudioItem: RouteArm(body=process_audio, queue="io"),
            },
        )
        summary = step(summarize, routed, key="summary")
        sink(routed)  # the summary's edge consumes the children; the join's own promise stays explicit
        return build(summary)

    runner = FlowRunner(app.get("r27_e2e_scenario"), module_pg_pool, wf_schema)  # pyright: ignore[reportArgumentType]  # Why: the object-typed fixtures.
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    result = await runner.result(flow_id)
    assert result == {"images": 2, "audios": 2}, result
    # THE ROWS ARE THE RECEIPT: two arms' children on their own queues,
    # every row succeeded.
    rows = await wf_conn.fetch(  # pyright: ignore[reportAttributeAccessIssue]  # Why: the object-typed fixture.
        f'SELECT step_key, queue, status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'media.item%'",
        flow_id,
    )
    assert len(rows) == 4, [dict(r) for r in rows]
    assert {str(r["queue"]) for r in rows if "ImageItem" in str(r["step_key"])} == {"gpu"}, [  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the object-typed fixture's Record members — the str() IS the runtime shape check.
        dict(r) for r in rows
    ]
    assert {str(r["queue"]) for r in rows if "AudioItem" in str(r["step_key"])} == {"io"}, [  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.
        dict(r) for r in rows
    ]
    assert all(r["status"] == "succeeded" for r in rows), [dict(r) for r in rows]  # pyright: ignore[reportArgumentType,reportIndexType]  # Why: the same walk.


# ── the map face: map_source's dict form IS the same machinery ──────────


def test_map_source_dict_form_is_the_same_machinery() -> None:
    """``map_source(src, {A: fn_a, B: fn_b})`` — the per-element case
    spelled at the map face (the reviewer's (b)) — lowers through the
    SAME route attachment: the compiled source carries ``map_arms`` and
    the same ``.join`` node."""
    from taskq.workflows import RouteArm, WorkflowApp, build, map_source, step

    app = WorkflowApp()

    @app.workflow("r27_map_dict_form")
    def r27_map_dict_form() -> Promise[object]:
        src = step(media_source, key="media")
        return build(
            map_source(
                src,
                {
                    ImageItem: RouteArm(body=process_image, queue="gpu"),
                    AudioItem: RouteArm(body=process_audio, queue="io"),
                },
            )
        )

    compiled = app.get("r27_map_dict_form")
    node = compiled.nodes["media"]
    assert node.map_arms is not None, "the dict form did not lower onto the route attachment"
    assert node.map_item is None
    image_tag = f"{ImageItem.__module__}.{ImageItem.__qualname__}"
    arm = node.map_arms[image_tag]
    assert arm.body is process_image
    assert arm.queue == "gpu"
    assert "media.join" in compiled.nodes
