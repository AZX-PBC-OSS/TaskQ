"""THE TYPE-TAGGED ROUTE PROOF — the per-element dispatch refutation.

THE SCENARIO (the routing-proof work order): a fan-out map whose
children produce a UNION per element (the success type + the failure
type — the demo's ``Summary | Unreadable`` shape), then a downstream
dispatch that routes EACH ELEMENT BY ITS TYPE — the successes to the
reducer, the failures to the dead-letter — WITHOUT a manual isinstance
ladder in a single body, and with the type story visible in the editor
(the narrowed arms).

THE ANSWER AT THE PRE-CURE HEAD (87a77a90): REFUTED. The
:class:`Route` dispatch key is the LITERAL TAG — the body's returned
outcome enum member's ``.value`` string (:meth:`Route.next_step` keys
on it; :meth:`Chain.next_child` normalizes enum → value and raises the
loud ``RouterNotTotal`` for any non-enum, non-str outcome). The payload
TYPE never keys a route; the flow API's union consumption is the
per-body ``match`` + ``assert_never`` ladder (the doc-ingest shape 7 —
the exhaustiveness idiom PER-NODE). The closest working shape is the
enum-ladder chain — the ladder lives IN the body, which is exactly what
the scenario forbids.

THE CURE (this lane's implementation — the Route surface's honest
extension, additive and small): the route's keys may be the union's
member TYPES (a TYPE-TAGGED route); the body returns the union ELEMENT
itself; the router dispatches on the element's runtime type; on a
type-tagged arm the ELEMENT IS THE RECORD (the child's payload's
``wf_item`` is the element, jsonb-encoded) — the arm's body declares
its arm's type (the narrowed arms in the editor) and the typed boundary
re-validates the round-trip (a mis-routed element dies LOUDLY in the
coercion, never silently). The enum face is untouched: an enum outcome
keeps the verbatim-payload law. The totality fence stands at BOTH
doors: declaration (the route must be total over the union's member
types) and runtime (``RouterNotTotal`` for the element type with no
arm — the body that lied about its type).

Red-first: the proof's pins ran RED at the pre-cure head (the receipt:
``.measurements/type-tagged-route-reds.json`` — the union declaration
itself refused, ``TypeError`` out of the enum-only bind).
"""

# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (the conftest's hashed per-module schema), never user input; every value is $-bound.

from __future__ import annotations

import pytest
from pydantic import BaseModel

from taskq.workflows import Promise, StepContext
from taskq.workflows.chain import (
    DONE,
    Chain,
    Route,
    RouterNotTotal,
    Step,
    chain_start,
)

# ── the demo's union (the doc-ingest shape: the success + the failure) ──


class Summary(BaseModel):
    doc_id: str
    text: str


class Unreadable(BaseModel):
    doc_id: str
    reason: str


#: The batch's ghost (the demo's own shape): a doc the source does NOT
#: have — the Unreadable arm's LIVE element.
_DOC_SOURCE: dict[str, str] = {f"doc-{i:03}": f"the text of document {i}" for i in range(12)}
_BATCH = [*sorted(_DOC_SOURCE), "doc-999"]


# ── the bodies: the union PRODUCTION is not a dispatch ladder ───────────


async def screen(ctx: object, item: dict[str, object]) -> Summary | Unreadable:
    """The map child's body: the UNION RETURN (the element itself — the
    type IS the tag; no enum, no ladder, no match)."""
    doc_id = str(item["doc_id"])
    text = _DOC_SOURCE.get(doc_id)
    if text is None:
        return Unreadable(doc_id=doc_id, reason="missing from the source")
    return Summary(doc_id=doc_id, text=text)


async def reduce_summary(ctx: object, item: Summary) -> dict[str, object]:
    """THE REDUCER — the success arm's body: the NARROWED ARM in the
    editor (the param declares ``Summary``; the typed boundary
    re-validates the round-trip). The body's own return proves the
    element arrived AS its type (attribute access on the model)."""
    return {"reduced": item.doc_id, "chars": len(item.text)}


async def dead_letter(ctx: object, item: Unreadable) -> dict[str, object]:
    """THE DEAD-LETTER — the failure arm's body: the OTHER narrowed arm
    (``item: Unreadable``)."""
    return {"dead": item.doc_id, "why": item.reason}


THE_CHAIN = Chain(
    name="type-tagged-route-proof",
    start="screen",
    steps={
        "screen": Step(
            body=screen,
            outcomes=Summary | Unreadable,  # THE UNION IS THE VOCABULARY
            route=Route({Summary: "reduce", Unreadable: "dead_letter"}),
        ),
        "reduce": Step(
            body=reduce_summary,
            outcomes=Summary,
            route=None,  # the chain ends here
        ),
        "dead_letter": Step(
            body=dead_letter,
            outcomes=Unreadable,
            route=None,
        ),
    },
)


# ── door 1: the declaration-time totality over the union's members ──────


def test_type_route_not_total_is_refused_at_declaration() -> None:
    """A type-tagged route missing a union member is REFUSED at
    declaration — the element type it drops would silently strand the
    record's chain (the same totality fence the enum face has had since
    T20)."""
    with pytest.raises(ValueError, match="missing") as exc_info:
        Chain(
            name="not-total",
            start="screen",
            steps={
                "screen": Step(
                    body=screen,
                    outcomes=Summary | Unreadable,
                    route=Route({Summary: "reduce"}),  # Unreadable DROPPED
                ),
            },
        )
    assert "Unreadable" in str(exc_info.value), str(exc_info.value)


def test_type_route_unknown_key_is_refused_at_declaration() -> None:
    """A type-tagged route with a key OUTSIDE the union is the same
    refusal's other face (the declaration names it)."""

    class Foreign(BaseModel):
        x: int = 0

    with pytest.raises(ValueError, match="unknown") as exc_info:
        Chain(
            name="unknown-key",
            start="screen",
            steps={
                "screen": Step(
                    body=screen,
                    outcomes=Summary | Unreadable,
                    route=Route({Summary: "reduce", Unreadable: "dead_letter", Foreign: "reduce"}),
                ),
            },
        )
    assert "Foreign" in str(exc_info.value), str(exc_info.value)


# ── door 2: the runtime loud refusal for the element type with no arm ───


async def lying_screen(ctx: object, item: dict[str, object]) -> Summary | Unreadable:
    """THE BODY THAT LIED: returns an element of a THIRD type — outside
    the declared union."""

    class Ghost(BaseModel):
        doc_id: str

    return Ghost(doc_id="ghost-1")  # type: ignore[return-value]  # Why: the drill — the foreign element IS the runtime door's subject.


# ── THE PROOF: the flow, lived end-to-end ───────────────────────────────


@pytest.mark.integration
async def test_type_tagged_route_routes_each_element_by_its_type(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """THE WHOLE SCENARIO, LIVED: a fan-out map whose children produce
    the UNION per element (13 elements: 12 readable + the ghost), the
    type-tagged route dispatching EACH element BY ITS TYPE — the
    successes to the reducer, the failures to the dead-letter — with NO
    isinstance ladder in any single body, and the run terminal with the
    ledger as the receipt: the rows prove which body took which
    element."""
    from taskq.workflows import FlowRunner, WorkflowApp, build, chain_source

    async def the_source(ctx: StepContext) -> None:
        await ctx.emit_batch(
            [
                chain_start(THE_CHAIN, {"doc_id": doc}, map_index=i, trace_id=f"doc-{i}")
                for i, doc in enumerate(_BATCH)
            ],
            cursor={"page": 0},
        )

    app = WorkflowApp()

    @app.workflow("type_tagged_route_proof")
    def type_tagged_route_proof() -> Promise[object]:
        return build(chain_source(THE_CHAIN, the_source, key="the_source"))

    runner = FlowRunner(app.get("type_tagged_route_proof"), module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    rows = await wf_conn.fetch(  # type: ignore[attr-defined]
        f'SELECT step_key, map_index, status, result FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__' "
        "AND step_key <> 'the_source' AND map_index IS NOT NULL "
        "ORDER BY map_index, id",
        flow_id,
    )
    # THE LEDGER IS THE RECEIPT: per element, which body took it.
    paths: dict[int, list[tuple[str, str]]] = {}
    for r in rows:
        paths.setdefault(int(r["map_index"]), []).append((str(r["step_key"]), str(r["status"])))
    # Every element's chain: screen → its arm. The successes (12) took
    # the reducer; the ghost took the dead-letter.
    reduced = {i for i, steps in paths.items() if any(k == "reduce" for k, _ in steps)}
    dead = {i for i, steps in paths.items() if any(k == "dead_letter" for k, _ in steps)}
    assert len(paths) == len(_BATCH), f"an element's chain is missing from the ledger: {paths}"
    assert len(reduced) == 12, f"the successes did not all reach the reducer: {reduced}"
    assert dead == {12}, f"the ghost did not reach the dead-letter alone: {dead}"
    assert all(status == "succeeded" for steps in paths.values() for _, status in steps), paths
    # THE ARMS' OWN LEDGER: the reducer's rows carry the reduced SUMMARIES
    # (the element arrived AS its type — attribute access worked); the
    # dead-letter's row NAMES the ghost with its reason.
    reduce_rows = [r for r in rows if r["step_key"] == "reduce"]
    assert len(reduce_rows) == 12
    ghost_row = next(r for r in rows if r["step_key"] == "dead_letter")
    assert "doc-999" in str(ghost_row["result"]), ghost_row["result"]
    root = await wf_conn.fetchrow(  # type: ignore[attr-defined]
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root is not None and root["status"] == "succeeded", root


@pytest.mark.integration
async def test_the_lying_element_type_reds_the_runtime_door(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """The runtime door on the type-tagged face: an element whose type
    has NO arm (the body that lied about its type) fails the row LOUDLY
    — ``error_class='RouterNotTotal'`` — the record names the defect,
    the chain visibly dies, never silently drops."""
    from taskq.workflows import FlowRunner, WorkflowApp, build, chain_source

    lying_chain = Chain(
        name="lying-element-chain",
        start="screen",
        steps={
            "screen": Step(
                body=lying_screen,
                outcomes=Summary | Unreadable,
                route=Route({Summary: "reduce", Unreadable: "dead_letter"}),
            ),
            "reduce": Step(body=reduce_summary, outcomes=Summary, route=None),
            "dead_letter": Step(body=dead_letter, outcomes=Unreadable, route=None),
        },
    )

    async def lying_source(ctx: StepContext) -> None:
        await ctx.emit_batch(
            [chain_start(lying_chain, {"doc_id": "doc-001"}, map_index=0, trace_id="doc-0")],
            cursor={"page": 0},
        )

    app = WorkflowApp()

    @app.workflow("lying_element_route")
    def lying_element_route() -> Promise[object]:
        return build(chain_source(lying_chain, lying_source, key="lying_source"))

    runner = FlowRunner(app.get("lying_element_route"), module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)

    row = await wf_conn.fetchrow(  # type: ignore[attr-defined]
        f'SELECT status, error_class FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'screen'",
        flow_id,
    )
    assert row is not None
    assert row["status"] == "failed", dict(row)
    assert row["error_class"] == "RouterNotTotal", dict(row)


@pytest.mark.integration
async def test_the_misroute_is_loud_not_silent(
    wf_conn: object,
    wf_schema: str,
    module_pg_pool: object,
    wf_sql: object,
) -> None:
    """THE MIS-ROUTE RECEIPT: an arm body declaring the WRONG arm's type
    (the reducer declaring ``Unreadable`` while Summary elements route
    there) cannot silently swallow the element — the typed boundary's
    re-validation fails the row LOUDLY (the coercion refusal names the
    model)."""
    from taskq.workflows import FlowRunner, WorkflowApp, build, chain_source

    async def wrong_arm(ctx: object, item: Unreadable) -> dict[str, object]:
        # The reducer's body declared the WRONG arm: a Summary element
        # lands here and the typed boundary refuses it.
        return {"dead": item.doc_id}

    the_misroute_chain = Chain(
        name="misroute-chain",
        start="screen",
        steps={
            "screen": Step(
                body=screen,
                outcomes=Summary | Unreadable,
                route=Route({Summary: "reduce", Unreadable: "dead_letter"}),
            ),
            "reduce": Step(body=wrong_arm, outcomes=Unreadable, route=None),
            "dead_letter": Step(body=dead_letter, outcomes=Unreadable, route=None),
        },
    )

    async def source(ctx: StepContext) -> None:
        await ctx.emit_batch(
            [chain_start(the_misroute_chain, {"doc_id": "doc-001"}, map_index=0, trace_id="doc-0")],
            cursor={"page": 0},
        )

    app = WorkflowApp()

    @app.workflow("misroute_chain")
    def misroute_chain() -> Promise[object]:
        return build(chain_source(misroute_chain, source, key="misroute_source"))

    runner = FlowRunner(app.get("misroute_chain"), module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)

    row = await wf_conn.fetchrow(  # type: ignore[attr-defined]
        f'SELECT status, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'reduce'",
        flow_id,
    )
    assert row is not None, "the mis-routed element's row is gone — a silent drop"
    assert row["status"] == "failed", dict(row)
    assert row["error_class"] not in (None, ""), dict(row)


# ── the runtime door's unit face (no PG): the element with no arm ───────


def test_router_not_total_unit_face_on_the_type_tagged_arms() -> None:
    """The unit face of door 2: ``Chain.next_child`` raises the loud
    ``RouterNotTotal`` for an element whose TYPE has no arm — the
    foreign element is named by its type tag."""
    from typing import Any

    chain = THE_CHAIN
    with pytest.raises(RouterNotTotal) as exc_info:
        chain.next_child(
            "screen",
            {"doc_id": "not-a-model"},  # type: ignore[arg-type]  # Why: the drill — a foreign element IS the door's subject.
            payload={"wf_item": {"doc_id": "x"}},
            map_index=0,
        )
    assert "dict" in str(exc_info.value) or "no route" in str(exc_info.value), str(exc_info.value)
    _ = DONE  # the terminal stays imported for the readers of this file
    _ = Any
