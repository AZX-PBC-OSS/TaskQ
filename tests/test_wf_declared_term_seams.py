"""THE DECLARED-TERM SEAMS: the wiring faces' stated laws where a term is
declared twice — once by an actor handle, once by the wiring site — or
declared in a second accepted spelling.

Each pin below names a contract the adoption cures state in their own
docstrings and guides, at the input that tells the two declarations apart:

* the step's explicit retry terms override the wired handle's — including
  the value that equals the step's own default (three attempts);
* ``capture_policy=`` and its ``capture=`` alias are ONE value on BOTH
  decorators, and a disagreement is refused (never a silent winner);
* ``result_ttl=`` accepts a ``timedelta`` on the route arm and the stored
  form is the seconds number (the duration law's "every face").
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from pydantic import BaseModel

from taskq.workflows import (
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    build,
    route,
    step,
)
from taskq.workflows._worker_execution import project_workflow_actor_configs


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class Item(BaseModel):
    v: int


async def _ingest_body(ctx: StepContext, params: Ingest) -> Report:
    return Report(ref=params.doc_id)


async def _item_source(ctx: StepContext) -> list[Item]:
    return [Item(v=1)]


async def _item_arm(ctx: object, item: Item) -> Item:
    return item


def test_a_step_declaring_three_attempts_overrides_a_handle_declaring_seven() -> None:
    """THE EXPLICIT-THREE SEAM: ``step(max_attempts=3)`` is a DECLARED
    three — the same number as the step's own default, which is why the
    fold's sentinel comparison cannot see it. The wiring site's explicit
    term overrides the handle's seven, exactly as every other explicit
    step term does (``max_concurrent``, ``result_ttl``, ``start_to_close``)
    and as the fold's own docstring states ("the step's explicit param
    still overrides"). A step wired with three attempts against an actor
    declared for seven must ladder three times, not seven.
    """
    app = WorkflowApp()

    @app.actor(max_attempts=7)
    async def seven(ctx: StepContext, params: Ingest) -> Report:
        return await _ingest_body(ctx, params)

    @app.workflow("retry-explicit-three-flow")
    def wf() -> Promise[object]:
        return build(step(seven, Ingest(doc_id="d1"), key="probe", max_attempts=3))

    node = app.get("retry-explicit-three-flow").nodes["probe"]
    assert node.max_attempts == 3, (
        "the step's explicit max_attempts=3 was treated as 'unset': the "
        f"handle's declared {node.max_attempts} attempts won instead — the "
        "explicit step term never overrode, so this node ladders more "
        "attempts than its wiring declares"
    )


def test_a_workflow_capture_policy_and_capture_alias_disagreeing_is_refused() -> None:
    """TWO SPELLINGS, ONE VALUE: ``capture_policy=`` is the canonical
    workflow-decorator name and ``capture=`` rides as the alias; a
    declaration passing both with different values is refused (the
    ``@app.actor`` face refuses it). A silent winner here decides which
    payloads a flow captures — the author's ``none`` accidentally
    capturing ``all`` (or the reverse) is a data-handling decision made by
    argument order, not by the author.
    """
    app = WorkflowApp()

    with pytest.raises(ValueError, match="capture"):

        @app.workflow("capture-disagreement-flow", capture="all", capture_policy="none")
        def wf() -> Promise[object]:
            return build(step(_ingest_body, Ingest(doc_id="d1"), key="probe"))


def test_a_route_arms_timedelta_result_ttl_projects_as_seconds() -> None:
    """THE DURATION LAW'S ROUTE-ARM FACE: ``RouteArm(result_ttl=…)`` is a
    second-denominated term, so the duration law's "every face" accepts a
    ``timedelta`` and normalizes it to the seconds number that is the only
    stored form. The arm's terms project onto its effective cohort's
    ``ActorConfig`` — the row the boot's config sync writes. A
    ``timedelta`` left unnormalized reaches that row as a duration object
    where the column (and every reader of it) expects seconds.
    """
    app = WorkflowApp()

    @app.workflow("route-arm-ttl-flow")
    def wf() -> Promise[object]:
        src = step(_item_source, key="source")
        routed = route(
            src,
            {
                Item: RouteArm(
                    body=_item_arm,
                    result_ttl=timedelta(minutes=2),  # pyright: ignore[reportArgumentType]  # Why: the duration law says this face accepts a timedelta; the annotation refusing it is the same unnormalized face.
                )
            },
        )
        return build(routed)

    projected = [c for c in project_workflow_actor_configs() if c.actor == "wf"]
    assert projected, "the route arm's cohort never projected"
    assert projected[0].result_ttl == 120.0, (
        "the route arm's declared two-minute result_ttl did not project as "
        f"the seconds number: got {projected[0].result_ttl!r}"
    )
