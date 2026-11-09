"""The child flow's join probes — the closed union: ``handle.wait()`` returns
``ChildOutcome[M] | Expired`` (``ChildOk[M] | ChildFailed |
ChildCancelled | Expired``), so the CHECKER forces the arms — the
``Decision | Expired`` pattern's sibling at the flow boundary.

Each ``MUST_ERROR(rules...)`` marker names a wrong-shape body that must
RED on both pinned checkers (pyright 1.1.414, ty 0.0.85) with a rule-id
from the marker's DECLARED set.

The bare-unwrap marker ran against the pre-child-flow
tree — ``spawn_flow`` did not exist, the parent's join had no typed
union to force anything; the capture rides
``.measurements/t28-typegate-.txt`` and the union's landing is
the flip."""

from __future__ import annotations

from pydantic import BaseModel

from taskq.workflows import ChildOk, Expired, StepContext


class SubReport(BaseModel):
    findings: list[str]


class Params(BaseModel):
    doc_id: str


async def probe_bare_unwrap_ignores_the_failed_arm(ctx: StepContext, params: Params) -> SubReport:
    """The bare unwrap: the body reads the result straight off the
    outcome — but the outcome may be ``ChildFailed``/``ChildCancelled``/
    ``Expired`` (the child died, the cascade ran, the join's patience
    expired). The union's forcing: NONE of them carries ``result``; the
    checker reds the attribute."""
    _ = params
    handle = await ctx.spawn_flow("some_child", input=None, result=SubReport)
    outcome = await handle.wait()
    return SubReport(
        findings=outcome.result.findings  # MUST_ERROR(reportAttributeAccessIssue, unresolved-attribute): the outcome is ChildOk | ChildFailed | ChildCancelled | Expired — the fail-close arms are forced
    )


async def probe_match_ignores_the_fail_close_arms(
    ctx: StepContext, params: Params
) -> SubReport:  # MUST_ERROR(reportReturnType, invalid-return-type): the match below handles only the ChildOk arm — the fail-close arms fall through and the body implicitly returns None against the declared SubReport (the checker reports the fall-through at the signature's return annotation)
    _ = params
    handle = await ctx.spawn_flow("some_child", input=None, result=SubReport)
    outcome = await handle.wait()
    match outcome:
        case ChildOk(result=report):
            return report
        case _:
            pass


async def probe_the_green_door(ctx: StepContext, params: Params) -> SubReport:
    """The green door (the shape the docs teach): every arm handled —
    the ok arm returns the child's result, the fail-close arms decide.
    This function must stay CLEAN (the gate reds on any error outside
    the markers — a green-surface regression is a failure)."""
    _ = params
    handle = await ctx.spawn_flow("some_child", input=None, result=SubReport)
    outcome = await handle.wait()
    match outcome:
        case ChildOk(result=report):
            return report
        case Expired():
            # the JOIN'S own patience expired — the fail-close: finish with
            # what the parent has.
            return SubReport(findings=[])
        case _ as other:
            # ty's narrowing does not see the closed union's totality through
            # the class patterns alone — the guard is the belt that keeps this
            # door clean on both checkers (unreachable when the union is
            # closed).
            raise AssertionError(f"unreachable: the union is closed, got {other!r}")
