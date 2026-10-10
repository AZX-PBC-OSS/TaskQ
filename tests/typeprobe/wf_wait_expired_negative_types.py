"""T26's AMENDMENT — the expiry-as-value probes: the wait's timeout face
returns the TYPED UNION (the declared decision models joined with the
house ``Expired`` member), so the CHECKER forces the fail-close arm.

Each ``MUST_ERROR(rules...)`` marker names a wrong-shape body that must
RED on BOTH pinned checkers (pyright 1.1.414, ty 0.0.85) with a rule-id
from the marker's DECLARED set.

THE RED-FIRST RECEIPT (the amendment's own): the bare-unwrap marker ran
against the PRE-UNION tree — ``wait_signal`` returned ``Any``, the
checker was SILENT on the exact shape that hangs an operator (a body
reading the decision off an outcome that may be the expiry); the
capture rides ``.measurements/t26-typegate-red-first.txt`` and the
union's landing is the flip. The exception face
(``SignalTimeoutError``) could never be forced: a body that never
catches it compiles clean.

The fall-through marker reds through the BODY's own declared return —
the loop-shape law's forcing — and stays as the second arm's drill.
"""

from __future__ import annotations

from pydantic import BaseModel

from taskq.workflows import Expired, StepContext


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Params(BaseModel):
    doc_id: str


async def probe_bare_unwrap_ignores_expired(ctx: StepContext, params: Params) -> Approval:
    """THE BARE UNWRAP: the body reads the decision straight off the
    outcome — but the outcome may be the EXPIRY member (the deadline
    passed the DB clock). The union's forcing: ``Expired`` carries no
    ``verdict``; the checker reds the attribute."""
    _ = params
    outcome = await ctx.wait_signal((Approval,), timeout_s=30.0)
    return Approval(
        verdict=outcome.verdict  # MUST_ERROR(reportAttributeAccessIssue, unresolved-attribute): the outcome is Approval | Expired — Expired carries no verdict; the fail-close arm is FORCED
    )


async def probe_match_ignores_expired(
    ctx: StepContext, params: Params
) -> Approval:  # MUST_ERROR(reportReturnType, invalid-return-type): the match below ignores the Expired arm — the fall-through implicitly returns None against the declared Approval (the marker rides the DEF line: the checker reports the fall-through at the signature's return annotation; the fail-close arm is not optional — the body's own signature enforces it)
    """THE MATCH THAT IGNORES THE ARM: only the decision case is
    handled — the Expired arm falls through and the body IMPLICITLY
    returns None against the declared return type."""
    _ = params
    outcome = await ctx.wait_signal((Approval,), timeout_s=30.0)
    match outcome:
        case Approval() as approval:
            return approval
        case _:
            pass


async def probe_the_green_door(ctx: StepContext, params: Params) -> Approval:
    """THE GREEN DOOR (the shape the docs teach): BOTH arms handled —
    the decision arm continues, the expiry arm is the fail-close. This
    function must stay CLEAN (the gate reds on any error outside the
    markers — a green-surface regression is a failure)."""
    _ = params
    outcome = await ctx.wait_signal((Approval,), timeout_s=30.0)
    match outcome:
        case Approval() as approval:
            return approval
        case Expired():
            # THE FAIL-CLOSE ARM — the closed union's second member,
            # named: the shape the docs teach.
            return Approval(verdict="finished-with-what-you-have")
        case _ as other:
            # THE TOTALITY BELT: ty's narrowing does not see the closed
            # union's totality through the class patterns alone — the
            # guard is the belt that keeps THIS door clean on both
            # checkers (unreachable when the union is closed).
            raise AssertionError(f"unreachable: the union is closed, got {other!r}")
