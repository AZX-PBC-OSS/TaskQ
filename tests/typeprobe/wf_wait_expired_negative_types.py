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


# ── T28's TIMER PROBES — the durable timer's typed wake ─────────────────
# ``ctx.wait_until`` returns the TYPED ``TimerWake`` (never a union, never
# Any): the wake's fields (key/scheduled/fired_at) narrow, a wrong
# ``when`` argument reds, and a body reading a foreign attribute off the
# wake reds — the clock's receipt is a typed value the checker sees.


async def probe_timer_green_door(ctx: StepContext, params: Params) -> str:
    """THE TIMER'S GREEN DOOR (the shape the docs' §4b teaches): the
    wake is consumed as the typed value it is. This function must stay
    CLEAN (the gate reds on any error outside the markers)."""
    _ = params
    wake = await ctx.wait_until(30.0, key="probe")
    return f"{wake.key}:{wake.lateness_s >= 0.0}"


async def probe_timer_when_must_be_temporal(
    ctx: StepContext, params: Params
) -> str:  # MUST_ERROR(reportArgumentType, invalid-argument-type): the deadline is a datetime | float — a bare str is the authoring error the typed door refuses
    """THE WRONG `when`: a string deadline is not a clock reading — the
    signature refuses it at the checker (both checkers red the
    argument)."""
    _ = params
    wake = await ctx.wait_until(
        "tomorrow"
    )  # MUST_ERROR(reportArgumentType, invalid-argument-type): str is not datetime | float
    return wake.key


async def probe_timer_wake_bare_unwrap(
    ctx: StepContext, params: Params
) -> str:  # MUST_ERROR(reportAttributeAccessIssue, unresolved-attribute): the wake carries NO decision fields — a body reading a human gate's verdict off the clock's receipt reds
    """THE WAKE IS NOT A DECISION: a body that treats the timer's
    receipt as a human gate's answer (reading a `verdict` off it) reds
    — the timer's face has no fail-close to borrow fields from."""
    _ = params
    wake = await ctx.wait_until(30.0, key="probe")
    return (
        wake.verdict
    )  # MUST_ERROR(reportAttributeAccessIssue, unresolved-attribute): TimerWake carries no verdict
