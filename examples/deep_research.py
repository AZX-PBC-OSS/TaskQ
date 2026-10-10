"""The worked HITL example (T26): the deep-research loop LIVE — the
maintainer's exact scenario, wired behind the broadcast.

The scenario: an agent researches a topic pass by pass. THE FIRST THREE
PASSES ARE FREE — nobody is asked. Past three, the loop needs a
human's approval to keep going: ``ctx.wait_signal((ContinueApproval,),
timeout_s=APPROVAL_TIMEOUT_S)``. The approval request is a ROW and a
BROADCAST: ``pg_notify`` on the ``taskq_wf_hold`` channel (the typed
listener — ``taskq.workflows.HitlListener`` — backfills the open holds
at start, then tails the NOTIFYs; a browser watches the same stream
over the admin's ``/sse/holds`` topic). A human answers through
``HitlClient.resolve`` (the admin's Resolve form rides the same door) —
approved, the loop continues to the full report.

**THE FAIL-CLOSE (the named result)**: nobody watching is not a hang
and not a crash — the wait's outcome is the CLOSED UNION
``ContinueApproval | Expired`` (T26's amendment: the expiry is a VALUE,
never an exception), and the body MATCHES the ``Expired`` member and
returns ``Done(carry.finish_with_what_you_have())``: the run SUCCEEDS
carrying the finish-with-what-you-have state. The user not watching is
a RESULT, typed, named, and CHECKER-FORCED (a body that ignores the arm
reds; a body that wants the FAILURE raises ``SignalTimeoutError``
itself off the member — the escalation ladder's own use).

THE LOOP'S SHAPE LAW, honored on purpose: a loop body declares ONE wait
per iteration (the answer-queue's cursor IS the iteration counter — a
wait on only SOME iterations mis-indexes the queue). The scenario's
"three loops" are therefore three research PASSES inside the first
iteration; the loop's OWN iterations are the approved extensions past
them. The body re-executes FROM THE TOP on resume — the research is
deterministic over the fake corpus, so the replay is cheap (the
re-execution doctrine).

The demo drives its own flows in-process (the same rows a worker
process would drive); ``APPROVAL_TIMEOUT_S`` is the REAL 120 s default
— tests scale it by monkeypatch, never by redefining the example.
"""

from __future__ import annotations

from typing import Any, Final

import asyncpg
from pydantic import BaseModel, Field

from taskq.workflows import (
    Done,
    Expired,
    FlowRunner,
    Promise,
    Refine,
    StepContext,
    WorkflowApp,
    build,
    loop,
)
from taskq.workflows.api import GateDecl
from taskq.workflows.ledger import RunClaim

#: THE REAL DEFAULT — the approval window a human gets (two minutes).
#: The tests scale this constant (condition-not-clock); the shipped
#: example waits the REAL two minutes before the fail-close.
APPROVAL_TIMEOUT_S: Final[float] = 120.0

#: The scenario's free march: three research passes before the gate.
FREE_PASSES: Final[int] = 3

#: The full report's bar: the approved extension's passes land here.
REPORT_PASSES: Final[int] = 5

#: The demo's fake corpus (the stand-in sources the loop "reads").
_CORPUS: Final[dict[str, str]] = {
    "pg-docs": "LISTEN/NOTIFY delivers on commit; a rolled-back tx is silent.",
    "hitl-notes": "The hold row is the truth; the knock is a pointer.",
    "research-101": "Three passes set the frame; a human sets the bar.",
    "broadcast-lab": "Backfill first, tail second — the missed window is unrepresentable.",
    "fail-close-9": "Finish with what you have beats waiting forever.",
    "loop-shapes": "One wait per iteration; the cursor is the counter.",
}

_FINISHED_WITH_WHAT_YOU_HAVE: Final[str] = "finished_with_what_you_have"
_REPORT_COMPLETE: Final[str] = "report_complete"


class ContinueApproval(BaseModel):
    """THE GATE'S PAYLOAD: the human's answer to "keep researching?"."""

    approved: bool
    note: str = ""


class ResearchState(BaseModel):
    """The carry: the notes so far + the gate's verdict + the report's
    named status (the fail-close's TYPED face)."""

    topic: str = "notify-broadcasts"
    notes: list[str] = Field(default_factory=list)
    approved: bool = False
    status: str = "researching"
    note: str = ""

    def research_passes(self, n: int) -> ResearchState:
        """The deterministic research (the resume replays it cheap):
        pass k reads source k and appends its digest."""
        notes = list(self.notes)
        for k in range(len(notes), len(notes) + n):
            source = sorted(_CORPUS)[k % len(_CORPUS)]
            notes.append(f"pass {k + 1}: {source} — {_CORPUS[source]}")
        return self.model_copy(update={"notes": notes})

    def finish_with_what_you_have(self, *, note: str = "") -> ResearchState:
        """THE FAIL-CLOSE'S NAMED RESULT: the notes stand, the report
        ships incomplete ON ITS OWN TERMS — never a hang, never a
        silent drop."""
        return self.model_copy(update={"status": _FINISHED_WITH_WHAT_YOU_HAVE, "note": note})


async def research_iteration(
    ctx: StepContext, carry: ResearchState
) -> Done[ResearchState] | Refine[ResearchState]:
    """The deep-research iteration: the free march (three passes), the
    approval gate, then one more pass per approved iteration until the
    report's bar. THE EXPIRY IS A VALUE (T26's amendment): the wait's
    outcome is the closed union ``ContinueApproval | Expired`` — the
    checker forces the fail-close arm (and it can ONLY because the
    body's ctx carries the REAL type: an ``Any`` ctx matches on Any and
    forces nothing)."""
    # THE FREE MARCH: the scenario's first three passes need nobody.
    # (Inside the loop's FIRST iteration — the shape law: the answer
    # queue's cursor is the iteration counter, so the gate runs once
    # per iteration from here on, never on only some of them.)
    if not carry.notes:
        carry = carry.research_passes(FREE_PASSES)
    if not carry.approved:
        outcome = await ctx.wait_signal(
            (ContinueApproval,),
            timeout_s=APPROVAL_TIMEOUT_S,  # the REAL default: 120.0
            reason="the research loop wants to continue past the free passes",
            tool="continue_approval",
            args={"topic": carry.topic, "passes_done": len(carry.notes)},
        )
        match outcome:
            case ContinueApproval() as approval:
                if not approval.approved:
                    # THE HUMAN SAID STOP: the same named result — the
                    # gate's note rides the state (the operator sees WHY
                    # it stopped).
                    return Done(carry.finish_with_what_you_have(note=approval.note))
                carry = carry.model_copy(update={"approved": True})
            case Expired():
                # THE FAIL-CLOSE: nobody watching — the loop finishes
                # with what it has (the typed expiry is a RESULT, never
                # a hang and never a crash).
                return Done(carry.finish_with_what_you_have())
    # THE APPROVED EXTENSION: one more pass per approved iteration; the
    # report ships at the bar.
    carry = carry.research_passes(1)
    if len(carry.notes) >= REPORT_PASSES:
        return Done(carry.model_copy(update={"status": _REPORT_COMPLETE}))
    return Refine(carry)


dr_app = WorkflowApp()

CONTINUE_GATE = GateDecl(
    name="ContinueApproval", payload_models=(ContinueApproval,), timeout_s=APPROVAL_TIMEOUT_S
)


@dr_app.workflow("deep_research")
def deep_research() -> Promise[object]:
    """The loop IS the terminal: the Done payload (the ResearchState)
    is the run's result — readable from the rows alone."""
    research = loop(
        "research",
        research_iteration,
        initial=ResearchState(),
        max_iterations=6,
        budget_s=3600.0,
        on_exhausted="escalate",
        gates=(CONTINUE_GATE,),
    )
    return build(research)


async def trigger_deep_research(
    pool: asyncpg.Pool, schema: str, topic: str = "notify-broadcasts"
) -> RunClaim:
    """One deep-research run (the demo's trigger face)."""
    runner = FlowRunner(dr_app.get("deep_research"), pool, schema)
    return await runner.create_flow(input={"topic": topic})


async def approve_continue(client: Any, hold_id: str, *, note: str = "go on") -> Any:
    """The human's yes, through the SAME typed door the admin's Resolve
    form rides (unchanged by T26 — the broadcast is the read face)."""
    return await client.resolve(hold_id, ContinueApproval(approved=True, note=note).model_dump())
