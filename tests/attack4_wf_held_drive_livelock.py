# Why: the schema is a fixture-derived test identifier, not user input; every value is $-bound.
"""ATTACK4 — the held-drive livelock (F-P4-HELD-DRIVE-NULL-DEADLINE).

A NO-DEADLINE hold is the LEGAL form (the W1 warning documents it), but
`drive(until="held")` reads the held representation through
`scheduled_at > now()` — NULL for a NULL deadline — and never sees the
hold: it spins max_ticks (5000 full dispatch polls, ~126 s measured).

This is a PHASE-3 finding (the runner predates PR-6): the pin is
strict-xfail with the REASON named — the fixer lands the cure (the hold
marker, `metadata ? 'hold'`, is the rows-alone predicate), removes the
marker, and the pin greens. A strict xfail that PASSES fails the suite,
so the flip is forced — the same mechanism the T17 contract probes used.
"""

from __future__ import annotations

import pytest
from pydantic import BaseModel

from taskq.workflows import FlowRunner, WorkflowApp, build, step


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


@pytest.mark.xfail(
    reason="F-P4-HELD-DRIVE-NULL-DEADLINE: the driver's held-question reads "
    "scheduled_at > now(), NULL for a legal no-deadline hold — the phase-3 "
    "fixer lands `metadata ? 'hold'` (see .measurements/"
    "p4-attack-held-drive-null-deadline.md)",
    strict=True,
)
async def test_drive_until_held_sees_a_no_deadline_hold(
    wf_pool: object, wf_schema: str
) -> None:
    app = WorkflowApp()

    @app.workflow("attack4_held_no_deadline")
    def attack4_held_no_deadline() -> object:
        async def body(ctx: object, params: Ingest) -> str:
            await ctx.wait_signal(Approval, reason="the legal eternal wait")  # type: ignore[attr-defined]  # Why: ctx is the runner's StepContext; the attribute is real.
            return "published"

        return build(step(body, Ingest(doc_id="d1"), key="review"))

    runner = FlowRunner(app.get("attack4_held_no_deadline"), wf_pool, wf_schema)  # type: ignore[arg-type]
    flow_id = await runner.create_flow()
    # The drive is BOUNDED (its own max_ticks pin) — the livelock class
    # this pin convicts is the WRONG ANSWER, not a hang.
    outcome = await runner.drive(flow_id, until="held", max_ticks=50)
    assert outcome == "held", f"the driver said {outcome!r} for a held run"
