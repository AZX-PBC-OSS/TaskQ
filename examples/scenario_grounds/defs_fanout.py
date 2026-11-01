"""Scenario 1's definitions: a document batch fans out to two queues —
a fast lane for the light items and a heavy lane for the dense ones —
then an approval step holds the flow for a human before anything is
published."""

from __future__ import annotations

from pydantic import BaseModel

from taskq.workflows import Promise, StepContext, WorkflowApp, build, map_source, step

app = WorkflowApp()


class Batch(BaseModel):
    """The run's input: the batch's light items and dense items."""

    fast: list[str]
    heavy: list[str]


class Polished(BaseModel):
    """One item's processed result."""

    doc_id: str
    lane: str


class Verdict(BaseModel):
    """The approval step's answer once a human decides."""

    approved: bool
    count: int
    note: str = ""


class Report(BaseModel):
    """The flow's published result."""

    published: list[str]
    verdict: str


async def split_fast(ctx: StepContext) -> list[str]:
    batch = Batch.model_validate(ctx.input)
    return batch.fast


async def split_heavy(ctx: StepContext) -> list[str]:
    batch = Batch.model_validate(ctx.input)
    return batch.heavy


async def polish_fast(ctx: StepContext, doc_id: str) -> Polished:
    return Polished(doc_id=doc_id, lane="fast")


async def polish_heavy(ctx: StepContext, doc_id: str) -> Polished:
    return Polished(doc_id=doc_id, lane="heavy")


class Approval(BaseModel):
    """The human's typed reply to the gate."""

    verdict: str  # "approve" | "reject"
    note: str = ""


async def approval_gate(ctx: StepContext, fast: list[Polished], heavy: list[Polished]) -> Verdict:
    """The fan-in IS the gate: both lanes' collected results arrive here,
    then the step holds until a human answers."""
    outcome = await ctx.wait_signal((Approval,), timeout_s=300.0)
    if isinstance(outcome, Approval):
        return Verdict(
            approved=outcome.verdict == "approve",
            count=len(fast) + len(heavy),
            note=outcome.note,
        )
    # The deadline passed with no answer: the fail-close arm — publish
    # nothing, say why.
    return Verdict(approved=False, count=0, note="the approval's deadline passed")


async def publish(ctx: StepContext, verdict: Verdict) -> Report:
    if not verdict.approved:
        return Report(published=[], verdict=f"withheld: {verdict.note}")
    return Report(published=[f"{verdict.count}-items"], verdict="published")


@app.workflow("ground_fanout")
def ground_fanout() -> Promise[object]:
    fast_items = step(split_fast, key="split_fast")
    fast = map_source(fast_items, polish_fast, queue="grounds-fast")
    heavy_items = step(split_heavy, key="split_heavy")
    heavy = map_source(heavy_items, polish_heavy, queue="grounds-heavy")
    approved = step(approval_gate, fast, heavy, key="approval")
    return build(step(publish, approved, key="publish"))
