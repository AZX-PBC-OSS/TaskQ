"""Scenario 5's definitions: a small fork-and-join flow and a flow with
an approval at its foot — the shapes whose history rows the retention
sweep retires once the run has been terminal past the horizon."""

from __future__ import annotations

from pydantic import BaseModel

from taskq import actor
from taskq.workflows import Promise, StepContext, WorkflowApp, build, step

app = WorkflowApp()


class Fact(BaseModel):
    """One branch's finding."""

    branch: str
    finding: str


class Summary(BaseModel):
    """The join's packed result."""

    findings: list[str]


async def source_step(ctx: StepContext) -> str:
    raw = ctx.input if isinstance(ctx.input, dict) else {}
    return str(raw.get("subject", "the subject"))


async def branch_b(ctx: StepContext, subject: str) -> Fact:
    # The branch's own ladder: its first attempt fails, its second
    # succeeds — the attempts' ledger rows are the record the sweep
    # will retire.
    if ctx.attempt < 2:
        raise RuntimeError("branch b's armed first-attempt failure")
    return Fact(branch="b", finding=f"{subject}: the second opinion")


async def branch_c(ctx: StepContext, subject: str) -> Fact:
    return Fact(branch="c", finding=f"{subject}: the third opinion")


async def join_step(ctx: StepContext, b: Fact, c: Fact) -> Summary:
    return Summary(findings=[b.finding, c.finding])


@app.workflow("ground_retained")
def ground_retained() -> Promise[object]:
    subject = step(source_step, key="a")
    b = step(branch_b, subject, key="b")
    c = step(branch_c, subject, key="c")
    return build(step(join_step, b, c, key="join"))


class Keep(BaseModel):
    """The keeper flow's answer (no holds, no history worth keeping)."""

    kept: bool


class GateReply(BaseModel):
    """The hold flow's human reply."""

    verdict: str
    note: str = ""


async def held_step(ctx: StepContext) -> Keep:
    """A step that waits on a human: the flow stays LIVE (never aged)
    until the answer — or forever, if none comes."""
    outcome = await ctx.wait_signal((GateReply,), timeout_s=3600.0)
    return Keep(kept=isinstance(outcome, GateReply))


@app.workflow("ground_keeper")
def ground_keeper() -> Promise[object]:
    return build(step(held_step, key="wait"))


class Ping(BaseModel):
    """A plain job the registry requires the worker to carry (the
    worker's boot refuses an empty actor registry); nothing in the
    scenario enqueues it."""

    note: str = ""


@actor(name="grounds_ping", queue="grounds-retained")
async def grounds_ping(payload: Ping) -> Ping:
    return payload


#: The worker's ``--actors`` reference.
ACTORS: dict[str, object] = {"grounds_ping": grounds_ping}
