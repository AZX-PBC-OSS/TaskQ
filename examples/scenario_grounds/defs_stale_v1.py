"""Scenario 7's definitions: one workflow under two definitions.

The scenario's own process compiles THIS module's graph (version one:
``prep`` then ``finish``) and creates the run with it. The worker
carries the SECOND module (``defs_stale_v2``): the old ``prep`` body is
gone from it — the redeploy removed the step. The rows, stamped by
version one, must meet version two's definitions cleanly: the surviving
step runs on, the missing one waits loudly, and when the definitions
come back the run finishes."""

from __future__ import annotations

from pydantic import BaseModel

from taskq import actor
from taskq.workflows import Promise, StepContext, WorkflowApp, build, step

app = WorkflowApp()


class PrepOutcome(BaseModel):
    """What the preparation step produced."""

    prepared: str


class FinishOutcome(BaseModel):
    """The flow's own result."""

    done: str


async def prep_step(ctx: StepContext) -> PrepOutcome:
    raw = ctx.input if isinstance(ctx.input, dict) else {}
    return PrepOutcome(prepared=f"{raw.get('subject', 'subject')}: prepared")


async def finish_step(ctx: StepContext, prep: PrepOutcome) -> FinishOutcome:
    return FinishOutcome(done=f"{prep.prepared} -> finished")


@app.workflow("ground_stale")
def ground_stale() -> Promise[object]:
    prep = step(prep_step, key="prep", actor="wf_stale", queue="grounds-stale")
    return build(step(finish_step, prep, key="finish", actor="wf_stale", queue="grounds-stale"))


class Ping(BaseModel):
    """A plain job the registry requires the worker to carry (the
    worker's boot refuses an empty actor registry); nothing in the
    scenario enqueues it."""

    note: str = ""


@actor(name="grounds_ping", queue="grounds-stale")
async def grounds_ping(payload: Ping) -> Ping:
    return payload


@actor(name="grounds_default_ping", queue="default")
async def grounds_default_ping(payload: Ping) -> Ping:
    return payload


#: The worker's ``--actors`` reference.
ACTORS: dict[str, object] = {
    "grounds_ping": grounds_ping,
    "grounds_default_ping": grounds_default_ping,
}
