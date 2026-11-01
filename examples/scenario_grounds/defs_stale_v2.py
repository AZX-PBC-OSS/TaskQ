"""Scenario 7's SECOND definition: the same workflow name, one step
removed. This is the deployment the worker wrongly carries — the
``prep`` body is gone from it (the redeploy dropped the step and
renamed its replacement). A row stamped ``prep`` by version one has no
body to resolve here."""

from __future__ import annotations

from pydantic import BaseModel

from taskq import actor
from taskq.workflows import Promise, StepContext, WorkflowApp, build, step

app = WorkflowApp()


class FinishOutcome(BaseModel):
    """The flow's own result (the v2 spelling)."""

    done: str


async def prep_v2_step(ctx: StepContext) -> str:
    raw = ctx.input if isinstance(ctx.input, dict) else {}
    return f"{raw.get('subject', 'subject')}: prepared by v2"


async def finish_step(ctx: StepContext, prep: str) -> FinishOutcome:
    return FinishOutcome(done=f"{prep} -> finished by v2")


@app.workflow("ground_stale")
def ground_stale() -> Promise[object]:
    prep = step(prep_v2_step, key="prep_v2", actor="wf_stale", queue="grounds-stale")
    return build(step(finish_step, prep, key="finish", actor="wf_stale", queue="grounds-stale"))


class Ping(BaseModel):
    """A plain job the registry requires the worker to carry (the
    worker's boot refuses an empty actor registry); nothing in the
    scenario enqueues it."""

    note: str = ""


@actor(name="grounds_ping", queue="grounds-stale")
async def grounds_ping(payload: Ping) -> Ping:
    return payload


#: The worker's ``--actors`` reference.
ACTORS: dict[str, object] = {"grounds_ping": grounds_ping}
