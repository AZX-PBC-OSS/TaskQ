"""Scenario 3's definitions: one actor capped at two.

The step declares ``max_concurrent=2``; the worker's dispatch claim
reads the cap from the actor's stored row and admits at most two of
this actor's rows per round. Ten runs race for those two seats."""

from __future__ import annotations

import asyncio

from pydantic import BaseModel

from taskq import actor
from taskq.workflows import Promise, StepContext, WorkflowApp, build, step

app = WorkflowApp()


class DampedOutcome(BaseModel):
    """The step's result: which run it served."""

    run: int


@app.actor(name="wf_damped", queue="grounds-damped", max_concurrent=2)
async def damped_step(ctx: StepContext) -> DampedOutcome:
    # The body is slow on purpose: the two-seat cap has to actually bind
    # while the other eight rows wait their turn.
    await asyncio.sleep(0.7)
    raw = ctx.input if isinstance(ctx.input, dict) else {}
    return DampedOutcome(run=int(raw.get("run", 0)))


@app.workflow("ground_damped")
def ground_damped() -> Promise[object]:
    return build(step(damped_step, key="work", actor="wf_damped", queue="grounds-damped"))


class Ping(BaseModel):
    """A plain job the registry requires the worker to carry (the
    worker's boot refuses an empty actor registry); nothing in the
    scenario enqueues it."""

    note: str = ""


@actor(name="grounds_ping", queue="grounds-damped")
async def grounds_ping(payload: Ping) -> Ping:
    return payload


#: The worker's ``--actors`` reference.
ACTORS: dict[str, object] = {"grounds_ping": grounds_ping}
