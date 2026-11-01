"""Scenario 6's definitions: a flaky step that fails twice, then
succeeds on its third try. The ladder is the step's own (three
attempts); the body arms its failures on a counter."""

from __future__ import annotations

from pydantic import BaseModel

from taskq import actor
from taskq.workflows import Promise, StepContext, WorkflowApp, build, step

app = WorkflowApp()

#: The body's own failure counter (armed by the scenario's fresh run:
#: every run of THIS module fails twice, then succeeds).
ATTEMPTS_SO_FAR = {"count": 0}


class FlakyOutcome(BaseModel):
    """The flaky step's result: the attempt that finally succeeded."""

    succeeded_on_attempt: int


class Confirmed(BaseModel):
    """The downstream step's confirmation."""

    note: str
    attempt: int


async def flaky_step(ctx: StepContext) -> FlakyOutcome:
    ATTEMPTS_SO_FAR["count"] += 1
    if ATTEMPTS_SO_FAR["count"] <= 2:
        raise RuntimeError(f"the flaky body's armed failure #{ATTEMPTS_SO_FAR['count']}")
    return FlakyOutcome(succeeded_on_attempt=int(ctx.attempt))


async def confirm_step(ctx: StepContext, result: FlakyOutcome) -> Confirmed:
    return Confirmed(note="the flaky step finished", attempt=result.succeeded_on_attempt)


@app.workflow("ground_ladder")
def ground_ladder() -> Promise[object]:
    worked = step(flaky_step, key="flaky", max_attempts=3)
    return build(step(confirm_step, worked, key="confirm"))


class Ping(BaseModel):
    """A plain job the registry requires the worker to carry (the
    worker's boot refuses an empty actor registry); nothing in the
    scenario enqueues it."""

    note: str = ""


@actor(name="grounds_ping", queue="default")
async def grounds_ping(payload: Ping) -> Ping:
    return payload


#: The worker's ``--actors`` reference.
ACTORS: dict[str, object] = {"grounds_ping": grounds_ping}
