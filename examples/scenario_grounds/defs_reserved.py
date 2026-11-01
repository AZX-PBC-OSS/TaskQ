"""Scenario 4's definitions: two stages share two rendering seats.

The workflow's step and a competing plain actor declare the SAME
concurrency reservation (``grounds_scene_slots``, two slots). The
plain actor is the seat-holder the scenario races against; the
workflow's step has to wait for it to stand up."""

from __future__ import annotations

import asyncio

from pydantic import BaseModel

from taskq import actor
from taskq.ratelimit import ConcurrencyReservation
from taskq.workflows import Promise, StepContext, WorkflowApp, build, step

#: The two seats both stages contend for. One reservation, declared once,
#: shared by name: the workflow's step and the plain actor. The lease is
#: short on purpose — a denial's retry hint is derived from the held
#: seats' own lease expiries, so a short lease makes the denial's wait
#: short (the hint the parked row's snooze obeys).
SCENE_SLOTS = ConcurrencyReservation("grounds_scene_slots", slots=2, lease=5)

app = WorkflowApp()


class SceneOutcome(BaseModel):
    """The step's result: which scene it rendered."""

    scene: str


async def render_scene(ctx: StepContext) -> SceneOutcome:
    raw = ctx.input if isinstance(ctx.input, dict) else {}
    await asyncio.sleep(0.5)
    return SceneOutcome(scene=str(raw.get("scene", "")))


@app.workflow("ground_reserved")
def ground_reserved() -> Promise[object]:
    return build(
        step(
            render_scene,
            key="render",
            actor="wf_scenes",
            queue="grounds-scenes",
            reservations=(SCENE_SLOTS,),
        )
    )


class HolderPayload(BaseModel):
    """The plain actor's payload (nothing rides it; the actor just
    holds a seat)."""

    note: str = ""


@actor(name="scene_holder", queue="grounds-holder", reservations=[SCENE_SLOTS])
async def scene_holder(payload: HolderPayload) -> HolderPayload:
    # The competing stage: holds one of the two seats while it works.
    await asyncio.sleep(4)
    return payload


#: The worker's ``--actors`` reference (the boot refuses an empty registry).
ACTORS: dict[str, object] = {"scene_holder": scene_holder}
