"""THE CURE PROBE'S FLOW DEFINITIONS MODULE.

This is what a FLOWS-CAPABLE worker's boot imports: the app + the chain
live here, so the boot's F3 projection compiles the workflow (the D1
definition registry's population in the worker's process) and projects
the (actor, queue) cohorts — 'wf' on 'default' (the source node) and
'wf-gpu' on 'gpu' (the chain) — into actor_config, and stamps the
workers row ``workflow_execution: true``.

THE SPLIT PLACEMENT (the design decision (b) — the flow's queues map to
DISTINCT ACTOR NAMES): the source node rides actor 'wf' on queue
'default'; the chain's steps ride actor 'wf-gpu' on queue 'gpu'. One
actor name, one queue — the estate's own law; the gpu step is a
gpu-NAMED actor.

The module also carries one PLAIN actor (the control): the vanilla
coexistence proof — a worker that runs plain actors AND executes flow
bodies is one worker wearing both hats, the production shape.
"""

from __future__ import annotations

import datetime as dt
import enum
import json
import os
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from taskq import actor
from taskq.workflows import (
    DONE,
    Chain,
    Route,
    Step,
    WorkflowApp,
    build,
    chain_start,
    chain_source,
)

BASE = Path("/tmp/opencode/execution-cure")
EVENTS = BASE / "cure-events.log"


class ScreenOutcome(enum.Enum):
    CLEAN = "clean"
    FLAGGED = "flagged"


def record(kind: str, body: dict[str, Any]) -> None:
    """THE EVENT RECORD: which process ran what. The probe's verdicts
    read the pids off this file — a body executed in the WRONG process
    is the red the probe exists to convict."""
    BASE.mkdir(parents=True, exist_ok=True)
    with EVENTS.open("a") as fh:
        fh.write(
            json.dumps({"kind": kind, "pid": os.getpid(), "at": dt.datetime.now(dt.UTC).isoformat(), **body})
            + "\n"
        )


# ── the plain control actor (the vanilla coexistence) ───────────────────


class PingPayload(BaseModel):
    n: int = 0


@actor(queue="default")
async def ping(payload: PingPayload, ctx: Any) -> dict[str, object]:
    record("plain-actor-ran", {"actor": "ping", "job_id": str(ctx.job_id)})
    return {"pong": True, "pid": os.getpid()}


registry: dict[str, object] = {"ping": ping}


# ── the probe workflow: source on default, chain on gpu ─────────────────


async def source_body(ctx: Any) -> None:
    """The paged source: ONE page, two chain starts (the emit tx)."""
    record("body-ran", {"body": "source", "job_id": str(ctx.job_id)})
    await ctx.emit_batch(
        [
            chain_start(CHAIN, {"doc_id": f"doc-{i}"}, map_index=i, trace_id=f"doc-{i}")
            for i in (1, 2)
        ],
        cursor={"page": 0},
    )


async def screen(ctx: Any, item: dict[str, object]) -> ScreenOutcome:
    record("body-ran", {"body": "screen", "job_id": str(ctx.job_id), "item": item})
    return ScreenOutcome.CLEAN


CHAIN = Chain(
    name="gpu-chain",
    start="screen",
    steps={
        "screen": Step(
            body=screen,
            outcomes=ScreenOutcome,
            route=Route({ScreenOutcome.CLEAN: DONE, ScreenOutcome.FLAGGED: DONE}),
        ),
    },
    # THE SPLIT PLACEMENT's gpu side: a gpu-NAMED actor on the gpu queue
    # (design decision (b) — distinct actor names per queue; the
    # one-queue-per-actor law holds by construction).
    actor="wf-gpu",
    queue="gpu",
)

app = WorkflowApp()


@app.workflow("cure_flow")
def cure_flow() -> object:
    # THE SPLIT PLACEMENT's default side: the source node rides the
    # app's default actor on the default queue.
    src = chain_source(CHAIN, source_body, key="doc_source")
    return build(src)
