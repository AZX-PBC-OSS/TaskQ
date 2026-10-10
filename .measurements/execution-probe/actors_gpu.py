"""THE PROBE'S VANILLA WORKER BOOT MODULE.

Deliberately NO taskq.workflows import (§16.1 models the vanilla
deployment: the worker that never installed taskq[flows]). One PLAIN
actor on the "gpu" queue — the control: it proves the worker pool is
alive and claiming on the gpu queue, so any workflow row that stays
unexecuted on this pool is unexecuted for a REASON, not because the
pool is dead.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from taskq import actor

EVENTS = Path("/tmp/opencode/execution-probe/worker-events.log")


class PlainPayload(BaseModel):
    n: int = 0


def _record(kind: str, body: dict[str, Any]) -> None:
    EVENTS.parent.mkdir(parents=True, exist_ok=True)
    with EVENTS.open("a") as fh:
        fh.write(json.dumps({"kind": kind, **body}) + "\n")


@actor(queue="gpu")
async def gpu_control(payload: PlainPayload, ctx: Any) -> dict[str, object]:
    """THE CONTROL: a plain actor on the gpu queue, executes normally."""
    _record("plain-actor-ran", {"actor": "gpu_control", "job_id": str(ctx.job_id)})
    return {"ran": True, "at": dt.datetime.now(dt.UTC).isoformat()}


registry = {"gpu_control": gpu_control}  # type: ignore[dict-item]
