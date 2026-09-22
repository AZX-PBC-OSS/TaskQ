"""Private test-only worker entry point for the conservation chaos tests.

Invoked via ``sys.executable -m tests._worker_harness_consv`` by
``tests/test_rt_conservation_chaos.py``. Loads ``WorkerSettings`` from the
environment, runs the production ``_main`` bootstrap with the module's
actors, and honors one fault-injection switch:

``TASKQ_CONSV_KILL_ON_TERMINAL=1``
    Patch ``PostgresBackend.mark_succeeded`` so the process dies by SIGKILL
    the instant the terminal DB write commits - before the terminal
    state-change publish and the result fanout can run. What the outside
    world observes afterwards is exactly what a SIGKILL in that window
    leaves behind: the committed row, the attempt ledger, the event trail,
    and no fanout.

Without the switch the harness is a plain worker (the ``tests._worker_harness``
shape plus the actors the chaos tests dispatch).
"""

import asyncio
import os
import signal
import sys
from typing import Any

from pydantic import BaseModel

from taskq.actor import actor
from taskq.backend.postgres import PostgresBackend
from taskq.context import JobContext
from taskq.settings import WorkerSettings
from taskq.worker.run import _main

_QUEUE = "consv_q"


class ConsvPayload(BaseModel):
    calls: int = 6


@actor(name="consv_progress", queue=_QUEUE)
async def consv_progress(
    payload: ConsvPayload, ctx: JobContext[ConsvPayload]
) -> dict[str, str | int]:
    for step in range(payload.calls):
        await ctx.progress(step=step)
    return {"marker": "landed", "calls": payload.calls}


@actor(name="consv_slow", queue=_QUEUE)
async def consv_slow(payload: ConsvPayload, ctx: JobContext[ConsvPayload]) -> None:
    _ = payload
    await asyncio.sleep(30.0)
    _ = ctx


_original_mark_succeeded = PostgresBackend.mark_succeeded
_original_mark_succeeded_with_conn = PostgresBackend.mark_succeeded_with_conn


async def _mark_succeeded_then_sigkill(self: Any, *args: Any, **kwargs: Any) -> Any:
    row = await _original_mark_succeeded(self, *args, **kwargs)
    # The terminal write has committed (mark_succeeded is the single
    # statement that lands status + result + attempt row + event). Die the
    # way a SIGKILL does: no cleanup, no publish, no flush drain.
    os.kill(os.getpid(), signal.SIGKILL)
    return row  # pragma: no cover - the process is gone before this returns


async def _mark_succeeded_with_conn_then_sigkill(self: Any, *args: Any, **kwargs: Any) -> Any:
    row = await _original_mark_succeeded_with_conn(self, *args, **kwargs)
    os.kill(os.getpid(), signal.SIGKILL)
    return row  # pragma: no cover - the process is gone before this returns


if os.environ.get("TASKQ_CONSV_KILL_ON_TERMINAL") == "1":
    # The consumer's success path rides the pooled-with-conn variant; the
    # plain variant is patched too so any caller dies the same way.
    PostgresBackend.mark_succeeded = _mark_succeeded_then_sigkill  # type: ignore[assignment]
    PostgresBackend.mark_succeeded_with_conn = _mark_succeeded_with_conn_then_sigkill  # type: ignore[assignment]

if __name__ == "__main__":
    settings = WorkerSettings.load()
    registry: dict[str, Any] = {
        "consv_progress": consv_progress,
        "consv_slow": consv_slow,
    }
    with asyncio.Runner() as runner:
        sys.exit(runner.run(_main(settings, actor_registry=registry)))
