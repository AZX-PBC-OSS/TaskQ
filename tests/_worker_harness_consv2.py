"""Private test-only worker entry point for the round-2 conservation chaos.

Invoked via ``sys.executable -m tests._worker_harness_consv2`` by
``tests/test_rt_conservation_chaos2.py``.  Same shape as
``tests/_worker_harness_consv.py`` (env-configured WorkerSettings, the
production ``_main`` bootstrap, one fault-injection switch), with the
injection the round-2 compositions need:

``TASKQ_CONSV2_KILL_ON_ABANDON=1``
    Patch ``PostgresBackend.mark_abandoned`` so the process dies by
    SIGKILL while the abandon write is IN FLIGHT inside the drain's
    ``shield_with_retrieval``: the real write is started as a task, the
    harness waits past the statement's dispatch, then kills.  What the
    outside world observes afterwards is exactly what a SIGKILL in that
    window leaves behind: either the committed abandon (row terminal,
    attempt row present) or a still-running row whose only remaining
    owners are the lease lapse and the next leader's reclaim - never a
    third shape.

The actors record the body-run effects ledger the composition's
exactly-once invariant reconciles against; the ledger DSN/schema ride
``TASKQ_CONSV2_RUN_DSN`` / ``TASKQ_CONSV2_RUN_SCHEMA``.
"""

# ruff: noqa: S608  # Why: only the schema identifier is interpolated, validated by the settings boundary; every caller-controlled value uses $N binding.

import asyncio
import os
import signal
import sys
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq.actor import actor
from taskq.backend.postgres import PostgresBackend
from taskq.context import JobContext
from taskq.settings import WorkerSettings
from taskq.worker.run import _main

_QUEUE = "consv2_q"


class Consv2Payload(BaseModel):
    body_secs: float = 1.0
    calls: int = 4


async def _record_body_run(ctx: JobContext[Consv2Payload]) -> None:
    run_dsn = os.environ.get("TASKQ_CONSV2_RUN_DSN")
    run_schema = os.environ.get("TASKQ_CONSV2_RUN_SCHEMA")
    assert run_dsn is not None and run_schema is not None
    conn = await asyncpg.connect(run_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{run_schema}".sys_effects (job_id, attempt, actor, kind) '
            "VALUES ($1, $2, $3, 'run')",
            ctx.job_id,
            ctx.attempt,
            ctx.actor,
        )
    finally:
        await conn.close()


@actor(name="consv2_fenced", queue=_QUEUE)
async def consv2_fenced(payload: Consv2Payload, ctx: JobContext[Consv2Payload]) -> None:
    await _record_body_run(ctx)
    await asyncio.sleep(payload.body_secs)
    raise RuntimeError("fenced-body-done")


@actor(name="consv2_progress", queue=_QUEUE)
async def consv2_progress(payload: Consv2Payload, ctx: JobContext[Consv2Payload]) -> None:
    await _record_body_run(ctx)
    for _step in range(payload.calls):
        await ctx.progress(step=_step)
        await asyncio.sleep(0.15)
    await asyncio.sleep(payload.body_secs)


_original_mark_abandoned = PostgresBackend.mark_abandoned


async def _mark_abandoned_then_sigkill_mid_flight(self: Any, *args: Any, **kwargs: Any) -> Any:
    inner = asyncio.ensure_future(_original_mark_abandoned(self, *args, **kwargs))
    # The statement is dispatched on the pool connection inside this
    # window; the kill lands while the drain's shield is still awaiting,
    # so the write's fate (committed server-side or rolled back with the
    # dying connection) is exactly the mid-shield uncertainty under test.
    await asyncio.sleep(0.05)
    os.kill(os.getpid(), signal.SIGKILL)
    return await inner  # pragma: no cover - the process is gone before this


if os.environ.get("TASKQ_CONSV2_KILL_ON_ABANDON") == "1":
    PostgresBackend.mark_abandoned = _mark_abandoned_then_sigkill_mid_flight  # type: ignore[assignment]

if __name__ == "__main__":
    settings = WorkerSettings.load()
    registry: dict[str, Any] = {
        "consv2_fenced": consv2_fenced,
        "consv2_progress": consv2_progress,
    }
    with asyncio.Runner() as runner:
        sys.exit(runner.run(_main(settings, actor_registry=registry)))
