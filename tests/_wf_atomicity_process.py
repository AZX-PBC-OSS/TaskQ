# ruff: noqa: S608  # Schema is the validated worker setting and values are $-bound.
"""Subprocess-owned application for two-process joined-page visibility proof."""

from __future__ import annotations

import asyncio
import os
from contextlib import AsyncExitStack

import asyncpg

from taskq.obs import setup_logging
from taskq.settings import WorkerSettings
from taskq.worker.run import _main
from taskq.workflows import (
    Chain,
    Promise,
    Step,
    StepContext,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
)
from tests._wf_atomicity_fixture import emit_joined


async def screen(ctx: StepContext, item: object) -> str:
    return "screened"


PROCESS_FLOW = "joined_page_two_process"
PROCESS_CHAIN = Chain(
    name="joined_page_records",
    start="screen",
    steps={"screen": Step(screen, str)},
)

APP = WorkflowApp()


@APP.workflow(PROCESS_FLOW)
def process_flow() -> Promise[object]:
    return build(chain_source(PROCESS_CHAIN, process_source, key="source", max_attempts=1))


async def process_source(ctx: StepContext) -> object:
    dsn = os.environ["TASKQ_PG_DSN"]
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    workflow_timeout = WorkerSettings.load().default_start_to_close
    if workflow_timeout is None or workflow_timeout.total_seconds() <= 0:
        raise AssertionError(
            "the process gate requires its actual configured workflow timeout, "
            f"default_start_to_close={workflow_timeout!r}"
        )
    deadline = asyncio.get_running_loop().time() + workflow_timeout.total_seconds()
    async with AsyncExitStack() as connections:
        caller = await asyncpg.connect(dsn)
        connections.push_async_callback(caller.close)
        signal = await asyncpg.connect(dsn)
        connections.push_async_callback(signal.close)
        await signal.execute(
            f'UPDATE "{schema}".atomicity_process_gate SET entered=true WHERE flow_id=$1',
            ctx.flow_id,
        )
        async with caller.transaction():
            await caller.execute(
                f'INSERT INTO "{schema}".atomicity_domain (flow_id,value) VALUES ($1,$2)',
                ctx.flow_id,
                "two-process page",
            )
            await caller.execute(
                f'INSERT INTO "{schema}".atomicity_stage (flow_id,ordinal,digest) VALUES ($1,0,$2)',
                ctx.flow_id,
                "two-process-digest",
            )
            emitted = await emit_joined(
                ctx,
                [
                    chain_start(
                        PROCESS_CHAIN, {"record_id": 200}, map_index=200, trace_id="record-200"
                    )
                ],
                cursor={"page": 0},
                connection=caller,
                schema=schema,
            )
            if len(emitted) != 1:
                raise AssertionError("joined emit did not return exactly one child")
            while True:
                gate = await signal.fetchrow(
                    f'SELECT entered,release FROM "{schema}".atomicity_process_gate '
                    "WHERE flow_id=$1",
                    ctx.flow_id,
                )
                if gate is not None and gate["release"]:
                    break
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise AssertionError(
                        "the process gate exceeded the actual configured workflow timeout "
                        f"({workflow_timeout!r}); last gate row={None if gate is None else dict(gate)!r}"
                    )
                await asyncio.sleep(min(0.02, remaining))
    # The connection objects themselves prove both async close callbacks completed.
    assert caller.is_closed() and signal.is_closed()
    return {"pages": 1}


if __name__ == "__main__":
    settings = WorkerSettings.load()
    # Match the production CLI boundary so dispatch_batch's real DEBUG records are
    # durable in this invocation's run-scoped sink, including zero-row attempts.
    setup_logging(
        level=settings.log_level,
        log_format=settings.log_format,
        events_level=settings.log_events_level,
    )
    with asyncio.Runner() as runner:
        raise SystemExit(runner.run(_main(settings, actor_registry={})))
