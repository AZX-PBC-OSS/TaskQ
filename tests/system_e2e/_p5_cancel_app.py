"""Generic P5 fleet bodies; every observation is an actual application row."""

# ruff: noqa: S608  # the schema identifier is the validated runtime carrier; values bind.

from __future__ import annotations

import asyncio
import enum
from datetime import timedelta
from typing import Any

from taskq.retry import RetryPolicy
from taskq.workflows import (
    Chain,
    ExecutionPolicy,
    ExhaustionEvent,
    Promise,
    RetrySpec,
    Step,
    StepContext,
    TerminalEffect,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
)

P5_APP = WorkflowApp()


class P5Outcome(enum.Enum):
    DONE = "done"


# A terminal-effect target whose ordinary body fails once; its effect writes a
# separate application result and reports the validated event to that same row.
async def terminal_failure(ctx: StepContext, item: object) -> P5Outcome:
    raise RuntimeError("one bounded terminal failure")


async def close_terminal(ctx: StepContext, event: ExhaustionEvent) -> None:
    schema = ctx._wsql.schema  # pyright: ignore[reportPrivateUsage]
    async with ctx._pool.acquire() as conn, conn.transaction():  # pyright: ignore[reportPrivateUsage]
        invocations = await conn.fetchval(
            f"""INSERT INTO "{schema}".p5_effect_results
                (flow_id,node_key,run_epoch,record_id,error_class,invocations)
               VALUES ($1,$2,$3,$4,$5,1)
            ON CONFLICT (flow_id,node_key,run_epoch)
            DO UPDATE SET invocations=p5_effect_results.invocations+1
            RETURNING invocations""",
            event.flow_id,
            event.node_key,
            event.run_epoch,
            1,
            event.error.error_class,
        )
    if ctx.input == "kill-after-domain" and invocations == 1:
        # The body committed its dedupe marker and domain effect, but has no
        # engine terminal receipt. A real process kills itself in that window;
        # surviving workers use the durable claim, never a fake kill helper.
        import os
        import signal

        os.kill(os.getpid(), signal.SIGKILL)


P5_EFFECT_CHAIN = Chain(
    name="p5-effect-chain",
    start="work",
    steps={
        "work": Step(
            body=terminal_failure,
            outcomes=P5Outcome,
            policy=ExecutionPolicy(
                retry=RetrySpec(
                    policy=RetryPolicy(max_attempts=1),
                    on_exhaustion=TerminalEffect(
                        body=close_terminal,
                        policy=ExecutionPolicy(
                            actor="p5-effect-handler",
                            queue="default",
                            retry=RetrySpec(policy=RetryPolicy(max_attempts=2)),
                        ),
                    ),
                )
            ),
        )
    },
)


async def effect_page(ctx: StepContext) -> None:
    await ctx.emit_batch(
        [chain_start(P5_EFFECT_CHAIN, {"record": 1}, map_index=1, trace_id="effect-r1")],
        cursor={"page": 0},
    )


@P5_APP.workflow("p5_effect")
def p5_effect() -> Promise[object]:
    return build(chain_source(P5_EFFECT_CHAIN, effect_page, key="source"))


async def close_after_first_deadline(ctx: StepContext, event: ExhaustionEvent) -> None:
    if ctx.attempt == 1:
        await asyncio.sleep(0.2)
        raise AssertionError("the failed deadline enforcement never reached its retry")
    await close_terminal(ctx, event)


P5_DEADLINE_CHAIN = Chain(
    name="p5-deadline-effect-chain",
    start="work",
    steps={
        "work": Step(
            body=terminal_failure,
            outcomes=P5Outcome,
            policy=ExecutionPolicy(
                retry=RetrySpec(
                    policy=RetryPolicy(max_attempts=1),
                    on_exhaustion=TerminalEffect(
                        body=close_after_first_deadline,
                        policy=ExecutionPolicy(
                            actor="p5-deadline-handler",
                            queue="default",
                            start_to_close=timedelta(seconds=0.05),
                            retry=RetrySpec(
                                policy=RetryPolicy(
                                    max_attempts=2,
                                    base=timedelta(seconds=0.001),
                                    cap=timedelta(seconds=0.001),
                                )
                            ),
                        ),
                    ),
                )
            ),
        )
    },
)


async def effect_deadline_page(ctx: StepContext) -> None:
    await ctx.emit_batch(
        [
            chain_start(
                P5_DEADLINE_CHAIN,
                {"record": 1},
                map_index=1,
                trace_id="effect-deadline-r1",
            )
        ],
        cursor={"page": 0},
    )


@P5_APP.workflow("p5_effect_deadline")
def p5_effect_deadline() -> Promise[object]:
    return build(chain_source(P5_DEADLINE_CHAIN, effect_deadline_page, key="source"))


P5_FLOWS: dict[str, Any] = {
    name: P5_APP.get(name)
    for name in (
        "p5_effect",
        "p5_effect_deadline",
    )
}
