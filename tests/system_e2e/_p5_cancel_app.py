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
    Done,
    ExecutionPolicy,
    ExhaustionEvent,
    Promise,
    Refine,
    RetrySpec,
    Step,
    StepContext,
    TerminalEffect,
    WorkflowApp,
    build,
    chain_source,
    chain_start,
    loop,
    map_source,
    step,
)

P5_APP = WorkflowApp()


class P5Outcome(enum.Enum):
    DONE = "done"


def _mode(ctx: StepContext) -> str:
    return str(ctx.input)


async def record(
    ctx: StepContext,
    observation: str,
    *,
    incarnations: int | None = None,
) -> None:
    """Application-owned persistence, not a log-only witness or a weak proxy."""
    origin = str(ctx.cancel_origin.name)
    schema = ctx._wsql.schema  # pyright: ignore[reportPrivateUsage]
    await ctx._pool.execute(  # pyright: ignore[reportPrivateUsage]  # system application proof reads its actual worker context
        f"""INSERT INTO "{schema}".p5_cancel_observations
            (flow_id,node_key,attempt,observation,origin,reason,principal,
             worker_id,incarnations)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (flow_id,node_key,attempt,observation) DO NOTHING""",
        ctx.flow_id,
        ctx.node_key,
        ctx.attempt,
        observation,
        origin,
        ctx.cancel_reason,
        ctx.cancel_principal,
        ctx._worker_id,  # pyright: ignore[reportPrivateUsage]  # extra witness only; worker claim rows are the second proof
        incarnations,
    )


async def _await_cooperative(ctx: StepContext) -> None:
    await record(ctx, "entered")
    await ctx.cancel_event.wait()
    await record(ctx, "observed", incarnations=1)


async def static_body(ctx: StepContext) -> None:
    schema = ctx._wsql.schema  # pyright: ignore[reportPrivateUsage]
    if _mode(ctx) == "shutdown":
        previously_observed = await ctx._pool.fetchval(  # pyright: ignore[reportPrivateUsage]
            f"""SELECT EXISTS (
                SELECT 1 FROM "{schema}".p5_cancel_observations
                WHERE flow_id=$1 AND origin='SHUTDOWN'
            )""",
            ctx.flow_id,
        )
        if previously_observed:
            await record(ctx, "replayed")
            return
    await _await_cooperative(ctx)
    if _mode(ctx) == "operator-raise":
        raise RuntimeError("ordinary failure after operator cancellation")
    if _mode(ctx) == "shutdown":
        raise RuntimeError("ordinary shutdown body exit; never the retry ladder")


async def source_window(ctx: StepContext) -> list[int]:
    return [1, 2]


async def map_body(ctx: StepContext, item: int) -> None:
    await static_body(ctx)


async def chain_context(ctx: StepContext, item: object) -> P5Outcome:
    await static_body(ctx)
    return P5Outcome.DONE


P5_CHAIN = Chain(
    name="p5-cancel-chain",
    start="work",
    steps={
        "work": Step(
            body=chain_context,
            outcomes=P5Outcome,
            policy=ExecutionPolicy(retry=RetrySpec(policy=RetryPolicy(max_attempts=1))),
        )
    },
)


async def chain_page(ctx: StepContext) -> None:
    if _mode(ctx) == "shutdown" and ctx.attempt > 1:
        await record(ctx, "replayed")
        await ctx.emit_batch(
            [chain_start(P5_CHAIN, {"record": 1}, map_index=1, trace_id="p5-r1")],
            cursor={"page": 0},
        )
        return
    await static_body(ctx)


@P5_APP.workflow("p5_static")
def p5_static() -> Promise[object]:
    return build(step(static_body, key="static", max_attempts=1))


@P5_APP.workflow("p5_map")
def p5_map() -> Promise[object]:
    source = step(source_window, key="source")
    return build(map_source(source, map_body, max_attempts=1))


@P5_APP.workflow("p5_chain")
def p5_chain() -> Promise[object]:
    return build(chain_source(P5_CHAIN, chain_page, key="chain_source"))


@P5_APP.workflow("p5_chain_source")
def p5_chain_source() -> Promise[object]:
    # The source IS the gated body for this cell. No page exists before it exits.
    return build(chain_source(P5_CHAIN, chain_page, key="chain_source"))


async def loop_body(ctx: StepContext, carry: int) -> Done[None] | Refine[int]:
    await static_body(ctx)
    return Done(None)


@P5_APP.workflow("p5_loop")
def p5_loop() -> Promise[object]:
    return build(loop("loop", loop_body, initial=0, max_iterations=2))


# A terminal-effect target whose ordinary body fails once; its effect writes a
# separate application result and reports the validated event to that same row.
async def terminal_failure(ctx: StepContext, item: object) -> P5Outcome:
    raise RuntimeError("one bounded terminal failure")


async def close_terminal(ctx: StepContext, event: ExhaustionEvent) -> None:
    schema = ctx._wsql.schema  # pyright: ignore[reportPrivateUsage]
    await record(ctx, "effect-entered")
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


# A two-page source with one live item parks before its second page while the
# first item is still executing. The parked body has no frame to signal.
P5_PARK_CHAIN = Chain(
    name="p5-park-chain",
    start="work",
    steps={"work": Step(body=chain_context, outcomes=P5Outcome)},
)


async def parked_pages(ctx: StepContext) -> None:
    await ctx.emit_batch(
        [chain_start(P5_PARK_CHAIN, {"record": 1}, map_index=1, trace_id="park-r1")],
        cursor={"page": 0},
    )
    await record(ctx, "first-page")
    await ctx.emit_batch(
        [chain_start(P5_PARK_CHAIN, {"record": 2}, map_index=2, trace_id="park-r2")],
        cursor={"page": 1},
    )
    await record(ctx, "second-page")


@P5_APP.workflow("p5_parked", max_in_flight=1)
def p5_parked() -> Promise[object]:
    return build(chain_source(P5_PARK_CHAIN, parked_pages, key="parked_source"))


P5_FLOWS: dict[str, Any] = {
    name: P5_APP.get(name)
    for name in (
        "p5_static",
        "p5_map",
        "p5_chain",
        "p5_chain_source",
        "p5_loop",
        "p5_effect",
        "p5_effect_deadline",
        "p5_parked",
    )
}
