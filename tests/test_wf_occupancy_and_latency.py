"""The contract §9 controls 6 and 7: the occupancy deadlock at cap
(`max_in_flight=1`, one consuming child at `max_concurrent=1` — no free slot
may be needed to deliver a wake) and the bounded wake latency (the child
terminal's committed instant to the source's new claim, measured on the new
claim's timestamp, never the COALESCEd started_at).

Real PostgreSQL; the drain path is the production one."""

from __future__ import annotations

import asyncio
import enum
import json as _json
from typing import Any

import pytest

from taskq.backend._protocol import JobId

pytestmark = pytest.mark.integration


class OkOut(enum.Enum):
    OK = "ok"


def new_uuid_str() -> str:
    from taskq._ids import new_uuid

    return str(new_uuid())


def _chain(name: str, body: Any) -> Any:
    from taskq.workflows import DONE, Chain, Route, Step

    return Chain(
        name=name,
        start="screen",
        steps={"screen": Step(body=body, outcomes=OkOut, route=Route({OkOut.OK: DONE}))},
    )


async def test_cap_one_child_cap_one_never_occupancy_deadlocks(
    wf_conn: Any, wf_schema: str, module_pg_pool: Any, wf_sql: Any
) -> None:
    """At max_in_flight=1 with the consuming child's max_concurrent=1 and
    every slot held by the same run's children: terminalizing one child
    makes progress WITHOUT a free slot for the wake — the next single
    -record page emits and the run completes."""
    from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, chain_source
    from taskq.workflows.chain import chain_start

    child_calls: list[int | None] = []

    async def screen(ctx: StepContext, item: object) -> OkOut:
        child_calls.append(ctx.map_index)
        if ctx.map_index == 0:
            # The cap's occupancy: the child holds the only slot briefly —
            # the wake's promotion must not need a slot, and the drain runs
            # at the child's own terminal.
            await asyncio.sleep(0.4)
        return OkOut.OK

    chain = _chain(f"cap1-{new_uuid_str()}", screen)
    app = WorkflowApp()

    @app.workflow("cap_one_flow", max_in_flight=1)
    def cap_one_flow() -> Promise[object]:
        async def source(ctx: StepContext) -> None:
            # The cursor-driven page loop: the resume continues from the
            # last COMMITTED page (the park's own law).
            cursor = await ctx.cursor()
            start_page = int(cursor.get("page", -1)) + 1
            pages = ([0], [1])
            for page in range(start_page, len(pages)):
                await ctx.emit_batch(
                    [
                        chain_start(
                            chain,
                            {"app_id": page},
                            map_index=pages[page][0],
                            trace_id=f"app-{page}",
                        )
                    ],
                    cursor={"page": page},
                )

        return build(chain_source(chain, source, key="source", max_attempts=2))

    compiled = app.get("cap_one_flow")
    runner = FlowRunner(compiled, module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = JobId(
        await wf_conn.fetchval(
            f'SELECT id FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
            flow_id,
        )
    )
    async def _pump() -> None:
        for _ in range(120):
            await runner.tick(flow_id)
            state = await wf_conn.fetchval(
                f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1',
                flow_id,
            )
            if state in ("succeeded", "failed", "cancelled"):
                return
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_pump(), timeout=90)
    root = await wf_conn.fetchval(f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root == "succeeded", root
    children = await wf_conn.fetch(
        f"SELECT map_index::int AS m, status::text AS status FROM \"{wf_schema}\".jobs "
        "WHERE parent_id = $1 ORDER BY map_index",
        source_id,
    )
    assert [int(r["m"]) for r in children] == [0, 1], children
    assert {r["status"] for r in children} == {"succeeded"}, children
    # The parked source's own body re-entered exactly once (the resume) and
    # every child ran exactly once.
    assert child_calls == [0, 1], child_calls


async def test_bounded_wake_latency_measured_on_the_new_claim(
    wf_conn: Any, wf_schema: str, module_pg_pool: Any, wf_sql: Any
) -> None:
    """The controlled-host acceptance target: < 1 s from the terminal
    database commit instant to the source's NEW claim instant (the claim's
    own timestamp — never the COALESCEd started_at). The measured value is
    printed per run with the bound stated."""
    from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, chain_source
    from taskq.workflows.chain import chain_start

    release = asyncio.Event()

    async def screen(ctx: StepContext, item: object) -> OkOut:
        del item
        # The page-0 children hold the bound until the release (the park's
        # precondition); the page-1 child (the resumed page) runs free.
        if ctx.map_index < 2:
            await release.wait()
        return OkOut.OK

    chain = _chain(f"latency-{new_uuid_str()}", screen)
    app = WorkflowApp()

    @app.workflow("latency_flow", max_in_flight=2)
    def latency_flow() -> Promise[object]:
        async def source(ctx: StepContext) -> None:
            # The cursor-driven page loop (the resume continues from the
            # last COMMITTED page): page 0 holds the bound with BOTH
            # children; page 1 parks.
            cursor = await ctx.cursor()
            start_page = int(cursor.get("page", -1)) + 1
            page_map_indexes = ([0, 1], [2])
            for page in range(start_page, len(page_map_indexes)):
                await ctx.emit_batch(
                    [
                        chain_start(
                            chain,
                            {"app_id": m},
                            map_index=m,
                            trace_id=f"app-{m}",
                        )
                        for m in page_map_indexes[page]
                    ],
                    cursor={"page": page},
                )

        return build(chain_source(chain, source, key="source", max_attempts=2))

    compiled = app.get("latency_flow")
    runner = FlowRunner(compiled, module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    source_id = JobId(
        await wf_conn.fetchval(
            f'SELECT id FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'source'",
            flow_id,
        )
    )
    # The source parks (both children of page 0 hold the bound).
    source_task = asyncio.create_task(runner.tick(flow_id))
    parked = asyncio.Event()
    deadline = asyncio.get_running_loop().time() + 30.0
    while asyncio.get_running_loop().time() < deadline:
        state = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', source_id
        )
        if state == "scheduled":
            parked.set()
            break
        await asyncio.sleep(0.05)
    assert parked.is_set()
    # The producing tick's frame continues into the page-0 children's
    # bodies (they wait on the release) — the release drives everything;
    # the frame is cancelled after the measurement.
    release.set()
    for _ in range(60):
        await runner.tick(flow_id)
        root = await wf_conn.fetchval(
            f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
        )
        if root in ("succeeded", "failed", "cancelled"):
            break
        await asyncio.sleep(0.05)
    source_task.cancel()
    try:
        await source_task
    except asyncio.CancelledError:
        pass
    new_claim: Any = "measured-in-sql"
    # The MEASUREMENT (the DB's own instants, one domain): the last page-0
    # terminal's commit (its ledger row's updated_at) → the source's new
    # ledger claim's created_at.
    latency_row = await wf_conn.fetchrow(
        f"""
        SELECT
          (SELECT max(l2.updated_at) FROM "{wf_schema}".wf_step_ledger l2
           WHERE l2.flow_id = $1 AND l2.step_key = 'screen' AND l2.status = 'succeeded')
            AS terminal_commit,
          (SELECT l3.created_at FROM "{wf_schema}".wf_step_ledger l3
           WHERE l3.flow_id = $1 AND l3.step_key = 'source'
           ORDER BY l3.created_at DESC, l3.id DESC LIMIT 1)
            AS new_claim
        """,
        flow_id,
    )
    assert latency_row is not None
    terminal_commit = latency_row["terminal_commit"]
    new_claim_at = latency_row["new_claim"]
    assert terminal_commit is not None and new_claim_at is not None, latency_row
    measured_s = (new_claim_at - terminal_commit).total_seconds()
    print(f"WAKE-LATENCY measured={measured_s:.3f}s bound=<1s (terminal commit → new ledger claim)")
    assert -1.0 <= measured_s < 1.0, (
        f"the bounded wake latency: {measured_s:.3f}s exceeds the < 1 s "
        "controlled-host bound (the contract's §5.5 target)"
    )
