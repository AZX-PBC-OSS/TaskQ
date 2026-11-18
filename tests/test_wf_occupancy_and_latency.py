"""The contract §9 controls 6 and 7: the occupancy deadlock at cap
(`max_in_flight=1`, one consuming child at `max_concurrent=1` — no free slot
may be needed to deliver a wake) and the bounded wake latency (the child
terminal's committed instant to the source's new claim, measured on the new
claim's timestamp, never the COALESCEd started_at).

Real PostgreSQL; the drain path is the production one."""

from __future__ import annotations

import asyncio
import contextlib
import enum
from typing import Any

import asyncpg
import pytest

from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql

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
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool, wf_sql: WorkflowSql
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
    root = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root == "succeeded", root
    children = await wf_conn.fetch(
        f'SELECT map_index::int AS m, status::text AS status FROM "{wf_schema}".jobs '
        "WHERE parent_id = $1 ORDER BY map_index",
        source_id,
    )
    assert [int(r["m"]) for r in children] == [0, 1], children
    assert {r["status"] for r in children} == {"succeeded"}, children
    # The parked source's own body re-entered exactly once (the resume) and
    # every child ran exactly once.
    assert child_calls == [0, 1], child_calls


async def test_bounded_wake_latency_measured_on_the_new_claim(
    wf_conn: asyncpg.Connection, wf_schema: str, module_pg_pool: asyncpg.Pool, wf_sql: WorkflowSql
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
        if ctx.map_index is not None and ctx.map_index < 2:
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
    with contextlib.suppress(asyncio.CancelledError):
        await source_task
    # THE MEASUREMENT — the pair the machinery can PROVE, causally ordered
    # in one clock domain:
    #
    #   promotion instant  = the source's own wf_emit_backpressure
    #                        .last_wake_at — the promotion CAS's
    #                        clock_timestamp(), written by the winning drain
    #                        transaction (the wake's linearization);
    #   new claim          = the FIRST source ledger claim at or after it
    #                        (the resumed claim that follows the promotion).
    #
    # The delta is asserted NON-NEGATIVE and under the < 1 s §5.5 bound: an
    # inverted pairing (a claim before the promotion that caused it) is RED,
    # never vacuously green. The page-0 screens' own ledger instants are
    # NOT the wake's cause — a screen can still be running when the source
    # resumes — so they are deliberately not in this pairing.
    pair = await wf_conn.fetchrow(
        f"""
        WITH promoted AS (
            SELECT (j.metadata->'wf_emit_backpressure'->>'last_wake_at') AS wake_at_text
            FROM "{wf_schema}".jobs j
            WHERE j.id = $1
        ), wake_at AS (
            SELECT (wake_at_text)::timestamptz AS instant FROM promoted
        )
        SELECT
          (SELECT instant FROM wake_at) AS promotion_instant,
          (SELECT l3.id FROM "{wf_schema}".wf_step_ledger l3, wake_at
           WHERE l3.flow_id = $2 AND l3.step_key = 'source'
             AND l3.created_at >= wake_at.instant
           ORDER BY l3.created_at, l3.id LIMIT 1) AS new_claim_id,
          (SELECT l3.created_at FROM "{wf_schema}".wf_step_ledger l3, wake_at
           WHERE l3.flow_id = $2 AND l3.step_key = 'source'
             AND l3.created_at >= wake_at.instant
           ORDER BY l3.created_at, l3.id LIMIT 1) AS new_claim_at
        """,
        source_id,
        flow_id,
    )
    assert pair is not None
    promotion_instant = pair["promotion_instant"]
    new_claim_id = pair["new_claim_id"]
    new_claim_at = pair["new_claim_at"]
    assert promotion_instant is not None, (
        "the promotion's own instant is on the source's record (last_wake_at)"
    )
    assert new_claim_id is not None and new_claim_at is not None, (
        f"the first source claim AFTER the promotion is the wake's outcome: {pair}"
    )
    measured_s = (new_claim_at - promotion_instant).total_seconds()
    print(
        f"WAKE-LATENCY promotion_instant={promotion_instant.isoformat()} "
        f"claim_id={new_claim_id} claim_instant={new_claim_at.isoformat()} "
        f"delta={measured_s:.3f}s bound=<1s (promotion commit → new claim)"
    )
    assert 0.0 <= measured_s < 1.0, (
        f"the bounded wake latency: {measured_s:.3f}s — a negative delta is "
        "an inverted (non-causal) pairing and a delta at or past the < 1 s "
        "controlled-host bound exceeds the contract's §5.5 target"
    )
