# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""The emit BACKPRESSURE's pins — the emit cap's decision.

The max-in-flight bound
is the RUN's admission control — how many non-terminal rows the run may
materialize — a per-workflow resource question, never one chain's shape
(a workflow may wire several chain sources; a Chain-level arg would make
two sources race for two independent bounds). It is declared on the
per-workflow POLICY surface — the same declaration that carries
capture/redact/channel:

    @app.workflow("application_sync", max_in_flight=200)

``None`` is the explicit unbounded (the author takes the dragon); the
default is the shipped ``EMIT_MAX_IN_FLIGHT_DEFAULT`` (1000) — the fence
is ON for the new surface.

The mechanism admits each WHOLE page under the run's admission lock before
writing children/edges/cursor. A full page raises typed ``EmitBackpressure``
before any write; the real runner parks with no timer or physical claim and
wakes from committed terminal state. The low-level pins below prove bound
safety without confusing their hand-driven claim with the full park runner.
Park/resume no longer spends the source's logical attempt or failure ledger.

The rejected shape: the bound pins ran RED against the tree before the fence
existed (the declared surface absent — the TypeError IS the red); the
unbounded variant's blow-out is the rejected red, captured live.
Captured: ``.measurements/t20-backpressure-*.txt``.
"""

from __future__ import annotations

import asyncio
import enum
import json
from typing import TYPE_CHECKING, Any

import asyncpg
import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.workflows import Promise
from taskq.workflows._emit import (
    EMIT_MAX_IN_FLIGHT_DEFAULT,
    EmitBackpressure,
    emit_batch,
)
from taskq.workflows.chain import DONE, Chain, Route, Step
from taskq.workflows.engine import finalize_node
from tests._wf_fixtures import RedLog, seed_flow
from tests.test_wf_emit_tx_pins import (
    _LEASE,
    claim_source,
    committed_children,
    make_source,
    page_children,
)

if TYPE_CHECKING:
    from taskq.workflows._sql import WorkflowSql

#: the bound the pins declare (a page of 4 cannot fit twice under it)
BOUND = 6
PAGE = 4
_PAGES: list[list[int]] = [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]


async def outstanding(
    pool: asyncpg.Pool, schema: str, flow_id: JobId, *, exclude: JobId | None = None
) -> int:
    """The run's non-terminal row count EXCLUDING the source's own row —
    the same semantics the emit's admission reads (the ledger's truth)."""
    async with pool.acquire() as conn:
        return int(
            await conn.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '
                "WHERE (metadata->>'flow_id')::uuid = $1 AND metadata ? 'flow_id' "
                "AND step_key <> '__flow__' "
                "AND ($2::uuid IS NULL OR id <> $2::uuid) "
                "AND status IN ('pending','scheduled','running')",
                flow_id,
                exclude,
            )
        )


async def drain_one_chain(pool: asyncpg.Pool, wf_sql: WorkflowSql, flow_id: JobId) -> bool:
    """One worker pass: claim + finalize one pending chain row (the REAL
    dispatch-claim's fence shape + the REAL finalize). False = none
    pending."""
    worker = JobId(new_uuid())
    async with pool.acquire() as conn:
        rec = await conn.fetchrow(
            f'SELECT id FROM "{wf_sql.schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'screen' "
            "AND status = 'pending' LIMIT 1",
            flow_id,
        )
        if rec is None:
            return False
        claimed = await conn.fetchrow(
            f"UPDATE \"{wf_sql.schema}\".jobs SET status = 'running', started_at = now(), "
            "attempt = LEAST(attempt + 1, 32767), claim_epoch = claim_epoch + 1, "
            "locked_by_worker = $2, lock_expires_at = now() + $3::interval, "
            "last_heartbeat_at = now() WHERE id = $1 AND status = 'pending' "
            "AND deps_pending = 0 RETURNING attempt, claim_epoch",
            JobId(rec["id"]),
            worker,
            _LEASE,
        )
    if claimed is None:
        return False
    result = await finalize_node(
        pool,
        wf_sql,
        flow_id=flow_id,
        job_id=JobId(rec["id"]),
        step_key="screen",
        worker_id=worker,
        attempt=int(claimed["attempt"]),
        claim_epoch=int(claimed["claim_epoch"]),
        outcome="succeeded",
        result={"value": {"screened": True}},
    )
    assert result.applied
    return True


# ── the fence: the pager blocks at the declared bound ───────────────────


@pytest.mark.integration
async def test_emit_blocks_at_the_declared_bound(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    redlog: RedLog,
) -> None:
    """Page 0 fits; page 1 immediately raises typed capacity control before
    any child/edge/cursor write. Ordinary saturation is not a timed RuntimeError.
    The outstanding count never exceeds the bound."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_schema, flow_id, wf_sql)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)

    # Page 0: fits (0 + 4 <= 6).
    await emit_batch(
        module_pg_pool,
        wf_sql,
        flow_id=flow_id,
        source_id=source_id,
        worker_id=worker,
        attempt=attempt,
        claim_epoch=epoch,
        children=page_children(_PAGES[0], flow_id),
        cursor={"page": 0},
        max_in_flight=BOUND,
    )
    assert await outstanding(module_pg_pool, wf_schema, flow_id, exclude=source_id) == PAGE

    # Page 1: does not fit — one ordinary capacity control, no timed failure.
    with pytest.raises(EmitBackpressure) as excinfo:
        await emit_batch(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children(_PAGES[1], flow_id),
            cursor={"page": 1},
            max_in_flight=BOUND,
            backpressure_timeout_s=1.0,
        )
    assert excinfo.value.bound == BOUND
    assert excinfo.value.page_width == PAGE
    assert excinfo.value.outstanding == PAGE

    # the count stayed at page 0's — never over the bound.
    after = await outstanding(module_pg_pool, wf_schema, flow_id, exclude=source_id)
    after_children = await committed_children(wf_conn, wf_schema, source_id)
    redlog.red(
        "t20-backpressure-block",
        "the bound REMOVED (the unbounded variant — the rejected red, "
        "test_unbounded_emit_is_the_convicted_red): the same fast "
        "source emits page 1 immediately and the outstanding count blows "
        "the bound — unbounded materialization",
        {"outstanding_after_blocked_emit": after, "bound": BOUND},
    )
    assert after == PAGE, "the blocked emit must not have materialized its page"
    assert after_children == PAGE
    assert after <= BOUND


@pytest.mark.integration
async def test_backpressure_releases_when_workers_drain(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    redlog: RedLog,
) -> None:
    """The bound is a THROTTLE: this low-level door's typed refusal retries
    only after the real worker makes committed progress. Full FlowRunner
    slot release is covered by the parking controls; this hand-driven source
    deliberately stays claimed to isolate whole-page admission/atomicity.
    """
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_schema, flow_id, wf_sql)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)

    max_seen = 0
    # 400 bounded worker iterations at 30 ms have a 12 s deterministic upper
    # service term. Thirty seconds gives that real drain work margin; retries
    # must not rely on pytest's unrelated global 300 s timeout.
    admission_deadline = asyncio.get_running_loop().time() + 30.0
    admission_retries = 0

    async def slow_worker() -> None:
        nonlocal max_seen
        for _ in range(400):
            await drain_one_chain(module_pg_pool, wf_sql, flow_id)
            seen = await outstanding(module_pg_pool, wf_schema, flow_id, exclude=source_id)
            max_seen = max(max_seen, seen)
            await asyncio.sleep(0.03)

    worker_task = asyncio.create_task(slow_worker())
    try:
        for p, page in enumerate(_PAGES):
            while True:
                try:
                    await emit_batch(
                        module_pg_pool,
                        wf_sql,
                        flow_id=flow_id,
                        source_id=source_id,
                        worker_id=worker,
                        attempt=attempt,
                        claim_epoch=epoch,
                        children=page_children(page, flow_id),
                        cursor={"page": p},
                        max_in_flight=BOUND,
                        backpressure_timeout_s=30.0,
                    )
                    break
                except EmitBackpressure as control:
                    admission_retries += 1
                    if asyncio.get_running_loop().time() >= admission_deadline:
                        final_rows = await wf_conn.fetch(
                            f"SELECT step_key, status::text AS status, attempt "
                            f'FROM "{wf_schema}".jobs WHERE parent_id = $1 ORDER BY map_index',
                            source_id,
                        )
                        actual_outstanding = await outstanding(
                            module_pg_pool, wf_schema, flow_id, exclude=source_id
                        )
                        raise AssertionError(
                            json.dumps(
                                {
                                    "page": p,
                                    "retries": admission_retries,
                                    "control": {
                                        "page_width": control.page_width,
                                        "bound": control.bound,
                                        "outstanding": control.outstanding,
                                    },
                                    "actual_outstanding": actual_outstanding,
                                    "worker_done": worker_task.done(),
                                    "child_rows": [dict(row) for row in final_rows],
                                },
                                default=str,
                                sort_keys=True,
                            )
                        ) from control
                    # The test-only drain task supplies actual committed
                    # terminal progress before this low-level retry.
                    await asyncio.sleep(0)
            seen = await outstanding(module_pg_pool, wf_schema, flow_id, exclude=source_id)
            max_seen = max(max_seen, seen)
    finally:
        await worker_task

    redlog.red(
        "t20-backpressure-throttle",
        "the throttle NOT admitting whole pages (a per-record check): "
        "the emit would land a page OVER the bound (outstanding + width > "
        "bound) — the bound is checked against the ADMISSION (the "
        "outstanding + the page's width), never the row alone",
        {"max_outstanding_seen": max_seen, "bound": BOUND},
    )
    assert max_seen <= BOUND, (
        f"THE UNBOUNDED EMIT: the outstanding count reached {max_seen} "
        f"against the declared bound {BOUND}"
    )
    assert await committed_children(wf_conn, wf_schema, source_id) == 12, (
        "all three pages landed exactly once under the throttle"
    )


@pytest.mark.integration
async def test_unbounded_emit_is_the_convicted_red(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    redlog: RedLog,
) -> None:
    """The captured red (DH9's obligation): the same fast source with the
    bound REMOVED (``max_in_flight=None`` — the explicit unbounded) blows
    the count the bounded pins hold: two pages back-to-back with no
    worker → 8 outstanding against the 6 the bounded variant never
    exceeds. The dragon, observed live."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_schema, flow_id, wf_sql)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)

    for p, page in enumerate(_PAGES[:2]):
        await emit_batch(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children(page, flow_id),
            cursor={"page": p},
            max_in_flight=None,  # the author takes the dragon
        )
        count = await outstanding(module_pg_pool, wf_schema, flow_id, exclude=source_id)
        redlog.red(
            f"t20-backpressure-unbounded-page{p}",
            "the unbounded emit — the count grows with every page, no "
            "admission, no pause (the memory/row blow-up the fence "
            "exists for)",
            {"outstanding": count, "bound_it_blew": BOUND},
        )
    blown = await outstanding(module_pg_pool, wf_schema, flow_id, exclude=source_id)
    assert blown > BOUND, (
        f"the rejected red did not reproduce: the unbounded emit reached "
        f"{blown}, the bound is {BOUND}"
    )


@pytest.mark.integration
async def test_page_wider_than_the_bound_is_refused(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
) -> None:
    """An admission that can never succeed (a page wider than the bound)
    is refused at the door — the loud config error, never a silent
    starve."""
    flow_id = await seed_flow(wf_conn, wf_schema)
    source_id = await make_source(wf_conn, wf_schema, flow_id, wf_sql)
    worker, attempt, epoch = await claim_source(wf_conn, wf_schema, source_id)
    with pytest.raises(ValueError, match="max_in_flight"):
        await emit_batch(
            module_pg_pool,
            wf_sql,
            flow_id=flow_id,
            source_id=source_id,
            worker_id=worker,
            attempt=attempt,
            claim_epoch=epoch,
            children=page_children(_PAGES[0], flow_id),
            cursor={"page": 0},
            max_in_flight=2,  # a page of 4 can never fit
        )
    assert await committed_children(wf_conn, wf_schema, source_id) == 0, (
        "the refused emit wrote nothing"
    )


def test_the_default_is_the_fence_on() -> None:
    """The shipped default is the fence ON (the author opts out loudly —
    ``max_in_flight=None`` is the explicit unbounded, never a silent
    default)."""
    assert EMIT_MAX_IN_FLIGHT_DEFAULT == 1000


# ── the declared home: the workflow-level declare, end to end ───────────


class BpOutcome(enum.Enum):
    OK = "ok"


async def bp_step(ctx: object, item: dict[str, object]) -> BpOutcome:
    await asyncio.sleep(0.01)  # the slow worker's shape
    return BpOutcome.OK


BP_CHAIN = Chain(
    name="t20-bp-chain",
    start="screen",
    steps={
        "screen": Step(body=bp_step, outcomes=BpOutcome, route=Route({BpOutcome.OK: DONE})),
    },
)

BP_PAGES = [[0, 1], [2, 3], [4, 5], [6, 7]]
BP_SOURCE_EMIT_LOG: list[tuple[int, int]] = []  # (page, the attempt that emitted it)


async def bp_source(ctx: Any) -> None:
    """Pages of 2 against bound 3 park under saturation and replay from the
    committed cursor with the same logical source attempt."""
    from taskq.workflows.chain import chain_start

    cursor = await ctx.cursor()
    start_page = int(cursor.get("page", -1)) + 1
    for p in range(start_page, len(BP_PAGES)):
        BP_SOURCE_EMIT_LOG.append((p, ctx.attempt))
        await ctx.emit_batch(
            [
                chain_start(BP_CHAIN, {"app_id": i}, map_index=i, trace_id=f"app-{i}")
                for i in BP_PAGES[p]
            ],
            cursor={"page": p},
        )


@pytest.mark.integration
async def test_max_in_flight_is_declared_at_the_workflow(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    redlog: RedLog,
) -> None:
    """The declared home, end to end: the workflow declares the bound once
    (``@app.workflow(..., max_in_flight=…)``); the compile carries it; the
    runner hands it to every ``ctx.emit_batch``; saturation parks without a
    claim or logical-attempt charge; committed child terminals wake replay
    from the cursor. Every chain row lands exactly once and terminal."""
    from taskq.workflows import FlowRunner, WorkflowApp, build, chain_source

    app = WorkflowApp()

    @app.workflow("t20_bp_flow", max_in_flight=3)
    def t20_bp_flow() -> Promise[object]:
        # Ordinary saturation no longer owns this real-failure budget.
        return build(chain_source(BP_CHAIN, bp_source, key="bp_source", max_attempts=2))

    compiled = app.get("t20_bp_flow")
    assert compiled.max_in_flight == 3, "the declared bound rides the compile"

    runner = FlowRunner(compiled, module_pg_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    rows = await wf_conn.fetch(
        f'SELECT step_key, map_index, trace_id, status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__' "
        "AND step_key <> 'bp_source'",
        flow_id,
    )
    screens = [r for r in rows if r["step_key"] == "screen"]
    assert len(screens) == 8, f"8 chain starts exactly once, got {len(screens)}"
    assert len({r["map_index"] for r in screens}) == 8, "zero duplicate records"
    assert all(r["status"] == "succeeded" for r in rows), "every row terminal under the throttle"
    redlog.red(
        "t20-backpressure-declared",
        "the bound declared NOWHERE (the unpinned decision — the broken "
        "window): every emit materializes unbounded; the dragon's door "
        "stands open",
        {
            "declared_max_in_flight": compiled.max_in_flight,
            "emit_calls": len(BP_SOURCE_EMIT_LOG),
            "stalls": len({a for _, a in BP_SOURCE_EMIT_LOG}),
        },
    )
    # Saturation caused real physical park/remint cycles, not retries: every
    # body entry remains logical one and the ledger contains zero failures.
    assert BP_SOURCE_EMIT_LOG
    assert {attempt for _page, attempt in BP_SOURCE_EMIT_LOG} == {1}, BP_SOURCE_EMIT_LOG
    failed_source_ledgers = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'bp_source' AND status = 'failed'",
        flow_id,
    )
    assert failed_source_ledgers == 0
    source = await wf_conn.fetchrow(
        "SELECT attempt, metadata FROM "
        f"\"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'bp_source'",
        flow_id,
    )
    assert source is not None and source["attempt"] == 1
    root = await wf_conn.fetchrow(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id)
    assert root is not None and root["status"] == "succeeded"
