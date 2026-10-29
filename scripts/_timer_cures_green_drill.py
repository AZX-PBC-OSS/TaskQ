"""THE TIMER-CURES LANE'S GREEN DRILL — the attack front's convictions,
re-derived live AT THE CURED TREE (each one must FAIL to reproduce).

The pre-cure live receipts (the convictions reproducing at the base
tree 08dccb92d) ride ``.measurements/timer-cures-red-drill-pre.txt`` and
``scripts/_timer_cures_red_drill.py`` (the pre-cure form, in history).

Usage: python scripts/_timer_cures_green_drill.py <dsn> <schema>
Exits 0 when EVERY conviction fails to reproduce; the first reproduced
conviction is the defect, printed and exited 1.
"""

from __future__ import annotations

import asyncio
import sys
import time

import asyncpg
from pydantic import BaseModel

import taskq.migrate
from taskq.workflows import (
    FlowRunner,
    Promise,
    StepContext,
    WorkflowApp,
    build,
    step,
)
from taskq.workflows.api._hitl import HitlClient, sweep_due_timers
from taskq.workflows.api._hitl_listen import Backfilled, HitlListener
from taskq.workflows.api._sql_runner import render_sql

DSN = sys.argv[1]
SCHEMA = sys.argv[2]


class __timer__Approval(BaseModel):  # noqa: N801  # Why: the TROJAN'S OWN NAME — the conviction is the name itself.
    verdict: str


class TrojannParams(BaseModel):
    pass


class LeakParams(BaseModel):
    pass


async def trojan_body(ctx: StepContext, params: TrojannParams) -> str:
    await ctx.wait_signal((__timer__Approval,), timeout_s=0.2, reason="the trojan's mint")
    return "answered"


async def timer_body(ctx: StepContext, params: LeakParams) -> str:
    await ctx.wait_until(30.0, key="leak")
    return "later"


def fail(conviction: str, detail: str) -> None:
    print(f"REPRODUCED (DEFECT): {conviction}\n{detail}")
    raise SystemExit(1)


async def main() -> None:
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="pre")
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="post")
    pool = await asyncpg.create_pool(DSN)

    app = WorkflowApp()

    @app.workflow("trojan_flow")
    def trojan_flow() -> Promise[object]:
        return build(step(trojan_body, TrojannParams(), key="trojan_node"))

    @app.workflow("leak_flow")
    def leak_flow() -> Promise[object]:
        return build(step(timer_body, LeakParams(), key="leak_node"))

    # ── CURE 1: the trojan mint must REFUSE ──────────────────────────
    print("== CURE 1: the __timer__ trojan prefix (the mint must refuse) ==")
    runner = FlowRunner(app.get("trojan_flow"), pool, SCHEMA)
    flow_id = (await runner.create_flow()).flow_id
    await runner.tick(flow_id)
    node = await pool.fetchrow(
        render_sql(
            "SELECT status, error_class, error_message FROM {schema}.jobs "
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'trojan_node'",
            SCHEMA,
        ),
        flow_id,
    )
    holds = await pool.fetchval(
        render_sql("SELECT count(*) FROM {schema}.wf_signals WHERE workflow_id = $1", SCHEMA),
        flow_id,
    )
    print(f"node: status={node['status']} error_class={node['error_class']}")
    print(f"holds minted: {holds}")
    if node["error_class"] != "ReservedTimerNamespaceError":
        fail(
            "the trojan mint",
            f"the node did not die NAMED (E17): {node['error_class']}",
        )
    if holds:
        fail("the trojan mint", "a hold row WAS written — the refusal is not at the mint")
    print("  -> CURED: the mint refuses loudly (E17), zero holds minted")

    # ── CURE 2: the timer must NOT ride the approval broadcast ───────
    print()
    print("== CURE 2: the timer leak into the approval broadcast ==")
    runner2 = FlowRunner(app.get("leak_flow"), pool, SCHEMA)
    leak_id = (await runner2.create_flow()).flow_id
    await runner2.drive(leak_id, until="held")

    listener = HitlListener(pool, SCHEMA)
    await listener.start()
    seen: list[str] = []

    async def collect() -> None:
        async for event in listener.events():
            sig = getattr(event, "signal", "")
            seen.append(f"{type(event).__name__}(signal={sig!r})")
            if isinstance(event, Backfilled):
                return

    try:
        await asyncio.wait_for(collect(), timeout=10)
    finally:
        await listener.stop()
    print(f"listener events: {seen}")
    timer_created = [s for s in seen if "HoldCreated" in s and "__timer__" in s]
    if timer_created:
        fail("the timer leak", f"the backfill surfaced the timer: {timer_created}")
    client_list = await HitlClient(pool, SCHEMA).list(leak_id)
    if client_list:
        fail("the timer leak", f"HitlClient.list() surfaced the timer: {client_list}")
    print("  -> CURED: the timer appears in NO pending-approval face")

    # ── CURE 4: the drain + the index-led scan ───────────────────────
    print()
    print("== CURE 4: the 10k-timer tail ==")
    n = 10000
    await pool.execute(
        render_sql(
            """INSERT INTO {schema}.wf_signals
            (id, workflow_id, node_key, signal_name, hold_epoch, call_id,
             payload, payload_schema, status, expires_at)
        SELECT gen_random_uuid(), gen_random_uuid(), 'n', '__timer__bulk', 1,
               'timer:x', '{}'::jsonb,
               '{"TimerWake": {"type": "object"}}'::jsonb, 'held',
               now() - interval '1 second'
        FROM generate_series(1, $1) g""",
            SCHEMA,
        ),
        n,
    )
    plan_rows = await pool.fetch(
        render_sql(
            "EXPLAIN (COSTS OFF) SELECT s.id FROM {schema}.wf_signals s "
            "WHERE s.status = 'held' "
            "AND starts_with(s.signal_name, '__timer__') "
            "AND s.expires_at IS NOT NULL AND s.expires_at <= clock_timestamp() "
            "ORDER BY s.expires_at, s.id LIMIT 100",
            SCHEMA,
        )
    )
    plan = "\n".join(r[0] for r in plan_rows)
    print("EXPLAIN of the due-scan (post-fix):")
    for r in plan_rows:
        print(f"  {r[0]}")
    if "Seq Scan" in plan:
        fail("the seq scan", "the due-scan is still a seq scan")
    if "wf_signals_timer_due_idx" not in plan:
        fail("the seq scan", f"the due-scan is not index-led:\n{plan}")

    t0 = time.perf_counter()
    fired = await sweep_due_timers(pool, schema=SCHEMA, drain=True)
    dt = time.perf_counter() - t0
    print(f"ONE drain call fired {fired} of {n} due timers in {dt * 1000:.1f} ms")
    if fired != n:
        fail("the 10k tail", f"the drain left {n - fired} due timers for later passes")
    print(
        f"  -> CURED: the tail is ONE sweep pass (~30 s cadence) + {dt:.2f} s of drain, "
        f"not 100 passes x 30 s ≈ 50 minutes"
    )

    await pool.execute(
        render_sql(
            "DELETE FROM {schema}.wf_signals WHERE signal_name = '__timer__bulk'",
            SCHEMA,
        )
    )
    await pool.close()
    print()
    print("ALL CONVICTIONS FAIL TO REPRODUCE — the cures hold live.")


if __name__ == "__main__":
    asyncio.run(main())
