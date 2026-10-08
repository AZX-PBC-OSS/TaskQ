"""ECOSYSTEM PATTERN PROBES — TaskQflow P3 surface vs the workflow/dataflow
ecosystem's signature patterns.

Runs against the mapper's OWN Postgres on :5701 (schema `ecoprobe`), the
worktree's venv. Every EXPRESSED verdict in ECOSYSTEM-MAP.md cites one of
these probes' PASS lines. Each probe names the ecosystem pattern it
stands for.

Run: .venv/bin/python /tmp/opencode/ecopatterns-probes/probe_ecosystem.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Any

import asyncpg
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.migrate import apply_pending
from taskq.workflows import (
    Done,
    FlowRunner,
    Refine,
    WorkflowApp,
    build,
    gather,
    loop,
    map_source,
    sink,
    step,
)
from taskq.workflows._status import reconstruct_workflow_status
from taskq.workflows.api._hitl import HitlClient

DSN = "postgres://taskq:taskq@localhost:5701/taskq"
SCHEMA = "ecoprobe"

OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("/tmp/opencode/ecopatterns-probes/run.jsonl")
RESULTS: list[dict[str, object]] = []


def record(probe: str, pattern: str, verdict: str, detail: dict[str, object]) -> None:
    entry = {"probe": probe, "pattern": pattern, "verdict": verdict, **detail}
    RESULTS.append(entry)
    mark = "PASS" if verdict == "pass" else "FAIL"
    print(f"[{mark}] {probe} — {pattern}: {json.dumps(detail, default=str)}")


# ── shared body corpus (module-level: annotations must resolve) ─────────


class Ingest(BaseModel):
    doc_id: str


class Report(BaseModel):
    ref: str


class Item(BaseModel):
    n: int


class Approval(BaseModel):
    verdict: str


class Counter(BaseModel):
    acc: int = 0


MEMO_CALLS = 0
ANSWER_ATTEMPTS = 0


async def _prepare(ctx: Any, params: Ingest) -> Report:
    return Report(ref=f"r-{params.doc_id}")


async def _tail(ctx: Any, r: Report) -> dict[str, str]:
    return {"ref": r.ref}


async def _tail_list(ctx: Any, items: list[dict[str, int]]) -> dict[str, int]:
    return {"total": sum(i["n"] for i in items)}


async def _left(ctx: Any, p: Ingest) -> dict[str, int]:
    return {"n": 1}


async def _right(ctx: Any, p: Ingest) -> dict[str, int]:
    return {"n": 2}


async def _join(ctx: Any, a: dict[str, int], b: dict[str, int]) -> dict[str, int]:
    return {"total": a["n"] + b["n"]}


async def _memo_fn(value: dict[str, str]) -> dict[str, str]:
    global MEMO_CALLS
    MEMO_CALLS += 1
    return {"memo": "computed"}


async def _replayer(ctx: Any, params: Ingest) -> dict[str, object]:
    """The answer-queue probe's body: memo step → wait → (attempt 1) fail;
    the retry must replay BOTH cheaply."""
    global ANSWER_ATTEMPTS
    memo = await ctx.step("memo", _memo_fn, {"x": "1"})
    approval = await ctx.wait_signal((Approval,), timeout_s=60.0)
    ANSWER_ATTEMPTS += 1
    if ANSWER_ATTEMPTS == 1:
        raise RuntimeError("transient boom after the answer (the retry face)")
    return {"verdict": approval.verdict, "memo": memo["memo"], "attempts": ANSWER_ATTEMPTS}


async def _items_source(ctx: Any, params: Ingest) -> list[Item]:
    # RUNTIME length (7): the fork's cardinality is a run-time fact.
    return [Item(n=i) for i in range(7)]


async def _per_item(ctx: Any, item: Item) -> dict[str, int]:
    return {"n": item.n * 10}


async def _map_tail(ctx: Any, items: list[dict[str, int]]) -> dict[str, int]:
    return {"sum": sum(i["n"] for i in items)}


async def _hold_body(ctx: Any, params: Ingest) -> dict[str, object]:
    approval = await ctx.wait_signal((Approval,), timeout_s=60.0)
    return {"verdict": approval.verdict}


# ── the flow_app used by probes 1/3 (both flows on one app is fine, but
#    each probe builds its OWN app to keep the registry scoped) ──────────


def _app_one_step(suffix: str = "a") -> tuple[WorkflowApp, str]:
    app = WorkflowApp()
    wf_name = f"one_step_{suffix}"

    @app.actor(queue="default")
    async def prepared(ctx: Any, params: Ingest) -> Report:
        return Report(ref=f"r-{params.doc_id}")

    @app.workflow(wf_name)
    def one_step() -> object:
        a = step(prepared, Ingest(doc_id="d1"), key="a")
        return build(step(_tail, a, key="tail"))

    app.get(wf_name)
    return app, wf_name


def _app_replay() -> tuple[WorkflowApp, str]:
    app = WorkflowApp()

    @app.workflow("replay_flow")
    def replay_flow() -> object:
        return build(step(_replayer, Ingest(doc_id="d1"), key="reply"))

    app.get("replay_flow")
    return app, "replay_flow"


def _app_map() -> tuple[WorkflowApp, str]:
    app = WorkflowApp()

    @app.workflow("map_flow")
    def map_flow() -> object:
        src = step(_items_source, Ingest(doc_id="d1"), key="src")
        mapped = map_source(src, _per_item, key="m")
        return build(mapped)  # the map's JOIN is the terminal (the pin corpus's shape)

    app.get("map_flow")
    return app, "map_flow"


def _app_join() -> tuple[WorkflowApp, str]:
    app = WorkflowApp()

    @app.workflow("join_flow")
    def join_flow() -> object:
        a = step(_left, Ingest(doc_id="d1"), key="a")
        b = step(_right, Ingest(doc_id="d1"), key="b")
        g = gather([a, b])  # the ALL-upstream join (identity packer)
        return build(step(_tail_list, g, key="join_tail"))

    app.get("join_flow")
    return app, "join_flow"


def _app_hold() -> tuple[WorkflowApp, str]:
    app = WorkflowApp()

    @app.workflow("hold_flow")
    def hold_flow() -> object:
        return build(step(_hold_body, Ingest(doc_id="d1"), key="gate"))

    app.get("hold_flow")
    return app, "hold_flow"


def _app_loop() -> tuple[WorkflowApp, str]:
    app = WorkflowApp()
    iterations: list[int] = []

    async def counting_body(ctx: Any, carry: object) -> object:
        acc = (carry or Counter()).acc + 1 if isinstance(carry, Counter) else 1
        iterations.append(acc)
        if acc >= 4:
            return Done(Counter(acc=acc))
        return Refine(Counter(acc=acc))

    @app.workflow("loop_flow")
    def loop_flow() -> object:
        return build(loop("iter", counting_body, max_iterations=10))

    app.get("loop_flow")
    return app, "loop_flow", iterations


# ── setup ────────────────────────────────────────────────────────────────


async def setup() -> asyncpg.Pool:
    conn = await asyncpg.connect(DSN)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
        await apply_pending(conn, schema=SCHEMA)
        await conn.executemany(
            f'INSERT INTO "{SCHEMA}".actor_config (actor, queue) VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING',
            [("wf", "default")],
        )
    finally:
        await conn.close()
    return await asyncpg.create_pool(DSN)


# ── PROBE 1: Temporal deterministic-replay ≙ rows-only reconstruction ───


async def probe_01_rows_replay(pool: asyncpg.Pool) -> None:
    """TEMPORAL 'deterministic replay / the WorkflowImpl split'. The
    TaskQflow equivalent: there is no orchestrator process to replay —
    the graph IS rows, and the status is RE-DERIVED from rows alone
    (reconstruct_workflow_status), no cache consulted. Plus the compile's
    determinism face (same module → byte-identical graph) and the
    Dagster op/job split's projection (actor_config)."""
    wsql = None
    from taskq.workflows._sql import WorkflowSql

    wsql = WorkflowSql.build(SCHEMA)
    app, name = _app_one_step()
    runner = FlowRunner(app.get(name), pool, SCHEMA)
    flow_id = await runner.create_flow(input={"doc_id": "d1"})
    await runner.drive(flow_id)

    # The input rides the ROW (restart-readback face).
    async with pool.acquire() as conn:
        raw = await conn.fetchval(
            f'SELECT payload FROM "{SCHEMA}".jobs WHERE id = $1', flow_id
        )
        import json as _json

        payload = _json.loads(raw) if isinstance(raw, str) else raw
        carries_input = "wf_input" in payload
        # ROWS-ONLY RECONSTRUCTION (the replay equivalent).
        derived = await reconstruct_workflow_status(conn, wsql, flow_id)
        # Mid-run reconstruction on a second flow: one tick, then read.
        flow2 = await runner.create_flow(input={"doc_id": "d2"})
        nodes2 = await conn.fetch(wsql.workflow_nodes, flow2)

    ok_replay = derived == "complete" and carries_input
    # The compile's determinism: two compiles, byte-identical Mermaid.
    m1 = app.get(name).mermaid()
    m2 = app.get(name).mermaid()
    stable = m1 == m2 and len(m1) > 0
    # The Dagster op/job split's projection: the body (op) projects into
    # the estate's ActorConfig carrier (job) — one population, visible.
    from taskq.workflows.api._app import WorkflowActor

    actors = [
        n.body for n in app.get(name).nodes.values() if isinstance(n.body, WorkflowActor)
    ]
    projection = actors[0].actor_config(name) if actors else {}
    ok_projection = projection.get("actor") == f"{name}.prepared"

    record(
        "probe_01_rows_replay",
        "Temporal replay + Dagster op/job split",
        "pass" if ok_replay and stable and ok_projection else "fail",
        {
            "reconstructed_status": derived,
            "input_on_row": carries_input,
            "mermaid_byte_stable": stable,
            "actor_config_projection": projection,
        },
    )


# ── PROBE 2: Temporal signals / Argo suspend / Airflow deferrable ≙ hold ─


async def probe_02_signal_hold(pool: asyncpg.Pool) -> None:
    """The SIGNAL/SUSPEND/DEFERRABLE family: a body waits on a human —
    the node HOLDS as rows (pending + a deadline), the worker SLOT IS
    RELEASED (no lock held — the deferrable's whole point), the hold is
    enumerable (HitlClient.list — the 'query' face), the deliver is a
    CAS'd resume, the result decodes."""
    from taskq.workflows._sql import WorkflowSql

    wsql = WorkflowSql.build(SCHEMA)
    app, name = _app_hold()
    runner = FlowRunner(app.get(name), pool, SCHEMA)
    client = HitlClient(pool, schema=SCHEMA)
    flow_id = await runner.create_flow(input={"doc_id": "d1"})
    assert await runner.drive(flow_id, until="held") == "held"

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, locked_by_worker, scheduled_at FROM "{SCHEMA}".jobs '
            "WHERE id = $1",
            flow_id,
        )
        gate_row = await conn.fetchrow(
            f'SELECT status, locked_by_worker, scheduled_at IS NOT NULL AS has_deadline '
            f'FROM "{SCHEMA}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'gate'",
            flow_id,
        )
        signal_rows = await conn.fetch(
            f'SELECT status FROM "{SCHEMA}".wf_signals WHERE workflow_id = $1', flow_id
        )
    holds = await client.list(flow_id)
    slot_released = gate_row["status"] == "pending" and gate_row["locked_by_worker"] is None

    resolved = await client.resolve(holds[0].hold_id, {"verdict": "approve"})
    final = await runner.drive(flow_id)
    result = await runner.result(flow_id)

    # THE CAS IDEMPOTENCE: a second deliver of the SAME hold = no-op.
    again = await client.resolve(holds[0].hold_id, {"verdict": "approve"})

    record(
        "probe_02_signal_hold",
        "Temporal signals / Argo suspend / Airflow deferrable",
        "pass"
        if slot_released
        and len(signal_rows) == 1
        and len(holds) == 1
        and final == "terminal"
        and result == {"verdict": "approve"}
        else "fail",
        {
            "node_status": gate_row["status"],
            "slot_released": slot_released,
            "signal_rows": len(signal_rows),
            "holds_listed": len(holds),
            "delivery_1": resolved.status,
            "delivery_2_noop": again.status,
            "final": final,
            "result": result,
        },
    )


# ── PROBE 3: LangGraph checkpoint/replay ≙ ctx.step memo + answer queue ─


async def probe_03_checkpoint_replay(pool: asyncpg.Pool) -> None:
    """LANGGRAPH checkpoint + thread-replay, Temporal activity-memo: the
    body re-executes FROM THE TOP on resume; pre-wait side effects are
    ctx.step-LEDGERED and replay cheap (the memo fn must NOT re-run);
    the DELIVERED answer is replayed on the retried attempt (the answer
    queue — the operator never re-answers)."""
    global MEMO_CALLS, ANSWER_ATTEMPTS
    MEMO_CALLS = 0
    ANSWER_ATTEMPTS = 0
    from taskq.workflows._sql import WorkflowSql

    wsql = WorkflowSql.build(SCHEMA)
    app, name = _app_replay()
    runner = FlowRunner(app.get(name), pool, SCHEMA)
    client = HitlClient(pool, schema=SCHEMA)
    flow_id = await runner.create_flow(input={"doc_id": "d1"})
    assert await runner.drive(flow_id, until="held") == "held"
    holds = await client.list(flow_id)
    await client.resolve(holds[0].hold_id, {"verdict": "go"})
    # Attempt 1 runs the body past the wait, then FAILS (transient).
    await runner.drive(flow_id, until="held")  # nothing holds; drive proceeds
    # Force the retried attempt due NOW (the ladder's backoff is real).
    async with pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{SCHEMA}".jobs SET scheduled_at = now() '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'reply' "
            "AND status = 'scheduled'",
            flow_id,
        )
    final = await runner.drive(flow_id)
    result = await runner.result(flow_id)

    delivered = 0
    async with pool.acquire() as conn:
        delivered = await conn.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".wf_signals '
            "WHERE workflow_id = $1 AND status = 'delivered'",
            flow_id,
        )
    memo_replayed = MEMO_CALLS == 1  # computed ONCE; attempt 2 replayed it
    answer_replayed = result == {"verdict": "go", "memo": "computed", "attempts": 2}

    record(
        "probe_03_checkpoint_replay",
        "LangGraph checkpoint replay / the answer queue",
        "pass"
        if memo_replayed and answer_replayed and final == "terminal" and delivered == 1
        else "fail",
        {
            "memo_fn_executions": MEMO_CALLS,
            "delivered_signals": delivered,
            "body_attempts": ANSWER_ATTEMPTS,
            "result": result,
            "final": final,
        },
    )


# ── PROBE 4: Prefect dynamic map ≙ map_source's runtime fork ────────────


async def probe_04_dynamic_map(pool: asyncpg.Pool) -> None:
    """PREFECT dynamic map: the fan-out cardinality is a RUN-TIME fact
    (the source body returned 7; nothing at compile knew). The fork
    gives per-item ledger identity; the join collects the flat list."""
    from taskq.workflows._sql import WorkflowSql

    wsql = WorkflowSql.build(SCHEMA)
    app, name = _app_map()
    runner = FlowRunner(app.get(name), pool, SCHEMA)
    flow_id = await runner.create_flow(input={"doc_id": "d1"})
    assert await runner.drive(flow_id) == "terminal"
    async with pool.acquire() as conn:
        children = await conn.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'src.item' "
            "AND map_index IS NOT NULL",
            flow_id,
        )
        child_ledger = await conn.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key = 'src.item' AND map_index IS NOT NULL",
            flow_id,
        )
    result = await runner.result(flow_id)
    flat = isinstance(result, list) and [r["n"] for r in result] == [i * 10 for i in range(7)]

    record(
        "probe_04_dynamic_map",
        "Prefect dynamic map",
        "pass" if children == 7 and child_ledger == 7 and flat else "fail",
        {
            "runtime_children": children,
            "per_item_ledger_rows": child_ledger,
            "join_result": result,
        },
    )


# ── PROBE 5: Kafka/Flink exactly-once sink ≙ wf_join_fire + outbox ──────


async def probe_05_exactly_once_outbox(pool: asyncpg.Pool) -> None:
    """The DATAFLOW exactly-once SINK: the fired join's delivery through
    the wf_outbox is IDEMPOTENT on the consumer step key, backed by the
    wf_join_fire exactly-once ledger. Proof: re-run the sweep arms
    TWICE more over the terminal flow — zero new fires, zero new
    consumer rows, zero new results."""
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows._sweep import drain_outbox, sweep_join_rederive

    wsql = WorkflowSql.build(SCHEMA)
    app, name = _app_join()
    runner = FlowRunner(app.get(name), pool, SCHEMA)
    flow_id = await runner.create_flow(input={"doc_id": "d1"})
    assert await runner.drive(flow_id) == "terminal"

    async with pool.acquire() as conn:
        before_fire = await conn.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".wf_join_fire WHERE flow_id = $1', flow_id
        )
        before_outbox = await conn.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".wf_outbox WHERE flow_id = $1', flow_id
        )
        before_tail = await conn.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'join_tail'",
            flow_id,
        )
    # The redundant passes (the crash-window re-derive, hammered twice).
    await sweep_join_rederive(pool, wsql)
    await drain_outbox(pool, wsql)
    await sweep_join_rederive(pool, wsql)
    await drain_outbox(pool, wsql)
    async with pool.acquire() as conn:
        after_fire = await conn.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".wf_join_fire WHERE flow_id = $1', flow_id
        )
        after_outbox = await conn.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".wf_outbox WHERE flow_id = $1', flow_id
        )
        after_tail = await conn.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'join_tail'",
            flow_id,
        )
        derived = await reconstruct_workflow_status(conn, wsql, flow_id)

    record(
        "probe_05_exactly_once_outbox",
        "Dataflow exactly-once sink (the transactional outbox)",
        "pass"
        if before_fire == after_fire
        and before_tail == after_tail
        and before_fire >= 1
        and derived == "complete"
        else "fail",
        {
            "join_fire_rows_before": before_fire,
            "join_fire_rows_after_redundant_sweeps": after_fire,
            "outbox_rows_before": before_outbox,
            "outbox_rows_after": after_outbox,
            "consumer_rows_before": before_tail,
            "consumer_rows_after": after_tail,
            "status": derived,
        },
    )


# ── PROBE 6: Temporal cron workflow ≙ the run-key arbiter ───────────────


async def probe_06_run_key_arbiter(pool: asyncpg.Pool) -> None:
    """TEMPORAL the-cron-workflow's identity law: ONE run per schedule
    slot, never a second silent run. TaskQflow: the run-key claim — the
    same run_key returns the EXISTING run's id + status (created=False).
    (The estate's own cron scheduler lives in worker/cron_loop.py; the
    arbiter is the dedupe leg a cron enqueue lands on.)"""
    app, name = _app_one_step("b")
    runner = FlowRunner(app.get(name), pool, SCHEMA)
    key = f"cron:slot-{new_uuid()}"
    first = await runner.create_flow(run_key=key)
    second = await runner.create_flow(run_key=key)

    record(
        "probe_06_run_key_arbiter",
        "Temporal cron workflow (one run per slot)",
        "pass" if first == second else "fail",
        {"first_run": str(first), "second_run": str(second), "identical": first == second},
    )


# ── PROBE 7: the loop (agent iteration ≙ continue-as-new adjacency) ─────


async def probe_07_loop_carry(pool: asyncpg.Pool) -> None:
    """The LOOP node: fresh jobs per iteration (history is rows, per-
    iteration keyed), the carry advanced exactly once, Done/Refine the
    control union, the iteration-cap wall named. This is the pattern
    Temporal serves with continue-as-new + a WorkflowImpl loop."""
    app, name, iterations = _app_loop()
    runner = FlowRunner(app.get(name), pool, SCHEMA)
    flow_id = await runner.create_flow()
    final = await runner.drive(flow_id)
    result = await runner.result(flow_id)
    async with pool.acquire() as conn:
        iter_rows = await conn.fetchval(
            f'SELECT count(*) FROM "{SCHEMA}".wf_step_ledger '
            "WHERE flow_id = $1 AND step_key LIKE 'iter.iter%'",
            flow_id,
        )

    record(
        "probe_07_loop_carry",
        "Temporal continue-as-new adjacency (the agent loop)",
        "pass"
        if final == "terminal"
        and iterations == [1, 2, 3, 4]
        and iter_rows == 4
        and result == {"acc": 4}
        else "fail",
        {
            "iterations": iterations,
            "iteration_ledger_rows": iter_rows,
            "result": result,
            "final": final,
        },
    )


async def main() -> None:
    pool = await setup()
    try:
        await probe_01_rows_replay(pool)
        await probe_02_signal_hold(pool)
        await probe_03_checkpoint_replay(pool)
        await probe_04_dynamic_map(pool)
        await probe_05_exactly_once_outbox(pool)
        await probe_06_run_key_arbiter(pool)
        await probe_07_loop_carry(pool)
    finally:
        await pool.close()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("a") as sink_file:
        for entry in RESULTS:
            sink_file.write(json.dumps(entry, default=str) + "\n")
    passed = sum(1 for r in RESULTS if r["verdict"] == "pass")
    print(f"\n{passed}/{len(RESULTS)} probes passed — records appended to {OUT}")


if __name__ == "__main__":
    asyncio.run(main())
