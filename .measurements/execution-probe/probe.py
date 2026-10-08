"""THE EXECUTION PROBE — does PRODUCTION execution of workflow nodes work?

Phase A (exec_a schema): a flow whose chain declares queue="gpu" is
CREATED (the public entry, FlowRunner.create_flow) and then LEFT ALONE.
A REAL vanilla worker process (separate process, `python -m taskq
worker`, an actors module that never imports taskq.workflows — the
no-taskq[flows] deployment) polls the gpu queue. The §9.1 question: does
the row's queue route the node to a pool that RESOLVES AND EXECUTES its
body?

Phase B (exec_b schema): same flow, but the probe process DRIVES it
(FlowRunner.drive) WHILE a second vanilla worker (plain "default" pool)
runs. Questions: does the flow complete? In WHICH process did the bodies
run? What did the vanilla worker do to the workflow rows it claimed?

The capture law: every verdict prints; the caller tees the whole run to
.measurements/. NO drive() in phase A — that is the point.

Run: python .measurements/execution-probe/probe.py
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import asyncpg

from taskq.migrate import apply_pending
from taskq.workflows import Chain, DONE, FlowRunner, Route, Step, WorkflowApp, build, chain_source, chain_start
DSN = os.environ.get("PROBE_DSN", "postgresql://taskq:taskq@localhost:5710/taskq")
BASE = Path("/tmp/opencode/execution-cure")
BASE.mkdir(parents=True, exist_ok=True)


def record(schema: str, kind: str, body: dict[str, Any]) -> None:
    BASE.mkdir(parents=True, exist_ok=True)
    with (BASE / f"{schema}-events.log").open("a") as fh:
        fh.write(json.dumps({"kind": kind, **body}) + "\n")


# ── the probe workflow: a chain on the gpu queue ─────────────────────────

PROBE_JOBS: dict[str, str] = {}


class ScreenOutcome(cls_enum := __import__("enum").Enum):
    CLEAN = "clean"
    FLAGGED = "flagged"


async def screen(ctx: Any, item: dict[str, object]) -> ScreenOutcome:
    job_id = str(ctx.job_id)
    PROBE_JOBS[job_id] = "screen"
    record(CTX_SCHEMA, "body-ran", {"body": "screen", "job_id": job_id})
    return ScreenOutcome.CLEAN


CHAIN = Chain(
    name="gpu-chain",
    start="screen",
    steps={
        "screen": Step(
            body=screen,
            outcomes=ScreenOutcome,
            route=Route({ScreenOutcome.CLEAN: DONE, ScreenOutcome.FLAGGED: DONE}),
        ),
    },
    actor="wf",
    queue="gpu",
)

app = WorkflowApp()


@app.workflow("gpu_flow")
def gpu_flow() -> object:
    src = chain_source(CHAIN, source_body, key="doc_source")
    return build(src)


async def source_body(ctx: Any) -> None:
    job_id = str(ctx.job_id)
    record(CTX_SCHEMA, "body-ran", {"body": "source", "job_id": job_id})
    await ctx.emit_batch(
        [chain_start(CHAIN, {"doc_id": f"doc-{i}"}, map_index=i, trace_id=f"doc-{i}") for i in (1, 2)],
        cursor={"page": 0},
    )


CTX_SCHEMA = ""  # set per phase

NODE_ROWS_SQL = """
SELECT id, step_key, actor, queue, status, attempt, map_index,
       metadata->>'flow_id' AS flow, metadata->>'released_reason' AS released,
       started_at, finished_at
FROM {schema}.jobs
WHERE (metadata->>'flow_id') IS NOT NULL
ORDER BY created_at, id
"""


async def dump_rows(conn: asyncpg.Connection, schema: str, label: str) -> list[dict[str, Any]]:
    rows = await conn.fetch(NODE_ROWS_SQL.format(schema=schema))
    print(f"== {label}")
    for r in rows:
        print("   ", dict(r))
    return [dict(r) for r in rows]


def start_worker(schema: str, queues: list[str], label: str) -> subprocess.Popen[str]:
    env = dict(
        os.environ,
        TASKQ_PG_DSN=DSN,
        TASKQ_SCHEMA_NAME=schema,
        PYTHONPATH=f"{ROOT / 'src'}:{Path(__file__).parent}",
    )
    cmd = [
        sys.executable,
        "-m",
        "taskq",
        "worker",
        "--actors",
        "actors_gpu:registry",
        "--poll-interval",
        "0.5",
    ]
    for q in queues:
        cmd.extend(["--queues", q])
    return subprocess.Popen(  # noqa: S603 — the probe launches ITS OWN command
        cmd,
        env=env,
        cwd=str(Path(__file__).parent),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


async def watch(conn: asyncpg.Connection, schema: str, seconds: float) -> list[dict[str, Any]]:
    """Poll the node rows every 2 s for *seconds*, printing transitions."""
    seen: dict[str, tuple[str, int, str | None]] = {}
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        rows = await dump_rows_quiet(conn, schema)
        for r in rows:
            key = str(r["id"])
            state = (r["status"], int(r["attempt"]), r["released"])
            if seen.get(key) != state:
                print(
                    f"   [{time.strftime('%H:%M:%S')}] {r['step_key']}"
                    f" (map={r['map_index']}) q={r['queue']} ->"
                    f" status={r['status']} attempt={r['attempt']}"
                    f" released={r['released']}"
                )
                seen[key] = state
        await asyncio.sleep(2)
    return await dump_rows_quiet(conn, schema)


async def dump_rows_quiet(conn: asyncpg.Connection, schema: str) -> list[dict[str, Any]]:
    rows = await conn.fetch(NODE_ROWS_SQL.format(schema=schema))
    return [dict(r) for r in rows]


async def phase_a() -> None:
    global CTX_SCHEMA
    schema = "exec_a"
    CTX_SCHEMA = schema
    print("=" * 72)
    print("PHASE A — the vanilla worker alone vs. a gpu-queue flow (NO drive)")
    print("=" * 72)
    conn = await asyncpg.connect(DSN)
    await apply_pending(conn, schema=schema)
    pool = await asyncpg.create_pool(DSN)
    os.environ["TASKQ_SCHEMA_NAME"] = schema  # not needed for the runner; explicit anyway

    compiled = app.get("gpu_flow")
    runner = FlowRunner(compiled, pool, schema)
    flow_id = await runner.create_flow()
    print(f"== flow created: {flow_id}")

    rows = await dump_rows(conn, schema, "the rows at create (pre-worker)")
    for r in rows:
        print(f"    {r['step_key']}: actor={r['actor']} queue={r['queue']} status={r['status']}")

    worker = start_worker(schema, ["gpu", "default"], "vanilla-both")
    print("== vanilla worker launched (queues=gpu,default, NO workflows import) — watching 30s")
    try:
        await watch(conn, schema, 30)
    finally:
        worker.terminate()
        out, _ = worker.communicate(timeout=15)
    (BASE / "worker-a.log").write_text(out or "")
    print("== the worker's log (last 40 lines):")
    for line in (out or "").splitlines()[-40:]:
        print("   |", line)

    events = BASE / f"{schema}-events.log"
    bodies = events.read_text().splitlines() if events.exists() else []
    print(f"== BODY EXECUTIONS OBSERVED (any process): {len(bodies)}")
    for line in bodies:
        print("   ", line)

    final = await dump_rows(conn, schema, "the rows at the end of phase A")
    executed = len(bodies) > 0
    flow_row = await conn.fetchrow(
        f"SELECT status FROM {schema}.jobs WHERE id = $1", flow_id
    )
    print(f"== flow root status: {flow_row['status']}")
    print(
        f"== PHASE A VERDICT: {'BODIES EXECUTED ON THE WORKER' if executed else 'NO BODY EVER EXECUTED — the queue routed the rows to a pool that cannot resolve them'}"
    )
    await pool.close()
    await conn.close()


async def phase_b() -> None:
    global CTX_SCHEMA
    schema = "exec_b"
    CTX_SCHEMA = schema
    print("=" * 72)
    print("PHASE B — FlowRunner.drive WHILE a vanilla worker (default pool) runs")
    print("=" * 72)
    conn = await asyncpg.connect(DSN)
    await apply_pending(conn, schema=schema)
    pool = await asyncpg.create_pool(DSN)

    compiled = app.get("gpu_flow")
    runner = FlowRunner(compiled, pool, schema)
    flow_id = await runner.create_flow()
    print(f"== flow created: {flow_id}")

    worker = start_worker(schema, ["default"], "vanilla-default")
    print("== vanilla worker launched (queues=default) — driving the flow")
    try:
        verdict = await runner.drive(flow_id)
        print(f"== drive verdict: {verdict!r}")
    finally:
        worker.terminate()
        out, _ = worker.communicate(timeout=15)
    (BASE / "worker-b.log").write_text(out or "")

    bodies = (BASE / f"{schema}-events.log")
    lines = bodies.read_text().splitlines() if bodies.exists() else []
    print(f"== BODY EXECUTIONS OBSERVED: {len(lines)}")
    for line in lines:
        print("   ", line)
    worker_lines = (out or "").splitlines()
    anf = [ln for ln in worker_lines if "actor-not-found" in ln or "workflow" in ln.lower()]
    print(f"== the worker's actor-not-found/workflow log lines: {len(anf)}")
    for line in anf[:20]:
        print("   |", line)
    await dump_rows(conn, schema, "the rows at the end of phase B")
    flow_row = await conn.fetchrow(
        f"SELECT status FROM {schema}.jobs WHERE id = $1", flow_id
    )
    print(f"== flow root status: {flow_row['status']}")
    await pool.close()
    await conn.close()


async def main() -> None:
    await phase_a()
    await phase_b()
    print("=" * 72)
    print("PROBE COMPLETE")


if __name__ == "__main__":
    asyncio.run(main())
