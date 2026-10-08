"""THE CURE PROBE — the placement proof (the execution verdict's probes
re-run as the cures' greens).

The deployment shape, real end to end: real migrations on a dedicated
Postgres (:5710), real ``python -m taskq worker`` SUBPROCESSES, real
dispatch. The bodies record their own (pid, job_id) — every verdict
below reads WHO EXECUTED, not who claims to have.

* PHASE A — the mixed fleet: ONE flows-capable worker (imports
  wf_defs; the boot's F3 projection syncs the workflow cohorts) AND
  ONE vanilla co-tenant (never imports taskq.workflows) — both polling
  gpu+default. The flow is created and LEFT ALONE. Questions: does the
  pending source row get claimed and EXECUTED on the right pool (the
  probe A green)? Does each body run EXACTLY ONCE (the probe A2 green
  — no snooze loop)? Does the vanilla co-tenant stay off the flow rows
  (the fence's defined behavior — never claimed, not snoozed)?

* PHASE B — drive() WHILE the capable worker runs: the in-process
  runner only ORCHESTRATES; the bodies must execute in the WORKER
  process (the probe B green — zero body executions in the driving
  process).

* PHASE S — the SPLIT PLACEMENT with both pools live: the capable
  worker on default ONLY, the capable worker on gpu ONLY. The source
  node (actor 'wf', queue 'default') must execute on the default
  worker's pid; the chain steps (actor 'wf-gpu', queue 'gpu') on the
  gpu worker's pid — each node on ITS pool, the §9.1 heterogeneous
  placement, end to end.

Run: PROBE_DSN=postgresql://taskq:taskq@localhost:5710/taskq \\
     python .measurements/execution-probe/probe_cure.py
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import asyncpg

from taskq.migrate import apply_pending
from taskq.workflows import FlowRunner

DSN = os.environ.get("PROBE_DSN", "postgresql://taskq:taskq@localhost:5710/taskq")
BASE = Path("/tmp/opencode/execution-cure")
EVENTS = BASE / "cure-events.log"
PROBE_DIR = Path(__file__).parent

ROWS_SQL = """
SELECT id, step_key, actor, queue, status, attempt, map_index,
       metadata->>'flow_id' AS flow
FROM {schema}.jobs
WHERE (metadata->>'flow_id') IS NOT NULL
ORDER BY created_at, id
"""


def worker_pid_events(pid: int) -> list[dict[str, Any]]:
    if not EVENTS.exists():
        return []
    out = []
    for line in EVENTS.read_text().splitlines():
        d = json.loads(line)
        if d.get("pid") == pid:
            out.append(d)
    return out


def body_events() -> list[dict[str, Any]]:
    if not EVENTS.exists():
        return []
    return [json.loads(ln) for ln in EVENTS.read_text().splitlines() if '"body-ran"' in ln]


def start_worker(
    schema: str,
    queues: list[str],
    actors_ref: str,
) -> subprocess.Popen[str]:
    env = dict(
        os.environ,
        TASKQ_PG_DSN=DSN,
        TASKQ_SCHEMA_NAME=schema,
        PYTHONPATH=f"{ROOT / 'src'}:{PROBE_DIR}",
    )
    cmd = [
        sys.executable,
        "-m",
        "taskq",
        "worker",
        "--actors",
        actors_ref,
        "--poll-interval",
        "0.3",
    ]
    for q in queues:
        cmd.extend(["--queues", q])
    return subprocess.Popen(  # noqa: S603 — the probe launches ITS OWN command
        cmd,
        env=env,
        cwd=str(PROBE_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


async def dump_rows(conn: asyncpg.Connection, schema: str) -> list[dict[str, Any]]:
    return [dict(r) for r in await conn.fetch(ROWS_SQL.format(schema=schema))]


async def wait_flow_terminal(
    conn: asyncpg.Connection, schema: str, flow_id: str, seconds: float
) -> str:
    deadline = time.monotonic() + seconds
    status = ""
    while time.monotonic() < deadline:
        status = (
            await conn.fetchval(
                f"SELECT status::text FROM {schema}.jobs WHERE id = $1", __import__("uuid").UUID(flow_id)
            )
        ) or ""
        if status in ("succeeded", "failed", "cancelled"):
            return status
        await asyncio.sleep(1)
    return status or "running"


def stop_worker(worker: subprocess.Popen[str], log_name: str) -> str:
    worker.terminate()
    out, _ = worker.communicate(timeout=20)
    (BASE / log_name).write_text(out or "")
    return out or ""


def assert_clean_phase(schema: str, label: str, worker_pids: dict[str, int]) -> None:
    """The phase's verdict lines — printed, the caller tees the run."""
    events = body_events()
    print(f"== [{label}] BODY EXECUTIONS OBSERVED: {len(events)}")
    for e in events:
        who = next((n for n, p in worker_pids.items() if p == e["pid"]), f"pid={e['pid']}")
        print(f"    {e['body']} job={e['job_id']} in {who}")


async def phase_a() -> None:
    print("=" * 72)
    print("PHASE A — the mixed fleet: capable worker + vanilla co-tenant")
    print("=" * 72)
    schema = "exec_c"
    conn = await asyncpg.connect(DSN)
    await apply_pending(conn, schema=schema)
    pool = await asyncpg.create_pool(DSN)

    if EVENTS.exists():
        EVENTS.unlink()  # this phase's events only

    import uuid as _uuid

    from taskq.workflows.api._app import WorkflowApp  # the defs module's app

    sys.path.insert(0, str(PROBE_DIR))
    import wf_defs

    compiled = wf_defs.app.get("cure_flow")
    runner = FlowRunner(compiled, pool, schema)
    flow_id = await runner.create_flow()
    print(f"== flow created: {flow_id}")

    rows = await dump_rows(conn, schema)
    for r in rows:
        print(f"    {r['step_key']}: actor={r['actor']} queue={r['queue']} status={r['status']}")

    capable = start_worker(schema, ["default", "gpu"], "wf_defs:registry")
    vanilla = start_worker(schema, ["default", "gpu"], "actors_gpu:registry")
    print("== capable worker (default+gpu) + vanilla co-tenant launched — waiting 40 s")

    status = await wait_flow_terminal(conn, schema, str(flow_id), 40)
    await asyncio.sleep(2)

    cap_out = stop_worker(capable, "worker-capable-a.log")
    van_out = stop_worker(vanilla, "worker-vanilla-a.log")

    print(f"== flow root status: {status}")
    rows = await dump_rows(conn, schema)
    for r in rows:
        print(f"    {r['step_key']} (map={r['map_index']}) q={r['queue']}: {r['status']}")

    events = body_events()
    cap_pid, van_pid = capable.pid, vanilla.pid
    in_capable = [e for e in events if e["pid"] == cap_pid]
    in_vanilla = [e for e in events if e["pid"] == van_pid]
    in_driver = [e for e in events if e["pid"] not in (cap_pid, van_pid)]

    per_job = Counter(e["job_id"] for e in events)
    doubles = {j: c for j, c in per_job.items() if c > 1}

    print(f"== bodies in the CAPABLE worker's process: {len(in_capable)}")
    print(f"== bodies in the VANILLA co-tenant's process: {len(in_vanilla)}")
    print(f"== bodies in ANY OTHER process (the probe's own pid et al): {len(in_driver)}")
    print(f"== bodies that ran MORE THAN ONCE (the snooze-loop red): {doubles or 'none'}")
    anf = [ln for ln in van_out.splitlines() if "actor-not-found" in ln or "workflow" in ln.lower()]
    print(f"== the vanilla co-tenant's flow-row claim/snooze lines: {len(anf)}")

    a_green = (
        status == "succeeded"
        and len(in_vanilla) == 0
        and len(in_driver) == 0
        and len(in_capable) == len(rows) - 1  # every node row minus the root ran
        and not doubles
        and len(anf) == 0
    )
    print(
        f"== PHASE A VERDICT: {'GREEN — the pending row was claimed + EXECUTED on the worker, ONCE, and the co-tenant never touched it' if a_green else 'RED'}"
    )
    await pool.close()
    await conn.close()


async def phase_b() -> None:
    print("=" * 72)
    print("PHASE B — drive() while the capable worker runs: who executes?")
    print("=" * 72)
    schema = "exec_d"
    conn = await asyncpg.connect(DSN)
    await apply_pending(conn, schema=schema)
    pool = await asyncpg.create_pool(DSN)

    if EVENTS.exists():
        EVENTS.unlink()

    sys.path.insert(0, str(PROBE_DIR))
    import wf_defs

    compiled = wf_defs.app.get("cure_flow")
    runner = FlowRunner(compiled, pool, schema)
    flow_id = await runner.create_flow()
    print(f"== flow created: {flow_id}")

    capable = start_worker(schema, ["default", "gpu"], "wf_defs:registry")
    print("== capable worker launched — driving (orchestration-only: execute=False)")
    try:
        verdict = await runner.drive(flow_id, execute=False)
        print(f"== drive verdict: {verdict!r}")
    finally:
        cap_out = stop_worker(capable, "worker-capable-b.log")

    events = body_events()
    driver_pid = os.getpid()
    in_worker = [e for e in events if e["pid"] == capable.pid]
    in_driver = [e for e in events if e["pid"] == driver_pid]
    print(f"== bodies in the WORKER process: {len(in_worker)}")
    print(f"== bodies in the DRIVING process: {len(in_driver)}")
    root = await conn.fetchval(
        f"SELECT status::text FROM {schema}.jobs WHERE id = $1", flow_id
    )
    print(f"== flow root status: {root}")
    b_green = root == "succeeded" and len(in_worker) >= 3 and len(in_driver) == 0
    print(
        f"== PHASE B VERDICT: {'GREEN — the flow succeeded with the bodies in the worker process, the driving process only orchestrating' if b_green else 'RED'}"
    )
    await pool.close()
    await conn.close()


async def phase_s() -> None:
    print("=" * 72)
    print("PHASE S — the split placement, both pools live: each node on ITS pool")
    print("=" * 72)
    schema = "exec_s"
    conn = await asyncpg.connect(DSN)
    await apply_pending(conn, schema=schema)
    pool = await asyncpg.create_pool(DSN)

    if EVENTS.exists():
        EVENTS.unlink()

    sys.path.insert(0, str(PROBE_DIR))
    import wf_defs

    compiled = wf_defs.app.get("cure_flow")
    runner = FlowRunner(compiled, pool, schema)
    flow_id = await runner.create_flow()
    print(f"== flow created: {flow_id}")

    default_pool_worker = start_worker(schema, ["default"], "wf_defs:registry")
    gpu_pool_worker = start_worker(schema, ["gpu"], "wf_defs:registry")
    print("== capable workers: default-only + gpu-only — waiting for the flow")

    status = await wait_flow_terminal(conn, schema, str(flow_id), 40)
    await asyncio.sleep(2)
    d_out = stop_worker(default_pool_worker, "worker-default-s.log")
    g_out = stop_worker(gpu_pool_worker, "worker-gpu-s.log")

    rows = await dump_rows(conn, schema)
    for r in rows:
        print(f"    {r['step_key']} (map={r['map_index']}) q={r['queue']}: {r['status']}")

    events = body_events()
    src_events = [e for e in events if e["body"] == "source"]
    screen_events = [e for e in events if e["body"] == "screen"]
    print(f"== source bodies: {len(src_events)} — pids: {[e['pid'] for e in src_events]}")
    print(f"== screen bodies: {len(screen_events)} — pids: {[e['pid'] for e in screen_events]}")

    src_right = all(e["pid"] == default_pool_worker.pid for e in src_events) and len(src_events) == 1
    screen_right = (
        all(e["pid"] == gpu_pool_worker.pid for e in screen_events)
        and len(screen_events) == 2
    )
    print(f"== flow root status: {status}")
    s_green = status == "succeeded" and src_right and screen_right
    print(
        f"== PHASE S VERDICT: {'GREEN — the source executed on the default pool, the chain on the gpu pool: each node on ITS pool' if s_green else 'RED'}"
    )
    await pool.close()
    await conn.close()


async def main() -> None:
    BASE.mkdir(parents=True, exist_ok=True)
    await phase_a()
    await phase_b()
    await phase_s()
    print("=" * 72)
    print("CURE PROBE COMPLETE")


if __name__ == "__main__":
    asyncio.run(main())
