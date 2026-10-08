"""PHASE A2 — the vanilla worker when the workflow actors' rows DO exist.

Phase A proved the workflow rows are INVISIBLE to the vanilla claim (the
actor_config LATERAL drops the never-synced 'wf' cohort). This phase
stamps the missing rows (the operator's manual sync — what the F3
projection WOULD have done) and reruns the same 30 s watch: does the
vanilla worker now claim the row, and what becomes of it?
"""

from __future__ import annotations

import asyncio
import glob
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

import asyncpg

from taskq.actor_config import ActorConfig
from taskq.migrate import apply_pending
from taskq.worker.startup import sync_actor_config

sys.path.insert(0, str(Path(__file__).parent))

DSN = os.environ.get("PROBE_DSN", "postgresql://taskq:taskq@localhost:5710/taskq")
BASE = Path("/tmp/opencode/execution-cure")

import probe  # noqa: E402  (the probe module carries the app + the chains)


async def main() -> None:
    schema = "exec_a2"
    probe.CTX_SCHEMA = schema
    conn = await asyncpg.connect(DSN)
    await apply_pending(conn, schema=schema)
    pool = await asyncpg.create_pool(DSN)

    # THE OPERATOR'S MANUAL SYNC — the rows the F3 projection should have
    # written at worker boot: every distinct (actor, queue) the flow's
    # rows will carry.
    for aq in {("wf", "default"), ("wf", "gpu")}:
        await sync_actor_config(
            conn,
            [ActorConfig(actor=aq[0], max_concurrent=None, queue=aq[1])],
            force=False,
            schema=schema,
        )
    print("== actor_config rows stamped for the workflow actors")

    compiled = probe.app.get("gpu_flow")
    runner = probe.FlowRunner(compiled, pool, schema)
    flow_id = await runner.create_flow()
    print(f"== flow created: {flow_id}")

    worker = probe.start_worker(schema, ["gpu", "default"], "vanilla-stamped")
    print("== vanilla worker launched — watching 30s")
    seen: dict[str, tuple[str, int, str | None]] = {}
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            rows = await conn.fetch(probe.NODE_ROWS_SQL.format(schema=schema))
            for r in rows:
                key = str(r["id"])
                state = (r["status"], int(r["attempt"]), r["released"])
                if seen.get(key) != state:
                    print(
                        f"   [{time.strftime('%H:%M:%S')}] {r['step_key']}"
                        f" q={r['queue']} -> status={r['status']}"
                        f" attempt={r['attempt']} released={r['released']}"
                    )
                    seen[key] = state
            await asyncio.sleep(2)
    finally:
        worker.terminate()
        out, _ = worker.communicate(timeout=15)
    (BASE / "worker-a2.log").write_text(out or "")

    events = sorted(glob.glob("/tmp/opencode/execution-cure/*-events.log"))
    fresh = []
    for p in events:
        for line in Path(p).read_text().splitlines():
            d = json.loads(line)
            fresh.append((Path(p).name, d))
    print(f"== BODY EXECUTIONS OBSERVED (all schemas, cumulative): {len(fresh)}")
    for name, d in fresh[-8:]:
        print(f"    {name}: {d}")

    anf = [ln for ln in (out or "").splitlines() if "actor-not-found" in ln]
    print(f"== the worker's actor-not-found lines: {len(anf)}")
    for line in anf[:10]:
        print("   |", line[:220])

    rows = await conn.fetch(probe.NODE_ROWS_SQL.format(schema=schema))
    print("== the rows at the end:")
    for r in rows:
        print("   ", dict(r))
    flow_row = await conn.fetchrow(
        f"SELECT status FROM {schema}.jobs WHERE id = $1", flow_id
    )
    print(f"== flow root status: {flow_row['status']}")
    await pool.close()
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
