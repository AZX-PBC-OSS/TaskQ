# ruff: noqa: S608, ASYNC220  # Why: the SQL carries only value parameters or a drill-owned identifier, and the subprocess spawns are the drill's own orchestration points — no untrusted text reaches any statement or argv.
"""DRILL — the resume drill: SIGKILL mid-fan-out, restart, the rows are the truth.

Phase A spawns a fresh driver process (its own flow, execution ledger
ON), waits until a slice of the fleet is terminal, then SIGKILLs the
process mid-fan-out. Phase B resumes the SAME flow id the way a
deployment resumes it: a real WORKER process — whose maintenance leader
owns the lock-reclaim sweep — picks the run up, re-claims exactly the
work that was not durably done, and drives the run to terminal. The
drill proves:

1. the entities already terminal when the kill landed did NOT re-run
   (exactly one execution-ledger row each — the idempotent face);
2. the incomplete entities finished (the re-claim re-runs exactly the
   work that was not durably done — their ledger rows may exceed one,
   and that is the honest record of the kill);
3. the run terminalizes with the full aggregate — the rows, not the
   dead process, are the truth.

One command (phases A and B are spawned and watched by the parent):

    python -m examples.site_audit.drills.resume
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys
import time

import asyncpg

import taskq.migrate
from examples.site_audit.drills import _harness
from examples.site_audit.pipeline import AuditInput, AuditReport, SiteSpec, app
from taskq.workflows import FlowRunner

N = 40
KILL_AFTER_TERMINAL = 5


async def _phase_launch() -> None:
    """Phase A: drive a fresh run until a slice is terminal, then die by
    SIGKILL (the drill's parent kills us; the rows are the handoff)."""
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    await taskq.migrate.apply_pending_locked(_harness.dsn(), schema=schema, phase="pre")
    await taskq.migrate.apply_pending_locked(_harness.dsn(), schema=schema, phase="post")
    pool = await asyncpg.create_pool(_harness.dsn())
    runner = FlowRunner(app.get("site_audit"), pool, schema)
    plan = AuditInput(
        sites=[SiteSpec(site_id=f"site-{i:03}", kind="sensor", reading=60.0) for i in range(N // 2)]
        + [SiteSpec(site_id=f"site-{i:03}", kind="panel", tilt_deg=20.0) for i in range(N // 2, N)]
    )
    flow_id = (await runner.create_flow(input=plan)).flow_id
    print(f"run={flow_id}", flush=True)

    # the drive task is INTENTIONALLY unawaited: the drill's parent
    # SIGKILLs this process mid-fan-out — that is the drill's premise.
    asyncio.create_task(runner.drive(flow_id, until="terminal"))  # noqa: RUF006
    while True:
        await asyncio.sleep(0.5)
        done = await pool.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '
            f"WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'plan.item%' "
            "AND status = 'succeeded'",
            flow_id,
        )
        if done >= KILL_AFTER_TERMINAL:
            # part of the fleet is durably done — park here: the
            # SIGKILL is this process's only exit (the fan-out keeps
            # running until the drill's parent lands the kill).
            print(f"KILL WINDOW OPEN pid={os.getpid()} with {done} children terminal", flush=True)
            await asyncio.Event().wait()
    print(f"KILLING pid={os.getpid()} with {done} children terminal", flush=True)


async def _phase_resume() -> None:
    """Phase B: a fresh process re-drives the SAME flow through the
    fleet's own recovery (the worker's maintenance leader reclaims the
    dead process's expired locks)."""
    schema = os.environ["TASKQ_SCHEMA_NAME"]
    pool = await asyncpg.create_pool(_harness.dsn())
    runner = FlowRunner(app.get("site_audit"), pool, schema)
    flow_id = sys.argv[2]

    # the pre-resume truth: WHICH sites were durably done at the kill
    rows = await pool.fetch(
        f'SELECT map_index, payload FROM "{schema}".jobs '
        f"WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'plan.item%' "
        "AND status = 'succeeded'",
        flow_id,
    )
    terminal_sites = {json.loads(r["payload"])["wf_item"]["site_id"] for r in rows}
    print(f"resuming run={flow_id}: {len(terminal_sites)} children were terminal at the kill")

    # the fleet finishes it: the worker's leader sweep re-claims the
    # killed process's expired locks and executes what remains
    worker = subprocess.Popen(
        [sys.executable, "-m", "examples.site_audit.drills.resume", "worker"],
        env=dict(os.environ),
    )
    try:
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            await asyncio.sleep(2)
            status = await pool.fetchval(
                f'SELECT status FROM "{schema}".jobs WHERE id::text = $1', flow_id
            )
            if status == "succeeded":
                break
        wall = time.monotonic() - (deadline - 300)

        report = AuditReport.model_validate(await runner.result(flow_id))
        ledger = await pool.fetch(
            f"SELECT site_id, count(*) AS n FROM {schema}.audit_executions "
            "WHERE flow_id = $1 GROUP BY site_id",
            flow_id,
        )
        exec_counts = {r["site_id"]: r["n"] for r in ledger}
        completed_once = [s for s in terminal_sites if exec_counts.get(s) == 1]
        re_ran = {s: n for s, n in exec_counts.items() if n > 1}
        print(f"resume wall={wall:.2f}s REPORT {report.model_dump()}")
        print(
            f"execution ledger: {len(exec_counts)} sites seen, "
            f"terminal-at-kill with exactly one execution={len(completed_once)}, "
            f"re-ran (the kill's honest record)={re_ran}"
        )
        assert report.planned == N and report.failed == [], report
        assert len(completed_once) == len(terminal_sites), (
            "a durably-done entity re-ran after the restart"
        )
        print(
            f"RESUME PROOF OK — {len(terminal_sites)} children were already terminal at the "
            f"kill and NONE re-ran; the incomplete ones finished; the run terminalized with "
            f"the full aggregate ({report.sensor_audited + report.panel_audited}/{N})"
        )
    finally:
        with contextlib.suppress(subprocess.TimeoutExpired):
            worker.send_signal(signal.SIGTERM)
            worker.wait(timeout=30)
    await pool.close()


def _worker() -> int:
    from taskq.settings import WorkerSettings
    from taskq.worker.run import worker_main

    settings = WorkerSettings.load()
    return worker_main(settings, actor_registry={})


async def drill() -> None:
    schema = _harness.use_schema("resume")
    await _harness.provision(schema)

    # the execution ledger records every audit body START (the drills' receipt)
    os.environ["TASKQ_AUDIT_EXEC_LEDGER"] = "1"
    env = dict(os.environ)

    proc = subprocess.Popen(
        [sys.executable, "-m", "examples.site_audit.drills.resume", "launch"], env=env
    )
    # poll the DB for the terminal slice, then SIGKILL mid-fan-out
    pool = await asyncpg.create_pool(_harness.dsn())
    flow_id = None
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        await asyncio.sleep(0.5)
        if proc.poll() is not None:
            raise SystemExit("launch phase exited before the kill window")
        row = await pool.fetchrow(
            f"SELECT id FROM \"{schema}\".jobs WHERE step_key = '__flow__' ORDER BY id DESC LIMIT 1"
        )
        if row is None:
            continue
        fid = str(row["id"])
        done = await pool.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '
            f"WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'plan.item%' "
            "AND status = 'succeeded'",
            fid,
        )
        if done and done >= KILL_AFTER_TERMINAL:
            flow_id = fid
            break
    if flow_id is None:
        raise SystemExit("the launch phase never reached the kill window")
    proc.send_signal(signal.SIGKILL)
    proc.wait()
    print(f"SIGKILLED the driver (pid={proc.pid}) mid-fan-out: run={flow_id}")

    # phase B: a fresh process resumes the SAME flow id through the fleet
    resume = subprocess.Popen(  # noqa: S603 — the drill's own module, fixed argv
        [sys.executable, "-m", "examples.site_audit.drills.resume", "resume", flow_id],
        env=env,
    )
    code = resume.wait(timeout=600)
    if code != 0:
        raise SystemExit(f"resume phase failed rc={code}")
    await pool.close()


if __name__ == "__main__":
    if sys.argv[1:2] == ["launch"]:
        _harness.run(_phase_launch)
    elif sys.argv[1:2] == ["resume"]:
        _harness.run(_phase_resume)
    elif sys.argv[1:2] == ["worker"]:
        raise SystemExit(_worker())
    else:
        _harness.run(drill)
