# ruff: noqa: S608  # Why: the SQL carries only value parameters or a drill-owned identifier — no untrusted text reaches any statement.
"""DRILL — the fan-out scale face (N=200) + the progress-push + admin faces.

200 sites fan out (one child per site), run to completion, and fan back
in. While the drive runs, THREE observability faces are captured:

1. THE PROGRESS PUSH — a ``ProgressListener`` subscribed to the run
   collects the pushed events while the fleet works (the receipt IS the
   event stream: count + samples);
2. THE ADMIN VIEW, MID-FLIGHT — the operator surface (``taskq flows
   list`` + ``taskq flows status``) sampled WHILE the fan-out is live;
3. THE TERMINAL AGGREGATE — the typed report decoded at the result read,
   all 200 accounted for.

The wall-clock is reported honestly. One command:

    python -m examples.site_audit.drills.scale
"""

from __future__ import annotations

import asyncio
import pathlib
import subprocess
import sys
import time

import asyncpg

from examples.site_audit.drills import _harness
from examples.site_audit.pipeline import AuditInput, AuditReport, SiteSpec, app
from taskq.workflows import FlowRunner, ProgressBackfilled, ProgressListener, ProgressUpdated

N = 200


async def _cli(*args: str) -> str:
    taskq_bin = str(pathlib.Path(sys.executable).parent / "taskq")
    proc = await asyncio.create_subprocess_exec(
        taskq_bin,
        *args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    out, _ = await proc.communicate()
    return out.decode()


async def drill() -> None:
    schema = _harness.use_schema("scale")
    await _harness.provision(schema)
    pool = await asyncpg.create_pool(_harness.dsn())
    runner = FlowRunner(app.get("site_audit"), pool, schema)

    plan = AuditInput(
        sites=[
            *[
                SiteSpec(site_id=f"site-{i:03}", kind="sensor", reading=60.0 + (i % 20))
                for i in range(N // 2)
            ],
            *[
                SiteSpec(site_id=f"site-{i:03}", kind="panel", tilt_deg=10.0 + (i % 25))
                for i in range(N // 2, N)
            ],
        ]
    )
    flow_id = (await runner.create_flow(input=plan)).flow_id
    print(f"run={flow_id}")

    # 1. the progress push: the listener collects while the drive runs
    events: list[ProgressUpdated | ProgressBackfilled] = []

    async def listen() -> None:
        listener = ProgressListener(pool, schema, flow_id=flow_id)
        async with listener:
            async for event in listener.updates():
                events.append(event)

    listener_task = asyncio.create_task(listen())

    # the drive + the mid-flight admin sample, concurrently
    started = time.monotonic()

    async def drive_and_sample() -> None:
        drive_task = asyncio.create_task(runner.drive(flow_id, until="terminal"))
        await asyncio.sleep(1.5)  # the fan-out is live now
        print("--- ADMIN VIEW (mid-fan-out) ---")
        print((await _cli("flows", "list", "--limit", "1")).rstrip())
        print((await _cli("flows", "status", str(flow_id))).rstrip())
        counts = await pool.fetchrow(
            f"SELECT count(*) FILTER (WHERE status = 'succeeded') AS done, "
            f"count(*) FILTER (WHERE status = 'running') AS live, "
            f"count(*) FILTER (WHERE status = 'pending') AS waiting "
            f"FROM \"{schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 "
            "AND step_key LIKE 'plan.item%'",
            flow_id,
        )
        if counts is not None:
            print(
                f"children mid-flight: succeeded={counts['done']} running={counts['live']} "
                f"pending={counts['waiting']}"
            )
        await drive_task

    await drive_and_sample()
    wall = time.monotonic() - started
    await asyncio.sleep(1.0)  # let the listener drain its last frames
    listener_task.cancel()

    # 2. the terminal faces
    report = AuditReport.model_validate(await runner.result(flow_id))
    pushed = [e for e in events if isinstance(e, ProgressUpdated)]
    print("--- ADMIN VIEW (terminal) ---")
    print((await _cli("flows", "status", str(flow_id))).rstrip())

    print("--- THE PROGRESS PUSH (the event stream's receipt) ---")
    distinct_nodes = {str(e.node_id) for e in pushed}
    print(
        f"events received: {len(events)} (updates={len(pushed)}, "
        f"distinct nodes named={len(distinct_nodes)})"
    )
    for e in pushed[:3]:
        print(f"  sample: node={e.node_id} source={e.source}")
    # the push is the POINTER (the docs' own law) — the per-entity payload
    # lives in the state/stream rows; the stream's reach is the receipt.

    print("--- THE AGGREGATE ---")
    print(f"AUDIT SCALE OK wall={wall:.2f}s REPORT {report.model_dump()}")
    assert report.planned == N
    assert report.sensor_audited == N // 2 and report.panel_audited == N // 2
    assert report.failed == []
    print(
        f"FAN-OUT SCALE PROOF OK — {N} entities fanned out, fanned in, "
        f"terminal aggregate exact, wall={wall:.2f}s"
    )
    await pool.close()


if __name__ == "__main__":
    _harness.run(drill)
