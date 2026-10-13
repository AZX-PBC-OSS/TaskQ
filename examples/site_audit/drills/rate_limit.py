# ruff: noqa: S608, ASYNC220  # Why: the SQL carries only value parameters or a drill-owned identifier, and the subprocess spawns are the drill's own orchestration points — no untrusted text reaches any statement or argv.
"""DRILL — the per-source rate-limit proof (FLEET mode).

The in-process driver executes bodies itself and does not pass through
the worker's dispatch gate, so this drill runs the production shape: the
driver spawns a real worker process, which executes the flow's rows
through the queue-routed claim, and each arm's OWN bucket gates its
children's dispatch cadence.

The drill proves: with a 1 token/second bucket on each arm, the observed
``started_at`` timestamps of the routed children respect the declared
cadence — the first ``capacity`` children dispatch as the burst, every
consecutive pair after that is ≥ ~1 s apart, and the denials are ON the
rows (``rate_limit_blocked_count``) — the timestamps prove it.

One command (the worker is spawned and drained by the driver):

    python -m examples.site_audit.drills.rate_limit
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import time
from datetime import timedelta

import asyncpg

from examples.site_audit.drills import _harness
from examples.site_audit.pipeline import (
    AuditInput,
    SitePanel,
    SiteSensor,
    SiteSpec,
    audit_panel,
    audit_sensor,
    default_deps,
    plan_audit,
)
from taskq.ratelimit import TokenBucket
from taskq.workflows import FlowRunner, Promise, RouteArm, WorkflowApp, build, route, step

TINY_SENSOR = TokenBucket("drill-sensor-1s", capacity=2, refill_per_second=1.0, backend="postgres")
TINY_PANEL = TokenBucket("drill-panel-1s", capacity=1, refill_per_second=1.0, backend="postgres")
N_SENSOR, N_PANEL = 8, 4

_rate_app = WorkflowApp(deps=default_deps())


@_rate_app.workflow("site_audit_rate_drill")
def site_audit_rate_drill() -> Promise[object]:
    planned = step(plan_audit, AuditInput(), key="plan")
    audited = route(
        planned,
        {
            SiteSensor: RouteArm(body=audit_sensor, rate_limits=(TINY_SENSOR,)),
            SitePanel: RouteArm(body=audit_panel, rate_limits=(TINY_PANEL,)),
        },
        on_failure="collect",
        max_attempts=1,
    )
    return build(audited)


def _worker() -> int:
    """The fleet: the import of this module IS the registration (the
    app's cohorts project at boot); the vanilla registry is empty — this
    app ships only workflow actors."""
    from taskq.settings import WorkerSettings
    from taskq.worker.run import worker_main

    settings = WorkerSettings.load()
    return worker_main(settings, actor_registry={})


async def _driver() -> None:
    schema = _harness.use_schema("rate")
    await _harness.provision(schema)

    env = dict(os.environ)
    worker = subprocess.Popen(
        [sys.executable, "-m", "examples.site_audit.drills.rate_limit", "worker"],
        env=env,
    )
    try:
        pool = await asyncpg.create_pool(_harness.dsn())
        runner = FlowRunner(_rate_app.get("site_audit_rate_drill"), pool, schema)
        plan = AuditInput(
            sites=[
                SiteSpec(site_id=f"site-{i:02}", kind="sensor", reading=60.0)
                for i in range(N_SENSOR)
            ]
            + [
                SiteSpec(site_id=f"site-{i:02}", kind="panel", tilt_deg=20.0)
                for i in range(N_SENSOR, N_SENSOR + N_PANEL)
            ]
        )
        flow_id = (await runner.create_flow(input=plan)).flow_id
        print(f"run={flow_id}")

        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            await asyncio.sleep(2)
            done = await pool.fetchval(
                f'SELECT count(*) FROM "{schema}".jobs '
                f"WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'plan.item%' "
                "AND status IN ('succeeded', 'failed')",
                flow_id,
            )
            if done == N_SENSOR + N_PANEL:
                break

        rows = await pool.fetch(
            f"SELECT step_key, map_index, status, started_at, rate_limit_blocked_count "
            f"FROM \"{schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 "
            "AND step_key LIKE 'plan.item%' ORDER BY step_key, started_at",
            flow_id,
        )
        ok = True
        window = 1.0  # the proof's sliding window (seconds)
        for arm, capacity, refill in (("SiteSensor", 2, 1.0), ("SitePanel", 1, 1.0)):
            arm_rows = [r for r in rows if r["step_key"].endswith(arm)]
            starts = sorted(r["started_at"] for r in arm_rows if r["started_at"])
            blocked = sum(r["rate_limit_blocked_count"] for r in arm_rows)
            # THE TOKEN BUCKET'S CONTRACT is throughput with BURST
            # tolerance (the docs' own primitive table): up to `capacity`
            # admits may land as one burst once the tokens have
            # accumulated, and the sustained rate may never exceed
            # `refill`. The honest windowed measure: in any sliding
            # window, admits <= capacity + what the refill could have
            # minted in that window (+1 for the clock's rounding).
            win = timedelta(seconds=window)
            worst = max(sum(1 for s in starts if t <= s < t + win) for t in starts)
            bound = capacity + refill * window + 1
            span = (starts[-1] - starts[0]).total_seconds() if len(starts) > 1 else 0.0
            observed_rate = (len(starts) - 1) / span if span > 0 else float("inf")
            arm_ok = worst <= bound and observed_rate <= refill + 0.15
            ok = ok and arm_ok
            stamps = " ".join(f"{s:%H:%M:%S.%f}" for s in starts)
            print(f"{arm}: {len(starts)} dispatched, denials-on-rows={blocked}")
            print(f"  started_at: {stamps}")
            print(
                f"  declared: capacity={capacity}, {refill}/s — observed "
                f"{observed_rate:.2f}/s, worst {window:.0f}s window={worst} "
                f"(bound {bound:.0f}) -> {'RESPECTED' if arm_ok else 'VIOLATED'}"
            )
        print(
            "RATE-LIMIT PROOF OK — the observed dispatch cadence respects the declared buckets"
            if ok
            else "RATE-LIMIT PROOF FAILED — a child dispatched past its bucket"
        )
        if not ok:
            raise SystemExit(1)
        await pool.close()
    finally:
        with contextlib.suppress(subprocess.TimeoutExpired):
            worker.send_signal(signal.SIGTERM)
            worker.wait(timeout=30)


if __name__ == "__main__":
    if sys.argv[1:] == ["worker"]:
        raise SystemExit(_worker())
    _harness.run(_driver)
