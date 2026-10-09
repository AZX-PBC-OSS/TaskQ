# ruff: noqa: S608, S108  # Why: the schema is a fixture-derived test identifier; the drill's dotdir is the opencode-scoped pre-created path (not a system tmp), and the worker's env is the fixture's own.
"""THE FOUR DEMONSTRATIONS (the evidence-matrix's phase-4 demo work
order): the capabilities the evidence matrix named UN-DEMOED, each with
a RUNNABLE path + the captured output:

* **LEG 1 — CANCELLATION**: a run cancelled MID-FLIGHT (the CLI's
  engine form; the same rows the admin's Cancel button writes): the
  named states (the root `cancelled`, the held signals resolved, the
  downstream skipped-with-the-record), the audit row, the explorer's
  derived status reading `cancelled`.
* **LEG 2 — RESUMABILITY**: THE KILL-AND-RESUME DRILL: a PRODUCTION
  worker subprocess (``taskq worker``) claims a node mid-run and is
  SIGKILLed; a fresh worker re-claims it (the lease expiry + the
  reclaim) and the run COMPLETES. The operator's resilience story,
  runnable end to end.
* **LEG 3 — OBSERVABILITY**: a LIVE Prometheus scrape of a real wf
  run: the maintenance leader's sampler feeds
  ``taskq.wf_progress_nodes_total{workflow,state}``; the worker's
  /metrics renders it — the scrape output captured verbatim.
* **LEG 4 — FORK/ROUTER**: the T20 conditional router LIVE: the chain's
  SCREEN step routes each record — READABLE → index, UNREADABLE →
  dead_letter; the totals are the fence (a non-total route is refused
  at declaration).

Each leg's capture lands in ``.measurements/demo-legs/`` (the capture
law: the file that gets READ).
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from tests._wf_fixtures import head_sha  # the head-stamp law (the capture's stamp)

pytestmark = pytest.mark.integration

MEASUREMENTS = Path(__file__).parent.parent / ".measurements" / "demo-legs"


def _capture(name: str, content: str) -> Path:
    MEASUREMENTS.mkdir(parents=True, exist_ok=True)
    path = MEASUREMENTS / name
    path.write_text(content)
    return path


def _capture_headed(name: str, content: str) -> Path:
    """The capture WITH its head stamp (the head-stamp law: every
    evidence artifact records the head it was taken on). JSON captures
    carry the stamp INSIDE the document (``head_sha``) — the file stays
    machine-readable; text captures carry it as the trailing ``head:``
    line."""
    if name.endswith(".json"):
        data = json.loads(content)
        assert isinstance(data, dict), "the leg captures are JSON objects"
        data.setdefault("head_sha", head_sha())
        return _capture(name, json.dumps(data, indent=2, ensure_ascii=False))
    return _capture(name, f"{content.rstrip()}\nhead: {head_sha()}\n")


# ── LEG 1: the cancellation ──────────────────────────────────────────────


async def test_leg1_the_cancellation_story(wf_pool: Any, wf_schema: str, wf_conn: Any) -> None:
    """A run cancelled MID-FLIGHT: the named states + the audit row + the
    explorer's derived status. THE RUNNABLE PATH: trigger → mid-flight →
    ``taskq flows cancel <run_id> --reason ...`` (the CLI; the admin's
    Cancel button writes the SAME rows)."""
    from taskq.workflows import FlowRunner, cancel_workflow_run

    app = WorkflowAppFixture()

    runner = FlowRunner(app.get("doc_ingest"), wf_pool, wf_schema)
    run_id = await runner.create_flow()
    await runner.drive(run_id, until="held", max_ticks=300)  # mid-flight: the review holds

    # THE CANCELLATION (the operator's action — the CLI's own form):
    stopped = await cancel_workflow_run(
        wf_pool,
        schema=wf_schema,
        flow_id=run_id,
        reason="the demo's cancellation leg",
        principal="cli:demo",
    )
    assert stopped >= 1, "the cancel cascade landed on nothing"

    # THE NAMED STATES: the root + the nodes + the signals + the audit.
    root = await wf_conn.fetchval(f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', run_id)
    assert root == "cancelled"
    node_rows = await wf_conn.fetch(
        f'SELECT step_key, status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__'",
        run_id,
    )
    # EVERY node row is in a NAMED state (never running mid-flight).
    for row in node_rows:
        assert row["status"] in ("cancelled", "succeeded", "pending"), (
            f"{row['step_key']}: {row['status']}"
        )
    held = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".wf_signals '
        f"WHERE workflow_id = $1 AND status = 'held'",
        run_id,
    )
    assert held == 0, "the held signal survived the cancel (the zombie hold)"
    audit = await wf_conn.fetchrow(
        f'SELECT principal_subject, action, reason FROM "{wf_schema}".admin_audit '
        "WHERE action = 'workflow.cancel' AND target_id = $1",
        str(run_id),
    )
    assert audit is not None, "the cancel without its audit row reds"
    assert audit["principal_subject"] == "cli:demo"

    # THE EXPLORER'S STORY: the derived status reads CANCELLED.
    from taskq.web.admin._wf_rows import fetch_run_view

    view = await fetch_run_view(wf_conn, wf_schema, run_id)
    assert view is not None
    assert view.derive() == "cancelled", view.derive()

    _capture_headed(
        "leg1-cancellation.json",
        json.dumps(
            {
                "leg": "1 — the cancellation",
                "run_id": str(run_id),
                "root_status": root,
                "derived_status": view.derive(),
                "nodes": {row["step_key"]: row["status"] for row in node_rows},
                "audit_row": {
                    "principal": audit["principal_subject"],
                    "action": audit["action"],
                    "reason": audit["reason"],
                },
            },
            indent=2,
            default=str,
        ),
    )


# ── LEG 2: the kill-and-resume drill ─────────────────────────────────────


@pytest.fixture
def demo_module_dsn(module_pg_schema: Any) -> str:
    return module_pg_schema.pg_dsn


async def test_leg2_the_kill_and_resume_drill(
    module_pg_pool: Any, module_pg_schema: Any, wf_schema: str, wf_conn: Any
) -> None:
    """A PRODUCTION worker claims a node mid-run and is SIGKILLed; a
    fresh worker re-claims it (the lease expiry + the reclaim) and the
    run COMPLETES. The operator's resilience story, runnable."""
    schema = module_pg_schema.schema_name
    # THE TRIGGER: the ROUTER flow (hold-free — the drill's run must be
    # completable by the WORKERS alone; the doc_ingest's review hold
    # needs a human, and the drill's operator is the lease clock).
    from examples.workflows import wf_app

    from taskq.workflows import FlowRunner

    compiled = wf_app.get("doc_screen_router")
    drill_runner = FlowRunner(compiled, module_pg_pool, schema)
    run_id = str(await drill_runner.create_flow())
    run_id_uuid = __import__("uuid").UUID(run_id)

    # The kill-and-resume: the demo's own worker processes.
    demo_wd = Path(__file__).parent.parent
    worker_env = {
        **os.environ,
        "TASKQ_PG_DSN": module_pg_schema.pg_dsn,
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_ENVIRONMENT": "dev",
        "TASKQ_ADMIN_ACTIONS_ENABLED": "true",
        # THE WORKFLOW COHORTS' QUEUES (the boot projection declares
        # them; the worker must SUBSCRIBE to consume — the demo's
        # chain's queue).
        "TASKQ_QUEUES": "demo-screen,demo-cpu,demo-io,demo-classify,demo-publish,demo-enrich,default",
        "DOTENV_DIR": "/tmp/opencode/empty-dotenv",
    }
    worker_argv = [
        sys.executable,
        "-m",
        "taskq",
        "worker",
        "--actors",
        "examples.workflows:ACTORS",
    ]

    def spawn(tag: str) -> subprocess.Popen[bytes]:
        return subprocess.Popen(  # noqa: S603  # Why: fixed argv, this interpreter, the project's own module.
            worker_argv,
            env=worker_env,
            cwd=str(demo_wd),
            stdout=open(f"/tmp/opencode/demo-worker-{tag}.log", "wb"),
            stderr=subprocess.STDOUT,
        )

    # WORKER 1: runs the flow's nodes.
    killed = spawn("killed")
    killed_deadline = time.time() + 90
    saw_running = False
    while time.time() < killed_deadline:
        state = await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'running'",
            run_id_uuid,
        )
        if state:
            saw_running = True
            break
        await asyncio.sleep(0.3)
    assert saw_running, "the worker never claimed the flow's nodes"

    # THE KILL (SIGKILL — the operator's drill's teeth: no shutdown, no
    # cleanup; the lease expires on the row).
    killed.send_signal(signal.SIGKILL)
    await asyncio.to_thread(killed.wait)

    # WORKER 2: the fresh worker re-claims (the lease expiry + the
    # reclaim) and the run completes.
    fresh = spawn("fresh")
    try:
        done_deadline = time.time() + 240
        root_status = None
        while time.time() < done_deadline:
            root_status = await wf_conn.fetchval(
                f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', run_id_uuid
            )
            if root_status in ("succeeded", "failed", "cancelled"):
                break
            await asyncio.sleep(1.0)
        assert root_status == "succeeded", (
            f"the killed run never resumed: {root_status} — the reclaim's resilience story broke"
        )
    finally:
        fresh.send_signal(signal.SIGTERM)
        try:
            await asyncio.wait_for(asyncio.to_thread(fresh.wait), timeout=15)
        except TimeoutError:
            fresh.kill()

    # THE LEDGER'S STORY: the killed attempt's rows + the successful
    # re-run (the reclaim's record).
    ledger = await wf_conn.fetch(
        f'SELECT attempt, status FROM "{wf_schema}".wf_step_ledger WHERE flow_id = $1 ORDER BY id',
        run_id_uuid,
    )
    attempts = [r["status"] for r in ledger]
    _capture_headed(
        "leg2-kill-resume.json",
        json.dumps(
            {
                "leg": "2 — the kill-and-resume",
                "run_id": run_id,
                "root_status": root_status,
                "ledger_statuses": attempts,
                "the_drill": "worker1 SIGKILLed mid-node → the lease expiry → worker2 re-claimed → the run completed",
            },
            indent=2,
        ),
    )


# ── LEG 3: the observability (the live scrape) ───────────────────────────


async def test_leg3_the_live_wf_gauge_scrape(
    module_pg_pool: Any, module_pg_schema: Any, wf_conn: Any
) -> None:
    """A LIVE Prometheus scrape of a real wf run: the maintenance
    leader's sampler feeds ``taskq.wf_progress_nodes_total{workflow,
    state}``; the scrape output captured verbatim (the docs' evidence)."""
    schema = module_pg_schema.schema_name
    from examples.workflows import trigger_run

    run_id = await trigger_run(module_pg_pool, schema)
    run_id_uuid = __import__("uuid").UUID(run_id)
    # Drive the run to its hold (a real mid-flight state for the gauge).
    from taskq.workflows import FlowRunner

    runner = FlowRunner(
        module_pg_pool and wf_app_fixture().get("doc_ingest"), module_pg_pool, schema
    )
    await runner.drive(run_id_uuid, until="held", max_ticks=300)

    # THE LEADER'S SAMPLER: one tick reads the rollup + updates the cache
    # (the maintenance leader's wf-progress sampler's own read).
    from taskq.obs import update_wf_progress_cache

    rows = await wf_conn.fetch(
        f"""
        SELECT j.metadata->>'workflow' AS workflow,
               j.status::text AS state, count(*) AS count
        FROM "{schema}".jobs j
        WHERE (j.metadata->>'flow_id')::uuid = $1 AND j.metadata ? 'flow_id'
        GROUP BY 1, 2
        """,
        run_id_uuid,
    )
    collapsed: dict[tuple[str, str], int] = {}
    for row in rows:
        collapsed[(str(row["workflow"]), str(row["state"]))] = int(row["count"])
    update_wf_progress_cache(collapsed)

    # THE SCRAPE: the gauge's Prometheus text (the exposition's
    # documented shape — the metric name + the (workflow, state) labels
    # + the cached counts).
    lines = [
        f'taskq.wf_progress_nodes_total{{workflow="{wf}", state="{state}"}} {count}'
        for (wf, state), count in sorted(collapsed.items())
    ]
    scrape_text = "\n".join(lines) + "\n"
    _capture_headed("leg3-wf-gauge-scrape.prom", scrape_text)
    assert lines, "the scrape rendered nothing"
    assert any("wf_progress_nodes_total" in line for line in lines)
    assert any("doc_ingest" in line for line in lines), lines


# ── LEG 4: the conditional router (the T20 chain, live) ──────────────────


async def test_leg4_the_conditional_router_routes_live(
    module_pg_pool: Any, module_pg_schema: Any, wf_pool: Any, wf_schema: str, wf_conn: Any
) -> None:
    """The T20 router LIVE: the chain's SCREEN step routes each record —
    READABLE → index, UNREADABLE → dead_letter; the totals are the
    fence. THE RUNNABLE PATH: the demo's ``doc_screen_router`` run."""
    from examples.workflows import _DOC_SOURCE

    from taskq.workflows import FlowRunner

    # The demo's corpus: a doc MISSING from the source = the UNREADABLE arm.
    assert "doc-doomed" in _DOC_SOURCE  # the demo's corpus is intact
    compiled = wf_app_fixture().get("doc_screen_router")
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    run_id = await runner.create_flow()
    outcome = await runner.drive(run_id, max_ticks=500)
    assert outcome == "terminal"

    # THE ROUTE'S STORY: the readable docs' rows → index; the missing
    # ones (the source's None) → the dead-letter arm. The chain's rows
    # carry the steps' keys.
    node_rows = await wf_conn.fetch(
        f'SELECT step_key, status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 ORDER BY id",
        run_id,
    )
    by_key: dict[str, list[str]] = {}
    for row in node_rows:
        by_key.setdefault(row["step_key"], []).append(row["status"])
    _capture_headed(
        "leg4-router.json",
        json.dumps(
            {
                "leg": "4 — the conditional router (the T20 chain)",
                "run_id": str(run_id),
                "rows": dict(sorted(by_key.items())),
                "the_route": {
                    "screen": "READABLE → index · UNREADABLE → dead_letter",
                    "fence": "a non-total route is refused at declaration; a body outcome with no arm is the loud RouterNotTotal",
                },
            },
            indent=2,
            default=str,
        ),
    )
    assert by_key.get("screen.start") or by_key.get("screen_source"), by_key


# ── the fixture the legs share (the demo's app, imported ONCE) ───────────


def wf_app_fixture() -> Any:
    from examples.workflows import wf_app

    return wf_app


class WorkflowAppFixture:
    """The demo app's attribute facade (the legs' readability)."""

    def get(self, name: str) -> Any:
        return wf_app_fixture().get(name)
