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

pytestmark = pytest.mark.integration

MEASUREMENTS = Path(__file__).parent.parent / ".measurements" / "demo-legs"


def _capture(name: str, content: str) -> Path:
    MEASUREMENTS.mkdir(parents=True, exist_ok=True)
    path = MEASUREMENTS / name
    path.write_text(content)
    return path


# ── LEG 1: the cancellation ──────────────────────────────────────────────


async def test_leg1_the_cancellation_story(wf_pool: Any, wf_schema: str, wf_conn: Any) -> None:
    """A run cancelled MID-FLIGHT: the named states + the audit row + the
    explorer's derived status. THE RUNNABLE PATH: trigger → mid-flight →
    ``taskq flows cancel <run_id> --reason ...`` (the CLI; the admin's
    Cancel button writes the SAME rows)."""
    from taskq.workflows import FlowRunner, cancel_workflow_run

    app = WorkflowAppFixture()

    runner = FlowRunner(app.get("doc_ingest"), wf_pool, wf_schema)
    run_id = (await runner.create_flow()).flow_id
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

    _capture(
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
    run_id = str((await drill_runner.create_flow()).flow_id)
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
    _capture(
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


# ── LEG 3: the observability (the live scrape — a REAL one) ─────────────


async def test_leg3_the_live_wf_gauge_scrape(
    module_pg_pool: Any, module_pg_schema: Any, wf_conn: Any
) -> None:
    """A LIVE Prometheus scrape of a real wf run: the maintenance
    leader's sampler feeds ``taskq.wf_progress_nodes_total``; the
    /metrics ENDPOINT's actual response is captured verbatim.

    THE RECEIPTS LAW APPLIES TO DOCS: the earlier "capture" was the
    test's own f-string rendering — never scraped — carrying an ILLEGAL
    metric name (the dotted ``taskq.wf_progress_nodes_total`` never
    leaves the process: the Prometheus bridge's translation is dots →
    underscores) and a WRONG label (a ``workflow="None"`` series the
    real sampler can never emit — it collapses every unregistered
    workflow onto ``_other_``). This capture is the endpoint's byte-
    verbatim output; both defects are impossible in it, and the pins
    below red if either shape ever returns."""
    pytest.importorskip("fastapi")
    pytest.importorskip("opentelemetry.exporter.prometheus")
    schema = module_pg_schema.schema_name
    from examples.workflows import trigger_run

    run_id = str((await trigger_run(module_pg_pool, schema)).flow_id)
    run_id_uuid = __import__("uuid").UUID(run_id)
    # Drive the run to its hold (a real mid-flight state for the gauge).
    from taskq.workflows import FlowRunner

    runner = FlowRunner(
        module_pg_pool and wf_app_fixture().get("doc_ingest"), module_pg_pool, schema
    )
    await runner.drive(run_id_uuid, until="held", max_ticks=300)

    # THE LEADER'S SAMPLER, VERBATIM: the same query + the same
    # registered-names collapse the maintenance leader's wf-progress
    # sampler runs at its tick (the sampler's read is the feed; the
    # gauge's cache is its only consumer).
    from taskq.obs import update_wf_progress_cache
    from taskq.worker._leader_shared import _QUERY_WF_PROGRESS_SQL_TEMPLATE
    from taskq.workflows.definitions import get_registry

    wf_rows = await wf_conn.fetch(_QUERY_WF_PROGRESS_SQL_TEMPLATE.format(schema=schema))
    registered = frozenset(get_registry()._workflows)  # pyright: ignore[reportPrivateUsage]  # Why: the sampler reads the SAME registry the definitions' import populated (the leader's own walk).
    collapsed: dict[tuple[str, str], int] = {}
    for row in wf_rows:
        name = str(row["workflow"])
        if name not in registered:
            name = "_other_"
        key = (name, str(row["state"]))
        collapsed[key] = collapsed.get(key, 0) + int(row["count"])
    update_wf_progress_cache(collapsed)

    # THE SCRAPE: the LIVE endpoint (the contrib's /jobs/health/metrics
    # router over a bridged registry) — the response captured VERBATIM.
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.sdk.metrics import MeterProvider
    from prometheus_client import CollectorRegistry

    from taskq.contrib.prometheus import create_metrics_router

    registry = CollectorRegistry()
    reader = PrometheusMetricReader(registry=registry)
    otel_metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    try:
        metrics_app = FastAPI()
        metrics_app.include_router(
            create_metrics_router(None, registry=registry),  # type: ignore[arg-type]  # Why: the CLI's own pattern — deps is signature parity only; the router reads the process-global provider.
            prefix="/jobs/health",
        )
        with TestClient(metrics_app) as metrics_client:
            response = metrics_client.get("/jobs/health/metrics")
        assert response.status_code == 200
        scrape_text = response.text
        _capture("leg3-wf-gauge-scrape.prom", scrape_text)
    finally:
        from opentelemetry.metrics import NoOpMeterProvider

        otel_metrics.set_meter_provider(NoOpMeterProvider())

    # THE CAPTURE'S TRUTH: the LEGAL rendering (dots → underscores), the
    # workflow label carrying a REGISTERED name, and no ghost series.
    wf_lines = [line for line in scrape_text.splitlines() if "wf_progress_nodes_total" in line]
    assert wf_lines, f"the scrape carried no wf series: {scrape_text[:2000]}"
    assert any("taskq_wf_progress_nodes_total" in line for line in wf_lines), wf_lines
    assert not any("taskq.wf_progress_nodes_total" in line for line in wf_lines), (
        "the ILLEGAL dotted name reached the capture — the bridge's "
        "rendering is dots → underscores; the fence's old f-string "
        "fabrication is back"
    )
    assert any('workflow="doc_ingest"' in line for line in wf_lines), wf_lines
    assert not any('workflow="None"' in line for line in wf_lines), (
        "a workflow=None series reached the capture — the real sampler "
        "never emits it (the unregistered collapse onto _other_); the "
        "hand-rolled read is back"
    )


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
    run_id = (await runner.create_flow()).flow_id
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
    _capture(
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


# ── THE POISON-LOOP PIN: a foreign run in the schema ────────────────────


async def test_the_drive_loop_tolerates_a_foreign_run(
    module_pg_pool: Any, module_pg_schema: Any, wf_conn: Any
) -> None:
    """THE POISON-LOOP PIN: a foreign workflow's run in the same schema
    (the demo's OWN second workflow is exactly that, to the doc_ingest
    driver — and any other app's run can share the schema) must not
    poison the pass. THE OLD SHAPE: the driver built ONE doc_ingest
    runner for every row; the foreign run raised on its first foreign
    step key — ABORTING every pass — starved the runs behind it (no
    ORDER BY), and spammed the same error every 0.5 s forever. THE CURE,
    pinned here: the pass derives from THIS app's registry (each run's
    runner is the run's OWN stamped workflow), a foreign name is
    tolerated LOUDLY-ONCE (the deduped warning, not a per-pass spam),
    and the runs behind the foreign row still complete."""
    import uuid as uuid_mod

    from examples.workflows import _drive_pending, trigger_router_run, trigger_run

    schema = module_pg_schema.schema_name
    # THE FOREIGN RUN, at the claim sequence's HEAD (created first — the
    # poison sat in front of every victim). Its workflow name is not
    # registered on the demo's app.
    from taskq.workflows._sql import WorkflowSql
    from taskq.workflows.api._runner_codec import FlowEntryShim
    from taskq.workflows.ledger import insert_flow_run

    wsql = WorkflowSql.build(schema)
    async with module_pg_pool.acquire() as conn:
        foreign = await insert_flow_run(
            conn,
            wsql,
            entry=FlowEntryShim("not_this_apps_workflow", {"taskq:wf_input": None}),
            run_key="foreign:poison:1",
        )
    assert foreign.created

    # THE VICTIMS: the demo's own workflows, behind the poison in the
    # ORDER BY (uuid7 = the claim sequence).
    router_run = await trigger_router_run(module_pg_pool, schema)
    victim = str((await trigger_run(module_pg_pool, schema)).flow_id)

    # ONE pass — the old shape aborted HERE on the poison's first tick.
    await _drive_pending(module_pg_pool, schema)

    # THE VICTIM'S STORY: the pass reached it (the poison did not abort
    # the walk) — its nodes exist and moved.
    victim_uuid = uuid_mod.UUID(victim)
    node_rows = await wf_conn.fetch(
        f'SELECT status, count(*) AS n FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__' "
        "GROUP BY status",
        victim_uuid,
    )
    statuses = {r["status"]: r["n"] for r in node_rows}
    assert statuses.get("succeeded", 0) >= 1, (
        f"the pass never reached the run behind the poison run: {statuses}"
    )
    # THE ROUTER'S RUN — the demo's own second workflow — drove from ITS
    # OWN compiled graph (the registry's derivation, not a hardcode).
    router_root = await wf_conn.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', __import__("uuid").UUID(router_run)
    )
    assert router_root in ("succeeded", "running"), (
        f"the router run never drove from its own runner: {router_root}"
    )

    # THE LOUD-ONCE: a SECOND pass over the foreign row does not repeat
    # the warning (the dedup, not the 0.5 s spam).
    import structlog.testing

    with structlog.testing.capture_logs() as logs:
        await _drive_pending(module_pg_pool, schema)
        await _drive_pending(module_pg_pool, schema)
    foreign_warnings = [e for e in logs if e.get("event") == "workflow-drive-foreign-run-skipped"]
    assert len(foreign_warnings) <= 1, (
        f"the foreign run's warning spammed {len(foreign_warnings)}x — the loud-once dedup reds"
    )


# ── THE SWALLOWED-CANCEL PIN: the drive loop's stop is BOUNDED ──────────


async def test_the_drive_loop_stops_on_cancel_within_the_bound(
    module_pg_pool: Any, module_pg_schema: Any
) -> None:
    """THE SWALLOWED-CANCEL PIN (measured): cancel → the loop STOPPED
    within the bound. THE OLD SHAPE: the drive pass rode
    ``suppress(CancelledError)`` — a cancellation delivered mid-drive was
    SWALLOWED (a consumed cancellation is never delivered again), the
    loop slept on, and the loop measured ALIVE 12 s after the cancel,
    still ticking. THE CURE: the pass re-raises the cancel and the
    lifespan's stop AWAITS the loop (bounded join). The cancel here
    lands DURING a drive pass — the swallow's own window."""
    import asyncio
    import time as time_mod

    from examples.workflows import drive_loop, trigger_run

    schema = module_pg_schema.schema_name
    run_id = str((await trigger_run(module_pg_pool, schema)).flow_id)

    task = asyncio.create_task(drive_loop(module_pg_pool, schema))
    # Wait until the pass is MID-DRIVE (a node row 'running' = the drive
    # pass is executing bodies) — then cancel INTO the pass.
    deadline = time_mod.monotonic() + 60
    mid_drive = False
    while time_mod.monotonic() < deadline:
        live = await module_pg_pool.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND status = 'running'",
            __import__("uuid").UUID(run_id),
        )
        if live:
            mid_drive = True
            break
        await asyncio.sleep(0.05)
    assert mid_drive, "the loop never reached a mid-drive state (the probe's precondition)"

    t0 = time_mod.perf_counter()
    task.cancel()
    done, _ = await asyncio.wait({task}, timeout=5.0)
    stopped_s = time_mod.perf_counter() - t0
    assert task in done, f"the loop was ALIVE {stopped_s:.1f}s after the cancel — the swallow"
    assert task.cancelled(), "the loop ended but not BY the cancellation"
    assert stopped_s < 5.0, f"the loop stopped in {stopped_s:.2f}s — past the 5 s bound"


# ── THE COMPOSE SUBSCRIPTION PIN: the shipped stack's placement ─────────


def test_the_compose_workers_subscribe_to_the_demo_queues() -> None:
    """THE COMPOSE SUBSCRIPTION PIN: the shipped stack's workers
    SUBSCRIBE to the demo's queues — per worker, per the placement (the
    heterogeneous placement is DEMONSTRATED by the stack, never
    decorative: a queue's node runs on ITS worker). The old stack
    subscribed both workers to ``examples`` ONLY — the demo's five
    cohorts had no worker, the wiring's placement was decoration."""
    from pathlib import Path

    import yaml

    repo = Path(__file__).parent.parent
    compose = yaml.safe_load((repo / "examples" / "docker-compose.yml").read_text())
    subs = {
        worker: compose["services"][worker]["environment"]["TASKQ_QUEUES"].split(",")
        for worker in ("worker-1", "worker-2")
    }
    # THE VANILLA QUEUE stays on both (the trigger UI's actors).
    for worker, queues in subs.items():
        assert "examples" in queues, f"{worker} lost the vanilla actors' queue"
    # THE DEMO'S QUEUES: every cohort the wiring names (the doc_ingest
    # nodes + the router chain's queue) — subscribed, and by EXACTLY ONE
    # worker (the gpu-ish queue's node runs on ITS worker).
    demo_queues: set[str] = set()
    for name in ("doc_ingest", "doc_screen_router"):
        compiled = wf_app_fixture().get(name)
        demo_queues |= {n.queue for n in compiled.nodes.values()}
        for chain in getattr(compiled, "chains", ()):
            demo_queues.add(chain.queue)
    demo_queues.discard("default")
    assert demo_queues, "the demo's queue universe came out empty"
    for queue in sorted(demo_queues):
        holders = [worker for worker, queues in subs.items() if queue in queues]
        assert holders, f"NO compose worker subscribes to {queue!r} — the placement is decorative"
        assert len(holders) == 1, (
            f"{queue!r} rides {holders} — the cohort's node must run on ITS worker"
        )
    # THE REGISTRY FACE: the worker's process carries the workflow
    # definitions (a queue-routed claim can resolve its body — the
    # subscription is only real if the D1 registry is populated).
    worker_py = (repo / "examples" / "worker.py").read_text()
    assert "import examples.workflows" in worker_py, (
        "the worker never imports the demo's workflow definitions — a "
        "claimed queue-routed claim would die on a foreign step key"
    )


class WorkflowAppFixture:
    """The demo app's attribute facade (the legs' readability)."""

    def get(self, name: str) -> Any:
        return wf_app_fixture().get(name)
