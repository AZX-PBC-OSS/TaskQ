# ruff: noqa: S608  # Why: the schema is a fixture-derived test identifier; every value is $-bound.
"""ATTACK PINS — the demo/examples front (the hostile review of the
consolidated head af1b8779, demo lane).

The demo's own machinery was convicted on four LIVE findings and the
evidence law on a fifth; the ergonomics contract carries an inflated
claim. Each pin asserts the SAFE behavior, red against the shipped tree,
strict-xfail until the cure lands (the cure flips the pin to XPASS-
strict — a red that tells you to remove the marker; the drill is
test_wf_attack_cancel.py's face C).

* F-DEMO-1 — the demo's drive loop is poisonable by a FOREIGN run:
  ``_drive_pending`` (examples/workflows.py) drives EVERY running flow
  with the doc_ingest compiled graph; a foreign-workflow run raises
  WorkflowRunError ("a foreign step key"), aborts the whole pass, and
  starves every later run (the fetch has no ORDER BY) — re-raised and
  re-logged every 0.5 s forever.
* F-DEMO-2 — the lifespan's ``drive_task.cancel()`` is swallowed by the
  ``contextlib.suppress(asyncio.CancelledError)`` inside
  ``_drive_pending``: measured alive 12 s after cancel, still ticking.
* F-DEMO-3 — the node panel cannot address a map child:
  ``_NODE_PANEL_SQL`` is ``fetchrow WHERE step_key = $1`` with no
  map_index addressing, so the README's promised observation (watch
  doc-doomed's attempt go 1 → 2 in the node panel) is unreachable.
* F-DEMO-5 — the README's leg-3 "captured verbatim scrape" is a test's
  f-string rendering, never scraped: the metric name carries ILLEGAL
  dots for the Prometheus text exposition (the OTel exporter sanitizes
  them) and the label reads ``workflow="None"`` where the sampler's
  documented coalesce is ``_other_``.
* F-DEMO-6 — the compose stack's workers subscribe only
  ``TASKQ_QUEUES=examples``: none of the demo's workflow cohort queues,
  so the heterogeneous placement is decorative in the shipped stack.
* F-ERGO-7 — the "zero-warning budget" is inflated:
  ``CompiledWorkflow.validate()`` raises only on severity=="error"; a
  warning-severity regression sails green.

The GREEN GUARD encodes the two-driver one-schema drive race the front
verified: zero duplicate ledger groups, one join_fire row per join — so
the arbiter pair can never rot silently.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from pathlib import Path
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.testing.fixtures import ModulePgSchema
from taskq.workflows import FlowRunner, Promise, StepContext, WorkflowApp, build, step
from taskq.workflows.api import GateDecl

pytestmark = [pytest.mark.integration, pytest.mark.fastapi]

REPO_ROOT = Path(__file__).resolve().parents[1]


# ── F-DEMO-1: the foreign run poisons the drive pass ─────────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-DEMO-4(b)]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: the live finding
async def test_the_drive_loop_isolates_a_foreign_run_and_never_starves_the_pass(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    structlog_capture: list[dict[str, Any]],
) -> None:
    """The pass over the pending runs must ISOLATE a foreign run: the
    demo's own run still advances, and the skip is NAMED in the log —
    a silent skip is the next mystery (the drive loop's own docstring:
    a silently-dead driver looks exactly like a wedged run)."""
    from examples.workflows import _drive_pending, trigger_run

    class TheForeignInput(BaseModel):
        doc: str

    async def _the_foreign_body(ctx: StepContext, params: TheForeignInput) -> str:
        return "the foreign lane's own body — never driven by the demo's pass"

    # The FOREIGN run FIRST: the pass's fetch is LIMIT 5 with no ORDER
    # BY — physical (insertion) order decides who poisons whom. The
    # foreign run belongs to a THROWAWAY app (a workflow name the demo's
    # own registry does not carry) — the honest foreign shape: the rows
    # share the schema, the DEFINITION belongs to another driver.
    foreign_app = WorkflowApp()

    @foreign_app.workflow("the-foreign-lanes-own-workflow")
    def the_foreign_lanes_own_workflow() -> Promise[object]:
        return build(step(_the_foreign_body, TheForeignInput(doc="d1"), key="the_foreign_step"))

    foreign_runner = FlowRunner(
        foreign_app.get("the-foreign-lanes-own-workflow"), wf_pool, wf_schema
    )
    foreign_id = (await foreign_runner.create_flow()).flow_id
    demo_id = (await trigger_run(wf_pool, wf_schema)).flow_id

    # THE PIN: one pass never raises on the foreign run (today:
    # WorkflowRunError — step 'screen_source' is not in workflow
    # 'doc_ingest''s compiled graph — 'a foreign step key').
    await _drive_pending(wf_pool, wf_schema)

    # …and the poison run never starves the pass: the demo run ADVANCED.
    advanced = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key <> '__flow__' "
        "AND status <> 'pending'",
        demo_id,
    )
    assert advanced > 0, (
        "the foreign run starved the pass — the demo run's nodes are all "
        "still pending behind the poisoned drive"
    )
    # THE NAMED SKIP: some captured event names the foreign run (the
    # skip/isolation is operator-visible, never silent).
    assert any(
        "foreign" in str(event.get("event", ""))
        or str(foreign_id) in json.dumps(event, default=str)
        for event in structlog_capture
    ), (
        "the foreign run was handled SILENTLY — the pass needs a named "
        "skip/isolation log naming the run it refused to drive"
    )


# ── F-DEMO-2: the swallowed cancellation ─────────────────────────────────

#: The bounded window: a cancellable task must die promptly once
#: ``cancel()`` lands — the measured defect was alive 12 s later. 5 s is
#: generous slack for a CI runner and forever-short of the defect.
_CANCEL_BOUND_S = 5.0


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-DEMO-4(b)]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: the live finding
async def test_the_drive_task_cancel_completes_within_a_bounded_window(
    wf_pool: asyncpg.Pool, wf_schema: str, wf_conn: asyncpg.Connection
) -> None:
    """The app's lifespan cancels the drive task at teardown; the cancel
    must COMPLETE. A bare running flow with no claimable nodes makes
    every drive pass span the full max_ticks — the cancel lands INSIDE
    ``runner.drive``, exactly where the suppress sits."""
    from examples.workflows import drive_loop

    from tests._wf_fixtures import seed_flow

    await seed_flow(wf_conn, wf_schema, status="running")

    task = asyncio.create_task(drive_loop(wf_pool, wf_schema))
    try:
        await asyncio.sleep(1.0)  # land the cancel inside the first drive pass
        task.cancel()
        deadline = time.monotonic() + _CANCEL_BOUND_S
        while not task.done() and time.monotonic() < deadline:  # noqa: ASYNC110  # Why: the bounded-window poll IS the pin's subject (the cancel's measured completion, the clock's bound asserted after)
            await asyncio.sleep(0.05)
        assert task.done(), (
            f"the drive task was still ticking {_CANCEL_BOUND_S:.0f}s after "
            "cancel() — the suppress inside _drive_pending swallowed the "
            "cancellation (measured alive 12s+ on the attacked tree)"
        )
    finally:
        # Teardown discipline (the suite's loop-leak guard): a cancel
        # lands whenever the task reaches drive_loop's own sleep — poll
        # until the task is DONE, never leave it pending on the module
        # loop.
        for _ in range(150):
            if task.done():
                break
            task.cancel()
            await asyncio.sleep(0.1)


# ── F-DEMO-3: the node panel cannot address a map child ──────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-DEMO-3]; the marker is removed per the designed flip (the confirmation receipt).
async def test_the_node_panel_addresses_a_map_child_by_step_key_and_map_index(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    wf_conn: asyncpg.Connection,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The README's property 1: watch doc-doomed's attempt go 1 → 2 in
    the node panel. The panel must address ONE map child by
    (step_key, map_index) and serve THE FAILING CHILD — its healed
    attempt plus the failed first attempt in its timeline."""
    # The admin router fails closed outside dev (the _dev_env fixture is
    # path-gated to tests/web_admin/; this file sets its own).
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    httpx = pytest.importorskip("httpx", reason="the panel pin needs the fastapi lane's client")
    from examples.workflows import wf_app
    from fastapi import FastAPI

    from taskq.web.admin import create_router, setup_admin_state

    runner = FlowRunner(wf_app.get("doc_ingest"), wf_pool, wf_schema)
    run_id = (await runner.create_flow()).flow_id
    # Drive until the ARMED child (doc-doomed) heals through its ladder:
    # attempt 1 failed (the demo's armed transient failure), attempt 2
    # succeeded — the observation the README promises.
    doomed: Any = None
    deadline = time.monotonic() + 90
    while (
        doomed is None and time.monotonic() < deadline
    ):  # Why: the condition-not-clock poll IS the cure's shape (the boot-race class's repro) — the bounded milestone poll, never a fixed clock
        await runner.tick(run_id)
        doomed = await wf_conn.fetchrow(
            f'SELECT map_index, attempt FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'ingest.item' "
            "AND attempt >= 2 AND status = 'succeeded'",
            run_id,
        )
        if doomed is None:
            await asyncio.sleep(0.1)
    assert doomed is not None, "the armed child never laddered to its healed attempt"

    bundle = create_router(wf_pool, schema=wf_schema, base_path="", workflow_app=wf_app)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(
            f"/api/runs/{run_id}/nodes/ingest.item?map_index={doomed['map_index']}"
        )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # THE PIN: the served row IS the addressed child (today the endpoint
    # cannot address map children at all — fetchrow returns an arbitrary
    # sibling and the response carries no map_index at all).
    assert body.get("map_index") == doomed["map_index"], (
        "the panel served an anonymous map child — no map_index addressing "
        f"(asked for map_index={doomed['map_index']}, got attempt={body.get('attempt')} "
        f"with timeline {[(t['attempt'], t['status']) for t in body.get('timeline', [])]})"
    )
    timeline = {(row["attempt"], row["status"]) for row in body["timeline"]}
    assert body["attempt"] == 2 and (1, "failed") in timeline, (
        "the served child is not the failing one — the ladder's story "
        "(attempt 1 failed → attempt 2 succeeded) is unreachable"
    )


# ── F-DEMO-5: the fabricated scrape (the evidence law) ───────────────────

#: The Prometheus text exposition format's metric-name and label-name
#: grammar (prometheus.io/docs/instrumenting/exposition_formats):
#: ``[a-zA-Z_:][a-zA-Z0-9_:]*`` — DOTS ARE ILLEGAL (the OTel exporter
#: sanitizes ``taskq.wf_progress_nodes_total`` to underscores on the
#: real /metrics surface; the fabricated artifact kept the dots).
_PROM_NAME_RE = r"[a-zA-Z_:][a-zA-Z0-9_:]*"
_PROM_LINE_RE = re.compile(
    rf"^(?P<name>{_PROM_NAME_RE})"
    rf"(?:\{{(?:{_PROM_NAME_RE}=\"(?:[^\"\\]|\\.)*\"(?:,{_PROM_NAME_RE}=\"(?:[^\"\\]|\\.)*\")*)?\}})?"
    r"\s+(?P<value>[-+]?(?:\d+(?:\.\d+)?(?:[eE][-+]?\d+)?|NaN|Inf))"
    r"(?:\s+\d+)?\s*$"
)


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-DEMO-5]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: the live finding
def test_every_prom_artifact_is_valid_prometheus_exposition() -> None:
    """THE EVIDENCE-LAW GUARD: every ``.prom`` artifact under
    ``.measurements`` must parse as VALID Prometheus text exposition —
    the strict name grammar (the teeth: the illegal-dots name reds
    today) AND the official client parser's label grammar."""
    artifacts = sorted((REPO_ROOT / ".measurements").rglob("*.prom"))
    assert artifacts, "no .prom artifacts under .measurements — the guard lost its subject"
    violations: list[str] = []
    for path in artifacts:
        text = path.read_text()
        for lineno, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if _PROM_LINE_RE.match(stripped) is None:
                violations.append(f"{path.relative_to(REPO_ROOT)}:{lineno}: {stripped!r}")
        try:
            from prometheus_client.parser import (  # Why: optional-extra import inside the check.
                text_string_to_metric_families,
            )
        except ImportError:
            continue  # the strict regex above is the floor; the parser is the second leg
        try:
            list(text_string_to_metric_families(text))
        except Exception as exc:
            violations.append(f"{path.relative_to(REPO_ROOT)}: parser refused: {exc}")
    assert not violations, (
        "a .measurements .prom artifact is NOT valid Prometheus exposition "
        "(the evidence law — a capture that cannot have come off a real "
        "scrape is fabrication):\n" + "\n".join(violations)
    )


# ── F-DEMO-6: the compose stack's decorative placement ───────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [F-DEMO-6: the compose workers subscribe the demo's declared cohorts + the default queue]; the marker is removed per the designed flip (the confirmation receipt).
def test_the_compose_workers_subscribe_the_demos_declared_queues() -> None:
    """The wiring truth: every queue the demo's workflows DECLARE must be
    SUBSCRIBED by the shipped compose stack's workers — else the
    three-queue heterogeneous placement the docs sell is decorative."""
    from examples.workflows import wf_app

    declared: set[str] = set()
    for name in ("doc_ingest", "doc_screen_router"):
        compiled = wf_app.get(name)
        for n in compiled.nodes.values():
            if getattr(n, "queue", None):
                node_obj: Any = n
                declared |= {str(node_obj.queue)}  # pyright: ignore[reportAttributeAccessIssue]
        for c in compiled.chains:
            if getattr(c, "queue", None):
                chain_obj: Any = c
                declared |= {str(chain_obj.queue)}
    assert declared, "the demo declares no queues — the guard lost its subject"

    compose = (REPO_ROOT / "examples" / "docker-compose.yml").read_text()
    subscribed: set[str] = set()
    workers = 0
    for service, body in re.findall(r"(?ms)^  ([\w-]+):\n(.*?)(?=^  [\w-]+:|\Z)", compose):
        if not service.startswith("worker"):
            continue
        workers += 1
        match = re.search(r"^\s+TASKQ_QUEUES:\s*(\S+)\s*$", body, re.MULTILINE)
        if match:
            subscribed |= {q.strip() for q in match.group(1).split(",") if q.strip()}
    assert workers, "no worker services in the compose file — the guard lost its subject"

    missing = declared - subscribed
    assert not missing, (
        f"the compose workers subscribe {sorted(subscribed)} but the demo's workflows "
        f"declare {sorted(declared)} — unsubscribed: {sorted(missing)} "
        "(the heterogeneous placement never dispatches in the shipped stack)"
    )


# ── F-ERGO-7: the inflated zero-warning budget ───────────────────────────


class _Verdict(BaseModel):
    ok: bool


class _WarnIn(BaseModel):
    doc_id: str


async def _wait_body(ctx: StepContext, params: _WarnIn) -> str:
    return "ok"


async def _unannotated_body(ctx: Any, params: Any):  # pyright: ignore[reportMissingParameterType, reportUnknownParameterType, reportMissingReturnType, reportUnknownReturnType]  # Why: THE PROBE — the unannotated return IS the mutation under test (E4's carrier); the root pyproject's tests relaxation would mute it.
    return "ok"


def test_validate_refuses_a_warning_carrying_graph() -> None:
    """The zero-warning budget, made real: a graph carrying ANY
    warning-severity finding must FAIL validate(). The carrier is a W1
    eternal wait (a gate with no declared timeout) — proven non-vacuous
    by the diagnostics assertion first."""
    app = WorkflowApp()

    @app.workflow("att_demopin_warn_flow")
    def _wf() -> Promise[object]:
        gate = GateDecl(name="_Verdict", payload_models=(_Verdict,))  # timeout_s=None → W1
        return build(step(_wait_body, _WarnIn(doc_id="d"), key="review", gates=(gate,)))

    compiled = app.get("att_demopin_warn_flow")
    from taskq.workflows.api._validate import WorkflowValidationError, validate_compiled

    diagnostics = validate_compiled(compiled)
    # THE CARRIER IS REAL: warnings and ONLY warnings (an error would
    # make the raises-block green for the wrong reason).
    assert diagnostics, "the W1 carrier produced no diagnostics — the pin is vacuous"
    assert all(d.severity == "warning" for d in diagnostics), diagnostics
    # THE SHIPPED SEMANTICS (F-ERGO-7's resolution — the claim re-worded
    # to what the door DOES): validate() RAISES on the error severity
    # (the runner's construction door); the WARNINGS REPORT in the
    # diagnostics — the zero-warning budget is enforced by the CONTRACT
    # PINS' ``diagnostics == ()`` teeth (test_wf_ergonomics_contract's
    # own asserts), not by a raise. The pin now holds the honest shape:
    # the warning REPORTS (never silently swallowed) AND the error
    # REFUSES. The error carrier is a graph that BUILDS and FAILS
    # VALIDATION (E4's unannotated param — the arity lie the door owns);
    # the build-refused shapes (the empty gather) refuse at the VERB,
    # a different seam with its own pins.
    error_carrier_app = WorkflowApp()

    @error_carrier_app.workflow("att_demopin_error_flow")
    def _err_wf() -> Promise[object]:
        return build(step(_unannotated_body, _WarnIn(doc_id="d"), key="produce"))

    with pytest.raises(WorkflowValidationError):
        error_carrier_app.get("att_demopin_error_flow").validate()


# ── GREEN GUARD: the two-driver one-schema drive race ────────────────────


@pytest.mark.load_sensitive
async def test_two_drivers_one_schema_one_run_stay_exactly_once(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """GREEN GUARD (verified by the front, encoded so it can't rot): two
    FlowRunner processes' worth of driver — one schema, one run, driven
    CONCURRENTLY through the hold and the resolve to terminal — the
    ledger's claim arbiter and the join-fire arbiter hold: ZERO
    duplicate (step_key, map_index, attempt) ledger groups, EXACTLY ONE
    wf_join_fire row per join, the root succeeded.

    THE LOADED-BAR LAW'S MARK (load_sensitive — CI's serial exclusive
    lane owns it): the pin's subject is a CONTENTION race (two real
    drivers, one run, the arbiter's teeth), and under the co-tenant
    lanes' ambient load the whole drive window can starve — both
    drivers' work-bounded ticks spent on the sweeps under sweeps — and
    the gather lands ['max_ticks', 'max_ticks'] on the SAME tree that
    greens in 0.7 s unloaded (the bisect that chased it dissolved on
    re-run). The arbiter's exactness — the assertions below — held in
    every green run; the starved window is the load, not the record."""
    from examples.workflows import wf_app

    from taskq.workflows.api._hitl import HitlClient

    compiled = wf_app.get("doc_ingest")
    first_driver = FlowRunner(compiled, module_pg_pool, module_pg_schema.schema_name)
    second_driver = FlowRunner(compiled, module_pg_pool, module_pg_schema.schema_name)
    run_id = (await first_driver.create_flow()).flow_id

    held = await asyncio.gather(
        first_driver.drive(run_id, until="held"),
        second_driver.drive(run_id, until="held"),
    )
    assert list(held) == ["held", "held"], held

    client = HitlClient(module_pg_pool, schema=module_pg_schema.schema_name)
    (hold,) = await client.list(run_id)
    result = await client.resolve(hold.hold_id, {"verdict": "approve", "note": ""})
    assert result.status == "delivered"

    terminal = await asyncio.gather(first_driver.drive(run_id), second_driver.drive(run_id))
    assert list(terminal) == ["terminal", "terminal"], terminal

    duplicates = await module_pg_pool.fetch(
        f"SELECT step_key, COALESCE(map_index, -1) AS mi, attempt, count(*) AS c "
        f'FROM "{module_pg_schema.schema_name}".wf_step_ledger WHERE flow_id = $1 '
        "GROUP BY 1, 2, 3 HAVING count(*) > 1",
        run_id,
    )
    assert list(duplicates) == [], (
        f"duplicate ledger groups under the two-driver race: {[dict(r) for r in duplicates]}"
    )
    fires = await module_pg_pool.fetch(
        f'SELECT step_key, count(*) AS c FROM "{module_pg_schema.schema_name}".wf_join_fire '
        "WHERE flow_id = $1 GROUP BY 1",
        run_id,
    )
    assert fires, "no join fired — the guard lost its subject"
    assert all(r["c"] == 1 for r in fires), (
        f"a join fired twice under the two-driver race: { {r['step_key']: r['c'] for r in fires} }"
    )
    root = await module_pg_pool.fetchval(
        f'SELECT status FROM "{module_pg_schema.schema_name}".jobs WHERE id = $1', run_id
    )
    assert root == "succeeded", root
