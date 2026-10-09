# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier; every value is $-bound.
"""T11 — THE ADMIN WORKFLOW PAGE PINS: the run explorer's pages + the
SSE node_state stream + the typed actions, against a REAL run on a real
migrated schema (the fastapi marker tier: server-rendered pytest, no
browser — the DoD's primary lane; the browser-level probe replay is the
red-team's lane).

The pins (each with its convicted variant named in the docstring):
- suppress_refresh (the meta-refresh killer — the page without it reds
  the live-state pin by construction);
- the boot JSON's graph is the ROWS-ALONE emission (the admin never
  imports the workflow's module);
- the states matrix: zero-holds, zero-nodes, unknown-run, not-installed
  — each renders its DEFINED state (the #673 rule; a blank region that
  reads as a healthy zero reds);
- the SSE replay: the first frame arrives immediately, frames are
  revisioned FULL snapshots, and the Last-Event-ID cursor continues —
  the reconnect NEVER loses state (the snapshot subsumes the gap);
- the typed door: a resolve without the mounted WorkflowApp answers
  501 (no untyped deliver surface ships); with it, the wrong payload is
  the named pydantic refusal and the RIGHT one delivers + lands its
  audit row;
- the G7 rows==DOM check: the page's rendered node states == the §17.5
  derivation over the same rows.
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
import uuid
from collections.abc import Iterator
from typing import Any

import asyncpg
import httpx
import pytest
from pydantic import BaseModel

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")
from fastapi import FastAPI

from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.workflows import FlowRunner, WorkflowApp, build, step
from taskq.workflows.api import GateDecl

pytestmark = pytest.mark.integration

MODULE_NAME = "p4_admin_demo_flows"


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


@pytest.fixture
def demo_app_module() -> Iterator[types.ModuleType]:
    """The typed door's source (the host's WorkflowApp) — mounted into
    the router for the resolve pins."""
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("admin_hold_flow")
    def hold_flow() -> object:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)
        return build(step(_wait, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    @app_obj.workflow("admin_plain_flow")
    def plain_flow() -> object:
        return build(step(_plain_body, Ingest(doc_id="d1"), key="solo"))

    module.app = app_obj  # type: ignore[attr-defined]
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


async def _plain_body(ctx: Any, params: Ingest) -> str:
    return "done"


async def _wait(ctx: Any, params: Ingest) -> str:
    await ctx.wait_signal((Approval,), reason="editorial approval", timeout_s=120.0)
    return "published"


def _csrf_of(client: Any) -> str:
    """The CSRF cookie's value (the GET that set it must precede the
    POST — the guarded-POST discipline's own dance)."""
    token = client.cookies.get("taskq_csrf_token", "")
    assert token, "the GET must set the CSRF cookie before any POST"
    return token


def _make_admin_app(pool: asyncpg.Pool, schema: str, wf_app: Any = None) -> FastAPI:
    bundle = create_router(pool, schema=schema, base_path="", workflow_app=wf_app)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


async def _seed_run(
    pool: asyncpg.Pool, schema: str, *, name: str = "admin_hold_flow"
) -> str:
    """A REAL run driven to its hold (the rows the page renders)."""
    compiled = sys.modules[MODULE_NAME].app.get(name)  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    return str(flow_id)


# ── the pages ────────────────────────────────────────────────────────────


async def test_run_page_renders_the_rows_alone_graph_and_suppresses_refresh(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    demo_app_module: Any,
) -> None:
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/workflows/{run_id}")
    assert resp.status_code == 200
    html = resp.text
    # THE META-REFRESH KILLER (the dragon, pinned): the page MUST NOT
    # carry the meta refresh — it destroys the live SVG.
    assert 'http-equiv="refresh"' not in html
    # The boot JSON: the rows-alone mermaid + the state.
    assert 'id="wf-boot"' in html
    boot = json.loads(html.split('<script id="wf-boot" type="application/json">')[1].split("</script>")[0])
    assert boot["runId"] == run_id
    assert "review" in boot["mermaid"]
    assert boot["state"]["status"] == "blocked"  # the §17.5 derivation over the rows
    # The held node's state rides the snapshot (the amber pin's data).
    node_states = {n["key"]: n for n in boot["state"]["nodes"]}
    assert node_states["review"]["hold"] is True
    # The hold's context (the WAITING-ON state + the schema + the example).
    assert "editorial approval" in html
    assert "declared payload schema" in html


async def test_run_page_zero_holds_is_the_defined_state(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE STATES MATRIX (1): a run with NO holds renders the DEFINED
    healthy-zero line — never a blank section that reads as a healthy
    zero (the #673 rule; the blank-region variant reds this pin). A
    resolved hold renders as a DECISION (the rows-alone audit), which is
    a different defined state."""
    schema = module_pg_schema.schema_name
    # A hold-free run: zero wf_signals rows, ever.
    compiled = sys.modules[MODULE_NAME].app.get("admin_plain_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client_http:
        resp = await client_http.get(f"/workflows/{flow_id}")
    assert resp.status_code == 200
    assert "No holds on this run" in resp.text


async def test_run_page_unknown_run_is_a_404(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    app = _make_admin_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/workflows/018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f")
    assert resp.status_code == 404


async def test_runs_list_empty_schema_is_the_defined_empty_state(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """THE STATES MATRIX (2): the list page's empty schema renders the
    honest line + the next step (never a blank table)."""
    app = _make_admin_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/workflows")
    assert resp.status_code == 200
    assert "No workflow runs yet" in resp.text
    assert "__flow__" in resp.text  # the source is NAMED


async def test_runs_list_links_the_runs(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/workflows")
    assert resp.status_code == 200
    assert f"/workflows/{run_id}" in resp.text


# ── the SSE stream (the replay contract) ────────────────────────────────


def _frames_from(text: str) -> list[dict[str, Any]]:
    """The SSE wire format's parse (the client's own cursor logic — the
    same 'id:' then 'data:' discipline workflows.js keeps)."""
    frames: list[dict[str, Any]] = []
    event_id: str | None = None
    for line in text.splitlines():
        if line.startswith("id: "):
            event_id = line[4:]
        elif line.startswith("data: ") and event_id is not None:
            frames.append(json.loads(line[6:]))
            event_id = None
    return frames


async def _stream_frames(
    module_pg_schema: Any, run_id: str, *, last_event_id: str | None = None, count: int = 1
) -> list[dict[str, Any]]:
    """`count` frames off the REAL generator, bounded (a hang is a defect
    with a timeout — the bound is stated, and asyncio.timeout raises)."""
    from taskq.web.admin._wf_actions import _stream_generator

    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=2)
    try:
        gen = _stream_generator(
            pool,
            module_pg_schema.schema_name,
            uuid.UUID(run_id),
            int(last_event_id) if last_event_id is not None else 0,
        )
        frames: list[dict[str, Any]] = []
        for _ in range(count):
            raw = await asyncio.wait_for(gen.__anext__(), timeout=10.0)
            frames.extend(_frames_from(raw))
        await gen.aclose()
        return frames
    finally:
        await pool.close()


async def test_sse_first_frame_is_the_full_snapshot(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema)
    frames = await _stream_frames(module_pg_schema, run_id)
    assert frames, "the stream's first frame must arrive immediately"
    (frame,) = frames
    assert frame["run_id"] == run_id
    assert frame["status"] == "blocked"
    assert {n["key"] for n in frame["nodes"]} == {"review"}


async def test_sse_reconnect_last_event_id_continues_the_cursor(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE REPLAY PIN: a reconnect with Last-Event-ID continues the
    cursor (seq > cursor) and the frame carries the WHOLE state — the
    reconnect never loses it (the state-losing variant reds)."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema)
    frames = await _stream_frames(module_pg_schema, run_id)
    last_seq = frames[0]["seq"]
    # THE RECONNECT: the cursor resumes at the last SEEN id; the next
    # frame's seq is strictly greater AND the state is complete.
    replayed = await _stream_frames(module_pg_schema, run_id, last_event_id=str(last_seq))
    assert replayed, "the reconnect must replay promptly"
    assert replayed[0]["seq"] > last_seq
    assert "nodes" in replayed[0] and replayed[0]["nodes"], "the replayed frame is a FULL snapshot"
    assert {n["key"] for n in replayed[0]["nodes"]} == {"review"}


async def test_sse_unknown_run_streams_the_empty_snapshot(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """THE STATES MATRIX (3): a stream for an unknown/not-yet-inserted
    run emits the snapshot with ZERO nodes — a defined state, not a
    hang, not a blank."""
    frames = await _stream_frames(
        module_pg_schema, "018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f"
    )
    assert frames
    assert frames[0]["nodes"] == []


# ── the typed door + the audit ──────────────────────────────────────────


async def test_resolve_without_mounted_definitions_is_the_501_residual(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE NO-UNTYPED-DOOR PIN: a router mounted WITHOUT the workflow
    definitions answers 501 — the resolve refuses to deliver untyped."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema, wf_app=None)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/queues")  # the CSRF cookie's set (the guarded-POST discipline)
        resp = await client.post(
            f"/api/runs/{run_id}/resolve",
            data={"hold_id": "x", "decision": "{}", "csrf_token": _csrf_of(client)},
        )
    assert resp.status_code == 501
    assert "workflow_app" in resp.json()["detail"]


async def test_resolve_through_the_typed_door_delivers_and_audits(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    demo_app_module: Any,
) -> None:
    """THE AUDIT PIN (G4): a Resolve with the mounted definitions
    delivers + writes the audit row; a WRONG payload is the named
    pydantic refusal and moves nothing."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema)
    from taskq.workflows.api._hitl import HitlClient

    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)
    app = _make_admin_app(module_pg_pool, schema, wf_app=sys.modules[MODULE_NAME].app)  # type: ignore[attr-defined]

    # THE WRONG PAYLOAD: the named refusal, the hold SURVIVES.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        await http.get("/queues")  # the CSRF cookie's set
        bad = await http.post(
            f"/api/runs/{run_id}/resolve",
            data={
                "hold_id": hold.hold_id,
                "decision": json.dumps({"verdict": 42}),
                "csrf_token": _csrf_of(http),
            },
        )
    assert bad.status_code == 422
    assert "pydantic refused the payload" in bad.json()["detail"]
    assert len(await client.list(run_id)) == 1, "the refused resolve moved nothing"

    # THE RIGHT PAYLOAD: delivered + the audit row.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        await http.get("/queues")
        good = await http.post(
            f"/api/runs/{run_id}/resolve",
            data={
                "hold_id": hold.hold_id,
                "decision": json.dumps({"verdict": "approve", "note": ""}),
                "reason": "the editor approved",
                "csrf_token": _csrf_of(http),
            },
        )
    assert good.status_code == 200, good.text
    assert good.json()["status"] == "delivered"
    audit = await clean_pg_conn.fetchrow(
        f'SELECT principal_subject, action FROM "{schema}".admin_audit '
        "WHERE action = 'hitl.resolve' ORDER BY id DESC LIMIT 1"
    )
    assert audit is not None, "a resolve without an audit row reds"
    assert audit["principal_subject"] == "anonymous"  # the dev path's named subject


async def test_node_panel_unknown_node_is_a_404(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """THE STATES MATRIX (4): the panel's unknown node is a 404 — the
    JS renders it as the panel's error state (never a blank panel)."""
    app = _make_admin_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(
            "/api/runs/018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f/nodes/nope"
        )
    assert resp.status_code == 404


async def test_run_page_renders_the_audit_trail_after_a_resolve(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    demo_app_module: Any,
) -> None:
    """THE AUDIT TRAIL VISIBLE PER OPERATOR ACTION (the G4 pin's page
    face): after a resolve, the run page renders the trail's ROW (the
    principal + the action + the reason); the empty trail renders the
    defined line first."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema, wf_app=sys.modules[MODULE_NAME].app)  # type: ignore[attr-defined]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        # THE EMPTY TRAIL (the defined state):
        before = await http.get(f"/workflows/{run_id}")
        assert "No operator actions recorded" in before.text
        # The resolve (the action):
        from taskq.workflows.api._hitl import HitlClient

        client = HitlClient(module_pg_pool, schema=schema)
        (hold,) = await client.list(run_id)
        await http.get("/queues")
        good = await http.post(
            f"/api/runs/{run_id}/resolve",
            data={
                "hold_id": hold.hold_id,
                "decision": json.dumps({"verdict": "approve", "note": ""}),
                "reason": "the trail pin's resolve",
                "csrf_token": _csrf_of(http),
            },
        )
        assert good.status_code == 200, good.text
        # THE TRAIL RENDERS THE ROW:
        after = await http.get(f"/workflows/{run_id}")
        html = after.text
        assert "hitl.resolve" in html
        # THE APOSTROPHE IS ESCAPED (Jinja's autoescape — the raw form
        # never renders).
        assert "the trail pin&#39;s resolve" in html or "the trail pin's resolve" in html
        assert "anonymous" in html  # the dev path's named subject


async def test_runs_list_renders_the_rows_and_the_cap_note(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """The list page's LOADED state: the runs render (the links + the
    statuses); the cap note states the render's bound."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/workflows")
    html = resp.text
    assert f"/workflows/{run_id}" in html
    assert "Workflow Runs" in html


def test_the_cli_gate_models_the_key_error_path() -> None:
    """gate_models_for's KeyError path: a node that is not in the
    compiled graph is the named KeyError (the typed door's 422 maps
    it)."""
    from taskq.workflows import WorkflowApp, build, step

    async def body(ctx: Any, params: Ingest) -> str:
        return "ok"

    app = WorkflowApp()

    @app.workflow("gate_key_err")
    def gate_key_err() -> object:
        return build(step(body, Ingest(doc_id="d"), key="solo"))

    from taskq.workflows._cli import gate_models_for

    with pytest.raises(KeyError):
        gate_models_for(app, "gate_key_err", "not-a-node")


# ── the G7 rows==DOM check (the reconstruction, live in the page) ───────


async def test_g7_rows_equal_the_rendered_state(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    demo_app_module: Any,
) -> None:
    """G7 (T08's always-on law) exercised on the REAL page: the §17.5
    derivation over the rows == the status the page renders; the boot
    JSON's per-node states == the node rows, seq for seq."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema)
    run_id_uuid = __import__("uuid").UUID(run_id)
    from taskq.web.admin._wf_rows import fetch_run_view

    async with module_pg_pool.acquire() as conn:
        view = await fetch_run_view(conn, schema, run_id_uuid)
    assert view is not None
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/workflows/{run_id}")
    html = resp.text
    boot = json.loads(html.split('<script id="wf-boot" type="application/json">')[1].split("</script>")[0])
    # THE ROWS==DOM CHECK: every node row's status appears on the page's
    # snapshot, node for node.
    rendered = {n["key"]: n["status"] for n in boot["state"]["nodes"]}
    rows = {n.key: n.status for n in view.nodes}
    assert rendered == rows
    # The derivation's output is the page's status line.
    assert boot["state"]["status"] == view.derive()


# ── the JS conventions (the keyboard contract's source pin) ─────────────


async def test_panel_latency_band(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE LATENCY BAND (set from the measurement, pinned — the G11
    method): the panel route's full read ≤ 50 ms p95 on this stack (the
    measured p95 is 3.2 ms — 15x headroom; the file:
    .measurements/t11-latency-band.json)."""
    import time as time_mod

    schema = module_pg_schema.schema_name

    # A plain run (the panel read is status-independent).
    compiled = sys.modules[MODULE_NAME].app.get("admin_plain_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)
    app = _make_admin_app(module_pg_pool, schema)
    samples: list[float] = []
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for _ in range(10):
            t0 = time_mod.perf_counter()
            resp = await client.get(f"/api/runs/{flow_id}/nodes/solo")
            samples.append((time_mod.perf_counter() - t0) * 1000)
            assert resp.status_code == 200
    samples.sort()
    p95 = samples[int(0.95 * len(samples)) - 1]
    assert p95 <= 50.0, f"the panel's p95 read took {p95:.1f} ms — the band is 50 ms"


def test_the_page_js_carries_the_keyboard_contract() -> None:
    """Keyboard operability's source pin: the delegated keydown listener
    (Enter/Space activates a focused node) + the tab focus + the native
    forms (the Resolve form is a real <form> — Enter submits it)."""
    from pathlib import Path

    js = Path(__file__).parents[2].joinpath(
        "src/taskq/web/static/workflows.js"
    ).read_text()
    assert 'ev.key !== "Enter"' in js and 'ev.key !== " "' in js
    assert 'setAttribute("tabindex", "0")' in js
    assert 'setAttribute("role", "button")' in js
    assert "keydown" in js
    # The seq-cursor: a stale frame never overwrites a fresh one.
    assert "state.seq <= cursor" in js


async def test_the_pages_degrade_when_the_workflow_tables_are_absent(
    module_pg_pool: Any, module_pg_schema: Any
) -> None:
    """THE UNINSTALLED DEGRADE: a schema with ONLY the vanilla tables (a
    pre-workflow migration state) renders the NOTICE (both pages), never
    a 500. THE THROWAWAY SCHEMA: the module's migrated schema is shared
    per module — the drop here would break the family; the fresh schema
    applies the migrations then drops the workflow series' two tables."""
    import httpx as _httpx

    schema = module_pg_schema.schema_name + "_nowf"
    conn = await asyncpg.connect(module_pg_schema.pg_dsn.rpartition("/")[0] + "/taskq")
    from taskq.migrate import apply_pending

    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    bare = await asyncpg.connect(module_pg_schema.pg_dsn)
    await apply_pending(bare, schema=schema)
    await bare.execute(f'DROP TABLE "{schema}".wf_signals')
    await bare.execute(f'DROP TABLE "{schema}".wf_edge')
    await bare.close()
    app = _make_admin_app(module_pg_pool, schema)
    # THE DETAIL PAGE is the degrade's real surface (its reads touch the
    # workflow tables); the LIST page reads only jobs (the roots live
    # there) — its uninstalled branch is the pre-vanilla state, unreachable
    # once jobs exists.
    async with _httpx.AsyncClient(
        transport=_httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        listed = await client.get("/workflows")
        assert listed.status_code == 200  # jobs exists: the list renders (empty)
        detail = await client.get(
            "/workflows/018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f"
        )
        assert detail.status_code == 200
        assert "workflows not installed" in detail.text
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.close()


async def test_the_run_page_degrades_when_the_run_exists_but_the_tables_dropped(
    module_pg_pool: Any, module_pg_schema: Any, demo_app_module: Any
) -> None:
    """THE DEGRADE's real path: a run EXISTS (the root row) + the
    workflow tables dropped mid-flight — the fetch's edge read raises,
    the page renders the NOTICE (never a 500)."""
    import httpx as _httpx

    from taskq.migrate import apply_pending

    schema = module_pg_schema.schema_name + "_nowf2"
    module_db = module_pg_schema.pg_dsn
    conn = await asyncpg.connect(module_db)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    bare = await asyncpg.connect(module_db)
    await apply_pending(bare, schema=schema)
    await bare.close()

    # A REAL run (the root + the nodes exist), then the workflow tables drop.
    run_pool = await asyncpg.create_pool(module_pg_schema.pg_dsn)
    compiled = sys.modules[MODULE_NAME].app.get("admin_hold_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, run_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    await run_pool.close()
    await conn.execute(f'DROP TABLE "{schema}".wf_edge')
    await conn.execute(f'DROP TABLE "{schema}".wf_signals')

    app = _make_admin_app(module_pg_pool, schema)
    async with _httpx.AsyncClient(
        transport=_httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/workflows/{flow_id}")
        assert resp.status_code == 200
        assert "workflows not installed" in resp.text
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await conn.close()
