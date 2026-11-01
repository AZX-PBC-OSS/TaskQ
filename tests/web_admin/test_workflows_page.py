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
from taskq.workflows import FlowRunner, Promise, WorkflowApp, build, map_source, step
from taskq.workflows.api import GateDecl

pytestmark = pytest.mark.integration

MODULE_NAME = "p4_admin_demo_flows"


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


class Secret(BaseModel):
    token: str = ""


async def _secret_body(ctx: Any, params: Secret) -> str:
    return "ok"


async def _map_source_body(ctx: Any, params: Ingest) -> list[str]:
    return ["doc-a", "doc-b", "doc-doomed"]


async def _map_item_body(ctx: Any, doc_id: str) -> str:
    # THE DOOMED CHILD (the README's observation): armed on the FIRST
    # attempt only — its attempt walks 1 → 2 while the siblings never
    # re-run.
    if doc_id == "doc-doomed" and ctx.attempt < 2:
        raise RuntimeError("the armed transient failure")
    return f"done:{doc_id}"


async def _ok_body(ctx: Any, params: Ingest) -> str:
    return "src-ok"


async def _doomed_mid(ctx: Any, params: Any) -> str:
    raise RuntimeError("mid's permanent doom")


async def _tail_body(ctx: Any, params: Any) -> str:
    return "tail-ok"


async def _map_item_permanently_doomed(ctx: Any, doc_id: str) -> str:
    if doc_id == "doc-doomed":
        raise RuntimeError("the doomed child's permanent failure")
    return f"done:{doc_id}"


async def _doomed_solo(ctx: Any, params: Ingest) -> str:
    raise RuntimeError("the doomed solo's permanent failure")


@pytest.fixture
def demo_app_module() -> Iterator[types.ModuleType]:
    """The typed door's source (the host's WorkflowApp) — mounted into
    the router for the resolve pins."""
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("admin_hold_flow")
    def hold_flow() -> Promise[object]:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)
        return build(step(_wait, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    @app_obj.workflow("admin_plain_flow")
    def plain_flow() -> Promise[object]:
        return build(step(_plain_body, Ingest(doc_id="d1"), key="solo"))

    @app_obj.workflow("admin_map_flow")
    def map_flow() -> Promise[object]:
        src = step(_map_source_body, Ingest(doc_id="d1"), key="ingest")
        items = map_source(src, _map_item_body, max_attempts=3)
        return build(items)

    # THE RERUN PINS' FLOWS (the rerun affordances' real rows):
    @app_obj.workflow("admin_doomed_flow")
    def doomed_flow() -> Promise[object]:
        src = step(_ok_body, Ingest(doc_id="d1"), key="src")
        mid = step(_doomed_mid, src, key="mid")
        return build(step(_tail_body, mid, key="tail"))

    @app_obj.workflow("admin_map_doomed_flow")
    def map_doomed_flow() -> Promise[object]:
        src = step(_map_source_body, Ingest(doc_id="d1"), key="ingest")
        items = map_source(src, _map_item_permanently_doomed, max_attempts=2)
        return build(items)

    @app_obj.workflow("admin_secret_flow")
    def secret_flow() -> Promise[object]:
        return build(step(_secret_body, Secret(token="sk-canary-1234567890"), key="with-secret"))

    @app_obj.workflow("admin_nocapture_flow", capture="none")
    def nocapture_flow() -> Promise[object]:
        return build(step(_doomed_solo, Ingest(doc_id="d1"), key="solo-doomed"))

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


async def _seed_run(pool: asyncpg.Pool, schema: str, *, name: str = "admin_hold_flow") -> str:
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
    boot = json.loads(
        html.split('<script id="wf-boot" type="application/json">')[1].split("</script>")[0]
    )
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


async def test_sse_unknown_run_is_a_404_at_subscribe(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """THE STATES MATRIX (3), Q7's cure superseding the empty-snapshot
    pin: a stream for an UNKNOWN run id is a 404 AT SUBSCRIBE — the
    estate's own ``/sse/{topic}`` convention (an unknown topic is
    refused at subscribe; the empty-snapshot-forever stream was an
    existence oracle behind the auth gate and a poll task per probe).
    The defined empty state stays for a run that EXISTS with no node
    rows yet (the generator's not-started arm)."""
    app = _make_admin_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/api/runs/018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f/stream")
    assert resp.status_code == 404


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
    from taskq.workflows import HitlClient

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
        resp = await client.get("/api/runs/018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f/nodes/nope")
    assert resp.status_code == 404


async def test_node_panel_addresses_map_children_by_step_and_map_index(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE MAP-INDEX ADDRESSING PIN (the README's observation made
    reachable): a map's children share one step key (``ingest.item``) —
    the panel addresses a CHILD by (step, ``map_index``). The demo's
    signature observation — ``watch doc-doomed's attempt go 1 → 2`` —
    is (step, map_index)-addressable, not step-key-arbitrary: the bare
    read lands on the FIRST child in map order + carries the children
    census, and ``?map_index=N`` reads THAT child."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("admin_map_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    outcome = await runner.drive(flow_id)
    assert outcome == "terminal"
    root = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', flow_id
    )
    assert root == "succeeded", root

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        # THE BARE READ: the FIRST child in map order + the children
        # census (one row per child, in map order — the click-through).
        bare = await client.get(f"/api/runs/{flow_id}/nodes/ingest.item")
        assert bare.status_code == 200
        body = bare.json()
        assert body["map_index"] == 0
        assert [c["map_index"] for c in body["children"]] == [0, 1, 2]

        # THE DOOMED CHILD, BY (step, map_index): its attempt walked
        # 1 → 2 (the ladder re-ran IT alone), and the ledger carries the
        # two attempts; a SIBLING's attempt stayed 1 — the reads are
        # per-child, never an arbitrary row of the shared step key.
        doomed = await client.get(f"/api/runs/{flow_id}/nodes/ingest.item?map_index=2")
        assert doomed.status_code == 200
        doomed_body = doomed.json()
        assert doomed_body["map_index"] == 2
        assert doomed_body["attempt"] == 2, doomed_body
        assert len(doomed_body["timeline"]) == 2, doomed_body["timeline"]

        sibling = await client.get(f"/api/runs/{flow_id}/nodes/ingest.item?map_index=0")
        sibling_body = sibling.json()
        assert sibling_body["attempt"] == 1, sibling_body

        # AN OUT-OF-RANGE child is the named 404, never a neighbor's row.
        missing = await client.get(f"/api/runs/{flow_id}/nodes/ingest.item?map_index=9")
        assert missing.status_code == 404
        negative = await client.get(f"/api/runs/{flow_id}/nodes/ingest.item?map_index=-1")
        assert negative.status_code == 400


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
        from taskq.workflows import HitlClient

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
    def gate_key_err() -> Promise[object]:
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
    boot = json.loads(
        html.split('<script id="wf-boot" type="application/json">')[1].split("</script>")[0]
    )
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

    js = Path(__file__).parents[2].joinpath("src/taskq/web/static/workflows.js").read_text()
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
    # The DDL is SCHEMA-scoped — the throwaway schema lives in the
    # module's OWN database (the old "/taskq" hardcode assumed the
    # lane's own container's default db; the cluster's admin db is
    # wherever the DSN points, the fixture owns that lifecycle).
    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
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
        detail = await client.get("/workflows/018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f")
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


# ── the rerun affordances (the node rerun + the run rerun) ──────────────


async def _seed_doomed_run(pool: asyncpg.Pool, schema: str) -> str:
    """A REAL failed run (the chain src → mid(doomed) → tail): mid burns
    its ladder and terminal-fails, tail blocks on it, the run fails."""
    compiled = sys.modules[MODULE_NAME].app.get("admin_doomed_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)
    return str(flow_id)


async def _node_row(pool: asyncpg.Pool, schema: str, run_id: str, step_key: str) -> dict[str, Any]:
    row = await pool.fetchrow(
        f"SELECT id, status, attempt, metadata->>'blocking_reason' AS blocked FROM \"{schema}\".jobs "
        f"WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = $2",
        uuid.UUID(run_id),
        step_key,
    )
    assert row is not None, f"the node row {step_key!r} must exist"
    return dict(row)


async def test_node_retry_route_reopens_a_failed_node_and_audits(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    demo_app_module: Any,
) -> None:
    """THE NODE RERUN PIN: the POST route rides the ENGINE'S OWN
    manual-resume verb (``retry_workflow_node``) — the failed node
    re-pends (the CAS), the blocked downstream clears (the closure's
    reset), the terminal-FAILED flow re-opens, and the verb's
    same-transaction audit row lands (G4: the resolve/cancel routes'
    own discipline). The attempt ORDINAL CONTINUES — the reopen buys
    the next claim one more attempt, it never resets the ladder."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_doomed_run(module_pg_pool, schema)
    # THE SEEDED CONTRACT: the run failed, mid terminal-failed, tail blocked.
    root_before = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', uuid.UUID(run_id)
    )
    assert root_before == "failed", root_before
    mid_before = await _node_row(module_pg_pool, schema, run_id, "mid")
    assert mid_before["status"] == "failed"
    tail_before = await _node_row(module_pg_pool, schema, run_id, "tail")
    assert tail_before["blocked"] == "failed_parent", tail_before

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")  # the CSRF cookie's set
        r = await http.post(
            f"/api/runs/{run_id}/nodes/mid/retry",
            data={"csrf_token": _csrf_of(http), "reason": "the rerun pin"},
        )
        assert r.status_code == 303, r.text
        assert r.headers["location"] == f"/workflows/{run_id}"
        page = await http.get(r.headers["location"])
        assert page.status_code == 200
        # THE TRAIL RENDERS THE ROW (the verb's same-tx audit):
        assert "workflow.retry_node" in page.text
        assert "the rerun pin" in page.text
    mid_after = await _node_row(module_pg_pool, schema, run_id, "mid")
    assert mid_after["status"] == "pending", (
        f"CONTRACT: the CAS re-arms the terminal-FAILED node, got {mid_after}"
    )
    tail_after = await _node_row(module_pg_pool, schema, run_id, "tail")
    assert tail_after["blocked"] == "join", (
        f"CONTRACT: the blocked closure re-opens (failed_parent → join), got {tail_after}"
    )
    root_after = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', uuid.UUID(run_id)
    )
    assert root_after == "running", "the terminal-FAILED flow re-opens"
    audit = await clean_pg_conn.fetchrow(
        f'SELECT action, principal_subject, reason, target_id, detail FROM "{schema}".admin_audit '
        "WHERE action = 'workflow.retry_node' ORDER BY id DESC LIMIT 1"
    )
    assert audit is not None, "a rerun without an audit row reds (G4)"
    assert audit["principal_subject"] == "anonymous"  # the dev path's named subject
    assert audit["reason"] == "the rerun pin"
    assert audit["target_id"] == f"{run_id}:mid"
    detail = audit["detail"]
    if isinstance(detail, str):  # asyncpg hands jsonb back as str without a codec
        detail = json.loads(detail)
    assert detail.get("run_id") == run_id, "the row keys the run's own trail"


async def test_node_retry_refusals_land_the_named_banner(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE REFUSED RERUN IS A DEFINED STATE (the cancel refusal's own
    contract): a live/unknown node (the CAS granted nothing) redirects
    with ``?error=rerun-not-applied``; a CANCELLED run (the verb's
    second-decision refusal) with ``?error=rerun-refused`` — each lands
    the run page 200 WITH its banner, never a 400 wall."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_doomed_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        # A SUCCEEDED node: the CAS grants nothing.
        r = await http.post(
            f"/api/runs/{run_id}/nodes/src/retry", data={"csrf_token": _csrf_of(http)}
        )
        assert r.status_code == 303
        assert "error=rerun-not-applied" in r.headers["location"]
        page = await http.get(r.headers["location"])
        assert page.status_code == 200
        assert "Rerun not applied" in page.text
        # AN UNKNOWN node key: the same defined refusal.
        r = await http.post(
            f"/api/runs/{run_id}/nodes/not-a-node/retry", data={"csrf_token": _csrf_of(http)}
        )
        assert r.status_code == 303
        assert "error=rerun-not-applied" in r.headers["location"]

    # THE CANCELLED RUN: the engine's deliberate-closure refusal.
    held_run = await _seed_run(module_pg_pool, schema)
    from taskq.backend._protocol import JobId
    from taskq.workflows import cancel_workflow_run

    await cancel_workflow_run(module_pg_pool, schema=schema, flow_id=JobId(held_run))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        r = await http.post(
            f"/api/runs/{held_run}/nodes/review/retry", data={"csrf_token": _csrf_of(http)}
        )
        assert r.status_code == 303
        assert "error=rerun-refused" in r.headers["location"]
        page = await http.get(r.headers["location"])
        assert page.status_code == 200
        assert "Rerun refused" in page.text


async def test_rerun_routes_are_csrf_guarded_and_actions_gated(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE GUARDS' PIN (the resolve/cancel routes' own discipline): both
    rerun routes refuse a POST without the CSRF token (403), a POST
    with a mismatched token (403), and every POST on a deployment that
    disabled actions (403, the config message — declared BEFORE the
    token check)."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_doomed_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    for post_url in (
        f"/api/runs/{run_id}/nodes/mid/retry",
        f"/api/runs/{run_id}/rerun",
    ):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
        ) as http:
            await http.get("/queues")
            bare = await http.post(post_url)
            assert bare.status_code == 403, f"{post_url} without a token must refuse"
            forged = await http.post(post_url, data={"csrf_token": "not-the-token"})
            assert forged.status_code == 403, f"{post_url} with a wrong token must refuse"
        # THE ACTIONS KNOB (fail-closed): the disabled deployment refuses.
        disabled_app = _make_admin_app(module_pg_pool, schema)
        disabled_app.state.settings.admin_actions_enabled = False
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=disabled_app),
            base_url="http://test",
            follow_redirects=False,
        ) as http:
            await http.get("/queues")
            refused = await http.post(
                post_url, data={"csrf_token": _csrf_of(http), "reason": "gated"}
            )
            assert refused.status_code == 403
            assert "TASKQ_ADMIN_ACTIONS_ENABLED" in refused.json()["detail"]


async def test_run_rerun_route_rearms_the_failed_nodes(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    demo_app_module: Any,
) -> None:
    """THE RUN RERUN PIN: the POST route composes the ENGINE'S OWN
    per-node resume verb over the run's terminal-FAILED step keys (the
    rows' census) — the failed node re-pends, the blocked downstream
    clears, the flow re-opens, the audit rows land. THE SCOPE, HONEST:
    the engine has NO run-level re-enqueue verb (a cancelled run or a
    fresh input is a NEW run) — the route invents none.

    THE MAP CENSUS ARM: a collapsed map child is not a row on the page,
    but the route reads EVERY failed row — the doomed child's step key
    re-arms (the CAS flips every failed row of the key) while the
    succeeded siblings stay untouched."""
    schema = module_pg_schema.schema_name
    # THE CHAIN ARM: exactly one failed step key ('mid').
    run_id = await _seed_doomed_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        r = await http.post(
            f"/api/runs/{run_id}/rerun",
            data={"csrf_token": _csrf_of(http), "reason": "the run rerun pin"},
        )
        assert r.status_code == 303, r.text
        assert r.headers["location"] == f"/workflows/{run_id}"
    mid_after = await _node_row(module_pg_pool, schema, run_id, "mid")
    assert mid_after["status"] == "pending", mid_after
    tail_after = await _node_row(module_pg_pool, schema, run_id, "tail")
    assert tail_after["blocked"] == "join", tail_after
    src_after = await _node_row(module_pg_pool, schema, run_id, "src")
    assert src_after["status"] == "succeeded", "a succeeded node is never re-armed"
    root_after = await module_pg_pool.fetchval(
        f'SELECT status FROM "{schema}".jobs WHERE id = $1', uuid.UUID(run_id)
    )
    assert root_after == "running"
    audit = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".admin_audit '
        f"WHERE action = 'workflow.retry_node' AND target_id = $1",
        f"{run_id}:mid",
    )
    assert audit and int(audit) == 1, "the verb's own audited row, one per failed key"

    # THE MAP ARM: the failed child re-arms THROUGH its step key; the
    # siblings (succeeded) and the source stay put.
    compiled = sys.modules[MODULE_NAME].app.get("admin_map_doomed_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    map_run = str((await runner.create_flow()).flow_id)
    await runner.drive(map_run)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        r = await http.post(f"/api/runs/{map_run}/rerun", data={"csrf_token": _csrf_of(http)})
        assert r.status_code == 303
    doomed = await module_pg_pool.fetchrow(
        f"SELECT status FROM \"{schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 "
        "AND step_key = 'ingest.item' AND map_index = 2",
        uuid.UUID(map_run),
    )
    assert doomed is not None and doomed["status"] == "pending", (
        "the collapsed child's step key re-arms through the run rerun"
    )
    sibling = await module_pg_pool.fetchrow(
        f"SELECT status FROM \"{schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 "
        "AND step_key = 'ingest.item' AND map_index = 0",
        uuid.UUID(map_run),
    )
    assert sibling is not None and sibling["status"] == "succeeded"


async def test_run_rerun_refuses_a_run_with_nothing_resumable(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE NOTHING-TO-DO ARM: a SUCCEEDED run has no failed node — the
    route refuses with the not-applied banner (the defined state), and
    moves nothing."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("admin_plain_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        r = await http.post(f"/api/runs/{flow_id}/rerun", data={"csrf_token": _csrf_of(http)})
        assert r.status_code == 303
        assert "error=rerun-not-applied" in r.headers["location"]
        page = await http.get(r.headers["location"])
        assert page.status_code == 200
        assert "Rerun not applied" in page.text


async def test_run_rerun_unknown_run_is_a_404(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """The run rerun's unknown id is a 404 (the stream's subscribe law's
    shape — no banner for a run that does not exist)."""
    app = _make_admin_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        r = await http.post(
            "/api/runs/018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f/rerun",
            data={"csrf_token": _csrf_of(http)},
        )
    assert r.status_code == 404


# ── the graph view (the boot's shape + the node table) ──────────────────


async def test_the_run_graph_boot_carries_every_node_and_edge(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE GRAPH VIEW PIN: the boot JSON's Mermaid text carries EVERY
    rendered node line and EVERY edge of the run's collapsed projection
    (the rows-alone emission), the page's server-rendered node table
    carries every rendered node key (the graph's second renderer — the
    degrade text's promise), and the legend declares the held + skipped
    fills (the statuses the stream paints)."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_doomed_run(module_pg_pool, schema)
    from taskq.web.admin._wf_rows import fetch_run_view

    async with module_pg_pool.acquire() as conn:
        view = await fetch_run_view(conn, schema, uuid.UUID(run_id))
    assert view is not None
    rendered = [n for n in view.nodes if not n.map_child]

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/workflows/{run_id}")
    assert resp.status_code == 200
    html = resp.text
    boot = json.loads(
        html.split('<script id="wf-boot" type="application/json">')[1].split("</script>")[0]
    )
    # THE BASE_PATH LAW (the boot carries the mounted prefix; the stream
    # URL is server-built absolute-with-base-path).
    assert boot["basePath"] == ""
    assert boot["streamUrl"] == f"/api/runs/{run_id}/stream"
    mermaid = boot["mermaid"]
    for node in rendered:
        assert f'"{node.key}"' in mermaid, f"node {node.key!r} missing from the boot mermaid"
        assert f'data-wf-node-row="{node.key}"' in html, (
            f"node {node.key!r} missing from the server-rendered node table"
        )
    for node in rendered:
        for parent in node.parents:
            assert f"{parent} --> {node.key}" in mermaid, (
                f"the edge {parent!r} → {node.key!r} missing from the boot mermaid"
            )
    # THE LEGEND's fills (the closed vocabulary the stream paints —
    # held + skipped joined, then cancelled/crashed/abandoned/scheduled).
    assert "wf-st-held-fill" in html
    assert "wf-st-skipped-fill" in html
    assert "wf-st-scheduled-fill" in html
    assert "wf-st-cancelled-fill" in html
    assert "wf-st-crashed-fill" in html
    assert "wf-st-abandoned-fill" in html


async def test_the_rerun_forms_render_on_the_failed_rows(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE RERUN AFFORDANCE'S PAGE FACE: the failed node's row carries
    the retry form (native POST, the CSRF field, the confirm dialog),
    the succeeded rows carry none, and the run card carries the RUN
    rerun form — all riding {{ base_path }}."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_doomed_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        html = (await client.get(f"/workflows/{run_id}")).text
    assert 'data-wf-form="rerun-node"' in html
    assert f'action="/api/runs/{run_id}/nodes/mid/retry"' in html
    assert f'action="/api/runs/{run_id}/rerun"' in html
    assert 'data-wf-form="rerun-run"' in html
    assert "data-confirm=" in html  # the confirm dialog's declaration
    # The succeeded row carries NO rerun form (the affordance is the
    # failed rows'; the CAS would refuse the rest).
    src_row = html.split('data-wf-node-row="src"')[1].split("</tr>")[0]
    assert "rerun-node" not in src_row
    # The actions-disabled deployment renders no rerun affordance at all.
    disabled_app = _make_admin_app(module_pg_pool, schema)
    disabled_app.state.settings.admin_actions_enabled = False
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=disabled_app), base_url="http://test"
    ) as client:
        disabled_html = (await client.get(f"/workflows/{run_id}")).text
    assert "rerun-node" not in disabled_html
    assert "rerun-run" not in disabled_html


# ── the node I/O panel (the wired input + the decoded output) ───────────


async def test_the_node_panel_exposes_the_wired_input_and_the_result(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE NODE I/O PIN: the panel's read carries the node's INPUT (the
    wired args off the row's payload — the entry input, the wiring's
    data args, the map child's item) and the OUTPUT (the decoded result
    envelope, unwrapped the read-side display's own way)."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("admin_map_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = str((await runner.create_flow()).flow_id)
    await runner.drive(flow_id)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        src = await client.get(f"/api/runs/{flow_id}/nodes/ingest")
        assert src.status_code == 200
        src_io = src.json()["io"]
        assert src_io["input"]["wf_args"] == [{"doc_id": "d1"}], src_io["input"]
        assert "doc-a" in src_io["result"]["text"], src_io["result"]
        assert src_io["capture"] is None
        assert src_io["capture_suppressed"] is False

        child = await client.get(f"/api/runs/{flow_id}/nodes/ingest.item?map_index=0")
        assert child.status_code == 200
        child_io = child.json()["io"]
        assert child_io["input"]["wf_item"] == "doc-a", child_io["input"]
        assert "done:doc-a" in child_io["result"]["text"], child_io["result"]


async def test_the_node_panel_masks_the_wired_input(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE REDACT LAW AT THE DISPLAY (the hold-context discipline's own
    pipeline): the wired args pass the engine's chain → token-head pass
    BEFORE the panel serves them — a canary credential in the wiring
    reads masked, never raw."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("admin_secret_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = str((await runner.create_flow()).flow_id)
    await runner.drive(flow_id)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/api/runs/{flow_id}/nodes/with-secret")
    assert resp.status_code == 200
    io = resp.json()["io"]
    import json as _json

    dumped = _json.dumps(io["input"])
    assert "canary" not in dumped, f"CONTRACT: the canary must never reach the panel, got {dumped}"
    assert "***" in dumped, "the masked form is what the operator reads"


async def test_the_node_panel_honors_the_capture_policies(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE CAPTURE DISCIPLINE'S PIN (two arms):
    * errors-only (the default): a failed node's capture — the engine's
      OWN redacted + truncated write — rides the panel AS WRITTEN;
    * ``none``: the policy captured NOTHING and the panel says so
      (``capture_suppressed``) — it never substitutes raw rows for a
      suppressed capture (the io block carries no error payload of its
      own; the row's error COLUMNS remain the Q5-bounded fields they
      always were)."""
    schema = module_pg_schema.schema_name

    # ARM 1 — the default policy's capture, as the engine wrote it:
    compiled = sys.modules[MODULE_NAME].app.get("admin_map_doomed_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    map_run = str((await runner.create_flow()).flow_id)
    await runner.drive(map_run)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/api/runs/{map_run}/nodes/ingest.item?map_index=2")
        assert resp.status_code == 200
        io = resp.json()["io"]
        assert io["capture"] is not None, io
        assert "permanent failure" in io["capture"]["error"], io["capture"]
        assert io["capture_suppressed"] is False
        # A FAILED node carries no result face (the failure path's
        # output is the capture — never an empty-envelope lie).
        assert io["result"] is None

    # ARM 2 — the declared ``none`` policy: suppressed, and named.
    compiled = sys.modules[MODULE_NAME].app.get("admin_nocapture_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    nc_run = str((await runner.create_flow()).flow_id)
    await runner.drive(nc_run)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/api/runs/{nc_run}/nodes/solo-doomed")
        assert resp.status_code == 200
        io = resp.json()["io"]
        assert io["capture"] is None, "the policy captured nothing — the panel serves none"
        assert io["capture_suppressed"] is True, (
            "CONTRACT: the suppression is a NAMED state, never a silent blank"
        )


def test_the_page_js_carries_the_base_path_and_rerun_laws() -> None:
    """THE JS LAW' S SOURCE PIN: the page script builds every URL on the
    boot's mounted prefix (the relative-URL 404 bug's conviction), asks
    once before the rerun forms submit (form[data-confirm] →
    window.confirm), and escapes the row key before it touches a
    node-table selector (the poisoned-row defense)."""
    from pathlib import Path

    js = Path(__file__).parents[2].joinpath("src/taskq/web/static/workflows.js").read_text()
    assert "boot.basePath" in js, "CONTRACT: every URL rides the boot's mounted prefix"
    assert "form[data-confirm]" in js and "window.confirm" in js, (
        "CONTRACT: the rerun forms confirm before they submit"
    )
    assert '[data-wf-node-row="' in js and "CSS.escape" in js, (
        "CONTRACT: the node-table selector escapes the attacker-controlled key"
    )
    assert 'BASE + "/api/runs/"' in js, "the Resolve door's fetch rides the base path too"
