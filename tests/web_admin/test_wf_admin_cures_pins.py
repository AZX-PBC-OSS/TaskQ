# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier; every value is $-bound.
"""THE ADMIN-SURFACE CURES' PINS — the red-team's convicted findings,
each cured and pinned red-first where the rig's red is reproducible in
the fast lane:

- Q1d THE DEAD MAP-COLLAPSE: the map progress read feeds the TRUE
  parents (the map-source nodes the fork/emit INSERT parented every
  child at), never the root — the hexagon + counter fire, the render is
  BOUNDED (a 10k fan-out renders one hexagon, never 10k raw node
  lines), and the §17.5 derivation still reads EVERY row (the collapse
  never hides a failure from the verdict).
- Q3 THE INVISIBLE PROBE: the typed door's 422 refusal writes its
  ``admin_audit`` row (refused + the reason) — the attacker's
  shape-probing lands IN the trail the page claims covers every
  Resolve/deliver; the hold survives.
- Q4 THE SSE SESSION-REVOCATION GAP: the run stream carries the SAME
  mid-stream re-check ``/sse/{topic}`` got for #316 — a revoked admin
  session's stream terminates at the next tick.
- Q-EXTRA THE 400-WALL REDIRECT: the refused cancel's
  ``?error=cancel-not-applied`` redirect renders the REFUSED-OP BANNER
  (200, the defined state), never a 400 wall; undeclared params still
  400.
- Q5 THE UNBOUNDED READS: the node panel's error fields are
  display-capped with the dropped-count named (``error_truncated``);
  the root payload is off the read path entirely.
- Q6 (the nits) THE TWO SELECTOR THROWS: the JS escapes the row key
  (CSS.escape) and sanitizes the status class — a poisoned row key
  cannot blind the run's live updates.
- Q7 THE UNKNOWN-RUN STREAM: the existence oracle is closed — an
  unknown run id is a 404 AT SUBSCRIBE (the ``/sse/{topic}`` convention).
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
from fastapi import FastAPI, HTTPException, Request

from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin.auth._session import IdentityClaims
from taskq.workflows import FlowRunner, WorkflowApp, build, map_source, step
from taskq.workflows.api import GateDecl

pytestmark = pytest.mark.integration

MODULE_NAME = "p4_admin_cure_flows"
_TICK = 0.02


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


async def _wait(ctx: Any, params: Ingest) -> str:
    await ctx.wait_signal((Approval,), reason="editorial approval", timeout_s=120.0)
    return "published"


async def _src(ctx: Any, params: Ingest) -> list[int]:
    return [1, 2, 3]


async def _doomed_src(ctx: Any, params: Ingest) -> list[int]:
    return [1, 2, 3]


async def _item_ok(value: int) -> dict[str, int]:
    return {"n": value}


async def _item_doomed(value: int) -> dict[str, int]:
    if value == 2:
        raise RuntimeError("the map child's doom")
    return {"n": value}


async def _tail(ctx: Any, params: Ingest) -> str:
    return "tail-done"


def _csrf_of(client: Any) -> str:
    token = client.cookies.get("taskq_csrf_token", "")
    assert token, "the GET must set the CSRF cookie before any POST"
    return token


@pytest.fixture
def cure_app_module() -> Iterator[types.ModuleType]:
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("cure_hold_flow")
    def hold_flow() -> object:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)
        return build(step(_wait, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    @app_obj.workflow("cure_map_flow")
    def map_flow() -> object:
        source = step(_src, Ingest(doc_id="d1"), key="src")
        mapped = map_source(source, _item_ok, key="map1")
        del mapped
        return build(step(_tail, source, key="tail"))

    @app_obj.workflow("cure_map_doomed_flow")
    def map_doomed_flow() -> object:
        source = step(_doomed_src, Ingest(doc_id="d1"), key="src")
        mapped = map_source(source, _item_doomed, key="map1")
        del mapped
        return build(step(_tail, source, key="tail"))

    module.app = app_obj  # type: ignore[attr-defined]
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


def _make_admin_app(pool: asyncpg.Pool, schema: str, wf_app: Any = None) -> FastAPI:
    bundle = create_router(pool, schema=schema, base_path="", workflow_app=wf_app)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


async def _seed_run(pool: asyncpg.Pool, schema: str, *, name: str = "cure_hold_flow") -> str:
    compiled = sys.modules[MODULE_NAME].app.get(name)  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, pool, schema)
    flow_id = await runner.create_flow()
    # THE HOLD FLOW stops at its hold (a drive to terminal would burn
    # max_ticks against the 120s hold deadline); the map flows run to
    # terminal.
    until = "held" if "hold" in name else "terminal"
    await runner.drive(flow_id, until=until)
    return str(flow_id)


# ── Q1d: THE DEAD MAP-COLLAPSE ───────────────────────────────────────────


async def test_map_hexagon_counter_feeds_from_the_true_parent(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    cure_app_module: Any,
) -> None:
    """THE CONVICTION (Q1d): the map children are born parented at the
    MAP SOURCE node; the old read passed the ROOT's id to
    ``parent_id = $1`` — no row ever parents at the root, every counter
    read zero, the hexagon could never fire. The cured read aggregates
    PER MAP SOURCE: the source node's map_done/map_children carry the
    children's true counts, the child rows collapse (``map_child``),
    and the ROOT's counter is the SUM across sources."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="cure_map_flow")
    from taskq.web.admin._wf_rows import fetch_run_view, rows_mermaid, run_state_json

    async with module_pg_pool.acquire() as conn:
        view = await fetch_run_view(conn, schema, uuid.UUID(run_id))
    assert view is not None
    src = next(n for n in view.nodes if n.key == "src")
    assert src.map_children == 3, (
        "CONTRACT: the hexagon's counter reads the children's TRUE parent "
        f"(the map source) — got src.map_children={src.map_children}, the dead "
        "read's zero"
    )
    assert src.map_done == 3  # the drive ran to terminal
    kids = [n for n in view.nodes if n.key == "src.item"]
    assert len(kids) == 3
    assert all(k.map_child for k in kids), "the child rows must carry the collapse mark"
    # THE ROOT'S COUNTER: the SUM across sources.
    assert view.root_map_children == 3 and view.root_map_done == 3
    # THE MERMAID: ZERO child boxes — the hexagon speaks for the fan-out.
    mermaid = rows_mermaid(view)
    assert "src.item" not in mermaid, "a collapsed child must render no line"
    assert '{{"' in mermaid or "{{" in mermaid, "the source renders the hexagon shape"
    # THE SNAPSHOT: the frame's node list is the collapsed projection.
    state = run_state_json(view, seq=1)
    keys = {n["key"] for n in state["nodes"]}
    assert "src.item" not in keys
    assert state["map_children"] == 3 and state["map_done"] == 3


async def test_10k_fan_out_renders_one_hexagon_never_10k_lines(
    cure_app_module: Any,
) -> None:
    """THE AMPLIFIER PIN (Q1d's server+client bound): a 10k-child
    fan-out's view renders a BOUNDED mermaid text and a BOUNDED frame —
    the render does not scale with the fan-out (the red: 10k raw node
    lines — the admin-page brick). Synthetic rows: the read path is the
    assembly, not the driver."""
    from taskq._ids import new_uuid
    from taskq.web.admin._wf_rows import _run_view_from_rows, rows_mermaid, run_state_json

    run_id = new_uuid()
    source_id = new_uuid()
    root = {
        "id": run_id,
        "actor": "wf",
        "status": "running",
        "created_at": None,
        "finished_at": None,
        "cancel_requested_at": None,
        "error_class": None,
        "error_message": None,
        "workflow": "big",
    }
    node_rows: list[dict[str, Any]] = [
        {
            "id": source_id,
            "step_key": "src",
            "status": "succeeded",
            "deps_pending": 0,
            "parent_id": None,
            "blocking_reason": None,
            "absorbed": False,
            "error_class": None,
            "error_message": None,
        }
    ]
    for _ in range(10_000):
        node_rows.append(
            {
                "id": new_uuid(),
                "step_key": "src.item",
                "status": "succeeded",
                "deps_pending": 0,
                "parent_id": source_id,
                "blocking_reason": None,
                "absorbed": False,
                "error_class": None,
                "error_message": None,
            }
        )
    progress = {"src": (10_000, 10_000)}
    view = _run_view_from_rows(root, node_rows, [], [], progress)
    mermaid = rows_mermaid(view)
    assert len(mermaid.splitlines()) <= 5, (
        "CONTRACT: the collapsed render must not scale with the fan-out "
        f"({len(mermaid.splitlines())} lines for 10k children — the red was 10k+)"
    )
    assert view.root_map_children == 10_000
    state = run_state_json(view, seq=1)
    assert len(state["nodes"]) == 1, "the frame carries ONE hexagon node, not 10k entries"


async def test_the_collapse_never_hides_a_failure_from_the_derivation(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    cure_app_module: Any,
) -> None:
    """THE ROWS-ARE-TRUTH GUARD on the collapse: a FAILED map child
    still derives the run FAILED (the §17.5 derivation reads every row)
    while the RENDER stays collapsed — the hexagon must not become a
    place for a failure to hide."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="cure_map_doomed_flow")
    from taskq.web.admin._wf_rows import fetch_run_view, run_state_json

    async with module_pg_pool.acquire() as conn:
        view = await fetch_run_view(conn, schema, uuid.UUID(run_id))
    assert view is not None
    assert view.derive() == "failed", (
        f"the map child's failure must fail the run, got {view.derive()!r}"
    )
    kids = [n for n in view.nodes if n.key == "src.item"]
    assert any(k.status == "failed" for k in kids), "the failed child row is on the record"
    state = run_state_json(view, seq=1)
    assert state["status"] == "failed"
    assert all(n["key"] != "src.item" for n in state["nodes"]), "the render stays collapsed"


# ── Q3: THE INVISIBLE PROBE (the audited refusal) ────────────────────────


async def test_wrong_shaped_resolve_writes_the_refused_audit_row(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    cure_app_module: Any,
) -> None:
    """THE CONVICTION (Q3): the shape-refused resolves (the typed door's
    422s) wrote ZERO audit rows while the page claims every
    Resolve/cancel/deliver is audited — the attacker's probing was
    INVISIBLE. THE CURE'S PIN: the wrong-shaped resolve → the audit row
    EXISTS (refused + the reason) AND the hold survives."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="cure_hold_flow")
    from taskq.workflows import HitlClient

    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)
    app = _make_admin_app(module_pg_pool, schema, wf_app=sys.modules[MODULE_NAME].app)  # type: ignore[attr-defined]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        await http.get("/queues")  # the CSRF cookie's set
        bad = await http.post(
            f"/api/runs/{run_id}/resolve",
            data={
                "hold_id": hold.hold_id,
                "decision": json.dumps({"verdict": 42, "smuggle": True}),
                "csrf_token": _csrf_of(http),
            },
        )
    assert bad.status_code == 422
    audit = await clean_pg_conn.fetchrow(
        f'SELECT action, principal_subject, reason, detail FROM "{schema}".admin_audit '
        "WHERE target_id = $1 ORDER BY id DESC LIMIT 1",
        hold.hold_id,
    )
    assert audit is not None, (
        "CONTRACT: the shape-refused resolve is AUDITED — the invisible-probe "
        "conviction's cure (a 422 with zero audit rows reds)"
    )
    assert audit["action"] == "hitl.resolve"
    detail = audit["detail"]
    if isinstance(detail, str):  # asyncpg hands jsonb back as str without a codec
        detail = json.loads(detail)
    assert detail.get("refused"), "the row records WHY the payload was refused"
    assert await client.list(run_id), "the refused resolve moved nothing — the hold survives"
    # AND THE PAGE'S CLAIM IS NOW TRUE: the trail renders the row.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        page = await http.get(f"/workflows/{run_id}")
    assert "hitl.resolve" in page.text, "the refused probe is visible in the trail"


async def test_wrong_shaped_deliver_writes_the_refused_audit_row_too(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    cure_app_module: Any,
) -> None:
    """The deliver door's refusal audits exactly like the resolve's (the
    page's claim names deliver)."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="cure_hold_flow")
    from taskq.workflows import HitlClient

    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)
    app = _make_admin_app(module_pg_pool, schema, wf_app=sys.modules[MODULE_NAME].app)  # type: ignore[attr-defined]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        await http.get("/queues")
        bad = await http.post(
            f"/api/runs/{run_id}/deliver",
            data={
                "node": "review",
                "payload": json.dumps({"verdict": 7}),
                "csrf_token": _csrf_of(http),
            },
        )
    assert bad.status_code == 422
    audit = await clean_pg_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".admin_audit WHERE target_id = $1',
        hold.hold_id,
    )
    assert audit and int(audit) >= 1, "the deliver's shape refusal is audited too"
    assert await client.list(run_id), "the hold survives"


# ── Q4: THE SSE SESSION-REVOCATION GAP ───────────────────────────────────


async def test_run_stream_ends_when_the_session_is_revoked_mid_stream(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    cure_app_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE CONVICTION (Q4): the run stream lacked the mid-stream
    session re-check ``/sse/{topic}`` got for #316 — a revoked admin
    session kept receiving frames. THE CURE'S PIN: the revoked
    session's stream TERMINATES at the next tick."""
    monkeypatch.setattr(
        "taskq.web.admin._wf_actions._STREAM_POLL_S", _TICK
    )  # the poll is the re-check cadence
    schema = module_pg_schema.schema_name
    run_id = await _seed_run(module_pg_pool, schema, name="cure_hold_flow")

    stream_pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=2)
    auth_state = {"valid": True}
    claims = IdentityClaims(subject="ops", email=None, groups=frozenset(), raw={})

    async def _dependency(request: Request) -> IdentityClaims:
        if not auth_state["valid"]:
            raise HTTPException(status_code=401, detail="session revoked")
        return claims

    async def _verifier(request: Request) -> bool:
        return auth_state["valid"]

    _dependency.session_verifier = _verifier  # pyright: ignore[reportFunctionMemberAccess]  # Why: the attribute create_auth_dependency attaches; the derive path reads it.

    bundle = create_router(stream_pool, schema=schema, base_path="", auth_dependency=_dependency)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)

    # THE RAW-ASGI READER (the #316 test's own shape): httpx's
    # ASGITransport buffers a streaming body to completion — a stream
    # that ends on revocation never returns to an aiter loop there.
    received: list[bytes] = []
    _disco = asyncio.Event()

    async def _receive() -> dict[str, Any]:
        await _disco.wait()
        return {"type": "http.disconnect", "body": b"", "more_body": False}

    async def _send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.body":
            received.append(message.get("body", b""))

    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": f"/api/runs/{run_id}/stream",
        "raw_path": f"/api/runs/{run_id}/stream".encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(b"host", b"test")],
        "client": ("testclient", 123),
        "server": ("testserver", 80),
    }
    task = asyncio.create_task(app(scope, _receive, _send))
    deadline = asyncio.get_running_loop().time() + 10.0
    while asyncio.get_running_loop().time() < deadline:
        if b"event: run_state" in b"".join(received):
            break
        await asyncio.sleep(0.005)
    assert b"event: run_state" in b"".join(received), "the first frame arrived"
    auth_state["valid"] = False  # THE REVOCATION mid-stream
    await asyncio.sleep(_TICK * 3)
    deadline = asyncio.get_running_loop().time() + 5.0
    while not task.done() and asyncio.get_running_loop().time() < deadline:  # noqa: ASYNC110  # Why: bounded task-done poll - completion is observable on the task, not a signal this task can await.
        await asyncio.sleep(0.01)
    assert task.done(), (
        "CONTRACT: a run stream whose session is revoked must END at the "
        "next re-check tick (the #316 gap's cure) — a stream that "
        "authenticates only at subscribe keeps delivering frames"
    )
    await stream_pool.close()


async def test_run_stream_generator_revoked_before_first_frame_yields_nothing(
    module_pg_schema: ModulePgSchema,
) -> None:
    """Generator-level pin (the #316 test's twin): an already-revoked
    session yields NO frame at all."""
    from taskq.web.admin._wf_actions import _stream_generator

    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=2)
    try:

        async def _revoked() -> bool:
            return False

        # The NULL uuid: the revoked check returns before ANY read — the
        # id is never bound (and a read the verifier never passes must
        # never touch the pool).
        gen = _stream_generator(pool, module_pg_schema.schema_name, uuid.UUID(int=0), 0, _revoked)
        frames: list[str] = []
        async with asyncio.timeout(5.0):
            async for frame in gen:
                frames.append(frame)
        assert frames == [], f"a revoked session gets no frames, got {frames!r}"
    finally:
        await pool.close()


async def test_run_stream_generator_flip_ends_at_next_tick(
    module_pg_schema: ModulePgSchema,
    cure_app_module: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The verifier passes for the first frames, then fails: the stream
    ends and never yields another frame (the #316 flip pin's twin)."""
    monkeypatch.setattr("taskq.web.admin._wf_actions._STREAM_POLL_S", _TICK)
    from taskq.web.admin._wf_actions import _stream_generator

    schema = module_pg_schema.schema_name
    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=2)
    try:
        run_id = await _seed_run(pool, schema, name="cure_hold_flow")
        calls = {"n": 0}

        async def _flip() -> bool:
            calls["n"] += 1
            return calls["n"] < 4

        gen = _stream_generator(pool, schema, uuid.UUID(run_id), 0, _flip)
        frames: list[str] = []
        async with asyncio.timeout(10.0):
            async for frame in gen:
                frames.append(frame)
        assert any("run_state" in f for f in frames), "the checked pre-flip frames delivered"
        run_state_frames = sum(f.count("event: run_state") for f in frames)
        assert run_state_frames <= 3, (
            f"the revoked stream must stop at the next tick — at most the in-flight "
            f"frame may follow the flip, got {run_state_frames} run_state frames"
        )
    finally:
        await pool.close()


# ── Q-EXTRA: THE 400-WALL REDIRECT (the refused-op banner) ───────────────


async def test_refused_cancel_lands_on_the_banner_never_a_400_wall(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    cure_app_module: Any,
) -> None:
    """THE CONVICTION (the extra): run_cancel's refusal redirects to
    ``?error=cancel-not-applied`` but the detail page refused ALL query
    params (400) — the operator landed on a wall. THE CURE'S PIN: the
    redirect renders 200 WITH the refused-op banner; an undeclared
    param still 400s (the refuse-undeclared law keeps its teeth)."""
    schema = module_pg_schema.schema_name
    # THE HONEST TERMINAL RUN: the map flow drives to terminal (the root
    # row written by the engine, the G7 law intact) — the cancel then
    # refuses for real (an already-terminal run cancels nothing).
    run_id = await _seed_run(module_pg_pool, schema, name="cure_map_flow")
    app = _make_admin_app(module_pg_pool, schema, wf_app=sys.modules[MODULE_NAME].app)  # type: ignore[attr-defined]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")
        # THE TERMINAL RUN: the cancel refuses (idempotent no-op) and
        # redirects with the banner key.
        r = await http.post(
            f"/api/runs/{run_id}/cancel", data={"csrf_token": _csrf_of(http), "reason": "x"}
        )
        assert r.status_code == 303
        loc = r.headers["location"]
        assert "error=cancel-not-applied" in loc
        followed = await http.get(loc)
        assert followed.status_code == 200, (
            f"CONTRACT: the refusal redirect lands on the DEFINED state, never a "
            f"400 wall — got {followed.status_code}"
        )
        assert "Cancel not applied" in followed.text, "the refused-op banner renders"
        # THE UNDECLARED PARAM still refuses (the law keeps its teeth).
        wall = await http.get(f"/workflows/{run_id}?bogus=1")
        assert wall.status_code == 400


# ── Q5: THE UNBOUNDED READS ──────────────────────────────────────────────


async def test_node_panel_error_fields_are_display_capped(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """THE CONVICTION (Q5): the panel returned error_message/traceback/
    metadata->>'error' WHOLE — a foreign row's 5MB fields made a 15MB
    response. THE CURE'S PIN: the read is display-capped with the
    dropped-count named (``error_truncated``); the ROW keeps the full
    detail."""
    from taskq._ids import new_uuid

    schema = module_pg_schema.schema_name
    run_id = new_uuid()
    node_id = new_uuid()
    big = "A" * 500_000
    async with module_pg_pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, attempt, max_attempts, '
            "retry_kind, step_key, metadata, status) "
            "VALUES ($1,'wf','q','{}'::jsonb,0,1,'transient','__flow__',$2::jsonb,'running')",
            run_id,
            json.dumps({"flow_id": str(run_id), "workflow": "bigboy"}),
        )
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, attempt, max_attempts, '
            "retry_kind, step_key, metadata, status, error_class, error_message, error_traceback) "
            "VALUES ($1,'wf','q','{}'::jsonb,0,1,'transient','n1',$2::jsonb,'failed','E',$3,$4)",
            node_id,
            json.dumps({"flow_id": str(run_id), "error": big}),
            big,
            big,
        )
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        resp = await http.get(f"/api/runs/{run_id}/nodes/n1")
    assert resp.status_code == 200
    body = resp.json()
    assert len(resp.content) < 200_000, (
        f"CONTRACT: the panel's read is display-capped — got {len(resp.content)} bytes "
        "for a 500KB x3 foreign row"
    )
    assert body["error_truncated"] is True, "the truncated marker rides the reply"
    assert "truncated" in (body["error_message"] or ""), "the dropped-count is named"
    # THE ROW IS THE TRUTH: the full detail stays in the rows.
    row = await module_pg_pool.fetchval(
        f'SELECT length(error_message) FROM "{schema}".jobs WHERE id = $1', node_id
    )
    assert row == 500_000


def test_the_root_payload_is_off_the_read_path() -> None:
    """Q5's second half: the root payload was deserialized on EVERY
    render/SSE poll for NOTHING (pure tax). The read no longer selects
    it and the view no longer carries it."""
    from taskq.web.admin import _wf_rows

    assert "payload" not in _wf_rows._RUN_ROOT_SQL, (
        "CONTRACT: the root payload is off the read path (the Q5 tax)"
    )
    assert not [f for f in _wf_rows.RunView.__dataclass_fields__ if f == "input_payload"], (
        "the view carries no input_payload"
    )


# ── Q6: THE TWO SELECTOR THROWS (the source pins) ────────────────────────


def test_the_page_js_escapes_the_row_key_and_sanitizes_the_status_class() -> None:
    """THE NITS' PINS: the poisoned-row defense in the source — the
    querySelector escapes the attacker-controlled key (CSS.escape), and
    the status class is the legend's closed vocabulary (a space-bearing
    hostile status paints as pending, never throws)."""
    from pathlib import Path

    js = Path(__file__).parents[2].joinpath("src/taskq/web/static/workflows.js").read_text()
    assert "CSS.escape(key)" in js, (
        "CONTRACT: the node-key selector escapes the row key — a poisoned key "
        "with a quote/backslash threw SyntaxError and blinded the run's live updates"
    )
    assert "STATUS_CLASSES.indexOf(cls) < 0" in js and '"wf-st-pending"' in js, (
        "CONTRACT: the status class is sanitized to the legend's closed vocabulary — "
        "a space-bearing status threw InvalidCharacterError (the same blinding)"
    )


# ── Q7: THE UNKNOWN-RUN STREAM (the existence oracle) ────────────────────


async def test_unknown_run_stream_is_a_404_at_subscribe(
    module_pg_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """THE CONVICTION (Q7, LOW): an unknown run id streamed the empty
    'unknown' snapshot forever — an existence oracle behind the auth
    gate and a poll task per probe. THE CURE'S PIN: the 404 AT
    SUBSCRIBE (the estate's own ``/sse/{topic}`` convention: an unknown
    topic is refused at subscribe; the same law, one level up)."""
    app = _make_admin_app(module_pg_pool, module_pg_schema.schema_name)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        resp = await http.get("/api/runs/018f1c7e-5a2b-7c3d-8e4f-9a0b1c2d3e4f/stream")
    assert resp.status_code == 404, (
        f"CONTRACT: an unknown run is a 404 AT SUBSCRIBE, got {resp.status_code}"
    )
