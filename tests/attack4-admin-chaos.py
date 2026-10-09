# ruff: noqa: N999, S608, TID251, ASYNC230  # Why: the brief mandates the dash-named attack4-* module; the seeded adversarial rows deliberately use uuid4 (a row whose id must NOT collinearly pack the B-tree); the open() writes the latency capture beside the test.
"""ATTACK4 — THE ADMIN PAGE UNDER FIRE (the phase-4 attacker's suite).

The stranger's adversarial probes against the workflow-run explorer:
NOT the builder's happy-path pins (tests/web_admin/test_workflows_page.py
owns those) — the chaos shapes, over a REAL uvicorn server (the builder's
own SSE tests ride the raw generator; httpx's ASGITransport buffers a
StreamingResponse's body, so a through-the-endpoint probe needs the real
transport — that honesty is this file's premise):

* the SSE feed under a STALE / hostile Last-Event-ID (a huge cursor, a
  negative cursor, a non-numeric cursor — the replay contract must not
  care);
* TWO browsers on one run — the revisioned snapshots interleave
  correctly (each consumer's own seq is monotonic; both see the same
  transition order);
* a KILLED connection mid-stream (the socket is closed at its poll —
  the server survives, the reconnect replays);
* the rows==DOM check under a resolve landing CONCURRENTLY with the page
  render (the boot JSON must be INTERNALLY consistent — the §17.5
  derivation over its own nodes == its status — whichever side of the
  race it caught);
* the XSS surface: a node's error text carrying HTML / script / a
  </script> closer (the boot JSON is rendered INSIDE a <script> tag —
  the tojson filter must break the closer), the hold's reason carrying
  markup, and the client's own conventions (textContent, never
  innerHTML, for every dynamic line);
* the audit trail's completeness: the resolve and the cancel each land
  their admin_audit row and the page's trail renders them;
* the panel latency band re-measured on a 200-child map (my own
  measurement, captured to .measurements/attack4/).

This file FIXES NOTHING: a red here is a finding, reported.
"""

from __future__ import annotations

import asyncio
import json
import socket
import sys
import time
import types
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any

import asyncpg
import httpx
import pytest
import uvicorn
from pydantic import BaseModel

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")
from fastapi import FastAPI

from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.workflows import FlowRunner, WorkflowApp, build, map_source, step
from taskq.workflows.api import GateDecl

pytestmark = pytest.mark.integration

MODULE_NAME = "attack4_admin_demo_flows"


class Approval(BaseModel):
    verdict: str
    note: str = ""


class Ingest(BaseModel):
    doc_id: str


class Item(BaseModel):
    n: int


async def _wait(ctx: Any, params: Ingest) -> str:
    await ctx.wait_signal((Approval,), reason="editorial approval", timeout_s=120.0)
    return "published"


async def _plain(ctx: Any, params: Ingest) -> str:
    return "done"


async def _items(ctx: Any, params: Ingest) -> list[Item]:
    return [Item(n=i) for i in range(200)]


async def _per_item(ctx: Any, item: Item) -> dict[str, int]:
    return {"n": item.n}


async def _map_tail(ctx: Any, items: list[dict[str, int]]) -> dict[str, int]:
    return {"sum": sum(i["n"] for i in items)}


def _module() -> types.ModuleType:
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("attack4a_hold_flow")
    def hold_flow() -> object:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)
        return build(step(_wait, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    @app_obj.workflow("attack4a_plain_flow")
    def plain_flow() -> object:
        return build(step(_plain, Ingest(doc_id="d1"), key="solo"))

    @app_obj.workflow("attack4a_map_flow")
    def map_flow() -> object:
        ingested = step(_items, Ingest(doc_id="d1"), key="ingest")
        children = map_source(ingested, _per_item)
        return build(step(_map_tail, children, key="tail"))

    module.app = app_obj  # type: ignore[attr-defined]
    return module


@pytest.fixture(scope="module")
def demo_env() -> Iterator[types.ModuleType]:
    """The module's dev posture + its WorkflowApp module (the typed
    door's source), for the whole module's life."""
    # The dev posture rides a raw MonkeyPatch (the suite-hygiene law: a
    # bare os.environ write has no teardown — the atk_iso incident). The
    # undo() runs at the fixture's end, every exit path.
    env_patch = pytest.MonkeyPatch()
    env_patch.setenv("TASKQ_ENVIRONMENT", "dev")
    env_patch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    env_patch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")
    module = _module()
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]
    env_patch.undo()


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="module")
async def live_server(
    module_pg_schema: ModulePgSchema, demo_env: types.ModuleType
) -> AsyncIterator[str]:
    """A REAL uvicorn server on an ephemeral port, in its OWN THREAD (the
    server's tasks + pool live on the thread's loop — a long-lived task
    on the module loop would trip the suite's leaked-task detector for
    every test after it), serving the admin router with the module's
    WorkflowApp mounted (the typed door)."""
    import threading

    schema = module_pg_schema.schema_name
    dsn = module_pg_schema.pg_dsn
    port = _free_port()

    def serve() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def build() -> FastAPI:
            # The thread's OWN pool (asyncpg pools are loop-bound).
            pg = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
            bundle = create_router(
                pg,
                schema=schema,
                base_path="",
                workflow_app=demo_env.app,  # type: ignore[attr-defined]
            )
            fast_app = FastAPI()
            setup_admin_state(fast_app, bundle)
            fast_app.include_router(bundle.router)
            return fast_app

        fast_app = loop.run_until_complete(build())
        config = uvicorn.Config(
            fast_app, host="127.0.0.1", port=port, log_level="error", lifespan="off"
        )
        server = uvicorn.Server(config)
        loop.run_until_complete(server.serve())
        loop.close()

    thread = threading.Thread(target=serve, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    # The port answers (bounded).
    deadline = asyncio.get_event_loop().time() + 30
    while asyncio.get_event_loop().time() < deadline:
        try:
            async with httpx.AsyncClient(base_url=base, timeout=2.0) as probe:
                resp = await probe.get("/queues")
            if resp.status_code == 200:
                break
        except (httpx.HTTPError, OSError):
            await asyncio.sleep(0.1)
    else:
        raise AssertionError("the live server never answered")
    yield base
    # The thread is daemon: the interpreter reaps it; give it a beat to
    # finish its in-flight polls.
    await asyncio.sleep(0.3)


def _make_admin_app(pg: asyncpg.Pool, schema: str, wf_app: Any = None) -> FastAPI:
    bundle = create_router(pg, schema=schema, base_path="", workflow_app=wf_app)
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


async def _run_seed(pool: asyncpg.Pool, schema: str, *, name: str = "attack4a_hold_flow") -> str:
    compiled = sys.modules[MODULE_NAME].app.get(name)  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id, until="held")
    return str(flow_id)


def _frames_from(text: str) -> list[dict[str, Any]]:
    frames: list[dict[str, Any]] = []
    event_id: str | None = None
    for line in text.splitlines():
        if line.startswith("id: "):
            event_id = line[4:]
        elif line.startswith("data: ") and event_id is not None:
            frames.append(json.loads(line[6:]))
            event_id = None
    return frames


async def _read_frames_for(
    base_url: str,
    run_id: str,
    *,
    count: int,
    cursor: str | None = None,
) -> list[dict[str, Any]]:
    """`count` data frames off the REAL endpoint (a real TCP stream),
    bounded — the read closes the socket as soon as it has them (the
    mid-stream kill)."""
    headers = {"Accept": "text/event-stream"}
    if cursor is not None:
        headers["Last-Event-ID"] = cursor
    client = httpx.AsyncClient(base_url=base_url, timeout=20.0)
    try:
        async with client.stream("GET", f"/api/runs/{run_id}/stream", headers=headers) as resp:
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/event-stream")
            raw = ""
            async with asyncio.timeout(20.0):
                async for chunk in resp.aiter_text():
                    raw += chunk
                    if len(_frames_from(raw)) >= count:
                        break
    finally:
        await client.aclose()
    return _frames_from(raw)[:count]


# ── the stale / hostile Last-Event-ID ───────────────────────────────────


async def _reap_stream_tasks() -> None:
    """Let the killed socket's server-side cycle notice the disconnect
    (one poll interval), then await any still-pending ASGI cycle tasks —
    the suite's own law: no task outlives its test."""
    await asyncio.sleep(1.2)
    pending = [
        t
        for t in asyncio.all_tasks()
        if t is not asyncio.current_task()
        and not t.done()
        and ("run_asgi" in repr(t.get_coro()) or "wrap" in repr(t.get_coro()))
    ]
    for t in pending:
        t.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.parametrize("cursor", ["999999", "-5", "not-a-number", "1e9"])
async def test_a_stale_or_hostile_last_event_id_still_replays_the_full_snapshot(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    live_server: str,
    cursor: str,
) -> None:
    """THE REPLAY CONTRACT under a hostile cursor: whatever the client
    sends in Last-Event-ID (stale-huge, negative, garbage, float-notation),
    the NEXT frame over the wire is the run's FULL snapshot — a reconnect
    can never strand the page on a stale render, and none of them 500s."""
    schema = module_pg_schema.schema_name
    run_id = await _run_seed(module_pg_pool, schema)
    try:
        frames = await _read_frames_for(live_server, run_id, count=1, cursor=cursor)
        assert frames, f"cursor {cursor!r}: the first frame is the snapshot"
        assert frames[0]["seq"] >= 1, f"cursor {cursor!r}: a non-positive seq leaked"
        assert {n["key"] for n in frames[0]["nodes"]} == {"review"}
        assert frames[0]["status"] == "blocked"
    finally:
        await _reap_stream_tasks()


# ── the kill mid-stream, then reconnect ─────────────────────────────────


async def test_the_kill_mid_stream_then_reconnect_replays(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    live_server: str,
) -> None:
    """CHAOS: the browser's socket is KILLED mid-stream (the read closes
    the real TCP connection after the first frame). The server must
    survive (a later request works), and the reconnect must replay the
    whole state — the snapshot subsumes the gap."""
    schema = module_pg_schema.schema_name
    run_id = await _run_seed(module_pg_pool, schema)
    try:
        first = await _read_frames_for(live_server, run_id, count=1)
        assert first and first[0]["nodes"], "the first consumer saw the snapshot"
        # THE SERVER SURVIVED the killed socket.
        async with httpx.AsyncClient(base_url=live_server, timeout=10.0) as client:
            probe = await client.get(f"/api/runs/{run_id}/nodes/review")
        assert probe.status_code == 200
        # THE RECONNECT replays the full state.
        again = await _read_frames_for(live_server, run_id, count=1)
        assert again[0]["nodes"] == first[0]["nodes"]
        assert again[0]["holds"] == first[0]["holds"]
    finally:
        await _reap_stream_tasks()


# ── two browsers on one run ─────────────────────────────────────────────


async def test_two_browsers_on_one_run_the_snapshots_interleave_correctly(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    live_server: str,
) -> None:
    """TWO browsers on one run: both consumers get their own revisioned
    snapshots; each consumer's own seq is monotonic; both see the SAME
    transition order — and the resolve that started it all lands EXACTLY
    ONCE (two concurrent resolves = one delivered, one no-op)."""
    from taskq.workflows.api._hitl import HitlClient

    schema = module_pg_schema.schema_name
    run_id = await _run_seed(module_pg_pool, schema)

    async def browser(count: int, cursor: str | None = None) -> list[dict[str, Any]]:
        try:
            return await _read_frames_for(live_server, run_id, count=count, cursor=cursor)
        finally:
            await _reap_stream_tasks()

    first_a, first_b = await asyncio.gather(browser(1), browser(1))
    assert first_a and first_b
    assert first_a[0]["nodes"] == first_b[0]["nodes"]
    assert first_a[0]["status"] == first_b[0]["status"] == "blocked"

    # The resolve, TWICE concurrently (two operators clicking the same
    # button): exactly ONE delivered.
    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)
    r1, r2 = await asyncio.gather(
        client.resolve(hold.hold_id, {"verdict": "approve"}),
        client.resolve(hold.hold_id, {"verdict": "approve"}),
    )
    assert sorted([r1.status, r2.status]) == ["delivered", "no-op"], (
        f"the double resolve was not exactly-once: {r1.status}, {r2.status}"
    )

    # The drive to terminal, then both browsers re-read from their own
    # stale cursors: both catch up to the SAME final derived state.
    compiled = sys.modules[MODULE_NAME].app.get("attack4a_hold_flow")  # type: ignore[attr-defined]
    await FlowRunner(compiled, module_pg_pool, schema).drive(uuid.UUID(run_id))
    tail_a, tail_b = await asyncio.gather(
        browser(1, cursor=str(first_a[0]["seq"])), browser(1, cursor=str(first_b[0]["seq"]))
    )
    assert tail_a and tail_b
    seqs_a = [f["seq"] for f in (first_a[0], tail_a[0])]
    seqs_b = [f["seq"] for f in (first_b[0], tail_b[0])]
    assert seqs_a == sorted(seqs_a), f"browser A's seq regressed: {seqs_a}"
    assert seqs_b == sorted(seqs_b), f"browser B's seq regressed: {seqs_b}"
    assert tail_a[0]["status"] == tail_b[0]["status"], (
        f"the two browsers disagree on the final state: {tail_a[0]['status']} vs {tail_b[0]['status']}"
    )


# ── the rows==DOM check under a concurrent resolve ───────────────────────


async def test_the_rows_dom_check_holds_under_a_resolve_landing_mid_render(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    demo_env: types.ModuleType,
) -> None:
    """A resolve lands WHILE the page renders (10 renders racing one
    resolve): every rendered page's boot JSON must be INTERNALLY
    consistent — the §17.5 derivation over ITS OWN node states == its
    status, and no render 500s mid-race."""
    schema = module_pg_schema.schema_name
    run_id = await _run_seed(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)

    async def render_once() -> dict[str, Any]:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get(f"/workflows/{run_id}")
        assert resp.status_code == 200
        boot = json.loads(
            resp.text.split('<script id="wf-boot" type="application/json">')[1].split("</script>")[
                0
            ]
        )
        return boot

    from taskq.workflows._status import NodeView, derive_workflow_status
    from taskq.workflows.api._hitl import HitlClient

    async def resolve_soon() -> None:
        await asyncio.sleep(0.01)
        client = HitlClient(module_pg_pool, schema=schema)
        (hold,) = await client.list(run_id)
        await client.resolve(hold.hold_id, {"verdict": "approve"})

    results = await asyncio.gather(
        *[render_once() for _ in range(10)], resolve_soon(), return_exceptions=True
    )
    boots = results[:-1]
    for r in boots:
        if isinstance(r, BaseException):
            raise r
    for boot in boots:
        assert isinstance(boot, dict)
        nodes = boot["state"]["nodes"]
        views = tuple(
            NodeView(status=n["status"], deps_pending=0, blocking_reason=None, held=n["hold"])
            for n in nodes
        )
        derived = derive_workflow_status(views)
        # The page's status == the derivation over the page's OWN nodes —
        # whichever side of the race this render caught.
        assert boot["state"]["status"] == derived, (
            f"the hybrid render: status {boot['state']['status']} vs "
            f"the derivation {derived} over the same boot"
        )


# ── the XSS / redecoration surface ───────────────────────────────────────


async def test_a_nodes_error_text_carrying_markup_is_escaped_everywhere(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    demo_env: types.ModuleType,
) -> None:
    """THE XSS PROBE: a node's error text contains <script>, an
    <img onerror>, and a literal </script> closer (the wf-boot JSON
    renders INSIDE a <script> tag — an unescaped closer is the injected
    script's exit). The page must render it ESCAPED (server), and the
    panel API must carry it as JSON data (never markup)."""
    schema = module_pg_schema.schema_name
    evil = "<script>alert(1)</script> <img src=x onerror=alert(2)> </script>"
    run_uuid = uuid.uuid4()
    # A run whose node carries the hostile error text (written AS a run's
    # own row — the error text is job-data, never operator input).
    await module_pg_pool.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, step_key, status, attempt, max_attempts, payload, metadata, queue, "
        " error_class, error_message, retry_kind, created_at) "
        "VALUES ($1, 'attack4', 'evil_step', 'failed', 1, 3, 'null'::jsonb, "
        " $2::jsonb, 'default', 'EvilError', $3, 'transient', now())",
        uuid.uuid4(),
        json.dumps({"flow_id": str(run_uuid), "workflow": "attack4a_evil"}),
        evil,
    )
    await module_pg_pool.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, step_key, status, payload, metadata, queue, max_attempts, retry_kind, created_at) "
        "VALUES ($1, 'attack4', '__flow__', 'failed', 'null'::jsonb, "
        " $2::jsonb, 'default', 3, 'transient', now())",
        run_uuid,
        json.dumps({"flow_id": str(run_uuid), "workflow": "attack4a_evil"}),
    )
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        page = await client.get(f"/workflows/{run_uuid}")
        assert page.status_code == 200
        html = page.text
        # THE SERVER RENDER: no raw markup survives into the HTML — the
        # sharper check: the wf-boot JSON's script tag must not be
        # CLOSED by the payload (the parse succeeding IS the proof).
        assert "<script>alert(1)" not in html
        assert "<img src=x onerror" not in html
        boot_json = html.split('<script id="wf-boot" type="application/json">')[1]
        boot = json.loads(boot_json.split("</script>")[0])
        assert boot["runId"] == str(run_uuid)
        # THE PANEL API: the raw error is DATA (a JSON string), not markup.
        panel = await client.get(f"/api/runs/{run_uuid}/nodes/evil_step")
        assert panel.status_code == 200
        assert panel.json()["error_message"] == evil  # intact as data
        assert panel.headers["content-type"].startswith("application/json")
    # THE CLIENT'S CONVENTION (the redecoration surface): every dynamic
    # line the workflow JS writes uses textContent — an innerHTML on an
    # error/label line is the injection's door.
    js = (
        __import__("pathlib").Path(__file__).parent.parent / "src/taskq/web/static/workflows.js"
    ).read_text()
    for line in js.splitlines():
        if "innerHTML" in line:
            # The ONLY allowed innerHTML sites: the mermaid SVG (the
            # vendored renderer's own output) and the deliberate clear.
            stripped = line.strip()
            assert "res.svg" in stripped or stripped.endswith('innerHTML = "";'), (
                f"the JS writes non-SVG markup via innerHTML: {stripped!r}"
            )


async def test_the_holds_reason_carrying_markup_is_escaped_on_the_page(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    demo_env: types.ModuleType,
) -> None:
    """The hold's reason (the author's declared why) carrying markup:
    the page's autoescape must render it inert."""
    schema = module_pg_schema.schema_name
    run_id = await _run_seed(module_pg_pool, schema)
    await module_pg_pool.execute(
        f'UPDATE "{schema}".wf_signals '
        "SET payload = $2::jsonb WHERE workflow_id = $1::uuid AND status = 'held'",
        uuid.UUID(run_id),
        json.dumps({"reason": "<b>own the risk</b><script>alert(3)</script>"}),
    )
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/workflows/{run_id}")
    html = resp.text
    assert "<b>own the risk</b>" not in html, "the reason's markup rode raw"
    assert "<script>alert(3)</script>" not in html
    assert "&lt;b&gt;own the risk&lt;/b&gt;" in html  # escaped, still readable


# ── the audit trail's completeness ───────────────────────────────────────


async def test_every_action_button_lands_its_audit_row(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    demo_env: types.ModuleType,
) -> None:
    """THE AUDIT TRAIL (G4) under fire: the resolve and the cancel each
    land their admin_audit row, attributed, and the page's trail renders
    them. A button whose action is NOT a row is the unaccountable
    surface."""
    schema = module_pg_schema.schema_name
    app = _make_admin_app(
        module_pg_pool,
        schema,
        wf_app=sys.modules[MODULE_NAME].app,  # type: ignore[attr-defined]
    )
    run_id = await _run_seed(module_pg_pool, schema)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/queues")
        csrf = client.cookies.get("taskq_csrf_token", "")
        assert csrf, "the GET must set the CSRF cookie first"
        from taskq.workflows.api._hitl import HitlClient

        hc = HitlClient(module_pg_pool, schema=schema)
        (hold,) = await hc.list(run_id)
        r = await client.post(
            f"/api/runs/{run_id}/resolve",
            data={
                "hold_id": hold.hold_id,
                "decision": json.dumps({"verdict": "approve"}),
                "csrf_token": csrf,
            },
        )
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "delivered"
        # THE CANCEL button (the run page's stop).
        r2 = await client.post(
            f"/api/runs/{run_id}/cancel",
            data={"reason": "attack4 audit probe", "csrf_token": csrf},
        )
        assert r2.status_code in (200, 303), r2.text
    rows = await module_pg_pool.fetch(
        f'SELECT action, principal_subject, target_type, target_id FROM "{schema}".admin_audit '
        "WHERE target_id = $1 OR target_id LIKE $2 OR detail->>'run_id' = $1 "
        "ORDER BY id",
        run_id,
        f"{run_id}:%",
    )
    actions = {r["action"] for r in rows}
    assert "hitl.resolve" in actions, f"the resolve left no audit row: {actions}"
    assert "workflow.cancel" in actions, f"the cancel left no audit row: {actions}"
    for row in rows:
        assert row["principal_subject"], f"an unattributed action rode: {row['action']}"
    # The page RENDERS the trail.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        page = await client.get(f"/workflows/{run_id}")
    assert "hitl.resolve" in page.text and "workflow.cancel" in page.text


# ── the panel latency under a 200-child map ─────────────────────────────


async def test_the_panel_and_page_latency_on_a_200_child_map(
    module_pg_schema: ModulePgSchema,
    module_pg_pool: asyncpg.Pool,
    demo_env: types.ModuleType,
) -> None:
    """THE PINNED 50 ms BAND, re-measured on the ADVERSARIAL shape: a
    run whose map has 200 children (the builder's band was measured on a
    small run — the collapsed hexagon must keep the panel cheap at map
    scale). MY OWN numbers, captured to .measurements/attack4/."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("attack4a_map_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    # Drive the fork: ticks until terminal or nothing claimable — the
    # read path under test does not care whether the children are
    # terminal, only how many rows exist.
    await runner.drive(flow_id)
    children = await module_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key LIKE 'ingest.item%'",
        flow_id,
    )
    assert children == 200, f"the map seeded {children} children, not 200"
    app = _make_admin_app(module_pg_pool, schema)
    panel_ms: list[float] = []
    page_ms: list[float] = []
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for _ in range(10):
            t0 = time.perf_counter()
            r = await client.get(f"/api/runs/{flow_id}/nodes/ingest.item")
            panel_ms.append((time.perf_counter() - t0) * 1000)
            assert r.status_code in (200, 404), r.status_code
        for _ in range(5):
            t0 = time.perf_counter()
            r = await client.get(f"/workflows/{flow_id}")
            page_ms.append((time.perf_counter() - t0) * 1000)
            assert r.status_code == 200
    panel_ms.sort()
    page_ms.sort()
    p95_panel = panel_ms[-1]
    med_page = page_ms[len(page_ms) // 2]
    print(
        f"\n[attack4] 200-child map: panel p95 {p95_panel:.1f} ms "
        f"(band 50 ms), run-page median {med_page:.1f} ms"
    )
    with open(".measurements/attack4/attack4-panel-latency-200map.json", "w") as f:
        json.dump(
            {"children": 200, "panel_ms_sorted": panel_ms, "page_ms_sorted": page_ms}, f, indent=2
        )
    assert p95_panel <= 50.0, f"the panel's p95 read {p95_panel:.1f} ms — the band is 50 ms"
