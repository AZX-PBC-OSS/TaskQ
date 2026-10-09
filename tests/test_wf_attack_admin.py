# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier (module_pg_schema); every value is $-bound — the estate's test-SQL precedent (test_workflows_page.py).
"""ATTACK PINS — the admin workflow-run page's attack front.

Provenance: the hostile review of the consolidated head (af1b8779), the
admin-workflow-page attack front. Five findings LANDED and are pinned
strict-xfail, each asserting the SAFE behavior against the live defect —
the cure flips each to XPASS-strict (a red that tells you to remove the
marker WITH the cure, the same drill test_wf_attack_cancel.py's Face C
documents). Three attacks FAILED and are pinned as green guards so the
verified-safe behavior can never rot silently.

The landed five:

* F-ADM-1 — the map-collapse read is DEAD. ``_wf_rows.fetch_run_view``
  passes the run's ROOT id into ``WORKFLOW_MAP_PROGRESS_SQL``
  (``WHERE c.parent_id = $1``) but the engine's fork parents map
  children at the MAP SOURCE node (``_fork.insert_fork(parent_id=
  source_row)``) and static nodes carry ``parent_id NULL`` — so the
  grouped read matches zero rows, ``map_children`` is ALWAYS 0, and the
  hexagon collapse + n/m counter (``rows_mermaid``'s ``{{…}}`` shape,
  whose docstring promises "ZERO child boxes") can never fire: a big
  map renders as thousands of raw node rows.
* F-ADM-2 — a shape-refused resolve at the page's validation layer
  (``_wf_actions._validate_through_gates`` raises the 422 BEFORE
  ``HitlClient.resolve`` — the layer whose own refusal IS audited — is
  ever called) writes ZERO audit rows, while the page's own template
  (workflow_detail.html's audit-trail section) claims "every
  Resolve/cancel/deliver is audited".
* F-ADM-3 — the run SSE stream (``_wf_actions.run_stream``) never wires
  the mid-stream ``session_verifier`` re-check that ``/sse/{topic}``
  got for #316 (sse.py:91-118): a revoked admin session keeps the
  stream until disconnect.
* F-ADM-4 — ``run_cancel``'s refusal redirect targets
  ``?error=cancel-not-applied``, but ``workflow_detail`` refuses ALL
  query params (``reject_unknown_query_params(request, ())``) — the
  operator lands on a 400 wall instead of the refusal banner the jobs
  page's refused-op contract (jobs.py's ``_ERROR_MESSAGES``) defines.
* F-ADM-5 (defense-in-depth) — the node panel's SELECT returns
  ``error_message`` / ``error_traceback`` / ``metadata->>'error'``
  unbounded; a row carrying megabytes of failure text serves whole
  (the estate's precedent: jobs.py's ``_truncate_traceback`` display
  cap with its "... (N more characters)" marker, the CLI's 120-char
  detail line).

The failed three (green guards): the boot JSON's ``tojson`` escapes
``</script>`` (GUARD-1), the graph renders FROM THE ROWS for a run
stamped with a never-importable workflow module (GUARD-2), and the
node-panel read binds the node key as a parameter (GUARD-3).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import time
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

import taskq.web.admin._wf_actions as wf_actions
from taskq._ids import new_uuid
from taskq.backend._protocol import JobId
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.web.admin._wf_rows import fetch_run_view, rows_mermaid
from taskq.web.admin.auth._session import IdentityClaims
from taskq.workflows import FlowRunner, WorkflowApp, build, map_source, step
from taskq.workflows.api import GateDecl
from tests._wf_fixtures import seed_edge, seed_flow, seed_running_node

pytestmark = [pytest.mark.integration, pytest.mark.fastapi]

MODULE_NAME = "att_adm_demo_flows"

_TICK = 0.02

#: F-ADM-5's stored payload size: far above any honest display cap (the
#: jobs page's 2000-char traceback bound, the CLI's 120-char detail
#: line), far below PG's text limit — the write LANDS, so the served
#: length proves boundedness at the READ.
_HUGE_CHARS = 200_000

#: The bound the pin asserts on the panel's served error text: THE
#: SHIPPED CONSTANT ITSELF (pin the constant, not a copy — a copy
#: drifts from the served truth; the sweepaudit's own precedent).
_PANEL_FIELD_BOUND = (  # Why: the constant must ride the shipped module — pin the shipped value, never a copy.
    __import__("taskq.web.admin._wf_actions", fromlist=["_PANEL_FIELD_CAP_CHARS"])._PANEL_FIELD_CAP_CHARS
)


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
    return [Item(n=i) for i in range(5)]


async def _per_item(ctx: Any, item: Item) -> dict[str, int]:
    return {"n": item.n}


async def _map_tail(ctx: Any, items: list[dict[str, int]]) -> dict[str, int]:
    return {"sum": sum(i["n"] for i in items)}


@pytest.fixture(autouse=True)
def _adm_dev_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """The dev posture (this file lives at tests/ root, outside the
    web_admin path-gated _dev_env fixture): create_router's fail-closed
    auth check stands down, mutations are enabled, and the CSRF cookie
    survives plain-http TestClient/ASGI-transport transport."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_ACTIONS_ENABLED", "true")
    monkeypatch.setenv("TASKQ_ADMIN_UI_SECURE_COOKIES", "false")


@pytest.fixture
def demo_app_module() -> Iterator[types.ModuleType]:
    """The typed door's source (the host's WorkflowApp) + the run seeds'
    flows — the same shape tests/web_admin/test_workflows_page.py
    derives, re-derived here so this file stands alone."""
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("att_adm_hold_flow")
    def hold_flow() -> object:
        gate = GateDecl(name="Approval", payload_models=(Approval,), timeout_s=120.0)
        return build(step(_wait, Ingest(doc_id="d1"), key="review", gates=(gate,)))

    @app_obj.workflow("att_adm_plain_flow")
    def plain_flow() -> object:
        return build(step(_plain, Ingest(doc_id="d1"), key="solo"))

    @app_obj.workflow("att_adm_map_flow")
    def map_flow() -> object:
        ingested = step(_items, Ingest(doc_id="d1"), key="ingest")
        children = map_source(ingested, _per_item, key="child")
        return build(step(_map_tail, children, key="tail"))

    module.app = app_obj  # type: ignore[attr-defined]
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


def _make_admin_app(
    pool: asyncpg.Pool, schema: str, *, wf_app: Any = None, auth_dependency: Any = None
) -> FastAPI:
    bundle = create_router(
        pool, schema=schema, base_path="", workflow_app=wf_app, auth_dependency=auth_dependency
    )
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


def _make_authed_admin_app(pool: asyncpg.Pool, schema: str) -> tuple[FastAPI, dict[str, bool]]:
    """The admin router behind the #316 pin's wiring shape: the auth
    dependency and its exposed ``session_verifier`` attribute read ONE
    shared revocation flag (mirrors test_sse_session_recheck.py's
    _make_admin_app one-for-one)."""
    auth_state = {"session_valid": True}
    claims = IdentityClaims(subject="ops", email=None, groups=frozenset(), raw={})

    async def _dependency(request: Request) -> IdentityClaims:
        _ = request
        if not auth_state["session_valid"]:
            raise HTTPException(status_code=401, detail="session revoked")
        return claims

    async def _verifier(request: Request) -> bool:
        _ = request
        return auth_state["session_valid"]

    _dependency.session_verifier = _verifier  # pyright: ignore[reportFunctionMemberAccess]  # Why: the re-check the SSO dependency exposes; create_auth_dependency attaches the same attribute.
    return _make_admin_app(pool, schema, auth_dependency=_dependency), auth_state


def _csrf_of(client: Any) -> str:
    """The CSRF cookie's value (the GET that set it must precede the
    POST — the guarded-POST discipline's own dance)."""
    token = client.cookies.get("taskq_csrf_token", "")
    assert token, "the GET must set the CSRF cookie before any POST"
    return token


async def _seed_held_run(pool: asyncpg.Pool, schema: str) -> str:
    """A REAL run driven to its hold (the typed door's subject)."""
    compiled = sys.modules[MODULE_NAME].app.get("att_adm_hold_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, until="held") == "held"
    return str(flow_id)


async def _seed_terminal_run(pool: asyncpg.Pool, schema: str) -> str:
    """A REAL run driven to terminal — a cancel addressed at it MUST
    refuse (the refusal-redirect path's subject)."""
    compiled = sys.modules[MODULE_NAME].app.get("att_adm_plain_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    return str(flow_id)


async def _seed_node(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    *,
    step_key: str,
    status: str,
    deps: int = 0,
) -> JobId:
    """One node row on the run (the graph-from-rows guards' shape)."""
    node_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, deps_pending, metadata) "
        "VALUES ($1, 'wf', 'default', '{}', 3, 'transient', $2, $3, $4, $5::jsonb)",
        node_id,
        status,
        step_key,
        deps,
        json.dumps({"flow_id": str(flow_id)}),
    )
    return JobId(node_id)


# ──────────────────────────────────────────────────────────────────────
# F-ADM-1 — THE pin: the map-collapse read is dead (the page's
# signature feature is currently dead code with live cost).
# ──────────────────────────────────────────────────────────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-ADM-1]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (F-ADM-1, landed @af1b8779): fetch_run_view passes the run ROOT id …
async def test_the_map_collapse_counts_the_real_children(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """Drive a REAL 5-item map run to terminal, then read the run view
    the page renders: the map-source node must carry the REAL child
    count (5, not 0), the mermaid emission must render the collapse
    shape (the hexagon with the n/m counter), and the child rows must
    not render as raw boxes (rows_mermaid's documented contract: "ZERO
    child boxes — the child detail lives in the paginated panel")."""
    schema = module_pg_schema.schema_name
    compiled = sys.modules[MODULE_NAME].app.get("att_adm_map_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"
    # The children EXIST as rows (the fork landed — per-item identity).
    child_count = await module_pg_pool.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'ingest.item'",
        flow_id,
    )
    assert child_count == 5, f"the drive itself is broken: {child_count} child rows"

    async with module_pg_pool.acquire() as conn:
        view = await fetch_run_view(conn, schema, uuid.UUID(str(flow_id)))
    assert view is not None
    by_key = {}
    for node in view.nodes:
        by_key.setdefault(node.key, node)
    source = by_key["ingest"]
    in_view = sum(1 for n in view.nodes if n.key == "ingest.item")
    assert (source.map_children, source.map_done) == (5, 5), (
        f"the map-collapse read is dead: the view carries {in_view} 'ingest.item' "
        f"child rows (5 drove to terminal), yet the 'ingest' node reports "
        f"map_children={source.map_children}, map_done={source.map_done} — the "
        "progress read joins the ROOT id while the children parent at the MAP "
        "SOURCE node, so the counter can never fire and a big map renders as "
        "thousands of raw nodes"
    )
    mermaid = rows_mermaid(view)
    assert 'ingest{{"ingest 5/5"}}' in mermaid, (
        "the collapse shape never fired: the map node must render as the "
        f"hexagon-with-counter ({{{{…}}}}), got:\n{mermaid}"
    )
    assert "ingest.item[" not in mermaid, (
        "the collapsed map must emit ZERO child boxes (the child detail lives "
        f"in the paginated panel), got:\n{mermaid}"
    )


# ──────────────────────────────────────────────────────────────────────
# F-ADM-2 — the shape-refused resolve writes ZERO audit rows while the
# page claims "every Resolve/cancel/deliver is audited".
# ──────────────────────────────────────────────────────────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-ADM-2]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (F-ADM-2, landed @af1b8779): _validate_through_gates raises the …
async def test_a_shape_refused_resolve_attempt_writes_an_audit_row(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    wf_conn: asyncpg.Connection,
    demo_app_module: Any,
) -> None:
    """A wrong-shape decision POST answers the named 422 and moves
    nothing — AND the refused attempt is a ROW in admin_audit (the
    page's own audit claim covers attempts, not only deliveries)."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_held_run(module_pg_pool, schema)
    from taskq.workflows.api._hitl import HitlClient

    client = HitlClient(module_pg_pool, schema=schema)
    (hold,) = await client.list(run_id)
    app = _make_admin_app(module_pg_pool, schema, wf_app=sys.modules[MODULE_NAME].app)  # type: ignore[attr-defined]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        await http.get("/queues")  # the CSRF cookie's set (the guarded-POST discipline)
        refused = await http.post(
            f"/api/runs/{run_id}/resolve",
            data={
                "hold_id": hold.hold_id,
                "decision": json.dumps({"verdict": 42}),  # the wrong SHAPE (verdict is a str)
                "reason": "the shape attack's attempt",
                "csrf_token": _csrf_of(http),
            },
        )
    assert refused.status_code == 422
    assert "pydantic refused the payload" in refused.json()["detail"]
    assert len(await client.list(run_id)) == 1, "the refused resolve moved nothing"
    audit_rows = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{schema}".admin_audit '
        "WHERE target_id = $1 OR detail->>'run_id' = $2",
        str(hold.hold_id),
        run_id,
    )
    assert audit_rows >= 1, (
        "the page's validation layer refused the resolve with a 422 and wrote NO "
        "audit row for the attempt — while workflow_detail.html's audit-trail "
        "section tells the operator 'every Resolve/cancel/deliver is audited'. "
        "A refusal an operator can dispute is exactly the audit trail's case; "
        "either the refusal lands a row (preferred — HitlClient's own boundary "
        "refusal already audits at ITS layer) or the claim comes off the page"
    )


# ──────────────────────────────────────────────────────────────────────
# F-ADM-3 — the run SSE stream has no mid-stream session re-check (the
# #316 cure reached /sse/{topic}, never this stream).
# ──────────────────────────────────────────────────────────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-ADM-4]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (F-ADM-3, landed @af1b8779): _wf_actions.run_stream authenticates …
async def test_the_run_stream_ends_when_the_session_is_revoked_mid_stream(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
    demo_app_module: types.ModuleType,
) -> None:
    """A run stream opened BEFORE session invalidation must not outlive
    it: at the next poll tick the re-check fails and the stream ENDS
    (modeled the way the #316 endpoint pin models /sse/{topic}: raw
    ASGI, a shared revocation flag, a bounded wait)."""
    monkeypatch.setattr(wf_actions, "_STREAM_POLL_S", _TICK)
    app, auth_state = _make_authed_admin_app(module_pg_pool, module_pg_schema.schema_name)
    # A REAL run (the held shape): the shipped stream endpoint serves a
    # run that EXISTS — a no-rows id is the honest 404, never a stream;
    # the revocation's subject is the stream's LIFETIME, not the lookup.
    run_id = await _seed_held_run(module_pg_pool, module_pg_schema.schema_name)

    received: list[bytes] = []
    disconnect = asyncio.Event()

    async def _receive() -> dict[str, Any]:
        # A real ASGI receive parks until the client sends or disconnects;
        # an instant-return receive would spin the streaming loop.
        await disconnect.wait()
        return {"type": "http.disconnect", "body": b"", "more_body": False}

    async def _send(message: dict[str, Any]) -> None:
        if message["type"] == "http.response.body":
            received.append(message.get("body", b""))

    path = f"/api/runs/{run_id}/stream"
    scope: dict[str, Any] = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [(b"host", b"test")],
        "client": ("testclient", 123),
        "server": ("testserver", 80),
    }
    task = asyncio.create_task(app(scope, _receive, _send))
    try:
        deadline = time.monotonic() + 5.0
        # Why not an Event: the waiter can start after the frame already
        # landed, so it polls the buffer rather than racing a one-shot set.
        while time.monotonic() < deadline:
            if b"event: run_state" in b"".join(received):
                break
            await asyncio.sleep(0.005)
        assert b"event: run_state" in b"".join(received), "the stream's first frame never arrived"

        auth_state["session_valid"] = False
        tail = len(received)
        # A revision change AFTER the revocation: the pre-cure stream SERVES
        # the frame to the dead session (the finding's own words: it keeps
        # receiving frames); the cured stream has already ended.
        await module_pg_pool.execute(
            f'INSERT INTO "{module_pg_schema.schema_name}".wf_signals '
            "(id, workflow_id, node_key, signal_name, hold_epoch, call_id, status) "
            "VALUES ($1, $2, 'probe', 'Probe', 1, 'att-adm-3', 'held')",
            new_uuid(),
            run_id,
        )
        deadline = time.monotonic() + 2.0
        while not task.done() and time.monotonic() < deadline:  # noqa: ASYNC110  # Why: bounded task-done poll — completion is observable on the task, not a signal this task can await (the #316 pin's own shape).
            await asyncio.sleep(0.005)

        assert task.done(), (
            "CONTRACT: a run stream whose session is revoked must end at the "
            "next re-check tick. REGRESSION (F-ADM-3): the run stream "
            "authenticates only at subscribe and is STILL OPEN 2s after the "
            "revocation; frames served to the dead session in that window: "
            f"{b''.join(received[tail:])!r}"
        )
        frames_after_flip = b"".join(received[tail:]).count(b"event: run_state")
        assert frames_after_flip <= 1, (
            f"at most the in-flight tick may follow the revocation, got "
            f"{frames_after_flip}: {b''.join(received[tail:])!r}"
        )
    finally:
        disconnect.set()
        if not task.done():
            task.cancel()
            with contextlib.suppress(BaseException):
                await task


# ──────────────────────────────────────────────────────────────────────
# F-ADM-4 — the refused-cancel redirect lands on a 400 wall.
# ──────────────────────────────────────────────────────────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the finding's cure has landed [F-ADM-4]; the marker is removed per the designed flip (the confirmation receipt). The finding's record, verbatim: LIVE FINDING (F-ADM-4, landed @af1b8779): run_cancel's refusal redirect targets …
async def test_the_refused_cancel_redirect_lands_on_a_rendered_refusal(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """A cancel addressed at a TERMINAL run refuses (0 rows flipped) and
    redirects with the refusal's banner key; the operator must LAND on
    the rendered run page with the refusal NAMED (the jobs page's
    refused-op contract — test_attack_cancel_refusal_truth.py's shape),
    never a 400 wall."""
    schema = module_pg_schema.schema_name
    run_id = await _seed_terminal_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False
    ) as http:
        await http.get("/queues")  # the CSRF cookie
        refused = await http.post(
            f"/api/runs/{run_id}/cancel",
            data={"csrf_token": _csrf_of(http), "reason": "late stop"},
        )
        assert refused.status_code == 303, (
            f"the refused cancel must still land the operator back on the run page, "
            f"got {refused.status_code}"
        )
        location = refused.headers["location"]
        assert "error=cancel-not-applied" in location, (
            f"the redirect must carry the refusal's banner key, got {location!r}"
        )
        landed = await http.get(location)
    assert landed.status_code == 200, (
        f"the refused-cancel redirect lands on a {landed.status_code} wall — "
        "workflow_detail refuses the very 'error' query param run_cancel "
        "redirects with; the operator reads a refusal as a broken page"
    )
    text = landed.text.lower()
    assert "not applied" in text or "refused" in text, (
        "the page the operator lands on after a REFUSED run cancel renders no "
        "refusal: silence reads as success (the refused-op contract)"
    )


# ──────────────────────────────────────────────────────────────────────
# F-ADM-5 (defense-in-depth) — the node panel serves the error fields
# unbounded.
# ──────────────────────────────────────────────────────────────────────


# THE FLIP (2026-10-09): this pin XPASSed-strict on the PR head — the cure landed [F-ADM-5: the panel's read-side bound (_bound_for_panel, the shipped 10k cap with the shipped truncation marker)]; the marker is removed per the designed flip (the confirmation receipt).
async def test_the_node_panel_bounds_the_error_fields_it_serves(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    wf_conn: asyncpg.Connection,
) -> None:
    """A node row storing 200k chars in EACH error field must serve
    BOUNDED text with an operator-visible truncation marker — the
    render-side bound holds even when the stored row is huge (the
    write-side bound is a separate pin's business; the panel is the
    defense-in-depth face)."""
    schema = module_pg_schema.schema_name
    flow_id = await seed_flow(wf_conn, schema, status="failed")
    await wf_conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, attempt, step_key, metadata, error_class, "
        "error_message, error_traceback, finished_at) "
        "VALUES ($1, 'wf', 'default', '{}', 1, 'transient', 'failed', 1, 'big', "
        "$2::jsonb, 'AttackError', $3, $4, clock_timestamp())",
        new_uuid(),
        json.dumps({"flow_id": str(flow_id), "error": "E" * _HUGE_CHARS}),
        "M" * _HUGE_CHARS,
        "T" * _HUGE_CHARS,
    )
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        resp = await http.get(f"/api/runs/{flow_id}/nodes/big")
    assert resp.status_code == 200
    body = resp.json()
    for field, stored in (
        ("error_message", "M" * _HUGE_CHARS),
        ("error_traceback", "T" * _HUGE_CHARS),
        ("captured_error", "E" * _HUGE_CHARS),
    ):
        value = body[field]
        assert value is not None and len(value) <= _PANEL_FIELD_BOUND, (
            f"the panel serves {field} unbounded: {len(value) if value is not None else None} "
            f"chars of the stored {len(stored)} cross to the operator — the estate's "
            "display-cap precedent (a bounded render + a truncation marker) never "
            "reached the node panel"
        )
        # THE MARKER'S SHIPPED VOCABULARY: the panel's own
        # '[truncated: +N characters stay in the row]' suffix (Q5's cure),
        # or the estate's older '... (N more characters)' shape — a silent
        # cut reads as the whole value.
        assert (
            "[truncated:" in value
            or "more characters" in value
            or value.endswith("...")
            or value.endswith("…")
        ), (
            f"the panel's {field} truncation must be VISIBLE (a truncation marker — "
            "the shipped '[truncated: …]' shape or the estate's '... (N more "
            "characters)' shape) — a silent cut reads as the whole value"
        )


# ──────────────────────────────────────────────────────────────────────
# The GREEN guards — attacks the front verified SAFE; encoded so the
# safe behavior can never rot silently.
# ──────────────────────────────────────────────────────────────────────


_HOSTILE_KEY = "x</script><script>alert(1)</script>"


async def test_the_boot_json_escapes_a_hostile_step_key(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    wf_conn: asyncpg.Connection,
) -> None:
    """GUARD-1 (verified safe @af1b8779): a step key carrying a
    ``</script>`` breakout renders INERT — Jinja's ``tojson`` escapes
    the boot JSON's markup bytes, so the key is NAMED in the graph data
    (fidelity) while the wire never carries the raw breakout (the XSS
    stays shut)."""
    schema = module_pg_schema.schema_name
    flow_id = await seed_flow(wf_conn, schema)  # a live root
    await seed_running_node(wf_conn, schema, flow_id, step_key=_HOSTILE_KEY)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        resp = await http.get(f"/workflows/{flow_id}")
    assert resp.status_code == 200
    html = resp.text
    assert _HOSTILE_KEY not in html, (
        "the hostile step key reached the wire RAW — the boot JSON stopped "
        "escaping and the page's <script> block is an XSS door"
    )
    # The extraction itself is teeth: a literal </script> inside the JSON
    # would truncate the block and json.loads would raise.
    boot_text = html.split('<script id="wf-boot" type="application/json">')[1].split("</script>")[0]
    boot = json.loads(boot_text)
    assert _HOSTILE_KEY in boot["mermaid"], (
        "the graph must still NAME the node (data fidelity) — the escape is "
        "the wire encoding, not a dropped row"
    )
    assert "\\u003c/script\\u003e" in boot_text, (
        "the escape's own shape (tojson's \\u003c) is gone from the wire"
    )


_NEVER_MODULE = "att_adm_never_importable_zzz"


async def test_the_run_page_renders_a_run_stamped_with_a_never_importable_module(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    wf_conn: asyncpg.Connection,
) -> None:
    """GUARD-2 (verified safe @af1b8779): the graph is built FROM THE
    ROWS — a run whose ``metadata.workflow`` stamp names a module that
    does not exist ANYWHERE renders fully (200); the render never
    imports the workflow's module (an import attempt would raise
    ModuleNotFoundError and 500 the page)."""
    schema = module_pg_schema.schema_name
    assert _NEVER_MODULE not in sys.modules
    flow_id = await seed_flow(wf_conn, schema, status="running", workflow=_NEVER_MODULE)
    parent = await _seed_node(wf_conn, schema, flow_id, step_key="a", status="succeeded")
    child = await _seed_node(wf_conn, schema, flow_id, step_key="b", status="pending", deps=1)
    await seed_edge(wf_conn, schema, child, parent, flow_id)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        resp = await http.get(f"/workflows/{flow_id}")
    assert resp.status_code == 200, (
        f"the run page must render from the rows alone, got {resp.status_code}"
    )
    boot = json.loads(
        resp.text.split('<script id="wf-boot" type="application/json">')[1].split("</script>")[0]
    )
    assert "a --> b" in boot["mermaid"], (
        f"the graph is the ROWS' emission (the edge ledger), got:\n{boot['mermaid']}"
    )
    assert _NEVER_MODULE in resp.text, "the workflow stamp renders as TEXT"
    assert _NEVER_MODULE not in sys.modules, "the render IMPORTED the workflow's module"


async def test_the_node_panel_binds_the_node_key_as_a_value(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    wf_conn: asyncpg.Connection,
) -> None:
    """GUARD-3 (verified safe @af1b8779): a node key carrying a SQLi
    payload (``x' OR '1'='1' --``) answers 404 — the key binds as a
    VALUE ($1), never interpolates into the predicate. A 200 here means
    the WHERE clause was string-built and the injection matched."""
    schema = module_pg_schema.schema_name
    flow_id = await seed_flow(wf_conn, schema)
    await _seed_node(wf_conn, schema, flow_id, step_key="innocent", status="succeeded")
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as http:
        control = await http.get(f"/api/runs/{flow_id}/nodes/innocent")
        attack = await http.get(f"/api/runs/{flow_id}/nodes/x' OR '1'='1' --")
    assert control.status_code == 200, "the control: the real node key resolves"
    assert attack.status_code == 404, (
        f"the node key must bind as a VALUE: got {attack.status_code} — a 200 "
        "says the predicate interpolated the key and the injection matched rows"
    )
