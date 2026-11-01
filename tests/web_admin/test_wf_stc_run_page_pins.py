# Why: every schema interpolation is a fixture-derived test identifier; every value is $-bound.
"""THE STEP DEADLINE'S ADMIN FACE — the run page shows the bound where
declared, and the deadline's reality for a RUNNING node.

The wf start_to_close cure (``tests/test_wf_start_to_close_pins.py``)
stamps the row's EXISTING ``jobs.start_to_close`` column at the node
INSERTs and enforces it at the runner's body seam; the run page's
usefulness bar: the operator SEES the bound on the node panel — the
declared deadline, and for a RUNNING node whose claim instant is
available the deadline's REALITY (the instant + the bound, with the time
left or the expiry's age) — through the page's own patterns (the status
chip's cell, the SSE frame's live patch). A node whose row declares no
bound renders no line (the defined absent face).
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
import uuid as _uuid
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import asyncpg
import httpx
import pytest
from pydantic import BaseModel

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")

from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.workflows import FlowRunner, Promise, WorkflowApp, build, step

pytestmark = pytest.mark.integration

MODULE_NAME = "wf_stc_admin_flows"


class Ingest(BaseModel):
    doc_id: str


async def _slow_ok(ctx: Any, params: Ingest) -> str:
    """The page's subject: slow enough that a 30s-bound step is observed
    RUNNING under its declared deadline."""
    await asyncio.sleep(0.05)
    return "done"


async def _plain(ctx: Any, params: Ingest) -> str:
    return "plain"


@pytest.fixture
def demo_app_module() -> Iterator[types.ModuleType]:
    """The registered definitions: one step carrying a declared bound
    (the page's subject), one carrying none (the absent face)."""
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("stc_page_flow")
    def stc_page() -> Promise[object]:
        declared = step(
            _slow_ok,
            Ingest(doc_id="d1"),
            key="declared",
            start_to_close=timedelta(seconds=30),
        )
        plain = step(_plain, Ingest(doc_id="d1"), key="plain")
        return build(declared, plain)

    module.app = app_obj  # type: ignore[attr-defined]
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


def _make_admin_app(pool: asyncpg.Pool, schema: str) -> Any:
    bundle = create_router(pool, schema=schema, base_path="")
    from fastapi import FastAPI

    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


async def _seed_running_run(pool: asyncpg.Pool, schema: str) -> str:
    """A REAL run observed mid-flight: the first tick claims the
    declared step (its body 0.05s — the drive returns with the run
    terminal, so the pin drives a SECOND run and reads the RUNNING
    face from the label's unit level instead)."""
    compiled = sys.modules[MODULE_NAME].app.get("stc_page_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, pool, schema)
    return str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)


async def test_run_page_renders_the_declared_bound(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE USEFULNESS PIN: the run page's node table renders the
    declared bound on the step that carries one (``deadline 30s``), and
    a step whose row declares no bound renders NO line."""
    schema = module_pg_schema.schema_name
    flow_id = await _seed_running_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/workflows/{flow_id}")
    assert resp.status_code == 200
    html = resp.text
    # THE BOUND WHERE DECLARED: the deadline line on the declared step's
    # row (the run terminalized — the label is the bound's plain face).
    declared_row = html.split('data-wf-node-row="declared"')[1].split("</tr>")[0]
    assert "data-wf-node-deadline" in declared_row, (
        "the declared step's row must render the deadline line (the "
        "operator's 'what is this step's deadline' answer)"
    )
    assert "deadline 30s" in declared_row, declared_row
    # THE ABSENT FACE: a step whose row declares no bound renders none.
    plain_row = html.split('data-wf-node-row="plain"')[1].split("</tr>")[0]
    assert "data-wf-node-deadline" not in plain_row


async def test_the_running_face_renders_the_deadline_reality(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE DEADLINE'S REALITY (the claim's instant's use): a RUNNING
    node's label carries the time left (the claim's instant + the
    bound); a non-running node's label is the bound alone. THE CLOCK
    ARBITER (OBSERVATION-1 — the single-arbiter doctrine): the
    remaining/age is the READ's own ``clock_timestamp()`` computation
    (``RunNode.deadline_remaining_s``) — the label never measures the
    DB-written ``started_at`` against the APP clock, so a DB two minutes
    ahead of the app no longer self-contradicts (the pre-cure shape: the
    DB-written instant read against the app clock as ``expired 120s
    ago`` on a step that just started). The derivation is the view's own
    (``RunNode.deadline_label`` — one spelling, the page's and the SSE
    frame's shared form)."""
    from datetime import UTC, datetime

    from taskq.web.admin._wf_rows import RunNode

    # THE DB IS TWO MINUTES AHEAD (the skew fixture's own shape): the
    # DB-written started_at reads two minutes in the FUTURE against the
    # app clock. The remaining is the DB's OWN arithmetic — the label
    # derives from IT, never from the app clock.
    db_started = datetime.now(UTC) + timedelta(minutes=2)
    running = RunNode(
        key="n",
        status="running",
        start_to_close_s=30.0,
        started_at=db_started.isoformat(),
        deadline_remaining_s=25.0,  # the READ's own clock_timestamp() product
    )
    label = running.deadline_label
    assert label is not None and "30s" in label and "25s left" in label, label
    assert "expired" not in label, (
        f"THE CLOCK ARBITER: the label must not measure the DB-written "
        f"instant against the app clock (the 2-min-ahead skew "
        f"self-contradiction), got {label!r}"
    )
    # THE EXPIRED FACE: the READ's negative remaining names the expiry's
    # age (its own arithmetic, never the app clock's).
    expired = RunNode(
        key="n",
        status="running",
        start_to_close_s=30.0,
        started_at=db_started.isoformat(),
        deadline_remaining_s=-15.0,
    ).deadline_label
    assert expired is not None and "expired 15s ago" in expired, expired
    # THE UNREADABLE FACE: a row dict predating the column (no computed
    # remaining) degrades to the bound alone — never a 500.
    unreadable = RunNode(
        key="n",
        status="running",
        start_to_close_s=30.0,
        started_at=db_started.isoformat(),
    ).deadline_label
    assert unreadable == "deadline 30s", unreadable
    # THE BARE FACE: a non-running node renders the bound alone.
    pending = RunNode(key="n", status="pending", start_to_close_s=30.0).deadline_label
    assert pending == "deadline 30s", pending
    # THE ABSENT FACE: no bound — no line.
    assert RunNode(key="n", status="running").deadline_label is None


async def test_sse_frame_carries_the_deadline_label(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE LIVE PATCH'S DATA: the SSE snapshot's node entries carry the
    deadline label (the JS paints/clears the line from the frame — the
    same pattern the deferral line keeps); the frame for a node whose
    row declares no bound carries none."""
    schema = module_pg_schema.schema_name
    flow_id = await _seed_running_run(module_pg_pool, schema)
    from taskq.web.admin._wf_actions import _stream_generator

    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=2)
    try:
        gen = _stream_generator(pool, schema, _uuid.UUID(flow_id), 0)
        raw = await asyncio.wait_for(gen.__anext__(), timeout=10.0)
        await gen.aclose()
    finally:
        await pool.close()
    frames = [json.loads(line[6:]) for line in raw.splitlines() if line.startswith("data: ")]
    assert frames, "the first frame must arrive immediately"
    nodes = {n["key"]: n for n in frames[0]["nodes"]}
    assert nodes["declared"]["deadline_label"] is not None, (
        "the declared step's frame carries the deadline label"
    )
    assert nodes["plain"]["deadline_label"] is None
