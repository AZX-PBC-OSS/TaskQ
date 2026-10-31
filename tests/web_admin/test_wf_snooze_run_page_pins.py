# ruff: noqa: S608  # Why: every schema interpolation is a fixture-derived test identifier; every value is $-bound.
"""THE SNOOZED STEP'S ADMIN FACE — the run page answers "why is this step
sitting here".

The wf snooze/RetryAfter disposition (``tests/test_wf_snooze_pins.py``)
lands the node row ``scheduled`` at the delay with the deferral stamp on
the row's metadata; the run page's usefulness bar: the operator SEES the
snoozed state where the run's steps show — the signal that deferred it,
the NEXT FIRE TIME, and the node's deferral count — rendered through the
page's existing patterns (the status chip's cell, the SSE frame's live
patch). A step that never deferred renders no line (the defined absent
face), and a node that leaves the snoozed state loses the line (the
frame's label is the row's truth, never a stale decoration).
"""

from __future__ import annotations

import asyncio
import json
import sys
import types
from collections.abc import Iterator
from datetime import timedelta
from typing import Any

import asyncpg
import httpx
import pytest
from pydantic import BaseModel

pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
pytest.importorskip("jinja2")

from taskq.exceptions import Snooze
from taskq.testing.fixtures import ModulePgSchema
from taskq.web.admin import create_router, setup_admin_state
from taskq.workflows import FlowRunner, Promise, WorkflowApp, build, step

pytestmark = pytest.mark.integration

MODULE_NAME = "wf_snooze_admin_flows"

FLOOR_S = 1.0

#: The bodies' per-run snooze ledger (the MODULE-LEVEL bodies are the D1
#: registry's own law — the re-registration compares the body map by
#: identity, so per-fixture closures are the duplicate-refusal shape).
#: Keyed by the node row's own id: every run mints fresh rows, so each
#: run snoozes exactly once and no test's state can bleed into another.
_SNOOZED_ONCE: set[str] = set()


class Ingest(BaseModel):
    doc_id: str


async def _snoozing(ctx: Any, params: Ingest) -> str:
    """The page's subject: snoozes ONCE per run (keyed by the node row's
    own id), then succeeds."""
    key = str(ctx.job_id)
    if key not in _SNOOZED_ONCE:
        _SNOOZED_ONCE.add(key)
        raise Snooze(timedelta(seconds=FLOOR_S))
    return "done"


async def _plain(ctx: Any, params: Ingest) -> str:
    return "plain"


async def _join(ctx: Any, a: str, b: str) -> str:
    return "joined"


@pytest.fixture
def demo_app_module() -> Iterator[types.ModuleType]:
    """The registered definitions: one step that snoozes once (the page's
    subject), one that never defers (the absent face)."""
    module = types.ModuleType(MODULE_NAME)
    app_obj = WorkflowApp()

    @app_obj.workflow("snooze_page_flow")
    def snooze_page() -> Promise[object]:
        snoozing = step(_snoozing, Ingest(doc_id="d1"), key="snoozing")
        plain = step(_plain, Ingest(doc_id="d1"), key="plain")
        return build(step(_join, snoozing, plain, key="tail"))

    module.app = app_obj  # type: ignore[attr-defined]
    sys.modules[MODULE_NAME] = module
    yield module
    del sys.modules[MODULE_NAME]


def _make_admin_app(pool: asyncpg.Pool, schema: str) -> Any:
    bundle = create_router(pool, schema=schema, base_path="")
    app = _bundle_app(bundle)
    return app


def _bundle_app(bundle: Any) -> Any:
    from fastapi import FastAPI

    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return app


async def _seed_snoozed_run(pool: asyncpg.Pool, schema: str) -> str:
    """A REAL run parked mid-snooze: the first tick claims + executes the
    source's children — the snoozing step's deferral lands, the row rests
    ``scheduled`` at the floor's delay."""
    compiled = sys.modules[MODULE_NAME].app.get("snooze_page_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, pool, schema)
    flow_id = str((await runner.create_flow(input=Ingest(doc_id="d1"))).flow_id)
    assert await runner.tick(flow_id)
    row = await pool.fetchrow(
        f"SELECT status FROM \"{schema}\".jobs WHERE (metadata->>'flow_id')::uuid = $1 "
        "AND step_key = 'snoozing'",
        __import__("uuid").UUID(flow_id),
    )
    assert row is not None and row["status"] == "scheduled", row
    return flow_id


async def test_run_page_renders_the_snoozed_state(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE USEFULNESS PIN: the run page's node table renders the snoozed
    step's deferral line — the signal, the next fire time, the deferral
    count — so the operator knows WHY the step is sitting there; a step
    that never deferred renders no line."""
    schema = module_pg_schema.schema_name
    flow_id = await _seed_snoozed_run(module_pg_pool, schema)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/workflows/{flow_id}")
    assert resp.status_code == 200
    html = resp.text
    # THE DEFERRAL LINE: the signal + the next fire + the count, on the
    # snoozing step's own row.
    snooze_row = html.split('data-wf-node-row="snoozing"')[1].split("</tr>")[0]
    assert "data-wf-node-deferral" in snooze_row, (
        "the snoozed step's row must render the deferral line (the "
        "operator's 'why is this step sitting here' answer)"
    )
    assert "Snooze" in snooze_row
    assert "snooze #" in snooze_row
    assert "next fire" in snooze_row
    # THE ABSENT FACE: a step that never deferred renders no line.
    plain_row = html.split('data-wf-node-row="plain"')[1].split("</tr>")[0]
    assert "data-wf-node-deferral" not in plain_row


async def test_sse_frame_carries_the_deferral_label(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE LIVE PATCH'S DATA: the SSE snapshot's node entries carry the
    deferral label (the JS paints/clears the line from the frame — the
    label is the row's truth, never a stale decoration); the frame for a
    step that never deferred carries none."""
    schema = module_pg_schema.schema_name
    flow_id = await _seed_snoozed_run(module_pg_pool, schema)
    from taskq.web.admin._wf_actions import _stream_generator

    pool = await asyncpg.create_pool(module_pg_schema.pg_dsn, min_size=1, max_size=2)
    try:
        gen = _stream_generator(pool, schema, __import__("uuid").UUID(flow_id), 0)
        raw = await asyncio.wait_for(gen.__anext__(), timeout=10.0)
        await gen.aclose()
    finally:
        await pool.close()
    frames = [json.loads(line[6:]) for line in raw.splitlines() if line.startswith("data: ")]
    assert frames, "the first frame must arrive immediately"
    nodes = {n["key"]: n for n in frames[0]["nodes"]}
    assert nodes["snoozing"]["deferral_label"] is not None, (
        "the snoozed step's frame carries the deferral label"
    )
    assert "Snooze" in nodes["snoozing"]["deferral_label"]
    assert nodes["plain"]["deferral_label"] is None


async def test_the_deferral_line_clears_when_the_step_moves_on(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE ROW'S TRUTH: past the delay the step re-claims and terminalizes
    — the deferral line is gone from the page (the label reads the row,
    and the row no longer sits)."""
    schema = module_pg_schema.schema_name
    flow_id = await _seed_snoozed_run(module_pg_pool, schema)
    compiled = sys.modules[MODULE_NAME].app.get("snooze_page_flow")  # type: ignore[attr-defined]
    runner = FlowRunner(compiled, module_pg_pool, schema)
    await asyncio.sleep(FLOOR_S + 0.5)
    for _ in range(30):
        status = await module_pg_pool.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1',
            __import__("uuid").UUID(flow_id),
        )
        if status in ("succeeded", "failed", "cancelled"):
            break
        await runner.tick(flow_id)
        await asyncio.sleep(0.2)
    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(f"/workflows/{flow_id}")
    assert resp.status_code == 200
    snooze_row = resp.text.split('data-wf-node-row="snoozing"')[1].split("</tr>")[0]
    assert "data-wf-node-deferral" not in snooze_row, (
        "the deferral line describes the SITTING state — a step that moved on must not keep it"
    )


def test_the_deferral_label_bounds_a_hostile_signal_stamp() -> None:
    """THE LABEL'S BOUND (C3 — H-C9's law at the deferral's face): the
    stamp's ``signal`` is row-sourced free text (the jsonb boundary's
    other side) — a hostile 10KB signal renders TRUNCATED through the
    node panel's own ``_truncate_error_text`` limit, never a 10KB line
    into the page's DOM and every SSE frame."""
    from taskq.web.admin._wf_rows import RunNode

    node = RunNode(
        key="solo",
        status="scheduled",
        attempt=0,
        snooze_count=1,
        deferral={"signal": "X" * 10240, "consume_budget": False, "attempt": 0},
    )
    label = node.deferral_label
    assert label is not None, "a sitting deferral renders its label"
    assert len(label) < 2500, (
        f"the hostile 10KB signal must render truncated (the error-text "
        f"bound), got a {len(label)}-character label"
    )
    assert "more characters" in label, (
        "the truncation is HONEST — the marker counts what the label no "
        "longer spells (the panel's own convention)"
    )
    # THE CLEAN FACE UNCHANGED: a normal signal renders whole.
    clean = RunNode(
        key="solo",
        status="scheduled",
        attempt=0,
        snooze_count=1,
        deferral={"signal": "Snooze", "consume_budget": False, "attempt": 0},
    )
    clean_label = clean.deferral_label
    assert clean_label is not None
    assert "Snooze" in clean_label and "more characters" not in clean_label


async def test_the_runs_list_exposes_a_pathologically_deferring_run(
    module_pg_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    demo_app_module: Any,
) -> None:
    """THE RUN-LEVEL VISIBILITY (C4): the runs LIST carries the deferral
    state at the RUN level — a pathologically deferring run is visible
    from the inventory (the badge: the sitting nodes' count, the next
    fire, the deferral depth — the row's own aggregated truth), never
    only after opening the run; a clean run renders no badge."""
    schema = module_pg_schema.schema_name
    flow_id = await _seed_snoozed_run(module_pg_pool, schema)
    # THE CLEAN RUN beside it: the same flow driven to terminal (the
    # snoozing step defers once, then completes) — no badge.
    clean_compiled = sys.modules[MODULE_NAME].app.get("snooze_page_flow")  # type: ignore[attr-defined]
    clean_runner = FlowRunner(clean_compiled, module_pg_pool, schema)
    clean_id = str((await clean_runner.create_flow(input=Ingest(doc_id="d2"))).flow_id)
    await asyncio.sleep(FLOOR_S + 0.5)
    status: str | None = "running"
    for _ in range(30):
        status = await module_pg_pool.fetchval(
            f'SELECT status FROM "{schema}".jobs WHERE id = $1',
            __import__("uuid").UUID(clean_id),
        )
        if status in ("succeeded", "failed", "cancelled"):
            break
        await clean_runner.tick(clean_id)
        await asyncio.sleep(0.2)
    assert status == "succeeded", status

    app = _make_admin_app(module_pg_pool, schema)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/workflows")
    assert resp.status_code == 200
    html = resp.text
    # THE DEFERRING RUN'S ROW: the badge with the sitting count, the next
    # fire, and the depth — the page's own badge patterns.
    deferring_row = next(
        (chunk for chunk in html.split("<tr>") if f"/workflows/{flow_id}" in chunk), ""
    )
    assert "data-wf-run-deferring" in deferring_row, (
        "the pathologically deferring run must be visible from the runs "
        "list (the run-level deferral badge)"
    )
    assert "1 deferred" in deferring_row, (
        f"the badge names the sitting nodes' count, got: {deferring_row[:300]}"
    )
    assert "next fire" in deferring_row
    assert "snooze #" in deferring_row
    # THE CLEAN RUN'S ROW: no badge (the defined absent face).
    clean_row = next(
        (chunk for chunk in html.split("<tr>") if f"/workflows/{clean_id}" in chunk), ""
    )
    assert "data-wf-run-deferring" not in clean_row, (
        "a run with no sitting deferral renders no badge"
    )
