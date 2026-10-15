"""THE FACES: the pushed progress stream captured by a listener; the
admin/display API's view mid-run AND at terminal; the typed success
receipt."""

from __future__ import annotations

import asyncio
import contextlib

import asyncpg
import pytest

from examples.ledgerwatch.client import await_terminal, submit_batch
from examples.ledgerwatch.display import admin_app, operator_view
from examples.ledgerwatch.feed import BatchSpec, build_batch
from examples.ledgerwatch.models import CompletionReport
from examples.ledgerwatch.tests.support import TEST_DSN, Fleet, wait_for
from taskq.workflows import FlowRunner, ProgressUpdated

pytestmark = pytest.mark.timeout(300)

FLEET_ENV: dict[str, str] = {"LW_PROFILE": "adverse"}


async def test_progress_stream_push_and_display_faces(
    fleet: Fleet, pool: asyncpg.Pool, schema: str
) -> None:
    batch = build_batch(BatchSpec(batch_id="B-faces", count=120, high_every=0))
    claim = await submit_batch(pool, schema, batch)
    assert claim.kind == "created"
    flow_id = str(claim.flow_id)

    # THE PUSH FACE: a listener captures the typed stream while the run
    # is live — on ITS OWN pool (a listener owns one dedicated
    # connection for its life; isolating it keeps the drill's shared
    # pool clean under cancellation).
    events: list[ProgressUpdated | object] = []
    capture = asyncio.create_task(_capture_on_own_pool(schema, flow_id, events))

    # THE DISPLAY FACE, MID-RUN: the ledger-derived view shows the run
    # in flight.
    runner = _runner(pool, schema)
    mid = await _mid_run_view(pool, runner, flow_id)
    assert mid, "the mid-run display was empty"
    assert any("ingest" in key for key in mid), (
        f"the mid-run display lacks the ingest node: {sorted(mid)[:5]}"
    )

    raw = await await_terminal(pool, schema, flow_id, timeout_s=180)
    report = CompletionReport.model_validate(raw)
    assert report.failed == []  # the typed receipt's integrity

    # THE LISTENER SAW THE RUN: the closed union's Updated events
    # arrived while the run was live (the push transport).
    await wait_for(
        lambda: len(events) > 0,
        timeout_s=30,
        interval=0.2,
        message="the progress listener captured nothing",
    )
    capture.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await capture
    assert any(isinstance(e, ProgressUpdated) for e in events), (
        f"the captured events carry no Update: {[type(e).__name__ for e in events[:5]]}"
    )

    # THE DISPLAY FACE, TERMINAL: every node's ledger-derived state,
    # keyed BY NAME.
    final = await operator_view(pool, runner.wsql, flow_id)
    assert final["ingest"]["status"] == "succeeded"
    assert final["report"]["status"] == "succeeded"
    children = [k for k in final if k.startswith("ingest.item[")]
    assert len(children) == 120, f"the display's collect: {len(children)} children"

    # THE STREAM CHANNEL: the bodies' own emissions are ON THE RECORD
    # (the ring): the enrich children's messages ride the payload.
    rows = await pool.fetch(
        f'SELECT payload FROM "{schema}".wf_node_stream'  # noqa: S608  # Why: the schema identifier only.
        " WHERE flow_id = $1 AND kind = 'progress' AND payload->>'message' LIKE 'enriching %' LIMIT 3",
        flow_id,
    )
    assert len(rows) > 0, "the enrich children's progress emissions never landed on the stream"

    # THE ADMIN FACE: the run explorer renders the run (200: the run's
    # identity + the node keys on the page).
    app = admin_app(pool, schema)
    from httpx import ASGITransport, AsyncClient

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://admin") as client:
        page = await client.get(f"/admin/workflows/{flow_id}")
        assert page.status_code == 200, f"the run explorer: {page.status_code}"
        assert "ingest" in page.text and flow_id[:8] in page.text, (
            "the run explorer's page does not render the run"
        )
        listing = await client.get("/admin/workflows")
        assert listing.status_code == 200


async def _capture_on_own_pool(schema: str, flow_id: str, events: list[object]) -> None:
    """The listener's own pool: its dedicated connection never touches
    the drill's shared pool."""
    import asyncpg as _pg

    from examples.ledgerwatch.display import stream_progress

    own = await _pg.create_pool(TEST_DSN, min_size=1, max_size=2)
    try:
        await stream_progress(own, schema, flow_id, events)
    finally:
        await own.close()


def _runner(pool: asyncpg.Pool, schema: str) -> FlowRunner:
    from examples.ledgerwatch.app import app

    return FlowRunner(app.get("ledger_watch"), pool, schema)


async def _mid_run_view(
    pool: asyncpg.Pool, runner: FlowRunner, flow_id: str
) -> dict[str, dict[str, object]]:
    async def _ready() -> dict[str, dict[str, object]] | None:
        from examples.ledgerwatch.display import operator_view

        view = await operator_view(pool, runner.wsql, flow_id)
        return view if view else None

    return await wait_for(_ready, timeout_s=60, message="the display never rendered the run")
