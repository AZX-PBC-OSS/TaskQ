"""THE FACES DRILL — the progress + admin + success faces:

* the PUSHED event stream captured live (the admin run page's SSE
  transport — revisioned full snapshots, mid-run AND terminal);
* the admin view mid-run + terminal: the runs list, the run pages, the
  child flows VISIBLE to the operator (the parent↔child links are
  rows — the app's own operator join);
* the typed success receipt (the decoded digest)."""

from __future__ import annotations

import asyncio
import contextlib
import json
import secrets
from collections.abc import AsyncIterator, Callable
from typing import Any

import asyncpg
import pytest

from examples.docshelf import store
from examples.docshelf.admin import build_admin_app
from examples.docshelf.app import SHELF_APP, deliver_batch
from examples.docshelf.models import DeliveryBatch, DocPayload, ShelfDigest
from examples.docshelf.tests.conftest import DOCSHELF_DSN, await_terminal, make_docs
from taskq.workflows import FlowRunner


async def _drain_sse(
    lines: AsyncIterator[str], sink: list[dict[str, Any]], stop: asyncio.Event
) -> None:
    """Collect SSE frames until the stop event (the stream's pushed
    frames: revisioned full snapshots)."""
    async for line in lines:
        if stop.is_set():
            return
        line = line.strip()
        if line.startswith("data:"):
            with contextlib.suppress(json.JSONDecodeError):
                sink.append(json.loads(line[5:].strip()))


@pytest.mark.timeout(280)
async def test_progress_admin_success_faces(
    pool: asyncpg.Pool,
    schema: str,
    workers: Any,
    run_receipt: Callable[[str, dict[str, Any]], None],
) -> None:
    docs: list[DocPayload] = make_docs(20, prefix="face")
    delivery_id = f"face-{secrets.token_hex(4)}"

    flow_ids = await deliver_batch(pool, schema, DeliveryBatch(delivery_id=delivery_id, docs=docs))
    assert len(flow_ids) == 1
    flow_id = flow_ids[0]
    await workers.spawn_ready(pool, tag="face-a", max_concurrency=8)

    # THE PUSHED EVENT STREAM, captured LIVE mid-run: the admin's run
    # SSE (revisioned full snapshots). The capture starts BEFORE the
    # workers touch the run and drains until the terminal.
    admin = build_admin_app(DOCSHELF_DSN, schema, auth=False)

    import socket

    import httpx
    import uvicorn

    # A real server (the SSE streams are unbounded — the ASGI test
    # transport would buffer them forever; a socket streams honestly).
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    config = uvicorn.Config(admin, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.1)
    assert server.started, "the admin sidecar never started"

    base = f"http://127.0.0.1:{port}"
    frames: list[dict[str, Any]] = []
    stop = asyncio.Event()

    try:
        async with (
            # S113 Why: an unbounded READ is the SSE contract (the
            # stream never ends); the connect/other budgets stay bounded.
            httpx.AsyncClient(base_url=base, timeout=httpx.Timeout(600.0, read=None)) as client,
            client.stream("GET", f"/admin/api/runs/{flow_id}/stream") as response,
        ):
            assert response.status_code == 200, (
                f"the run's SSE stream refused: {response.status_code}"
            )
            collector = asyncio.create_task(_drain_sse(response.aiter_lines(), frames, stop))

            status = await await_terminal(pool, schema, flow_id, timeout_s=200)
            await asyncio.sleep(2.0)
            stop.set()
            collector.cancel()
            import contextlib

            with contextlib.suppress(asyncio.CancelledError):
                await collector

        assert status == "succeeded"

        # THE FRAMES: at least one MID-RUN revision + a terminal revision.
        assert frames, "the SSE stream pushed nothing"
        terminal_frames = [f for f in frames if f.get("status") in ("succeeded", "failed")]

        # THE ADMIN VIEW, terminal: the run page renders FROM THE ROWS.
        async with httpx.AsyncClient(base_url=base) as client:
            page = await client.get(f"/admin/workflows/{flow_id}")
            assert page.status_code == 200, "the run explorer never rendered the run"

            runs_list = await client.get("/admin/workflows")
            assert runs_list.status_code == 200

        # THE CHILD FLOWS, VISIBLE TO THE OPERATOR: the parent↔child links
        # are ROWS (the run-key convention makes the runs findable; the
        # link rows join them). Each child run ALSO renders on the explorer.
        links = await store.child_links(pool, schema)
        mine = [lnk for lnk in links if lnk["parent_run_id"] == flow_id]
        assert mine, "no child flows were linked for this run"
        async with httpx.AsyncClient(base_url=base) as client:
            for link in mine:
                child_page = await client.get(f"/admin/workflows/{link['child_run_id']}")
                assert child_page.status_code == 200, (
                    f"the child run {link['child_run_id']} never rendered on the explorer"
                )
    finally:
        server.should_exit = True
        import contextlib

        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.wait_for(server_task, timeout=10)

    # THE TYPED SUCCESS RECEIPT.
    runner = FlowRunner(SHELF_APP.get("docshelf_batch"), pool, schema)
    digest = ShelfDigest.model_validate(await runner.result(flow_id))
    assert digest.delivery_id == delivery_id
    everything = digest.contracts_published + digest.ledger_entries + digest.quarantined
    assert sorted(everything) == sorted(d.doc_id for d in docs)

    run_receipt(
        "faces-receipt",
        {
            "flow_id": flow_id,
            "sse_frames_captured": len(frames),
            "sse_terminal_frames": len(terminal_frames),
            "child_flows_linked": len(mine),
            "digest_total_docs": digest.total_docs,
            "child_links_note": (
                "the parent↔child link is the app's own rows "
                "(the child edge is invisible to the graph view — friction F3)"
            ),
        },
    )
