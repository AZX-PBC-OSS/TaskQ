"""THE ROUTE-SOURCE WEDGE — the live repro (CURE 1's red receipt).

cache=True on a typed-route source: run 1 stores; run 2 HITS and the hit
path never rebuilds the route fork (the arm dispatch reads the element's
RUNTIME type — the cached envelope carries only encoded dicts) — the
route children never spawn, the join never fires, and the run WEDGES
FOREVER with the source row reading `succeeded` wearing a wf_cache_hit
receipt.
"""

from __future__ import annotations

import asyncio
import json

import asyncpg
import taskq.migrate
from pydantic import BaseModel

from taskq.workflows import Promise, StepContext, WorkflowApp, build, route, run, step

DSN = "postgresql://postgres:taskq@localhost:5810/taskq"
SCHEMA = "route_wedge_repro"


class ImageItem(BaseModel):
    doc_id: str


class AudioItem(BaseModel):
    doc_id: str


class ImageResult(BaseModel):
    doc_id: str


class AudioResult(BaseModel):
    doc_id: str


CALLS = {"source": 0, "image": 0, "audio": 0, "collect": 0}


async def media_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
    CALLS["source"] += 1
    return [ImageItem(doc_id="d1"), AudioItem(doc_id="d2")]


async def process_image(ctx: StepContext, item: ImageItem) -> ImageResult:
    CALLS["image"] += 1
    return ImageResult(doc_id=item.doc_id)


async def process_audio(ctx: StepContext, item: AudioItem) -> AudioResult:
    CALLS["audio"] += 1
    return AudioResult(doc_id=item.doc_id)


async def collect(ctx: StepContext, results: list[object]) -> list[object]:
    CALLS["collect"] += 1
    return results


app = WorkflowApp()


@app.workflow("route_cache_flow")
def flow() -> Promise[object]:
    src = step(media_source, key="source", cache=True)
    routed = route(src, {ImageItem: process_image, AudioItem: process_audio})
    return build(step(collect, routed, key="collect"))


async def snapshot(conn: asyncpg.Connection, flow_id: str) -> dict[str, object]:
    rows = await conn.fetch(
        f'SELECT step_key, status::text AS status, metadata FROM "{SCHEMA}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1 ORDER BY step_key",
        flow_id,
    )
    return {
        r["step_key"]: {
            "status": r["status"],
            "cache_hit": json.loads(r["metadata"]).get("wf_cache_hit") is not None
            if r["metadata"]
            else False,
        }
        for r in rows
    }


async def main() -> None:
    conn = await asyncpg.connect(DSN)
    await conn.execute(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
    await conn.close()
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="pre")
    await taskq.migrate.apply_pending_locked(DSN, schema=SCHEMA, phase="post")
    pool = await asyncpg.create_pool(DSN)

    print("=== RUN 1 (the MISS — the body pays, the fork spawns) ===")
    r1 = await run(app.get("route_cache_flow"), pool, SCHEMA, input=None, key="wedge:1")
    conn = await asyncpg.connect(DSN)
    print("status:", await _flow_status(conn, str(r1.flow_id)))
    for k, v in (await snapshot(conn, str(r1.flow_id))).items():
        print(" ", k, v)
    print("calls:", CALLS)

    CALLS.update({"source": 0, "image": 0, "audio": 0, "collect": 0})
    print("=== RUN 2 (the HIT — the wedge's seat) ===")
    from taskq.workflows import FlowRunner

    runner = FlowRunner(app.get("route_cache_flow"), pool, SCHEMA)
    claim = await runner.create_flow(input=None, run_key="wedge:2")
    try:
        await asyncio.wait_for(runner.drive(claim.flow_id), timeout=15)
        print("run returned — status:", await _flow_status(conn, str(claim.flow_id)))
    except TimeoutError:
        print(
            "THE WEDGE: the run did NOT terminalize in 15s "
            "(the collect row's reserved dep on the join never released)"
        )
    print("status:", await _flow_status(conn, str(claim.flow_id)))
    for k, v in (await snapshot(conn, str(claim.flow_id))).items():
        print(" ", k, v)
    print("calls:", CALLS)

    await pool.close()
    await conn.close()


async def _flow_status(conn: asyncpg.Connection, flow_id: str) -> str:
    return await conn.fetchval(
        f'SELECT status::text FROM "{SCHEMA}".jobs WHERE id = $1', flow_id
    )


asyncio.run(main())
