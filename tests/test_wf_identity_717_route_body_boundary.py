"""Regression pin for the closed F1 finding: the route body path performs no
identity catalog read.

A route's binding limit is the join counter (``jobs.deps_pending``), refused
deterministically by ``validate_fork`` inside the finalize's machinery boundary.
Reading the identity domain on the body path (the prior defect) put a runner
catalog statement inside the body failure boundary: a transport fault there was
recorded and terminalized as a BODY failure and the retry ladder burned.

This pin injects the DB boundary's transport class at the identity resolver and
proves the route path never calls it: the oversized route still refuses on the
first attempt with the named count class, writes no child, and no failure is
attributed to the transport fault. A future re-introduction of a body-path
catalog read turns this red.

Real PostgreSQL; the house ``wf_*`` fixtures.
"""

# ruff: noqa: S608  # Why: the fixture schema identifier is interpolated; every value is $-bound.

from __future__ import annotations

from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    WorkflowApp,
    build,
    route,
    step,
)

pytestmark = pytest.mark.integration

#: One past the join counter (``jobs.deps_pending`` is a smallint).
ELEMENTS = 32769


class ImageItem(BaseModel):
    doc_id: str
    uri: str


class AudioItem(BaseModel):
    doc_id: str
    uri: str


async def test_route_over_count_never_reads_the_identity_domain_on_the_body_path(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import taskq.workflows._types as types_mod

    async def process_image(ctx: Any, item: ImageItem) -> str:
        del ctx
        return item.doc_id

    async def process_audio(ctx: Any, item: AudioItem) -> str:
        del ctx
        return item.doc_id

    async def big_route_source(ctx: Any) -> list[ImageItem | AudioItem]:
        del ctx
        return [ImageItem(doc_id=str(i), uri="u") for i in range(ELEMENTS)]

    app = WorkflowApp()

    @app.workflow("route_body_boundary")
    def flow() -> Promise[object]:
        src = step(big_route_source, key="media")
        return build(
            route(
                src,
                {
                    ImageItem: RouteArm(body=process_image),
                    AudioItem: RouteArm(body=process_audio),
                },
            )
        )

    calls: list[int] = []

    async def _lost(_conn: object, _schema: str) -> int:
        calls.append(1)
        raise ConnectionError("connection lost during the identity catalog read")

    monkeypatch.setattr(types_mod, "resolve_map_index_domain", _lost)

    runner = FlowRunner(app.get("route_body_boundary"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    verdict = await runner.drive(flow_id)
    assert verdict == "terminal", verdict
    assert calls == [], "the route body path read the identity catalog"

    node = await wf_conn.fetchrow(
        f"""SELECT id, status, attempt, error_class FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'media'""",
        flow_id,
    )
    assert node is not None and node["status"] == "failed", dict(node) if node else None
    assert node["error_class"] == "FanInBoundExceededError", dict(node) if node else None
    assert int(node["attempt"]) == 1, dict(node) if node else None
    children = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs WHERE parent_id = $1', node["id"]
    )
    assert int(children or 0) == 0, f"the refused route wrote {children} child rows"
