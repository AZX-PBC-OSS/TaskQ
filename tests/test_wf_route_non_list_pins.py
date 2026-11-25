"""Real-Postgres pins for typed-route source return-shape enforcement.

A ``route(source, arms)`` source step declares ``list[UnionMember...]``:

* An empty list ``[]`` is an honest empty corpus: the fork writes a
  zero-child join that fires immediately and delivers ``[]`` downstream.
* A non-empty list of declared union members forks per-element arm jobs
  and fans back in at ``<source>.join``.
* Any non-list return value (``None`` from a forgotten ``return``, a bare
  single model instance instead of ``[model]``, or a scalar/dict) is a
  deterministic programming error: the source step must terminal-fail on
  attempt 1 with an explicit error (no transient retry loop), create no
  child or join rows, and fail the flow instead of hanging with a stuck
  downstream consumer or silently succeeding without a join.
"""

# ruff: noqa: S608  # Why: every schema interpolation is the fixture-derived per-module schema identifier; every value is $-bound.

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import cast

import asyncpg
import pytest
from opentelemetry import trace
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId
from taskq.backend._records import _job_row_from_record
from taskq.testing.otel import setup_tracer
from taskq.workflows import (
    FlowRunner,
    Promise,
    RouteArm,
    StepContext,
    WorkflowApp,
    WorkflowRunError,
    build,
    map_source,
    route,
    step,
)
from taskq.workflows._worker_execution import execute_flow_job, project_workflow_actor_configs

pytestmark = pytest.mark.integration

#: Bounded tick cap: deterministic failure completes in a handful of ticks,
#: while a missing-join wedge loops until the cap expires.
_BOUNDED_TICKS = 80


class ImageItem(BaseModel):
    doc_id: str
    uri: str


class AudioItem(BaseModel):
    doc_id: str
    uri: str


class ImageResult(BaseModel):
    doc_id: str
    ocr: str


class AudioResult(BaseModel):
    doc_id: str
    transcript: str


async def _process_image(ctx: StepContext, item: ImageItem) -> ImageResult:
    del ctx
    return ImageResult(doc_id=item.doc_id, ocr=f"ocr:{item.doc_id}")


async def _process_audio(ctx: StepContext, item: AudioItem) -> AudioResult:
    del ctx
    return AudioResult(doc_id=item.doc_id, transcript=f"tr:{item.doc_id}")


async def _consume_results(
    ctx: StepContext, items: list[ImageResult | AudioResult]
) -> dict[str, int]:
    del ctx
    return {"count": len(items)}


def _build_consumer_app(
    name: str,
    source_body: Callable[[StepContext], Awaitable[list[ImageItem | AudioItem]]],
    *,
    dict_form: bool = False,
    split_arm_queues: bool = True,
) -> WorkflowApp:
    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        src = step(source_body, key="media", max_attempts=3)
        arms = {
            ImageItem: RouteArm(
                body=_process_image, queue="gpu" if split_arm_queues else "default"
            ),
            AudioItem: RouteArm(body=_process_audio, queue="io" if split_arm_queues else "default"),
        }
        routed = map_source(src, arms) if dict_form else route(src, arms)
        consumer = step(_consume_results, routed, key="consume")
        return build(consumer)

    return app


async def _assert_deterministic_route_source_failure(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    *,
    workflow_name: str,
    bad_value: object,
    with_consumer: bool,
    dict_form: bool = False,
) -> None:
    calls: list[int] = []

    async def bad_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        calls.append(1)
        return cast("list[ImageItem | AudioItem]", bad_value)

    if with_consumer:
        app = _build_consumer_app(workflow_name, bad_source, dict_form=dict_form)
    else:
        app = WorkflowApp()

        @app.workflow(workflow_name)
        def terminal_route_flow() -> Promise[object]:
            src = step(bad_source, key="media", max_attempts=3)
            routed = route(
                src,
                {
                    ImageItem: RouteArm(body=_process_image, queue="gpu"),
                    AudioItem: RouteArm(body=_process_audio, queue="io"),
                },
            )
            return build(routed)

    runner = FlowRunner(app.get(workflow_name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    verdict = await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS)

    source_row = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'media'",
        flow_id,
    )
    root_row = await wf_conn.fetchrow(
        f'SELECT status, error_class FROM "{wf_schema}".jobs WHERE id = $1::uuid',
        flow_id,
    )
    forked_rows = await wf_conn.fetch(
        f'SELECT step_key, status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid "
        "AND (step_key LIKE 'media.item%' OR step_key = 'media.join')",
        flow_id,
    )

    assert verdict == "terminal", (
        f"route source returning {type(bad_value).__name__} wedged "
        f"(verdict={verdict!r}, root={dict(root_row) if root_row else None}, "
        f"source={dict(source_row) if source_row else None})"
    )
    assert root_row is not None and root_row["status"] == "failed", (
        f"expected flow root status='failed', got {dict(root_row) if root_row else None}"
    )
    assert source_row is not None, "source step row missing"
    assert source_row["status"] == "failed", dict(source_row)
    assert int(source_row["attempt"]) == 1, (
        f"non-list route source must fail deterministically without retrying: {dict(source_row)}"
    )
    assert calls == [1], f"source body retried unexpectedly (calls={len(calls)})"
    assert source_row["error_class"] == "RouterNotTotal", (
        f"source row must record error_class='RouterNotTotal': {dict(source_row)}"
    )
    error_message = source_row["error_message"] or ""
    assert "media" in error_message, f"error_message must name the source: {dict(source_row)}"
    assert type(bad_value).__name__ in error_message, (
        f"error_message must name the offending runtime type "
        f"{type(bad_value).__name__!r}: {dict(source_row)}"
    )
    assert not forked_rows, (
        f"refused route source must not spawn children or join: {[dict(r) for r in forked_rows]}"
    )
    with pytest.raises(WorkflowRunError, match="RouterNotTotal"):
        await runner.result(flow_id)

    if with_consumer:
        consumer_row = await wf_conn.fetchrow(
            f'SELECT status, deps_pending, finished_at FROM "{wf_schema}".jobs '
            "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'consume'",
            flow_id,
        )
        assert consumer_row is not None
        assert consumer_row["finished_at"] is None, dict(consumer_row)
        assert consumer_row["status"] != "succeeded", dict(consumer_row)


async def test_route_source_returning_none_fails_deterministically(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """A route source returning ``None`` fails deterministically on attempt 1
    and terminates the flow ``failed`` instead of wedging downstream consumers."""
    await _assert_deterministic_route_source_failure(
        wf_conn,
        wf_schema,
        wf_pool,
        workflow_name="route_non_list_none",
        bad_value=None,
        with_consumer=True,
    )


async def test_route_source_returning_bare_model_fails_deterministically(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """A route source returning a bare single model instance fails
    deterministically on attempt 1 instead of wedging downstream consumers."""
    await _assert_deterministic_route_source_failure(
        wf_conn,
        wf_schema,
        wf_pool,
        workflow_name="route_non_list_bare_model",
        bad_value=ImageItem(doc_id="d1", uri="s3://b/d1.png"),
        with_consumer=True,
    )


async def test_dict_form_route_source_returning_non_list_fails_deterministically(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The dict-form ``map_source`` spelling lowers to the same fail-closed
    route machinery instead of reviving the silent non-list wedge."""
    await _assert_deterministic_route_source_failure(
        wf_conn,
        wf_schema,
        wf_pool,
        workflow_name="route_non_list_dict_form",
        bad_value=None,
        with_consumer=True,
        dict_form=True,
    )


@pytest.mark.parametrize(
    ("label", "bad_value"),
    [
        ("dict", {"doc_id": "d1", "uri": "s3://b/d1.png"}),
        ("scalar", "not-a-list"),
    ],
)
async def test_route_source_returning_scalar_or_dict_fails_deterministically(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    label: str,
    bad_value: object,
) -> None:
    """A route source returning a non-list dict or scalar fails
    deterministically on attempt 1 instead of wedging downstream consumers."""
    await _assert_deterministic_route_source_failure(
        wf_conn,
        wf_schema,
        wf_pool,
        workflow_name=f"route_non_list_{label}",
        bad_value=bad_value,
        with_consumer=True,
    )


async def test_route_source_returning_non_list_without_consumer_fails_not_succeeds(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """When the route is directly the terminal promise (no downstream consumer),
    returning ``None`` must still fail the flow deterministically rather than
    silently finalizing ``succeeded`` with ``result() is None``."""
    await _assert_deterministic_route_source_failure(
        wf_conn,
        wf_schema,
        wf_pool,
        workflow_name="route_non_list_direct_terminal",
        bad_value=None,
        with_consumer=False,
    )


async def test_route_source_empty_list_control_succeeds(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """Control: returning ``[]`` spawns a zero-child join, delivers ``[]``
    to the downstream consumer, and terminates the flow ``succeeded``."""

    async def empty_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        return []

    app = _build_consumer_app("route_control_empty", empty_source)
    runner = FlowRunner(app.get("route_control_empty"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    verdict = await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS)

    assert verdict == "terminal", verdict
    root = await wf_conn.fetchrow(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
    )
    assert root is not None and root["status"] == "succeeded", dict(root) if root else None
    assert await runner.result(flow_id) == {"count": 0}


async def test_plain_map_non_list_control_still_uses_the_empty_join(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The untyped map sibling keeps its separate non-list-to-empty coercion;
    the typed route's new refusal must not leak into that existing contract."""

    async def none_source(ctx: StepContext) -> list[ImageItem]:
        del ctx
        return cast("list[ImageItem]", None)

    app = WorkflowApp()

    @app.workflow("plain_map_non_list_control")
    def flow() -> Promise[object]:
        src = step(none_source, key="plain_map")
        mapped = map_source(src, _process_image)
        return build(step(_consume_results, mapped, key="consume"))

    runner = FlowRunner(app.get("plain_map_non_list_control"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS) == "terminal"
    assert await runner.result(flow_id) == {"count": 0}
    join_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'plain_map.join'",
        flow_id,
    )
    assert join_status == "succeeded"


async def test_route_source_valid_union_list_control_succeeds(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """Control: returning ``[ImageItem, AudioItem]`` routes both elements,
    joins their results, and delivers them to the downstream consumer."""

    async def valid_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        return [
            ImageItem(doc_id="d1", uri="s3://b/d1.png"),
            AudioItem(doc_id="d2", uri="s3://b/d2.wav"),
        ]

    app = _build_consumer_app("route_control_valid", valid_source)
    runner = FlowRunner(app.get("route_control_valid"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    verdict = await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS)

    assert verdict == "terminal", verdict
    root = await wf_conn.fetchrow(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
    )
    assert root is not None and root["status"] == "succeeded", dict(root) if root else None
    assert await runner.result(flow_id) == {"count": 2}


async def test_non_list_diagnostic_survives_a_long_legal_step_key(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The 500-character terminal-message bound must not let a legal long key
    truncate away the offending type and the required list return shape."""

    async def bad_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        return cast("list[ImageItem | AudioItem]", None)

    long_key = "media_" + "m" * 600
    app = WorkflowApp()

    @app.workflow("route_non_list_long_key")
    def flow() -> Promise[object]:
        src = step(bad_source, key=long_key, max_attempts=3)
        return build(
            route(
                src,
                {
                    ImageItem: RouteArm(body=_process_image),
                    AudioItem: RouteArm(body=_process_audio),
                },
            )
        )

    runner = FlowRunner(app.get("route_non_list_long_key"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS) == "terminal"

    source = await wf_conn.fetchrow(
        f'SELECT status, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = $2",
        flow_id,
        long_key,
    )
    assert source is not None
    assert source["status"] == "failed" and source["error_class"] == "RouterNotTotal"
    message = source["error_message"] or ""
    assert "NoneType" in message, message
    assert "list[...]" in message, message


async def test_no_arm_diagnostic_survives_a_long_legal_step_key(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The missing-arm refusal shares the 500-character bound: the offending
    element type and the remedy must survive a legal long key too."""

    class VideoItem(BaseModel):
        doc_id: str

    async def lying_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        return cast("list[ImageItem | AudioItem]", [VideoItem(doc_id="d1")])

    long_key = "media_" + "m" * 600
    app = WorkflowApp()

    @app.workflow("route_no_arm_long_key")
    def flow() -> Promise[object]:
        src = step(lying_source, key=long_key, max_attempts=3)
        return build(
            route(
                src,
                {
                    ImageItem: RouteArm(body=_process_image),
                    AudioItem: RouteArm(body=_process_audio),
                },
            )
        )

    runner = FlowRunner(app.get("route_no_arm_long_key"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS) == "terminal"

    source = await wf_conn.fetchrow(
        f'SELECT status, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = $2",
        flow_id,
        long_key,
    )
    assert source is not None
    assert source["status"] == "failed" and source["error_class"] == "RouterNotTotal"
    message = source["error_message"] or ""
    assert "VideoItem" in message, message
    assert "no route arm" in message, message


async def test_route_success_without_prebuilt_fork_fails_closed(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """The defensive success-finalize invariant refuses a missing route fork
    before persistence; valid execution cannot produce this call, so the direct
    internal seam is the proportionate way to keep the guard from disappearing."""

    async def source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        return []

    app = WorkflowApp()

    @app.workflow("route_missing_fork_guard")
    def flow() -> Promise[object]:
        src = step(source, key="media")
        return build(
            route(
                src,
                {
                    ImageItem: RouteArm(body=_process_image),
                    AudioItem: RouteArm(body=_process_audio),
                },
            )
        )

    compiled = app.get("route_missing_fork_guard")
    runner = FlowRunner(compiled, wf_pool, wf_schema)
    with pytest.raises(RuntimeError, match="finalized succeeded without its route fork"):
        await runner._finalize_success(
            JobId(new_uuid()),
            {"step_key": "media", "map_index": None},
            1,
            compiled.nodes["media"],
            {"value": []},
        )


async def test_fleet_route_source_returning_non_list_fails_deterministically(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The production queue-dispatch door returns the deterministic ladder's
    outcome and commits the same attempt-1 refusal as the in-process driver."""
    calls: list[int] = []

    async def bad_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        calls.append(1)
        return cast("list[ImageItem | AudioItem]", None)

    app = _build_consumer_app("route_non_list_fleet", bad_source, split_arm_queues=False)
    runner = FlowRunner(app.get("route_non_list_fleet"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    for config in project_workflow_actor_configs():
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".actor_config (actor, queue) VALUES ($1, $2) '
            "ON CONFLICT (actor) DO NOTHING",
            config.actor,
            config.queue,
        )
    worker_id = new_uuid()
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id, hostname, pid, queues, metadata) '
        "VALUES ($1, 'route-non-list-pin', 1, '{default}', $2::jsonb)",
        worker_id,
        json.dumps({"workflow_execution": True}),
    )
    dispatched = await dispatch_batch(
        wf_conn,
        sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
        queues=["default"],
        limit_n=5,
        worker_id=worker_id,
        lock_lease=timedelta(seconds=30),
    )
    assert len(dispatched) == 1, dispatched
    assert dispatched[0]["step_key"] == "media"

    execution = await execute_flow_job(
        pool=wf_pool,
        schema=wf_schema,
        worker_id=JobId(worker_id),
        job=_job_row_from_record(dispatched[0]),
    )

    assert execution.outcome == "laddered", execution
    assert calls == [1], calls
    source = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class FROM "{wf_schema}".jobs WHERE id = $1',
        dispatched[0]["id"],
    )
    assert source is not None
    assert dict(source) == {
        "status": "failed",
        "attempt": 1,
        "error_class": "RouterNotTotal",
    }
    forked = await wf_conn.fetchval(
        f'SELECT count(*) FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid "
        "AND (step_key LIKE 'media.item%' OR step_key = 'media.join')",
        flow_id,
    )
    assert forked == 0

    assert await runner.drive(flow_id, execute=False, max_ticks=_BOUNDED_TICKS) == "terminal"
    root_status = await wf_conn.fetchval(
        f'SELECT status FROM "{wf_schema}".jobs WHERE id = $1', flow_id
    )
    assert root_status == "failed"


async def test_non_list_refusal_uses_the_standard_deterministic_terminal_path(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The refusal honors capture policy and publishes the ordinary terminal
    projection and ERROR/laddered span, rather than the removed ad-hoc path's
    successful span and missing terminal projection."""
    _, exporter = setup_tracer(monkeypatch)

    async def bad_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        return cast("list[ImageItem | AudioItem]", None)

    app = WorkflowApp()

    @app.workflow("route_non_list_observability", capture="none")
    def flow() -> Promise[object]:
        src = step(bad_source, key="media", max_attempts=3)
        return build(
            route(
                src,
                {
                    ImageItem: RouteArm(body=_process_image),
                    AudioItem: RouteArm(body=_process_audio),
                },
            )
        )

    runner = FlowRunner(app.get("route_non_list_observability"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS) == "terminal"

    source_id = await wf_conn.fetchval(
        f'SELECT id FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'media'",
        flow_id,
    )
    assert source_id is not None
    terminal_payload = await wf_conn.fetchval(
        f'SELECT payload FROM "{wf_schema}".wf_node_stream '
        "WHERE node_id = $1 AND kind = 'wf.node.terminal' ORDER BY seq DESC LIMIT 1",
        source_id,
    )
    payload = (
        json.loads(terminal_payload) if isinstance(terminal_payload, str) else terminal_payload
    )
    assert payload["outcome"] == "failed", payload
    assert payload["error_class"] == "RouterNotTotal", payload

    ledger = await wf_conn.fetchrow(
        f'SELECT status, capture FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'media'",
        flow_id,
    )
    assert ledger is not None and ledger["status"] == "failed", ledger
    assert ledger["capture"] is None, "capture='none' must reach the refusal's finalize"

    span = exporter.span_named("step media")
    assert span is not None, [s.name for s in exporter.spans]
    assert span.attributes is not None
    assert span.attributes.get("taskq.outcome") == "laddered"
    assert span.status is not None and span.status.status_code == trace.StatusCode.ERROR
    laddered = [event for event in span.events if event.name == "wf.laddered"]
    assert len(laddered) == 1, [event.name for event in span.events]
    assert laddered[0].attributes is not None
    assert laddered[0].attributes.get("retried") is False
