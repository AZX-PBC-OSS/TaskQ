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

The pack also holds the review's surrounding contract:

* the two doors that would skip the fork are refused at the wiring
  (``cache=True`` by E17 and the verb; ``skip=`` by the verb);
* the fail-closed success guard raises an engine-private class routed
  through the deterministic ladder, never an unnamed escape;
* ``_EngineRouterNotTotal`` scopes the deterministic classification to the
  engine's own route throws — a body raising or chaining the public
  ``RouterNotTotal`` keeps its own retry budget and recorded class;
* a fenced deterministic terminal reports ``fenced`` (the ledger's own
  label) and emits no ``wf.laddered`` event;
* ``chain_source``'s key-space fence checks the node key it is about to
  add, including against its own chain's step keys;
* the chain face's route refusal uses the same ladder contract as the
  graph face (capture policy, terminal projection, ERROR span, bounded
  message and bounded logging);
* a plain map over a source that declares no ``list[...]`` still builds,
  and the no-arm refusal names the dispatch tag rather than a bare class
  name.
"""

# ruff: noqa: S608  # Why: every schema interpolation is the fixture-derived per-module schema identifier; every value is $-bound.

from __future__ import annotations

import asyncio
import collections
import enum
import json
from collections.abc import Awaitable, Callable, Iterator
from datetime import timedelta
from typing import Any, Literal, cast

import asyncpg
import pytest
import structlog
from opentelemetry import trace
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
from taskq.backend._protocol import JobId
from taskq.backend._records import _job_row_from_record
from taskq.testing.otel import setup_tracer
from taskq.workflows import (
    DONE,
    Chain,
    Exit,
    FlowRunner,
    Promise,
    Route,
    RouteArm,
    Step,
    StepContext,
    WorkflowApp,
    WorkflowBuildError,
    WorkflowRunError,
    build,
    chain_source,
    map_source,
    route,
    step,
)
from taskq.workflows._types import ForkSpec
from taskq.workflows._worker_execution import execute_flow_job, project_workflow_actor_configs
from taskq.workflows.api._runner_errors import (
    _RouteForkMissingError,  # pyright: ignore[reportPrivateUsage]  # Why: the pin proves the engine-private guard routes to the ladder; the underscore keeps it off the public surface.
)
from taskq.workflows.chain import RouterNotTotal, chain_start

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
    assert root_row["error_class"] == "UnabsorbedNodeFailure", (
        f"the root must record the engine's unabsorbed-failure class: {dict(root_row)}"
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
        # The consumer never released: it is still pending with its reserved
        # dependency counter at 1 — the join that would release it never
        # spawned. `finished_at IS NULL` alone is also true of `running`, so
        # the state that matters is asserted directly.
        assert consumer_row["status"] == "pending", dict(consumer_row)
        assert int(consumer_row["deps_pending"]) == 1, dict(consumer_row)
        assert consumer_row["finished_at"] is None, dict(consumer_row)


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
    with pytest.raises(_RouteForkMissingError, match="finalized succeeded without its route fork"):
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


def _assert_span_names_the_public_router_class(span: Any) -> None:
    """The engine's private route class never reaches the telemetry surface.

    Both surfaces a reader keys on — the ``wf.laddered`` event's ``error_class``
    and the ``exception`` event's ``exception.type`` — name the public class;
    the private spelling appears in no span or event attribute (including the
    rendered traceback)."""
    assert span.attributes is not None
    assert "_EngineRouterNotTotal" not in str(span.attributes), span.attributes

    laddered = [event for event in span.events if event.name == "wf.laddered"]
    assert len(laddered) == 1, [event.name for event in span.events]
    laddered_attributes = laddered[0].attributes
    assert laddered_attributes is not None
    assert laddered_attributes.get("error_class") == "RouterNotTotal", laddered_attributes

    exceptions = [event for event in span.events if event.name == "exception"]
    assert len(exceptions) == 1, [event.name for event in span.events]
    exception_attributes = exceptions[0].attributes
    assert exception_attributes is not None
    assert exception_attributes.get("exception.type") == "RouterNotTotal", exception_attributes

    for event in span.events:
        assert event.attributes is not None
        assert "_EngineRouterNotTotal" not in str(event.attributes), event.attributes


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
    _assert_span_names_the_public_router_class(span)


# ── C1: skip= on a route source is refused at the wiring ────────────────


async def _one_image_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
    del ctx
    return [ImageItem(doc_id="d1", uri="s3://b/d1.png")]


def test_route_source_with_skip_is_refused_at_the_wiring() -> None:
    """The dispatch-time skip gate finalizes the source without entering the
    body path where the route fork is built. A skipped source would read
    ``succeeded`` with no ``<source>.join``, stranding the consumer forever —
    the state the success-finalize guard forbids. The wiring refuses it, so
    the guard is never the only fence."""
    app = WorkflowApp()

    @app.workflow("route_skip_refused")
    def flow() -> Promise[object]:
        src = step(_one_image_source, key="media", skip=lambda _state: False)
        return build(route(src, {ImageItem: RouteArm(body=_process_image)}))

    with pytest.raises(WorkflowBuildError, match="skip="):
        app.get("route_skip_refused")


# ── W1: the fail-closed guard routes through the deterministic ladder ────


async def test_the_missing_fork_guard_terminal_fails_the_node(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The private guard's raise cannot escape the machinery boundary: it is
    caught at the miss-path finalize and routed through the deterministic
    ladder, so the guard's day produces a named failed row — never a silent
    ``running`` one with no terminal and nothing to grep."""

    async def source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        return []

    app = _build_consumer_app("route_missing_fork_routed", source)
    runner = FlowRunner(app.get("route_missing_fork_routed"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id

    async def _guard_fires(*args: Any, **kwargs: Any) -> Any:
        del args, kwargs
        raise _RouteForkMissingError("a route source finalized succeeded without its route fork")

    runner._finalize_success = _guard_fires  # type: ignore[method-assign]  # Why: the pin injects the guard at its own seam — the escape-point contract's face.
    assert await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS) == "terminal"

    row = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'media'",
        flow_id,
    )
    assert row is not None
    assert row["status"] == "failed", dict(row)
    assert int(row["attempt"]) == 1, dict(row)
    assert row["error_class"] == "_RouteForkMissingError", dict(row)


# ── W2: the engine's route class is scoped to engine throws ───────────────


async def _run_plain_step(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    *,
    body: Callable[[StepContext], Awaitable[None]],
    workflow_name: str,
) -> tuple[list[int], dict[str, object]]:
    calls: list[int] = []

    async def counted(ctx: StepContext) -> None:
        calls.append(1)
        await body(ctx)

    app = WorkflowApp()

    @app.workflow(workflow_name)
    def flow() -> Promise[object]:
        return build(step(counted, key="work", max_attempts=3))

    runner = FlowRunner(app.get(workflow_name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS) == "terminal"
    row = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'work'",
        flow_id,
    )
    assert row is not None
    return calls, dict(row)


async def test_a_body_raising_the_public_router_class_keeps_its_retry_budget(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """A plain, non-route body that raises the public ``RouterNotTotal`` is
    not an engine refusal: it burns its declared budget and records its own
    class name. Only the engine's private subclass is deterministic."""

    async def body(ctx: StepContext) -> None:
        del ctx
        raise RouterNotTotal("the body's own refusal")

    calls, row = await _run_plain_step(
        wf_conn, wf_schema, wf_pool, body=body, workflow_name="plain_public_router_raise"
    )
    assert calls == [1, 1, 1], calls
    assert int(row["attempt"]) == 3, row
    assert row["error_class"] == "RouterNotTotal", row
    assert row["error_message"] == "the body's own refusal", row


async def test_a_body_chaining_the_public_router_class_keeps_its_own_class(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The blast radius is bounded: a body that wraps the public class with
    ``raise ... from ...`` keeps the wrapper's retry budget and records the
    wrapper's class — the engine-private subclass never matches it."""

    async def body(ctx: StepContext) -> None:
        del ctx
        try:
            raise RouterNotTotal("an inner refusal, handled and wrapped")
        except RouterNotTotal as inner:
            raise RuntimeError("transient upstream blip") from inner

    calls, row = await _run_plain_step(
        wf_conn, wf_schema, wf_pool, body=body, workflow_name="plain_public_router_cause"
    )
    assert calls == [1, 1, 1], calls
    assert int(row["attempt"]) == 3, row
    assert row["error_class"] == "RuntimeError", row
    assert row["error_message"] == "transient upstream blip", row


# ── W3: a fenced deterministic terminal reports fenced, not laddered ──────


async def test_a_peer_reclaim_mid_refusal_leaves_one_terminal_and_a_fenced_label(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A lease expiry hands the row to a peer while the body is still running
    (the dispatch CAS's ``attempt + 1, claim_epoch + 1``). The stale attempt's
    refusal must be fenced out: no second terminal, no terminal projection,
    its ledger claim ``fenced`` — and the tail's own outcome label must say
    ``fenced`` (the ledger's word), not ``laddered``, with no ``wf.laddered``
    event on the span."""
    _, exporter = setup_tracer(monkeypatch)
    body_entered = asyncio.Event()
    release_body = asyncio.Event()

    async def slow_bad_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        body_entered.set()
        await release_body.wait()
        return cast("list[ImageItem | AudioItem]", None)

    app = _build_consumer_app("route_reclaim_race", slow_bad_source, split_arm_queues=False)
    runner = FlowRunner(app.get("route_reclaim_race"), wf_pool, wf_schema)
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
        "VALUES ($1, 'reclaim-race-pin', 1, '{default}', $2::jsonb)",
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
    job_id = dispatched[0]["id"]
    first = await wf_conn.fetchrow(f'SELECT attempt FROM "{wf_schema}".jobs WHERE id = $1', job_id)
    assert first is not None
    first_attempt = int(first["attempt"])

    stale = asyncio.create_task(
        execute_flow_job(
            pool=wf_pool,
            schema=wf_schema,
            worker_id=JobId(worker_id),
            job=_job_row_from_record(dispatched[0]),
        )
    )
    await asyncio.wait_for(body_entered.wait(), timeout=30)
    # The peer claim, as the dispatch CAS writes it for a row a reclaim handed
    # back to the fleet: the fence pair the stale finalize keys on moves on.
    peer = await wf_conn.fetchrow(
        f'UPDATE "{wf_schema}".jobs SET attempt = attempt + 1, claim_epoch = claim_epoch + 1 '
        "WHERE id = $1 AND status = 'running' RETURNING attempt",
        job_id,
    )
    assert peer is not None and int(peer["attempt"]) == first_attempt + 1
    release_body.set()
    execution = await asyncio.wait_for(stale, timeout=60)
    assert execution.outcome == "fenced", execution

    row = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class, finished_at FROM "{wf_schema}".jobs WHERE id = $1',
        job_id,
    )
    assert row is not None
    assert dict(row) == {
        "status": "running",
        "attempt": first_attempt + 1,
        "error_class": None,
        "finished_at": None,
    }, dict(row)
    ledger = await wf_conn.fetch(
        f'SELECT attempt, status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'media' ORDER BY attempt",
        flow_id,
    )
    assert [(int(r["attempt"]), r["status"]) for r in ledger] == [(first_attempt, "fenced")], [
        dict(r) for r in ledger
    ]
    stream = await wf_conn.fetch(
        f'SELECT kind FROM "{wf_schema}".wf_node_stream WHERE node_id = $1', job_id
    )
    assert [r["kind"] for r in stream] == ["wf.node.started"], [dict(r) for r in stream]

    # The fenced attempt is not a laddered failure: the span carries the fenced
    # label and no ``wf.laddered`` event (the event's retried=False would
    # otherwise over-count a terminal this attempt never wrote).
    span = exporter.span_named("step media")
    assert span is not None, [s.name for s in exporter.spans]
    assert span.attributes is not None
    assert span.attributes.get("taskq.outcome") == "fenced"
    assert not [event for event in span.events if event.name == "wf.laddered"], [
        event.name for event in span.events
    ]
    assert not [event for event in span.events if event.name == "exception"], [
        event.name for event in span.events
    ]


# ── W4: chain_source's key-space fence checks the node key it adds ────────


class _ScreenOutcome(enum.Enum):
    OK = "ok"


async def _screen_body(ctx: StepContext, item: object) -> _ScreenOutcome:
    del ctx, item
    return _ScreenOutcome.OK


def _screen_chain(name: str) -> Chain:
    return Chain(
        name=name,
        start="screen",
        steps={
            "screen": Step(
                body=_screen_body,
                outcomes=_ScreenOutcome,
                route=Route({_ScreenOutcome.OK: DONE}),
            )
        },
    )


async def _chain_emitter(ctx: StepContext) -> None:
    del ctx


def test_chain_source_key_equal_to_its_own_step_key_is_refused() -> None:
    """``chain_source``'s clash refusal ("a step key is the ledger's identity,
    never a shadow") must check the node key it is about to add, not only the
    wired nodes and other chains' steps. A source keyed as one of its own
    chain's step keys would be resolved through ``_chain_steps`` at runtime —
    the source's own finalize misrouted into the chain router, silently
    discarding a route's fork."""
    name = f"chaincollide_{new_uuid().hex}"
    chain = _screen_chain(f"chain-{name}")
    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(chain_source(chain, _chain_emitter, key="screen"))

    with pytest.raises(WorkflowBuildError, match="collide"):
        app.get(name)


# ── W5: the no-arm refusal names the dispatch tag, not the bare name ──────


class TagImage(BaseModel):
    """The route's declared union member (module scope — the shape the
    wiring's ``list[...]`` resolution requires)."""

    doc_id: str


def _tag_image_from_a_factory() -> type[BaseModel]:
    """A second class NAMED ``TagImage``: the same-qualname-twin family
    (factory / plugin registry / module split). The bare name agrees with
    the declared member; the dispatch tag does not."""

    class TagImage(BaseModel):
        doc_id: str

    return TagImage


async def _handle_tag_image(ctx: StepContext, item: TagImage) -> ImageResult:
    del ctx
    return ImageResult(doc_id=item.doc_id, ocr=item.doc_id)


async def test_no_arm_refusal_names_the_dispatch_tag(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The arm was chosen by the element's TYPE TAG (``module.qualname``); the
    refusal must name the same vocabulary. A bare ``__name__`` is
    indistinguishable from the declared member it drifted from, and no other
    artefact records the offender."""
    drifted = _tag_image_from_a_factory()
    assert drifted.__name__ == TagImage.__name__ == "TagImage"
    assert drifted.__qualname__ != TagImage.__qualname__

    async def lying_source(ctx: StepContext) -> list[TagImage]:
        del ctx
        return cast("list[TagImage]", [drifted(doc_id="d1")])

    app = WorkflowApp()

    @app.workflow("route_no_arm_dispatch_tag")
    def flow() -> Promise[object]:
        src = step(lying_source, key="media", max_attempts=3)
        return build(route(src, {TagImage: RouteArm(body=_handle_tag_image)}))

    runner = FlowRunner(app.get("route_no_arm_dispatch_tag"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS) == "terminal"

    source = await wf_conn.fetchrow(
        f'SELECT status, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'media'",
        flow_id,
    )
    assert source is not None
    assert source["status"] == "failed", dict(source)
    assert source["error_class"] == "RouterNotTotal", dict(source)
    message = source["error_message"] or ""
    assert "element #0" in message, message
    assert "no route arm" in message, message
    assert drifted.__qualname__ in message, message


# ── W6/W7/W8: the chain face uses the same ladder contract ───────────────


class ChainScreenOutcome(enum.Enum):
    CLEAN = "clean"
    FLAGGED = "flagged"


def _parity_chain(
    lying_body: Callable[[object, dict[str, object]], Awaitable[Any]],
    name: str,
) -> Chain:
    """One-step chain whose body returns a foreign outcome."""
    return Chain(
        name=name,
        start="screen",
        steps={
            "screen": Step(
                body=lying_body,
                outcomes=ChainScreenOutcome,
                route=Route(
                    {
                        ChainScreenOutcome.CLEAN: DONE,
                        ChainScreenOutcome.FLAGGED: DONE,
                    }
                ),
            ),
        },
    )


def _parity_source_body(chain: Chain) -> Callable[[StepContext], Awaitable[None]]:
    async def source(ctx: StepContext) -> None:
        await ctx.emit_batch(
            [chain_start(chain, {"app_id": 1}, map_index=1, trace_id="trace-app-1")],
            cursor={"page": 0},
        )

    return source


async def _drive_chain_workflow(
    *,
    workflow_name: str,
    lying_body: Callable[[object, dict[str, object]], Awaitable[Any]],
    capture: Literal["none", "errors-only", "all"],
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> tuple[FlowRunner, JobId]:
    chain = _parity_chain(lying_body, f"chain-{workflow_name}")
    app = WorkflowApp()

    @app.workflow(workflow_name, capture=capture)
    def flow() -> Promise[object]:
        return build(chain_source(chain, _parity_source_body(chain), key="src"))

    runner = FlowRunner(app.get(workflow_name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    await runner.drive(flow_id)
    return runner, flow_id


async def _chain_screen_row(
    wf_conn: asyncpg.Connection, wf_schema: str, flow_id: JobId
) -> asyncpg.Record:
    row = await wf_conn.fetchrow(
        f'SELECT id, status, attempt, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'screen'",
        flow_id,
    )
    assert row is not None, "the chain step row is missing"
    return row


async def _foreign_outcome_body(ctx: object, item: dict[str, object]) -> ChainScreenOutcome:
    del ctx, item
    return cast("ChainScreenOutcome", "corrupted")


async def test_chain_router_refusal_honours_the_declared_capture_policy(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """``capture="none"`` must reach the chain refusal's finalize, exactly as
    it does on the graph face: the ladder passes the compiled capture policy,
    so no captured payload lands in the ledger."""
    _, flow_id = await _drive_chain_workflow(
        workflow_name="chain_router_capture_none",
        lying_body=_foreign_outcome_body,
        capture="none",
        wf_pool=wf_pool,
        wf_schema=wf_schema,
    )
    row = await _chain_screen_row(wf_conn, wf_schema, flow_id)
    assert row["status"] == "failed" and row["error_class"] == "RouterNotTotal", dict(row)
    assert int(row["attempt"]) == 1, dict(row)

    ledger = await wf_conn.fetchrow(
        f'SELECT status, capture FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'screen'",
        flow_id,
    )
    assert ledger is not None and ledger["status"] == "failed", dict(ledger) if ledger else None
    assert ledger["capture"] is None, (
        "the chain face ignored the declared capture='none' and persisted a "
        f"captured payload into the ledger: {ledger['capture']!r}"
    )


async def test_chain_router_refusal_publishes_the_terminal_projection(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The refused chain step projects ``wf.node.terminal`` like the graph
    face — a live progress reader flips its badge instead of keeping the
    node's last state as a start."""
    _, flow_id = await _drive_chain_workflow(
        workflow_name="chain_router_projection",
        lying_body=_foreign_outcome_body,
        capture="errors-only",
        wf_pool=wf_pool,
        wf_schema=wf_schema,
    )
    row = await _chain_screen_row(wf_conn, wf_schema, flow_id)
    assert row["status"] == "failed" and row["error_class"] == "RouterNotTotal", dict(row)

    payload = await wf_conn.fetchval(
        f'SELECT payload FROM "{wf_schema}".wf_node_stream '
        "WHERE node_id = $1 AND kind = 'wf.node.terminal' ORDER BY seq DESC LIMIT 1",
        row["id"],
    )
    assert payload is not None, (
        "the chain face's RouterNotTotal terminal published no "
        "'wf.node.terminal' projection (the graph face does)"
    )


async def test_chain_router_refusal_step_span_reports_the_failure(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed chain step must not report an OK span: it reaches
    ``record_step_laddered`` (``taskq.outcome="laddered"``, ERROR) exactly as
    the graph refusal does."""
    _, exporter = setup_tracer(monkeypatch)

    await _drive_chain_workflow(
        workflow_name="chain_router_span",
        lying_body=_foreign_outcome_body,
        capture="errors-only",
        wf_pool=wf_pool,
        wf_schema=wf_schema,
    )

    span = exporter.span_named("step screen")
    assert span is not None, [s.name for s in exporter.spans]
    assert span.attributes is not None
    assert span.attributes.get("taskq.outcome") == "laddered", (
        "the chain face's failed step span reports "
        f"{span.attributes.get('taskq.outcome')!r} on a node the row records as failed"
    )
    assert span.status is not None and span.status.status_code == trace.StatusCode.ERROR, (
        f"the failed chain step's span status is {span.status}"
    )
    laddered = [event for event in span.events if event.name == "wf.laddered"]
    assert len(laddered) == 1, [event.name for event in span.events]
    _assert_span_names_the_public_router_class(span)


async def test_chain_router_refusal_durable_message_keeps_the_remedy(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The chain refusal puts the remedy before the unbounded outcome, so a
    2000-char foreign string cannot erase the diagnosis at the 500-char
    terminal bound."""
    long_outcome = "x" * 2000

    async def lying_body(ctx: object, item: dict[str, object]) -> ChainScreenOutcome:
        del ctx, item
        return cast("ChainScreenOutcome", long_outcome)

    _, flow_id = await _drive_chain_workflow(
        workflow_name="chain_router_diagnostics",
        lying_body=lying_body,
        capture="errors-only",
        wf_pool=wf_pool,
        wf_schema=wf_schema,
    )
    row = await _chain_screen_row(wf_conn, wf_schema, flow_id)
    assert row["status"] == "failed" and row["error_class"] == "RouterNotTotal", dict(row)
    assert int(row["attempt"]) == 1, dict(row)
    message = row["error_message"] or ""
    assert "has no route" in message, (
        f"the durable refusal lost its remedy to the 500-char terminal bound: {message!r}"
    )


async def test_chain_router_refusal_does_not_log_the_unbounded_outcome(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The one surviving ``node.router-not-total`` emission logged the body's
    whole returned value at WARNING. The chain face now refuses through the
    ladder, which emits no such line — the durable row, the terminal
    projection and the ERROR span carry the failure instead."""
    long_outcome = "x" * 2000

    async def lying_body(ctx: object, item: dict[str, object]) -> ChainScreenOutcome:
        del ctx, item
        return cast("ChainScreenOutcome", long_outcome)

    with structlog.testing.capture_logs() as logs:
        _, flow_id = await _drive_chain_workflow(
            workflow_name="chain_router_log_bound",
            lying_body=lying_body,
            capture="errors-only",
            wf_pool=wf_pool,
            wf_schema=wf_schema,
        )

    row = await _chain_screen_row(wf_conn, wf_schema, flow_id)
    assert row["status"] == "failed" and row["error_class"] == "RouterNotTotal", dict(row)

    for entry in [item for item in logs if item.get("event") == "node.router-not-total"]:
        logged = entry.get("outcome")
        assert isinstance(logged, str)
        assert len(logged) <= 200, (
            f"the router-not-total emission logged {len(logged)} characters "
            "of the body's returned value (the deleted graph-face copy bounded it at 200)"
        )


# ── W9: a plain map source need not declare list[...] ────────────────────


class MapProbeItem(BaseModel):
    doc_id: str


async def _single_item_source(ctx: StepContext) -> MapProbeItem:
    """A source declaring a BARE MODEL return — not a ``list[...]``."""
    del ctx
    return MapProbeItem(doc_id="d")


async def _map_item_body(ctx: StepContext, item: MapProbeItem) -> MapProbeItem:
    del ctx
    return MapProbeItem(doc_id=item.doc_id)


def test_plain_map_source_need_not_declare_a_list_return() -> None:
    """The untyped map face accepts a source that declares no ``list[...]``
    at all — the declared return is not what distinguishes it from the route,
    so a future fence must not assume otherwise."""
    app = WorkflowApp()

    @app.workflow("plain_map_non_list_decl")
    def flow() -> Promise[object]:
        src = step(_single_item_source, key="one")
        return build(map_source(src, _map_item_body))

    compiled = app.get("plain_map_non_list_decl")
    assert compiled.nodes["one"].map_item is not None, "the map attached to the source"


def test_route_over_the_same_non_list_source_is_refused_at_build() -> None:
    """The control: the ROUTE refuses the identical source at the wiring."""
    app = WorkflowApp()

    @app.workflow("route_non_list_decl")
    def flow() -> Promise[object]:
        src = step(_single_item_source, key="one")
        return build(route(src, {MapProbeItem: RouteArm(body=_map_item_body)}))

    with pytest.raises(WorkflowBuildError, match="does not declare a"):
        app.get("route_non_list_decl")


# ── R6: a typed route over a chain_source keeps its fork ─────────────────


class _RouteChainOutcome(enum.Enum):
    OK = "ok"


async def _route_chain_screen_body(ctx: StepContext, item: object) -> _RouteChainOutcome:
    del ctx, item
    return _RouteChainOutcome.OK


async def test_route_over_a_chain_source_forks_and_joins(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """A ``chain_source`` supplies its route fork like every other kind: the
    arm child spawns, the join fires, the consumer runs, and the chain's own
    step runs beside them."""
    suffix = new_uuid().hex
    chain = Chain(
        name=f"route-chain-{suffix}",
        start="screen",
        queue="default",
        steps={
            "screen": Step(
                body=_route_chain_screen_body,
                outcomes=_RouteChainOutcome,
                route=Route({_RouteChainOutcome.OK: DONE}),
            )
        },
    )

    async def source_body(ctx: StepContext) -> list[ImageItem | AudioItem]:
        await ctx.emit_batch(
            [chain_start(chain, {"record_id": 1}, map_index=1, trace_id="record-1")],
            cursor={"page": 0},
        )
        return [ImageItem(doc_id="d1", uri="s3://b/d1.png")]

    name = f"route_over_chain_source_{suffix}"
    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        src = chain_source(chain, source_body, key="source")
        routed = route(
            src,
            {
                ImageItem: RouteArm(body=_process_image),
                AudioItem: RouteArm(body=_process_audio),
            },
        )
        return build(step(_consume_results, routed, key="consume"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    verdict = await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS)
    rows = await wf_conn.fetch(
        f'SELECT step_key, status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid",
        flow_id,
    )
    detail = {row["step_key"]: row["status"] for row in rows}

    assert verdict == "terminal", detail
    assert detail["source"] == "succeeded", detail
    assert detail["source.join"] == "succeeded", detail
    assert any(
        key.startswith("source.item:") and status == "succeeded" for key, status in detail.items()
    ), detail
    assert detail["consume"] == "succeeded", detail
    assert detail["screen"] == "succeeded", detail
    assert await runner.result(flow_id) == {"count": 1}


# ── R2: Exit is handled above the map_arms arm ───────────────────────────


async def test_exit_from_a_route_source_is_skipped_not_stranded(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """``Exit`` is a legal return from any body and is tested ABOVE the
    ``map_arms`` arm, so a route source's fork is never built — the exit
    machinery must mark the consumers skipped rather than strand them."""
    name = f"route_exit_{new_uuid().hex}"

    async def source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        return cast("list[ImageItem | AudioItem]", Exit(None))

    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        routed = route(
            step(source, key="media"),
            {
                ImageItem: RouteArm(body=_process_image),
                AudioItem: RouteArm(body=_process_audio),
            },
        )
        return build(step(_consume_results, routed, key="consume"))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    verdict = await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS)
    rows = await wf_conn.fetch(
        f'SELECT step_key, status, result, finished_at FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid ORDER BY step_key",
        flow_id,
    )
    detail = [dict(row) for row in rows]
    assert verdict == "terminal", f"verdict={verdict!r} rows={detail}"
    consumer = next(row for row in rows if row["step_key"] == "consume")
    assert consumer["finished_at"] is not None, detail
    assert consumer["status"] == "succeeded", detail
    assert '"skipped": true' in (consumer["result"] or ""), detail
    assert '"exit_from": "media"' in (consumer["result"] or ""), detail


# ── R9/O7: the refusal never renders the value; width is the element walk ─


class _Unrenderable:
    """A non-list whose ``__repr__`` is expensive and raising — the shape the
    deleted handler's ``repr(outcome_value)[:200]`` would have paid for."""

    def __init__(self) -> None:
        self.renders = 0

    def __repr__(self) -> str:
        self.renders += 1
        raise AssertionError("the refusal must not render the outcome value")


async def test_refusal_never_renders_the_offending_value(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The non-list message names the runtime type only: no ``repr()`` of a
    huge (or unrenderable) body return on the failure path."""
    name = f"route_unrenderable_{new_uuid().hex}"
    payload = _Unrenderable()

    async def source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        return cast("list[ImageItem | AudioItem]", payload)

    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(
            route(
                step(source, key="media", max_attempts=3),
                {
                    ImageItem: RouteArm(body=_process_image),
                    AudioItem: RouteArm(body=_process_audio),
                },
            )
        )

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS) == "terminal"
    row = await wf_conn.fetchrow(
        f'SELECT status, attempt, error_class, error_message FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'media'",
        flow_id,
    )
    assert row is not None
    assert row["status"] == "failed", dict(row)
    assert int(row["attempt"]) == 1, dict(row)
    assert row["error_class"] == "RouterNotTotal", dict(row)
    assert "_Unrenderable" in (row["error_message"] or ""), dict(row)
    assert payload.renders == 0, f"the refusal rendered the value {payload.renders} time(s)"


def _route_only_app(name: str) -> WorkflowApp:
    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        src = step(_one_image_source, key="media")
        return build(
            route(
                src,
                {
                    ImageItem: RouteArm(body=_process_image),
                    AudioItem: RouteArm(body=_process_audio),
                },
            )
        )

    return app


def _fork(app: WorkflowApp, name: str, pool: asyncpg.Pool, schema: str, value: object) -> ForkSpec:
    """Drive the gate directly: ``_route_fork`` is the whole decision surface
    and its raise happens before any row or catalog read."""
    runner = FlowRunner(app.get(name), pool, schema)
    return runner._route_fork(  # pyright: ignore[reportPrivateUsage]  # Why: the white-box seam is the gate's whole decision surface; there is no public verb for a single element walk.
        {"step_key": "media"}, runner.compiled.nodes["media"], value
    )


@pytest.mark.parametrize(
    ("label", "shape"),
    [
        ("tuple", lambda: (ImageItem(doc_id="d1", uri="s3://b/d1.png"),)),
        ("userlist", lambda: collections.UserList([ImageItem(doc_id="d1", uri="s3://b/d1.png")])),
        ("deque", lambda: collections.deque([ImageItem(doc_id="d1", uri="s3://b/d1.png")])),
        ("str", lambda: "not-a-list"),
        ("bytes", lambda: b"not-a-list"),
    ],
)
async def test_non_list_sequence_shapes_are_refused(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
    label: str,
    shape: Callable[[], object],
) -> None:
    """Every iterable-but-not-``list`` shape lands on the refusal and the
    message names its own runtime type (a tuple is the most likely authoring
    mistake after a forgotten ``return``)."""
    name = f"route_shape_{label}"
    app = _route_only_app(name)
    value = shape()
    with pytest.raises(RouterNotTotal) as raised:
        _fork(app, name, wf_pool, wf_schema, value)
    message = str(raised.value)
    assert type(value).__name__ in message, message
    assert "MAP face" in message, message


async def test_iterator_and_generator_shapes_are_refused(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A one-shot iterator and a generator are not lists: both refuse, so a
    lazy sequence can never silently produce an empty route."""

    def items() -> Iterator[ImageItem]:
        yield ImageItem(doc_id="d1", uri="s3://b/d1.png")

    name = "route_shape_iterator"
    app = _route_only_app(name)
    for value in (iter([ImageItem(doc_id="d1", uri="s3://b/d1.png")]), items()):
        with pytest.raises(RouterNotTotal) as raised:
            _fork(app, name, wf_pool, wf_schema, value)
        assert type(value).__name__ in str(raised.value), str(raised.value)


class _ListSubclass(list[ImageItem]):
    """The shape the gate ACCEPTS: a plain ``list`` subclass."""


class _LyingLenList(list[ImageItem]):
    """A ``list`` subclass whose ``__len__`` disagrees with its contents."""

    def __len__(self) -> int:
        return 0


async def test_list_subclass_forks_every_element(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """The gate admits a ``list`` subclass, so the element walk — never
    ``len()`` — must decide the fork's width."""
    name = "route_shape_list_subclass"
    app = _route_only_app(name)

    fork = _fork(
        app,
        name,
        wf_pool,
        wf_schema,
        _ListSubclass(
            [
                ImageItem(doc_id="d1", uri="s3://b/d1.png"),
                ImageItem(doc_id="d2", uri="s3://b/d2.png"),
            ]
        ),
    )
    assert len(fork.children) == 2, [child.step_key for child in fork.children]

    lying = _fork(
        app,
        name,
        wf_pool,
        wf_schema,
        _LyingLenList([ImageItem(doc_id="d1", uri="s3://b/d1.png")]),
    )
    assert len(lying.children) == 1, [child.step_key for child in lying.children]


# ── O4: the refusal's stranded consumer is recoverable ───────────────────


async def test_retrying_the_refused_route_source_recovers_the_run(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The consumer the refusal stranded is not a dead path: retrying the
    failed source re-runs it, the fresh fork writes the join and the
    consumer's edge, and the run completes. The refusal also writes no
    exhaustion-effect intent (a graph-level step declares no after-terminal
    effect)."""
    calls: list[int] = []

    async def fixable_source(ctx: StepContext) -> list[ImageItem | AudioItem]:
        del ctx
        calls.append(1)
        if len(calls) == 1:
            return cast("list[ImageItem | AudioItem]", None)
        return [ImageItem(doc_id="d1", uri="s3://b/d1.png")]

    app = _build_consumer_app("route_refusal_then_retry", fixable_source)
    runner = FlowRunner(app.get("route_refusal_then_retry"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS) == "terminal"

    consumer = await wf_conn.fetchrow(
        f'SELECT status, deps_pending FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'consume'",
        flow_id,
    )
    assert consumer is not None
    assert consumer["status"] == "pending" and int(consumer["deps_pending"]) == 1, dict(consumer)
    source = await wf_conn.fetchrow(
        f'SELECT status, error_class FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'media'",
        flow_id,
    )
    assert source is not None
    assert source["status"] == "failed" and source["error_class"] == "RouterNotTotal", (
        source["status"],
        source["error_class"],
    )
    # The claim history the refusal leaves: one attempt, terminal, and no
    # phantom 'running' row beside the terminal.
    refused_ledger = await wf_conn.fetch(
        f'SELECT attempt, status FROM "{wf_schema}".wf_step_ledger '
        "WHERE flow_id = $1 AND step_key = 'media' ORDER BY attempt",
        flow_id,
    )
    assert [(int(r["attempt"]), r["status"]) for r in refused_ledger] == [(1, "failed")], [
        dict(r) for r in refused_ledger
    ]
    assert (
        await wf_conn.fetchval(
            f'SELECT count(*) FROM "{wf_schema}".wf_outbox WHERE flow_id = $1::uuid', flow_id
        )
        == 0
    )

    assert await runner.retry_node(flow_id, "media") is True
    assert await runner.drive(flow_id, max_ticks=_BOUNDED_TICKS) == "terminal"

    rows = await wf_conn.fetch(
        f'SELECT step_key, status FROM "{wf_schema}".jobs '
        "WHERE (metadata->>'flow_id')::uuid = $1::uuid",
        flow_id,
    )
    statuses = {r["step_key"]: r["status"] for r in rows}
    assert statuses["__flow__"] == "succeeded", statuses
    assert statuses["consume"] == "succeeded", statuses
    assert statuses["media.join"] == "succeeded", statuses
    assert await runner.result(flow_id) == {"count": 1}


async def test_a_body_group_carrying_the_public_router_class_is_not_substituted(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
) -> None:
    """The group face keeps the bound too: a non-route body's
    ``ExceptionGroup`` carrying a public ``RouterNotTotal`` leaf is not
    classified by it — the group burns the declared budget and records its
    own wrapper class, so the engine-private membership cannot hide a
    non-deterministic sibling behind a route-shaped leaf."""

    async def body(ctx: StepContext) -> None:
        del ctx
        raise ExceptionGroup(
            "body fan-out",
            [RuntimeError("upstream blip"), RouterNotTotal("an inner refusal")],
        )

    calls, row = await _run_plain_step(
        wf_conn, wf_schema, wf_pool, body=body, workflow_name="plain_public_router_group"
    )
    assert calls == [1, 1, 1], calls
    assert int(row["attempt"]) == 3, row
    assert row["error_class"] == "ExceptionGroup", row


async def test_list_of_non_members_names_the_element_shape(
    wf_pool: asyncpg.Pool,
    wf_schema: str,
) -> None:
    """A list whose elements are not union members passes the outer gate and
    dies in the element walk naming the element's own type tag."""
    name = "route_shape_nested"
    app = _route_only_app(name)
    for element in (None, [ImageItem(doc_id="d1", uri="s3://b/d1.png")], {"doc_id": "d1"}):
        with pytest.raises(RouterNotTotal) as raised:
            _fork(app, name, wf_pool, wf_schema, [element])
        message = str(raised.value)
        assert "no route arm" in message, message
        assert type(element).__name__ in message, message
