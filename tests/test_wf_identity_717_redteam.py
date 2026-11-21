"""The widened record identity under adversarial runtime paths (red-team pins).

These pins attack the assumptions behind the #717 widening that the door-level
repair pins do not exercise: the identity must stay continuous across
32767→32768 *through the live runner* (claim, ladder, fork, ledger), the widened
identity binds must read back on the memoized/replay face, the new array/drain
bigint binds must still serve the supported pre-POST window, and a real broken
carrier shape (a half-applied widening) must converge to a deterministic
terminal instead of looping reclaim.

Real PostgreSQL; the house ``wf_*`` fixtures (fully migrated, all-bigint) and
throwaway PRE-only schemas built by the product migration runner.
"""

# ruff: noqa: S608  # Why: only fixture-derived throwaway schema identifiers are interpolated; every value is $-bound.

from __future__ import annotations

import enum
import json
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_base62, new_uuid
from taskq.constants import BIGINT_MAX
from taskq.migrate import apply_pending
from taskq.workflows._fork import insert_fork
from taskq.workflows._sql import WorkflowSql
from taskq.workflows._types import (
    ChildSpec,
    ConsumerBinding,
    EmitChild,
    ForkSpec,
    JoinSpec,
    _consumer_binding_doc,
)
from taskq.workflows.api._graph import chain_source
from taskq.workflows.engine import render_workflow_sql
from tests._wf_fixtures import seed_flow

pytestmark = pytest.mark.integration

BOUNDARY_IDS = (32767, 32768)


class Ok(enum.Enum):
    OK = "ok"


class RouteImageItem(BaseModel):
    doc_id: str
    uri: str


class RouteAudioItem(BaseModel):
    doc_id: str
    uri: str


async def test_streaming_chain_runs_continuously_across_the_16bit_boundary(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """One streamed page carries record 32767 and record 32768; every chain
    row of both terminalizes exactly once with its own identity."""
    from taskq.workflows import DONE, Chain, FlowRunner, Route, Step, WorkflowApp, build

    async def first(ctx: Any, item: object) -> Ok:
        del ctx, item
        return Ok.OK

    async def second(ctx: Any, item: object) -> Ok:
        del ctx, item
        return Ok.OK

    chain = Chain(
        name=f"boundary-{new_uuid().hex}",
        start="s1",
        steps={
            "s1": Step(body=first, outcomes=Ok, route=Route({Ok.OK: "s2"})),
            "s2": Step(body=second, outcomes=Ok, route=Route({Ok.OK: DONE})),
        },
    )

    async def source_body(ctx: Any) -> None:
        from taskq.workflows.chain import chain_start

        await ctx.emit_batch(
            [
                chain_start(chain, {"record": idx}, map_index=idx, trace_id=f"rec-{idx}")
                for idx in BOUNDARY_IDS
            ],
            cursor={"page": 0},
        )

    name = f"boundary_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> object:
        return build(chain_source(chain, source_body, key="source", max_attempts=2))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    rows = await wf_conn.fetch(
        f"""SELECT step_key, map_index, status
            FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND step_key IN ('s1', 's2')
            ORDER BY step_key, map_index""",
        flow_id,
    )
    got = [(r["step_key"], int(r["map_index"]), r["status"]) for r in rows]
    expected = [(step, idx, "succeeded") for step in ("s1", "s2") for idx in BOUNDARY_IDS]
    assert got == expected, got

    ledger = await wf_conn.fetch(
        f"""SELECT step_key, map_index, status, count(*) AS n
            FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key IN ('s1', 's2')
            GROUP BY step_key, map_index, status""",
        flow_id,
    )
    ledger_rows = {(r["step_key"], int(r["map_index"]), r["status"]): int(r["n"]) for r in ledger}
    for step in ("s1", "s2"):
        for idx in BOUNDARY_IDS:
            assert ledger_rows.get((step, idx, "succeeded")) == 1, (step, idx, ledger_rows)
    assert all(status == "succeeded" for _step, _idx, status in ledger_rows), ledger_rows

    root = await wf_conn.fetchval(
        f'SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1::uuid', flow_id
    )
    assert root == "succeeded", root


async def test_chain_step_ladder_at_a_high_identity_counts_every_attempt(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A failing chain step at map_index 32768 runs its full ladder: the
    failure counter (``_FAILED_COUNT_SQL``) must count the widened identity,
    not mis-bind it to zero or raise on the int16 domain."""
    from taskq.workflows import Chain, FlowRunner, Step, WorkflowApp, build

    async def boom(ctx: Any, item: object) -> object:
        del ctx, item
        raise ValueError("transient boom")

    chain = Chain(
        name=f"boom-{new_uuid().hex}",
        start="s1",
        steps={"s1": Step(body=boom, outcomes=object)},
    )
    record = 32768

    async def source_body(ctx: Any) -> None:
        from taskq.workflows.chain import chain_start

        await ctx.emit_batch(
            [chain_start(chain, {"record": record}, map_index=record, trace_id="rec-32768")],
            cursor={"page": 0},
        )

    name = f"boom_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> object:
        return build(chain_source(chain, source_body, key="source", max_attempts=2))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    ledger = await wf_conn.fetch(
        f"""SELECT attempt, status FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key = 's1' AND map_index = $2::bigint
            ORDER BY attempt""",
        flow_id,
        record,
    )
    assert [(int(r["attempt"]), r["status"]) for r in ledger] == [
        (1, "failed"),
        (2, "failed"),
        (3, "failed"),
    ]

    node = await wf_conn.fetchrow(
        f"""SELECT status, attempt, error_class FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 's1'""",
        flow_id,
    )
    assert node is not None and node["status"] == "failed", dict(node)  # pyright: ignore[reportCallIssue]  # Why: asyncpg.Record is a mapping at runtime; the stub's constructor overloads are narrower.
    assert int(node["attempt"]) == 3, dict(node)  # pyright: ignore[reportCallIssue]  # Why: the same runtime-mapping read.
    assert node["error_class"] == "ValueError", dict(node)  # pyright: ignore[reportCallIssue]  # Why: the same runtime-mapping read.


async def test_memoized_terminal_lookup_reads_a_widened_identity(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    """The replay's memoized terminal read (``$3::bigint``) must find the row
    at 32768 and must not mistake its sibling at 0 for it."""
    from taskq.workflows.ledger import memoized_step_result

    flow_id = new_uuid()
    await wf_conn.execute(
        f"""INSERT INTO "{wf_schema}".wf_step_ledger
            (id, flow_id, job_id, step_key, map_index, attempt, status, result)
            VALUES ($1, $2, $3, 'sub', 32768, 1, 'succeeded', '{{"v": 7}}'::jsonb)""",
        new_uuid(),
        flow_id,
        new_uuid(),
    )
    hit = await memoized_step_result(
        wf_conn, wf_sql, flow_id=flow_id, step_key="sub", map_index=32768
    )
    assert hit is not None and hit.status == "succeeded", hit
    assert (
        await memoized_step_result(wf_conn, wf_sql, flow_id=flow_id, step_key="sub", map_index=0)
        is None
    )


async def test_low_identity_fork_and_outbox_drain_on_a_pre_post_schema(pg_dsn: str) -> None:
    """The supported mixed-version window: the release's new ``::bigint[]``
    array bind (``insert_fork``) and the drain's read still serve a
    not-yet-widened (all-smallint) schema for every low identity, so a new
    binary is safe before the POST applies. The scalar doors' cast pins live
    in tests/test_wf_identity_warm_cache_scalar_binds.py."""
    schema = "prepost_probe_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    pool = await asyncpg.create_pool(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")
        wsql = render_workflow_sql(schema)
        flow = await seed_flow(conn, schema)

        fork = ForkSpec(
            children=(
                ChildSpec(step_key="m.item", actor="wf", queue="default", map_index=0),
                ChildSpec(step_key="m.item", actor="wf", queue="default", map_index=32767),
            ),
            join=JoinSpec(step_key="m.join", actor="wf", queue="default"),
        )
        ids, _join = await insert_fork(
            conn, wsql, flow_id=flow, parent_id=flow, parent_step_key="m", fork=fork
        )
        got = await conn.fetch(
            f'SELECT map_index FROM "{schema}".jobs WHERE id = ANY($1::uuid[]) ORDER BY map_index',
            ids,
        )
        assert [int(r["map_index"]) for r in got] == [0, 32767]

        binding_doc = _consumer_binding_doc(
            ConsumerBinding(step_key="consumer", actor="wf", queue="default")
        )
        await conn.execute(
            f"""INSERT INTO "{schema}".wf_outbox
                (id, join_job_id, flow_id, consumer_step_key, map_index, bindings)
                VALUES ($1, $2, $3, 'consumer', 32767, $4::jsonb)""",
            new_uuid(),
            flow,
            flow,
            json.dumps(binding_doc),
        )
        from taskq.workflows._sweep import drain_outbox

        delivered = await drain_outbox(
            pool, wsql, default_max_attempts=3, default_retry_kind="transient"
        )
        assert delivered >= 1, delivered
        row = await conn.fetchrow(
            f"""SELECT map_index FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'consumer'""",
            flow,
        )
        assert row is not None and int(row["map_index"]) == 32767, dict(row) if row else None
    finally:
        await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_real_mixed_carrier_schema_error_terminalizes_on_the_first_attempt(
    pg_dsn: str,
) -> None:
    """A real half-widened schema (``jobs_archive`` still ``integer``) reaching
    the high-candidate resolver must fail the source deterministically on the
    FIRST attempt with the named deployment class — never re-pend into
    machinery reclaim."""
    from taskq.workflows import DONE, Chain, FlowRunner, Route, Step, WorkflowApp, build

    schema = "x4_probe_" + new_base62(8, precision="random").lower()
    conn = await asyncpg.connect(pg_dsn)
    pool = await asyncpg.create_pool(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema, phase="pre")
        await conn.execute(
            f'ALTER TABLE "{schema}".jobs_archive ALTER COLUMN map_index TYPE integer'
        )

        async def body(ctx: Any, item: object) -> Ok:
            del ctx, item
            return Ok.OK

        chain = Chain(
            name=f"x4-{new_uuid().hex}",
            start="s1",
            steps={"s1": Step(body=body, outcomes=Ok, route=Route({Ok.OK: DONE}))},
        )

        async def source_body(ctx: Any) -> None:
            from taskq.workflows.chain import chain_start

            await ctx.emit_batch(
                [chain_start(chain, {"record": 32768}, map_index=32768, trace_id="rec")],
                cursor={"page": 0},
            )

        name = f"x4_{new_uuid().hex}"
        app = WorkflowApp()

        @app.workflow(name)
        def flow() -> object:
            return build(chain_source(chain, source_body, key="source", max_attempts=2))

        runner = FlowRunner(app.get(name), pool, schema)
        flow_id = (await runner.create_flow()).flow_id
        outcome = await runner.drive(flow_id)
        assert outcome == "terminal", outcome

        source = await conn.fetchrow(
            f"""SELECT status, attempt, error_class FROM "{schema}".jobs
                WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'source'""",
            flow_id,
        )
        assert source is not None and source["status"] == "failed", dict(source)  # pyright: ignore[reportCallIssue]  # Why: asyncpg.Record is a mapping at runtime; the stub's constructor overloads are narrower.
        assert source["error_class"] == "MapIndexSchemaError", dict(source)  # pyright: ignore[reportCallIssue]  # Why: the same runtime-mapping read.
        assert int(source["attempt"]) == 1, (
            "must terminalize on the first attempt",
            dict(source),  # pyright: ignore[reportCallIssue]  # Why: the same runtime-mapping read.
        )
    finally:
        await pool.close()
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await conn.close()


async def test_reemitting_the_same_high_identity_page_collides_and_duplicates_nothing(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool, wf_sql: WorkflowSql
) -> None:
    """The acceptance's deterministic-identity property at the widened
    identity: a lost-acknowledgement re-emit of the same page collides on the
    idempotency arbiter and writes no second row."""
    from taskq.workflows._emit import PageDivergedError
    from tests._wf_fixtures import claim_view, seed_running_node

    record = 40000
    flow = await seed_flow(wf_conn, wf_schema)
    source = await seed_running_node(wf_conn, wf_schema, flow, step_key="source")
    worker, attempt, epoch = await claim_view(wf_conn, wf_schema, source)
    page = [
        EmitChild(
            step_key="source",
            actor="wf",
            queue="default",
            payload={"record": record},
            trace_id=f"record-{record}",
            map_index=record,
        )
    ]
    kwargs: dict[str, Any] = {
        "flow_id": flow,
        "source_id": source,
        "worker_id": worker,
        "attempt": attempt,
        "claim_epoch": epoch,
        "children": page,
        "cursor": {"page": 0},
    }
    from taskq.workflows._emit import emit_batch

    first = await emit_batch(wf_pool, wf_sql, **kwargs)
    assert len(first) == 1
    with pytest.raises(PageDivergedError):
        await emit_batch(wf_pool, wf_sql, **kwargs)
    rows = await wf_conn.fetch(
        f'SELECT map_index FROM "{wf_schema}".jobs WHERE parent_id = $1', source
    )
    assert [int(r["map_index"]) for r in rows] == [record], rows


async def test_collect_fan_in_reads_the_high_identity_ledger_history(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_sql: WorkflowSql
) -> None:
    """``COLLECT_FAN_IN_SQL``'s widened ``$4::bigint`` identity bind: an
    absorbing collect join must read the failure history of a child at a high
    identity (a narrow int16 bind would raise mid-cascade)."""
    record = 32768
    flow = new_uuid()

    for attempt in (1, 2):
        await wf_conn.execute(
            f"""INSERT INTO "{wf_schema}".wf_step_ledger
                (id, flow_id, job_id, step_key, map_index, attempt, status,
                 error_class, error_message)
                VALUES ($1, $2, $3, 'm.item', $4, $5, 'failed', 'ValueError', 'boom')""",
            new_uuid(),
            flow,
            new_uuid(),
            record,
            attempt,
        )

    join = new_uuid()
    fairness = str(flow)
    await wf_conn.execute(
        f"""INSERT INTO "{wf_schema}".jobs
            (id, actor, queue, payload, status, max_attempts, retry_kind, step_key,
             deps_pending, metadata, idempotency_scope, idempotency_key, fairness_key)
            VALUES ($1, 'wf', 'default', '{{}}', 'pending', 3, 'transient', 'm.join',
                    1, jsonb_build_object('flow_id', $2::text, 'blocking_reason', 'join'),
                    'workflow-run', $3, $2)""",
        join,
        fairness,
        f"join-{join}",
    )
    failed_child = new_uuid()
    await wf_conn.execute(
        f"""INSERT INTO "{wf_schema}".jobs
            (id, actor, queue, payload, status, max_attempts, retry_kind, step_key,
             map_index, metadata, idempotency_scope, idempotency_key, fairness_key)
            VALUES ($1, 'wf', 'default', '{{}}', 'running', 3, 'transient', 'm.item',
                    $2, jsonb_build_object('flow_id', $3::text),
                    'workflow-run', $4, $3)""",
        failed_child,
        record,
        fairness,
        f"term-{failed_child}",
    )
    await wf_conn.execute(
        f"""INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, failure_policy)
            VALUES ($1, $2, $3, 'collect')""",
        join,
        failed_child,
        flow,
    )

    rows = await wf_conn.fetch(wf_sql.collect_fan_in, failed_child, flow, "m.item", record)
    assert len(rows) == 1, rows
    attempts = rows[0]["attempts"]
    import json as _json

    decoded = _json.loads(attempts) if isinstance(attempts, str) else attempts
    assert [item["attempt"] for item in decoded] == [1, 2], decoded


async def test_route_over_the_join_count_refuses_deterministically(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """A route past the join counter is refused by the finalize's machinery
    boundary as the named count error, on the first attempt, before any child
    row — never as an identity refusal and never as a body failure. (The route
    fork deliberately performs no identity catalog read on the body path.)"""
    from taskq.workflows import (
        FlowRunner,
        Promise,
        RouteArm,
        WorkflowApp,
        build,
        route,
        step,
    )

    async def process_image(ctx: Any, item: RouteImageItem) -> str:
        del ctx
        return item.doc_id

    async def process_audio(ctx: Any, item: RouteAudioItem) -> str:
        del ctx
        return item.doc_id

    #: One past the join counter (``jobs.deps_pending`` is a smallint).
    elements = 32769

    async def big_route_source(ctx: Any) -> list[RouteImageItem | RouteAudioItem]:
        del ctx
        return [RouteImageItem(doc_id=str(i), uri="u") for i in range(elements)]

    app = WorkflowApp()

    @app.workflow("route_over_count")
    def flow() -> Promise[object]:
        src = step(big_route_source, key="media")
        return build(
            route(
                src,
                {
                    RouteImageItem: RouteArm(body=process_image),
                    RouteAudioItem: RouteArm(body=process_audio),
                },
            )
        )

    runner = FlowRunner(app.get("route_over_count"), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

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


async def test_chain_at_the_max_bigint_identity_runs_end_to_end(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The widened domain's actual top end: a record at ``BIGINT_MAX`` is
    emitted, claimed, forked and re-claimed at the same identity. (The
    ``32767 → 32768`` pin covers the boundary; this covers the domain max.)"""
    from taskq.workflows import DONE, Chain, FlowRunner, Route, Step, WorkflowApp, build

    async def first(ctx: Any, item: object) -> Ok:
        del ctx, item
        return Ok.OK

    async def second(ctx: Any, item: object) -> Ok:
        del ctx, item
        return Ok.OK

    chain = Chain(
        name=f"bigmax-{new_uuid().hex}",
        start="s1",
        steps={
            "s1": Step(body=first, outcomes=Ok, route=Route({Ok.OK: "s2"})),
            "s2": Step(body=second, outcomes=Ok, route=Route({Ok.OK: DONE})),
        },
    )

    async def source_body(ctx: Any) -> None:
        from taskq.workflows.chain import chain_start

        await ctx.emit_batch(
            [chain_start(chain, {"record": 1}, map_index=BIGINT_MAX, trace_id="rec-max")],
            cursor={"page": 0},
        )

    name = f"bigmax_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> object:
        return build(chain_source(chain, source_body, key="source", max_attempts=2))

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = (await runner.create_flow()).flow_id
    assert await runner.drive(flow_id) == "terminal"

    rows = await wf_conn.fetch(
        f"""SELECT step_key, map_index, status FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key IN ('s1', 's2')
            ORDER BY step_key""",
        flow_id,
    )
    assert [(r["step_key"], int(r["map_index"]), r["status"]) for r in rows] == [
        ("s1", BIGINT_MAX, "succeeded"),
        ("s2", BIGINT_MAX, "succeeded"),
    ]
    ledger = await wf_conn.fetch(
        f"""SELECT step_key, map_index, status FROM "{wf_schema}".wf_step_ledger
            WHERE flow_id = $1::uuid AND step_key IN ('s1', 's2')""",
        flow_id,
    )
    assert {(r["step_key"], int(r["map_index"]), r["status"]) for r in ledger} == {
        ("s1", BIGINT_MAX, "succeeded"),
        ("s2", BIGINT_MAX, "succeeded"),
    }


async def test_admin_panel_reads_a_real_high_identity_child(
    wf_conn: asyncpg.Connection, wf_schema: str, wf_pool: asyncpg.Pool
) -> None:
    """The node panel's child addressing at a widened identity: a real child
    at 40000 is read (200 with its own map_index), not merely a 404 for a
    missing one."""
    pytest.importorskip("fastapi", reason="requires taskq[fastapi]")
    from fastapi import FastAPI

    from taskq.web.admin import create_router, setup_admin_state

    async def _allow(_request: Any) -> None:
        return None

    app = FastAPI()
    bundle = create_router(wf_pool, schema=wf_schema, base_path="", auth_dependency=_allow)
    setup_admin_state(app, bundle)
    endpoint = next(
        route.endpoint  # pyright: ignore[reportAttributeAccessIssue]  # Why: the matched route is an APIRoute.
        for route in bundle.router.routes
        if getattr(route, "name", "") == "node_panel"
    )
    run_id = new_uuid()
    child = new_uuid()
    await wf_conn.execute(
        f"""INSERT INTO "{wf_schema}".jobs
            (id, actor, queue, payload, status, max_attempts, retry_kind, step_key,
             map_index, metadata, idempotency_scope, idempotency_key, fairness_key)
            VALUES ($1, 'wf', 'default', '{{}}', 'succeeded', 3, 'transient', 'review',
                    $2, jsonb_build_object('flow_id', $3::text), 'workflow-run', $4, $3)""",
        child,
        40000,
        str(run_id),
        f"child-{child}",
    )
    body = await endpoint(run_id, "review", wf_pool, wf_schema, 40000)
    assert body["map_index"] == 40000, body
