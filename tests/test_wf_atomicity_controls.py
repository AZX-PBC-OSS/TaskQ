# ruff: noqa: S608  # Schema names are generated test identifiers; values remain $-bound.
"""Behavior controls for transaction-joining pages and all-emitted settlement."""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Literal, TypeVar, cast, overload
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._protocol import ConnLike, JobId
from taskq.backend._sql import build_heartbeat_sql, parse_rowcount
from taskq.backend._sweeps import sweep_expired_results, sweep_scheduled_to_pending
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps
from taskq.worker.heartbeat import _lease_renewal_threshold
from taskq.workflows import (
    Chain,
    CompiledWorkflow,
    FlowRunner,
    Promise,
    Step,
    StepContext,
    WorkflowApp,
    WorkflowRunError,
    build,
    chain_source,
    step,
    transactional_step,
)
from taskq.workflows._capability import (
    WORKFLOW_EXECUTION_SCHEMA,
    WorkflowWorkerCapabilityError,
)
from taskq.workflows._emit import EmitBackpressure, EmitFencedError, PageDivergedError
from taskq.workflows._sweep import sweep_join_rederive
from taskq.workflows.api._runner_exit import cancel_workflow_run, retry_workflow_node
from taskq.workflows.engine import finalize_node
from tests._wf_atomicity_fixture import (
    RealWorker,
    application_tables,
    barrier_state,
    children,
    create_guarded,
    cursor,
    edge_count,
    emit_joined,
    execution_path,
    fire_count,
    insert_domain,
    insert_stage,
    json_dict,
    json_object,
    managed_pg_pool,
    native_barrier_declared,
    new_label,
    node_id,
    page,
    read_joined_cursor,
    render_direct_claim,
    row,
    row_lock_waiters,
    settlement_connection,
    settlement_counts,
    stage_count,
    two_node_flow,
    wait_event,
    wait_until,
    write_settlement,
)

# The current level 2 explicitly excludes transaction execution. This specification
# allocates the fixed level 3; it never derives its expectation from implementation state.
TRANSACTIONAL_EXECUTION_SCHEMA = 3


if TYPE_CHECKING:
    from taskq.testing.fixtures import ModulePgSchema
    from taskq.workflows._sql import WorkflowSql


async def _ok_screen(ctx: StepContext, item: object) -> str:
    return "ok"


def _chain(name: str) -> Chain:
    return Chain(name=name, start="screen", steps={"screen": Step(_ok_screen, str)})


async def _run_source_scenario(
    pool: asyncpg.Pool,
    control: asyncpg.Connection,
    dsn: str,
    schema: str,
    scenario: Callable[[StepContext], Awaitable[object]],
    *,
    max_in_flight: int | None = None,
    runner_out: list[FlowRunner] | None = None,
) -> tuple[JobId, list[BaseException]]:
    """Run an authored source through guarded creation, dispatch and worker execution."""
    errors: list[BaseException] = []

    async def source(ctx: StepContext) -> object:
        try:
            return await scenario(ctx)
        except BaseException as exc:
            errors.append(exc)
            raise

    name = new_label("joined_source")
    app = WorkflowApp()
    decorator = app.workflow(name, max_in_flight=max_in_flight)

    @decorator
    def flow() -> Promise[object]:
        return build(
            chain_source(_chain(new_label("records")), source, key="source", max_attempts=1)
        )

    runner = FlowRunner(app.get(name), pool, schema)
    if runner_out is not None:
        runner_out.append(runner)
    flow_id = await create_guarded(runner)
    worker = await RealWorker.open(pool, control, dsn, schema)
    try:
        await worker.run(runner.compiled, flow_id, "source")
    finally:
        await worker.close()
    return flow_id, errors


def _assert_body_completed(errors: list[BaseException]) -> None:
    assert errors == [], (
        "the guarded production source did not complete its scenario: "
        f"{[(type(exc).__name__, str(exc)) for exc in errors]!r}"
    )


def _assert_atomic_connections(actual: list[bool], count: int) -> None:
    assert len(actual) == count
    assert actual == [True] * count, (
        "settlement did not use the required public atomic path: "
        f"transactional_step={'present' if native_barrier_declared() else 'absent'}; "
        f"ctx.connection per callback={['present' if value else 'absent' for value in actual]}"
    )


async def _caller_page_scenario(
    ctx: StepContext,
    *,
    caller_pool: asyncpg.Pool,
    observer: asyncpg.Connection,
    schema: str,
) -> object:
    """Application-visible page visibility, read, COMMIT and rollback on one caller session."""
    caller = await caller_pool.acquire()
    try:
        async with caller.transaction():
            await insert_domain(caller, schema, ctx.flow_id, "page zero")
            await insert_stage(caller, schema, ctx.flow_id, 0)
            successful_children = await emit_joined(
                ctx,
                page((10, 11)),
                cursor={"page": 0},
                connection=caller,
                schema=schema,
            )
            same = await children(caller, schema, ctx.job_id)
            assert [item["id"] for item in same] == list(successful_children)
            assert await edge_count(caller, schema, ctx.job_id) == 2
            first_cursor = await read_joined_cursor(ctx, connection=caller, schema=schema)
            assert first_cursor == {"page": 0}
            with pytest.raises(WorkflowRunError) as stale_read:
                await ctx.cursor()
            assert not isinstance(stale_read.value, asyncpg.PostgresError)
            cursor_message = str(stale_read.value).lower()
            assert "cursor" in cursor_message
            assert any(
                phrase in cursor_message
                for phrase in ("same connection", "same-connection", "stale")
            ), f"cursor refusal does not name its same-session contract: {cursor_message}"
            assert await children(observer, schema, ctx.job_id) == []
            assert await cursor(observer, schema, ctx.job_id) is None
            assert await settlement_counts(observer, schema, ctx.flow_id) == (0, 0, 0)
            assert await stage_count(observer, schema, ctx.flow_id) == 0

        assert len(successful_children) == 2
        assert await children(observer, schema, ctx.job_id) != []
        assert await cursor(observer, schema, ctx.job_id) == {"page": 0}
        committed = await children(observer, schema, ctx.job_id)
        rollback = caller.transaction()
        await rollback.start()
        try:
            await insert_domain(caller, schema, ctx.flow_id, "page one")
            await insert_stage(caller, schema, ctx.flow_id, 1)
            await emit_joined(
                ctx,
                page((12,)),
                cursor={"page": 1},
                connection=caller,
                schema=schema,
            )
        finally:
            await rollback.rollback()
        assert await children(observer, schema, ctx.job_id) == committed
        assert await cursor(observer, schema, ctx.job_id) == {"page": 0}
        assert await edge_count(observer, schema, ctx.job_id) == 2
        assert await stage_count(observer, schema, ctx.flow_id) == 1
        assert await settlement_counts(observer, schema, ctx.flow_id) == (1, 0, 0)
        return {"pages": 2}
    finally:
        if caller.is_in_transaction():
            await caller.execute("ROLLBACK")
        await caller_pool.release(caller)


@pytest.mark.integration
async def test_joined_pages_share_caller_visibility_commit_and_rollback(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: ModulePgSchema,
) -> None:
    """One pool connection hosts source claim, caller writes, children and cursor."""
    dsn = module_pg_schema.pg_dsn
    async with (
        managed_pg_pool(dsn, wf_conn, max_size=1) as one,
        application_tables(wf_conn, wf_schema),
    ):

        async def scenario(ctx: StepContext) -> object:
            return await _caller_page_scenario(
                ctx, caller_pool=one, observer=wf_conn, schema=wf_schema
            )

        flow_id, errors = await _run_source_scenario(one, wf_conn, dsn, wf_schema, scenario)
        _assert_body_completed(errors)
        assert (await row(wf_conn, wf_schema, flow_id, "source"))["status"] == "succeeded"


@pytest.mark.integration
async def test_stale_second_page_rolls_back_the_successful_first_page_and_application_writes(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The stale claim is contextual: no competing connection mutates a joined row."""
    dsn = module_pg_schema.pg_dsn

    async with application_tables(wf_conn, wf_schema):

        async def scenario(ctx: StepContext) -> object:
            stale = replace(ctx, claim_epoch=ctx.claim_epoch + 1)
            caller = await wf_pool.acquire()
            try:
                with pytest.raises(EmitFencedError):
                    async with caller.transaction():
                        await insert_domain(caller, wf_schema, ctx.flow_id, "whole unit")
                        await insert_stage(caller, wf_schema, ctx.flow_id, 0)
                        live = await emit_joined(
                            ctx,
                            page((20, 21)),
                            cursor={"page": 0},
                            connection=caller,
                            schema=wf_schema,
                        )
                        assert len(live) == 2, "the earlier page did not really succeed"
                        await insert_stage(caller, wf_schema, ctx.flow_id, 1)
                        await emit_joined(
                            stale,
                            page((22,)),
                            cursor={"page": 1},
                            connection=caller,
                            schema=wf_schema,
                        )
                assert not caller.is_in_transaction()
                persisted = await row(wf_conn, wf_schema, ctx.flow_id, "source")
                assert persisted["claim_epoch"] == ctx.claim_epoch
                assert stale.claim_epoch != persisted["claim_epoch"]
                return {"pages": 2}
            finally:
                if caller.is_in_transaction():
                    await caller.execute("ROLLBACK")
                await wf_pool.release(caller)

        flow_id, errors = await _run_source_scenario(wf_pool, wf_conn, dsn, wf_schema, scenario)
        _assert_body_completed(errors)
        source_id = await node_id(wf_conn, wf_schema, flow_id, "source")
        assert await settlement_counts(wf_conn, wf_schema, flow_id) == (0, 0, 0), (
            "the earlier successful page or application rows survived the managed rollback: "
            f"{await settlement_counts(wf_conn, wf_schema, flow_id)!r}"
        )
        assert await stage_count(wf_conn, wf_schema, flow_id) == 0
        assert await children(wf_conn, wf_schema, source_id) == [], (
            "the earlier page left durable children: "
            f"{[dict(item) for item in await children(wf_conn, wf_schema, source_id)]!r}"
        )
        assert await edge_count(wf_conn, wf_schema, source_id) == 0
        assert await cursor(wf_conn, wf_schema, source_id) is None
        assert (
            await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1 '
                "AND step_key='source' AND status='failed'",
                flow_id,
            )
            == 0
        ), "the propagating stale claim spent an internal failure retry"


@pytest.mark.integration
@pytest.mark.parametrize("refusal", ["saturated", "oversized"])
async def test_admission_refusal_rolls_back_before_any_park(
    refusal: str,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Both saturated and impossible pages leave application and engine writes uncommitted."""
    async with application_tables(wf_conn, wf_schema):

        async def scenario(ctx: StepContext) -> object:
            caller = await wf_pool.acquire()
            try:
                async with caller.transaction():
                    await emit_joined(
                        ctx,
                        page((30,)),
                        cursor={"page": 0},
                        connection=caller,
                        schema=wf_schema,
                    )
                expected: type[Exception] = (
                    EmitBackpressure if refusal == "saturated" else ValueError
                )
                with pytest.raises(expected):
                    async with caller.transaction():
                        await insert_domain(caller, wf_schema, ctx.flow_id, "must roll back")
                        await insert_stage(caller, wf_schema, ctx.flow_id, 1)
                        await emit_joined(
                            ctx,
                            page((31, 32) if refusal == "oversized" else (31,)),
                            cursor={"page": 1},
                            connection=caller,
                            schema=wf_schema,
                        )
                assert not caller.is_in_transaction()
                during = await row(wf_conn, wf_schema, ctx.flow_id, "source")
                metadata = json_dict(during["metadata"])
                assert during["status"] == "running"
                assert "blocking_reason" not in metadata and "wf_deferral" not in metadata
                assert during["attempt"] == 1, (
                    "admission was internally retried or refunded in its unit"
                )
                return {"pages": 2}
            finally:
                if caller.is_in_transaction():
                    await caller.execute("ROLLBACK")
                await wf_pool.release(caller)

        flow_id, errors = await _run_source_scenario(
            wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema, scenario, max_in_flight=1
        )
        _assert_body_completed(errors)
        source_id = await node_id(wf_conn, wf_schema, flow_id, "source")
        assert len(await children(wf_conn, wf_schema, source_id)) == 1
        assert await edge_count(wf_conn, wf_schema, source_id) == 1
        assert await cursor(wf_conn, wf_schema, source_id) == {"page": 0}
        assert await stage_count(wf_conn, wf_schema, flow_id) == 0
        assert await settlement_counts(wf_conn, wf_schema, flow_id) == (0, 0, 0)


@pytest.mark.integration
async def test_diverged_page_rolls_back_its_application_unit_without_replaying_children(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    async with application_tables(wf_conn, wf_schema):

        async def scenario(ctx: StepContext) -> object:
            caller = await wf_pool.acquire()
            try:
                async with caller.transaction():
                    await emit_joined(
                        ctx,
                        page((40,)),
                        cursor={"page": 0},
                        connection=caller,
                        schema=wf_schema,
                    )
                with pytest.raises(PageDivergedError):
                    async with caller.transaction():
                        await insert_domain(caller, wf_schema, ctx.flow_id, "diverged")
                        await insert_stage(caller, wf_schema, ctx.flow_id, 1)
                        await emit_joined(
                            ctx,
                            page((40,)),
                            cursor={"page": 1},
                            connection=caller,
                            schema=wf_schema,
                        )
                assert not caller.is_in_transaction()
                return {"pages": 2}
            finally:
                if caller.is_in_transaction():
                    await caller.execute("ROLLBACK")
                await wf_pool.release(caller)

        flow_id, errors = await _run_source_scenario(
            wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema, scenario
        )
        _assert_body_completed(errors)
        source_id = await node_id(wf_conn, wf_schema, flow_id, "source")
        assert len(await children(wf_conn, wf_schema, source_id)) == 1
        assert await edge_count(wf_conn, wf_schema, source_id) == 1
        assert await cursor(wf_conn, wf_schema, source_id) == {"page": 0}
        assert await settlement_counts(wf_conn, wf_schema, flow_id) == (0, 0, 0)
        assert await stage_count(wf_conn, wf_schema, flow_id) == 0
        assert (
            await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1 '
                "AND step_key='source' AND status='failed'",
                flow_id,
            )
            == 0
        )


@pytest.mark.integration
async def test_propagating_backpressure_rolls_back_before_the_real_park_and_wake(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The control signal escapes rollback before production parks, then wake reclaims once."""
    from taskq.workflows._emit import wake_emit_backpressure

    phase: list[str] = []
    runners: list[FlowRunner] = []
    async with application_tables(wf_conn, wf_schema):

        async def scenario(ctx: StepContext) -> object:
            if not phase:
                phase.append("initial")
            caller = await wf_pool.acquire()
            try:
                if phase == ["initial", "parked"]:
                    phase.append("resumed")
                    async with caller.transaction():
                        await emit_joined(
                            ctx,
                            page((36,)),
                            cursor={"page": 1},
                            connection=caller,
                            schema=wf_schema,
                        )
                    return {"pages": 2}
                async with caller.transaction():
                    await emit_joined(
                        ctx,
                        page((35,)),
                        cursor={"page": 0},
                        connection=caller,
                        schema=wf_schema,
                    )
                    # This baseline is deliberately committed and must remain distinguishable
                    # from every write belonging to the refused second page.
                    await insert_domain(caller, wf_schema, ctx.flow_id, "baseline")
                    await insert_stage(caller, wf_schema, ctx.flow_id, 0)
                try:
                    async with caller.transaction():
                        await insert_domain(caller, wf_schema, ctx.flow_id, "must roll back")
                        await insert_stage(caller, wf_schema, ctx.flow_id, 1)
                        await emit_joined(
                            ctx,
                            page((36,)),
                            cursor={"page": 1},
                            connection=caller,
                            schema=wf_schema,
                        )
                except EmitBackpressure:
                    # This is an observation outside the managed scope, not a commit.
                    # Re-raise so only the genuine owning runner may translate into parking.
                    assert not caller.is_in_transaction()
                    assert await settlement_counts(wf_conn, wf_schema, ctx.flow_id) == (1, 0, 0)
                    baseline = await wf_conn.fetchval(
                        f'SELECT value FROM "{wf_schema}".atomicity_domain WHERE flow_id=$1',
                        ctx.flow_id,
                    )
                    assert baseline == "baseline"
                    assert await stage_count(wf_conn, wf_schema, ctx.flow_id) == 1
                    assert await cursor(wf_conn, wf_schema, ctx.job_id) == {"page": 0}
                    before_park = await row(wf_conn, wf_schema, ctx.flow_id, "source")
                    before_park_metadata = json_dict(before_park["metadata"])
                    assert before_park["status"] == "running"
                    assert before_park["attempt"] == 1
                    assert "blocking_reason" not in before_park_metadata
                    assert "wf_emit_backpressure" not in before_park_metadata
                    await wf_conn.execute(
                        f'INSERT INTO "{wf_schema}".atomicity_park_rollback_witness '
                        "(flow_id,rolled_back) VALUES ($1,true)",
                        ctx.flow_id,
                    )
                    phase.append("parked")
                    raise
                return {"pages": 2}
            finally:
                if caller.is_in_transaction():
                    await caller.execute("ROLLBACK")
                await wf_pool.release(caller)

        flow_id, errors = await _run_source_scenario(
            wf_pool,
            wf_conn,
            module_pg_schema.pg_dsn,
            wf_schema,
            scenario,
            max_in_flight=1,
            runner_out=runners,
        )
        assert len(errors) == 1 and isinstance(errors[0], EmitBackpressure), errors
        assert runners
        source_id = await node_id(wf_conn, wf_schema, flow_id, "source")
        # This is the real production saturation and park, not a caller-caught substitute.
        parked = await row(wf_conn, wf_schema, flow_id, "source")
        parked_metadata = json_dict(parked["metadata"])
        assert parked["status"] == "scheduled" and parked["scheduled_at"] is None
        assert parked["attempt"] == 0 and parked["locked_by_worker"] is None
        assert parked["lock_expires_at"] is None and parked["last_heartbeat_at"] is None
        assert parked["snooze_count"] == 0
        assert parked_metadata["blocking_reason"] == "backpressure"
        assert json_dict(parked_metadata["wf_deferral"]) == {
            "signal": "EmitBackpressure",
            "consume_budget": False,
            "attempt": 0,
        }
        pressure = json_dict(parked_metadata["wf_emit_backpressure"])
        assert pressure["version"] == 1 and pressure["page_width"] == 1
        assert pressure["bound"] == 1 and pressure["outstanding"] == 1
        assert pressure["deferrals"] == 1 and pressure["wakes"] == 0
        assert parked_metadata["wf_source_logical_attempt"] == 1
        assert "wf_retry" not in parked_metadata
        assert (
            await wf_conn.fetchval(
                f'SELECT status::text FROM "{wf_schema}".wf_step_ledger '
                "WHERE flow_id=$1 AND step_key='source' AND job_id=$2",
                flow_id,
                source_id,
            )
            == "awaited"
        )
        witness = await wf_conn.fetchrow(
            f'SELECT rolled_back,observed_at FROM "{wf_schema}".atomicity_park_rollback_witness '
            "WHERE flow_id=$1",
            flow_id,
        )
        assert witness is not None and witness["rolled_back"] is True
        assert isinstance(witness["observed_at"], datetime)
        witness_at = witness["observed_at"]
        assert witness_at.tzinfo is not None and witness_at.utcoffset() is not None
        assert isinstance(pressure["deferred_at"], str)
        deferred_text = pressure["deferred_at"]
        deferred_at = datetime.fromisoformat(
            deferred_text.removesuffix("Z") + "+00:00"
            if deferred_text.endswith("Z")
            else deferred_text
        )
        assert deferred_at.tzinfo is not None and deferred_at.utcoffset() is not None
        # The committed rollback witness precedes or equals the production park at database
        # timestamp precision, compared as aware instants rather than formatted strings.
        assert witness_at <= deferred_at
        assert await settlement_counts(wf_conn, wf_schema, flow_id) == (1, 0, 0)
        assert await stage_count(wf_conn, wf_schema, flow_id) == 1
        assert len(await children(wf_conn, wf_schema, source_id)) == 1
        assert await edge_count(wf_conn, wf_schema, source_id) == 1
        assert await cursor(wf_conn, wf_schema, source_id) == {"page": 0}

        capable = await RealWorker.open(wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema)
        try:
            # Only a genuine terminal child frees capacity. That real finalization supplies
            # the complementary committed read; the additional fresh wake is idempotent.
            child_claim = await capable.claim("screen")
            finalized = await finalize_node(
                wf_pool,
                wf_sql,
                flow_id=flow_id,
                job_id=JobId(child_claim["id"]),
                step_key="screen",
                worker_id=capable.worker_id,
                attempt=int(child_claim["attempt"]),
                claim_epoch=int(child_claim["claim_epoch"]),
                outcome="succeeded",
                result={"value": {"screened": True}},
                map_index=35,
            )
            assert finalized.applied
            await wake_emit_backpressure(wf_pool, wf_sql, flow_id=flow_id)
            woken = await row(wf_conn, wf_schema, flow_id, "source")
            assert woken["status"] == "pending"
            assert woken["scheduled_at"] is not None
            assert woken["required_worker_schema"] == parked["required_worker_schema"]
            woken_metadata = json_dict(woken["metadata"])
            assert "blocking_reason" not in woken_metadata
            assert json_dict(woken_metadata["wf_emit_backpressure"])["wakes"] == 1
            assert await capable.run(runners[0].compiled, flow_id, "source") == "succeeded"
            resumed = await row(wf_conn, wf_schema, flow_id, "source")
            assert resumed["status"] == "succeeded" and resumed["attempt"] == 1
            assert resumed["snooze_count"] == 0
            assert resumed["required_worker_schema"] == parked["required_worker_schema"]
            assert json_dict(resumed["metadata"])["wf_source_logical_attempt"] == 1
            assert phase == ["initial", "parked", "resumed"] and len(errors) == 1
            assert len(await children(wf_conn, wf_schema, source_id)) == 2
            assert await edge_count(wf_conn, wf_schema, source_id) == 2
            assert await cursor(wf_conn, wf_schema, source_id) == {"page": 1}
            assert await wf_conn.fetchval(
                f"SELECT array_agg(status::text ORDER BY created_at,id)::text[] "
                f'FROM "{wf_schema}".wf_step_ledger '
                "WHERE flow_id=$1 AND step_key='source' AND job_id=$2",
                flow_id,
                source_id,
            ) == ["awaited", "succeeded"]
        finally:
            await capable.close()


class _SessionFacade:
    def __init__(self) -> None:
        self.methods: list[str] = []

    def is_in_transaction(self) -> bool:
        self.methods.append("is_in_transaction")
        return True

    async def execute(self, *args: object, **kwargs: object) -> object:
        self.methods.append("execute")
        raise AssertionError("a facade data method was called")

    async def fetch(self, *args: object, **kwargs: object) -> object:
        self.methods.append("fetch")
        raise AssertionError("a facade data method was called")

    async def fetchrow(self, *args: object, **kwargs: object) -> object:
        self.methods.append("fetchrow")
        raise AssertionError("a facade data method was called")

    async def fetchval(self, *args: object, **kwargs: object) -> object:
        self.methods.append("fetchval")
        raise AssertionError("a facade data method was called")


async def _typed_connection_refusal(
    ctx: StepContext,
    connection: object,
    schema: str,
    *,
    expected_page: int,
    semantic_fragments: tuple[tuple[str, ...], ...],
) -> None:
    with pytest.raises(WorkflowRunError) as refusal:
        await emit_joined(
            ctx,
            page((expected_page,)),
            cursor={"page": 0},
            connection=cast("ConnLike", connection),
            schema=schema,
        )
    assert not isinstance(refusal.value, asyncpg.PostgresError)
    message = str(refusal.value).lower()
    for family in semantic_fragments:
        assert any(fragment in message for fragment in family), (
            f"page {expected_page} refusal does not identify {family!r}: {message!r}"
        )


@pytest.mark.integration
async def test_joined_connection_validation_is_explicit_before_data_methods(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Unsupported sessions fail by contract, not by arity or a later PostgreSQL error."""
    foreign_dsn = urlsplit(module_pg_schema.pg_dsn)
    postgres_dsn = urlunsplit(foreign_dsn._replace(path="/postgres"))

    async with application_tables(wf_conn, wf_schema):

        async def scenario(ctx: StepContext) -> object:
            facade = _SessionFacade()
            await _typed_connection_refusal(
                ctx,
                facade,
                wf_schema,
                expected_page=70,
                semantic_fragments=(("connection", "session"), ("stable", "physical", "asyncpg")),
            )
            # Transaction metadata may be checked never, once, or more than once;
            # every facade data method remains forbidden in every order.
            assert all(method == "is_in_transaction" for method in facade.methods), facade.methods

            bare = await asyncpg.connect(module_pg_schema.pg_dsn)
            try:
                await _typed_connection_refusal(
                    ctx,
                    bare,
                    wf_schema,
                    expected_page=71,
                    semantic_fragments=(("connection", "session"), ("transaction",)),
                )
                await bare.execute("BEGIN")
                with pytest.raises(asyncpg.DivisionByZeroError):
                    await bare.execute("SELECT 1/0")
                await _typed_connection_refusal(
                    ctx,
                    bare,
                    wf_schema,
                    expected_page=72,
                    semantic_fragments=(("transaction",), ("aborted", "failed", "inerror")),
                )
                await bare.execute("ROLLBACK")
                async with bare.transaction():
                    await _typed_connection_refusal(
                        ctx,
                        bare,
                        "definitely_foreign_schema",
                        expected_page=73,
                        semantic_fragments=(("schema",), ("connection", "same")),
                    )
            finally:
                await bare.close()

            foreign = await asyncpg.connect(postgres_dsn)
            try:
                async with foreign.transaction():
                    await _typed_connection_refusal(
                        ctx,
                        foreign,
                        wf_schema,
                        expected_page=74,
                        semantic_fragments=(("database",), ("foreign", "mismatch", "same")),
                    )
            finally:
                await foreign.close()

            # Negative pages 70-74 have no durable or same-unit effect; only page 75
            # below is the intentional positive PoolConnectionProxy emission.
            assert await children(wf_conn, wf_schema, ctx.job_id) == []
            assert await edge_count(wf_conn, wf_schema, ctx.job_id) == 0
            assert await cursor(wf_conn, wf_schema, ctx.job_id) is None
            assert await settlement_counts(wf_conn, wf_schema, ctx.flow_id) == (0, 0, 0)
            assert await stage_count(wf_conn, wf_schema, ctx.flow_id) == 0

            caller = await wf_pool.acquire()
            try:
                assert isinstance(caller, asyncpg.pool.PoolConnectionProxy)
                pid = await caller.fetchval("SELECT pg_backend_pid()")
                async with caller.transaction():
                    emitted = await emit_joined(
                        ctx,
                        page((75,)),
                        cursor={"page": 0},
                        connection=caller,
                        schema=wf_schema,
                    )
                    assert len(emitted) == 1
                    assert await read_joined_cursor(ctx, connection=caller, schema=wf_schema) == {
                        "page": 0
                    }
                    assert await caller.fetchval("SELECT pg_backend_pid()") == pid
                assert not caller.is_in_transaction()
                return {"pages": 1}
            finally:
                if caller.is_in_transaction():
                    await caller.execute("ROLLBACK")
                await wf_pool.release(caller)

        flow_id, errors = await _run_source_scenario(
            wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema, scenario
        )
        _assert_body_completed(errors)
        source_id = await node_id(wf_conn, wf_schema, flow_id, "source")
        positive_children = await children(wf_conn, wf_schema, source_id)
        assert [item["map_index"] for item in positive_children] == [75]
        assert await edge_count(wf_conn, wf_schema, source_id) == 1
        assert await cursor(wf_conn, wf_schema, source_id) == {"page": 0}


@pytest.mark.integration
async def test_guarded_sources_refuse_direct_and_legacy_claims_before_any_body(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    calls: list[str] = []

    async def source(ctx: StepContext) -> object:
        calls.append("body")
        return {"pages": 0}

    name = new_label("guarded_source")
    app = WorkflowApp()

    @app.workflow(name)
    def flow() -> Promise[object]:
        return build(
            chain_source(_chain(new_label("records")), source, key="source", max_attempts=1)
        )

    runner = FlowRunner(app.get(name), wf_pool, wf_schema)
    flow_id = await create_guarded(runner)
    print("atomicity PostgreSQL server_version:", await wf_conn.fetchval("SHOW server_version"))
    source_id = await node_id(wf_conn, wf_schema, flow_id, "source")
    assert (await row(wf_conn, wf_schema, flow_id, "source"))["required_worker_schema"] == (
        WORKFLOW_EXECUTION_SCHEMA
    )
    with pytest.raises(WorkflowWorkerCapabilityError) as direct:
        await runner.tick(flow_id)
    assert direct.value.required_worker_schema == WORKFLOW_EXECUTION_SCHEMA
    assert calls == []

    old_worker = JobId(new_uuid())
    await wf_conn.execute(
        f"INSERT INTO \"{wf_schema}\".actor_config (actor,queue) VALUES ('wf','default') "
        "ON CONFLICT (actor) DO NOTHING"
    )
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".workers (id,hostname,pid,queues,metadata) '
        "VALUES ($1,'legacy',1,'{default}','{\"workflow_execution\":true}'::jsonb)",
        old_worker,
    )
    try:
        with pytest.raises(asyncpg.PostgresError) as direct_claim:
            await wf_conn.fetchval(
                render_direct_claim(wf_schema),
                source_id,
                old_worker,
            )
        assert direct_claim.value.sqlstate == "WF001"
        with pytest.raises(asyncpg.PostgresError) as fleet_claim:
            from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch

            await dispatch_batch(
                wf_conn,
                sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
                queues=["default"],
                limit_n=1,
                worker_id=old_worker,
                lock_lease=timedelta(seconds=30),
            )
        assert fleet_claim.value.sqlstate == "WF001"
        persisted = await row(wf_conn, wf_schema, flow_id, "source")
        assert persisted["status"] == "pending"
        assert persisted["attempt"] == 0 and persisted["locked_by_worker"] is None
        assert calls == []
        assert (
            await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1', flow_id
            )
            == 0
        )

        capable = await RealWorker.open(wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema)
        try:
            assert await capable.run(runner.compiled, flow_id, "source") == "succeeded"
        finally:
            await capable.close()
        assert calls == ["body"]
        assert (await row(wf_conn, wf_schema, flow_id, "source"))["status"] == "succeeded"
    finally:
        await wf_conn.execute(f'DELETE FROM "{wf_schema}".workers WHERE id=$1', old_worker)


@dataclass(slots=True)
class _BarrierRun:
    compiled: CompiledWorkflow
    flow_id: JobId
    source_id: JobId
    barrier_id: JobId
    worker: RealWorker

    async def close(self) -> None:
        await self.worker.close()


async def _open_barrier_run(
    pool: asyncpg.Pool,
    control: asyncpg.Connection,
    dsn: str,
    schema: str,
    source: Callable[..., Awaitable[object]],
    settle: Callable[..., Awaitable[object]],
) -> _BarrierRun:
    name = new_label("atomic_barrier")
    _app, compiled = two_node_flow(name, source, settle, _chain(new_label("records")))
    runner = FlowRunner(compiled, pool, schema)
    flow_id = await create_guarded(runner)
    worker = await RealWorker.open(pool, control, dsn, schema)
    try:
        source_id = await node_id(control, schema, flow_id, "source")
        barrier_id = await node_id(control, schema, flow_id, "settle")

        marker = await barrier_state(control, schema, barrier_id)
        assert isinstance(marker["metadata"], dict)
        assert marker["metadata"]["blocking_reason"] == "join"
        for key in ("source", "settle"):
            requirement = (await row(control, schema, flow_id, key))["required_worker_schema"]
            assert type(requirement) is int
            await worker.assert_boot_supports(requirement)
        return _BarrierRun(compiled, flow_id, source_id, barrier_id, worker)
    except BaseException:
        await worker.close()
        raise


async def _claim_emitted_children(
    worker: RealWorker, schema: str, source_id: JobId, count: int
) -> list[dict[str, object]]:
    claimed = [await worker.claim("screen") for _ in range(count)]
    remaining = await children(worker.control, schema, source_id)
    assert [item["id"] for item in claimed] == [item["id"] for item in remaining]
    assert all(item["status"] == "running" for item in remaining)
    return claimed


async def _claim_settlement_then_children(
    run: _BarrierRun, other: RealWorker, schema: str, count: int
) -> tuple[dict[str, object], list[dict[str, object]]]:
    settlement = await run.worker.claim("settle")
    running_children = await _claim_emitted_children(other, schema, run.source_id, count)
    return settlement, running_children


async def _write_callback_settlement(
    ctx: StepContext,
    schema: str,
    page_number: int,
) -> tuple[int, bool]:
    async with settlement_connection(ctx) as (connection, atomic):
        if atomic:
            await write_settlement(connection, schema, ctx.flow_id, page_number=page_number)
        else:
            async with connection.transaction():
                await write_settlement(connection, schema, ctx.flow_id, page_number=page_number)
        return int(await connection.fetchval("SELECT pg_backend_pid()")), atomic


@pytest.mark.integration
async def test_atomic_settlement_is_invisible_until_its_node_terminal_commits(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The source result is real, and uncommitted settlement rows are invisible."""
    dsn = module_pg_schema.pg_dsn
    first_page = asyncio.Event()
    finish_source = asyncio.Event()
    written = asyncio.Event()
    finish_settlement = asyncio.Event()
    invocations: list[object] = []
    atomic_connections: list[bool] = []

    async with application_tables(wf_conn, wf_schema):

        async def source(ctx: StepContext) -> object:
            await ctx.emit_batch(page((80,)), cursor={"page": 0})
            first_page.set()
            await finish_source.wait()
            await ctx.emit_batch(page((81,)), cursor={"page": 1})
            return {"pages": 2, "token": "source-result-token"}

        async def settle(ctx: StepContext, source_result: object) -> None:
            invocations.append(source_result)
            _pid, atomic = await _write_callback_settlement(ctx, wf_schema, 1)
            atomic_connections.append(atomic)
            written.set()
            await finish_settlement.wait()
            raise RuntimeError("failed settlement attempt after its writes")

        run = await _open_barrier_run(wf_pool, wf_conn, dsn, wf_schema, source, settle)
        other = await RealWorker.open(wf_pool, wf_conn, dsn, wf_schema)
        source_task = asyncio.create_task(run.worker.run(run.compiled, run.flow_id, "source"))
        settlement_task: asyncio.Task[str] | None = None
        try:
            await wait_event(first_page, "the first source page")
            pending = await barrier_state(wf_conn, wf_schema, run.barrier_id)
            assert pending["status"] == "pending" and pending["deps_pending"] == 1
            assert invocations == []
            finish_source.set()
            await asyncio.wait_for(source_task, timeout=20)
            settlement, _claimed = await _claim_settlement_then_children(run, other, wf_schema, 2)
            settlement_task = asyncio.create_task(
                run.worker.execute_claim(run.compiled, run.flow_id, settlement)
            )
            await wait_event(written, "the first settlement write")
            observed = await settlement_counts(wf_conn, wf_schema, run.flow_id)
            assert observed == (0, 0, 0), (
                "the application settlement became visible before the barrier terminal commit: "
                f"{observed!r}; transactional_step="
                f"{'present' if native_barrier_declared() else 'absent'}; "
                f"ctx.connection={['present' if value else 'absent' for value in atomic_connections]}"
            )
            finish_settlement.set()
            await asyncio.wait_for(settlement_task, timeout=20)
        finally:
            finish_source.set()
            finish_settlement.set()
            if not source_task.done():
                source_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await source_task
            if settlement_task is not None and not settlement_task.done():
                settlement_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await settlement_task
            await other.close()
            await run.close()
        assert invocations == [{"pages": 2, "token": "source-result-token"}]


@pytest.mark.integration
async def test_failed_settlement_rolls_back_then_manual_retry_commits_once(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    dsn = module_pg_schema.pg_dsn
    writes = [asyncio.Event(), asyncio.Event()]
    releases = [asyncio.Event(), asyncio.Event()]
    inputs: list[object] = []
    atomic_connections: list[bool] = []

    async with application_tables(wf_conn, wf_schema):

        async def source(ctx: StepContext) -> object:
            await ctx.emit_batch(page((90,)), cursor={"page": 0})
            return {"pages": 1, "token": "retry-input"}

        async def settle(ctx: StepContext, source_result: object) -> None:
            index = len(inputs)
            inputs.append(source_result)
            _pid, atomic = await _write_callback_settlement(ctx, wf_schema, 0)
            atomic_connections.append(atomic)
            writes[index].set()
            await releases[index].wait()
            if index == 0:
                raise RuntimeError("deliberate ordinary settlement failure")

        run = await _open_barrier_run(wf_pool, wf_conn, dsn, wf_schema, source, settle)
        other = await RealWorker.open(wf_pool, wf_conn, dsn, wf_schema)
        tasks: list[asyncio.Task[str]] = []
        try:
            await run.worker.run(run.compiled, run.flow_id, "source")
            settlement, _claimed = await _claim_settlement_then_children(run, other, wf_schema, 1)
            first = asyncio.create_task(
                run.worker.execute_claim(run.compiled, run.flow_id, settlement)
            )
            tasks.append(first)
            await wait_event(writes[0], "the failed settlement writes")
            leaked_before_release = await settlement_counts(wf_conn, wf_schema, run.flow_id)
            releases[0].set()
            await asyncio.wait_for(first, timeout=20)
            assert (await barrier_state(wf_conn, wf_schema, run.barrier_id))["status"] == "failed"
            leaked_after_failure = await settlement_counts(wf_conn, wf_schema, run.flow_id)
            assert (
                await retry_workflow_node(
                    wf_pool,
                    schema=wf_schema,
                    flow_id=run.flow_id,
                    node_key="settle",
                    reason="retry one failed settlement",
                    principal="atomicity-test",
                )
                == 1
            )
            second = asyncio.create_task(run.worker.run(run.compiled, run.flow_id, "settle"))
            tasks.append(second)
            await wait_event(writes[1], "the retried settlement writes")
            releases[1].set()
            await asyncio.wait_for(second, timeout=20)
            settled_counts = await settlement_counts(wf_conn, wf_schema, run.flow_id)
            assert leaked_before_release == leaked_after_failure == (0, 0, 0), (
                f"the failed settlement prefix was independently committed: "
                f"before={leaked_before_release!r}, after={leaked_after_failure!r}; "
                f"transactional_step={'present' if native_barrier_declared() else 'absent'}; "
                f"ctx.connection={['present' if value else 'absent' for value in atomic_connections]}"
            )
            assert settled_counts == (1, 1, 1)
            assert inputs == [{"pages": 1, "token": "retry-input"}] * 2
            _assert_atomic_connections(atomic_connections, 2)
            assert (await barrier_state(wf_conn, wf_schema, run.barrier_id))[
                "status"
            ] == "succeeded"
            ledger = await wf_conn.fetch(
                f'SELECT status::text AS status FROM "{wf_schema}".wf_step_ledger '
                "WHERE flow_id=$1 AND step_key='settle' ORDER BY attempt",
                run.flow_id,
            )
            assert [item["status"] for item in ledger] == ["failed", "succeeded"]
        finally:
            for event in releases:
                event.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
            await other.close()
            await run.close()


@asynccontextmanager
async def _kill_decrement(schema: str, control: asyncpg.Connection, barrier_id: JobId):
    table = f'"{schema}".atomicity_kill_control'
    function = f'"{schema}".atomicity_kill_decrement'
    await control.execute(
        f"CREATE TABLE {table} (barrier_id uuid PRIMARY KEY,armed boolean NOT NULL)"
    )
    try:
        await control.execute(f"INSERT INTO {table} VALUES ($1,true)", barrier_id)
        await control.execute(
            f"CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
            f"IF EXISTS (SELECT 1 FROM {table} WHERE barrier_id=NEW.id AND armed) THEN "
            "PERFORM pg_terminate_backend(pg_backend_pid()); END IF; RETURN NEW; END $$"
        )
        await control.execute(
            f"CREATE TRIGGER atomicity_kill_decrement BEFORE UPDATE OF deps_pending ON "
            f'"{schema}".jobs FOR EACH ROW WHEN (OLD.deps_pending>NEW.deps_pending) '
            f"EXECUTE FUNCTION {function}()"
        )
        yield table
    finally:
        await control.execute(f'DROP TRIGGER IF EXISTS atomicity_kill_decrement ON "{schema}".jobs')
        await control.execute(f"DROP FUNCTION IF EXISTS {function}()")
        await control.execute(f"DROP TABLE IF EXISTS {table}")


@pytest.mark.integration
async def test_committed_source_and_lost_release_heal_once_without_expiring_its_result(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Real backend death separates source terminal from marked barrier release."""
    invocations: list[object] = []

    async def source(ctx: StepContext) -> object:
        await ctx.emit_batch(page((100,)), cursor={"page": 0})
        return {"pages": 1, "token": "crash-input"}

    async def settle(ctx: StepContext, source_result: object) -> None:
        invocations.append(source_result)

    run = await _open_barrier_run(
        wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema, source, settle
    )
    try:
        async with _kill_decrement(wf_schema, wf_conn, run.barrier_id) as control_table:
            with contextlib.suppress(
                asyncpg.PostgresError, asyncpg.InterfaceError, ConnectionError
            ):
                await run.worker.run(run.compiled, run.flow_id, "source")
            source_row = await row(wf_conn, wf_schema, run.flow_id, "source")
            assert source_row["status"] == "succeeded"
            stranded = await barrier_state(wf_conn, wf_schema, run.barrier_id)
            assert stranded["status"] == "pending" and stranded["deps_pending"] == 1
            assert await fire_count(wf_conn, wf_schema, run.barrier_id) == 0
            assert json_object(source_row["result"]) == {
                "value": {"pages": 1, "token": "crash-input"}
            }
            await wf_conn.execute(
                f"UPDATE \"{wf_schema}\".jobs SET result_expires_at=now()-interval '1 second' "
                "WHERE id=$1",
                run.source_id,
            )
            assert await sweep_expired_results(wf_conn, schema=wf_schema) == 0
            assert (await row(wf_conn, wf_schema, run.flow_id, "source"))["result"] is not None
            await wf_conn.execute(
                f"UPDATE {control_table} SET armed=false WHERE barrier_id=$1", run.barrier_id
            )
            await sweep_join_rederive(wf_pool, wf_sql)
            assert (await barrier_state(wf_conn, wf_schema, run.barrier_id))["deps_pending"] == 0
            assert await fire_count(wf_conn, wf_schema, run.barrier_id) == 1
            await sweep_join_rederive(wf_pool, wf_sql)
            assert await fire_count(wf_conn, wf_schema, run.barrier_id) == 1
            assert invocations == [], "the sweep invoked the settlement body"
            # Once the join has fired, the source result no longer needs special retention.
            assert await sweep_expired_results(wf_conn, schema=wf_schema) == 1
            assert (await row(wf_conn, wf_schema, run.flow_id, "source"))["result"] is None
    finally:
        await run.close()


@pytest.mark.integration
async def test_settlement_backend_death_rolls_back_and_replay_consumes_the_same_source_result(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    module_pg_schema: ModulePgSchema,
) -> None:
    """A real killed settlement backend leaves neither a prefix nor a duplicate replay."""
    dsn = module_pg_schema.pg_dsn
    writes = [asyncio.Event(), asyncio.Event()]
    releases = [asyncio.Event(), asyncio.Event()]
    pids: list[int] = []
    inputs: list[object] = []
    atomic_connections: list[bool] = []

    async with application_tables(wf_conn, wf_schema):

        async def source(ctx: StepContext) -> object:
            await ctx.emit_batch(page((110,)), cursor={"page": 0})
            return {"pages": 1, "token": "kill-input"}

        async def settle(ctx: StepContext, source_result: object) -> None:
            index = len(inputs)
            inputs.append(source_result)
            async with settlement_connection(ctx) as (connection, atomic):
                if atomic:
                    await write_settlement(connection, wf_schema, ctx.flow_id, page_number=0)
                else:
                    async with connection.transaction():
                        await write_settlement(connection, wf_schema, ctx.flow_id, page_number=0)
                pids.append(int(await connection.fetchval("SELECT pg_backend_pid()")))
                atomic_connections.append(atomic)
                writes[index].set()
                await releases[index].wait()
                if index == 0:
                    await connection.fetchval("SELECT 1")

        run = await _open_barrier_run(wf_pool, wf_conn, dsn, wf_schema, source, settle)
        other = await RealWorker.open(wf_pool, wf_conn, dsn, wf_schema)
        tasks: list[asyncio.Task[str]] = []
        try:
            await run.worker.run(run.compiled, run.flow_id, "source")
            settlement, _claimed = await _claim_settlement_then_children(run, other, wf_schema, 1)
            first = asyncio.create_task(
                run.worker.execute_claim(run.compiled, run.flow_id, settlement)
            )
            tasks.append(first)
            await wait_event(writes[0], "the settlement PID and application write")
            assert len(pids) == 1
            assert await wf_conn.fetchval("SELECT pg_terminate_backend($1)", pids[0]) is True
            releases[0].set()
            with contextlib.suppress(
                asyncpg.PostgresError, asyncpg.InterfaceError, ConnectionError
            ):
                await asyncio.wait_for(first, timeout=20)
            killed_counts = await settlement_counts(wf_conn, wf_schema, run.flow_id)
            assert await fire_count(wf_conn, wf_schema, run.barrier_id) == 1
            assert (
                len(
                    await wf_conn.fetch(
                        f"SELECT id FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid=$1 "
                        "AND step_key='settle'",
                        run.flow_id,
                    )
                )
                == 1
            ), "backend death duplicated the barrier row"

            retried_failed = False

            async def ready_for_replay() -> bool:
                nonlocal retried_failed
                from tests.test_wf_emit_tx_pins import reclaim_and_promote

                state = await barrier_state(wf_conn, wf_schema, run.barrier_id)
                if state["status"] == "failed" and not retried_failed:
                    assert (
                        await retry_workflow_node(
                            wf_pool,
                            schema=wf_schema,
                            flow_id=run.flow_id,
                            node_key="settle",
                            reason="retry after a killed settlement",
                            principal="atomicity-test",
                        )
                        == 1
                    )
                    retried_failed = True
                    return True
                await reclaim_and_promote(wf_pool, wf_sql)
                state = await barrier_state(wf_conn, wf_schema, run.barrier_id)
                return state["status"] == "pending"

            await wait_until(ready_for_replay, label="the killed settlement claim recovery")
            second = asyncio.create_task(run.worker.run(run.compiled, run.flow_id, "settle"))
            tasks.append(second)
            await wait_event(writes[1], "the replayed settlement write")
            releases[1].set()
            await asyncio.wait_for(second, timeout=20)
            replayed_counts = await settlement_counts(wf_conn, wf_schema, run.flow_id)
            assert killed_counts == (0, 0, 0), (
                "the killed settlement backend left durable application rows: "
                f"{killed_counts!r}; replay reached counts={replayed_counts!r}"
            )
            assert replayed_counts == (1, 1, 1)
            assert inputs == [{"pages": 1, "token": "kill-input"}] * 2
            _assert_atomic_connections(atomic_connections, 2)
            assert (await barrier_state(wf_conn, wf_schema, run.barrier_id))[
                "status"
            ] == "succeeded"
            assert await fire_count(wf_conn, wf_schema, run.barrier_id) == 1
            assert (
                await wf_conn.fetchval(
                    f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1 '
                    "AND step_key='settle' AND status='succeeded'",
                    run.flow_id,
                )
                == 1
            )
        finally:
            for event in releases:
                event.set()
            for task in tasks:
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
            await other.close()
            await run.close()


@pytest.mark.integration
async def test_committed_settlement_survives_a_later_emitted_child_failure(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    module_pg_schema: ModulePgSchema,
) -> None:
    """All-emitted settlement does not imply all-emitted children succeeded."""
    dsn = module_pg_schema.pg_dsn
    inputs: list[object] = []

    async with application_tables(wf_conn, wf_schema):

        async def source(ctx: StepContext) -> object:
            await ctx.emit_batch(page((120, 121)), cursor={"page": 0})
            return {"pages": 1, "token": "post-settlement-child"}

        async def settle(ctx: StepContext, source_result: object) -> None:
            inputs.append(source_result)
            await _write_callback_settlement(ctx, wf_schema, 0)

        run = await _open_barrier_run(wf_pool, wf_conn, dsn, wf_schema, source, settle)
        other = await RealWorker.open(wf_pool, wf_conn, dsn, wf_schema)
        try:
            await run.worker.run(run.compiled, run.flow_id, "source")
            settlement, claimed = await _claim_settlement_then_children(run, other, wf_schema, 2)
            assert (
                await run.worker.execute_claim(run.compiled, run.flow_id, settlement) == "succeeded"
            )
            assert await settlement_counts(wf_conn, wf_schema, run.flow_id) == (1, 1, 1)
            assert inputs == [{"pages": 1, "token": "post-settlement-child"}]
            assert (await barrier_state(wf_conn, wf_schema, run.barrier_id))[
                "status"
            ] == "succeeded"
            running = await children(wf_conn, wf_schema, run.source_id)
            assert len(running) == 2 and all(item["status"] == "running" for item in running)
            assert (
                await wf_conn.fetchval(
                    f'SELECT status::text FROM "{wf_schema}".jobs WHERE id=$1', run.flow_id
                )
                == "running"
            )

            failing = claimed[0]
            applied = await finalize_node(
                wf_pool,
                wf_sql,
                flow_id=run.flow_id,
                job_id=JobId(failing["id"]),
                step_key="screen",
                worker_id=other.worker_id,
                attempt=int(failing["attempt"]),
                claim_epoch=int(failing["claim_epoch"]),
                outcome="failed",
                error_class="LaterChildFailure",
                error_message="one emitted child failed after application settlement",
                map_index=120,
            )
            assert applied.applied
            sibling = claimed[1]
            sibling_result = await finalize_node(
                wf_pool,
                wf_sql,
                flow_id=run.flow_id,
                job_id=JobId(sibling["id"]),
                step_key="screen",
                worker_id=other.worker_id,
                attempt=int(sibling["attempt"]),
                claim_epoch=int(sibling["claim_epoch"]),
                outcome="succeeded",
                result={"value": {"screened": True}},
                map_index=121,
            )
            assert sibling_result.applied
            await wf_conn.execute(wf_sql.workflow_root_maintain, 200)
            assert (
                await wf_conn.fetchval(
                    f'SELECT status::text FROM "{wf_schema}".jobs WHERE id=$1', run.flow_id
                )
                == "failed"
            )
            assert await settlement_counts(wf_conn, wf_schema, run.flow_id) == (1, 1, 1)
            barrier = await barrier_state(wf_conn, wf_schema, run.barrier_id)
            assert barrier["status"] == "succeeded"
            barrier_metadata = barrier["metadata"]
            assert isinstance(barrier_metadata, dict)
            assert barrier_metadata["blocking_reason"] == "join"
            assert await fire_count(wf_conn, wf_schema, run.barrier_id) == 1
            assert (
                await wf_conn.fetchval(
                    f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1 '
                    "AND step_key='settle' AND status='succeeded'",
                    run.flow_id,
                )
                == 1
            )
        finally:
            await other.close()
            await run.close()


@pytest.mark.integration
async def test_stale_settlement_no_match_rolls_back_every_application_write(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    dsn = module_pg_schema.pg_dsn
    written = asyncio.Event()
    release = asyncio.Event()
    calls: list[object] = []
    atomic_connections: list[bool] = []

    async with application_tables(wf_conn, wf_schema):

        async def source(ctx: StepContext) -> object:
            await ctx.emit_batch(page((130,)), cursor={"page": 0})
            return {"pages": 1, "token": "stale-settlement"}

        async def settle(ctx: StepContext, source_result: object) -> None:
            calls.append(source_result)
            _pid, atomic = await _write_callback_settlement(ctx, wf_schema, 0)
            atomic_connections.append(atomic)
            # Finalization has not locked root then node, so this changes only the claim fence.
            await wf_conn.execute(
                f'UPDATE "{wf_schema}".jobs SET claim_epoch=claim_epoch+1 WHERE id=$1',
                run.barrier_id,
            )
            written.set()
            await release.wait()

        run = await _open_barrier_run(wf_pool, wf_conn, dsn, wf_schema, source, settle)
        other = await RealWorker.open(wf_pool, wf_conn, dsn, wf_schema)
        task: asyncio.Task[str] | None = None
        try:
            await run.worker.run(run.compiled, run.flow_id, "source")
            settlement, _claimed = await _claim_settlement_then_children(run, other, wf_schema, 1)
            task = asyncio.create_task(
                run.worker.execute_claim(
                    run.compiled, run.flow_id, settlement, expect_same_claim=False
                )
            )
            await wait_event(written, "the stale settlement finalization")
            release.set()
            await asyncio.wait_for(task, timeout=20)
            assert await settlement_counts(wf_conn, wf_schema, run.flow_id) == (0, 0, 0), (
                "a no-match node fence committed its stale application writes; "
                f"transactional_step={'present' if native_barrier_declared() else 'absent'}; "
                f"ctx.connection={['present' if value else 'absent' for value in atomic_connections]}"
            )
            assert calls == [{"pages": 1, "token": "stale-settlement"}]
            state = await barrier_state(wf_conn, wf_schema, run.barrier_id)
            assert state["attempt"] == settlement["attempt"]
            assert state["claim_epoch"] == int(settlement["claim_epoch"]) + 1
            assert state["required_worker_schema"] == settlement["required_worker_schema"]
            assert await fire_count(wf_conn, wf_schema, run.barrier_id) == 1
            assert (
                len(
                    await wf_conn.fetch(
                        f"SELECT id FROM \"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid=$1 "
                        "AND step_key='settle'",
                        run.flow_id,
                    )
                )
                == 1
            )
            assert (
                await wf_conn.fetchval(
                    f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1 '
                    "AND step_key='settle' AND status='fenced'",
                    run.flow_id,
                )
                == 1
            )
        finally:
            release.set()
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await other.close()
            await run.close()


@pytest.mark.integration
async def test_cancelled_joined_body_releases_a_clean_connection_and_replays_under_the_same_claim(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    async with application_tables(wf_conn, wf_schema):

        async def scenario(ctx: StepContext) -> object:
            caller = await wf_pool.acquire()
            entered = asyncio.Event()
            never = asyncio.Event()
            page_written = False

            async def body() -> None:
                nonlocal page_written
                try:
                    async with caller.transaction():
                        await insert_domain(caller, wf_schema, ctx.flow_id, "cancelled page")
                        await insert_stage(caller, wf_schema, ctx.flow_id, 0)
                        await emit_joined(
                            ctx,
                            page((140, 141)),
                            cursor={"page": 0},
                            connection=caller,
                            schema=wf_schema,
                        )
                        page_written = True
                        entered.set()
                        await never.wait()
                except BaseException:
                    entered.set()
                    raise

            task = asyncio.create_task(body())
            try:
                await wait_event(entered, "the joined page or its actual setup failure")
                if not page_written:
                    await task
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert not caller.is_in_transaction()
                assert await caller.fetchval("SELECT 1") == 1
                assert await children(wf_conn, wf_schema, ctx.job_id) == []
                assert await settlement_counts(wf_conn, wf_schema, ctx.flow_id) == (0, 0, 0)
                source = await row(wf_conn, wf_schema, ctx.flow_id, "source")
                assert source["attempt"] == ctx.attempt and source["status"] == "running"
                assert "wf_repend" not in json_dict(source["metadata"])
                async with caller.transaction():
                    replayed = await emit_joined(
                        ctx,
                        page((140, 141)),
                        cursor={"page": 0},
                        connection=caller,
                        schema=wf_schema,
                    )
                assert len(replayed) == 2
                return {"pages": 1}
            finally:
                if not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
                if caller.is_in_transaction():
                    await caller.execute("ROLLBACK")
                await wf_pool.release(caller)

        flow_id, errors = await _run_source_scenario(
            wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema, scenario
        )
        _assert_body_completed(errors)
        source_id = await node_id(wf_conn, wf_schema, flow_id, "source")
        assert len(await children(wf_conn, wf_schema, source_id)) == 2
        assert await edge_count(wf_conn, wf_schema, source_id) == 2
        assert await cursor(wf_conn, wf_schema, source_id) == {"page": 0}
        assert (
            await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1 '
                "AND step_key='source' AND status IN ('failed','fenced')",
                flow_id,
            )
            == 0
        )


async def _settlement_cancel_order(
    *,
    cancel_first: bool,
    control: asyncpg.Connection,
    schema: str,
    pool: asyncpg.Pool,
    dsn: str,
) -> None:
    written = asyncio.Event()
    release = asyncio.Event()
    calls: list[object] = []
    atomic_connections: list[bool] = []
    async with application_tables(control, schema):

        async def source(ctx: StepContext) -> object:
            await ctx.emit_batch(page((150 if cancel_first else 151,)), cursor={"page": 0})
            return {"pages": 1, "token": "cancel-input"}

        async def settle(ctx: StepContext, source_result: object) -> None:
            calls.append(source_result)
            _pid, atomic = await _write_callback_settlement(ctx, schema, 0)
            atomic_connections.append(atomic)
            written.set()
            await release.wait()

        run = await _open_barrier_run(pool, control, dsn, schema, source, settle)
        other = await RealWorker.open(pool, control, dsn, schema)
        task: asyncio.Task[str] | None = None
        try:
            await run.worker.run(run.compiled, run.flow_id, "source")
            settlement, _claimed = await _claim_settlement_then_children(run, other, schema, 1)
            task = asyncio.create_task(
                run.worker.execute_claim(run.compiled, run.flow_id, settlement)
            )
            await wait_event(written, "the cancel/settlement write handshake")
            if cancel_first:
                await cancel_workflow_run(
                    pool,
                    schema=schema,
                    flow_id=run.flow_id,
                    reason="cancel before commit",
                    principal="atomicity-test",
                )
            release.set()
            await asyncio.wait_for(task, timeout=20)
            if not cancel_first:
                await cancel_workflow_run(
                    pool,
                    schema=schema,
                    flow_id=run.flow_id,
                    reason="cancel after commit",
                    principal="atomicity-test",
                )
            counts = await settlement_counts(control, schema, run.flow_id)
            expected = (0, 0, 0) if cancel_first else (1, 1, 1)
            assert counts == expected, (
                f"cancel_first={cancel_first} left application/TaskQ settlement effects: "
                f"{counts!r}; transactional_step="
                f"{'present' if native_barrier_declared() else 'absent'}; "
                f"ctx.connection={['present' if value else 'absent' for value in atomic_connections]}"
            )
            _assert_atomic_connections(atomic_connections, 1)
            assert calls == [{"pages": 1, "token": "cancel-input"}]
            barrier = await barrier_state(control, schema, run.barrier_id)
            assert barrier["status"] == ("cancelled" if cancel_first else "succeeded")
            assert (
                await control.fetchval(
                    f'SELECT status::text FROM "{schema}".jobs WHERE id=$1', run.flow_id
                )
                == "cancelled"
            )
            assert await fire_count(control, schema, run.barrier_id) == 1
        finally:
            release.set()
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await other.close()
            await run.close()


@pytest.mark.integration
@pytest.mark.parametrize("cancel_first", [True, False])
async def test_cancel_and_settlement_have_two_defined_serializations(
    cancel_first: bool,
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    await _settlement_cancel_order(
        cancel_first=cancel_first,
        control=wf_conn,
        schema=wf_schema,
        pool=wf_pool,
        dsn=module_pg_schema.pg_dsn,
    )


@pytest.mark.integration
async def test_cancel_and_finalize_contend_on_the_real_root_then_node_rows(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Actual row locks, not merely one convenient racing interleaving, guard the order."""
    from tests._wf_fixtures import claim_view, seed_flow, seed_running_node

    for _ in range(3):
        flow_id = await seed_flow(wf_conn, wf_schema)
        settlement_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="settle")
        worker, attempt, epoch = await claim_view(wf_conn, wf_schema, settlement_id)
        resources = AsyncExitStack()
        backend_pids: dict[str, list[int]] = {"finalize": [], "cancel": []}
        lock_diagnostics: set[str] = set()

        holder = await wf_pool.acquire()
        tx = holder.transaction()
        await tx.start()
        tasks: tuple[asyncio.Task[object], asyncio.Task[int]] | None = None
        try:  # Every entered role/connection pool closes even on cancellation.
            finalize_pool = await resources.enter_async_context(
                managed_pg_pool(
                    module_pg_schema.pg_dsn,
                    wf_conn,
                    max_size=1,
                    pool_factory=partial(
                        _role_backend_pool, role="finalize", backend_pids=backend_pids
                    ),
                )
            )
            cancel_pool = await resources.enter_async_context(
                managed_pg_pool(
                    module_pg_schema.pg_dsn,
                    wf_conn,
                    max_size=1,
                    pool_factory=partial(
                        _role_backend_pool, role="cancel", backend_pids=backend_pids
                    ),
                )
            )
            assert (
                await holder.fetchval(
                    f'SELECT id FROM "{wf_schema}".jobs WHERE id=$1 FOR UPDATE', flow_id
                )
                == flow_id
            )
            assert (
                await holder.fetchval(
                    f'SELECT id FROM "{wf_schema}".jobs WHERE id=$1 FOR UPDATE', settlement_id
                )
                == settlement_id
            )
            finalize = asyncio.create_task(
                finalize_node(
                    finalize_pool,
                    wf_sql,
                    flow_id=flow_id,
                    job_id=settlement_id,
                    step_key="settle",
                    worker_id=worker,
                    attempt=attempt,
                    claim_epoch=epoch,
                    outcome="succeeded",
                    result={"value": {"settled": True}},
                )
            )
            cancel = asyncio.create_task(
                cancel_workflow_run(
                    cancel_pool,
                    schema=wf_schema,
                    flow_id=flow_id,
                    reason="contended cancel",
                    principal="atomicity-test",
                )
            )
            tasks = (finalize, cancel)

            # Both role-bound backend PIDs must wait on this run's root lock, not any DB wait.
            await wait_until(
                partial(
                    _matched_root_waiters,
                    wf_conn,
                    wf_schema,
                    flow_id,
                    tasks,
                    backend_pids,
                    lock_diagnostics,
                ),
                label="both root-then-node contenders",
            )
            print("contender backend identities:", backend_pids)
            await tx.rollback()
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True), timeout=10
            )
            errors = [result for result in results if isinstance(result, BaseException)]
            assert errors == [], f"root then node contention produced {errors!r}"
            node_status = (await row(wf_conn, wf_schema, flow_id, "settle"))["status"]
            root_status = await wf_conn.fetchval(
                f'SELECT status::text FROM "{wf_schema}".jobs WHERE id=$1', flow_id
            )
            assert (node_status, root_status) in {
                ("succeeded", "cancelled"),  # settlement commit serialized first
                ("cancelled", "cancelled"),  # cancellation committed first
            }
            terminal_events = await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".job_events '
                "WHERE job_id=$1 AND kind='state_change' "
                "AND detail->>'to_state'=$2",
                settlement_id,
                node_status,
            )
            assert terminal_events == 1, (
                f"mismatched outcome facts: node={node_status!r}, root={root_status!r}, "
                f"matching terminal event count={terminal_events!r}"
            )
        finally:
            if tasks is not None:
                for task in tasks:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            if holder.is_in_transaction():
                await tx.rollback()
            await wf_pool.release(holder)
            await resources.aclose()


async def _role_backend_pool(
    dsn: str, *, role: str, backend_pids: dict[str, list[int]]
) -> asyncpg.Pool[asyncpg.Record]:
    return await asyncpg.create_pool(
        dsn,
        min_size=1,
        max_size=1,
        setup=partial(_observe_backend_pid, role=role, backend_pids=backend_pids),
    )


async def _observe_backend_pid(
    connection: ConnLike, *, role: str, backend_pids: dict[str, list[int]]
) -> None:
    pid = await connection.fetchval("SELECT pg_backend_pid()")
    assert type(pid) is int
    backend_pids[role].append(pid)


async def _matched_root_waiters(
    conn: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    tasks: tuple[asyncio.Task[object], asyncio.Task[int]],
    backend_pids: dict[str, list[int]],
    diagnostics: set[str],
) -> bool:
    for role, task in zip(("finalize", "cancel"), tasks, strict=True):
        if not task.done():
            continue
        try:
            outcome = task.result()
        except BaseException as exc:
            raise AssertionError(f"{role} failed before root contention: {exc!r}") from exc
        raise AssertionError(f"{role} completed early with outcome {outcome!r}")
    expected = {pid for pids in backend_pids.values() for pid in pids}
    actual = await row_lock_waiters(conn, schema, flow_id)
    waiting_activities: list[asyncpg.Record] = []
    if len(expected) == 2:
        waiting_activities = await conn.fetch(
            "SELECT pid FROM pg_stat_activity "
            "WHERE datname = current_database() "
            "AND pid=ANY($1::int[]) AND wait_event_type='Lock' "
            "AND cardinality(pg_blocking_pids(pid)) > 0 "
            "AND position($2 in query) > 0",
            sorted(expected),
            f"{schema}.jobs",
        )
    key = repr((sorted(expected), sorted(actual)))
    if actual != expected and key not in diagnostics:
        diagnostics.add(key)
        activities = await conn.fetch(
            "SELECT pid,state,wait_event_type,wait_event,pg_blocking_pids(pid) AS blockers,"
            "left(query,120) AS statement FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid=ANY($1::int[])",
            sorted(expected),
        )
        print(
            "root lock mismatch:",
            flow_id,
            {"expected": sorted(expected), "actual": sorted(actual)},
            [dict(item) for item in activities],
        )
    matched = (
        bool(actual & expected) and {int(item["pid"]) for item in waiting_activities} == expected
    )
    if matched:
        print(
            "matched root-then-node waiter PIDs:",
            flow_id,
            {"roles": backend_pids, "target": flow_id},
        )
    return matched


@pytest.mark.integration
async def test_gated_and_unconditional_heartbeats_skip_a_contested_settlement_and_preserve_disowned(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Skip only a brief CAS→COMMIT row, never abandon a long-held claim.

    Before the terminal CAS both seats renew the settlement normally. During the real
    root→node critical section, the sibling still renews under the derived command budget.
    The exemption is only the single terminal round trip; a connection that stays locked
    beyond its actual deadline is outside this proof and is never licensed to lapse siblings.
    """
    from tests._wf_fixtures import seed_flow, seed_running_node

    dsn: str = module_pg_schema.pg_dsn
    flow_id = await seed_flow(wf_conn, wf_schema)
    settlement_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="settle")
    sibling_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="sibling")
    disowned_id = await seed_running_node(wf_conn, wf_schema, flow_id, step_key="disowned")
    worker = await RealWorker.open(wf_pool, wf_conn, dsn, wf_schema)
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET locked_by_worker=$2,lock_expires_at=now()-interval '1 second' "
        "WHERE id=ANY($1::uuid[])",
        [settlement_id, sibling_id, disowned_id],
        worker.worker_id,
    )
    from taskq.testing.settings import make_integration_settings_dict

    settings = WorkerSettings.load_from_dict(
        {
            **make_integration_settings_dict(dsn),
            "TASKQ_SCHEMA_NAME": wf_schema,
            "TASKQ_HEARTBEAT_INTERVAL": "0.5",
            "TASKQ_HEARTBEAT_COMMAND_TIMEOUT": "0.1",
            "TASKQ_LOCK_LEASE": "3.0",
        }
    )
    lease = timedelta(seconds=settings.lock_lease)
    threshold = _lease_renewal_threshold(
        lease,
        settings.heartbeat_interval,
        settings.max_heartbeat_failures,
        settings.heartbeat_command_timeout,
    )
    live_sql, gated_sql, reservations_sql = build_heartbeat_sql(
        wf_schema, renewal_threshold=threshold
    )
    heartbeat_resources = AsyncExitStack()
    heartbeat_pool = await heartbeat_resources.enter_async_context(
        managed_pg_pool(
            dsn,
            wf_conn,
            max_size=1,
            command_timeout=settings.heartbeat_command_timeout,
        )
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=heartbeat_pool,
        heartbeat_pool=heartbeat_pool,
        worker_pool=heartbeat_pool,
        notify_conn=None,
        leader_conn=None,
    )
    backend = PostgresBackend(
        deps,
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=settings.cancellation_grace_period),
        cleanup_grace_period=timedelta(seconds=settings.cleanup_grace_period),
    )

    async def gated_tick() -> int:
        # Production bounds the role-pool acquire by one interval. Only once it lands
        # does one command budget include BEGIN, all three writes, and COMMIT.
        async with heartbeat_pool.acquire(timeout=settings.heartbeat_interval) as conn:
            async with asyncio.timeout(settings.heartbeat_command_timeout):
                async with conn.transaction():
                    await conn.execute(live_sql, worker.worker_id, "{}")
                    tag = await conn.execute(
                        gated_sql, worker.worker_id, lease, [disowned_id], threshold
                    )
                    await conn.execute(reservations_sql, worker.worker_id, lease, [disowned_id])
                    return parse_rowcount(tag)

    initial_leases = {
        key: (await row(wf_conn, wf_schema, flow_id, key))["lock_expires_at"]
        for key in ("settle", "sibling", "disowned")
    }
    pre_cas_gated = await gated_tick()
    pre_cas_unconditional = await backend.heartbeat_jobs(
        worker.worker_id, lease, disowned=[disowned_id]
    )
    assert pre_cas_gated == pre_cas_unconditional == 2
    pre_cas_leases = {
        key: (await row(wf_conn, wf_schema, flow_id, key))["lock_expires_at"]
        for key in ("settle", "sibling", "disowned")
    }
    assert pre_cas_leases["settle"] > initial_leases["settle"], (
        "ordinary pre-CAS renewal must renew the settlement row, not permanently skip it"
    )
    assert pre_cas_leases["sibling"] > initial_leases["sibling"]
    assert pre_cas_leases["disowned"] == initial_leases["disowned"]

    # Both rows are now renewal candidates for the opposed critical-section leg. The
    # pre-CAS proof above is separate: it cannot accidentally make the gated renewal
    # return zero merely because neither candidate is due any more.
    await wf_conn.execute(
        f"UPDATE \"{wf_schema}\".jobs SET lock_expires_at=now()-interval '1 second' "
        "WHERE id=ANY($1::uuid[])",
        [settlement_id, sibling_id],
    )

    holder = await wf_pool.acquire()
    hold = holder.transaction()
    await hold.start()
    sibling_finalize: asyncio.Task[object] | None = None
    finalize_started = 0.0
    try:
        # This is the settlement critical section's actual root→node lock order.
        await holder.execute(f'SELECT 1 FROM "{wf_schema}".jobs WHERE id=$1 FOR UPDATE', flow_id)
        await holder.execute(
            f'SELECT 1 FROM "{wf_schema}".jobs WHERE id=$1 FOR UPDATE', settlement_id
        )
        before = {
            key: (await row(wf_conn, wf_schema, flow_id, key))["lock_expires_at"]
            for key in ("settle", "sibling", "disowned")
        }
        gated_rows = 0
        gated_failure: BaseException | None = None
        try:
            gated_rows = await gated_tick()
        except (TimeoutError, asyncpg.PostgresError) as exc:
            gated_failure = exc

        unconditional_rows = 0
        unconditional_failure: BaseException | None = None
        try:
            async with asyncio.timeout(
                settings.heartbeat_interval + settings.heartbeat_command_timeout
            ):
                unconditional_rows = await backend.heartbeat_jobs(
                    worker.worker_id, lease, disowned=[disowned_id]
                )
        except (TimeoutError, asyncpg.PostgresError) as exc:
            unconditional_failure = exc

        after = {
            key: (await row(wf_conn, wf_schema, flow_id, key))["lock_expires_at"]
            for key in ("settle", "sibling", "disowned")
        }
        # The sibling starts while root→node locks are actually contended. Releasing
        # the one terminal round trip lets its own root→node finalization proceed.
        finalize_started = time.monotonic()
        sibling_finalize = asyncio.create_task(
            finalize_node(
                wf_pool,
                wf_sql,
                flow_id=flow_id,
                job_id=sibling_id,
                step_key="sibling",
                worker_id=worker.worker_id,
                attempt=1,
                claim_epoch=0,
                outcome="succeeded",
                result={"value": {"sibling": True}},
            )
        )
        await wait_until(
            lambda: _one_root_waiter(wf_conn, wf_schema, flow_id),
            label="the sibling finalization waiting on the actual root tuple",
        )
        await hold.rollback()
        finalize_bound_s = settings.heartbeat_interval + settings.heartbeat_command_timeout
        try:
            await asyncio.wait_for(sibling_finalize, timeout=finalize_bound_s)
        finally:
            finalize_elapsed = time.monotonic() - finalize_started
        assert finalize_elapsed <= finalize_bound_s, (
            f"sibling finalization took {finalize_elapsed:.3f}s beyond one derived "
            f"heartbeat command cycle ({finalize_bound_s}s)"
        )
        assert (await row(wf_conn, wf_schema, flow_id, "sibling"))["status"] == "succeeded"
        print(
            "contested sibling finalization:",
            {
                "elapsed_s": finalize_elapsed,
                "bound_s": finalize_bound_s,
                "root_lock": flow_id,
            },
        )
        assert (
            gated_failure is None
            and unconditional_failure is None
            and gated_rows == unconditional_rows == 1
        ), (
            "a heartbeat seat was starved or renewed the wrong row set under its derived budget: "
            f"tick_budget={settings.heartbeat_command_timeout}s, interval={settings.heartbeat_interval}s, "
            f"lock_lease={settings.lock_lease}s, renewal_threshold={threshold!r}; "
            f"gated_rows={gated_rows}, unconditional_rows={unconditional_rows}, "
            f"gated={gated_failure!r}, unconditional={unconditional_failure!r}, "
            f"before={before!r}, after={after!r}"
        )
        assert after["sibling"] > before["sibling"]
        assert after["settle"] == before["settle"], (
            "the contested settlement lease was rewritten instead of skipped"
        )
        assert after["disowned"] == before["disowned"], (
            "a disowned row was renewed by one of the shared ownership cores"
        )
        assert (await row(wf_conn, wf_schema, flow_id, "disowned"))["status"] == "running"
        assert (await row(wf_conn, wf_schema, flow_id, "disowned"))[
            "locked_by_worker"
        ] == worker.worker_id
    finally:
        # The successful contention leg already rolled the holder back. Never issue a
        # second terminal operation on that completed asyncpg transaction during teardown.
        if holder.is_in_transaction():
            await hold.rollback()
        if sibling_finalize is not None:
            if not sibling_finalize.done():
                sibling_finalize.cancel()
            await asyncio.gather(sibling_finalize, return_exceptions=True)
        await wf_pool.release(holder)
        await heartbeat_resources.aclose()
        await worker.close()


@pytest.mark.integration
async def test_gated_heartbeat_explain_uses_the_due_and_ownership_indexes(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The three-way shortlist has executable index plans at a populated fixture size."""
    from tests._wf_fixtures import seed_flow, seed_running_node

    flow_id = await seed_flow(wf_conn, wf_schema)
    worker = await RealWorker.open(wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema)
    try:
        for index in range(512):
            await seed_running_node(wf_conn, wf_schema, flow_id, step_key=f"held_{index}")
        await wf_conn.execute(
            f'UPDATE "{wf_schema}".jobs SET locked_by_worker=$1,'
            "lock_expires_at=CASE WHEN step_key='held_0' THEN now()-interval '1 second' "
            "ELSE now()+interval '1 day' END WHERE parent_id IS NULL AND step_key LIKE 'held_%'",
            worker.worker_id,
        )
        lease = timedelta(seconds=worker.settings.lock_lease)
        threshold = _lease_renewal_threshold(
            lease,
            worker.settings.heartbeat_interval,
            worker.settings.max_heartbeat_failures,
            worker.settings.heartbeat_command_timeout,
        )
        statement = build_heartbeat_sql(wf_schema, renewal_threshold=threshold)[1]
        async with wf_conn.transaction():
            await wf_conn.execute(f'ANALYZE "{wf_schema}".jobs')
            await wf_conn.execute(
                "PREPARE atomicity_heartbeat_plan(uuid,interval,uuid[],interval) AS " + statement
            )
            execute_plan = (
                "EXPLAIN (FORMAT JSON) EXECUTE atomicity_heartbeat_plan("
                f"'{worker.worker_id}'::uuid,'{lease.total_seconds()} seconds'::interval,"
                f"ARRAY[]::uuid[],'{threshold.total_seconds()} seconds'::interval)"
            )
            default_raw = await wf_conn.fetchval(execute_plan)
            # A second plan is a planner-influence diagnostic only and never supplies the proof.
            await wf_conn.execute("SET LOCAL enable_seqscan=off")
            influenced_raw = await wf_conn.fetchval(execute_plan)
            await wf_conn.execute("DEALLOCATE atomicity_heartbeat_plan")
        default_plan = json.dumps(json_object(default_raw))
        influenced_plan = json.dumps(json_object(influenced_raw))
        print("default-planner heartbeat plan:", default_plan)
        print("supplementary enable_seqscan=off plan:", influenced_plan)
        assert "Seq Scan" not in default_plan, default_plan
        assert "jobs_running_lock_expires_idx" in default_plan, default_plan
        assert (
            "jobs_running_heartbeat_deadline_idx" in default_plan
            or "jobs_locked_by_worker_running_idx" in default_plan
        ), default_plan
        assert '"Node Type": "Append"' in default_plan, default_plan
        assert influenced_plan
    finally:
        await worker.close()


@pytest.mark.integration
async def test_pool_of_one_settles_then_runs_tx2_on_the_same_still_held_connection(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: ModulePgSchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A downstream decrement observes the settlement backend on a one-connection pool."""
    dsn: str = module_pg_schema.pg_dsn
    callback_pids: list[int] = []
    atomic_connections: list[bool] = []
    after_calls: list[object] = []
    after_activity: list[object] = []
    resource_order: list[tuple[str, object]] = []

    class ObservedPool(asyncpg.Pool):
        def __init__(
            self,
            *connect_args: object,
            min_size: int,
            max_size: int,
            max_queries: int,
            max_inactive_connection_lifetime: float,
            connect: Callable[..., Awaitable[ConnLike]] | None = None,
            setup: Callable[[ConnLike], Awaitable[object]] | None = None,
            init: Callable[[ConnLike], Awaitable[object]] | None = None,
            reset: Callable[[ConnLike], Awaitable[object]] | None = None,
            loop: asyncio.AbstractEventLoop | None,
            connection_class: type[asyncpg.Connection],
            record_class: type[asyncpg.Record],
            **connect_kwargs: object,
        ) -> None:
            super().__init__(
                *connect_args,
                min_size=min_size,
                max_size=max_size,
                max_queries=max_queries,
                max_inactive_connection_lifetime=max_inactive_connection_lifetime,
                connect=connect,
                setup=setup,
                init=init,
                reset=reset,
                loop=loop,
                connection_class=connection_class,
                record_class=record_class,
                **connect_kwargs,
            )

        async def release(
            self,
            connection: asyncpg.pool.PoolConnectionProxy[asyncpg.Record],
            *,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: asyncpg Pool.release's exact public keyword is required by this override.
        ) -> None:
            pid = await connection.fetchval("SELECT pg_backend_pid()")
            await super().release(connection, timeout=timeout)
            # The event is appended only after asyncpg's actual release/reset completed.
            resource_order.append(("release", pid))

    async def observed_pool_factory(actual_dsn: str) -> asyncpg.Pool[asyncpg.Record]:
        # These are the ordinary asyncpg limits read from its actual public constructor.
        pool = ObservedPool(
            actual_dsn,
            min_size=1,
            max_size=1,
            max_queries=50_000,
            max_inactive_connection_lifetime=300.0,
            connect=None,
            setup=None,
            init=None,
            reset=None,
            loop=asyncio.get_running_loop(),
            connection_class=asyncpg.Connection,
            record_class=asyncpg.Record,
        )
        return await pool

    async with (
        managed_pg_pool(dsn, wf_conn, max_size=1, pool_factory=observed_pool_factory) as one,
        application_tables(wf_conn, wf_schema),
    ):

        async def source(ctx: StepContext) -> object:
            return {"pages": 0, "token": "pool-one"}

        async def settle(ctx: StepContext, source_result: object) -> None:
            pid, atomic = await _write_callback_settlement(ctx, wf_schema, 0)
            callback_pids.append(pid)
            atomic_connections.append(atomic)
            resource_order.append(("callback-exit", pid))

        async def after(ctx: StepContext, settlement: object) -> None:
            after_calls.append(settlement)
            activity = await wf_conn.fetchrow(
                "SELECT state,backend_xid FROM pg_stat_activity "
                "WHERE datname = current_database() AND pid=$1",
                callback_pids[0],
            )
            after_activity.append(None if activity is None else dict(activity))
            if activity is not None:
                assert activity["backend_xid"] is None, (
                    "the settlement backend was still in a transaction during the successor body"
                )
            assert await settlement_counts(wf_conn, wf_schema, run_flow) == (1, 1, 1)

        from taskq.workflows import step
        from tests._wf_atomicity_fixture import transactional_barrier

        name = new_label("pool_one_barrier")
        app = WorkflowApp()

        @app.workflow(name)
        def flow() -> Promise[object]:
            source_decl = chain_source(
                _chain(new_label("records")), source, key="source", max_attempts=1
            )
            barrier_decl = transactional_barrier(settle, source_decl, key="settle")
            return build(step(after, barrier_decl, key="after", max_attempts=1))

        from taskq.workflows._progress import (
            FLUSH_CADENCE_S,
            PROGRESS_RING_BOUND,
            ProgressEmitter,
        )

        class ObservedEmitter(ProgressEmitter):
            def __init__(
                self,
                pool: asyncpg.Pool[asyncpg.Record],
                wsql: WorkflowSql,
                *,
                flow_id: JobId,
                node_id: JobId,
                ring_bound: int = PROGRESS_RING_BOUND,
                cadence_s: float = FLUSH_CADENCE_S,
                schema_decl: type[BaseModel] | None = None,
                enabled: bool = True,
            ) -> None:
                super().__init__(
                    pool,
                    wsql,
                    flow_id=flow_id,
                    node_id=node_id,
                    ring_bound=ring_bound,
                    cadence_s=cadence_s,
                    schema_decl=schema_decl,
                    enabled=enabled,
                )
                self.observed_node_id = node_id

            async def aclose(self) -> None:
                settled = await wf_conn.fetchval(
                    f'SELECT status::text FROM "{wf_schema}".jobs '
                    "WHERE (metadata->>'flow_id')::uuid=$1 AND step_key='settle'",
                    run_flow,
                )
                if self.observed_node_id == settle_id and settled == "succeeded" and callback_pids:
                    resource_order.append(("progress-aclose", callback_pids[0]))
                await super().aclose()

        monkeypatch.setattr("taskq.workflows.api._runner.ProgressEmitter", ObservedEmitter)
        runner = FlowRunner(app.get(name), one, wf_schema)
        run_flow = await create_guarded(runner)
        settle_id = await node_id(wf_conn, wf_schema, run_flow, "settle")
        after_id = await node_id(wf_conn, wf_schema, run_flow, "after")
        await wf_conn.execute(
            f'CREATE TABLE "{wf_schema}".atomicity_tx2_pid (node_id uuid,pid integer)'
        )
        await wf_conn.execute(
            f'CREATE FUNCTION "{wf_schema}".atomicity_refuse_postterminal_application_call() '
            "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT 1 FROM "
            f"\"{wf_schema}\".jobs WHERE (metadata->>'flow_id')::uuid=NEW.flow_id "
            "AND step_key='settle' AND status='succeeded') THEN RAISE EXCEPTION "
            "'application call after settlement terminal'; END IF; RETURN NEW; END $$"
        )
        for application_table in (
            "atomicity_domain",
            "atomicity_history",
            "atomicity_applied_cursor",
        ):
            await wf_conn.execute(
                f"CREATE TRIGGER atomicity_no_drone_call BEFORE INSERT OR UPDATE ON "
                f'"{wf_schema}".{application_table} FOR EACH ROW EXECUTE FUNCTION '
                f'"{wf_schema}".atomicity_refuse_postterminal_application_call()'
            )
        await wf_conn.execute(
            f'CREATE FUNCTION "{wf_schema}".atomicity_record_tx2_pid() RETURNS trigger '
            "LANGUAGE plpgsql AS $$ BEGIN INSERT INTO "
            f'"{wf_schema}".atomicity_tx2_pid VALUES (NEW.id,pg_backend_pid()); '
            "RETURN NEW; END $$"
        )
        await wf_conn.execute(
            f"CREATE TRIGGER atomicity_record_tx2_pid BEFORE UPDATE OF deps_pending ON "
            f'"{wf_schema}".jobs FOR EACH ROW WHEN (OLD.deps_pending>NEW.deps_pending) '
            f'EXECUTE FUNCTION "{wf_schema}".atomicity_record_tx2_pid()'
        )
        worker = await RealWorker.open(one, wf_conn, dsn, wf_schema)
        try:
            await worker.run(runner.compiled, run_flow, "source")
            await asyncio.wait_for(worker.run(runner.compiled, run_flow, "settle"), timeout=10)
            assert callback_pids
            tx2_pids = [
                int(item["pid"])
                for item in await wf_conn.fetch(
                    f'SELECT pid FROM "{wf_schema}".atomicity_tx2_pid WHERE node_id=$1', after_id
                )
            ]
            await worker.run(runner.compiled, run_flow, "after")
            final_barrier = await row(wf_conn, wf_schema, run_flow, "settle")
            callback_exit = next(
                (
                    index
                    for index, event in enumerate(resource_order)
                    if event == ("callback-exit", callback_pids[0])
                ),
                -1,
            )
            releases_after_exit = [
                index
                for index, event in enumerate(resource_order)
                if event == ("release", callback_pids[0]) and index > callback_exit
            ]
            close_after_terminal = next(
                (
                    index
                    for index, event in enumerate(resource_order)
                    if event == ("progress-aclose", callback_pids[0])
                    and (not releases_after_exit or index > releases_after_exit[0])
                ),
                -1,
            )
            assert (
                after_calls == [{}]
                and len(after_activity) == 1
                and after_activity[0] is not None
                and tx2_pids == callback_pids
                and atomic_connections == [True]
                and final_barrier["status"] == "succeeded"
                and callback_exit >= 0
                and bool(releases_after_exit)
                and close_after_terminal > releases_after_exit[0]
            ), (
                "pool-one terminal/tx2/resource tail differs: "
                f"settlement={callback_pids!r}, tx2={tx2_pids!r}, after={after_activity!r}, "
                f"atomic={atomic_connections!r}, after_calls={after_calls!r}, "
                f"barrier={final_barrier['status']!r}; "
                f"transactional_step={'present' if native_barrier_declared() else 'absent'}; "
                f"ctx.connection={['present' if value else 'absent' for value in atomic_connections]}; "
                f"callback_exit={callback_exit}, releases={releases_after_exit!r}, "
                f"progress_close={close_after_terminal}, resources={resource_order!r}"
            )
        finally:
            await worker.close()
            await wf_conn.execute(
                f'DROP TRIGGER IF EXISTS atomicity_record_tx2_pid ON "{wf_schema}".jobs'
            )
            await wf_conn.execute(
                f'DROP FUNCTION IF EXISTS "{wf_schema}".atomicity_record_tx2_pid()'
            )
            await wf_conn.execute(f'DROP TABLE IF EXISTS "{wf_schema}".atomicity_tx2_pid')
            for application_table in (
                "atomicity_domain",
                "atomicity_history",
                "atomicity_applied_cursor",
            ):
                await wf_conn.execute(
                    f'DROP TRIGGER IF EXISTS atomicity_no_drone_call ON "{wf_schema}".{application_table}'
                )
            await wf_conn.execute(
                f'DROP FUNCTION IF EXISTS "{wf_schema}".atomicity_refuse_postterminal_application_call()'
            )


@pytest.mark.integration
async def test_real_committed_settlement_with_disconnected_commit_ack_is_not_reapplied(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The TCP peer disappears after PG's COMMIT and before ReadyForQuery reaches asyncpg."""
    from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch
    from tests._wf_atomicity_proxy import LostCommitProxy

    direct_dsn: str = module_pg_schema.pg_dsn
    proxy = LostCommitProxy(direct_dsn)
    proxy_dsn = await proxy.start()
    pool = await asyncio.wait_for(asyncpg.create_pool(proxy_dsn), timeout=5)
    calls: list[object] = []
    atomic_connections: list[bool] = []
    async with application_tables(wf_conn, wf_schema):

        async def source(ctx: StepContext) -> object:
            return {"pages": 0, "token": "lost-commit-ack"}

        async def settle(ctx: StepContext, source_result: object) -> None:
            calls.append(source_result)
            _pid, atomic = await _write_callback_settlement(ctx, wf_schema, 0)
            atomic_connections.append(atomic)
            proxy.arm()

        name = new_label("lost_commit_barrier")
        _app, compiled = two_node_flow(name, source, settle, _chain(new_label("records")))
        runner = FlowRunner(compiled, pool, wf_schema)
        flow_id = await create_guarded(runner)
        barrier_id = await node_id(wf_conn, wf_schema, flow_id, "settle")
        worker = await asyncio.wait_for(
            RealWorker.open(pool, wf_conn, direct_dsn, wf_schema), timeout=5
        )
        try:
            await asyncio.wait_for(worker.run(compiled, flow_id, "source"), timeout=15)
            with contextlib.suppress(
                asyncpg.PostgresError, asyncpg.InterfaceError, ConnectionError
            ):
                await asyncio.wait_for(worker.run(compiled, flow_id, "settle"), timeout=15)
            assert proxy.disconnects == 1, "the client received the commit acknowledgement"
            assert await settlement_counts(wf_conn, wf_schema, flow_id) == (1, 1, 1)
            assert (await barrier_state(wf_conn, wf_schema, barrier_id))["status"] == "succeeded"
            assert (
                await wf_conn.fetchval(
                    f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1 '
                    "AND step_key='settle' AND status='succeeded'",
                    flow_id,
                )
                == 1
            )
            _assert_atomic_connections(atomic_connections, 1)
            replay = await dispatch_batch(
                wf_conn,
                sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
                queues=["default"],
                limit_n=1,
                worker_id=worker.worker_id,
                lock_lease=timedelta(seconds=30),
            )
            assert replay == [], [dict(item) for item in replay]
            assert calls == [{"pages": 0, "token": "lost-commit-ack"}]
            assert await settlement_counts(wf_conn, wf_schema, flow_id) == (1, 1, 1)
        finally:
            await asyncio.wait_for(worker.close(), timeout=5)
            # The proxy deliberately owns disconnected transports; close them before
            # asking asyncpg to close client objects which can no longer receive a final packet.
            await asyncio.wait_for(proxy.close(), timeout=5)
            await asyncio.wait_for(pool.close(), timeout=5)
            assert proxy.open_resource_count == 0
            assert proxy.server is not None and not proxy.server.is_serving()


@pytest.mark.integration
async def test_two_worker_processes_cannot_claim_joined_children_before_caller_commit(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
    tmp_path: Path,
) -> None:
    """Real OS workers race an open joined transaction and see children only after commit."""
    from tests.system_e2e._harness import graceful_stop, reap, spawn_joined_worker
    from tests.system_e2e._wf_harness import wait_flow_terminal

    dsn: str = module_pg_schema.pg_dsn
    from taskq.testing.settings import make_integration_settings_dict

    configured = make_integration_settings_dict(dsn)
    configured["TASKQ_DEFAULT_START_TO_CLOSE"] = "20"
    # Keep the gate inside the actual workflow deadline; old 30-second waits are not inherited.
    process_settings = WorkerSettings.load_from_dict(configured)
    workflow_deadline = process_settings.default_start_to_close
    assert workflow_deadline is not None
    terminal_bound_s = (
        workflow_deadline.total_seconds()
        + 2 * process_settings.heartbeat_interval
        + process_settings.heartbeat_command_timeout
    )
    async with application_tables(wf_conn, wf_schema):
        from tests._wf_atomicity_process import APP, PROCESS_FLOW

        runner = FlowRunner(APP.get(PROCESS_FLOW), wf_pool, wf_schema)
        flow_id = await create_guarded(runner, run_key=new_label("two-process"))
        source_id = await node_id(wf_conn, wf_schema, flow_id, "source")
        await wf_conn.execute(
            f'CREATE TABLE "{wf_schema}".atomicity_process_gate '
            "(flow_id uuid PRIMARY KEY,entered boolean NOT NULL DEFAULT false,"
            "release boolean NOT NULL DEFAULT false)"
        )
        workers = []
        try:
            await wf_conn.execute(
                f'INSERT INTO "{wf_schema}".atomicity_process_gate (flow_id) VALUES ($1)', flow_id
            )
            for name in ("source-a", "source-b"):
                workers.append(
                    await spawn_joined_worker(
                        wf_conn,
                        dsn,
                        wf_schema,
                        tag=f"atomicity-{name}",
                        entry="tests._wf_atomicity_process",
                        log_sink=str(tmp_path / f"atomicity-worker-{name}.log"),
                        extra_env={
                            **configured,
                            "TASKQ_QUEUES": "default",
                            "TASKQ_SCHEMA_NAME": wf_schema,
                            "TASKQ_LOG_LEVEL": "DEBUG",
                        },
                    )
                )
            await wait_until(
                lambda: _process_entered(wf_conn, wf_schema, flow_id),
                label="the subprocess source entered its open joined transaction",
            )
            assert await children(wf_conn, wf_schema, source_id) == []
            assert await settlement_counts(wf_conn, wf_schema, flow_id) == (0, 0, 0)
            assert (
                await wf_conn.fetchval(
                    f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1 '
                    "AND step_key='screen'",
                    flow_id,
                )
                == 0
            )
            worker_b_id = await wf_conn.fetchval(
                f'SELECT id FROM "{wf_schema}".workers WHERE pid=$1', workers[1].proc.pid
            )
            assert isinstance(worker_b_id, UUID)
            worker_b_sink = tmp_path / "atomicity-worker-source-b.log"
            dispatch_start_line = len(
                (await asyncio.to_thread(worker_b_sink.read_text)).splitlines()
            )
            await wait_until(
                lambda: _dispatch_attempt_observed(
                    worker_b_sink, JobId(worker_b_id), dispatch_start_line
                ),
                label="worker B's real production dispatch inside the open-transaction window",
                bound_s=workflow_deadline.total_seconds(),
            )
            assert (await row(wf_conn, wf_schema, flow_id, "source"))["status"] == "running"
            print(
                "worker B production dispatch observed in open window:",
                {"worker_id": worker_b_id, "sink": str(worker_b_sink)},
            )
            await wf_conn.execute(
                f'UPDATE "{wf_schema}".atomicity_process_gate SET release=true WHERE flow_id=$1',
                flow_id,
            )
            status, elapsed = await wait_flow_terminal(
                wf_conn, wf_schema, str(flow_id), bound_s=terminal_bound_s
            )
            assert status == "complete", f"two-process run landed {status!r} after {elapsed:.3f}s"
            assert len(await children(wf_conn, wf_schema, source_id)) == 1
            assert await cursor(wf_conn, wf_schema, source_id) == {"page": 0}
            assert await edge_count(wf_conn, wf_schema, source_id) == 1
            assert await settlement_counts(wf_conn, wf_schema, flow_id) == (1, 0, 0)
            assert await stage_count(wf_conn, wf_schema, flow_id) == 1
            workers_rows = await wf_conn.fetch(
                f"SELECT id,metadata FROM \"{wf_schema}\".workers WHERE metadata->>'workflow_execution'='true'"
            )
            booted = [json_dict(item["metadata"]) for item in workers_rows]
            assert (
                len([item for item in booted if type(item.get("workflow_execution_schema")) is int])
                >= 2
            )
        finally:
            with contextlib.suppress(asyncpg.PostgresError):
                await wf_conn.execute(
                    f'UPDATE "{wf_schema}".atomicity_process_gate SET release=true'
                )
            for worker in workers:
                try:
                    await asyncio.to_thread(graceful_stop, worker)
                except (OSError, RuntimeError):
                    reap(worker)
            await wf_conn.execute(f'DROP TABLE IF EXISTS "{wf_schema}".atomicity_process_gate')


type StatementDoor = Literal["execute", "fetch", "fetchrow", "fetchval"]
_OtherRecord = TypeVar("_OtherRecord", bound=asyncpg.Record)


def _normalized_sql(query: str) -> str:
    """Compare statement semantics without coupling the matcher to line wrapping."""
    return " ".join(query.split())


def _is_engine_cursor_query(query: str, schema: str) -> bool:
    """Identify the fenced source-cursor UPDATE with its full claim/cancel signature."""
    statement = _normalized_sql(query)
    required = (
        "SET metadata = metadata || $2::jsonb",
        "WHERE id = $1",
        "status = 'running'",
        "locked_by_worker = $3",
        "attempt = $4",
        "claim_epoch = $5",
        "cancel_phase = 0",
        "NOT metadata ? 'cancel_phase'",
        "metadata->>'flow_id'",
        "RETURNING id",
    )
    return statement.startswith(f"UPDATE {schema}.jobs ") and all(
        fragment in statement for fragment in required
    )


async def _peer_waits_for_caller_application_row(
    control: asyncpg.Connection,
    schema: str,
    flow_id: JobId,
    ordinal: int,
    peer_pid: int,
    caller_pid: int,
    caller_xid: int,
    peer_query: str,
) -> bool:
    """Observe the peer's ungranted xid lock for its exact application-row UPDATE."""
    target_location = await control.fetchval(
        f'SELECT ctid::text FROM "{schema}".atomicity_deadlock WHERE flow_id=$1 AND ordinal=$2',
        flow_id,
        ordinal,
    )
    assert isinstance(target_location, str)
    waiting = await control.fetchrow(
        "SELECT l.pid,l.transactionid::text AS waiting_xid,a.pid AS activity_pid,"
        "a.query,a.wait_event_type,a.wait_event,pg_blocking_pids(a.pid) AS blockers "
        "FROM pg_locks AS l JOIN pg_stat_activity AS a ON a.pid=l.pid "
        "WHERE l.pid=$1 AND l.locktype='transactionid' AND NOT l.granted "
        "AND l.mode='ShareLock' AND l.transactionid::text::bigint=$3::bigint "
        "AND a.datname = current_database() "
        "AND a.wait_event_type='Lock' "
        "AND array_position(pg_blocking_pids(a.pid),$2::int) IS NOT NULL",
        peer_pid,
        caller_pid,
        caller_xid,
    )
    if waiting is None:
        return False
    assert waiting["activity_pid"] == peer_pid
    assert _normalized_sql(str(waiting["query"])) == _normalized_sql(peer_query)
    print(
        "deadlock peer application-row lock witness:",
        {
            "peer_pid": peer_pid,
            "caller_pid": caller_pid,
            "target_location": target_location,
            "waiting_xid": waiting["waiting_xid"],
            "caller_xid": caller_xid,
            "wait_event": waiting["wait_event"],
            "blockers": waiting["blockers"],
            "query": waiting["query"],
        },
    )
    return True


@pytest.mark.integration
async def test_real_deadlock_escapes_joined_scope_without_an_internal_retry(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    module_pg_schema: ModulePgSchema,
) -> None:
    """The engine cursor is the victim; every real protocol call is counted."""
    engine_cursor_calls: list[tuple[StatementDoor, str]] = []

    class CountingConnection(asyncpg.Connection):
        def _record_cursor_statement(self, door: StatementDoor, query: str) -> None:
            # Every public statement door shares this pre-await observation. Failed
            # calls count as well; unrelated reads and app writes never match the fence.
            if _is_engine_cursor_query(query, wf_schema):
                engine_cursor_calls.append((door, query))

        async def execute(
            self,
            query: str,
            *args: object,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: exact asyncpg.Connection.execute override.
        ) -> str:
            self._record_cursor_statement("execute", query)
            return await super().execute(query, *args, timeout=timeout)

        @overload
        async def fetch(
            self,
            query: str,
            *args: object,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: exact asyncpg.Connection.fetch override.
            record_class: None = None,
        ) -> list[asyncpg.Record]: ...

        @overload
        async def fetch(
            self,
            query: str,
            *args: object,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: exact asyncpg.Connection.fetch override.
            record_class: type[_OtherRecord],
        ) -> list[_OtherRecord]: ...

        @overload
        async def fetch(
            self,
            query: str,
            *args: object,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: exact asyncpg.Connection.fetch override.
            record_class: type[_OtherRecord] | None,
        ) -> list[asyncpg.Record] | list[_OtherRecord]: ...

        async def fetch(
            self,
            query: str,
            *args: object,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: exact asyncpg.Connection.fetch override.
            record_class: type[_OtherRecord] | None = None,
        ) -> list[asyncpg.Record] | list[_OtherRecord]:
            self._record_cursor_statement("fetch", query)
            return await super().fetch(query, *args, timeout=timeout, record_class=record_class)

        async def fetchval(
            self,
            query: str,
            *args: object,
            column: int = 0,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: exact asyncpg.Connection.fetchval override.
        ) -> object:
            self._record_cursor_statement("fetchval", query)
            return await super().fetchval(query, *args, column=column, timeout=timeout)

        @overload
        async def fetchrow(
            self,
            query: str,
            *args: object,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: exact asyncpg.Connection.fetchrow override.
            record_class: None = None,
        ) -> asyncpg.Record | None: ...

        @overload
        async def fetchrow(
            self,
            query: str,
            *args: object,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: exact asyncpg.Connection.fetchrow override.
            record_class: type[_OtherRecord],
        ) -> _OtherRecord | None: ...

        @overload
        async def fetchrow(
            self,
            query: str,
            *args: object,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: exact asyncpg.Connection.fetchrow override.
            record_class: type[_OtherRecord] | None,
        ) -> asyncpg.Record | _OtherRecord | None: ...

        async def fetchrow(
            self,
            query: str,
            *args: object,
            timeout: float | None = None,  # noqa: ASYNC109  # Why: exact asyncpg.Connection.fetchrow override.
            record_class: type[_OtherRecord] | None = None,
        ) -> asyncpg.Record | _OtherRecord | None:
            self._record_cursor_statement("fetchrow", query)
            return await super().fetchrow(query, *args, timeout=timeout, record_class=record_class)

    async with application_tables(wf_conn, wf_schema):
        source_locked = asyncio.Event()
        caller_deadlock_timeout_ms = 10
        peer_deadlock_timeout_ms = 1000
        peer_handshake_bound_s = (caller_deadlock_timeout_ms + peer_deadlock_timeout_ms) / 1000.0

        async def scenario(ctx: StepContext) -> object:
            async with AsyncExitStack() as connections:
                caller = await asyncpg.connect(
                    module_pg_schema.pg_dsn, connection_class=CountingConnection
                )
                connections.push_async_callback(caller.close)
                other = await asyncpg.connect(module_pg_schema.pg_dsn)
                connections.push_async_callback(other.close)
                peer_pid = await other.fetchval("SELECT pg_backend_pid()")
                assert type(peer_pid) is int
                other_task: asyncio.Task[None] | None = None
                try:
                    await wf_conn.execute(
                        f"INSERT INTO \"{wf_schema}\".atomicity_deadlock VALUES ($1,1,'original')",
                        ctx.flow_id,
                    )
                    peer_update = (
                        f'UPDATE "{wf_schema}".atomicity_deadlock '
                        "SET digest='other' WHERE ordinal=1"
                    )

                    async def contend() -> None:
                        tx = other.transaction()
                        await tx.start()
                        try:
                            await other.execute(
                                f"SET LOCAL deadlock_timeout='{peer_deadlock_timeout_ms}ms'"
                            )
                            # B owns the source row first, then blocks on A's exact
                            # caller-written deadlock row. A's engine cursor then closes the cycle.
                            await other.fetchval(
                                f'SELECT id FROM "{wf_schema}".jobs WHERE id=$1 FOR UPDATE',
                                ctx.job_id,
                            )
                            source_locked.set()
                            await other.execute(peer_update)
                        finally:
                            await tx.rollback()

                    with pytest.raises(asyncpg.DeadlockDetectedError):
                        async with caller.transaction():
                            await caller.execute(
                                f"SET LOCAL deadlock_timeout='{caller_deadlock_timeout_ms}ms'"
                            )
                            await insert_domain(caller, wf_schema, ctx.flow_id, "deadlock unit")
                            await insert_stage(caller, wf_schema, ctx.flow_id, 0)
                            # This exact application row belongs to the caller's unit before B
                            # can request it. Merely inserting domain/stage rows would not do so.
                            await caller.execute(
                                f'UPDATE "{wf_schema}".atomicity_deadlock '
                                "SET digest=$2 WHERE flow_id=$1 AND ordinal=1",
                                ctx.flow_id,
                                "caller-unit",
                            )
                            caller_pid = await caller.fetchval("SELECT pg_backend_pid()")
                            caller_xid = await caller.fetchval(
                                "SELECT txid_current()::text::bigint"
                            )
                            assert type(caller_pid) is int and type(caller_xid) is int
                            other_task = asyncio.create_task(contend())
                            await wait_event(
                                source_locked,
                                "the peer holding the actual source row",
                                bound_s=peer_handshake_bound_s,
                            )
                            await wait_until(
                                partial(
                                    _peer_waits_for_caller_application_row,
                                    wf_conn,
                                    wf_schema,
                                    ctx.flow_id,
                                    1,
                                    peer_pid,
                                    caller_pid,
                                    caller_xid,
                                    peer_update,
                                ),
                                label="the peer's exact caller-owned application-row lock",
                                bound_s=peer_handshake_bound_s,
                            )
                            # The real cursor UPDATE closes an observed lock cycle and is
                            # the first statement made a genuine PostgreSQL deadlock victim.
                            await emit_joined(
                                ctx,
                                page((160, 161)),
                                cursor={"page": 0},
                                connection=caller,
                                schema=wf_schema,
                            )
                    if other_task is not None:
                        await asyncio.wait_for(other_task, timeout=5)
                    assert not caller.is_in_transaction()
                    assert await children(wf_conn, wf_schema, ctx.job_id) == []
                finally:
                    if other_task is not None and not other_task.done():
                        other_task.cancel()
                        await asyncio.gather(other_task, return_exceptions=True)
            assert caller.is_closed() and other.is_closed()
            return {"pages": 1}

        flow_id, errors = await _run_source_scenario(
            wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema, scenario
        )
        _assert_body_completed(errors)
        source_id = await node_id(wf_conn, wf_schema, flow_id, "source")
        assert len(engine_cursor_calls) == 1, (
            "production cursor statement was not observed exactly once "
            "(missing or internally retried); "
            f"actual calls={len(engine_cursor_calls)}; "
            f"doors={[door for door, _query in engine_cursor_calls]}"
        )
        assert await children(wf_conn, wf_schema, source_id) == []
        assert await edge_count(wf_conn, wf_schema, source_id) == 0
        assert await cursor(wf_conn, wf_schema, source_id) is None
        assert await settlement_counts(wf_conn, wf_schema, flow_id) == (0, 0, 0)
        assert await stage_count(wf_conn, wf_schema, flow_id) == 0
        assert (
            await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1 '
                "AND step_key='source' AND status='failed'",
                flow_id,
            )
            == 0
        )


@pytest.mark.integration
async def test_public_transactional_declaration_requires_schema_level_three(
    wf_conn: asyncpg.Connection,
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    wf_sql: WorkflowSql,
    module_pg_schema: ModulePgSchema,
) -> None:
    """Public declaration stamps level 3; the level-2 fixture makes no such claim."""
    ordinary_calls: list[str] = []

    async def ordinary_body(ctx: StepContext) -> str:
        ordinary_calls.append("ordinary")
        return "ordinary"

    ordinary_name = new_label("ordinary_public")
    ordinary_app = WorkflowApp()

    @ordinary_app.workflow(ordinary_name)
    def ordinary_flow() -> Promise[object]:
        return build(step(ordinary_body, key="only"))

    ordinary_runner = FlowRunner(ordinary_app.get(ordinary_name), wf_pool, wf_schema)
    ordinary_id = (await ordinary_runner.create_flow()).flow_id
    assert await ordinary_runner.drive(ordinary_id) == "terminal"
    assert ordinary_calls == ["ordinary"]
    ordinary_requirements = await wf_conn.fetch(
        f'SELECT required_worker_schema FROM "{wf_schema}".jobs '
        "WHERE id=$1 OR (metadata->>'flow_id')::uuid=$1",
        ordinary_id,
    )
    assert [item[0] for item in ordinary_requirements] == [None, None]

    calls: list[str] = []
    paths: list[str] = []
    async with application_tables(wf_conn, wf_schema):

        async def source(ctx: StepContext) -> object:
            calls.append("source")
            invocation = calls.count("source")
            if invocation == 1:
                raise RuntimeError("one genuine body failure before any page write")
            caller = await wf_pool.acquire()
            try:
                record = 170 if invocation == 2 else 171
                async with caller.transaction():
                    if invocation == 2:
                        await insert_domain(caller, wf_schema, ctx.flow_id, "public declaration")
                        await insert_stage(caller, wf_schema, ctx.flow_id, 0)
                    emitted = await emit_joined(
                        ctx,
                        page((record,)),
                        cursor={"page": record - 170},
                        connection=caller,
                        schema=wf_schema,
                    )
                    assert len(emitted) == 1
                if invocation == 2:
                    # The second whole page propagates genuine capacity control out of
                    # its application transaction. It becomes the wake-lineage row below.
                    async with caller.transaction():
                        await insert_stage(caller, wf_schema, ctx.flow_id, 1)
                        await emit_joined(
                            ctx,
                            page((171,)),
                            cursor={"page": 1},
                            connection=caller,
                            schema=wf_schema,
                        )
                return {"pages": 2, "token": "public-atomic"}
            finally:
                if caller.is_in_transaction():
                    await caller.execute("ROLLBACK")
                await wf_pool.release(caller)

        async def settle(ctx: StepContext, source_result: object) -> None:
            calls.append("settle")
            assert source_result == {"pages": 2, "token": "public-atomic"}
            paths.append(execution_path(ctx))

        # The public typed door, imported directly: its presence is the
        # capability-3 declaration contract, not an optional attribute.
        declaration = transactional_step
        name = new_label("transactional_public")
        app = WorkflowApp()

        # The bound counts the run's own static settlement row as one unit
        # of materialization capacity (the accepted in-flight law: only a
        # truly parked row is weightless), so two units fit one page: the
        # first page emits, the second refuses while its predecessor is
        # outstanding, and completing that child wakes the parked source.
        @app.workflow(name, max_in_flight=2)
        def flow() -> Promise[object]:
            source_declaration = chain_source(
                _chain(new_label("public_records")), source, key="source", max_attempts=2
            )
            return build(declaration(settle, source_declaration, key="settle", max_attempts=1))

        # Ordinary public creation, not the internal guarded-create fixture.
        runner = FlowRunner(app.get(name), wf_pool, wf_schema)
        flow_id = (await runner.create_flow()).flow_id
        root_and_static = await wf_conn.fetch(
            f'SELECT id,step_key,required_worker_schema FROM "{wf_schema}".jobs '
            "WHERE id=$1 OR (metadata->>'flow_id')::uuid=$1 ORDER BY step_key",
            flow_id,
        )
        assert [item["step_key"] for item in root_and_static] == ["__flow__", "settle", "source"]
        assert [item["required_worker_schema"] for item in root_and_static] == [3, 3, 3]

        with pytest.raises(WorkflowWorkerCapabilityError) as direct:
            await runner.tick(flow_id)
        assert direct.value.required_worker_schema == TRANSACTIONAL_EXECUTION_SCHEMA
        assert calls == []
        assert (
            await wf_conn.fetchval(
                f'SELECT count(*) FROM "{wf_schema}".wf_step_ledger WHERE flow_id=$1', flow_id
            )
            == 0
        )

        below_worker = JobId(new_uuid())
        await wf_conn.execute(
            f"INSERT INTO \"{wf_schema}\".actor_config (actor,queue) VALUES ('wf','default') "
            "ON CONFLICT (actor) DO NOTHING"
        )
        await wf_conn.execute(
            f'INSERT INTO "{wf_schema}".workers (id,hostname,pid,queues,metadata) '
            "VALUES ($1,'level-2-test-worker',1,'{default}',"
            '\'{"workflow_execution":true,"workflow_execution_schema":2}\'::jsonb)',
            below_worker,
        )
        try:
            from taskq.backend._dispatch_sql import DISPATCH_STRICT_FIFO_SQL, dispatch_batch

            with pytest.raises(asyncpg.PostgresError) as refused:
                await dispatch_batch(
                    wf_conn,
                    sql=DISPATCH_STRICT_FIFO_SQL.format(schema=wf_schema),
                    queues=["default"],
                    limit_n=1,
                    worker_id=below_worker,
                    lock_lease=timedelta(seconds=30),
                )
            assert refused.value.sqlstate == "WF001"
            source_before_claim = await row(wf_conn, wf_schema, flow_id, "source")
            assert source_before_claim["status"] == "pending"
            assert source_before_claim["attempt"] == 0
            assert source_before_claim["locked_by_worker"] is None
            assert calls == []

            capable = await RealWorker.open(wf_pool, wf_conn, module_pg_schema.pg_dsn, wf_schema)
            try:
                assert await capable.run(runner.compiled, flow_id, "source") == "laddered"
                repended = await row(wf_conn, wf_schema, flow_id, "source")
                assert repended["id"] == source_before_claim["id"]
                assert repended["status"] == "scheduled"
                assert repended["required_worker_schema"] == TRANSACTIONAL_EXECUTION_SCHEMA
                assert repended["locked_by_worker"] is None
                assert repended["lock_expires_at"] is None
                await wait_until(
                    lambda: _source_retry_is_due(wf_conn, wf_schema, flow_id),
                    label="the public source's actual re-pend is due",
                )
                await sweep_scheduled_to_pending(wf_conn, schema=wf_schema)
                # The reminted physical claim preserves the same durable row's requirement.
                assert await capable.run(runner.compiled, flow_id, "source") == "deferred"
                parked = await row(wf_conn, wf_schema, flow_id, "source")
                assert parked["status"] == "scheduled" and parked["scheduled_at"] is None
                assert parked["attempt"] == 0 and parked["locked_by_worker"] is None
                assert parked["required_worker_schema"] == TRANSACTIONAL_EXECUTION_SCHEMA
                assert json_dict(parked["metadata"])["blocking_reason"] == "backpressure"

                # Completing the first genuinely emitted child releases the one available
                # slot. The production terminal wake promotes that exact source row, not a copy.
                first_child = await capable.claim("screen")
                assert first_child["required_worker_schema"] == TRANSACTIONAL_EXECUTION_SCHEMA
                assert (
                    await capable.execute_claim(runner.compiled, flow_id, first_child)
                    == "succeeded"
                )
                woken = await row(wf_conn, wf_schema, flow_id, "source")
                assert woken["id"] == parked["id"]
                assert woken["status"] == "pending" and woken["scheduled_at"] is not None
                assert woken["required_worker_schema"] == TRANSACTIONAL_EXECUTION_SCHEMA
                assert "blocking_reason" not in json_dict(woken["metadata"])

                assert await capable.run(runner.compiled, flow_id, "source") == "succeeded"
                assert await capable.run(runner.compiled, flow_id, "settle") == "succeeded"
                assert await capable.run(runner.compiled, flow_id, "screen") == "succeeded"
            finally:
                await capable.close()
            await wf_conn.execute(wf_sql.workflow_root_maintain, 200)
            all_rows = await wf_conn.fetch(
                f"SELECT step_key,status::text AS status,required_worker_schema "
                f"FROM \"{wf_schema}\".jobs WHERE id=$1 OR (metadata->>'flow_id')::uuid=$1 "
                "ORDER BY step_key",
                flow_id,
            )
            assert [item["step_key"] for item in all_rows] == [
                "__flow__",
                "screen",
                "screen",
                "settle",
                "source",
            ]
            assert [item["required_worker_schema"] for item in all_rows] == [3, 3, 3, 3, 3]
            assert all(item["status"] == "succeeded" for item in all_rows)
            # Three source invocations (the genuine failure, the parked
            # attempt, the resumed one) then the settlement: each body
            # execution is one invocation, the park included.
            assert calls == ["source", "source", "source", "settle"]
            assert paths == ["transactional_step=present; ctx.connection=present"]
            assert await settlement_counts(wf_conn, wf_schema, flow_id) == (1, 0, 0)
        finally:
            await wf_conn.execute(f'DELETE FROM "{wf_schema}".workers WHERE id=$1', below_worker)


async def _one_root_waiter(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> bool:
    return bool(await row_lock_waiters(conn, schema, flow_id))


async def _dispatch_attempt_observed(path: Path, worker_id: JobId, start_line: int) -> bool:
    """A new production dispatch with no row claimed in the open caller window."""
    if not await asyncio.to_thread(path.is_file):
        return False
    content = await asyncio.to_thread(path.read_text)
    for line in content.splitlines()[start_line:]:
        try:
            event = json_object(line)
        except json.JSONDecodeError:
            # A concurrent writer may expose its not-yet-complete final line.
            continue
        if (
            isinstance(event, dict)
            and event.get("event") == "dispatch"
            and event.get("worker_id") == str(worker_id)
        ):
            assert event.get("count") == 0, event
            print("worker B production zero-row dispatch:", event)
            return True
    return False


async def _source_retry_is_due(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> bool:
    state = await row(conn, schema, flow_id, "source")
    return state["status"] == "pending" or (
        state["status"] == "scheduled"
        and state["scheduled_at"] is not None
        and state["scheduled_at"] <= await conn.fetchval("SELECT now()")
    )


async def _process_entered(conn: asyncpg.Connection, schema: str, flow_id: JobId) -> bool:
    return bool(
        await conn.fetchval(
            f'SELECT entered FROM "{schema}".atomicity_process_gate WHERE flow_id=$1', flow_id
        )
    )
