# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""N-4 (the ruling's layers 3-5): a missing flow root at the failed-parent
seat is a loud typed corruption — `WorkflowRootMissingError` — raised at
ENTRY before any tx2 write; the fleet executor passes it through
UNWRAPPED; the worker's own release arm attempts one budget-free
10-second release whose read-back against the committed tx1 terminal is
`noop` — the REAL fence's own outcome — with the whole child row
byte-identical and zero new attempt/event/ledger rows."""

from __future__ import annotations

import asyncio
import contextlib
import enum
import json as _json
import types
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.actor import actor
from taskq.backend._protocol import JobId
from taskq.context import JobContext
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration


def new_uuid_str() -> str:
    from taskq._ids import new_uuid

    return str(new_uuid())


class _WfPayload(BaseModel):
    pass


@actor(name="wf")
async def _wf_placeholder(payload: _WfPayload, ctx: JobContext[_WfPayload]) -> None:
    del payload, ctx


async def _seed_flow_with_child(
    conn: Any, schema: str, *, with_join: bool = False
) -> tuple[str, str]:
    """A running root, a RUNNING claimed child (its exact claim identity)
    — and optionally the child's pending fail_closed join (the cascade's
    own would-be target)."""
    flow_id = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, fairness_key, idempotency_scope, "
        "idempotency_key) VALUES ($1, 'n4', 'default', '{}', 3, 'transient', "
        "'running', '__flow__', $2::jsonb, $3, 'workflow-run', $4)",
        flow_id,
        _json.dumps({"flow_id": flow_id, "workflow": "n4_flow"}),
        flow_id,
        f"flow:{flow_id}",
    )
    child = str(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
        "max_attempts, retry_kind, step_key, metadata, claim_epoch, "
        "locked_by_worker, lock_expires_at, last_heartbeat_at, fairness_key, "
        "idempotency_scope, idempotency_key) "
        "VALUES ($1, 'n4', 'default', '{}', 'running', 1, 1, 'transient', 'screen', "
        "$2::jsonb, 1, $1, now() + interval '90 seconds', now(), $3, 'workflow-run', $4)",
        child,
        _json.dumps({"flow_id": flow_id}),
        flow_id,
        f"wf:{flow_id}:screen",
    )
    if with_join:
        join = str(new_uuid())
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, '
            "max_attempts, retry_kind, step_key, deps_pending, metadata, "
            "scheduled_at, created_at, fairness_key, idempotency_scope, idempotency_key) "
            "VALUES ($1, 'n4', 'default', '{}', 'pending', 2, 'transient', 'join_step', "
            "1, $2::jsonb, clock_timestamp() + interval '1 hour', clock_timestamp(), "
            "$3, 'workflow-run', $4)",
            join,
            _json.dumps({"flow_id": flow_id, "blocking_reason": "join"}),
            flow_id,
            f"wf:{flow_id}:join",
        )
        await conn.execute(
            f'INSERT INTO "{schema}".wf_edge (child_id, parent_id, flow_id, '
            "failure_policy) VALUES ($1, $2, $3, 'fail_closed')",
            join,
            child,
            flow_id,
        )
    return flow_id, child


async def _child_row(conn: Any, schema: str, child: str) -> dict[str, Any]:
    raw = await conn.fetchrow(f'SELECT * FROM "{schema}".jobs WHERE id = $1', child)
    assert raw is not None
    return dict(raw)


async def _counts(conn: Any, schema: str, child: str) -> tuple[int, int, int]:
    attempts = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_attempts WHERE job_id = $1', child
    )
    events = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".job_events WHERE job_id = $1', child
    )
    ledger = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".wf_step_ledger WHERE job_id = $1', child
    )
    return int(attempts), int(events), int(ledger)


def _real_row(
    child: str,
    flow_id: str,
    dsn: str = "",
    schema: str = "",
    metadata: Any = None,
    actor_name: str = "n4",
) -> Any:
    """A JobRow carrying the REAL claimed child's own fence identity."""
    from datetime import datetime

    from taskq.backend._protocol import JobRow

    return JobRow(
        id=JobId(child),
        actor=actor_name,
        queue="default",
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=0,
        status="running",
        priority=0,
        attempt=1,
        max_attempts=1,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=datetime.now(UTC),
        scheduled_at=datetime.now(UTC),
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=JobId(child),
        lock_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        cancel_requested_at=None,
        cancel_phase=0,
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        idempotency_scope="",
        trace_id=None,
        span_id=None,
        metadata=metadata if metadata is not None else {"flow_id": flow_id},
        tags=(),
    )


async def test_missing_root_direct_rolls_back_before_any_write(
    wf_conn: Any, wf_schema: str, wf_pool: Any, wf_sql: WorkflowSql
) -> None:
    """The direct seat: with the flow's root row GONE, the real
    `finalize_node` commits the child's tx1 terminal and tx2 raises the
    exact `WorkflowRootMissingError` — zero cascade/fan-in/decrement/fire/
    outbox writes, the join row unstamped, the committed terminal
    preserved."""
    from taskq.workflows._worker_execution import WorkflowBodyUnresolvableError
    from taskq.workflows.api._runner_errors import (
        WorkflowRootMissingError,
        WorkflowRunError,
    )
    from taskq.workflows.engine import finalize_node

    flow_id, child = await _seed_flow_with_child(wf_conn, wf_schema, with_join=True)
    join_id = await wf_conn.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata->>'blocking_reason' = 'join' LIMIT 1""",
        flow_id,
    )
    join_meta_before = await wf_conn.fetchval(
        f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', join_id
    )
    outbox_before = await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_outbox')

    await wf_conn.execute(f'DELETE FROM "{wf_schema}".jobs WHERE id = $1', flow_id)

    with pytest.raises(WorkflowRootMissingError) as exc_info:
        await finalize_node(
            wf_pool,
            wf_sql,
            flow_id=JobId(flow_id),
            job_id=JobId(child),
            step_key="screen",
            worker_id=JobId(child),
            attempt=1,
            claim_epoch=1,
            outcome="failed",
            error_class="ValueError",
            error_message="exhausted",
            ledger_id=None,
            ledger_claimed_at=None,
        )
    raised = exc_info.value
    assert not isinstance(raised, WorkflowBodyUnresolvableError)
    assert isinstance(raised, WorkflowRunError)
    assert str(flow_id) in str(raised) and child in str(raised), str(raised)

    # ZERO tx2 writes: the join row unstamped (byte-identical metadata),
    # no outbox rows, no decrement (the join's deps_pending unchanged),
    # and the committed tx1 terminal's own state stands.
    join_meta_after = await wf_conn.fetchval(
        f'SELECT metadata FROM "{wf_schema}".jobs WHERE id = $1', join_id
    )
    assert join_meta_after == join_meta_before, "the cascade never stamped the join"
    assert await wf_conn.fetchval(f'SELECT count(*) FROM "{wf_schema}".wf_outbox') == outbox_before
    deps_after = await wf_conn.fetchval(
        f'SELECT deps_pending FROM "{wf_schema}".jobs WHERE id = $1', join_id
    )
    assert int(deps_after) == 1, "no decrement ran"
    child_after = await _child_row(wf_conn, wf_schema, child)
    assert child_after["status"] == "failed", child_after
    assert child_after["error_class"] == "ValueError", child_after


async def test_missing_root_fleet_executor_passes_the_exact_class_through(
    wf_conn: Any, wf_schema: str, module_pg_pool: Any, module_pg_schema: Any
) -> None:
    """The REAL fleet-claimed door (`run_fleet_claimed_step` — the
    execution leg `_worker_execution`'s except chain wraps): the body
    deletes the run's own root row mid-flight (the real corruption
    sequence) and its genuine failure exhausts; the raise arrives at the
    door as the UNWRAPPED `WorkflowRootMissingError` — never the
    body-unresolvable translation — and the committed tx1 terminal
    stands."""
    from taskq.workflows import DONE, Chain, Promise, Route, Step, WorkflowApp, build
    from taskq.workflows._worker_execution import WorkflowBodyUnresolvableError
    from taskq.workflows.api._graph import chain_source
    from taskq.workflows.api._runner_errors import WorkflowRootMissingError

    dsn = module_pg_schema.pg_dsn

    class Ok(enum.Enum):
        OK = "ok"

    async def screen_delete_root_then_fail(ctx: Any, item: object) -> Ok:
        import asyncpg

        del item
        conn = await asyncpg.connect(dsn)
        try:
            await conn.execute(f'DELETE FROM "{wf_schema}".jobs WHERE id = $1', flow_id_ref[0])
        finally:
            await conn.close()
        raise RuntimeError("the genuine failure after the root's deletion")

    from datetime import timedelta as _td

    from taskq.retry import RetryPolicy
    from taskq.workflows._policy import ExecutionPolicy, RetrySpec

    chain = Chain(
        name=f"n4-{new_uuid().hex}",
        start="screen",
        steps={
            "screen": Step(
                body=screen_delete_root_then_fail,
                outcomes=Ok,
                route=Route({Ok.OK: DONE}),
                policy=ExecutionPolicy(
                    retry=RetrySpec(
                        policy=RetryPolicy(
                            max_attempts=1,
                            base=_td(seconds=0.01),
                            cap=_td(seconds=0.02),
                            jitter=0,
                        )
                    )
                ),
            )
        },
    )

    async def source_body(ctx: Any) -> None:
        from taskq.workflows.chain import chain_start

        await ctx.emit_batch(
            [chain_start(chain, {"n": 0}, map_index=0, trace_id="n4-0")],
            cursor={"page": 0},
        )

    flow_id_ref: list[str] = []

    name = f"n4_fleet_{new_uuid().hex}"
    app = WorkflowApp()

    @app.workflow(name, max_in_flight=4)
    def n4_flow() -> Promise[object]:
        return build(chain_source(chain, source_body, key="source", max_attempts=1))

    from taskq.workflows import FlowRunner

    fleet_worker = JobId(new_uuid_str())
    runner = FlowRunner(app.get(name), module_pg_pool, wf_schema, worker_id=fleet_worker)
    flow_row = await runner.create_flow()
    real_flow_id = str(flow_row.flow_id)
    flow_id_ref.append(real_flow_id)
    # THE WOULD-BE CASCADE TARGET: the child's own pending fail_closed
    # join (the row the raise must leave UNSTAMPED).
    join = str(new_uuid())
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".jobs (id, actor, queue, payload, status, '
        "max_attempts, retry_kind, step_key, deps_pending, metadata, "
        "scheduled_at, created_at, fairness_key, idempotency_scope, idempotency_key) "
        "VALUES ($1, 'n4', 'default', '{}', 'pending', 2, 'transient', 'join_step', "
        "1, $2::jsonb, clock_timestamp() + interval '1 hour', clock_timestamp(), "
        "$3::uuid, 'workflow-run', $4)",
        join,
        _json.dumps({"flow_id": real_flow_id, "blocking_reason": "join"}),
        real_flow_id,
        f"wf:{real_flow_id}:join",
    )
    # The join's edge cannot exist before the child does; the cascade's
    # blocked arm needs the edge — the control seeds it after the emit.
    # Tick 1: the source runs (the child does not exist yet — the tick's
    # claimable set was read before the emit) and emits the screen child.
    await runner.tick(JobId(real_flow_id))
    child_id = await module_pg_pool.fetchval(
        f"""SELECT id FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid AND step_key = 'screen'
            LIMIT 1""",
        real_flow_id,
    )
    assert child_id is not None
    await wf_conn.execute(
        f'INSERT INTO "{wf_schema}".wf_edge (child_id, parent_id, flow_id, '
        "failure_policy) VALUES ($1, $2, $3::uuid, 'fail_closed')",
        join,
        child_id,
        real_flow_id,
    )
    # THE FLEET CLAIM SHAPE: the row is running under the fleet worker's
    # lock, the attempt incremented, the epoch bumped; the ledger claim
    # exists.
    from taskq.workflows.ledger import claim_step_ledger

    async with module_pg_pool.acquire() as conn:
        await claim_step_ledger(
            conn,
            runner.wsql,
            flow_id=JobId(real_flow_id),
            job_id=JobId(str(child_id)),
            step_key="screen",
            map_index=None,
            attempt=1,
            claim_epoch=1,
        )
    await module_pg_pool.execute(
        f"""UPDATE "{wf_schema}".jobs SET status = 'running', attempt = 1,
            claim_epoch = 1, locked_by_worker = $2, lock_expires_at = now()
            + interval '90 seconds', last_heartbeat_at = now()
            WHERE id = $1""",
        child_id,
        fleet_worker,
    )
    claimed = await module_pg_pool.fetchrow(
        f"""SELECT id, step_key, map_index, payload, trace_id, start_to_close,
                   max_attempts, retry_kind
            FROM "{wf_schema}".jobs WHERE id = $1""",
        child_id,
    )
    assert claimed is not None
    row = dict(claimed)
    # THE ROOT VANISHES mid-run (the real corruption sequence): the body
    # (the registered definition's own body) deletes the root row, then
    # fails; the runner's tx1 commits the terminal and tx2 raises at the
    # failed-parent seat's entry.
    with pytest.raises(WorkflowRootMissingError) as exc_info:
        await runner.run_fleet_claimed_step(
            JobId(real_flow_id),
            row,
            attempt=1,
            claim_epoch=1,
        )
    assert not isinstance(exc_info.value, WorkflowBodyUnresolvableError), type(
        exc_info.value
    ).__name__
    # THE COMMITTED TX1 TERMINAL stands; the join row was never stamped.
    child_terminal = await module_pg_pool.fetchval(
        f"""SELECT status::text FROM "{wf_schema}".jobs WHERE id = $1""", child_id
    )
    assert child_terminal == "failed", child_terminal
    join_meta = await module_pg_pool.fetchval(
        f"""SELECT metadata->>'blocking_reason' FROM "{wf_schema}".jobs
            WHERE (metadata->>'flow_id')::uuid = $1::uuid
              AND metadata->>'blocking_reason' = 'join' LIMIT 1""",
        real_flow_id,
    )
    assert join_meta == "join", "the cascade never stamped the join (the raise preceded it)"


class _BackendSettings:
    """The declared ``BackendSettings`` protocol's double (the
    test_admin_audit_trail mirror)."""

    schema_name: str
    dispatch_oversample: int = 2
    dispatcher_command_timeout: float = 5.0
    result_max_bytes: int = 1_000_000
    event_writer_batch_size: int = 500
    event_writer_statement_timeout_ms: float = 5_000.0
    event_writer_reduced_batch_divisor: int = 4
    sweep_breaker_failure_threshold: int = 3
    sweep_breaker_window_secs: float = 600.0
    max_pending_lock_timeout_ms: float = 5000.0
    unique_for_lock_timeout_ms: float = 5000.0

    def __init__(self, schema: str) -> None:
        self.schema_name = schema


class _BackendDeps:
    """The declared ``BackendDeps`` protocol's double (the same mirror)."""

    settings: _BackendSettings
    worker_pool: Any
    heartbeat_pool: Any
    dispatcher_pool: Any = None

    def __init__(self, schema: str, pool: Any) -> None:
        self.settings = _BackendSettings(schema)
        self.worker_pool = pool
        self.heartbeat_pool = pool


async def test_worker_release_arm_attempts_the_bounded_noop_release(
    wf_conn: Any, wf_schema: str, module_pg_pool: Any, module_pg_schema: Any
) -> None:
    """The worker's own arm, over the REAL backend: the release attempt
    names its reason with the bounded 10-second delay and the claim's
    attempt/epoch; against the committed tx1 terminal the REAL fence
    reads back `noop`; the WHOLE child row stays byte-identical with zero
    new attempt/event/ledger rows; the claim resolves; the loop moves
    on. The body-unresolvable and dispatch-flow-failed vocabulary never
    appears (structlog's capture is the record)."""
    from unittest.mock import patch

    import structlog.testing

    from taskq._di.registry import ProviderRegistry
    from taskq._di.scope import Scope
    from taskq.backend.clock import Clock
    from taskq.backend.postgres import PostgresBackend
    from taskq.client._enqueuer import SubJobEnqueuer
    from taskq.settings import WorkerSettings
    from taskq.testing.clock import FakeClock
    from taskq.worker.cancel import ActiveJobRegistry
    from taskq.worker.run import di_consumer_loop
    from taskq.worker.shutdown import ShutdownPhase
    from taskq.workflows import _worker_execution as real_seam
    from taskq.workflows.api._runner_errors import WorkflowRootMissingError
    from tests._di_scopes import bootstrap_scopes, make_scopes
    from tests.conftest import unique_health_sock_path

    flow_id, child = await _seed_flow_with_child(wf_conn, wf_schema)
    await wf_conn.execute(
        f"""UPDATE "{wf_schema}".jobs SET status = 'failed',
            error_class = 'ValueError', finished_at = clock_timestamp()
            WHERE id = $1""",
        child,
    )
    before = await _child_row(wf_conn, wf_schema, child)
    before_counts = await _counts(wf_conn, wf_schema, child)

    real_backend = PostgresBackend(
        _BackendDeps(wf_schema, module_pg_pool),  # pyright: ignore[reportArgumentType]  # Why: the protocol double declares every field the backend's paths read (the test_admin_audit_trail mirror).
        clock=FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC)),
        cancellation_grace_period=timedelta(seconds=5),
        cleanup_grace_period=timedelta(seconds=5),
    )

    async def _raising_door(**kwargs: object) -> Any:
        raise WorkflowRootMissingError(flow_id, child)

    def _seam() -> types.ModuleType:
        stub = types.ModuleType("wf-seam-root-missing")
        stub.execute_flow_job = _raising_door  # type: ignore[attr-defined]
        stub.WorkflowBodyUnresolvableError = real_seam.WorkflowBodyUnresolvableError  # type: ignore[attr-defined]
        stub.WorkflowRootMissingError = real_seam.WorkflowRootMissingError  # type: ignore[attr-defined]
        return stub

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": module_pg_schema.pg_dsn,
            "TASKQ_SCHEMA_NAME": wf_schema,
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("root_missing_n4"),
        },
        validate=False,
    )
    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC)))
    registry.validate()
    process_scope, thread_scope, loop_scope = make_scopes(registry)
    await bootstrap_scopes(registry, process_scope, thread_scope, loop_scope, settings)

    from types import SimpleNamespace

    deps = SimpleNamespace(
        settings=settings,
        dispatcher_pool=module_pg_pool,
        heartbeat_pool=module_pg_pool,
        worker_pool=module_pg_pool,
        active_jobs=ActiveJobRegistry(),
        producer_stop_event=asyncio.Event(),
        shutdown_phase=ShutdownPhase.NONE,
        disowned_jobs=set(),
        drain_failures=0,
    )

    job = _real_row(child, flow_id)
    witness = _real_row(new_uuid_str(), "", metadata={}, actor_name="wf")
    local_queue: asyncio.Queue[Any] = asyncio.Queue()
    await local_queue.put(job)
    await local_queue.put(witness)
    stop_event = asyncio.Event()
    dispatched: list[Any] = []

    async def _witness_dispatch(**kwargs: object) -> str:
        dispatched.append(kwargs.get("job"))
        stop_event.set()
        return "succeeded"

    loop_task: asyncio.Task[Any] | None = None
    try:
        with (
            structlog.testing.capture_logs() as captured,
            patch(
                "taskq.worker.run._workflow_execution_seam",
                return_value=_seam(),
            ),
            patch("taskq.worker.run.dispatch_one_job", side_effect=_witness_dispatch),
        ):
            loop_task = asyncio.create_task(
                di_consumer_loop(
                    deps,  # type: ignore[arg-type]  # Why: the duck-typed deps carry exactly the fields the loop reads (the existing loop controls' shape).
                    local_queue,
                    stop_event,
                    backend=real_backend,
                    worker_id=JobId(new_uuid_str()),
                    registry=registry,
                    process_scope=process_scope,
                    thread_scope=thread_scope,
                    loop_scope=loop_scope,
                    actor_registry={"wf": _wf_placeholder},  # the module-level placeholder
                    enqueuer=SubJobEnqueuer(
                        loop_scope_resolved=None, worker_pool=None, backend=real_backend
                    ),
                )
            )
            try:
                await asyncio.wait_for(loop_task, timeout=15)
            except TimeoutError:
                for e in captured:
                    print("CAPTURED:", e)
                print("DISPATCHED:", len(dispatched))
                loop_task.cancel()
                pytest.fail("the loop never exited — the witness's dispatch never set the stop")
    finally:
        if loop_task is not None and not loop_task.done():
            loop_task.cancel()
            with contextlib.suppress(BaseException):
                await loop_task
        await loop_scope.shutdown()
        await thread_scope.shutdown()
        await process_scope.shutdown()

    # THE RELEASE ATTEMPT: the REAL fence's own tri-state noop against the
    # committed tx1 terminal; the whole child row byte-identical; zero new
    # attempt/event/ledger rows; no release metadata persisted.
    after = await _child_row(wf_conn, wf_schema, child)
    after_counts = await _counts(wf_conn, wf_schema, child)
    assert after == before, {
        key: (before.get(key), after.get(key))
        for key in set(before) | set(after)
        if before.get(key) != after.get(key)
    }
    assert after_counts == before_counts, "zero new attempt/event/ledger rows"
    assert after["status"] == "failed", after
    # THE LOOP MOVED ON to the witness job.
    assert len(dispatched) == 1, dispatched
    # THE VOCABULARY: the distinct root-missing log; never the
    # body-unresolvable or dispatch-flow-failed owners.
    events = [e["event"] for e in captured if "event" in e]
    assert "dispatch-workflow-root-missing" in events, events
    assert "dispatch-workflow-body-unresolvable" not in events, events
    assert "dispatch-flow-failed" not in events, events
    assert "dispatch-workflow-root-missing-release-noop" in events, events
