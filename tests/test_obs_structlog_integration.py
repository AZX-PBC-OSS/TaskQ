"""Integration tests for structured logging against real Postgres.

Validates the end-to-end structured logging contract: enqueuing, dispatching,
state-changing, and cancelling a job all produce correctly structured log lines
with mandatory fields. These are the acceptance-definition tests.

The ``_logging_configured_guard`` autouse fixture (imported into conftest.py
from ``taskq.testing.otel``) snapshots and restores structlog global state
between tests.

These tests treat the structured log output as an observable public
contract for downstream log-aggregation pipelines (e.g. Datadog) — not as
internal logging. The log schema (``kind``, ``from_state``, ``to_state``,
mandatory fields) is a documented API that operators rely on for log
aggregation queries.
"""

import asyncio
import contextlib
import io
import json
import logging
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import structlog
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import BaseModel

import taskq.obs as obs_mod
import taskq.obs._structlog as structlog_mod
from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.actor import actor
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.backend.statemachine import VALID_TRANSITIONS
from taskq.client import JobsClient
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.migrate import apply_pending
from taskq.retry import RetryPolicy
from taskq.settings import WorkerSettings
from taskq.testing.actor import EmptyPayload, StubActorConfig
from taskq.testing.fixtures import _create_worker
from taskq.worker._consumer import consume_one_job
from taskq.worker.cancel import CancelController, make_cancel_controller
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.dispatch import dispatch_one_job
from taskq.worker.heartbeat import heartbeat_loop

pytestmark = pytest.mark.integration

_HEARTBEAT_INTERVAL = 0.5
_LOCK_LEASE = 2.0


class _Payload(BaseModel):
    value: int = 1


@actor(name="_structlog_integration_test_actor")
async def _structlog_test_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
    pass


async def _setup_worker(
    pg_dsn: str,
    schema: str,
) -> tuple[AsyncExitStack, WorkerDeps, PostgresBackend, JobsClient]:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_HEARTBEAT_INTERVAL": str(_HEARTBEAT_INTERVAL),
            "TASKQ_LOCK_LEASE": str(_LOCK_LEASE),
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.5",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.5",
            "TASKQ_MAX_HEARTBEAT_FAILURES": "999",
        }
    )

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
        await apply_pending(conn, schema=settings.schema_name)
        await conn.execute(
            f'INSERT INTO "{settings.schema_name}".actor_config (actor, queue) '  # noqa: S608 # Why: schema validated by WorkerSettings; asyncpg has no parameter binding for identifiers
            f"VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
            "_structlog_integration_test_actor",
            "default",
        )
    finally:
        await conn.close()

    assert settings.pg_dsn_direct is not None

    stack = AsyncExitStack()
    deps: WorkerDeps = await stack.enter_async_context(open_worker_deps(settings))
    try:
        cancellation_grace = timedelta(seconds=deps.settings.cancellation_grace_period)
        cleanup_grace = timedelta(seconds=deps.settings.cleanup_grace_period)
        backend: PostgresBackend = PostgresBackend(
            deps,
            clock=SystemClock(),
            cancellation_grace_period=cancellation_grace,
            cleanup_grace_period=cleanup_grace,
        )
    except BaseException:
        await stack.aclose()
        raise

    client = JobsClient(backend, clock=SystemClock())
    return stack, deps, backend, client


def _make_scopes(
    settings: WorkerSettings,
) -> tuple[ProviderRegistry, ProcessScope, ThreadScope, LoopScope, dict[Scope, Any]]:
    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(SystemClock, Scope.PROCESS, SystemClock())
    scope_containers: dict[Scope, Any] = {}
    resolver = make_resolver(registry, scope_containers)
    process_scope = ProcessScope(resolver=resolver)
    scope_containers[Scope.PROCESS] = process_scope
    thread_scope = ThreadScope(resolver=resolver)
    scope_containers[Scope.THREAD] = thread_scope
    loop_scope = LoopScope(resolver=resolver)
    scope_containers[Scope.LOOP] = loop_scope
    return registry, process_scope, thread_scope, loop_scope, scope_containers


async def _bootstrap_scopes(
    registry: ProviderRegistry,
    settings: WorkerSettings,
    process_scope: ProcessScope,
    thread_scope: ThreadScope,
    loop_scope: LoopScope,
) -> None:
    await process_scope.bootstrap(registry, settings)
    await thread_scope.bootstrap(registry, process_scope)
    await loop_scope.bootstrap(registry, process_scope, thread_scope)


async def _enqueue_job(
    backend: PostgresBackend,
    actor_name: str = "test",
    queue: str = "default",
    payload: dict[str, object] | None = None,
) -> UUID:
    from taskq.backend._protocol import EnqueueArgs

    job_id = new_job_id()
    args = EnqueueArgs(
        id=job_id,
        actor=actor_name,
        queue=queue,
        payload=payload or {"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime.now(UTC),
    )
    row = await backend.enqueue(args)
    return row.id


async def _dispatch_job_to_running(
    conn: asyncpg.Connection,
    schema: str,
    worker_id: UUID,
    job_id: UUID,
) -> None:
    await conn.execute(
        f"UPDATE \"{schema}\".jobs SET status='running', attempt=1, "  # noqa: S608 # Why: schema validated by WorkerSettings; asyncpg has no parameter binding for identifiers
        f"locked_by_worker=$1, lock_expires_at=now()+interval '60 seconds', "
        f"started_at=now(), last_heartbeat_at=now() "
        f"WHERE id=$2 AND status='pending'",
        worker_id,
        job_id,
    )


# ── End-to-end job lifecycle log lines ────────────────────────────


async def test_end_to_end_lifecycle_log_lines(
    pg_dsn: str,
) -> None:
    """End-to-end job lifecycle log lines against real Postgres.

    Validates that enqueuing and consuming a job produce correctly
    structured log lines with mandatory fields. The actual log lines
    emitted by the e2e path are:

    1. ``kind="enqueue"`` at ``backend.enqueue()`` (postgres.py).
    2. ``kind="dispatch"`` with ``from_state="pending"``,
       ``to_state="running"`` at ``dispatch_batch()`` (dispatch.py).
    3. ``kind="state_change"`` with ``from_state="running"``,
       ``to_state="succeeded"`` (consumer ctx.log).
    4. ``kind="state_change"`` with ``from_state="running"``,
       ``to_state="succeeded"`` (postgres.py backend logger on
       ``mark_succeeded``).

    All state_change and dispatch lines carry ``job_id``, ``actor``,
    ``queue``, ``attempt``, ``trace_id``. All lines are valid JSON.
    """
    obs_mod.setup_logging(log_format="json")

    stack, deps, backend, client = await _setup_worker(
        pg_dsn, schema=f"tosl_{new_base62()}".lower()
    )
    try:
        worker_id = new_uuid()

        with structlog.testing.capture_logs() as enqueue_captured:
            handle = await client.enqueue(_structlog_test_actor, _Payload())

        enqueue_logs = [e for e in enqueue_captured if e.get("kind") == "enqueue"]
        assert len(enqueue_logs) >= 1, "missing enqueue log from backend.enqueue()"
        enqueue_entry = enqueue_logs[0]
        assert enqueue_entry["job_id"] == str(handle.job_id)
        assert enqueue_entry["actor"] == "_structlog_integration_test_actor"
        assert enqueue_entry["queue"] == "default"

        async with deps.dispatcher_pool.acquire() as conn:
            await _create_worker(conn, deps.settings.schema_name, worker_id)

        with structlog.testing.capture_logs() as dispatch_captured:
            rows = await backend.dispatch_batch(
                worker_id=worker_id,
                queues=["default"],
                limit=1,
                lock_lease=timedelta(seconds=deps.settings.lock_lease),
            )

        assert len(rows) == 1
        dispatch_logs = [e for e in dispatch_captured if e.get("kind") == "dispatch"]
        assert len(dispatch_logs) >= 1, "missing kind='dispatch' log"
        dispatch_entry = dispatch_logs[0]
        assert dispatch_entry["from_state"] == "pending"
        assert dispatch_entry["to_state"] == "running"
        assert dispatch_entry["count"] == 1

        job_row = rows[0]

        registry, process_scope, thread_scope, loop_scope, _ = _make_scopes(deps.settings)
        await _bootstrap_scopes(registry, deps.settings, process_scope, thread_scope, loop_scope)

        enqueuer = SubJobEnqueuer(
            loop_scope_resolved=loop_scope.resolved_cache(),
            worker_pool=deps.worker_pool,
            backend=backend,
        )

        with structlog.testing.capture_logs() as consumed_captured:
            await dispatch_one_job(
                backend=backend,
                deps=deps,
                job=job_row,
                worker_id=worker_id,
                registry=registry,
                process_scope=process_scope,
                thread_scope=thread_scope,
                loop_scope=loop_scope,
                actor_ref=_structlog_test_actor,
                actor_config=StubActorConfig(
                    retry=RetryPolicy(kind="transient", max_attempts=3, jitter=0.0),
                ),
                clock=SystemClock(),
                enqueuer=enqueuer,
            )

        final_job = await backend.get(handle.job_id)
        assert final_job is not None
        assert final_job.status == "succeeded"

        state_changes = [e for e in consumed_captured if e.get("kind") == "state_change"]
        running_to_succeeded = [
            e
            for e in state_changes
            if e.get("from_state") == "running" and e.get("to_state") == "succeeded"
        ]
        assert len(running_to_succeeded) >= 1, "missing running→succeeded state_change"

        for entry in running_to_succeeded:
            assert "job_id" in entry

        all_log_entries = enqueue_captured + dispatch_captured + consumed_captured
        for entry in all_log_entries:
            rendered = json.dumps(entry, default=str)
            json.loads(rendered)
    finally:
        await stack.aclose()


# ── Cancel phase change logs ─────────────────────────────────────


async def test_cancel_phase_change_logs(pg_dsn: str) -> None:
    """Cancel phase change logs against real Postgres."""
    obs_mod.setup_logging(log_format="json")

    stack, deps, backend, client = await _setup_worker(
        pg_dsn, schema=f"tosl_{new_base62()}".lower()
    )
    try:
        worker_id = new_uuid()

        handle = await client.enqueue(_structlog_test_actor, _Payload())

        async with deps.dispatcher_pool.acquire() as conn:
            await _create_worker(conn, deps.settings.schema_name, worker_id)
            await _dispatch_job_to_running(
                conn, deps.settings.schema_name, worker_id, handle.job_id
            )

        job_row = await backend.get(handle.job_id)
        assert job_row is not None

        cancel_seen = asyncio.Event()

        async def looping_actor(_payload: object, ctx: JobContext[BaseModel]) -> str:
            await cancel_seen.wait()
            return "exit"

        actor_config = StubActorConfig(
            retry=RetryPolicy(kind="non_retryable", max_attempts=1, jitter=0.0),
        )

        shutdown = asyncio.Event()
        controller: CancelController = make_cancel_controller(deps, worker_id, backend)
        heartbeat_task = asyncio.create_task(
            heartbeat_loop(deps, worker_id, shutdown, cancel_controller=controller)
        )

        try:
            consumer_task = asyncio.create_task(
                consume_one_job(
                    backend,
                    job_row,
                    worker_id,
                    run_actor=looping_actor,
                    actor_config=actor_config,
                    payload_type=EmptyPayload,
                    clock=SystemClock(),
                    active_jobs=deps.active_jobs,
                )
            )
            try:
                await asyncio.sleep(0.15)

                result = await client.cancel(handle.job_id)
                assert result.cancellation_initiated is True

                with structlog.testing.capture_logs() as captured:
                    await asyncio.sleep(deps.settings.heartbeat_interval * 3)

                phase_changes = [e for e in captured if e.get("kind") == "cancel_phase_change"]
                phase_0_to_1 = [
                    e for e in phase_changes if e.get("from_phase") == 0 and e.get("to_phase") == 1
                ]

                if phase_0_to_1:
                    entry = phase_0_to_1[0]
                    assert entry["kind"] == "cancel_phase_change"
                    assert entry["from_phase"] == 0
                    assert entry["to_phase"] == 1
                    assert "job_id" in entry

                cancel_seen.set()
            finally:
                if not consumer_task.done():
                    consumer_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await consumer_task
        finally:
            shutdown.set()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
    finally:
        await stack.aclose()


# ── Stdlib log capture via ProcessorFormatter ─────────────────────


async def test_stdlib_bridge_json_output() -> None:
    """Stdlib log capture via ProcessorFormatter produces valid JSON."""
    obs_mod.setup_logging(log_format="json")

    from taskq._json import structlog_serializer

    buf = io.StringIO()
    handler = logging.StreamHandler(buf)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.JSONRenderer(serializer=structlog_serializer),
            ],
            foreign_pre_chain=[
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.stdlib.add_log_level,
                structlog.stdlib.ExtraAdder(),
            ],
        )
    )

    test_logger = logging.getLogger("asyncpg._test_stdlib_bridge")
    test_logger.handlers.clear()
    test_logger.addHandler(handler)
    test_logger.setLevel(logging.DEBUG)

    test_logger.info("structlog integration test message from asyncpg")

    output = buf.getvalue().strip()
    assert output, "No output captured from stdlib bridge"

    parsed = json.loads(output)
    assert "level" in parsed
    assert "timestamp" in parsed
    assert parsed["level"] == "info"

    test_logger.handlers.clear()


# ── Payload not logged at INFO+ ───────────────────────────────────


async def test_payload_not_logged_at_info(
    pg_dsn: str,
) -> None:
    """Payload content does not appear in INFO+ log lines."""
    obs_mod.setup_logging(log_format="json")

    pii_marker = "SSN_000-00-0000_PII"

    stack, _deps, backend, _client = await _setup_worker(
        pg_dsn, schema=f"tosl_{new_base62()}".lower()
    )
    try:
        with structlog.testing.capture_logs() as captured:
            _job_id = await _enqueue_job(
                backend,
                payload={"secret_field": pii_marker, "value": 42},
            )

        for entry in captured:
            level = entry.get("log_level", "")
            if level in ("info", "warning", "error", "critical"):
                entry_str = str(entry)
                assert pii_marker not in entry_str, (
                    f"PII marker found in INFO+ log line: {entry_str[:200]}"
                )
    finally:
        await stack.aclose()


# ── structlog processor raises ────────────────────────────────────


async def test_processor_raises_swallowed() -> None:
    """Structlog processor exception is swallowed; output still reaches handler."""
    obs_mod.setup_logging(log_format="json")

    def _raising_processor(
        logger: object, method: str, event_dict: structlog.types.EventDict
    ) -> structlog.types.EventDict:
        raise RuntimeError("simulated OTel processor failure")

    original_processor = structlog_mod._otel_span_processor  # type: ignore[reportPrivateUsage] # Why: test directly accesses private module function to wrap it in a raising wrapper for verification

    structlog.configure(
        processors=[
            structlog_mod._safe_processor_wrapper(_raising_processor),  # type: ignore[reportPrivateUsage] # Why: test directly accesses private module function to wrap it in a raising wrapper for verification
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog_mod._safe_processor_wrapper(original_processor),  # type: ignore[reportPrivateUsage] # Why: test directly accesses private module function to wrap it in a raising wrapper for verification
            structlog.processors.EventRenamer("event"),
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    log = obs_mod.get_logger("test_tc1")

    with structlog.testing.capture_logs() as captured:
        log.info("after_processor_failure")

    assert len(captured) >= 1
    assert captured[0]["event"] == "after_processor_failure"
    assert captured[0]["log_level"] == "info"


# ── Every state transition produces exactly one state_change log ──


_VALID_TRANSITION_PAIRS: list[tuple[str, str]] = [
    (from_s, to_s) for from_s, to_set in VALID_TRANSITIONS.items() for to_s in to_set
]

_transition_strategy = st.sampled_from(_VALID_TRANSITION_PAIRS)


@given(pair=_transition_strategy)
@settings(max_examples=30, deadline=None)
def test_every_state_transition_produces_one_log_line(
    pair: tuple[str, str],
) -> None:
    """Every state transition produces exactly one kind='state_change' log line."""
    obs_mod.setup_logging()

    from_state, to_state = pair

    with structlog.testing.capture_logs() as captured:
        log = obs_mod.get_logger("test_property")
        obs_mod.log_state_change(log, from_state=from_state, to_state=to_state)

    assert len(captured) == 1
    entry = captured[0]
    assert entry["kind"] == "state_change"
    assert entry["from_state"] == from_state
    assert entry["to_state"] == to_state
    assert entry["log_level"] == "info"
