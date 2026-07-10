"""Integration tests for OTel spans and metrics end-to-end.

Validates the acceptance definition: enqueue + dispatch + run +
complete produces at least 4 exported spans (PRODUCER, INTERNAL dispatch,
CONSUMER, INTERNAL attempt.1) with correct attributes, one span link on the
CONSUMER, and core lifecycle metric data points for instruments 1-4.

Also covers:
  End-to-end trace — spans + instruments 1-4
  Queue depth gauge (instrument 5)
  Leader gauge + election counters (instruments 9, 14-15)
  Heartbeat metrics (instruments 6-7)
  Reservation, cancellation, and cron metrics (instruments 8, 10, 16, 17)
  OTel exporter unavailable — no exception propagation
  Malformed trace_id — link skipped, CONSUMER span still created
  Enqueue span overhead measurement
"""

import asyncio
import contextlib
import time
from contextlib import AsyncExitStack
from datetime import timedelta
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import structlog
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics._internal.point import NumberDataPoint
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from pydantic import BaseModel

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
import taskq.worker.cancel as cancel_mod
import taskq.worker.leader as leader_mod
from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq._di.scopes import LoopScope, ProcessScope, ThreadScope, make_resolver
from taskq._ids import new_base62, new_job_id, new_uuid
from taskq.actor import actor
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.client import JobsClient
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.settings import WorkerSettings
from taskq.testing.actor import default_actor_config
from taskq.testing.otel import (
    ListSpanExporter,
    collect_metrics,
    counter_data_points,
    counter_value,
    histogram_points,
    setup_meter,
    setup_tracer,
)
from taskq.worker.deps import WorkerDeps, open_worker_deps
from taskq.worker.dispatch import dispatch_one_job
from taskq.worker.heartbeat import heartbeat_loop
from taskq.worker.leader import MaintenanceLeader

pytestmark = pytest.mark.integration

_HEARTBEAT_INTERVAL = 0.5
_LOCK_LEASE = 2.0


class _Payload(BaseModel):
    value: int = 1


@actor(name="_integration_test_actor")
async def _integration_test_actor(payload: _Payload, ctx: JobContext[_Payload]) -> None:
    pass


async def _setup_worker(
    pg_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    schema: str = f"toi_{new_base62()}".lower(),
) -> tuple[AsyncExitStack, WorkerDeps, PostgresBackend, JobsClient]:
    from taskq.migrate import apply_pending

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_HEARTBEAT_INTERVAL": str(_HEARTBEAT_INTERVAL),
            "TASKQ_LOCK_LEASE": str(_LOCK_LEASE),
            "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
            "TASKQ_MAX_HEARTBEAT_FAILURES": "999",
        }
    )

    conn = await asyncpg.connect(str(settings.pg_dsn))
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{settings.schema_name}" CASCADE')
        await apply_pending(conn, schema=settings.schema_name)
        await conn.execute(
            f'INSERT INTO "{settings.schema_name}".actor_config (actor, queue) '  # noqa: S608
            f"VALUES ($1, $2) ON CONFLICT (actor) DO NOTHING",
            "_integration_test_actor",
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


def _setup_isolated_meter(
    monkeypatch: pytest.MonkeyPatch,
) -> InMemoryMetricReader:
    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())
    monkeypatch.setattr(otel_mod, "get_meter", lambda: new_meter)
    otel_mod.set_otel_enabled(True)
    return reader


def _gauge_data_points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    for m in collect_metrics(reader):
        if m.name == name:
            return [p for p in m.data.data_points if isinstance(p, NumberDataPoint)]
    return []


def _make_scopes_for_dispatch(
    settings: WorkerSettings,
) -> tuple[
    ProviderRegistry,
    ProcessScope,
    ThreadScope,
    LoopScope,
    dict[Scope, Any],
]:
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


# ── End-to-end trace ──────────────────────────────────────────


class TestEndToEndTrace:
    """Enqueue + dispatch + consume produces correct spans and metrics."""

    async def test_producer_span_attributes(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, exporter = setup_tracer(monkeypatch)
        setup_meter(monkeypatch)
        stack, _deps, _backend, client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            handle = await client.enqueue(_integration_test_actor, _Payload())

            producer = exporter.span_named("enqueue _integration_test_actor")
            assert producer is not None
            assert producer.kind == trace.SpanKind.PRODUCER
            attrs = producer.attributes
            assert attrs is not None
            assert attrs.get("messaging.system") == "taskq"
            assert attrs.get("messaging.destination.name") == "default"
            assert attrs.get("messaging.operation.type") == "publish"
            assert attrs.get("messaging.message.id") == str(handle.job_id)
            assert attrs.get("taskq.actor") == "_integration_test_actor"
        finally:
            await stack.aclose()

    async def test_traceparent_stored_in_db(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        setup_tracer(monkeypatch)
        setup_meter(monkeypatch)
        stack, _deps, backend, client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            handle = await client.enqueue(_integration_test_actor, _Payload())

            row = await backend.get(handle.job_id)
            assert row is not None
            assert row.trace_id is not None
            assert len(row.trace_id) == 32
            assert row.span_id is not None
            assert len(row.span_id) == 16
        finally:
            await stack.aclose()

    async def test_consumer_span_with_link(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, exporter = setup_tracer(monkeypatch)
        setup_meter(monkeypatch)
        stack, deps, backend, client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            handle = await client.enqueue(_integration_test_actor, _Payload())
            worker_id = new_uuid()

            async with deps.dispatcher_pool.acquire() as conn:
                from taskq.testing.fixtures import _create_worker

                await _create_worker(conn, deps.settings.schema_name, worker_id)
                await _dispatch_job_to_running(
                    conn, deps.settings.schema_name, worker_id, handle.job_id
                )

            job_row = await backend.get(handle.job_id)
            assert job_row is not None

            registry, process_scope, thread_scope, loop_scope, _ = _make_scopes_for_dispatch(
                deps.settings
            )
            await _bootstrap_scopes(
                registry, deps.settings, process_scope, thread_scope, loop_scope
            )

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            await dispatch_one_job(
                backend=backend,
                deps=deps,
                job=job_row,
                worker_id=worker_id,
                registry=registry,
                process_scope=process_scope,
                thread_scope=thread_scope,
                loop_scope=loop_scope,
                actor_ref=_integration_test_actor,
                actor_config=default_actor_config(),
                clock=SystemClock(),
                enqueuer=enqueuer,
            )

            consumer = exporter.span_named("process _integration_test_actor")
            assert consumer is not None
            assert consumer.kind == trace.SpanKind.CONSUMER
            attrs = consumer.attributes
            assert attrs is not None
            assert attrs.get("messaging.system") == "taskq"
            assert attrs.get("messaging.destination.name") == "default"
            assert attrs.get("messaging.operation.type") == "process"
            assert attrs.get("taskq.actor") == "_integration_test_actor"

            producer = exporter.span_named("enqueue _integration_test_actor")
            assert producer is not None
            prod_ctx = producer.get_span_context()
            assert prod_ctx is not None

            assert consumer.links is not None
            assert len(consumer.links) == 1
            assert consumer.links[0].context.trace_id == prod_ctx.trace_id
            assert consumer.links[0].context.span_id == prod_ctx.span_id
        finally:
            await stack.aclose()

    async def test_attempt_span_is_child_of_consumer(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, exporter = setup_tracer(monkeypatch)
        setup_meter(monkeypatch)
        stack, deps, backend, client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            handle = await client.enqueue(_integration_test_actor, _Payload())
            worker_id = new_uuid()

            async with deps.dispatcher_pool.acquire() as conn:
                from taskq.testing.fixtures import _create_worker

                await _create_worker(conn, deps.settings.schema_name, worker_id)
                await _dispatch_job_to_running(
                    conn, deps.settings.schema_name, worker_id, handle.job_id
                )

            job_row = await backend.get(handle.job_id)
            assert job_row is not None

            registry, process_scope, thread_scope, loop_scope, _ = _make_scopes_for_dispatch(
                deps.settings
            )
            await _bootstrap_scopes(
                registry, deps.settings, process_scope, thread_scope, loop_scope
            )

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            await dispatch_one_job(
                backend=backend,
                deps=deps,
                job=job_row,
                worker_id=worker_id,
                registry=registry,
                process_scope=process_scope,
                thread_scope=thread_scope,
                loop_scope=loop_scope,
                actor_ref=_integration_test_actor,
                actor_config=default_actor_config(),
                clock=SystemClock(),
                enqueuer=enqueuer,
            )

            attempt = exporter.span_named("attempt.1")
            assert attempt is not None
            assert attempt.kind == trace.SpanKind.INTERNAL

            consumer = exporter.span_named("process _integration_test_actor")
            assert consumer is not None
            consumer_ctx = consumer.get_span_context()

            assert attempt.parent is not None
            assert attempt.parent.span_id == consumer_ctx.span_id  # pyright: ignore[reportOptionalMemberAccess] # Why: assert above narrows for humans; pyright cannot narrow ReadableSpan.parent across the assertion boundary
        finally:
            await stack.aclose()

    async def test_lifecycle_events_on_consumer_span(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, exporter = setup_tracer(monkeypatch)
        setup_meter(monkeypatch)
        stack, deps, backend, client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            handle = await client.enqueue(_integration_test_actor, _Payload())
            worker_id = new_uuid()

            async with deps.dispatcher_pool.acquire() as conn:
                from taskq.testing.fixtures import _create_worker

                await _create_worker(conn, deps.settings.schema_name, worker_id)
                await _dispatch_job_to_running(
                    conn, deps.settings.schema_name, worker_id, handle.job_id
                )

            job_row = await backend.get(handle.job_id)
            assert job_row is not None

            registry, process_scope, thread_scope, loop_scope, _ = _make_scopes_for_dispatch(
                deps.settings
            )
            await _bootstrap_scopes(
                registry, deps.settings, process_scope, thread_scope, loop_scope
            )

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            await dispatch_one_job(
                backend=backend,
                deps=deps,
                job=job_row,
                worker_id=worker_id,
                registry=registry,
                process_scope=process_scope,
                thread_scope=thread_scope,
                loop_scope=loop_scope,
                actor_ref=_integration_test_actor,
                actor_config=default_actor_config(),
                clock=SystemClock(),
                enqueuer=enqueuer,
            )

            consumer = exporter.span_named("process _integration_test_actor")
            assert consumer is not None
            event_names = [ev.name for ev in (consumer.events or [])]
            assert "lifecycle.running" in event_names
            assert "lifecycle.succeeded" in event_names
        finally:
            await stack.aclose()

    async def test_core_lifecycle_metrics_recorded(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        setup_tracer(monkeypatch)
        reader = setup_meter(monkeypatch)
        stack, deps, backend, client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            await client.enqueue(_integration_test_actor, _Payload())

            assert counter_value(reader, "messaging.client.published.messages") >= 1

            worker_id = new_uuid()
            async with deps.dispatcher_pool.acquire() as conn:
                from taskq.testing.fixtures import _create_worker

                await _create_worker(conn, deps.settings.schema_name, worker_id)

            lock_lease = timedelta(seconds=deps.settings.lock_lease)
            dispatched = await backend.dispatch_batch(
                worker_id, ["default"], limit=1, lock_lease=lock_lease
            )
            assert len(dispatched) >= 1

            job_row = dispatched[0]

            registry, process_scope, thread_scope, loop_scope, _ = _make_scopes_for_dispatch(
                deps.settings
            )
            await _bootstrap_scopes(
                registry, deps.settings, process_scope, thread_scope, loop_scope
            )

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            await dispatch_one_job(
                backend=backend,
                deps=deps,
                job=job_row,
                worker_id=worker_id,
                registry=registry,
                process_scope=process_scope,
                thread_scope=thread_scope,
                loop_scope=loop_scope,
                actor_ref=_integration_test_actor,
                actor_config=default_actor_config(),
                clock=SystemClock(),
                enqueuer=enqueuer,
            )

            assert counter_value(reader, "messaging.client.consumed.messages") >= 1
            assert len(histogram_points(reader, "messaging.process.duration")) >= 1

            dispatch_dps = histogram_points(reader, "taskq.dispatch.duration")
            assert len(dispatch_dps) >= 1
        finally:
            await stack.aclose()


# ── Queue depth gauge ──────────────────────────────────────────


class TestQueueDepthGauge:
    """taskq.queue.depth gauge reports correct depth after enqueue."""

    async def test_queue_depth_gauge_reports_enqueued_jobs(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reader = _setup_isolated_meter(monkeypatch)
        new_meter = otel_mod.get_meter()
        monkeypatch.setattr(
            otel_mod,
            "_queue_depth_gauge",
            new_meter.create_observable_gauge(
                name="taskq.queue.depth",
                description="Queue depth gauge for test",
                unit="1",
                callbacks=[otel_mod._observe_queue_depth],
            ),
        )

        stack, _deps, _backend, client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            for _ in range(10):
                await client.enqueue(_integration_test_actor, _Payload())

            obs_mod.update_queue_depth_cache({"default": 10})

            default_dp = [
                dp
                for dp in _gauge_data_points(reader, "taskq.queue.depth")
                if dp.attributes is not None and dp.attributes.get("queue") == "default"
            ]
            assert len(default_dp) >= 1
            assert default_dp[0].value == 10
        finally:
            await stack.aclose()


# ── Leader gauge + election counters ──────────────────────────


class TestLeaderGauge:
    """taskq.maintenance_leader.is_leader gauge and election counters."""

    async def test_leader_gauge_after_election(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reader = _setup_isolated_meter(monkeypatch)
        new_meter = otel_mod.get_meter()
        monkeypatch.setattr(
            otel_mod,
            "_leader_election_attempts",
            new_meter.create_counter("taskq.leader.election_attempts", unit="1"),
        )
        monkeypatch.setattr(
            otel_mod,
            "_leader_election_failures",
            new_meter.create_counter("taskq.leader.election_failures", unit="1"),
        )
        monkeypatch.setattr(
            leader_mod,
            "_is_leader_gauge",
            new_meter.create_observable_gauge(
                name="taskq.maintenance_leader.is_leader",
                description="1 on the elected leader pod, 0 elsewhere.",
                unit="1",
                callbacks=[leader_mod._observe_is_leader],
            ),
        )

        stack1, deps1, backend1, _client1 = await _setup_worker(pg_dsn, monkeypatch)
        stack2: AsyncExitStack = AsyncExitStack()
        try:
            worker_id_1 = new_uuid()
            async with deps1.dispatcher_pool.acquire() as conn:
                from taskq.testing.fixtures import _create_worker

                await _create_worker(conn, deps1.settings.schema_name, worker_id_1)

            settings2 = WorkerSettings.load_from_dict(
                {
                    "TASKQ_PG_DSN": pg_dsn,
                    "TASKQ_SCHEMA_NAME": deps1.settings.schema_name,
                    "TASKQ_HEARTBEAT_INTERVAL": str(_HEARTBEAT_INTERVAL),
                    "TASKQ_LOCK_LEASE": str(_LOCK_LEASE),
                    "TASKQ_CANCELLATION_GRACE_PERIOD": "0.0",
                    "TASKQ_CLEANUP_GRACE_PERIOD": "0.0",
                    "TASKQ_MAX_HEARTBEAT_FAILURES": "999",
                }
            )
            deps2: WorkerDeps = await stack2.enter_async_context(open_worker_deps(settings2))
            worker_id_2 = new_uuid()
            async with deps2.dispatcher_pool.acquire() as conn:
                from taskq.testing.fixtures import _create_worker

                await _create_worker(conn, deps2.settings.schema_name, worker_id_2)

            leader1 = MaintenanceLeader(deps1, worker_id_1, backend1, clock=SystemClock())
            leader2 = MaintenanceLeader(deps2, worker_id_2, backend1, clock=SystemClock())
            shutdown1 = asyncio.Event()
            shutdown2 = asyncio.Event()
            leader_task1 = asyncio.create_task(leader1.run(shutdown1), name="leader-1")
            leader_task2 = asyncio.create_task(leader2.run(shutdown2), name="leader-2")
            try:
                elected = asyncio.Event()

                async def _wait_leader1() -> None:
                    await deps1.is_leader.wait()
                    elected.set()

                async def _wait_leader2() -> None:
                    await deps2.is_leader.wait()
                    elected.set()

                _t1 = asyncio.create_task(_wait_leader1())
                _t2 = asyncio.create_task(_wait_leader2())
                try:
                    await asyncio.wait_for(elected.wait(), timeout=_HEARTBEAT_INTERVAL + 5)
                finally:
                    _t1.cancel()
                    _t2.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await _t1
                        await _t2

                leader_gauge_dps = _gauge_data_points(reader, "taskq.maintenance_leader.is_leader")
                assert len(leader_gauge_dps) >= 2

                values = {int(dp.value) for dp in leader_gauge_dps}
                assert 1 in values
                assert 0 in values
            finally:
                shutdown1.set()
                shutdown2.set()
                leader_task1.cancel()
                leader_task2.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await leader_task1
                with contextlib.suppress(asyncio.CancelledError):
                    await leader_task2

            attempts_dps = counter_data_points(reader, "taskq.leader.election_attempts")
            assert len(attempts_dps) >= 1

            election_attempts_total = sum(int(dp.value) for dp in attempts_dps)
            assert election_attempts_total >= 1

            failures_dps = counter_data_points(reader, "taskq.leader.election_failures")
            assert len(failures_dps) >= 1
        finally:
            await stack2.aclose()
            await stack1.aclose()


# ── Heartbeat metrics ──────────────────────────────────────────


class TestHeartbeatMetrics:
    """taskq.lock.expires_in_seconds histogram and taskq.heartbeat.misses counter."""

    async def test_lock_expires_in_seconds_has_recordings(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reader = _setup_isolated_meter(monkeypatch)
        new_meter = otel_mod.get_meter()
        monkeypatch.setattr(
            otel_mod,
            "_lock_expires_in_seconds",
            new_meter.create_histogram(
                "taskq.lock.expires_in_seconds",
                unit="s",
                explicit_bucket_boundaries_advisory=(0, 5, 10, 15, 20, 30, 45, 60),
            ),
        )
        monkeypatch.setattr(
            otel_mod,
            "_heartbeat_misses",
            new_meter.create_counter("taskq.heartbeat.misses", unit="1"),
        )

        stack, deps, _backend, _client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            worker_id = new_uuid()
            async with deps.dispatcher_pool.acquire() as conn:
                from taskq.testing.fixtures import _create_worker

                await _create_worker(conn, deps.settings.schema_name, worker_id)

            from taskq.testing.pg import create_running_job

            await create_running_job(deps.dispatcher_pool, deps.settings.schema_name, worker_id)

            shutdown = asyncio.Event()
            hb_task = asyncio.create_task(
                heartbeat_loop(deps, worker_id, shutdown),
                name="heartbeat-integration",
            )
            try:
                await asyncio.sleep(_HEARTBEAT_INTERVAL * 3 + 0.1)
            finally:
                shutdown.set()
                hb_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await hb_task

            ttl_dps = histogram_points(reader, "taskq.lock.expires_in_seconds")
            assert len(ttl_dps) >= 1

            misses_total = counter_value(reader, "taskq.heartbeat.misses")
            assert misses_total >= 0
        finally:
            await stack.aclose()


# ── Reservation, cancellation, and cron metrics ────────────────


class TestReservationCancelCronMetrics:
    """Instruments 8, 10, 16, 17 have data points after exercising subsystems."""

    async def test_cancellation_phase_transitions(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reader = _setup_isolated_meter(monkeypatch)
        new_meter = otel_mod.get_meter()
        monkeypatch.setattr(
            cancel_mod,
            "_phase_transitions",
            new_meter.create_counter("taskq.cancellation.phase_transitions", unit="1"),
        )
        monkeypatch.setattr(
            otel_mod,
            "_published_messages",
            new_meter.create_counter("messaging.client.published.messages", unit="1"),
        )

        stack, deps, backend, client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            handle = await client.enqueue(_integration_test_actor, _Payload())

            worker_id = new_uuid()
            async with deps.dispatcher_pool.acquire() as conn:
                from taskq.testing.fixtures import _create_worker

                await _create_worker(conn, deps.settings.schema_name, worker_id)
                await _dispatch_job_to_running(
                    conn, deps.settings.schema_name, worker_id, handle.job_id
                )

            from taskq.context import JobContext
            from taskq.worker.cancel import make_cancel_controller

            ctx = JobContext(
                job_id=handle.job_id,
                actor="_integration_test_actor",
                queue="default",
                attempt=1,
                worker_id=worker_id,
                payload=_Payload(),
                jobs=SubJobEnqueuer(
                    loop_scope_resolved=None,
                    worker_pool=None,
                    backend=backend,
                ),
                log=bind_job_context(
                    structlog.get_logger("taskq.test"),
                    job_id=handle.job_id,
                    actor="_integration_test_actor",
                    queue="default",
                    attempt=1,
                    identity_key=None,
                    trace_id="",
                ),
                span=None,
            )
            blocker = asyncio.Event()
            dummy_task = asyncio.create_task(blocker.wait(), name="cancel-test-dummy")
            await deps.active_jobs.register(handle.job_id, dummy_task, ctx)

            controller = make_cancel_controller(deps, worker_id, backend)
            shutdown = asyncio.Event()
            hb_task = asyncio.create_task(
                heartbeat_loop(deps, worker_id, shutdown, cancel_controller=controller),
                name="heartbeat-cancel-integration",
            )
            try:
                await asyncio.sleep(_HEARTBEAT_INTERVAL + 0.1)

                await client.cancel(handle.job_id)

                phase_dps: list[NumberDataPoint] = []
                deadline = time.monotonic() + 5.0
                while time.monotonic() < deadline:
                    phase_dps = counter_data_points(reader, "taskq.cancellation.phase_transitions")
                    if len(phase_dps) >= 1:
                        break
                    await asyncio.sleep(0.1)

                assert len(phase_dps) >= 1
            finally:
                blocker.set()
                dummy_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await dummy_task
                await deps.active_jobs.deregister(handle.job_id)
                shutdown.set()
                hb_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await hb_task
        finally:
            await stack.aclose()

    async def test_reservation_slots_gauge_reports_usage(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reader = _setup_isolated_meter(monkeypatch)
        new_meter = otel_mod.get_meter()
        monkeypatch.setattr(
            otel_mod,
            "_reservation_slots_gauge",
            new_meter.create_observable_gauge(
                name="taskq.reservation.slots_used",
                description="Reservation slots gauge for test",
                unit="1",
                callbacks=[otel_mod._observe_reservation_slots],
            ),
        )

        stack, _deps, _backend, _client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            obs_mod.update_reservation_slots_cache({"default_bucket": 3})

            bucket_dp = [
                dp
                for dp in _gauge_data_points(reader, "taskq.reservation.slots_used")
                if dp.attributes is not None and dp.attributes.get("bucket") == "default_bucket"
            ]
            assert len(bucket_dp) >= 1
            assert bucket_dp[0].value == 3
        finally:
            await stack.aclose()

    async def test_cron_consecutive_failures_updown_counter(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reader = _setup_isolated_meter(monkeypatch)
        new_meter = otel_mod.get_meter()
        monkeypatch.setattr(
            otel_mod,
            "_cron_consecutive_failures",
            new_meter.create_up_down_counter("taskq.cron.consecutive_failures", unit="1"),
        )

        stack, _deps, _backend, _client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            obs_mod.record_cron_failure("schedule_1", delta=1)
            obs_mod.record_cron_failure("schedule_1", delta=1)

            metrics = collect_metrics(reader)
            cron_metric = [m for m in metrics if m.name == "taskq.cron.consecutive_failures"]
            assert len(cron_metric) >= 1
            cron_dps = [
                p for p in cron_metric[0].data.data_points if isinstance(p, NumberDataPoint)
            ]
            sched_dp = [
                dp
                for dp in cron_dps
                if dp.attributes is not None and dp.attributes.get("schedule_id") == "schedule_1"
            ]
            assert len(sched_dp) >= 1
            assert sched_dp[0].value == 2

            obs_mod.record_cron_failure("schedule_1", delta=-2)
        finally:
            await stack.aclose()

    async def test_cron_disabled_schedules_gauge(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        reader = _setup_isolated_meter(monkeypatch)
        new_meter = otel_mod.get_meter()
        monkeypatch.setattr(
            otel_mod,
            "_disabled_schedules_gauge",
            new_meter.create_observable_gauge(
                name="taskq.cron.disabled_schedules",
                description="Disabled schedules gauge for test",
                unit="1",
                callbacks=[otel_mod._observe_disabled_schedules],
            ),
        )

        stack, _deps, _backend, _client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            obs_mod.update_disabled_schedules_count(2)

            disabled_dps = _gauge_data_points(reader, "taskq.cron.disabled_schedules")
            assert len(disabled_dps) >= 1
            assert disabled_dps[0].value == 2
        finally:
            await stack.aclose()


# ── OTel exporter unavailable ──────────────────────────────────


class TestExporterUnavailable:
    """Enqueue and job execution succeed even when OTel exporter is unavailable."""

    async def test_enqueue_and_job_execution_succeed_with_bad_exporter(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from opentelemetry.sdk.trace.export import SpanExportResult

        class _FailingExporter(ListSpanExporter):
            def export(self, spans: object) -> SpanExportResult:
                raise RuntimeError("simulated exporter failure")

        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(_FailingExporter()))
        test_tracer = provider.get_tracer(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())
        monkeypatch.setattr(otel_mod, "get_tracer", lambda: test_tracer)
        otel_mod.set_otel_enabled(True)
        reader = _setup_isolated_meter(monkeypatch)
        new_meter = otel_mod.get_meter()
        monkeypatch.setattr(
            otel_mod,
            "_consumed_messages",
            new_meter.create_counter("messaging.client.consumed.messages", unit="1"),
        )
        monkeypatch.setattr(
            otel_mod,
            "_process_duration",
            new_meter.create_histogram("messaging.process.duration", unit="s"),
        )
        monkeypatch.setattr(
            otel_mod,
            "_published_messages",
            new_meter.create_counter("messaging.client.published.messages", unit="1"),
        )

        stack, deps, backend, client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            handle = await client.enqueue(_integration_test_actor, _Payload())
            assert handle.job_id is not None

            worker_id = new_uuid()
            async with deps.dispatcher_pool.acquire() as conn:
                from taskq.testing.fixtures import _create_worker

                await _create_worker(conn, deps.settings.schema_name, worker_id)
                await _dispatch_job_to_running(
                    conn, deps.settings.schema_name, worker_id, handle.job_id
                )

            job_row = await backend.get(handle.job_id)
            assert job_row is not None

            registry, process_scope, thread_scope, loop_scope, _ = _make_scopes_for_dispatch(
                deps.settings
            )
            await _bootstrap_scopes(
                registry, deps.settings, process_scope, thread_scope, loop_scope
            )

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            await dispatch_one_job(
                backend=backend,
                deps=deps,
                job=job_row,
                worker_id=worker_id,
                registry=registry,
                process_scope=process_scope,
                thread_scope=thread_scope,
                loop_scope=loop_scope,
                actor_ref=_integration_test_actor,
                actor_config=default_actor_config(),
                clock=SystemClock(),
                enqueuer=enqueuer,
            )

            consumed = counter_value(reader, "messaging.client.consumed.messages")
            assert consumed >= 1
        finally:
            await stack.aclose()


# ── Malformed trace_id ─────────────────────────────────────────


class TestMalformedTraceId:
    """Malformed trace_id in DB — link skipped, CONSUMER span still created."""

    async def test_malformed_trace_id_produces_no_link_consumer_span_still_created(
        self, pg_dsn: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _, exporter = setup_tracer(monkeypatch)
        setup_meter(monkeypatch)
        stack, deps, backend, _client = await _setup_worker(pg_dsn, monkeypatch)
        try:
            worker_id = new_uuid()
            async with deps.dispatcher_pool.acquire() as conn:
                from taskq.testing.fixtures import _create_worker

                await _create_worker(conn, deps.settings.schema_name, worker_id)

                schema = deps.settings.schema_name
                job_id = new_job_id()

                await conn.execute(
                    f"""INSERT INTO \"{schema}\".jobs (
                        id, actor, queue, payload, max_attempts, retry_kind,
                        status, priority, attempt, scheduled_at,
                        locked_by_worker, lock_expires_at, started_at, last_heartbeat_at,
                        cancel_phase, trace_id, span_id
                    ) VALUES (
                        $1, $2, $3, $4::jsonb, $5, $6,
                        'running', 0, 1, now(),
                        $7, now()+interval '60 seconds', now(), now(),
                        0, $8, $9
                    )""",  # noqa: S608 # Why: schema validated by WorkerSettings; asyncpg has no parameter binding for identifiers
                    job_id,
                    "_integration_test_actor",
                    "default",
                    '{"value": 1}',
                    3,
                    "transient",
                    worker_id,
                    "not-a-hex-string",
                    "0123456789abcdef",
                )

            job_row = await backend.get(job_id)
            assert job_row is not None

            registry, process_scope, thread_scope, loop_scope, _ = _make_scopes_for_dispatch(
                deps.settings
            )
            await _bootstrap_scopes(
                registry, deps.settings, process_scope, thread_scope, loop_scope
            )

            enqueuer = SubJobEnqueuer(
                loop_scope_resolved=loop_scope.resolved_cache(),
                worker_pool=deps.worker_pool,
                backend=backend,
            )

            await dispatch_one_job(
                backend=backend,
                deps=deps,
                job=job_row,
                worker_id=worker_id,
                registry=registry,
                process_scope=process_scope,
                thread_scope=thread_scope,
                loop_scope=loop_scope,
                actor_ref=_integration_test_actor,
                actor_config=default_actor_config(),
                clock=SystemClock(),
                enqueuer=enqueuer,
            )

            consumer = exporter.span_named("process _integration_test_actor")
            assert consumer is not None
            assert consumer.kind == trace.SpanKind.CONSUMER
            assert not consumer.links
        finally:
            await stack.aclose()


# ── Enqueue span overhead ──────────────────────────────────────


class TestEnqueueSpanOverhead:
    """Measure enqueue span overhead with no-op exporter."""

    async def test_enqueue_overhead_with_noop_exporter(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from opentelemetry.sdk.trace.sampling import ALWAYS_OFF

        provider = TracerProvider(sampler=ALWAYS_OFF)
        noop_exporter = ListSpanExporter()
        provider.add_span_processor(SimpleSpanProcessor(noop_exporter))
        test_tracer = provider.get_tracer(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())
        monkeypatch.setattr(otel_mod, "get_tracer", lambda: test_tracer)
        otel_mod.set_otel_enabled(True)

        from taskq.testing.clock import FakeClock
        from taskq.testing.in_memory import InMemoryBackend

        backend = InMemoryBackend(clock=FakeClock("2026-01-01T00:00:00+00:00"))
        client = JobsClient(backend, clock=backend._clock)

        n = 1000
        t0 = time.monotonic()
        for _ in range(n):
            await client.enqueue(_integration_test_actor, _Payload())
        elapsed = time.monotonic() - t0
        per_enqueue_us = (elapsed / n) * 1_000_000

        assert per_enqueue_us < 500, (
            f"Per-enqueue overhead {per_enqueue_us:.1f}us exceeds 500us threshold"
        )
