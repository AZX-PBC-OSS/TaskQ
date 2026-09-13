"""The lock-contention emitters at the REAL acquisition sites.

The prune and archive-expiry loops lose the schema-qualified advisory
lock to another session at their ``pg_try_advisory_lock`` call. The
existing coverage pins the SKIP behavior (no sweep runs); what was never
pinned is the new emitter: the losing side must record
``taskq.leader.lock_contention`` with the exact lock name — the signal
that would have exposed two schemas silently sharing one lock, and the
only observable difference between "pruned by someone else" and "never
prunes at all".

These tests hold the lock with a REAL second connection on the REAL
schema-qualified name and drive one loop iteration whose lock attempt
runs on a REAL connection — the emitter is pinned at the acquisition
site, not by calling the obs function directly. The asserted surface is
behavioural throughout: the emitted counter, no sweep work running, and
the loop retrying rather than dying.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import cast

import asyncpg
import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.constants import schema_lock_name
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.worker import _leader_sweeps
from taskq.worker._leader_shared import SweepContext
from taskq.worker._leader_sweeps import _archive_expiry_loop, _prune_loop
from taskq.worker.deps import WorkerDeps

pytestmark = pytest.mark.integration

_CONTENTION_METRIC = "taskq.leader.lock_contention"


class _RealConnPool:
    """Pool stand-in yielding a REAL connection (the lock attempt must
    actually contend with the holder's session)."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(
        self,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's keyword-only timeout.
    ) -> AsyncGenerator[asyncpg.Connection, None]:
        yield self._conn


class _InstantCroniter:
    """Croniter stub that always fires ~50 ms in the future, so the loop
    reaches the lock attempt quickly (same seam as the coverage tests)."""

    def __init__(self, expr: str, start_time: object) -> None:
        pass

    def get_next(self, dt_type: type[datetime]) -> datetime:
        return datetime.now(UTC) + timedelta(seconds=0.05)


@pytest.fixture
def contention_metrics(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Fresh SDK instrument for the lock-contention counter, enabled."""
    from opentelemetry.sdk.metrics import MeterProvider

    import taskq.obs as obs_mod
    import taskq.obs._otel as otel_mod

    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(
        obs_mod.INSTRUMENTATION_NAME,
        otel_mod._version(),  # pyright: ignore[reportPrivateUsage]  # Why: same private-helper access as the sweep-metrics fixture.
    )
    monkeypatch.setattr(otel_mod, "_lock_contention", meter.create_counter(_CONTENTION_METRIC))
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    return reader


def _ctx(conn: asyncpg.Connection, *, schema: str) -> SweepContext:
    settings = WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": "postgresql://x:x@localhost/x", "TASKQ_SCHEMA_NAME": schema},
        validate=False,
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_RealConnPool(conn),  # pyright: ignore[reportArgumentType]  # Why: the loop's lock attempt must run on a real connection against the real holder.
        heartbeat_pool=_RealConnPool(conn),  # pyright: ignore[reportArgumentType]
        worker_pool=_RealConnPool(conn),  # pyright: ignore[reportArgumentType]
        notify_conn=None,
        leader_conn=None,
    )
    deps.is_leader.set()
    return SweepContext(
        deps=deps,
        backend=cast("Backend", object()),
        # A clock these loops never consult: their cadence is croniter's
        # wall-clock fire time, stubbed per-test.
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        worker_id=new_uuid(),
    )


async def _drive_loop_while_lock_held(
    loop: Callable[[SweepContext, asyncio.Event], Awaitable[None]],
    ctx: SweepContext,
    *,
    lock_name: str,
    reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
    min_losses: int = 2,
) -> int:
    """Run one prune/archive loop while another session holds its lock,
    until the contention counter has recorded *min_losses* losses for the
    exact lock name, then stop it. Returns the recorded loss count.

    The wait condition is the emitted metric, not log text: the metric is
    the observable carrier of the contract (the losing side records the
    loss), and log capture is an implementation detail no test should
    gate on. ``min_losses >= 2`` proves the loop retried the acquisition
    after a loss rather than dying."""
    monkeypatch.setattr(_leader_sweeps.cr, "croniter", _InstantCroniter)
    shutdown = asyncio.Event()
    task = asyncio.create_task(loop(ctx, shutdown))
    try:
        for _ in range(400):
            points = _contention_points(reader, lock_name)
            if points and points[0].value >= min_losses:
                break
            await asyncio.sleep(0.01)
        assert not task.done(), (
            "the loop died after losing the lock — a teardown mutes the "
            "contention signal permanently"
        )
        points = _contention_points(reader, lock_name)
        assert points, (
            f"no lock_contention recorded for {lock_name!r} — the losing side "
            "is the detector and must record the loss"
        )
        return int(points[0].value)
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def _contention_points(reader: InMemoryMetricReader, lock_name: str) -> list[NumberDataPoint]:
    from taskq.testing.otel import counter_data_points

    return [
        dp
        for dp in counter_data_points(reader, _CONTENTION_METRIC)
        if dp.attributes == {"lock": lock_name}
    ]


async def test_prune_loop_lock_loss_emits_contention_with_lock_name(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    contention_metrics: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Another session holds the prune lock on the schema-qualified name:
    the prune loop's losing acquisition must record lock_contention with
    that exact name, run NO prune work, and stay alive to retry."""
    schema = module_pg_schema.schema_name
    lock_name = schema_lock_name("prune", schema)

    prune_calls: list[object] = []
    original_prune = _leader_sweeps.prune_terminal_jobs

    async def _spy_prune(*args: object, **kwargs: object) -> object:
        prune_calls.append(args)
        return await original_prune(*args, **kwargs)  # pyright: ignore[reportAny]  # Why: passthrough spy; the return type is whatever the real sweep returns and is never read here.

    monkeypatch.setattr(_leader_sweeps, "prune_terminal_jobs", _spy_prune)

    holder = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        got = await holder.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
        )
        assert got is True, "test setup: the holder must own the prune lock"

        ctx = _ctx(clean_pg_conn, schema=schema)
        losses = await _drive_loop_while_lock_held(
            _prune_loop,
            ctx,
            lock_name=lock_name,
            reader=contention_metrics,
            monkeypatch=monkeypatch,
        )

        assert losses >= 2, "the loop must retry the acquisition after a lost lock, not die or hang"
        assert not prune_calls, "no prune work may run while another session holds the lock"
    finally:
        await holder.close()


async def test_archive_expiry_loop_lock_loss_emits_contention_with_lock_name(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    contention_metrics: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same pin for the archive-expiry acquisition site."""
    schema = module_pg_schema.schema_name
    lock_name = schema_lock_name("archive_expiry", schema)

    sweep_calls: list[object] = []
    original_sweep = _leader_sweeps.archive_expiry_sweep

    async def _spy_sweep(*args: object, **kwargs: object) -> object:
        sweep_calls.append(args)
        return await original_sweep(*args, **kwargs)  # pyright: ignore[reportAny]  # Why: passthrough spy; the return type is whatever the real sweep returns and is never read here.

    monkeypatch.setattr(_leader_sweeps, "archive_expiry_sweep", _spy_sweep)

    holder = await asyncpg.connect(module_pg_schema.pg_dsn)
    try:
        got = await holder.fetchval(
            "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", lock_name
        )
        assert got is True, "test setup: the holder must own the archive-expiry lock"

        ctx = _ctx(clean_pg_conn, schema=schema)
        losses = await _drive_loop_while_lock_held(
            _archive_expiry_loop,
            ctx,
            lock_name=lock_name,
            reader=contention_metrics,
            monkeypatch=monkeypatch,
        )

        assert losses >= 2, "the loop must retry the acquisition after a lost lock, not die or hang"
        assert not sweep_calls, (
            "no archive-expiry work may run while another session holds the lock"
        )
    finally:
        await holder.close()
