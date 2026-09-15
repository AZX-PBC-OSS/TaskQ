"""Sweep telemetry must be emitted on the TIMEOUT path, not only on success.

The original invisibility: the scheduled-wake sweep's duration histogram
and row counter were recorded only AFTER the awaited call returned, so the
deadline that aborted the sweep also aborted the code that would have
recorded it — a livelocking sweep emitted NO samples at all, not zero.
The pins here: duration always (including the failure path), a timeout
counter on the deadline family, and NO row sample when the call timed out
(rows stay unbound — a 0-row sample would be indistinguishable from a
healthy empty sweep). The success path stays fully instrumented.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import cast

import asyncpg
import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskq.testing.assertions import wait_for_condition
from taskq.testing.otel import (
    collect_metrics,
    counter_data_points,
    counter_value,
    histogram_points,
)

_DURATION_METRIC = "taskq.maintenance_leader.sweep_duration_ms"
_ROWS_METRIC = "taskq.maintenance_leader.sweep_rows"
_TIMEOUTS_METRIC = "taskq.maintenance_leader.sweep_timeouts"


@pytest.fixture
def sweep_metric_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation for the leader sweep instruments.

    Replaces the module-level instruments the leader loops call through —
    ``taskq.worker._leader_shared``'s duration histogram and rows counter,
    plus the obs-level sweep-timeouts counter — with fresh SDK instruments
    backed by ``InMemoryMetricReader``. ``_otel_enabled`` is forced on
    because ``record_sweep_timeout`` is a no-op while it is off.
    monkeypatch auto-restores everything, including a fresh success-stamp
    cache so a prior test's stamp cannot satisfy this one's assertion.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    import taskq.obs as obs_mod
    import taskq.obs._otel as otel_mod
    import taskq.worker._leader_shared as shared_mod

    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(
        obs_mod.INSTRUMENTATION_NAME,
        otel_mod._version(),  # pyright: ignore[reportPrivateUsage]  # Why: same private-helper access as tests/test_obs_sweep_counters.py's metric_reader fixture.
    )

    monkeypatch.setattr(
        shared_mod,
        "_sweep_duration_hist",
        meter.create_histogram(_DURATION_METRIC, unit="ms"),
    )
    monkeypatch.setattr(
        shared_mod,
        "_sweep_rows_counter",
        meter.create_counter(_ROWS_METRIC, unit="1"),
    )
    monkeypatch.setattr(
        otel_mod,
        "_sweep_timeouts",
        meter.create_counter(_TIMEOUTS_METRIC, unit="1"),
    )
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    monkeypatch.setattr(otel_mod, "_sweep_success_cache", {})  # pyright: ignore[reportPrivateUsage]  # Why: fresh cache for the success-stamp assertion; monkeypatch restores the module's dict afterwards.
    return reader


def _samples_for_sweep(reader: InMemoryMetricReader, name: str) -> int:
    """Sum of histogram sample COUNTS recorded for sweep_name == *name*."""
    total = 0
    for point in histogram_points(reader, _DURATION_METRIC):
        if point.attributes == {"sweep_name": name}:
            total += int(point.count)
    return total


class _StalledBackend:
    """Backend stand-in whose scheduled_to_pending never beats the deadline."""

    def __init__(self, stall_secs: float) -> None:
        self._stall_secs = stall_secs
        self.calls = 0

    async def scheduled_to_pending(self) -> int:
        self.calls += 1
        await asyncio.sleep(self._stall_secs)
        return 0  # unreachable under the deadline; keeps the return type honest


class _InstantBackend:
    """Backend stand-in promoting instantly — the healthy sweep shape."""

    def __init__(self, promoted: int) -> None:
        self._promoted = promoted
        self.calls = 0

    async def scheduled_to_pending(self) -> int:
        self.calls += 1
        return self._promoted


class _FakeConn:
    async def execute(self, sql: str, *args: object) -> str:
        return "OK"


class _FakePool:
    """Dispatcher-pool stand-in for the count>0 wake NOTIFY."""

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_FakeConn, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's keyword-only timeout.
        yield _FakeConn()


def _make_wake_leader(
    backend: object,
    *,
    command_timeout: float,
    dispatcher_pool: object,
) -> object:
    """A MaintenanceLeader carrying only what _scheduled_wake_loop touches.

    The SimpleNamespace deps are the established light seam for this loop
    (tests/test_watchdog_safety.py): liveness, the leader gate, settings
    with the command timeout, and the dispatcher pool for the wake NOTIFY.
    """
    from types import SimpleNamespace
    from typing import cast

    from taskq._ids import new_uuid
    from taskq.backend._protocol import Backend
    from taskq.backend.clock import SystemClock
    from taskq.worker._watchdog import LoopLiveness
    from taskq.worker.deps import WorkerDeps
    from taskq.worker.leader import MaintenanceLeader

    is_leader = asyncio.Event()
    is_leader.set()
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=LoopLiveness(),
            is_leader=is_leader,
            settings=SimpleNamespace(
                schema_name="taskq", dispatcher_command_timeout=command_timeout
            ),
            dispatcher_pool=dispatcher_pool,
        ),
    )
    return MaintenanceLeader(
        deps,
        new_uuid(),
        cast(Backend, backend),
        clock=SystemClock(),
    )


async def _drive_one_iteration(
    leader: object,
    backend: _StalledBackend | _InstantBackend,
    reader: InMemoryMetricReader,
) -> None:
    """Run ``_scheduled_wake_loop`` until its first iteration's telemetry is
    observable, then stop the loop.

    The stop condition is the metric itself (not a fixed sleep) so the
    timeout-path test cannot stop the loop before the deadline fires.
    """
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))  # pyright: ignore[reportAttributeAccessIssue]  # Why: leader is a real MaintenanceLeader; object-typed to keep the lazy import out of this helper's signature.
    try:
        await wait_for_condition(
            lambda: backend.calls >= 1 and _samples_for_sweep(reader, "scheduled_to_pending") >= 1,
            description="the wake loop must attempt the sweep and record its telemetry",
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_timeout_path_records_duration_and_timeout_without_row_sample(
    sweep_metric_reader: InMemoryMetricReader,
) -> None:
    """The assertion the original invisibility escaped: a sweep call cut
    short by the iteration deadline still records a duration sample and a
    sweep_timeouts increment — and records NO row sample."""
    backend = _StalledBackend(stall_secs=0.5)
    leader = _make_wake_leader(backend, command_timeout=0.05, dispatcher_pool=None)
    await _drive_one_iteration(leader, backend, sweep_metric_reader)

    assert _samples_for_sweep(sweep_metric_reader, "scheduled_to_pending") >= 1, (
        "a timed-out sweep recorded no duration sample — the failure path is invisible again"
    )
    timeout_points = [
        dp
        for dp in counter_data_points(sweep_metric_reader, _TIMEOUTS_METRIC)
        if dp.attributes == {"sweep_name": "scheduled_to_pending"}
    ]
    assert len(timeout_points) == 1 and timeout_points[0].value == 1, (
        "the deadline family must increment taskq.maintenance_leader.sweep_timeouts"
    )
    row_points = [
        dp
        for dp in counter_data_points(sweep_metric_reader, _ROWS_METRIC)
        if dp.attributes == {"sweep_name": "scheduled_to_pending"}
    ]
    assert row_points == [], (
        "a timed-out sweep must not record a row sample — rows stayed unbound, "
        "and a 0-row sample would be indistinguishable from a healthy empty sweep"
    )


async def test_success_path_records_rows_and_success_stamp_without_timeout(
    sweep_metric_reader: InMemoryMetricReader,
) -> None:
    """The healthy sweep stays fully instrumented: rows=3 on the counter, a
    success stamp, a duration sample — and no timeout."""
    backend = _InstantBackend(promoted=3)
    leader = _make_wake_leader(backend, command_timeout=5.0, dispatcher_pool=_FakePool())
    await _drive_one_iteration(leader, backend, sweep_metric_reader)

    assert _samples_for_sweep(sweep_metric_reader, "scheduled_to_pending") >= 1
    row_points = [
        dp
        for dp in counter_data_points(sweep_metric_reader, _ROWS_METRIC)
        if dp.attributes == {"sweep_name": "scheduled_to_pending"}
    ]
    assert len(row_points) == 1 and row_points[0].value == 3, (
        f"a successful sweep promoting 3 must record rows=3; got {row_points}"
    )
    assert counter_value(sweep_metric_reader, _TIMEOUTS_METRIC) == 0

    import taskq.obs._otel as otel_mod

    assert otel_mod._sweep_success_cache.get("scheduled_to_pending") is not None, (  # pyright: ignore[reportPrivateUsage]  # Why: reading the cache the fixture replaced; the stamp is the success gauge's input.
        "a successful sweep must stamp sweep_last_success_seconds"
    )
    # The reader must actually be wired: one collect, non-empty, proves the
    # instruments above are the ones the loop called.
    assert collect_metrics(sweep_metric_reader), "expected collected metrics"


# ── gauge sampler loops: the failure path must reach the metric plane ──
#
# The three gauge samplers the leader runs (queue depth, backlog
# detection, reservation slots) feed gauges an operator alerts on. When
# a sampler's query fails, the gauge it feeds does not go missing — it
# keeps reporting the LAST value it was fed. A frozen depth gauge and a
# healthy flat backlog are the same picture, so a metrics-only operator
# (the normal case; logs and metrics are usually different systems, and
# alert rules read metrics) cannot tell that the sampler died. These
# pins require each sampler's failure to reach the metric plane under
# its own sweep_name, exactly as the scheduled-wake sweep's does.


class _FailingConn:
    """Connection stand-in whose every read raises, as a dead sampler query does."""

    def __init__(self, exc: Exception) -> None:
        self._exc = exc
        self.calls = 0

    async def fetch(self, sql: str, *args: object) -> list[object]:
        self.calls += 1
        raise self._exc

    async def fetchval(self, sql: str, *args: object) -> object:
        self.calls += 1
        raise self._exc

    async def execute(self, sql: str, *args: object) -> str:
        return "OK"

    def is_closed(self) -> bool:
        return False


class _ConnPool:
    """Dispatcher-pool stand-in yielding one fixed connection."""

    def __init__(self, conn: object) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[object, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's keyword-only timeout.
        yield self._conn


def _sampler_ctx(dispatcher_pool: object) -> object:
    """A SweepContext for the gauge samplers, with leadership held.

    Only the settings, pool and leader gate the sampler loops read are
    populated; the sampling interval is short so one failing iteration is
    observable without a sleep-based wait.
    """
    from datetime import UTC, datetime

    from taskq._ids import new_uuid
    from taskq.backend._protocol import Backend
    from taskq.settings import WorkerSettings
    from taskq.testing.clock import FakeClock
    from taskq.worker._leader_shared import SweepContext
    from taskq.worker.deps import WorkerDeps

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://taskq:taskq@127.0.0.1:1/taskq",
            "TASKQ_QUEUE_DEPTH_INTERVAL": "0.01",
            "TASKQ_RESERVATION_SLOTS_INTERVAL": "0.01",
        },
        validate=False,
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=dispatcher_pool,  # pyright: ignore[reportArgumentType]  # Why: pool stand-in satisfying the acquire() surface the sampler loops use, the same seam as tests/test_rt_worker_backlog_loop.py.
        heartbeat_pool=dispatcher_pool,  # pyright: ignore[reportArgumentType]  # Why: see above.
        worker_pool=dispatcher_pool,  # pyright: ignore[reportArgumentType]  # Why: see above.
        notify_conn=None,
        leader_conn=None,
    )
    deps.is_leader.set()
    return SweepContext(
        deps=deps,
        backend=cast("Backend", object()),
        clock=FakeClock(datetime(2025, 1, 1, tzinfo=UTC)),
        worker_id=new_uuid(),
    )


async def _drive_failing_sampler(
    loop_name: str,
    conn: _FailingConn,
    reader: InMemoryMetricReader,
    sweep_name: str,
) -> None:
    """Run one gauge-sampler loop against a connection whose reads raise.

    Stops as soon as the sampler has attempted its query at least twice,
    so the assertions read a settled failure rather than racing the first
    iteration.
    """
    from taskq.worker import _leader_sweeps

    loop_fn = getattr(_leader_sweeps, loop_name)
    ctx = _sampler_ctx(_ConnPool(conn))
    shutdown = asyncio.Event()
    task = asyncio.create_task(loop_fn(ctx, shutdown))
    try:
        await wait_for_condition(
            lambda: conn.calls >= 2,
            description=f"{sweep_name} sampler must attempt its query and fail",
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def _timeout_value(reader: InMemoryMetricReader, sweep_name: str) -> int:
    return sum(
        int(dp.value)
        for dp in counter_data_points(reader, _TIMEOUTS_METRIC)
        if dp.attributes == {"sweep_name": sweep_name}
    )


@pytest.mark.parametrize(
    ("loop_name", "sweep_name"),
    [
        ("_queue_depth_loop", "queue_depth"),
        ("_backlog_detection_loop", "backlog_detection"),
        ("_reservation_slots_loop", "reservation_slots"),
    ],
)
async def test_gauge_sampler_failure_is_visible_on_the_metric_plane(
    sweep_metric_reader: InMemoryMetricReader,
    loop_name: str,
    sweep_name: str,
) -> None:
    """A gauge sampler whose query fails every tick is countable in metrics.

    This is the failure that hides best in this system. The gauge the
    sampler feeds keeps serving its last value, so depth stops rising,
    age stops growing and every dashboard flattens out — the same picture
    a drained queue paints. Nothing fails, no job is affected, and the
    only trace is a WARN line in a log stream that alert rules do not
    read. The contract: the failure reaches the metric plane under the
    sampler's own sweep_name, so a rule can fire on a detector that has
    stopped detecting.
    """
    conn = _FailingConn(asyncpg.PostgresConnectionError("connection reset"))
    await _drive_failing_sampler(loop_name, conn, sweep_metric_reader, sweep_name)

    assert conn.calls >= 2, "the sampler never ran its query; the setup, not the system, failed"
    assert _timeout_value(sweep_metric_reader, sweep_name) >= 1, (
        f"the {sweep_name} sampler failed every tick and emitted no metric naming it — "
        "the gauge it feeds keeps reporting its last value, so a stalled detector is "
        "indistinguishable from a healthy flat backlog to anyone reading metrics"
    )


@pytest.mark.parametrize(
    ("loop_name", "sweep_name"),
    [
        ("_queue_depth_loop", "queue_depth"),
        ("_backlog_detection_loop", "backlog_detection"),
        ("_reservation_slots_loop", "reservation_slots"),
    ],
)
async def test_failing_gauge_sampler_publishes_no_success_stamp(
    sweep_metric_reader: InMemoryMetricReader,
    loop_name: str,
    sweep_name: str,
) -> None:
    """A sampler that never completes a read never stamps success.

    The success stamp drives the staleness view an operator uses to ask
    "is this loop still making progress?". A stamp written before or
    regardless of the read completing turns that view into a liveness
    check on the loop's ``while`` statement, which survives every failure
    the read can have.
    """
    import taskq.obs._otel as otel_mod

    conn = _FailingConn(asyncpg.PostgresConnectionError("connection reset"))
    await _drive_failing_sampler(loop_name, conn, sweep_metric_reader, sweep_name)

    assert otel_mod._sweep_success_cache.get(sweep_name) is None, (  # pyright: ignore[reportPrivateUsage]  # Why: the stamp cache is the staleness gauge's input; the fixture installs a fresh one per test.
        f"the {sweep_name} sampler stamped success while every one of its reads raised — "
        "the staleness view now reports a dead detector as healthy"
    )
