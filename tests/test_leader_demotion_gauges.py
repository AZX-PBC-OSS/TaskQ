"""Leader-only gauges must stop exporting when this process loses leadership.

``taskq.queue.depth``, ``taskq.reservation.slots_used`` and
``taskq.jobs.stranded`` are populated ONLY by the elected leader's sweep
loops.  A demoted worker that keeps its last cached values exports numbers
it no longer has any authority over -- and it exports them during a
failover, which is precisely when an operator is reading the dashboard.

"Cleared" here means ABSENT, not zero: an observable gauge whose callback
yields nothing produces no data point, so the collector marks the series
stale and the new leader's series is the only one answering.  Exporting a
zero would instead be an active assertion that the queue is empty, which
silences ``queue_depth > N`` alerts and corrupts any ``sum``/``min``
aggregation across pods.
"""

import asyncio
from collections.abc import Iterator
from types import SimpleNamespace
from typing import TYPE_CHECKING, cast
from uuid import uuid4

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.backend._protocol import Backend
from taskq.backend.clock import SystemClock
from taskq.testing.otel import collect_metrics
from taskq.worker._watchdog import LoopLiveness
from taskq.worker.leader import MaintenanceLeader

if TYPE_CHECKING:
    from taskq.worker.deps import WorkerDeps
else:
    WorkerDeps = object

_LEADER_ONLY_GAUGES: tuple[tuple[str, str], ...] = (
    ("taskq.queue.depth", "_queue_depth_gauge"),
    ("taskq.reservation.slots_used", "_reservation_slots_gauge"),
    ("taskq.jobs.stranded", "_stranded_jobs_gauge"),
)

_CALLBACKS: dict[str, str] = {
    "taskq.queue.depth": "_observe_queue_depth",
    "taskq.reservation.slots_used": "_observe_reservation_slots",
    "taskq.jobs.stranded": "_observe_stranded_jobs",
}


def _isolated_leader_gauges(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Re-create the three leader-only observable gauges on a private reader.

    Follows the established pattern in ``tests/test_otel_integration.py``:
    the module-level callbacks are kept (they are what reads the caches) and
    only the meter/instrument objects are swapped, so the assertions run
    against the real exported metric stream.
    """
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())  # pyright: ignore[reportPrivateUsage]  # Why: mirrors _setup_isolated_meter in tests/test_otel_integration.py.
    monkeypatch.setattr(otel_mod, "get_meter", lambda: meter)
    otel_mod.set_otel_enabled(True)
    for name, attr in _LEADER_ONLY_GAUGES:
        monkeypatch.setattr(
            otel_mod,
            attr,
            meter.create_observable_gauge(
                name=name,
                unit="1",
                callbacks=[getattr(otel_mod, _CALLBACKS[name])],
            ),
        )
    return reader


def _points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == name:
            return [p for p in metric.data.data_points if isinstance(p, NumberDataPoint)]
    return []


class _DeadConn:
    """A leader-monitor connection whose liveness probe always fails."""

    def is_closed(self) -> bool:
        return False

    async def fetchval(self, *_args: object, **_kwargs: object) -> object:
        raise ConnectionResetError("leader monitor connection died")

    async def close(self) -> None:
        return None


class _ObservableLeaderFlag(asyncio.Event):
    """``is_leader`` that also signals the instant leadership is dropped.

    Lets the test await the real demotion transition instead of polling.
    """

    def __init__(self) -> None:
        super().__init__()
        self.demoted: asyncio.Event = asyncio.Event()

    def clear(self) -> None:
        super().clear()
        self.demoted.set()


def _leader(is_leader: asyncio.Event) -> MaintenanceLeader:
    deps = cast(
        WorkerDeps,
        SimpleNamespace(
            liveness=LoopLiveness(),
            is_leader=is_leader,
            leader_conn=None,
            owns_leader_conn=False,
            settings=SimpleNamespace(schema_name="taskq", dispatcher_command_timeout=2.5),
        ),
    )
    return MaintenanceLeader(deps, uuid4(), cast(Backend, SimpleNamespace()), clock=SystemClock())


@pytest.fixture(autouse=True)
def _reset_leader_caches() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]  # Why: autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    yield None
    obs_mod.update_queue_depth_cache({})
    obs_mod.update_reservation_slots_cache({})
    obs_mod.update_stranded_jobs_cache({})


async def test_leader_only_gauges_are_absent_after_demotion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Failover: the watchdog probe fails, leadership is lost, and this
    process must stop exporting the leader-only series entirely."""
    reader = _isolated_leader_gauges(monkeypatch)

    obs_mod.update_queue_depth_cache({"default": 7})
    obs_mod.update_reservation_slots_cache({"bucket_a": 3})
    obs_mod.update_stranded_jobs_cache({"ghost_actor": 2})

    # Sanity: while leading, the values ARE exported.
    assert [p.value for p in _points(reader, "taskq.queue.depth")] == [7]
    assert [p.value for p in _points(reader, "taskq.reservation.slots_used")] == [3]
    assert [p.value for p in _points(reader, "taskq.jobs.stranded")] == [2]

    is_leader = _ObservableLeaderFlag()
    is_leader.set()
    leader = _leader(is_leader)
    leader._leader_monitor_conn = cast("object", _DeadConn())  # type: ignore[assignment]  # Why: test double for the leader-monitor conn; only fetchval/is_closed/close are exercised.

    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._watchdog_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: the watchdog loop IS the demotion path under test.
    try:
        await asyncio.wait_for(is_leader.demoted.wait(), timeout=5.0)
    finally:
        shutdown.set()
        is_leader.set()
        await asyncio.wait_for(task, timeout=5.0)

    for name, _attr in _LEADER_ONLY_GAUGES:
        assert _points(reader, name) == [], (
            f"{name} still exported after demotion: "
            f"{[(p.attributes, p.value) for p in _points(reader, name)]}"
        )
