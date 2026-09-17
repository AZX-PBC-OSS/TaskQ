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
    from taskq.worker.leader import MaintenanceLeader
    from tests._leader_stub_deps import stub_deps

    is_leader = asyncio.Event()
    is_leader.set()
    deps = stub_deps(
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


# ── the per-actor backlog read's OWN failure arms (#249) ──────────────
#
# Unlike the loops above, the per-actor backlog read is ISOLATED inside the
# backlog tick: its failure must not cost the tick the fleet-wide samples,
# so it can never reach the tick-level handler that counts under
# "backlog_detection". Its two failure arms (the fetch raising, and rows
# coming back with a shape the cache rebuild cannot read) used to log a
# bare WARN and move on, and the fetch arm's empty fallback CLEARS the
# per-actor series, which resolves TaskQQueueDepthHigh (its operand is the
# oldest-pending age) at the exact moment the incident it alerts on is
# killing the read, with no metric naming the loss. Both arms must count on
# the sweep-timeouts counter under the read's own sweep_name, which is what
# keeps TaskQSweepTimeouts firing through the outage.


class _BacklogTickConn:
    """Connection stand-in for the backlog tick: fleet-wide reads answer,
    and the per-actor backlog read does whatever the test needs.

    Dispatches on the statement's own shape the way the real tick issues
    them: the by-status UNION, the two fleet fetchvals, the per-actor
    running GROUP BY, and the per-actor backlog GROUP BY (the pair walk,
    the widest-shaped statement in the tick, and the first to hit the
    statement timeout under the incident load it exists to expose).
    """

    def __init__(
        self,
        *,
        actor_exc: Exception | None = None,
        actor_rows: list[dict[str, object]] | None = None,
    ) -> None:
        self._actor_exc = actor_exc
        self._actor_rows = actor_rows
        self.actor_backlog_calls = 0
        self.fleet_calls = 0

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        norm = " ".join(sql.lower().split())
        if "group by actor, queue" in norm:
            self.actor_backlog_calls += 1
            if self._actor_exc is not None:
                raise self._actor_exc
            assert self._actor_rows is not None  # Why: an answering conn is built with rows.
            return self._actor_rows
        self.fleet_calls += 1
        if "group by actor" in norm:  # running-by-actor read
            return [{"actor": "resize_image", "running": 2, "oldest_age": 41.5}]
        return [{"status": "scheduled", "count": 12}, {"status": "pending", "count": 3}]

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fleet_calls += 1
        if "lock_expires_at" in " ".join(sql.lower().split()):
            return 87
        return 87.5

    async def execute(self, sql: str, *args: object) -> str:
        return "OK"

    def is_closed(self) -> bool:
        return False


def _spy_backlog_cache_updates(monkeypatch: pytest.MonkeyPatch) -> dict[str, list[object]]:
    """Record every gauge-cache update the backlog tick makes, per cache.

    Spies the sweeps module's own imported update names (the established
    instrumentation seam) so the tick under test never writes the process
    singleton caches: the neighboring sampler tests here fail every read,
    but this tick's fleet reads SUCCEED, and a test that leaves
    ``_jobs_by_status_cache`` populated leaks its numbers into whichever
    test runs next.
    """
    from taskq.worker import _leader_sweeps

    calls: dict[str, list[object]] = {
        "by_status": [],
        "scheduled_count": [],
        "oldest_due_age": [],
        "running_lease_expired": [],
        "jobs_running": [],
        "actor_oldest_running_age": [],
        "actor_backlog": [],
        "actor_oldest_pending_age": [],
    }
    seams = {
        "update_jobs_by_status_cache": "by_status",
        "update_scheduled_count_cache": "scheduled_count",
        "update_oldest_due_age_cache": "oldest_due_age",
        "update_running_lease_expired_cache": "running_lease_expired",
        "update_jobs_running_cache": "jobs_running",
        "update_actor_oldest_running_age_cache": "actor_oldest_running_age",
        "update_actor_backlog_cache": "actor_backlog",
        "update_actor_oldest_pending_age_cache": "actor_oldest_pending_age",
    }
    for attr, key in seams.items():
        monkeypatch.setattr(_leader_sweeps, attr, calls[key].append)
    return calls


async def _drive_backlog_tick_until_actor_read_settles(
    conn: _BacklogTickConn,
) -> None:
    """Run the backlog loop until the per-actor read has failed at least
    twice (a settled failure across ticks, not a race with the first one),
    then stop it."""
    from taskq.worker import _leader_sweeps

    ctx = _sampler_ctx(_ConnPool(conn))
    shutdown = asyncio.Event()
    task = asyncio.create_task(_leader_sweeps._backlog_detection_loop(ctx, shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: same private-loop seam the parametrized sampler tests above drive.
    try:
        await wait_for_condition(
            lambda: conn.actor_backlog_calls >= 2,
            description="the backlog tick must attempt its per-actor read at least twice",
        )
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_actor_backlog_read_failure_counts_and_clears_only_its_own_gauges(
    sweep_metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """THE #249 pin: the per-actor backlog read fails while every fleet
    read in the same tick succeeds.

    What the failure must produce, all at once: the per-actor series go
    ABSENT (the empty fallback: the honest state, never a stale claim),
    the fleet-wide samples still land (the isolation the read exists
    under), and the failure increments
    taskq.maintenance_leader.sweep_timeouts under the actor_backlog
    sweep_name, the counter TaskQSweepTimeouts reads. Without that
    increment, a metrics-only operator watched TaskQQueueDepthHigh
    resolve itself exactly when the incident it alerts on killed the read,
    with nothing but an unalertable WARN log naming the loss.
    """
    conn = _BacklogTickConn(
        actor_exc=asyncpg.exceptions.QueryCanceledError(
            "canceling statement due to statement timeout"
        )
    )
    calls = _spy_backlog_cache_updates(monkeypatch)
    await _drive_backlog_tick_until_actor_read_settles(conn)

    assert conn.actor_backlog_calls >= 2, "setup: the per-actor read must have failed twice"
    assert _timeout_value(sweep_metric_reader, "actor_backlog") >= 1, (
        "the per-actor backlog read failed every tick and no metric named it — "
        "the cleared oldest-pending-age series resolves TaskQQueueDepthHigh "
        "under the exact incident that alert exists for, and a WARN log line "
        "is not a signal an alert rule can read"
    )
    assert _timeout_value(sweep_metric_reader, "backlog_detection") == 0, (
        "the per-actor failure must stay contained by its own isolation — "
        "the fleet-wide handler firing means the raise escaped and the tick "
        "lost its other samples"
    )
    # The honest state: both per-actor caches rebuilt from the empty
    # snapshot every failing tick, so the series go absent rather than
    # freeze at readings the worker can no longer see.
    assert calls["actor_backlog"] and all(u == {} for u in calls["actor_backlog"]), (
        f"expected only empty per-actor depth updates; got {calls['actor_backlog']!r}"
    )
    assert calls["actor_oldest_pending_age"] and all(
        u == {} for u in calls["actor_oldest_pending_age"]
    ), f"expected only empty per-actor age updates; got {calls['actor_oldest_pending_age']!r}"
    # The isolation: the fleet-wide samples of the same tick still land.
    assert calls["by_status"] and calls["by_status"][-1] == {"scheduled": 12, "pending": 3}, (
        f"a failed per-actor read cost the tick its jobs-by-status sample: {calls!r}"
    )
    assert calls["scheduled_count"] and calls["scheduled_count"][-1] == 12
    assert calls["oldest_due_age"] and calls["oldest_due_age"][-1] == 87.5
    assert calls["running_lease_expired"] and calls["running_lease_expired"][-1] == 87


@pytest.mark.parametrize(
    ("actor_rows", "shape_id"),
    [
        # Both keys missing: the first comprehension (depth) raises.
        ([{"actor": "emails", "queue": "default"}], "both-keys-missing"),
        # The half-valid row: depth reads fine, oldest_age is malformed.
        # As call arguments the comprehensions ran update #1 (a fresh depth
        # cache) and then raised in comprehension #2, landing the fresh
        # depth beside a FROZEN age cache, the alert's operand, a mixed
        # state the fix-round review flagged; built as locals first, the
        # failure is atomic (neither cache is written).
        (
            [{"actor": "emails", "queue": "default", "depth": 7}],
            "half-valid-row",
        ),
    ],
)
async def test_actor_backlog_malformed_row_counts_atomically_on_the_metric_plane(
    sweep_metric_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
    actor_rows: list[dict[str, object]],
    shape_id: str,
) -> None:
    """The read's OTHER failure arm: rows came back, but their shape is not
    what the cache rebuild reads (a KeyError inside the rebuild).

    The rebuild never runs -- BOTH caches stay at their last values
    together, never a fresh one beside a frozen one (the frozen half would
    be exactly the TaskQQueueDepthHigh operand) -- and that arm must reach
    the metric plane too, because a gauge frozen at a stale reading is no
    more alertable than an absent one: the operator still needs something
    that names the loss while the alert's operand sits at whatever it last
    read. Red for the half-valid shape pre-fix (the depth update ran
    before the raise).
    """
    conn = _BacklogTickConn(actor_rows=actor_rows)
    calls = _spy_backlog_cache_updates(monkeypatch)
    await _drive_backlog_tick_until_actor_read_settles(conn)

    assert conn.actor_backlog_calls >= 2, "setup: the per-actor read must have returned twice"
    assert _timeout_value(sweep_metric_reader, "actor_backlog") >= 1, (
        f"a malformed per-actor row ({shape_id}) failed the rebuild every tick "
        "and no metric named it — the per-actor gauges froze at their last "
        "values with nothing an alert rule can read"
    )
    assert _timeout_value(sweep_metric_reader, "backlog_detection") == 0
    # The atomic-failure shape itself: the rebuild raised before EITHER
    # update ran, so no per-actor update fires while the fleet samples of
    # the same ticks keep landing: a mixed fresh-depth/frozen-age state
    # can never occur.
    assert calls["actor_backlog"] == [], (
        f"{shape_id}: the depth cache must not be written when the age "
        f"snapshot cannot be built; got {calls['actor_backlog']!r}"
    )
    assert calls["actor_oldest_pending_age"] == [], (
        f"{shape_id}: got {calls['actor_oldest_pending_age']!r}"
    )
    assert calls["by_status"] and calls["by_status"][-1] == {"scheduled": 12, "pending": 3}
