"""Adversarial pins for the sweep telemetry restructure (metrics off the
success path).

The restructure's own invariant, stated where the emitters live
(``taskq.obs._otel``): "The emitters below are called from ``finally``
blocks and failure branches so a timing-out sweep is recorded as such."
Every awaited sweep call in the leader loops is attacked against that
invariant here:

- a deadline-aborted call must leave a duration sample, a
  ``sweep_timeouts`` increment, NO row sample and NO success stamp;
- a NON-deadline transient (connection loss) must leave a duration sample
  and NO timeout increment — the timeout counter counts aborted batches,
  not dead sockets;
- a call that COMPLETED must never be counted as timed out, even when a
  later await in the same deadline window (the wake NOTIFY) times out —
  rows plus a timeout increment for the same call is a contradiction an
  operator cannot read.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import cast

import asyncpg
import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.backend.clock import Clock, SystemClock
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker._leader_shared import SweepContext
from taskq.worker._leader_sweeps import _sweep_loop
from taskq.worker.deps import WorkerDeps
from taskq.worker.leader import MaintenanceLeader

_DURATION_METRIC = "taskq.maintenance_leader.sweep_duration_ms"
_ROWS_METRIC = "taskq.maintenance_leader.sweep_rows"
_TIMEOUTS_METRIC = "taskq.maintenance_leader.sweep_timeouts"

_PG_DSN = "postgresql://taskq:taskq@127.0.0.1:1/taskq"


@pytest.fixture
def telemetry_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Fresh SDK instruments for the sweep telemetry, isolated per test.

    Patches the module-level instruments the loops call through — the
    duration histogram and rows counter in ``_leader_shared``, plus the
    obs-level timeouts counter — with fresh InMemoryMetricReader-backed
    instruments, forces ``_otel_enabled`` on (the obs emitters are no-ops
    while it is off) and swaps in a fresh success-stamp cache so a prior
    test's stamp cannot satisfy this one's assertions. monkeypatch restores
    everything, including the real success cache.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    import taskq.obs as obs_mod
    import taskq.obs._otel as otel_mod
    import taskq.worker._leader_shared as shared_mod

    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(
        obs_mod.INSTRUMENTATION_NAME,
        otel_mod._version(),  # pyright: ignore[reportPrivateUsage]  # Why: same private-helper access as the sweep-metrics fixture in tests/test_sweep_timeout_metrics.py.
    )
    monkeypatch.setattr(
        shared_mod, "_sweep_duration_hist", meter.create_histogram(_DURATION_METRIC, unit="ms")
    )
    monkeypatch.setattr(
        shared_mod, "_sweep_rows_counter", meter.create_counter(_ROWS_METRIC, unit="1")
    )
    monkeypatch.setattr(
        otel_mod, "_sweep_timeouts", meter.create_counter(_TIMEOUTS_METRIC, unit="1")
    )
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    monkeypatch.setattr(otel_mod, "_sweep_success_cache", {})  # pyright: ignore[reportPrivateUsage]  # Why: fresh cache so this test's stamp assertions are its own; monkeypatch restores the module's dict.
    return reader


def _duration_samples(reader: InMemoryMetricReader, sweep: str) -> int:
    """Sum of histogram sample COUNTS recorded for sweep_name == *sweep*."""
    from taskq.testing.otel import histogram_points

    total = 0
    for point in histogram_points(reader, _DURATION_METRIC):
        if point.attributes == {"sweep_name": sweep}:
            total += int(point.count)
    return total


def _rows_value(reader: InMemoryMetricReader, sweep: str) -> int:
    """Summed row-counter value for sweep_name == *sweep* (0 when absent)."""
    from taskq.testing.otel import counter_data_points

    return sum(
        int(dp.value)
        for dp in counter_data_points(reader, _ROWS_METRIC)
        if dp.attributes == {"sweep_name": sweep}
    )


def _timeout_value(reader: InMemoryMetricReader, sweep: str) -> int:
    from taskq.testing.otel import counter_data_points

    return sum(
        int(dp.value)
        for dp in counter_data_points(reader, _TIMEOUTS_METRIC)
        if dp.attributes == {"sweep_name": sweep}
    )


# ── Test doubles ─────────────────────────────────────────────────────────


class _ConnStub:
    """asyncpg.Connection stand-in; fetchval/execute are scriptable."""

    def __init__(
        self,
        *,
        fetchval: object | None = 0,
        fetchval_exc: BaseException | None = None,
        execute_result: str = "DELETE 0",
        execute_exc: BaseException | None = None,
    ) -> None:
        self._fetchval = fetchval
        self._fetchval_exc = fetchval_exc
        self._execute_result = execute_result
        self._execute_exc = execute_exc
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        if self._fetchval_exc is not None:
            raise self._fetchval_exc
        return self._fetchval

    async def execute(self, sql: str, *args: object) -> str:
        if self._execute_exc is not None:
            raise self._execute_exc
        return self._execute_result

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        return []

    def is_closed(self) -> bool:
        return False


class _PoolStub:
    """Pool stand-in; acquire either yields a conn or raises."""

    def __init__(
        self, conn: _ConnStub | None = None, *, acquire_exc: BaseException | None = None
    ) -> None:
        self._conn = conn
        self._acquire_exc = acquire_exc

    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_ConnStub, None]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire's keyword-only timeout.
        if self._acquire_exc is not None:
            raise self._acquire_exc
        yield self._conn if self._conn is not None else _ConnStub()


class _ScriptedBackend:
    """Backend stand-in whose sweeps return or raise per script.

    ``reclaim``/``deadline``/``results``/``leaked`` map to the four sweep
    methods the leader sweep loop calls; each entry is either an int to
    return or an exception instance to raise. The wake loop uses
    ``scheduled_to_pending``.
    """

    def __init__(
        self,
        *,
        reclaim: int | BaseException = 0,
        deadline: int | BaseException = 0,
        results: int | BaseException = 0,
        leaked: int | BaseException = 0,
        scheduled_to_pending: int | BaseException = 0,
        has_pg_sweeps: bool = True,
    ) -> None:
        self._script = {
            "reclaim": reclaim,
            "deadline": deadline,
            "results": results,
            "leaked": leaked,
            "scheduled_to_pending": scheduled_to_pending,
        }
        self._has_pg_sweeps = has_pg_sweeps
        self.calls: dict[str, int] = {}

    def _outcome(self, key: str) -> int:
        self.calls[key] = self.calls.get(key, 0) + 1
        value = self._script[key]
        if isinstance(value, BaseException):
            raise value
        return value

    async def reclaim_expired_locks(self, cg: object, ug: object) -> int:
        return self._outcome("reclaim")

    async def deadline_sweep(self) -> int:
        return self._outcome("deadline")

    async def scheduled_to_pending(self) -> int:
        return self._outcome("scheduled_to_pending")

    # The hasattr gate keys off this method's presence.
    async def sweep_leaked_reservation_slots(self, conn: object, *, schema: str) -> int:
        return self._outcome("leaked")

    async def sweep_expired_results(
        self, conn: object, *, schema: str, batch_size: int = 100
    ) -> int:
        return self._outcome("results")


def _settings(**env_overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": _PG_DSN}
    data.update(env_overrides)
    return WorkerSettings.load_from_dict(data, validate=False)


def _deps(
    *,
    dispatcher_pool: object,
    is_leader: bool,
    settings: WorkerSettings | None = None,
) -> WorkerDeps:
    s = settings if settings is not None else _settings()
    deps = WorkerDeps(
        settings=s,
        dispatcher_pool=dispatcher_pool,  # pyright: ignore[reportArgumentType]  # Why: pool stand-in satisfying the acquire() surface the loops use; same seam as the leader-sweeps coverage tests.
        heartbeat_pool=dispatcher_pool,  # pyright: ignore[reportArgumentType]
        worker_pool=dispatcher_pool,  # pyright: ignore[reportArgumentType]
        notify_conn=None,
        leader_conn=_ConnStub(),  # pyright: ignore[reportArgumentType]
    )
    if is_leader:
        deps.is_leader.set()
    return deps


def _sweep_ctx(
    backend: _ScriptedBackend,
    *,
    dispatcher_pool: object,
    is_leader: bool = True,
    settings: WorkerSettings | None = None,
) -> SweepContext:
    return SweepContext(
        deps=_deps(dispatcher_pool=dispatcher_pool, is_leader=is_leader, settings=settings),
        backend=cast("Backend", backend),
        clock=cast("Clock", FakeClock(datetime(2025, 1, 1, tzinfo=UTC))),
        worker_id=new_uuid(),
    )


def _wake_leader(
    backend: _ScriptedBackend,
    *,
    dispatcher_pool: object,
) -> MaintenanceLeader:
    """A MaintenanceLeader carrying what ``_scheduled_wake_loop`` touches."""
    deps = _deps(dispatcher_pool=dispatcher_pool, is_leader=True)
    return MaintenanceLeader(
        deps,
        new_uuid(),
        cast("Backend", backend),
        clock=SystemClock(),
    )


async def _stop(task: asyncio.Task[object], shutdown: asyncio.Event) -> None:
    """Stop a driven loop task: set shutdown, cancel, reap."""
    shutdown.set()
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


# ── The wake NOTIFY after a completed sweep must not count as a sweep
# timeout ────────────────────────────────────────────────────────────────


async def test_notify_timeout_after_completed_sweep_is_not_a_sweep_timeout(
    telemetry_reader: InMemoryMetricReader,
) -> None:
    """The sweep call returned 3 rows, then the wake-NOTIFY pool acquire
    timed out: the sweep was NOT aborted, so ``sweep_timeouts`` must stay
    0 — rows and the success stamp are recorded, the deadline casualty is
    the NOTIFY, and counting the same call as both completed (rows sample)
    and aborted (timeout increment) is a contradiction the
    TaskQSweepTimeouts alert would page on."""
    backend = _ScriptedBackend(scheduled_to_pending=3)
    # Pool exhaustion shape: acquire raises TimeoutError, as asyncpg does.
    leader = _wake_leader(backend, dispatcher_pool=_PoolStub(acquire_exc=TimeoutError()))

    rows_recorded = asyncio.Event()
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    try:
        for _ in range(400):
            if (
                backend.calls.get("scheduled_to_pending")
                and _rows_value(telemetry_reader, "scheduled_to_pending") == 3
            ):
                rows_recorded.set()
                break
            await asyncio.sleep(0.01)
        assert rows_recorded.is_set(), "the wake loop must attempt and record the sweep"
    finally:
        await _stop(task, shutdown)

    import taskq.obs._otel as otel_mod

    assert _rows_value(telemetry_reader, "scheduled_to_pending") == 3
    assert otel_mod._sweep_success_cache.get("scheduled_to_pending") is not None  # pyright: ignore[reportPrivateUsage]  # Why: the stamp is the success gauge's input; the fixture swapped in the cache being read.
    assert _duration_samples(telemetry_reader, "scheduled_to_pending") >= 1
    assert _timeout_value(telemetry_reader, "scheduled_to_pending") == 0, (
        "the sweep call completed (rows sample recorded) — a NOTIFY-path timeout "
        "must not increment sweep_timeouts for it"
    )


# ── Non-deadline transients: duration yes, rows no, timeout counter no ──


async def test_wake_loop_connection_loss_records_duration_not_timeout(
    telemetry_reader: InMemoryMetricReader,
) -> None:
    """A connection-loss transient (not the deadline family) in the wake
    sweep must still leave a duration sample — but NO row sample and NO
    ``sweep_timeouts`` increment: the timeout counter counts deadline-
    aborted batches, and a dead socket is a different failure family."""
    backend = _ScriptedBackend(
        scheduled_to_pending=asyncpg.ConnectionDoesNotExistError("connection closed")
    )
    leader = _wake_leader(backend, dispatcher_pool=_PoolStub())

    duration_seen = asyncio.Event()
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._scheduled_wake_loop(shutdown))
    try:
        for _ in range(400):
            if _duration_samples(telemetry_reader, "scheduled_to_pending") >= 1:
                duration_seen.set()
                break
            await asyncio.sleep(0.01)
        assert duration_seen.is_set(), "a failed sweep recorded no duration sample"
    finally:
        await _stop(task, shutdown)

    assert _timeout_value(telemetry_reader, "scheduled_to_pending") == 0, (
        "connection loss is not a deadline abort — the timeout counter must not count it"
    )
    assert _rows_value(telemetry_reader, "scheduled_to_pending") == 0


async def test_sweep_loop_reclaim_timeout_records_duration_and_timeout_without_rows(
    telemetry_reader: InMemoryMetricReader,
) -> None:
    """The sweep-1 call aborted by a server-side cancel: duration sample,
    ``sweep_timeouts`` increment for ``expired_locks``, NO row sample —
    the finally-path discipline at the sweep loop's own call sites."""
    backend = _ScriptedBackend(
        reclaim=asyncpg.QueryCanceledError("canceling statement due to statement timeout")
    )
    settings = _settings()
    settings.sweep_interval = 0.05  # bypasses the ge=1.0 field constraint by hand
    ctx = _sweep_ctx(backend, dispatcher_pool=_PoolStub(), settings=settings)

    duration_seen = asyncio.Event()
    shutdown = asyncio.Event()
    task = asyncio.create_task(_sweep_loop(ctx, shutdown))
    try:
        for _ in range(400):
            if _duration_samples(telemetry_reader, "expired_locks") >= 1:
                duration_seen.set()
                break
            await asyncio.sleep(0.01)
        assert duration_seen.is_set(), "an aborted sweep-1 call recorded no duration sample"
    finally:
        await _stop(task, shutdown)

    assert _timeout_value(telemetry_reader, "expired_locks") == 1
    assert _rows_value(telemetry_reader, "expired_locks") == 0


async def test_sweep_loop_deadline_timeout_records_duration_and_timeout_without_rows(
    telemetry_reader: InMemoryMetricReader,
) -> None:
    """The deadline sweep aborted by a server-side cancel: a duration
    sample and a ``sweep_timeouts`` increment for ``deadline_exceeded``,
    NO row sample — the same finally-path discipline as sweep 1, one call
    site down."""
    backend = _ScriptedBackend(
        deadline=asyncpg.QueryCanceledError("canceling statement due to statement timeout")
    )
    settings = _settings()
    settings.sweep_interval = 0.05  # bypasses the ge=1.0 field constraint by hand
    ctx = _sweep_ctx(backend, dispatcher_pool=_PoolStub(), settings=settings)

    timeout_seen = asyncio.Event()
    shutdown = asyncio.Event()
    task = asyncio.create_task(_sweep_loop(ctx, shutdown))
    try:
        for _ in range(400):
            if _timeout_value(telemetry_reader, "deadline_exceeded") >= 1:
                timeout_seen.set()
                break
            await asyncio.sleep(0.01)
        assert timeout_seen.is_set(), (
            "the deadline sweep timed out without recording a timeout — the "
            "failure path is invisible at this call site"
        )
    finally:
        await _stop(task, shutdown)

    assert _duration_samples(telemetry_reader, "deadline_exceeded") >= 1
    assert _rows_value(telemetry_reader, "deadline_exceeded") == 0


async def test_sweep_loop_expired_results_timeout_records_duration_and_timeout_without_rows(
    telemetry_reader: InMemoryMetricReader,
) -> None:
    """The result-TTL sweep aborted by a server-side cancel: a duration
    sample and a ``sweep_timeouts`` increment for ``expired_results``, NO
    row sample.  This call site runs on a dispatcher-pool connection, so
    the abort propagates through the acquire block — the wiring must still
    record it."""
    backend = _ScriptedBackend(
        results=asyncpg.QueryCanceledError("canceling statement due to statement timeout")
    )
    settings = _settings()
    settings.sweep_interval = 0.05  # bypasses the ge=1.0 field constraint by hand
    ctx = _sweep_ctx(backend, dispatcher_pool=_PoolStub(), settings=settings)

    timeout_seen = asyncio.Event()
    shutdown = asyncio.Event()
    task = asyncio.create_task(_sweep_loop(ctx, shutdown))
    try:
        for _ in range(400):
            if _timeout_value(telemetry_reader, "expired_results") >= 1:
                timeout_seen.set()
                break
            await asyncio.sleep(0.01)
        assert timeout_seen.is_set(), (
            "the result-TTL sweep timed out without recording a timeout — the "
            "failure path is invisible at this call site"
        )
    finally:
        await _stop(task, shutdown)

    assert _duration_samples(telemetry_reader, "expired_results") >= 1
    assert _rows_value(telemetry_reader, "expired_results") == 0


async def test_sweep_loop_stale_workers_timeout_records_duration_and_timeout_without_rows(
    telemetry_reader: InMemoryMetricReader,
) -> None:
    """The stale-worker cleanup aborted mid-statement by a server-side
    cancel (the ``execute`` on the pooled conn raises): a duration sample
    and a ``sweep_timeouts`` increment for ``stale_workers``, NO row
    sample.  This site calls ``cleanup_stale_workers`` directly rather
    than a backend method, so the recording happens in the loop's own
    except/finally — the discipline this file exists to pin."""
    backend = _ScriptedBackend()
    conn = _ConnStub(
        execute_exc=asyncpg.QueryCanceledError("canceling statement due to statement timeout")
    )
    settings = _settings()
    settings.sweep_interval = 0.05  # bypasses the ge=1.0 field constraint by hand
    ctx = _sweep_ctx(backend, dispatcher_pool=_PoolStub(conn=conn), settings=settings)

    timeout_seen = asyncio.Event()
    shutdown = asyncio.Event()
    task = asyncio.create_task(_sweep_loop(ctx, shutdown))
    try:
        for _ in range(400):
            if _timeout_value(telemetry_reader, "stale_workers") >= 1:
                timeout_seen.set()
                break
            await asyncio.sleep(0.01)
        assert timeout_seen.is_set(), (
            "the stale-worker cleanup timed out without recording a timeout — "
            "the failure path is invisible at this call site"
        )
    finally:
        await _stop(task, shutdown)

    assert _duration_samples(telemetry_reader, "stale_workers") >= 1
    assert _rows_value(telemetry_reader, "stale_workers") == 0


# ── The leaked-slots call site ───────────────────────────────────────────


async def test_leaked_slots_timeout_records_duration_and_timeout_without_rows(
    telemetry_reader: InMemoryMetricReader,
) -> None:
    """A server-side cancel of the leaked-slots sweep must be recorded as
    an aborted call: a duration sample and a ``sweep_timeouts`` increment
    for ``leaked_slots`` — NOT silence. Success-path-only instrumentation
    here would be the original invisibility, one call site down."""
    backend = _ScriptedBackend(leaked=asyncpg.QueryCanceledError("canceling statement"))
    settings = _settings()
    settings.sweep_interval = 0.05  # bypasses the ge=1.0 field constraint by hand
    ctx = _sweep_ctx(backend, dispatcher_pool=_PoolStub(), settings=settings)

    duration_seen = asyncio.Event()
    shutdown = asyncio.Event()
    task = asyncio.create_task(_sweep_loop(ctx, shutdown))
    try:
        for _ in range(400):
            if _timeout_value(telemetry_reader, "leaked_slots") >= 1:
                duration_seen.set()
                break
            await asyncio.sleep(0.01)
        assert duration_seen.is_set(), (
            "the leaked-slots sweep timed out without recording a timeout — the "
            "failure path is invisible at this call site"
        )
    finally:
        await _stop(task, shutdown)

    assert _duration_samples(telemetry_reader, "leaked_slots") >= 1
    assert _rows_value(telemetry_reader, "leaked_slots") == 0


# ── The stale-batches call site ──────────────────────────────────────────


async def test_stale_batches_timeout_records_duration_and_timeout_without_rows(
    telemetry_reader: InMemoryMetricReader,
) -> None:
    """A deadline abort of the stale-batches sweep must leave a duration
    sample and a ``sweep_timeouts`` increment for ``stale_batches`` — not
    nothing. The bounded/batched rewrite made this sweep's calls abortable
    by the same deadlines as its siblings; its telemetry must follow."""
    backend = _ScriptedBackend()
    conn = _ConnStub(fetchval_exc=TimeoutError())
    settings = _settings()
    settings.sweep_interval = 0.05  # bypasses the ge=1.0 field constraint by hand
    ctx = _sweep_ctx(backend, dispatcher_pool=_PoolStub(conn=conn), settings=settings)

    timeout_seen = asyncio.Event()
    shutdown = asyncio.Event()
    task = asyncio.create_task(_sweep_loop(ctx, shutdown))
    try:
        for _ in range(400):
            if _timeout_value(telemetry_reader, "stale_batches") >= 1:
                timeout_seen.set()
                break
            await asyncio.sleep(0.01)
        assert timeout_seen.is_set(), (
            "the stale-batches sweep timed out without recording a timeout — the "
            "failure path is invisible at this call site"
        )
    finally:
        await _stop(task, shutdown)

    assert _duration_samples(telemetry_reader, "stale_batches") >= 1
    assert _rows_value(telemetry_reader, "stale_batches") == 0


async def test_stale_batches_server_cancel_is_transient_not_a_bug(
    telemetry_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A server-side statement cancel (the SQLSTATE-57014 shape a DBA or a
    server ``statement_timeout`` produces) during the stale-batches sweep
    is TRANSIENT: the loop retries the sweep on its next tick and never
    escalates to the unexpected-error backstop, which tolerates a short
    streak and then deliberately kills the worker.

    Asserted on observable behaviour, not log text: a bug classification
    increments ``leader_loop_unexpected_errors_total`` and, at the streak
    cap, re-raises out of the loop task — so a cancel that is classified
    transiently leaves that counter at zero, keeps the sweep being
    re-attempted well past the cap, and leaves the loop task alive.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    import taskq.obs as obs_mod
    import taskq.obs._otel as otel_mod
    import taskq.worker._transient as transient_mod
    from taskq.testing.otel import counter_value

    # Fresh counter for the backstop's emitted signal, so a prior test's
    # increments cannot satisfy this one's zero-assertion (and vice versa).
    unexpected_reader = InMemoryMetricReader()
    unexpected_meter = MeterProvider(metric_readers=[unexpected_reader]).get_meter(
        obs_mod.INSTRUMENTATION_NAME,
        otel_mod._version(),  # pyright: ignore[reportPrivateUsage]  # Why: same private-helper access as the telemetry fixture above.
    )
    monkeypatch.setattr(
        transient_mod,
        "_unexpected_loop_errors",
        unexpected_meter.create_counter(
            "taskq.worker.leader_loop_unexpected_errors_total", unit="1"
        ),
    )

    backend = _ScriptedBackend()
    conn = _ConnStub(fetchval_exc=asyncpg.QueryCanceledError("canceling statement"))
    settings = _settings()
    settings.sweep_interval = 0.05  # bypasses the ge=1.0 field constraint by hand
    ctx = _sweep_ctx(backend, dispatcher_pool=_PoolStub(conn=conn), settings=settings)

    # More consecutive cancel-aborted iterations than the backstop's
    # streak cap: at the cap, an unexpected classification re-raises and
    # the task dies; a transient one keeps retrying.
    required_attempts = transient_mod.DEFAULT_MAX_CONSECUTIVE_UNEXPECTED + 3

    shutdown = asyncio.Event()
    task = asyncio.create_task(_sweep_loop(ctx, shutdown))
    try:
        for _ in range(600):
            attempts = sum(1 for sql, _ in conn.fetchval_calls if "batches" in sql)
            if attempts >= required_attempts or task.done():
                break
            await asyncio.sleep(0.01)

        stale_batches_attempts = sum(1 for sql, _ in conn.fetchval_calls if "batches" in sql)
        assert stale_batches_attempts >= required_attempts, (
            f"the stale-batches sweep was attempted only {stale_batches_attempts} times — "
            "a transient classification retries every iteration"
        )
        assert not task.done(), (
            "the loop task died under repeated server-side cancels — a transient "
            "abort must never escalate to the backstop's deliberate kill"
        )
        assert (
            counter_value(unexpected_reader, "taskq.worker.leader_loop_unexpected_errors_total")
            == 0
        ), (
            "a server-side cancel of the stale-batches sweep hit the bug backstop — "
            "it is a transient abort and must take the sweep's warning path"
        )
    finally:
        await _stop(task, shutdown)

    assert _timeout_value(telemetry_reader, "stale_batches") >= 1, (
        "a deadline-family abort of the stale-batches sweep must increment sweep_timeouts"
    )
    assert _rows_value(telemetry_reader, "stale_batches") == 0


# ── The cron loop call site ──────────────────────────────────────────────


class _CronConnStub:
    """Cron conn stand-in: an always-open transaction, nothing else."""

    def transaction(self) -> object:
        class _Tx:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *a: object) -> bool:
                return False

        return _Tx()

    def is_closed(self) -> bool:
        return False

    async def close(self) -> None:
        pass


def _cron_leader() -> MaintenanceLeader:
    """A MaintenanceLeader carrying what ``_cron_loop`` touches, with a
    real WorkerSettings so the loop's deadline and tick cap are the
    production defaults."""
    deps = _deps(dispatcher_pool=_PoolStub(), is_leader=True)
    leader = MaintenanceLeader(
        deps,
        new_uuid(),
        cast("Backend", _ScriptedBackend()),
        clock=SystemClock(),
    )
    leader._cron_conn = _CronConnStub()  # type: ignore[reportAttributeAccessIssue]  # Why: the cron conn is assigned post-construction by the leader's own conn-management code; the test injects the stub through the same attribute.
    return leader


async def test_cron_tick_timeout_records_duration_and_timeout_without_rows(
    telemetry_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cron tick cut by its iteration deadline must record a duration
    sample and a ``sweep_timeouts`` increment for ``cron`` — and NO row
    sample (``fired`` stayed unbound, so a 0-row sample would be
    indistinguishable from a healthy tick with nothing due).  Pre-fix the
    cron loop emitted no sweep metric at all; this is the pin that the
    deadline path now reports like every other leader loop."""

    async def _timeout_tick(*a: object, **k: object) -> None:
        raise TimeoutError("iteration deadline")

    monkeypatch.setattr("taskq.worker.leader.tick_cron", _timeout_tick)
    leader = _cron_leader()

    timeout_seen = asyncio.Event()
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._cron_loop(shutdown))
    try:
        for _ in range(400):
            if _timeout_value(telemetry_reader, "cron") >= 1:
                timeout_seen.set()
                break
            await asyncio.sleep(0.01)
        assert timeout_seen.is_set(), (
            "a deadline-aborted cron tick recorded no timeout — the cron "
            "failure path is invisible again"
        )
    finally:
        await _stop(task, shutdown)

    assert _duration_samples(telemetry_reader, "cron") >= 1
    assert _rows_value(telemetry_reader, "cron") == 0, (
        "a timed-out tick must not record a row sample — nothing was fired"
    )


async def test_cron_tick_success_records_rows_and_duration_without_timeout(
    telemetry_reader: InMemoryMetricReader,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A healthy tick firing 2 schedules records ``sweep_rows{cron} == 2``,
    a duration sample, and a success stamp — and NO timeout increment."""

    async def _two_fire_tick(*a: object, **k: object) -> int:
        return 2

    monkeypatch.setattr("taskq.worker.leader.tick_cron", _two_fire_tick)
    leader = _cron_leader()

    rows_seen = asyncio.Event()
    shutdown = asyncio.Event()
    task = asyncio.create_task(leader._cron_loop(shutdown))
    try:
        for _ in range(400):
            if _rows_value(telemetry_reader, "cron") == 2:
                rows_seen.set()
                break
            await asyncio.sleep(0.01)
        assert rows_seen.is_set(), "a healthy cron tick recorded no row sample"
    finally:
        await _stop(task, shutdown)

    assert _rows_value(telemetry_reader, "cron") == 2
    assert _duration_samples(telemetry_reader, "cron") >= 1
    assert _timeout_value(telemetry_reader, "cron") == 0

    import taskq.obs._otel as otel_mod

    assert otel_mod._sweep_success_cache.get("cron") is not None  # pyright: ignore[reportPrivateUsage]  # Why: the stamp is the success gauge's input; the fixture swapped in the cache being read.,
