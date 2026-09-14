"""Adversarial pins for ``_drain_bounded`` (the per-tick bounded drain).

The drain's contract, per its docstring and the bounded-sweep design: up
to ``sweep_drain_batches - 1`` further committed batches after the parent
call, the bound READ FROM SETTINGS (not hardcoded), liveness ticked
between batches so the watchdog cannot age the loop out mid-drain,
cooperative shutdown between batches, and per-call telemetry — a mid-drain
abort keeps the already-committed batches' row samples and records the
failed call as a timeout, NOT as zero rows.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import cast

import asyncpg
import pytest
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.backend.clock import Clock
from taskq.settings import WorkerSettings
from taskq.worker._leader_shared import SweepContext
from taskq.worker._leader_sweeps import _drain_bounded
from taskq.worker.deps import WorkerDeps

_DURATION_METRIC = "taskq.maintenance_leader.sweep_duration_ms"
_ROWS_METRIC = "taskq.maintenance_leader.sweep_rows"
_TIMEOUTS_METRIC = "taskq.maintenance_leader.sweep_timeouts"

_PG_DSN = "postgresql://taskq:taskq@127.0.0.1:1/taskq"


class _LivenessRecorder:
    """Records every liveness tick (name, period) in call order."""

    def __init__(self) -> None:
        self.ticks: list[tuple[str, float]] = []

    def tick(self, name: str, *, period: float = 0.0) -> None:
        self.ticks.append((name, period))

    def forget(
        self, name: str
    ) -> None:  # pragma: no cover  # Why: drain never forgets; present to satisfy the deps surface.
        pass


class _ScriptedCall:
    """The drain's callable, scripted per invocation.

    Each entry is either an int to return or an exception to raise; a
    callable entry receives the call index and returns the value (used to
    flip shutdown mid-drain).
    """

    def __init__(self, script: list[object]) -> None:
        self._script = script
        self.calls = 0

    async def __call__(self) -> int:
        idx = self.calls
        self.calls += 1
        entry = self._script[min(idx, len(self._script) - 1)]
        result = entry(idx) if callable(entry) else entry
        if isinstance(result, BaseException):
            raise result
        return cast("int", result)


@pytest.fixture
def telemetry_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Fresh SDK instruments for the drain's telemetry (same isolation
    rationale as the sweep-telemetry attack file's fixture)."""
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
    return reader


def _drain_ctx(
    *,
    sweep_drain_batches: int,
    sweep_interval: float = 30.0,
    liveness: _LivenessRecorder | None = None,
) -> tuple[SweepContext, _LivenessRecorder]:
    """SweepContext with REAL WorkerSettings so the drain bound is proven
    to come from the setting (a hardcoded bound would ignore the override
    and the call-count assertion would fail)."""
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": _PG_DSN,
            "TASKQ_SWEEP_DRAIN_BATCHES": str(sweep_drain_batches),
            "TASKQ_SWEEP_INTERVAL": str(sweep_interval),
        },
        validate=False,
    )
    assert settings.sweep_drain_batches == sweep_drain_batches
    recorder = liveness if liveness is not None else _LivenessRecorder()
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=None,  # pyright: ignore[reportArgumentType]  # Why: the drain never touches a pool; the parent call already holds its own connection.
        heartbeat_pool=None,  # pyright: ignore[reportArgumentType]
        worker_pool=None,  # pyright: ignore[reportArgumentType]
        notify_conn=None,
        leader_conn=None,
        liveness=recorder,  # pyright: ignore[reportArgumentType]
    )
    ctx = SweepContext(
        deps=deps,
        backend=cast("Backend", object()),
        clock=cast("Clock", object()),  # pyright: ignore[reportArgumentType]  # Why: the drain touches neither backend nor clock; only its callable runs.
        worker_id=new_uuid(),
    )
    return ctx, recorder


def _rows_value(reader: InMemoryMetricReader, sweep: str) -> int:
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


def _duration_samples(reader: InMemoryMetricReader, sweep: str) -> int:
    from taskq.testing.otel import histogram_points

    return sum(
        int(point.count)
        for point in histogram_points(reader, _DURATION_METRIC)
        if point.attributes == {"sweep_name": sweep}
    )


async def _drain(
    ctx: SweepContext,
    script: list[object],
) -> tuple[bool, _ScriptedCall]:
    """Run one drain over *script*; returns (clean, the callable)."""
    call = _ScriptedCall(script)
    shutdown = asyncio.Event()
    clean = await _drain_bounded(
        ctx,
        shutdown,
        sweep_name="expired_locks",
        call=cast("Callable[[], Awaitable[int]]", call),
        warn_event="sweep-expired-locks-failed",
        warn_kind="sweep_expired_locks_failed",
    )
    return clean, call


# ── The bound: read from settings, leaving the remainder for next tick ──


@pytest.mark.parametrize(
    ("setting", "expected_drain_calls"),
    [
        (2, 1),
        (3, 2),
        (5, 4),
    ],
)
async def test_drain_bound_comes_from_settings_and_caps_the_tick(
    setting: int, expected_drain_calls: int
) -> None:
    """A backlog larger than one tick's bound must leave the remainder for
    the next tick: exactly ``sweep_drain_batches - 1`` drain calls execute,
    the bound read from ``settings.sweep_drain_batches`` (an operator
    override changes the count; a hardcoded bound would not)."""
    ctx, _ = _drain_ctx(sweep_drain_batches=setting)
    clean, call = await _drain(ctx, [5] * 20)

    assert clean is True
    assert call.calls == expected_drain_calls, (
        f"sweep_drain_batches={setting} must mean {expected_drain_calls} drain "
        f"calls after the parent call; got {call.calls}"
    )


async def test_drain_bound_one_means_no_drain_calls() -> None:
    """``sweep_drain_batches=1`` is the "parent call only" configuration:
    the drain loop body must not run at all."""
    ctx, _ = _drain_ctx(sweep_drain_batches=1)
    clean, call = await _drain(ctx, [5] * 20)

    assert clean is True
    assert call.calls == 0


# ── Termination on an empty batch ────────────────────────────────────────


async def test_drain_stops_at_empty_batch() -> None:
    """The drain's termination condition is the sweep's own empty window:
    a full batch then an empty one stops the drain after the empty call."""
    ctx, _ = _drain_ctx(sweep_drain_batches=8)
    clean, call = await _drain(ctx, [5, 0])

    assert clean is True
    assert call.calls == 2, "5-then-0 must stop the drain after the empty call"


# ── Mid-drain abort: committed batches stay recorded, failed call is a
# timeout, never a lost-rows sample ───────────────────────────────────────


async def test_mid_drain_cancel_keeps_committed_rows_and_marks_only_failed_call(
    telemetry_reader: InMemoryMetricReader,
) -> None:
    """Batches 1-2 commit (5 rows each), batch 3 is cancelled server-side:
    the committed batches' 10 rows stay on the rows counter, the failed
    call increments ``sweep_timeouts`` and records a duration — and no row
    sample for it (rows stayed unbound; 0 would read as a healthy empty
    batch). The drain reports unclean so the backstop streak is not reset."""
    ctx, _ = _drain_ctx(sweep_drain_batches=8)
    clean, call = await _drain(
        ctx,
        [
            5,
            5,
            asyncpg.QueryCanceledError("canceling statement due to statement timeout"),
        ],
    )

    assert clean is False
    assert call.calls == 3
    assert _rows_value(telemetry_reader, "expired_locks") == 10, (
        "the two committed batches' rows must stay recorded — an aborted third "
        "call must not erase or zero them"
    )
    assert _timeout_value(telemetry_reader, "expired_locks") == 1
    assert _duration_samples(telemetry_reader, "expired_locks") == 3, (
        "every call records a duration sample, the aborted one included"
    )


async def test_mid_drain_connection_loss_records_no_timeout(
    telemetry_reader: InMemoryMetricReader,
) -> None:
    """A NON-deadline transient mid-drain (connection loss) ends the drain
    unclean with the committed rows intact — and the timeout counter must
    NOT increment: it counts deadline-aborted batches, not dead sockets."""
    ctx, _ = _drain_ctx(sweep_drain_batches=8)
    clean, call = await _drain(
        ctx,
        [5, asyncpg.ConnectionDoesNotExistError("connection closed")],
    )

    assert clean is False
    assert call.calls == 2
    assert _rows_value(telemetry_reader, "expired_locks") == 5
    assert _timeout_value(telemetry_reader, "expired_locks") == 0
    assert _duration_samples(telemetry_reader, "expired_locks") == 2


# ── Cooperative shutdown ─────────────────────────────────────────────────


async def test_drain_stops_promptly_when_shutdown_set_midway() -> None:
    """Shutdown set after batch 2 must stop the drain before batch 3: the
    drain checks the flag between batches, and every batch already
    committed keeps its progress (a pause, not a rollback)."""
    ctx, _ = _drain_ctx(sweep_drain_batches=8)
    shutdown = asyncio.Event()

    def _flip_shutdown_and_return_five(_idx: int) -> int:
        shutdown.set()
        return 5

    call = _ScriptedCall([5, 5, _flip_shutdown_and_return_five, 5, 5])
    clean = await _drain_bounded(
        ctx,
        shutdown,
        sweep_name="expired_locks",
        call=cast("Callable[[], Awaitable[int]]", call),
        warn_event="sweep-expired-locks-failed",
        warn_kind="sweep_expired_locks_failed",
    )

    assert clean is True, "a shutdown-paused drain is a clean stop, not a failure"
    assert call.calls == 3, (
        "shutdown set on batch 3's return must stop the drain there — batch 4 "
        "would only run if the between-batches check were missing"
    )


# ── Liveness between batches ─────────────────────────────────────────────


async def test_drain_ticks_liveness_between_batches() -> None:
    """A long drain must not look like a stalled loop to the watchdog:
    the drain ticks liveness between batches, with the sweep loop's own
    name and period."""
    ctx, recorder = _drain_ctx(sweep_drain_batches=5, sweep_interval=7.0)
    clean, call = await _drain(ctx, [5, 5, 5, 0])

    assert clean is True
    assert call.calls == 4
    sweep_ticks = [t for t in recorder.ticks if t[0] == "leader.sweep"]
    assert len(sweep_ticks) >= call.calls, (
        "the drain must tick liveness between batches — a watchdog reading "
        f"these stamps would see a stale loop; ticks={recorder.ticks}"
    )
    assert all(period == 7.0 for _, period in sweep_ticks), (
        "the drain's tick period must match the loop's outer tick period"
    )
