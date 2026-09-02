"""A contended cron tick must be observable, not silent.

`tick_cron` took `pg_try_advisory_xact_lock` and, on failure, returned with no
log, no metric and no counter. That is benign for the sub-second leader-handover
overlap it exists to cover -- but it is indistinguishable from cron having
stopped entirely.

The dangerous case: the lock is transaction-scoped and releases on
COMMIT/ROLLBACK, which never happens if the holding session was partitioned
without a FIN (a VNet blip, unlike a process kill, which sends a FIN and
releases cleanly). Every subsequent tick on the new leader then returns here.
Cron stops firing fleet-wide, and neither `taskq.cron.disabled_schedules` nor
`consecutive_failures` moves, because `fire_schedule` never runs.

Note this narrows the original report: the leader lock path is NOT the same
shape. `leader.py` calls `record_election_attempt(won=False)` and logs a warning
on every failure branch, and the prune/archive sweeps log on contention too.
Cron was the genuine outlier.

These tests drive :func:`taskq.worker.cron_loop.tick_cron` against a recording
connection and assert the OBSERVABLE outcome of each lock verdict -- the
statements the tick issues, the counter it records, the log it emits -- rather
than the shape of its source text.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast
from uuid import UUID

import pytest
import structlog.testing
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.constants import CRON_LOCK_NAME
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, as_backend
from taskq.testing.otel import counter_data_points
from taskq.worker import cron_loop

if TYPE_CHECKING:
    import asyncpg

_NOW = datetime(2026, 1, 1, tzinfo=UTC)
_WORKER_ID = UUID("00000000-0000-0000-0000-0000000000c1")
_CONTENTION_COUNTER = "taskq.cron.lock_contention"


class _RecordingConn:
    """Records every statement :func:`tick_cron` issues.

    The lock verdict is the only input; everything downstream of it is what
    the tests observe.  A due-schedule query reaching this fake is the
    observable proof the tick proceeded past the lock.
    """

    def __init__(self, *, lock_acquired: bool) -> None:
        self._lock_acquired = lock_acquired
        self.queries: list[str] = []

    async def fetchval(self, query: str, *args: object) -> object:
        self.queries.append(query)
        if "pg_try_advisory_xact_lock" in query:
            return self._lock_acquired
        if "clock_timestamp" in query:
            return _NOW
        raise AssertionError(f"unexpected fetchval: {query}")

    async def fetch(self, query: str, *args: object) -> list[asyncpg.Record]:
        self.queries.append(query)
        return []

    @property
    def read_due_schedules(self) -> bool:
        """Whether the tick got as far as reading the due-schedule set."""
        return any("cron_schedules" in q for q in self.queries)


@pytest.fixture
def metric_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation for the cron lock-contention counter."""
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(
        obs_mod.INSTRUMENTATION_NAME, otel_mod._version()
    )
    monkeypatch.setattr(
        otel_mod,
        "_cron_lock_contention",
        meter.create_counter(_CONTENTION_COUNTER, unit="1"),
    )
    return reader


async def _tick(conn: _RecordingConn) -> None:
    await cron_loop.tick_cron(
        cast("asyncpg.Connection", conn),
        WorkerSettings(),
        as_backend(FakeBackend()),
        "public",
        _WORKER_ID,
    )


async def test_a_contended_tick_counts_logs_and_reads_no_schedules(
    metric_reader: InMemoryMetricReader,
) -> None:
    """Losing the advisory lock must leave a trace and fire nothing: the
    contention counter moves, a diagnosable log line is emitted, and the tick
    never reaches the due-schedule query."""
    conn = _RecordingConn(lock_acquired=False)

    with structlog.testing.capture_logs() as logs:
        await _tick(conn)

    assert conn.read_due_schedules is False

    points = counter_data_points(metric_reader, _CONTENTION_COUNTER)
    # No dimensions: worker_id is a per-process UUID and would mint a new
    # Azure Monitor time series on every deploy, restart and autoscale.
    assert [(p.value, p.attributes) for p in points] == [(1, {})]

    entry = next(e for e in logs if e["event"] == "cron-tick-lock-contended")
    # Per-worker attribution lives here instead, where cardinality is free.
    assert entry["worker_id"] == str(_WORKER_ID)
    assert entry["lock"] == CRON_LOCK_NAME


async def test_an_uncontended_tick_reads_schedules_and_records_no_contention(
    metric_reader: InMemoryMetricReader,
) -> None:
    """The counter is a contention signal, not a tick counter: a tick that wins
    the lock proceeds to the due-schedule query and records nothing."""
    conn = _RecordingConn(lock_acquired=True)

    with structlog.testing.capture_logs() as logs:
        await _tick(conn)

    assert conn.read_due_schedules is True
    assert counter_data_points(metric_reader, _CONTENTION_COUNTER) == []
    assert [e for e in logs if e["event"] == "cron-tick-lock-contended"] == []


async def test_a_contended_tick_records_nothing_when_telemetry_is_off(
    metric_reader: InMemoryMetricReader, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Telemetry off must mean no instrument traffic at all -- not a silently
    swallowed exception -- while the tick's skip semantics are unchanged."""
    monkeypatch.setattr(otel_mod, "_otel_enabled", False)
    conn = _RecordingConn(lock_acquired=False)

    await _tick(conn)

    assert conn.read_due_schedules is False
    assert counter_data_points(metric_reader, _CONTENTION_COUNTER) == []


def test_leader_path_was_already_observable() -> None:
    """Pins the refutation, so the narrower claim is not re-widened later:
    the leader election lock, unlike cron's, already reports every loss."""
    from pathlib import Path

    leader = (
        Path(__file__).resolve().parent.parent / "src" / "taskq" / "worker" / "leader.py"
    ).read_text()
    assert "record_election_attempt" in leader
    assert "election-lock-attempt-failed" in leader
