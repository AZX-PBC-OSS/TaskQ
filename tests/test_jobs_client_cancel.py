"""Tests for JobsClient.cancel counter increment.

Covers: JobsClient.cancel increments ``taskq.cancellation.requested``
exactly once per call regardless of ``cancellation_initiated`` outcome.
"""

from datetime import UTC, datetime

import pytest
from opentelemetry.sdk.metrics._internal.point import NumberDataPoint
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, Metric

from taskq._ids import new_job_id
from taskq.backend import EnqueueArgs
from taskq.client._jobs import JobsClient
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend


@pytest.fixture
def otel_requested_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation for the cancel-requested counter.

    Replaces ``taskq.obs._cancellation_requested`` with a fresh counter backed
    by ``InMemoryMetricReader``. monkeypatch auto-restores the original.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    import taskq.obs as obs_mod
    import taskq.obs._otel as otel_mod

    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())

    monkeypatch.setattr(
        otel_mod,
        "_cancellation_requested",
        new_meter.create_counter("taskq.cancellation.requested"),
    )

    return reader


def _collect_metrics(reader: InMemoryMetricReader) -> list[Metric]:
    md = reader.get_metrics_data()
    assert md is not None
    results: list[Metric] = []
    for rm in md.resource_metrics:
        for sm in rm.scope_metrics:
            results.extend(sm.metrics)
    return results


def _data_points(reader: InMemoryMetricReader, metric_name: str) -> list[NumberDataPoint]:
    for m in _collect_metrics(reader):
        if m.name == metric_name:
            return list(m.data.data_points)  # type: ignore[return-value] # Why: counter metrics always produce NumberDataPoint instances.
    return []


_NOW = datetime(2026, 1, 1, tzinfo=UTC)


async def test_cancel_increments_requested_counter_once(
    otel_requested_reader: InMemoryMetricReader,
) -> None:
    """JobsClient.cancel increments taskq.cancellation.requested exactly once."""
    clock = FakeClock(_NOW)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_NOW,
        )
    )

    await client.cancel(job_id)

    dps = _data_points(otel_requested_reader, "taskq.cancellation.requested")
    assert len(dps) >= 1
    assert dps[0].value == 1


async def test_cancel_increments_requested_counter_regardless_of_initiated(
    otel_requested_reader: InMemoryMetricReader,
) -> None:
    """Multiple cancel calls all increment the counter, even when
    cancellation_initiated is False on later calls.
    """
    clock = FakeClock(_NOW)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)

    job_id = new_job_id()
    await backend.enqueue(
        EnqueueArgs(
            id=job_id,
            actor="test",
            queue="default",
            payload={},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=_NOW,
        )
    )

    result1 = await client.cancel(job_id)
    assert result1.cancellation_initiated is True

    result2 = await client.cancel(job_id)
    assert result2.cancellation_initiated is False

    dps = _data_points(otel_requested_reader, "taskq.cancellation.requested")
    assert len(dps) >= 1
    assert dps[0].value == 2


async def test_cancel_keyerror_still_increments_counter(
    otel_requested_reader: InMemoryMetricReader,
) -> None:
    """KeyError on non-existent job: record_cancel_requested is called
    before backend.get(), so the counter is still incremented.
    """
    clock = FakeClock(_NOW)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend)

    job_id = new_job_id()

    with pytest.raises(KeyError):
        await client.cancel(job_id)

    dps = _data_points(otel_requested_reader, "taskq.cancellation.requested")
    assert len(dps) >= 1
    assert dps[0].value == 1
