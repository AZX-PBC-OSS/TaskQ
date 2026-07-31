"""Client-layer unit tests for JobsClient.cancel_where and TaskQ.cancel_where."""

from datetime import UTC, datetime

import pytest
from opentelemetry.sdk.metrics._internal.point import NumberDataPoint
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, Metric

from taskq.backend._protocol import JobFilter
from taskq.client._jobs import JobsClient
from taskq.exceptions import EmptyFilterError
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args
from taskq.types import BulkCancelResult

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


@pytest.fixture
def otel_requested_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation for the cancel-requested counter."""

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


async def test_client_cancel_where_with_tags() -> None:
    """JobsClient.cancel_where cancels jobs by tag filter."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)

    for _ in range(3):
        await backend.enqueue(make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW))

    result = await client.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="offboard",
    )

    assert isinstance(result, BulkCancelResult)
    assert result.cancelled_directly == 3


async def test_client_cancel_where_empty_filter_raises() -> None:
    """Empty filter (no predicates) raises EmptyFilterError."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)

    with pytest.raises(EmptyFilterError, match="filter predicate"):
        await client.cancel_where(JobFilter(), reason="oops")


async def test_client_cancel_where_empty_tags_tuple_raises() -> None:
    """JobFilter(tags=()) is an empty filter — must raise EmptyFilterError."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)

    with pytest.raises(EmptyFilterError):
        await client.cancel_where(JobFilter(tags=()), reason="oops")


async def test_client_cancel_where_empty_filter_override() -> None:
    """allow_empty_filter=True bypasses the guardrail."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)

    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW))
    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW))

    result = await client.cancel_where(
        JobFilter(),
        reason="drain all",
        allow_empty_filter=True,
    )

    assert result.cancelled_directly == 2


async def test_client_cancel_where_with_status_filter() -> None:
    """Status filter alone is a valid predicate (not empty)."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)

    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW))

    result = await client.cancel_where(
        JobFilter(status="pending"),
        reason="drain pending",
    )

    assert result.cancelled_directly == 1


async def test_client_cancel_where_with_active_filter() -> None:
    """Active filter alone is a valid predicate (not empty)."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)

    await backend.enqueue(make_enqueue_args(scheduled_at=_NOW))

    result = await client.cancel_where(
        JobFilter(active=True),
        reason="drain active",
    )

    assert result.cancelled_directly == 1


async def test_client_cancel_where_increments_counter(
    otel_requested_reader: InMemoryMetricReader,
) -> None:
    """cancel_where increments taskq.cancellation.requested once."""
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend)

    await backend.enqueue(make_enqueue_args(tags=("tenant-acme",), scheduled_at=_NOW))

    await client.cancel_where(
        JobFilter(tags=("tenant-acme",)),
        reason="test",
    )

    dps = _data_points(otel_requested_reader, "taskq.cancellation.requested")
    assert len(dps) >= 1
    assert dps[0].value == 1


async def test_client_cancel_where_translates_schema_errors() -> None:
    """cancel_where wraps UndefinedTableError in SchemaNotMigratedError."""
    from taskq.exceptions import SchemaNotMigratedError

    backend = InMemoryBackend(clock=FakeClock(_NOW))
    client = JobsClient(backend, settings=type("S", (), {"schema_name": "test_schema"})())

    await backend.enqueue(make_enqueue_args(tags=("x",), scheduled_at=_NOW))

    import asyncpg

    original = backend.cancel_where

    async def raise_undefined(*args: object, **kwargs: object) -> None:
        raise asyncpg.exceptions.UndefinedTableError("relation does not exist")

    backend.cancel_where = raise_undefined  # type: ignore[method-assign]
    try:
        with pytest.raises(SchemaNotMigratedError):
            await client.cancel_where(JobFilter(tags=("x",)), reason="test")
    finally:
        backend.cancel_where = original  # type: ignore[method-assign]
