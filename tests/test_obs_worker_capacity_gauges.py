"""Unit tests for the worker capacity gauges — ``taskq.worker.active_jobs``
and ``taskq.worker.max_concurrency`` — the OTel twins of the health
socket's hand-rendered ``taskq_active_jobs`` that a real scrape never
reached, and the per-actor ``taskq.jobs.running`` gauge's observer.
"""

from collections.abc import Iterator

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.testing.otel import collect_metrics


class _Registry:
    """The slice of ActiveJobRegistry the gauge reads."""

    def __init__(self, n: int) -> None:
        self.n = n

    def count(self) -> int:
        return self.n


@pytest.fixture
def gauge_reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(obs_mod.INSTRUMENTATION_NAME)
    meter.create_observable_gauge(
        "taskq.worker.active_jobs",
        callbacks=[otel_mod._observe_worker_active_jobs],  # pyright: ignore[reportPrivateUsage]  # Why: exercising the production callback is the point of the test.
    )
    meter.create_observable_gauge(
        "taskq.worker.max_concurrency",
        callbacks=[otel_mod._observe_worker_max_concurrency],  # pyright: ignore[reportPrivateUsage]  # Why: as above.
    )
    meter.create_observable_gauge(
        "taskq.jobs.running",
        callbacks=[otel_mod._observe_jobs_running],  # pyright: ignore[reportPrivateUsage]  # Why: as above.
    )
    yield reader
    obs_mod.set_worker_capacity_source(None, 0)
    obs_mod.update_jobs_running_cache({})


def _points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == name:
            return list(metric.data.data_points)  # type: ignore[union-attr]  # Why: a gauge's data is always Gauge; the SDK types data as a union.
    return []


def test_capacity_gauges_report_nothing_until_a_worker_is_hosted(
    gauge_reader: InMemoryMetricReader,
) -> None:
    assert _points(gauge_reader, "taskq.worker.active_jobs") == []
    assert _points(gauge_reader, "taskq.worker.max_concurrency") == []


def test_capacity_gauges_read_the_live_registry_and_the_ceiling(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """Utilisation is active_jobs / max_concurrency per process: both
    series are label-free, and active_jobs follows the registry on every
    collection rather than a stamped copy."""
    registry = _Registry(3)
    obs_mod.set_worker_capacity_source(registry, 16)

    active = _points(gauge_reader, "taskq.worker.active_jobs")
    ceiling = _points(gauge_reader, "taskq.worker.max_concurrency")
    assert [(p.value, dict(p.attributes or {})) for p in active] == [(3, {})]
    assert [(p.value, dict(p.attributes or {})) for p in ceiling] == [(16, {})]

    registry.n = 16
    assert [p.value for p in _points(gauge_reader, "taskq.worker.active_jobs")] == [16]


def test_running_gauge_is_one_series_per_actor(gauge_reader: InMemoryMetricReader) -> None:
    obs_mod.update_jobs_running_cache({"resize_image": 2, "send_email": 1})
    reported = {
        str(p.attributes["actor"]): int(p.value)
        for p in _points(gauge_reader, "taskq.jobs.running")
        if p.attributes
    }
    assert reported == {"resize_image": 2, "send_email": 1}
    obs_mod.update_jobs_running_cache({})
    assert _points(gauge_reader, "taskq.jobs.running") == []
