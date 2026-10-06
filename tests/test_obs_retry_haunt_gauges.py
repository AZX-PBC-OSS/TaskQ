"""Unit pins for the retry-ladder (haunt) gauges and their label-less
siblings: ``taskq.jobs.retrying``, ``taskq.jobs.retry_headroom``,
``taskq.jobs.scheduled_horizon_seconds``.

The blind spot audited: a job sitting at attempt 6 of 1000 was invisible
in metrics — by_status counts it as a healthy pending row and
taskq.jobs.attempt_failures last moved five attempts ago. The gauges are
the cache-push observable pattern; the sampler-side shape (ONE grouped
read producing both per-actor series) is pinned in the loops' own tests
and the live review suite.
"""

from collections.abc import Iterator

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.testing.otel import collect_metrics


@pytest.fixture
def gauge_reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME)
    monkeypatch.setattr(
        otel_mod,
        "_jobs_retrying_gauge",
        meter.create_observable_gauge(
            "taskq.jobs.retrying",
            callbacks=[otel_mod._observe_jobs_retrying],  # pyright: ignore[reportPrivateUsage]  # Why: exercising the production callback is the point of the test.
        ),
    )
    monkeypatch.setattr(
        otel_mod,
        "_jobs_retry_headroom_gauge",
        meter.create_observable_gauge(
            "taskq.jobs.retry_headroom",
            callbacks=[otel_mod._observe_jobs_retry_headroom],  # pyright: ignore[reportPrivateUsage]  # Why: as above.
        ),
    )
    monkeypatch.setattr(
        otel_mod,
        "_scheduled_horizon_gauge",
        meter.create_observable_gauge(
            "taskq.jobs.scheduled_horizon_seconds",
            callbacks=[otel_mod._observe_scheduled_horizon],  # pyright: ignore[reportPrivateUsage]  # Why: as above.
        ),
    )
    yield reader
    obs_mod.update_jobs_retrying_cache({})
    obs_mod.update_jobs_retry_headroom_cache({})
    obs_mod.update_scheduled_horizon_cache(0.0)


def _points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == name:
            return list(metric.data.data_points)  # type: ignore[union-attr]  # Why: a gauge's data is always Gauge; the SDK types data as a union.
    return []


def test_retrying_gauge_reports_one_series_per_actor(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """The haunt shape made visible: an actor's live rows past attempt 0."""
    obs_mod.update_jobs_retrying_cache({"emailer": 3, "sync": 1})
    reported = {
        str(dp.attributes["actor"]): int(dp.value)
        for dp in _points(gauge_reader, "taskq.jobs.retrying")
        if dp.attributes
    }
    assert reported == {"emailer": 3, "sync": 1}


def test_retry_headroom_gauge_carries_the_min_headroom(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """The alertable operand: per actor, the WORST live row's remaining
    attempts. A job at attempt 6/1000 reports 994; a job on its final
    attempt reports 0."""
    obs_mod.update_jobs_retry_headroom_cache({"tall_ladder": 994, "last_attempt": 0})
    reported = {
        str(dp.attributes["actor"]): int(dp.value)
        for dp in _points(gauge_reader, "taskq.jobs.retry_headroom")
        if dp.attributes
    }
    assert reported == {"tall_ladder": 994, "last_attempt": 0}


def test_retry_gauges_vanish_when_the_population_drains(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """An actor whose retrying rows all resolved must vanish from BOTH
    series rather than freeze — a frozen headroom would page on a ladder
    that already ended."""
    obs_mod.update_jobs_retrying_cache({"emailer": 2})
    obs_mod.update_jobs_retry_headroom_cache({"emailer": 4})
    assert _points(gauge_reader, "taskq.jobs.retrying")
    assert _points(gauge_reader, "taskq.jobs.retry_headroom")
    obs_mod.update_jobs_retrying_cache({})
    obs_mod.update_jobs_retry_headroom_cache({})
    assert _points(gauge_reader, "taskq.jobs.retrying") == []
    assert _points(gauge_reader, "taskq.jobs.retry_headroom") == []


def test_retry_gauges_share_one_label_vocabulary(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """Both gauges are keyed by actor alone — the same label set — so an
    alert joining population against headroom is a plain vector
    operation, never a join modifier."""
    obs_mod.update_jobs_retrying_cache({"a": 1})
    obs_mod.update_jobs_retry_headroom_cache({"a": 2})
    retrying_labels = {
        tuple(sorted(dp.attributes)) for dp in _points(gauge_reader, "taskq.jobs.retrying")
    }
    headroom_labels = {
        tuple(sorted(dp.attributes)) for dp in _points(gauge_reader, "taskq.jobs.retry_headroom")
    }
    assert retrying_labels == {("actor",)}
    assert headroom_labels == {("actor",)}


def test_scheduled_horizon_gauge_is_label_less(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """The label-less-twin convention: horizon joins the other label-less
    backlog operands (oldest_due_age, scheduled_count) under vector and."""
    obs_mod.update_scheduled_horizon_cache(3600.0)
    points = _points(gauge_reader, "taskq.jobs.scheduled_horizon_seconds")
    assert len(points) == 1
    assert points[0].attributes == {}
    assert float(points[0].value) == 3600.0


def test_scheduled_horizon_gauge_reports_negative_when_overdue(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """MAX(scheduled_at) - now is a measurement, not a clamp: a negative
    horizon means even the furthest-armed job is overdue — the promotion
    stall shape. The gauge must not flatten it to zero."""
    obs_mod.update_scheduled_horizon_cache(-42.5)
    points = _points(gauge_reader, "taskq.jobs.scheduled_horizon_seconds")
    assert len(points) == 1
    assert float(points[0].value) == -42.5
