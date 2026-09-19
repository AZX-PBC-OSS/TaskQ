"""Unit tests for the gauges that make an unserved queue visible:
``taskq.queue.live_workers`` (per-queue live workers, capped like the depth
gauge it is joined with) and the ``reason`` label on ``taskq.jobs.stranded``.
"""

from collections.abc import Iterator

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.testing.otel import collect_metrics

_CAP = otel_mod._MAX_QUEUE_LABEL_VALUES  # pyright: ignore[reportPrivateUsage]  # Why: the test asserts behaviour AT the cap; duplicating the constant would let it drift from the thing it guards.


@pytest.fixture
def gauge_reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME)
    monkeypatch.setattr(
        otel_mod,
        "_queue_live_workers_gauge",
        meter.create_observable_gauge(
            "taskq.queue.live_workers",
            callbacks=[otel_mod._observe_queue_live_workers],  # pyright: ignore[reportPrivateUsage]  # Why: exercising the production callback is the point of the test.
        ),
    )
    monkeypatch.setattr(
        otel_mod,
        "_stranded_jobs_gauge",
        meter.create_observable_gauge(
            "taskq.jobs.stranded",
            callbacks=[otel_mod._observe_stranded_jobs],  # pyright: ignore[reportPrivateUsage]  # Why: as above.
        ),
    )
    yield reader
    obs_mod.update_queue_live_workers_cache({})
    obs_mod.update_stranded_jobs_cache({})


def _points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == name:
            return list(metric.data.data_points)  # type: ignore[union-attr]  # Why: a gauge's data is always Gauge; the SDK types data as a union.
    return []


def test_live_workers_gauge_reports_one_series_per_queue(
    gauge_reader: InMemoryMetricReader,
) -> None:
    obs_mod.update_queue_live_workers_cache({"default": 2, "reports": 1})
    reported = {
        str(dp.attributes["queue"]): int(dp.value)
        for dp in _points(gauge_reader, "taskq.queue.live_workers")
        if dp.attributes
    }
    assert reported == {"default": 2, "reports": 1}


def test_live_workers_gauge_clears_on_an_empty_sample(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """A queue that lost its last live worker must vanish from the series -
    that absence is exactly what TaskQQueueUnserved joins against."""
    obs_mod.update_queue_live_workers_cache({"default": 2})
    assert _points(gauge_reader, "taskq.queue.live_workers")
    obs_mod.update_queue_live_workers_cache({})
    assert _points(gauge_reader, "taskq.queue.live_workers") == []


def test_live_workers_gauge_is_capped_like_the_depth_gauge(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """Same partition as taskq.queue.depth: the largest ``cap`` queues keep
    their series, the rest collapse onto ONE ``_other_`` series carrying
    their sum, so the two gauges share a label vocabulary and a bound."""
    counts = {f"q-{i}": 1 for i in range(_CAP + 30)}
    counts["busy"] = 9
    obs_mod.update_queue_live_workers_cache(counts)

    points = _points(gauge_reader, "taskq.queue.live_workers")
    reported = {str(dp.attributes["queue"]): int(dp.value) for dp in points if dp.attributes}
    assert len(points) == _CAP + 1
    assert reported["busy"] == 9
    assert reported["_other_"] == 31  # cap + 31 queues, cap admitted (busy among them)
    assert sum(reported.values()) == sum(counts.values())


def test_stranded_gauge_carries_the_reason_label(gauge_reader: InMemoryMetricReader) -> None:
    """One series per (actor, reason): the two conditions have different
    remediations, and a per-actor total made the detector look wrong to an
    operator who found the actor_config row present."""
    obs_mod.update_stranded_jobs_cache(
        {
            ("ghost", "no_actor_config"): 3,
            ("routed_nowhere", "unserved_queue"): 2,
        }
    )
    reported = {
        (str(dp.attributes["actor"]), str(dp.attributes["reason"])): int(dp.value)
        for dp in _points(gauge_reader, "taskq.jobs.stranded")
        if dp.attributes
    }
    assert reported == {
        ("ghost", "no_actor_config"): 3,
        ("routed_nowhere", "unserved_queue"): 2,
    }
