"""Unit pins for the SSE health pair:
``taskq.admin.sse.connections`` ({topic, surface}) and
``taskq.admin.sse.rejections`` ({topic, surface}, served as
``taskq_admin_sse_rejections_total``).

The blind spot audited: 55 open streams and 5 rejections were invisible
in metrics — the cap's only trace was a client-side 429. The route-level
emissions (the admin endpoint's inline 429, the shared
``acquire_sse_slot`` helper) are pinned in test_sse_connection_caps.py;
this module pins the instruments' own contracts.
"""

from collections.abc import Iterator

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.obs import (
    record_sse_connection_closed,
    record_sse_connection_opened,
    record_sse_rejection,
)
from taskq.testing.otel import collect_metrics

_TOPICS = ("queues", "jobs", "workers", "history", "progress-stream")
_SURFACES = ("admin", "progress")


@pytest.fixture
def gauge_reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME)
    monkeypatch.setattr(
        otel_mod,
        "_sse_connections_gauge",
        meter.create_observable_gauge(
            "taskq.admin.sse.connections",
            callbacks=[otel_mod._observe_sse_connections],  # pyright: ignore[reportPrivateUsage]  # Why: exercising the production callback is the point of the test.
        ),
    )
    # The rejections counter is a LAZY instrument: resolved on the CURRENT
    # meter at call time, so the fixture must swap the module's meter
    # accessor (and clear the per-meter memo) for the isolated reader to
    # see the counts — the meter-swap contract test_obs.py pins.
    monkeypatch.setattr(otel_mod, "get_meter", lambda: meter)
    monkeypatch.setattr(otel_mod, "_lazy_counters", {})
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    # Isolate the process-global level: tests that acquired slots under
    # foreign family keys (test_sse_connection_caps.py) hold real
    # reservations in this same module state.
    monkeypatch.setattr(otel_mod, "_sse_connections_cache", {})
    yield reader
    # Drain the process-global level between tests (the rebind means the
    # fixture's own snapshot must be cleared through the module global).
    for surface in (*_SURFACES, "_other_"):
        for topic in (*_TOPICS, "_other_"):
            for _ in range(2000):
                record_sse_connection_closed(surface, topic)
    from taskq.obs._otel import (
        _sse_connections_cache as _cache_after,  # pyright: ignore[reportPrivateUsage]
    )

    assert not _cache_after


def _points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == name:
            return list(metric.data.data_points)  # type: ignore[union-attr]  # Why: a gauge's data is always Gauge; the SDK types data as a union.
    return []


def test_connections_gauge_levels_by_topic_and_surface(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """One series per (topic, surface): the two surfaces never share a
    series even on a shared topic name."""
    record_sse_connection_opened("admin", "jobs")
    record_sse_connection_opened("admin", "jobs")
    record_sse_connection_opened("progress", "progress-stream")
    reported = {
        (str(dp.attributes["topic"]), str(dp.attributes["surface"])): int(dp.value)
        for dp in _points(gauge_reader, "taskq.admin.sse.connections")
        if dp.attributes
    }
    assert reported == {
        ("jobs", "admin"): 2,
        ("progress-stream", "progress"): 1,
    }


def test_connections_gauge_vanishes_at_zero(gauge_reader: InMemoryMetricReader) -> None:
    """A closed stream's series disappears rather than freezing at its
    last level — the gauge must read held slots, never a stale claim."""
    record_sse_connection_opened("admin", "jobs")
    assert _points(gauge_reader, "taskq.admin.sse.connections")
    record_sse_connection_closed("admin", "jobs")
    assert _points(gauge_reader, "taskq.admin.sse.connections") == []


def test_connections_gauge_clamps_double_close(gauge_reader: InMemoryMetricReader) -> None:
    """A double close is a caller bug and must never drive the level
    negative: the clamp keeps the gauge's floor at 0."""
    record_sse_connection_opened("admin", "jobs")
    record_sse_connection_closed("admin", "jobs")
    record_sse_connection_closed("admin", "jobs")
    assert _points(gauge_reader, "taskq.admin.sse.connections") == []


def test_connections_gauge_is_bounded_by_the_closed_vocabulary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cardinality pin: the cache's key space is the closed enum
    product — 2 surfaces x 5 topics — and a foreign topic string
    collapses onto ``_other_`` rather than minting a series, so any
    connect/close lifetime mints at most 12 series."""
    monkeypatch.setattr(otel_mod, "_sse_connections_cache", {})
    for surface in _SURFACES:
        for topic in _TOPICS:
            record_sse_connection_opened(surface, topic)
    for i in range(500):
        record_sse_connection_opened("admin", f"tenant-topic-{i}")
    from taskq.obs._otel import (
        _sse_connections_cache,  # pyright: ignore[reportPrivateUsage]  # Why: the pin is on the cache's bound.
    )

    assert len(_sse_connections_cache) == len(_TOPICS) * len(_SURFACES) + 1
    assert _sse_connections_cache[("admin", "_other_")] == 500


def test_rejections_counter_moves_at_the_429(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """The counter's labels match the level gauge's exactly, so the two
    join: rejections rising beside connections pinned at the cap is
    saturation."""
    record_sse_rejection("admin", "jobs")
    record_sse_rejection("progress", "progress-stream")
    names = {
        metric.name: list(metric.data.data_points)  # type: ignore[union-attr]
        for metric in collect_metrics(gauge_reader)
    }
    rejections = names.get("taskq.admin.sse.rejections")
    assert rejections is not None
    reported = {
        (str(dp.attributes["topic"]), str(dp.attributes["surface"])): int(dp.value)  # type: ignore[union-attr]  # Why: counter data points are always NumberDataPoint; the SDK types the union.
        for dp in rejections
        if dp.attributes
    }
    assert reported == {
        ("jobs", "admin"): 1,
        ("progress-stream", "progress"): 1,
    }
