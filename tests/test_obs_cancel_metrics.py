"""Unit pins for the executing-side cancel metrics:
``taskq.jobs.cancels_actored_total`` ({actor}, the worker cancel
controller's durable counter) and ``taskq.jobs.cancel_pending`` (the
leader-sampled level gauge).

The blind spot audited: a CLI cancel wrote the row and exited — the
issuer's own ``taskq.cancellation.requested`` increment died with its
process, so the scrape showed nothing. The counter moves on the WORKER
whose poll acts on the request; the gauge counts the rows the protocol
still owes a terminal write.
"""

from collections.abc import Iterator

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.obs import record_cancel_actored
from taskq.testing.otel import collect_metrics


@pytest.fixture
def otel_reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())
    monkeypatch.setattr(
        otel_mod,
        "_cancels_actored",
        meter.create_counter("taskq.jobs.cancels_actored"),
    )
    monkeypatch.setattr(
        otel_mod,
        "_cancel_pending_gauge",
        meter.create_observable_gauge(
            "taskq.jobs.cancel_pending",
            callbacks=[otel_mod._observe_cancel_pending],  # pyright: ignore[reportPrivateUsage]  # Why: exercising the production callback is the point of the test.
        ),
    )
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    yield reader
    obs_mod.update_cancel_pending_cache(None)


def _points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == name:
            return list(metric.data.data_points)  # type: ignore[union-attr]  # Why: gauge/counter data is always NumberDataPoint here.
    return []


def test_cancel_actored_counts_per_actor(
    otel_reader: InMemoryMetricReader,
) -> None:
    """One increment per cancel the worker begins acting on, labeled by
    the running job's registered actor."""
    record_cancel_actored("emailer")
    record_cancel_actored("emailer")
    record_cancel_actored("renderer")
    reported = {
        str(dp.attributes["actor"]): int(dp.value)
        for dp in _points(otel_reader, "taskq.jobs.cancels_actored")
        if dp.attributes
    }
    assert reported == {"emailer": 2, "renderer": 1}


def test_cancel_actored_is_the_executing_side_not_the_issuer(
    otel_reader: InMemoryMetricReader,
) -> None:
    """The two cancel counters are different surfaces: the issuer-side
    ``taskq.cancellation.requested`` stays unlabeled and untouched by the
    executing-side recorder — a CLI cancel's worker scrape moves only
    the actored one."""
    record_cancel_actored("emailer")
    names = {metric.name for metric in collect_metrics(otel_reader)}
    assert "taskq.jobs.cancels_actored" in names
    assert "taskq.cancellation.requested" not in names


def test_cancel_pending_gauge_reports_the_leader_sample(
    otel_reader: InMemoryMetricReader,
) -> None:
    """The level gauge carries the leader's count, label-less (the fleet
    total is the alertable shape)."""
    obs_mod.update_cancel_pending_cache(3)
    points = _points(otel_reader, "taskq.jobs.cancel_pending")
    assert len(points) == 1
    assert points[0].attributes == {}
    assert int(points[0].value) == 3


def test_cancel_pending_gauge_clears_on_demotion(
    otel_reader: InMemoryMetricReader,
) -> None:
    """The demotion clear rebinds to the empty state — the series goes
    stale and the new leader's is the one answering, never a frozen
    level (and never an active claim of 0)."""
    obs_mod.update_cancel_pending_cache(3)
    assert _points(otel_reader, "taskq.jobs.cancel_pending")
    obs_mod.update_cancel_pending_cache(None)
    assert _points(otel_reader, "taskq.jobs.cancel_pending") == []
