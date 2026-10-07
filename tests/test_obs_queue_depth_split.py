"""Unit pins for the queue-depth status split:
``taskq.queue.depth_by_status`` ({queue, status}).

The blind spot audited: the fleet's depth gauge folded pending and
scheduled into one number per queue, so a growing `scheduled` share (the
promotion-stall signature) was invisible per queue. The split ships as a
SIBLING instrument, not a relabel of ``taskq.queue.depth``: the shipped
alert set (``TaskQQueueUnserved``'s ``on(queue)`` join) and the
cardinality proofs pin the depth gauge's one-series-per-queue label set.
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
        "_queue_depth_by_status_gauge",
        meter.create_observable_gauge(
            "taskq.queue.depth_by_status",
            callbacks=[otel_mod._observe_queue_depth_by_status],  # pyright: ignore[reportPrivateUsage]  # Why: exercising the production callback is the point of the test.
        ),
    )
    monkeypatch.setattr(
        otel_mod,
        "_queue_depth_gauge",
        meter.create_observable_gauge(
            "taskq.queue.depth",
            callbacks=[otel_mod._observe_queue_depth],  # pyright: ignore[reportPrivateUsage]  # Why: the join-identity test reads both gauges from the same reader.
        ),
    )
    yield reader
    obs_mod.update_queue_depth_by_status_cache({})
    obs_mod.update_queue_depth_cache({})


def _points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == name:
            return list(metric.data.data_points)  # type: ignore[union-attr]  # Why: a gauge's data is always Gauge; the SDK types data as a union.
    return []


def test_depth_by_status_carries_the_status_split(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """One series per (queue, status): a queue holding both a pending
    backlog and a future-armed scheduled wave reads both."""
    obs_mod.update_queue_depth_by_status_cache(
        {("default", "pending"): 7, ("default", "scheduled"): 120, ("reports", "pending"): 1}
    )
    reported = {
        (str(dp.attributes["queue"]), str(dp.attributes["status"])): int(dp.value)
        for dp in _points(gauge_reader, "taskq.queue.depth_by_status")
        if dp.attributes
    }
    assert reported == {
        ("default", "pending"): 7,
        ("default", "scheduled"): 120,
        ("reports", "pending"): 1,
    }


def test_depth_by_status_splits_the_pairs_total_exactly(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """The pair split's per-queue sum must equal the depth gauge's view
    of the same queues — that identity is what makes the two joinable."""
    obs_mod.update_queue_depth_cache({"default": 127, "reports": 1})
    obs_mod.update_queue_depth_by_status_cache(
        {("default", "pending"): 7, ("default", "scheduled"): 120, ("reports", "pending"): 1}
    )
    split_totals: dict[str, int] = {}
    for dp in _points(gauge_reader, "taskq.queue.depth_by_status"):
        if dp.attributes:
            q = str(dp.attributes["queue"])
            split_totals[q] = split_totals.get(q, 0) + int(dp.value)
    depth = {
        str(dp.attributes["queue"]): int(dp.value)
        for dp in _points(gauge_reader, "taskq.queue.depth")
        if dp.attributes
    }
    assert split_totals == depth


def test_depth_by_status_is_capped_like_the_depth_gauge(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """Same partition doctrine: the largest (queue, status) PAIRS keep
    their series, the rest collapse onto ONE `_other_` pair carrying
    their summed depth."""
    pairs: dict[tuple[str, str], int] = {}
    for i in range(_CAP + 30):
        pairs[(f"q-{i}", "pending")] = 1
    pairs[("busy", "pending")] = 9
    obs_mod.update_queue_depth_by_status_cache(pairs)

    points = _points(gauge_reader, "taskq.queue.depth_by_status")
    reported = {
        (str(dp.attributes["queue"]), str(dp.attributes["status"])): int(dp.value)
        for dp in points
        if dp.attributes
    }
    assert len(points) == _CAP + 1
    assert reported[("busy", "pending")] == 9
    assert reported[("_other_", "_other_")] == 31
    assert sum(reported.values()) == sum(pairs.values())


def test_depth_by_status_clears_on_an_empty_sample(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """A demoted leader's clear must take the series with it — a frozen
    split would claim authority over queues it no longer samples."""
    obs_mod.update_queue_depth_by_status_cache({("default", "pending"): 7})
    assert _points(gauge_reader, "taskq.queue.depth_by_status")
    obs_mod.update_queue_depth_by_status_cache({})
    assert _points(gauge_reader, "taskq.queue.depth_by_status") == []
