"""Red-team pins for the ``taskq.queue.depth`` gauge's label cardinality.

The job-side instruments bound their ``queue`` label through
``_bounded_queue()`` (``obs/_otel.py:361-371``): the first
``_MAX_QUEUE_LABEL_VALUES`` (100) distinct names keep their own series,
everything past the cap collapses onto the fixed ``_other_`` value — the
~100-values-per-dimension ceiling Azure Monitor's guidance sets, behind
which a subscription's 50k-series cap throttles ingestion of EVERY custom
metric. The gauge is the one instrument that skips the seam:
``_observe_queue_depth`` (``obs/_otel.py:632-634``) yields one
``Observation`` per raw cache key, and the cache is filled by a
leader-side ``GROUP BY queue`` — so label cardinality is however many
distinct queue names exist in ``jobs``, unbounded.

The fix is NOT the counter-site five-liner: a gauge is observable, not
additive, so each overflow queue must not yield its own ``_other_``
observation. The signal-preserving shape is partition-then-aggregate:
queues inside the cap keep their series; everything past the cap
contributes to ONE ``_other_`` observation carrying the SUM of the
overflow depths.

These tests drive the real callback through the real SDK reader, with
``_queue_label_values`` reset per test (it is process-global admission
state).
"""

from collections.abc import Iterator

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.testing.otel import collect_metrics

_CAP = otel_mod._MAX_QUEUE_LABEL_VALUES  # pyright: ignore[reportPrivateUsage]  # Why: the test asserts behaviour AT the cap; duplicating the constant would let it drift from the thing it guards.
_OVERFLOW = "_other_"


@pytest.fixture
def gauge_reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    """Per-test reader for the queue-depth gauge, with fresh admission state."""
    monkeypatch.setattr(otel_mod, "_queue_label_values", set())
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME)
    monkeypatch.setattr(
        otel_mod,
        "_queue_depth_gauge",
        meter.create_observable_gauge(
            "taskq.queue.depth",
            callbacks=[otel_mod._observe_queue_depth],  # pyright: ignore[reportPrivateUsage]  # Why: exercising the production callback is the point of the test.
        ),
    )
    yield reader
    obs_mod.update_queue_depth_cache({})


def _queue_depth_points(reader: InMemoryMetricReader) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == "taskq.queue.depth":
            return list(metric.data.data_points)  # type: ignore[union-attr]  # Why: a gauge's data is always Gauge; the SDK types data as a union.
    return []


def _depths_by_label(points: list[NumberDataPoint]) -> dict[str, int]:
    return {str(dp.attributes["queue"]): int(dp.value) for dp in points if dp.attributes}


def test_queue_depth_gauge_bounds_queue_label_cardinality(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """More distinct queue names than the cap must not produce more than
    cap + 1 series: the first ``cap`` names keep their label, the rest
    collapse onto one ``_other_`` series."""
    depths = {f"tenant-queue-{i}": 1 for i in range(_CAP + 50)}
    obs_mod.update_queue_depth_cache(depths)

    points = _queue_depth_points(gauge_reader)
    labels = {str(dp.attributes["queue"]) for dp in points if dp.attributes}

    assert len(labels) <= _CAP + 1, (
        f"the queue.depth gauge emitted {len(labels)} distinct queue labels "
        f"for {_CAP + 50} queues — it reads the raw cache key and never "
        "passes through _bounded_queue (obs/_otel.py:632-634), unlike its "
        "four job-side siblings. Label cardinality here is the distinct "
        "queue-name count in jobs (a leader-side GROUP BY), i.e. unbounded, "
        "and past Azure Monitor's per-dimension guidance it is the whole "
        "subscription's custom-metric ingestion that gets throttled."
    )
    assert len(points) <= _CAP + 1, (
        f"the gauge callback yielded {len(points)} observations — the "
        "overflow must be ONE aggregated observation, not one per overflow "
        "queue sharing the '_other_' label."
    )


def test_queue_depth_gauge_overflow_series_aggregates_depth(
    gauge_reader: InMemoryMetricReader,
) -> None:
    """The ``_other_`` series must carry the SUM of the overflow queues'
    depths — bounding the cardinality must not drop the signal: the total
    depth reported across all series must equal the true total."""
    in_cap = {f"queue-{i}": 2 for i in range(_CAP)}
    overflow = {f"extra-{i}": 3 for i in range(20)}
    depths = {**in_cap, **overflow}
    obs_mod.update_queue_depth_cache(depths)

    reported = _depths_by_label(_queue_depth_points(gauge_reader))

    assert sum(reported.values()) == sum(depths.values()), (
        f"gauge reported total depth {sum(reported.values())} across "
        f"{len(reported)} series, true total {sum(depths.values())}. "
        "Collapsing overflow queues onto '_other_' must aggregate their "
        "depths (partition-then-aggregate); mapping labels per item either "
        "drops depth or yields duplicate '_other_' observations whose "
        "semantics are SDK-dependent."
    )
    overflow_points = [v for k, v in reported.items() if k == _OVERFLOW]
    assert len(overflow_points) == 1, (
        f"expected exactly one '{_OVERFLOW}' series, found {len(overflow_points)}."
    )
