"""The scaling-operator metric contract, pinned.

A future dynamic worker-scaling operator scrapes the OTel surface at fleet
scale and makes scaling decisions off it, so the series it reads are a
CONTRACT, not an implementation detail: the gauge set it needs, the label
vocabulary the series share, and the cardinality bounds that keep a scrape
bounded no matter how many queues a deployment mints.

Three pins live here:

1. ``taskq.queue.utilization`` — the one series an operator would otherwise
   COMPUTE client-side from depth ÷ live_workers ÷ actor capacity. The
   computation needs ``actor_config`` (a table the scraper does not have),
   so the leader sampler publishes the ratio as a first-class gauge,
   sampled in the same tick as ``taskq.queue.depth`` and
   ``taskq.queue.live_workers`` so the three join on ``queue`` without
   describing different moments.
2. The label-cardinality bounds as constants: the ``queue`` /
   cron-``actor`` / ``bucket`` caps and their shared ``_other_`` overflow.
   A future label addition that mints unbounded series fails here first.
3. The per-queue leader gauges' boundedness behaviour: fed more distinct
   queues than the cap, each emits exactly ``cap + 1`` series and the
   ``_other_`` series carries the summed remainder (the reported total
   always equals the true total).
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from opentelemetry.metrics import CallbackOptions
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
from taskq.testing.otel import collect_metrics

_CAP = otel_mod._MAX_QUEUE_LABEL_VALUES  # pyright: ignore[reportPrivateUsage]  # Why: the tests assert behaviour AT the cap; duplicating the constant would let it drift from the thing it guards.


@pytest.fixture
def utilization_reader(monkeypatch: pytest.MonkeyPatch) -> Iterator[InMemoryMetricReader]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(obs_mod.INSTRUMENTATION_NAME)
    monkeypatch.setattr(
        otel_mod,
        "_queue_utilization_gauge",
        meter.create_observable_gauge(
            "taskq.queue.utilization",
            callbacks=[otel_mod._observe_queue_utilization],  # pyright: ignore[reportPrivateUsage]  # Why: exercising the production callback is the point of the test.
        ),
    )
    yield reader
    obs_mod.update_queue_utilization_cache({})


def _points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    for metric in collect_metrics(reader):
        if metric.name == name:
            return list(metric.data.data_points)  # type: ignore[union-attr]  # Why: a gauge's data is always Gauge; the SDK types data as a union.
    return []


# ── 1. taskq.queue.utilization ──────────────────────────────────────────


def test_utilization_gauge_reports_one_series_per_queue(
    utilization_reader: InMemoryMetricReader,
) -> None:
    obs_mod.update_queue_utilization_cache({"default": 0.5, "reports": 1.25})
    reported = {
        str(dp.attributes["queue"]): float(dp.value)
        for dp in _points(utilization_reader, "taskq.queue.utilization")
        if dp.attributes
    }
    assert reported == {"default": 0.5, "reports": 1.25}


def test_utilization_gauge_clears_on_an_empty_sample(
    utilization_reader: InMemoryMetricReader,
) -> None:
    """A queue with zero effective capacity reports NO utilization series
    (the sampler omits it), so an empty cache must clear the series rather
    than freeze it at the last ratio."""
    obs_mod.update_queue_utilization_cache({"default": 0.5})
    assert _points(utilization_reader, "taskq.queue.utilization")
    obs_mod.update_queue_utilization_cache({})
    assert _points(utilization_reader, "taskq.queue.utilization") == []


def test_utilization_gauge_is_capped_like_the_depth_gauge(
    utilization_reader: InMemoryMetricReader,
) -> None:
    """Same partition as taskq.queue.depth / taskq.queue.live_workers: the
    largest ``cap`` queues keep their series, the rest collapse onto ONE
    ``_other_`` series carrying their summed utilization, so the three
    gauges share a label vocabulary and a bound and a PromQL join on
    ``queue`` cannot fan out."""
    ratios = {f"q-{i}": 1.0 for i in range(_CAP + 30)}
    ratios["busy"] = 2.5
    obs_mod.update_queue_utilization_cache(ratios)

    points = _points(utilization_reader, "taskq.queue.utilization")
    reported = {str(dp.attributes["queue"]): float(dp.value) for dp in points if dp.attributes}
    assert len(points) == _CAP + 1
    assert reported["busy"] == 2.5
    assert reported["_other_"] == 31.0  # 131 queues fed, cap admitted (busy among them)
    assert sum(reported.values()) == sum(ratios.values())


# ── 2. The cardinality bounds as constants ──────────────────────────────


def test_label_cap_constants_are_pinned() -> None:
    """The caps ARE the operator contract's cardinality guarantee: 100
    admitted values per capped dimension (the ~100-values-per-dimension
    ceiling the cloud vendors' guidance sets), one shared overflow value.
    Raising one of these silently would break every bounded scrape that
    sized its TSDB off this module's docs — the bump must land here."""
    assert otel_mod._MAX_QUEUE_LABEL_VALUES == 100  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._MAX_ACTOR_LABEL_VALUES == 100  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._MAX_BUCKET_LABEL_VALUES == 100  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._QUEUE_LABEL_OVERFLOW == "_other_"  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._ACTOR_LABEL_OVERFLOW == "_other_"  # pyright: ignore[reportPrivateUsage]
    assert otel_mod._BUCKET_LABEL_OVERFLOW == "_other_"  # pyright: ignore[reportPrivateUsage]


def test_capped_dimensions_overflow_at_the_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every capped dimension funnels through one admission core: the first
    ``cap`` distinct values are admitted (never evicted), everything past
    the cap collapses onto the fixed overflow value — so the series count
    is hard-bounded at cap + 1 no matter how many distinct values the
    callers mint."""
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    for bounded, values in (
        (otel_mod._bounded_queue, otel_mod._queue_label_values),  # pyright: ignore[reportPrivateUsage]
        (otel_mod._bounded_cron_actor, otel_mod._cron_actor_label_values),  # pyright: ignore[reportPrivateUsage]
        (otel_mod._bounded_bucket, otel_mod._bucket_label_values),  # pyright: ignore[reportPrivateUsage]
    ):
        values.clear()
        admitted = [f"v-{i}" for i in range(_CAP)]
        overflow = [f"past-{i}" for i in range(_CAP + 50)]
        out = {bounded(v) for v in [*admitted, *overflow]}
        assert out == set(admitted) | {"_other_"}, bounded.__name__
        values.clear()


# ── 3. The per-queue leader gauges' boundedness ─────────────────────────


def test_per_queue_leader_gauges_emit_at_most_cap_plus_one_series(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The boundedness guarantee itself, per gauge: an unbounded feed of
    distinct queue names yields at most cap + 1 observations, and the
    overflow series carries the summed remainder so the reported total
    always equals the true total. A future gauge added to this family must
    route through the same partition (_observe_capped_per_queue) — a gauge
    that yields one series per input instead reds here."""
    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    counts = {f"queue-{i}": 1 for i in range(_CAP + 50)}

    for update, observe in (
        (obs_mod.update_queue_depth_cache, otel_mod._observe_queue_depth),  # pyright: ignore[reportPrivateUsage]
        (obs_mod.update_queue_live_workers_cache, otel_mod._observe_queue_live_workers),  # pyright: ignore[reportPrivateUsage]
        (obs_mod.update_queue_utilization_cache, otel_mod._observe_queue_utilization),  # pyright: ignore[reportPrivateUsage]
    ):
        update(counts)
        observations = list(observe(CallbackOptions()))  # pyright: ignore[reportPrivateUsage]
        assert len(observations) == _CAP + 1, update.__name__
        reported = {
            str(obs.attributes["queue"]): obs.value for obs in observations if obs.attributes
        }
        assert reported["_other_"] == 50, update.__name__
        assert sum(reported.values()) == len(counts), update.__name__
    # Every cache the loop feeds was rebound above; leave them empty.
    obs_mod.update_queue_depth_cache({})
    obs_mod.update_queue_live_workers_cache({})
    obs_mod.update_queue_utilization_cache({})
