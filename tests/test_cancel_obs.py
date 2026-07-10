"""Unit tests for cancellation OTel counters.

``taskq.cancellation.phase_transitions`` counter, ``_record_phase_transition``,
and ``taskq.cancellation.requested`` counter.
"""

import pytest
from opentelemetry.sdk.metrics._internal.point import NumberDataPoint
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, Metric

from taskq.backend._protocol import CancelPhase


@pytest.fixture
def otel_reader(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Per-test OTel meter isolation via monkeypatch.

    OTel providers are process-global. Rather than swapping the global provider
    (blocked by OTel's _Once guard) or reloading modules (breaks cross-file
    imports), this fixture replaces the module-level counter objects with fresh
    copies created on a per-test MeterProvider backed by InMemoryMetricReader.
    monkeypatch auto-restores the originals on teardown.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    import taskq.obs as obs_mod
    import taskq.obs._otel as otel_mod
    import taskq.worker.cancel as cancel_mod

    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())

    monkeypatch.setattr(
        otel_mod,
        "_cancellation_requested",
        new_meter.create_counter("taskq.cancellation.requested"),
    )
    monkeypatch.setattr(
        cancel_mod,
        "_phase_transitions",
        new_meter.create_counter("taskq.cancellation.phase_transitions"),
    )

    return reader


def _collect_metrics(reader: InMemoryMetricReader) -> list[Metric]:
    """Flush the test reader and return flat list of metrics."""
    md = reader.get_metrics_data()
    assert md is not None
    results: list[Metric] = []
    for rm in md.resource_metrics:
        for sm in rm.scope_metrics:
            results.extend(sm.metrics)
    return results


def _data_points(reader: InMemoryMetricReader, metric_name: str) -> list[NumberDataPoint]:
    """Return data points for a counter metric by name."""
    for m in _collect_metrics(reader):
        if m.name == metric_name:
            return list(m.data.data_points)  # type: ignore[return-value] # Why: counter metrics always produce NumberDataPoint instances; pyright's union type with histogram variants is overly broad.
    return []


# ── taskq.cancellation.requested ─────────────────────────────────────────


def test_record_cancel_requested_bumps_counter(otel_reader: InMemoryMetricReader) -> None:
    """Calling record_cancel_requested increments the requested counter."""
    from taskq.obs import record_cancel_requested

    record_cancel_requested()

    dps = _data_points(otel_reader, "taskq.cancellation.requested")
    assert len(dps) == 1
    assert dps[0].value == 1
    assert dps[0].attributes == {}


def test_record_cancel_requested_multiple_calls(otel_reader: InMemoryMetricReader) -> None:
    """Multiple calls accumulate on the counter."""
    from taskq.obs import record_cancel_requested

    for _ in range(3):
        record_cancel_requested()

    dps = _data_points(otel_reader, "taskq.cancellation.requested")
    assert dps[0].value == 3


# ── taskq.cancellation.phase_transitions (in worker/cancel.py) ───────────


def test_phase_transition_counter_registered(otel_reader: InMemoryMetricReader) -> None:
    """Counter ``taskq.cancellation.phase_transitions`` is registered with the meter.

    The OTel SDK only exposes metrics in the reader after they have data points,
    so we record a sample transition to force the counter to appear.
    """
    from taskq.worker.cancel import _record_phase_transition

    _record_phase_transition(CancelPhase.NONE, CancelPhase.COOPERATIVE)

    metrics = _collect_metrics(otel_reader)
    names = {m.name for m in metrics}
    assert "taskq.cancellation.phase_transitions" in names


def test_record_phase_transition_0_to_1(otel_reader: InMemoryMetricReader) -> None:
    """_record_phase_transition(NONE, COOPERATIVE) creates a timeseries with the right attribute pair."""
    from taskq.worker.cancel import _record_phase_transition

    _record_phase_transition(CancelPhase.NONE, CancelPhase.COOPERATIVE)

    dps = _data_points(otel_reader, "taskq.cancellation.phase_transitions")
    assert len(dps) >= 1
    series_0_1 = [dp for dp in dps if dp.attributes == {"from_phase": 0, "to_phase": 1}]
    assert len(series_0_1) == 1
    assert series_0_1[0].value == 1


def test_record_phase_transition_abandonment_sentinel(
    otel_reader: InMemoryMetricReader,
) -> None:
    """_record_phase_transition(FORCED, ABANDON_PENDING) produces a distinct timeseries for the abandonment transition."""
    from taskq.worker.cancel import _record_phase_transition

    _record_phase_transition(CancelPhase.COOPERATIVE, CancelPhase.FORCED)
    _record_phase_transition(CancelPhase.FORCED, CancelPhase.ABANDON_PENDING)

    dps = _data_points(otel_reader, "taskq.cancellation.phase_transitions")
    series_1_2 = [dp for dp in dps if dp.attributes == {"from_phase": 1, "to_phase": 2}]
    series_2_3 = [dp for dp in dps if dp.attributes == {"from_phase": 2, "to_phase": 3}]

    assert len(series_1_2) == 1
    assert series_1_2[0].value == 1
    assert len(series_2_3) == 1
    assert series_2_3[0].value == 1
