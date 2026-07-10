"""OTel test utilities: span exporter, tracer/meter setup, and metric query helpers.

Provides a self-contained test OTel stack so individual tests don't
need to set up providers, exporters, or patching themselves.
"""

import logging
from collections.abc import Generator, Sequence
from typing import Any

import pytest
import structlog
from opentelemetry import trace
from opentelemetry.sdk.metrics._internal.point import HistogramDataPoint, NumberDataPoint
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, Metric
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod
import taskq.obs._structlog as structlog_mod

__all__ = [
    "ListSpanExporter",
    "_logging_configured_guard",
    "_otel_enabled_guard",
    "_save_logging_configured",
    "_save_otel_enabled",
    "collect_metrics",
    "counter_data_points",
    "counter_value",
    "histogram_points",
    "restore_logging_configured",
    "restore_otel_enabled",
    "setup_meter",
    "setup_tracer",
]


class ListSpanExporter(SpanExporter):
    """Records finished spans and provides name-based query helpers.

    Tests should use ``span_named``, ``spans_named``, and ``events_on``
    instead of indexing into ``self.spans`` directly.
    """

    def __init__(self) -> None:
        self.spans: list[ReadableSpan] = []

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        self.spans.extend(spans)
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        pass

    def span_named(self, name: str) -> ReadableSpan | None:
        """Return the first finished span with *name*, or None."""
        for s in self.spans:
            if s.name == name:
                return s
        return None

    def spans_named(self, name: str) -> list[ReadableSpan]:
        """Return all finished spans with *name*."""
        return [s for s in self.spans if s.name == name]

    def events_on(self, span_name: str, event_name: str) -> list[Any]:
        """Return all events named *event_name* on the first span named *span_name*.

        Returns an empty list if the span doesn't exist.
        """
        span = self.span_named(span_name)
        if span is None:
            return []
        return [ev for ev in span.events if ev.name == event_name]

    def spans_with_kind(self, kind: trace.SpanKind) -> list[ReadableSpan]:
        """Return all finished spans with the given *kind*."""
        return [s for s in self.spans if s.kind == kind]


def setup_tracer(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[TracerProvider, ListSpanExporter]:
    """Create a test-scoped TracerProvider and patch ``obs.get_tracer``.

    OTel's global TracerProvider has a ``_Once`` guard that prevents
    ``trace.set_tracer_provider`` from being called more than once per
    process.  Patching ``obs_mod.get_tracer`` to return a tracer from
    our per-test provider avoids the guard entirely.

    Also sets ``otel_mod._otel_enabled = True`` so that
    ``safe_start_span`` delegates to the real tracer.
    """
    exporter = ListSpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    test_tracer = provider.get_tracer(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())  # type: ignore[reportPrivateUsage]  # Why: testing utility reads private module state for test-scoped tracer setup.
    monkeypatch.setattr(otel_mod, "get_tracer", lambda: test_tracer)
    monkeypatch.setattr(obs_mod, "get_tracer", lambda: test_tracer)
    otel_mod.set_otel_enabled(True)
    return provider, exporter


def setup_meter(
    monkeypatch: pytest.MonkeyPatch,
    *,
    extra_instruments: dict[str, str] | None = None,
) -> InMemoryMetricReader:
    """Create a test-scoped MeterProvider and patch ``obs.get_meter``.

    By default patches the instruments used by the dispatch and consumer
    paths.  Pass *extra_instruments* as ``{module_attr: instrument_type}``
    (where type is ``"counter"`` or ``"histogram"``) to patch additional
    module-level instruments.

    Returns the ``InMemoryMetricReader`` for metric assertions.
    """
    from opentelemetry.sdk.metrics import MeterProvider

    reader = InMemoryMetricReader()
    new_provider = MeterProvider(metric_readers=[reader])
    new_meter = new_provider.get_meter(obs_mod.INSTRUMENTATION_NAME, otel_mod._version())  # type: ignore[reportPrivateUsage]  # Why: testing utility reads private module state for test-scoped meter setup.

    monkeypatch.setattr(
        otel_mod,
        "_dispatch_duration",
        new_meter.create_histogram("taskq.dispatch.duration", unit="s"),
    )
    monkeypatch.setattr(
        otel_mod,
        "_consumed_messages",
        new_meter.create_counter("messaging.client.consumed.messages", unit="1"),
    )
    monkeypatch.setattr(
        otel_mod,
        "_process_duration",
        new_meter.create_histogram("messaging.process.duration", unit="s"),
    )
    monkeypatch.setattr(
        otel_mod,
        "_published_messages",
        new_meter.create_counter("messaging.client.published.messages", unit="1"),
    )
    monkeypatch.setattr(otel_mod, "get_meter", lambda: new_meter)
    monkeypatch.setattr(obs_mod, "get_meter", lambda: new_meter)

    if extra_instruments:
        for attr, kind in extra_instruments.items():
            if kind == "counter":
                monkeypatch.setattr(otel_mod, attr, new_meter.create_counter(attr, unit="1"))
            elif kind == "histogram":
                monkeypatch.setattr(otel_mod, attr, new_meter.create_histogram(attr, unit="s"))

    return reader


def _save_otel_enabled() -> bool:
    """Snapshot ``obs._otel._otel_enabled`` for later restoration."""
    return otel_mod._otel_enabled  # type: ignore[reportPrivateUsage]  # Why: testing utility snapshots private module flag for restoration.


def restore_otel_enabled(saved: bool) -> None:
    """Restore ``obs._otel._otel_enabled`` to a previously saved value."""
    otel_mod._otel_enabled = saved  # type: ignore[reportPrivateUsage]  # Why: testing utility restores private module flag from snapshot.


@pytest.fixture(autouse=True)
def _otel_enabled_guard() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture; referenced by pytest runner via reflection.
    original = otel_mod._otel_enabled  # type: ignore[reportPrivateUsage]  # Why: testing utility reads private module flag for teardown guard.
    try:
        yield
    finally:
        otel_mod._otel_enabled = original  # type: ignore[reportPrivateUsage]  # Why: testing utility restores private module flag on teardown.


def _save_logging_configured() -> bool:
    """Snapshot ``obs._structlog._logging_configured`` for later restoration."""
    return structlog_mod._logging_configured  # type: ignore[reportPrivateUsage]  # Why: testing utility snapshots private module flag for restoration.


def restore_logging_configured(saved: bool) -> None:
    """Restore ``obs._structlog._logging_configured`` to a previously saved value."""
    structlog_mod._logging_configured = saved  # type: ignore[reportPrivateUsage]  # Why: testing utility restores private module flag from snapshot.


@pytest.fixture(autouse=True)
def _logging_configured_guard() -> Generator[None, None, None]:  # pyright: ignore[reportUnusedFunction]  # Why: pytest autouse fixture; referenced by pytest runner via reflection.
    structlog.reset_defaults()
    structlog_mod._logging_configured = False  # type: ignore[reportPrivateUsage]  # Why: test fixture resets private module flag for isolation, following _otel_enabled_guard precedent.
    for handler in list(logging.root.handlers):
        if isinstance(handler, logging.StreamHandler) and isinstance(
            handler.formatter, structlog.stdlib.ProcessorFormatter
        ):
            logging.root.removeHandler(handler)  # type: ignore[reportUnknownArgumentType]  # Why: pyright cannot narrow StreamHandler generic from isinstance check on handlers list; the handler is always a valid Handler.
    original_level = logging.root.level
    try:
        yield
    finally:
        structlog.reset_defaults()
        structlog_mod._logging_configured = False  # type: ignore[reportPrivateUsage]  # Why: test fixture resets private module flag for isolation, following _otel_enabled_guard precedent.
        for handler in list(logging.root.handlers):
            if isinstance(handler, logging.StreamHandler) and isinstance(
                handler.formatter, structlog.stdlib.ProcessorFormatter
            ):
                logging.root.removeHandler(handler)  # type: ignore[reportUnknownArgumentType]  # Why: pyright cannot narrow StreamHandler generic from isinstance check on handlers list; the handler is always a valid Handler.
        logging.root.setLevel(original_level)


def collect_metrics(reader: InMemoryMetricReader) -> list[Metric]:
    """Flush the reader and return a flat list of all metrics."""
    md = reader.get_metrics_data()
    if md is None:
        return []
    results: list[Metric] = []
    for rm in md.resource_metrics:
        for sm in rm.scope_metrics:
            results.extend(sm.metrics)
    return results


def histogram_points(reader: InMemoryMetricReader, name: str) -> list[HistogramDataPoint]:
    """Return histogram data points for the metric named *name*."""
    for m in collect_metrics(reader):
        if m.name == name:
            return list(m.data.data_points)  # type: ignore[return-value]  # Why: histogram metric data_points are always HistogramDataPoint instances.
    return []


def counter_value(reader: InMemoryMetricReader, name: str) -> int:
    """Return the summed counter value for the metric named *name*.

    Returns 0 if the metric has not been recorded.
    """
    for m in collect_metrics(reader):
        if m.name == name:
            total = 0
            for p in m.data.data_points:
                if isinstance(p, NumberDataPoint):
                    total += int(p.value)
            return total
    return 0


def counter_data_points(reader: InMemoryMetricReader, name: str) -> list[NumberDataPoint]:
    """Return NumberDataPoint list for the counter named *name*."""
    for m in collect_metrics(reader):
        if m.name == name:
            return [p for p in m.data.data_points if isinstance(p, NumberDataPoint)]
    return []
