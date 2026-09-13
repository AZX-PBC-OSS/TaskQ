"""Pins the OpenTelemetry surface TaskQ's floors are chosen against.

The floors in pyproject.toml (`opentelemetry-api>=1.42.0` and the matching
`otel` extra) are a measured boundary, not a guess: 1.42.0 is the lowest
version the suite ran green on. These tests fail loudly if a version inside
that range stops providing what TaskQ relies on, instead of letting it surface
as an ImportError or an AttributeError somewhere deep in a metrics assertion.

They exist mainly because of one seam. `src/taskq/testing/otel.py` used to
import `HistogramDataPoint` and `NumberDataPoint` from
`opentelemetry.sdk.metrics._internal.point`, which carries no stability
guarantee at all: a private module can be renamed in any release, including a
patch. Both names turn out to be re-exported from the public
`opentelemetry.sdk.metrics.export` (verified identical objects, and listed in
its `__all__`, on 1.42.0, 1.43.0 and 1.44.0), so the import moved there and the
private dependency is gone. What is left to defend is the shape of the two
dataclasses, which the public path does not promise field-by-field.
"""

import dataclasses

import pytest

pytest.importorskip("opentelemetry.sdk")

# Import follows the importorskip guard above deliberately.
from opentelemetry.sdk.metrics.export import (
    HistogramDataPoint,
    NumberDataPoint,
)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import taskq.obs._otel as otel_mod

# The fields TaskQ actually reads, not the full dataclass. A future release may
# add fields freely; removing or renaming one of these is what breaks us.
_NUMBER_FIELDS_USED = frozenset({"attributes", "value"})
_HISTOGRAM_FIELDS_USED = frozenset(
    {"attributes", "count", "sum", "bucket_counts", "explicit_bounds"}
)


def test_data_points_are_importable_from_the_public_module() -> None:
    """Neither name may drift back to a private module.

    `opentelemetry.sdk.metrics.export` is the supported path. If a future
    release drops these from it, the fix is a narrow shim with a clear message,
    not a quiet reach back into `_internal`.
    """
    from opentelemetry.sdk.metrics import export

    assert "NumberDataPoint" in export.__all__
    assert "HistogramDataPoint" in export.__all__


def test_number_data_point_keeps_the_fields_taskq_reads() -> None:
    """`counter_value` and `counter_data_points` read `.value` and `.attributes`."""
    present = {f.name for f in dataclasses.fields(NumberDataPoint)}
    missing = _NUMBER_FIELDS_USED - present
    assert not missing, (
        f"NumberDataPoint no longer provides {sorted(missing)}. "
        "src/taskq/testing/otel.py reads these; adjust it and the floor together."
    )


def test_histogram_data_point_keeps_the_fields_taskq_reads() -> None:
    """`histogram_points` hands these straight to callers asserting on them."""
    present = {f.name for f in dataclasses.fields(HistogramDataPoint)}
    missing = _HISTOGRAM_FIELDS_USED - present
    assert not missing, (
        f"HistogramDataPoint no longer provides {sorted(missing)}. "
        "src/taskq/testing/otel.py returns these; adjust it and the floor together."
    )


def test_prometheus_reader_accepts_the_registry_kwarg() -> None:
    """The reason the floor is 1.42.0 and not lower.

    `opentelemetry-exporter-prometheus` 0.62b0 hard-codes the global REGISTRY
    (opentelemetry-python #5055), so isolated metric scrapes are impossible.
    0.63b0 added the public `registry=` kwarg and requires
    `opentelemetry-sdk~=1.42.0`, which is what pins the whole floor set to
    1.42.0. Measured: on 0.62b0 this suite produces 7 TypeErrors in
    tests/test_prometheus_metrics.py.
    """
    import inspect

    pytest.importorskip("opentelemetry.exporter.prometheus")
    from opentelemetry.exporter.prometheus import PrometheusMetricReader

    assert "registry" in inspect.signature(PrometheusMetricReader.__init__).parameters


# ── The memoized-tracer contract ───────────────────────────────────────
#
# ``get_tracer()`` used to call ``importlib.metadata.version`` per span
# (~320µs, benchmarks/ab_otel_hotspots.py); it now resolves the tracer
# once per process. The pins below hold the two halves of that design:
# the memo actually memoizes, and — the part that makes memoization
# legal — a tracer resolved BEFORE an SDK registers still rebinds to the
# real one afterwards (the API's ProxyTracer re-checks the global
# provider on every span start, opentelemetry/trace/__init__.py
# ``ProxyTracer._tracer``). Memoization must never pin the proxy/no-op
# behavior into a worker that configures its SDK later.


def test_get_tracer_resolves_once_and_memoizes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two ``get_tracer()`` calls hit ``trace.get_tracer`` exactly once and
    return the same object — the memo is the whole optimization."""
    monkeypatch.setattr(otel_mod, "_library_tracer", None)  # pyright: ignore[reportPrivateUsage]  # Why: reset the memo so this test observes its own resolution, not one an earlier test warmed.

    import opentelemetry.trace as trace_api

    calls: list[tuple[str, str]] = []
    real_get_tracer = trace_api.get_tracer

    def counting_get_tracer(name: str, version: str | None = None, **kwargs: object):  # type: ignore[no-untyped-def]
        calls.append((name, version or ""))
        return real_get_tracer(name, version, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(trace_api, "get_tracer", counting_get_tracer)

    first = otel_mod.get_tracer()
    second = otel_mod.get_tracer()

    assert first is second
    assert calls == [(otel_mod.INSTRUMENTATION_NAME, otel_mod._version())]


def test_memoized_tracer_rebinds_after_sdk_registration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The transition contract: a tracer memoized with NO provider yields
    non-recording spans, and the SAME memoized object yields recording
    spans once a real provider registers. If memoization ever swaps the
    ProxyTracer for something that pins the no-op behavior, a worker that
    configures its SDK after first span goes silently dark — this pin is
    what makes the memoization safe to keep."""
    import opentelemetry.trace as trace_api

    # Force the no-provider path for the resolution (and restore whatever
    # the process had afterwards — never set the global for real).
    monkeypatch.setattr(trace_api, "_TRACER_PROVIDER", None)  # pyright: ignore[reportPrivateUsage]
    monkeypatch.setattr(otel_mod, "_library_tracer", None)  # pyright: ignore[reportPrivateUsage]

    tracer = otel_mod.get_tracer()
    pre = tracer.start_span("pre-registration")
    assert not pre.is_recording()  # Why: a method on the OTel Span API, not a property.
    pre.end()

    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(trace_api, "_TRACER_PROVIDER", provider)  # pyright: ignore[reportPrivateUsage]

    # The SAME memoized object, now backed by the real provider.
    assert otel_mod.get_tracer() is tracer
    post = tracer.start_span("post-registration")
    assert post.is_recording()
    post.end()
    assert {s.name for s in exporter.get_finished_spans()} == {"post-registration"}
