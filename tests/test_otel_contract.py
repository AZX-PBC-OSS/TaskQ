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
