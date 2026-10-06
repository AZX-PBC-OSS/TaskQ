"""The ``[prometheus]`` extra's contract, proven under ONLY its dependencies.

The CI test-extras leg for this extra syncs ``--extra prometheus`` (plus
fastapi, which the router suites need), but THIS module deliberately
imports nothing beyond what the extra itself ships —
``opentelemetry-exporter-prometheus`` and ``prometheus-client`` (which
brings the OTel SDK) — plus taskq. That is the honesty pin for the leg:
if the extra's own surface (the OTel→Prometheus bridge that turns
``taskq.obs``' instruments into the exposition the metrics endpoint
serves) breaks under a dependency change, this module reds even though
the other suites in the leg could mask it behind their fastapi imports.

The meter isolation follows the sanctioned pattern from
``tests/test_otel_contract.py``'s ``meter_reader`` fixture: the module
singletons are re-pointed at instruments on a private ``MeterProvider``
wired to a ``PrometheusMetricReader`` — the ``registry=`` kwarg whose
floor the extra's pyproject pin documents — and the global provider is
never touched.
"""

from __future__ import annotations

import pytest

pytest.importorskip("opentelemetry.exporter.prometheus")

pytestmark = [pytest.mark.prometheus]


def test_the_prometheus_extra_renders_a_taskq_emit_site_into_the_exposition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real ``taskq.obs`` emit site reaches the Prometheus text exposition.

    Imports ONLY the [prometheus] extra's dependencies. Drives
    ``obs.record_heartbeat_miss`` — an actual production emit site —
    through its real gate, then asserts the scrape renders the
    documented ``taskq_heartbeat_misses_total`` series.
    """
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.sdk.metrics import MeterProvider
    from prometheus_client import CollectorRegistry, generate_latest

    import taskq.obs._otel as otel_mod

    registry = CollectorRegistry()
    # Hold the reader in a local: the SDK's collect path does not keep the
    # reader alive on its own, and a garbage-collected reader collects
    # NOTHING (silently empty exposition — measured: an inline
    # ``MeterProvider(metric_readers=[PrometheusMetricReader(registry)])``
    # yields ``b""``). test_prometheus_metrics.py's _PromEnv holds it as an
    # attribute for the same reason.
    reader = PrometheusMetricReader(registry=registry)
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter(otel_mod.INSTRUMENTATION_NAME, otel_mod._version())
    counter = meter.create_counter(
        "taskq.heartbeat.misses",  # the name taskq/obs/_otel.py binds
        description="Heartbeat renewal failures.",
        unit="1",
    )
    monkeypatch.setattr(otel_mod, "_heartbeat_misses", counter)
    otel_mod.set_otel_enabled(True)

    otel_mod.record_heartbeat_miss("contract-probe-worker")

    exposition = generate_latest(registry).decode("utf-8")
    assert "taskq_heartbeat_misses_total" in exposition, (
        "the [prometheus] extra's bridge dropped a taskq emit site: the "
        "OTel instrument recorded through the real emit path never reached "
        "the registry's exposition"
    )
    del counter, meter
    provider.shutdown()
