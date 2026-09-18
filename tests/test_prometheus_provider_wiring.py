"""Contract tests for ``ensure_prometheus_meter_provider`` — the auto-wiring
``create_metrics_router`` performs at router creation (process start on the
shipped ``taskq ui serve`` path).

The contract under test, per outcome:

- ``wired`` — nothing was configured: a ``PrometheusMetricReader``-backed
  ``MeterProvider`` becomes the process-global provider, so the documented
  quick-start populates the ``taskq_*`` series the shipped rules.yaml
  references.
- ``already_bridged`` — a bridge into the scraped registry already exists
  (operator-wired, or a second router creation): nothing is touched.
- ``provider_without_bridge`` — an operator SDK provider exists but no
  reader feeds the scraped registry: the provider is LEFT in place (never
  clobbered) and a WARNING names the gap, because the mounted endpoint
  would otherwise answer 200 with zero ``taskq_*`` series.
- ``provider_set_blocked`` — the OTel set-once guard was already consumed
  by a non-SDK provider: the constructed bridge is torn down (its
  collector unregistered) and a WARNING names the cause.
- ``otel_disabled`` — emission is switched off: no provider is installed.

Every scenario mutates process-global OTel state behind a set-once guard,
so each runs in a clean subprocess — the same isolation seam
tests/test_prometheus_metrics_unwired_by_default.py uses.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

pytestmark = [pytest.mark.otel]

pytest.importorskip("fastapi")
pytest.importorskip("opentelemetry.exporter.prometheus")


def _run_scenario(body: str) -> dict[str, str]:
    """Run *body* in a clean subprocess and parse its ``KEY:VALUE`` lines.

    The body prints one ``KEY:VALUE`` per line; anything else it prints is
    ignored by the parser but surfaces in the failure message.
    """
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell — the current interpreter running this file's own literal scenario bodies; no untrusted input.
        [sys.executable, "-c", body],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"subprocess failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    out: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, sep, value = line.partition(":")
        if sep and key.isupper():
            out[key] = value
    assert out, f"scenario printed no KEY:VALUE lines: {result.stdout!r}"
    return out


def test_wires_a_provider_when_nothing_is_configured() -> None:
    """The documented quick-start state (no OTEL_* env vars, no operator
    provider): the helper installs a PrometheusMetricReader-backed SDK
    provider as the process-global provider — the wiring that makes
    "mounted automatically" true of the data, not just the route."""
    out = _run_scenario(
        """
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider

from taskq.contrib.prometheus import ensure_prometheus_meter_provider

outcome = ensure_prometheus_meter_provider()
provider = metrics.get_meter_provider()
print("OUTCOME:" + outcome)
print("PROVIDER_IS_SDK:" + str(isinstance(provider, SdkMeterProvider)))
print("HAS_READERS:" + str(bool(provider._all_metric_readers)))
"""
    )
    assert out["OUTCOME"] == "wired"
    assert out["PROVIDER_IS_SDK"] == "True"
    assert out["HAS_READERS"] == "True"


def test_wired_scrape_serves_recorded_series() -> None:
    """End to end at the helper level: after ``wired``, a series recorded
    through the real obs emitter lands in the default registry's scrape —
    the pre-fix state served only Python process defaults here."""
    out = _run_scenario(
        """
from prometheus_client import REGISTRY, generate_latest

from taskq.contrib.prometheus import ensure_prometheus_meter_provider
from taskq.obs import _otel as obs

outcome = ensure_prometheus_meter_provider()
obs.record_heartbeat_miss("w1")
text = generate_latest(REGISTRY).decode()
print("OUTCOME:" + outcome)
print("SERIES_PRESENT:" + str("taskq_heartbeat_misses_total" in text))
"""
    )
    assert out["OUTCOME"] == "wired"
    assert out["SERIES_PRESENT"] == "True"


def test_never_replaces_an_operator_wired_bridge() -> None:
    """An operator who wired their own PrometheusMetricReader + provider
    before process start keeps exactly that provider — the helper is a
    no-op, and the operator's reader is the one feeding the registry."""
    out = _run_scenario(
        """
from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client import REGISTRY

from taskq.contrib.prometheus import ensure_prometheus_meter_provider

reader = PrometheusMetricReader(registry=REGISTRY)
operator_provider = MeterProvider(metric_readers=[reader])
metrics.set_meter_provider(operator_provider)

outcome = ensure_prometheus_meter_provider()
print("OUTCOME:" + outcome)
print("PROVIDER_UNTOUCHED:" + str(metrics.get_meter_provider() is operator_provider))
"""
    )
    assert out["OUTCOME"] == "already_bridged"
    assert out["PROVIDER_UNTOUCHED"] == "True"


def test_second_router_creation_is_a_noop() -> None:
    """``create_metrics_router`` is called per mount (ui serve mounts once;
    an embedding app may mount the same router shape more than once): the
    second creation must not construct a second reader on the same
    registry or attempt a second ``set_meter_provider``."""
    out = _run_scenario(
        """
from taskq.contrib.prometheus import ensure_prometheus_meter_provider

first = ensure_prometheus_meter_provider()
second = ensure_prometheus_meter_provider()
print("FIRST:" + first)
print("SECOND:" + second)
"""
    )
    assert out["FIRST"] == "wired"
    assert out["SECOND"] == "already_bridged"


def test_operator_sdk_provider_without_a_bridge_warns_and_is_left_alone() -> None:
    """An OTLP-only SDK pipeline (no PrometheusMetricReader) mounted beside
    the route is the dangerous silent shape: 200, valid text, zero
    ``taskq_*`` series. The provider is never replaced; the startup WARNING
    is the surface that says the shipped alert rules cannot fire."""
    out = _run_scenario(
        """
import structlog.testing
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskq.contrib.prometheus import ensure_prometheus_meter_provider

operator_provider = MeterProvider(metric_readers=[InMemoryMetricReader()])
metrics.set_meter_provider(operator_provider)

with structlog.testing.capture_logs() as logs:
    outcome = ensure_prometheus_meter_provider()

events = [e.get("event") for e in logs]
print("OUTCOME:" + outcome)
print("PROVIDER_UNTOUCHED:" + str(metrics.get_meter_provider() is operator_provider))
print("WARNED:" + str("prometheus-metrics-reader-missing" in events))
"""
    )
    assert out["OUTCOME"] == "provider_without_bridge"
    assert out["PROVIDER_UNTOUCHED"] == "True"
    assert out["WARNED"] == "True", "the missing-bridge startup warning must fire"


def test_set_once_blocked_by_a_non_sdk_provider_unregisters_the_orphan() -> None:
    """A non-SDK provider installed first (here: an explicit NoOp) wins
    OTel's set-once guard, so the constructed bridge would be inert —
    instruments follow the global provider, not it. The helper detects
    the shadowing, shuts its provider down (unregistering the orphaned
    collector from the registry), and warns."""
    out = _run_scenario(
        """
import structlog.testing
from opentelemetry import metrics
from opentelemetry.metrics import NoOpMeterProvider
from prometheus_client import REGISTRY

from taskq.contrib.prometheus import ensure_prometheus_meter_provider
from taskq.contrib.prometheus._metrics import _registry_has_otel_bridge

metrics.set_meter_provider(NoOpMeterProvider())

with structlog.testing.capture_logs() as logs:
    outcome = ensure_prometheus_meter_provider()

events = [e.get("event") for e in logs]
print("OUTCOME:" + outcome)
print("WARNED:" + str("prometheus-metrics-provider-blocked" in events))
print("BRIDGE_LEFT_BEHIND:" + str(_registry_has_otel_bridge(REGISTRY)))
"""
    )
    assert out["OUTCOME"] == "provider_set_blocked"
    assert out["WARNED"] == "True"
    assert out["BRIDGE_LEFT_BEHIND"] == "False", (
        "the inert bridge's collector must be unregistered — leaving it "
        "would serve an empty OTel family set indistinguishable from a "
        "wired-but-idle deployment"
    )


def test_otel_disabled_installs_no_provider() -> None:
    """With emission switched off, mounting the route installs nothing —
    the off switch governs — and logs at INFO so the empty-of-taskq_*
    scrape is attributable to configuration, not a defect."""
    out = _run_scenario(
        """
import structlog.testing
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider

from taskq.contrib.prometheus import ensure_prometheus_meter_provider
from taskq.obs import set_otel_enabled

set_otel_enabled(False)
with structlog.testing.capture_logs() as logs:
    outcome = ensure_prometheus_meter_provider()

events = [e.get("event") for e in logs]
print("OUTCOME:" + outcome)
print(
    "PROVIDER_IS_SDK:"
    + str(isinstance(metrics.get_meter_provider(), SdkMeterProvider))
)
print("LOGGED:" + str("prometheus-metrics-otel-disabled" in events))
"""
    )
    assert out["OUTCOME"] == "otel_disabled"
    assert out["PROVIDER_IS_SDK"] == "False"
    assert out["LOGGED"] == "True"


def test_bridge_detection_reads_slots_the_pinned_client_still_has() -> None:
    """``_registry_has_otel_bridge`` enumerates collectors through two
    private ``CollectorRegistry`` slots because prometheus_client has no
    public listing. A client release that renames either would make the
    detection see an empty registry, register a second bridge, and turn
    every scrape into duplicated exposition — so the slots are pinned
    against the installed client, and a bump that drops one fails here
    rather than at the scrape."""
    from prometheus_client import CollectorRegistry

    from taskq.contrib.prometheus._metrics import (
        _registry_has_otel_bridge,  # pyright: ignore[reportPrivateUsage]  # Why: the pin is about this helper's private-slot reads.
    )

    registry = CollectorRegistry()
    for slot in ("_collector_to_names", "_collectors_without_names"):
        assert hasattr(registry, slot), (
            f"prometheus_client's CollectorRegistry no longer has {slot!r}; the "
            "bridge detection in taskq.contrib.prometheus._metrics reads it"
        )
    assert not _registry_has_otel_bridge(registry)

    class _CustomCollector:
        """The bridge's collector, by module and name — what the helper matches."""

        def collect(self) -> list[object]:
            return []

    _CustomCollector.__module__ = "opentelemetry.exporter.prometheus"
    registry.register(_CustomCollector())  # pyright: ignore[reportArgumentType]  # Why: the registry only needs a collect(); the stub is the bridge collector's shape.
    assert _registry_has_otel_bridge(registry), (
        "a registered bridge collector must be visible through the pinned slots"
    )
