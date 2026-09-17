"""Contract tests for ``taskq.obs.configure_exporters`` — the SDK exporter
wiring the ``taskq worker`` CLI performs at startup.

Before this wiring existed a stock worker exported nothing: the OTel
environment variables are read by the SDK's configurator, which only
``opentelemetry-instrument`` ran, so a container with
``OTEL_EXPORTER_OTLP_ENDPOINT`` set stayed green while every shipped
alert rule was inert.

The contract, per outcome:

- ``configured`` — the variables (or ``TASKQ_METRICS_PORT``) asked for
  exporters and the SDK is installed: real SDK providers are installed
  once and ``otel-exporter-configured`` names what was wired.
- ``sdk_missing`` — exporters were asked for but the package is missing:
  a WARNING names the extra, nothing crashes, the proxies stay.
- ``disabled`` — ``TASKQ_OTEL_AUTOCONFIGURE=false``: nothing is touched.
- ``preconfigured`` — the embedding application already set a provider:
  that exact provider stays.
- ``none`` / ``sdk_disabled`` — nothing was asked for, or the SDK is
  switched off by ``OTEL_SDK_DISABLED``.

Scenarios that install a provider mutate process-global OTel state behind
a set-once guard, so each runs in a clean subprocess (the isolation seam
``tests/test_prometheus_provider_wiring.py`` uses); the pure planning and
the no-provider outcomes run in-process.
"""

from __future__ import annotations

import socket
import subprocess
import sys
from dataclasses import dataclass

import pytest
import structlog.testing

from taskq.obs import (
    _exporter as exporter_mod,  # pyright: ignore[reportPrivateUsage]  # Why: the planner is the unit under test.
)
from taskq.obs import configure_exporters


@dataclass(frozen=True)
class _Settings:
    """The slice of WorkerSettings the wiring reads."""

    otel_autoconfigure: bool = True
    metrics_port: int | None = None
    health_host: str = "127.0.0.1"


def _run_scenario(body: str, *, env: dict[str, str] | None = None) -> dict[str, str]:
    """Run *body* in a clean subprocess and parse its ``KEY:VALUE`` lines."""
    import os

    child_env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("OTEL_") and not k.startswith("TASKQ_")
    }
    child_env.update(env or {})
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell — the current interpreter running this file's own literal scenario bodies; no untrusted input.
        [sys.executable, "-c", body],
        capture_output=True,
        text=True,
        timeout=60,
        env=child_env,
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


_PREAMBLE = """
import structlog.testing
from dataclasses import dataclass
from opentelemetry import metrics, trace
from opentelemetry.sdk.metrics import MeterProvider as SdkMeterProvider
from opentelemetry.sdk.trace import TracerProvider as SdkTracerProvider

from taskq.obs import configure_exporters


@dataclass(frozen=True)
class Settings:
    otel_autoconfigure: bool = True
    metrics_port: int | None = None
    health_host: str = "127.0.0.1"


def report(outcome, events):
    print("OUTCOME:" + outcome)
    print("TRACER_IS_SDK:" + str(isinstance(trace.get_tracer_provider(), SdkTracerProvider)))
    print("METER_IS_SDK:" + str(isinstance(metrics.get_meter_provider(), SdkMeterProvider)))
    configured = [e for e in events if e["event"] == "otel-exporter-configured"]
    print("CONFIGURED_LINES:" + str(len(configured)))
    if configured:
        line = configured[-1]
        print("TRACES:" + str(line.get("traces")))
        print("METRICS:" + str(line.get("metrics")))
        print("SOURCE:" + str(line.get("source")))
    unavailable = [e for e in events if e["event"] == "otel-exporter-unavailable"]
    print("UNAVAILABLE_EXTRAS:" + ",".join(str(e.get("extra")) for e in unavailable))
    print("WARNINGS:" + str(sum(1 for e in events if e["log_level"] == "warning")))
"""


# ── in-process: the planner ───────────────────────────────────────────


def test_endpoint_alone_selects_the_spec_default_exporters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The specification defaults traces and metrics to ``otlp``; the Python
    SDK's configurator applies no default of its own, so a bare endpoint
    would otherwise install providers with nothing attached."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")
    plan = exporter_mod._plan(_Settings(), prometheus_available=True)  # pyright: ignore[reportPrivateUsage]
    assert plan.traces == ("otlp",)
    assert plan.metrics == ("otlp",)
    assert plan.logs == ()
    assert plan.sources == ("env",)
    # Nothing came from the environment's own exporter lists, so the
    # defaults must be handed to the configurator as extras.
    assert plan.extras("traces") == ["otlp"]
    assert plan.extras("metrics") == ["otlp"]


def test_explicit_exporter_variables_are_not_duplicated_as_extras(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The configurator appends its own environment parse; an
    environment-selected exporter listed again as an extra would be built
    twice. ``none`` selects nothing and suppresses the default."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "console, otlp")
    monkeypatch.setenv("OTEL_METRICS_EXPORTER", "none")
    plan = exporter_mod._plan(_Settings(), prometheus_available=True)  # pyright: ignore[reportPrivateUsage]
    assert plan.traces == ("console", "otlp")
    assert plan.metrics == ()
    assert plan.extras("traces") == []
    assert plan.extras("metrics") == []


def test_metrics_port_adds_the_prometheus_reader_once() -> None:
    """``TASKQ_METRICS_PORT`` adds the SDK's Prometheus pull reader; when the
    operator also named it in OTEL_METRICS_EXPORTER it is not added twice,
    and without the package it is not planned at all."""
    with_port = exporter_mod._plan(_Settings(metrics_port=9464), prometheus_available=True)  # pyright: ignore[reportPrivateUsage]
    assert with_port.metrics == ("prometheus",)
    assert with_port.sources == ("prometheus",)
    assert with_port.prometheus_port == 9464

    missing = exporter_mod._plan(_Settings(metrics_port=9464), prometheus_available=False)  # pyright: ignore[reportPrivateUsage]
    assert missing.metrics == ()
    assert not missing.requested


def test_metrics_port_named_in_env_is_planned_once(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_METRICS_EXPORTER", "prometheus")
    plan = exporter_mod._plan(_Settings(metrics_port=9464), prometheus_available=True)  # pyright: ignore[reportPrivateUsage]
    assert plan.metrics == ("prometheus",)
    assert plan.extras("metrics") == []
    assert plan.sources == ("env", "prometheus")


# ── in-process: outcomes that never install a provider ───────────────


def test_nothing_requested_leaves_the_proxies_and_says_so() -> None:
    with structlog.testing.capture_logs() as events:
        outcome = configure_exporters(_Settings())
    assert outcome == "none"
    configured = [e for e in events if e["event"] == "otel-exporter-configured"]
    assert configured and configured[0]["source"] == "none"


def test_autoconfigure_off_touches_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """``TASKQ_OTEL_AUTOCONFIGURE=false`` is the opt-out: with every trigger
    set, nothing is planned, wired, or warned about."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")
    monkeypatch.setenv("OTEL_TRACES_EXPORTER", "console")
    with structlog.testing.capture_logs() as events:
        outcome = configure_exporters(_Settings(otel_autoconfigure=False, metrics_port=9464))
    assert outcome == "disabled"
    assert [e["event"] for e in events] == ["otel-exporter-autoconfigure-disabled"]


def test_sdk_disabled_is_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")
    monkeypatch.setenv("OTEL_SDK_DISABLED", "true")
    with structlog.testing.capture_logs() as events:
        outcome = configure_exporters(_Settings())
    assert outcome == "sdk_disabled"
    assert [e["event"] for e in events] == ["otel-exporter-sdk-disabled"]


def test_env_set_without_the_sdk_warns_and_names_the_extra(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The failure this wiring exists to remove must not become a crash:
    exporters asked for, SDK missing → one WARNING naming ``otel``, the
    proxies stay, the worker starts."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317")
    monkeypatch.setitem(sys.modules, "opentelemetry.sdk._configuration", None)
    with structlog.testing.capture_logs() as events:
        outcome = configure_exporters(_Settings())
    assert outcome == "sdk_missing"
    unavailable = [e for e in events if e["event"] == "otel-exporter-unavailable"]
    assert len(unavailable) == 1
    assert unavailable[0]["extra"] == "otel"
    assert unavailable[0]["log_level"] == "warning"
    assert unavailable[0]["source"] == "env"


def test_metrics_port_without_the_prometheus_extra_warns_and_names_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "opentelemetry.exporter.prometheus", None)
    with structlog.testing.capture_logs() as events:
        outcome = configure_exporters(_Settings(metrics_port=9464))
    assert outcome == "sdk_missing"
    unavailable = [e for e in events if e["event"] == "otel-exporter-unavailable"]
    assert [e["extra"] for e in unavailable] == ["prometheus"]


# ── subprocess: outcomes that install (or refuse to replace) a provider ─


def test_env_set_with_sdk_installs_providers_once_and_logs_the_wiring() -> None:
    """The headline contract: exporter variables + SDK → real SDK tracer and
    meter providers, one ``otel-exporter-configured`` line naming them, and
    a second call finds them already set rather than installing again."""
    out = _run_scenario(
        _PREAMBLE
        + """
with structlog.testing.capture_logs() as events:
    first = configure_exporters(Settings())
    second = configure_exporters(Settings())
report(first, events)
print("SECOND:" + second)
print("PRECONFIGURED_LINES:" + str(sum(1 for e in events if e["event"] == "otel-exporter-preconfigured")))
""",
        env={"OTEL_TRACES_EXPORTER": "console", "OTEL_METRICS_EXPORTER": "console"},
    )
    assert out["OUTCOME"] == "configured"
    assert out["TRACER_IS_SDK"] == "True"
    assert out["METER_IS_SDK"] == "True"
    assert out["CONFIGURED_LINES"] == "1"
    assert out["TRACES"] == "console"
    assert out["METRICS"] == "console"
    assert out["SOURCE"] == "env"
    assert out["WARNINGS"] == "0"
    assert out["SECOND"] == "preconfigured"
    assert out["PRECONFIGURED_LINES"] == "1"


def test_endpoint_alone_installs_otlp_exporters() -> None:
    """``OTEL_EXPORTER_OTLP_ENDPOINT`` alone must wire OTLP for traces and
    metrics (the spec default), not providers with nothing attached. The
    endpoint is never contacted: the process reports and exits before any
    export interval, skipping the SDK's atexit flush."""
    out = _run_scenario(
        _PREAMBLE
        + """
import os, sys
with structlog.testing.capture_logs() as events:
    outcome = configure_exporters(Settings())
report(outcome, events)
tp = trace.get_tracer_provider()
mp = metrics.get_meter_provider()
print("SPAN_PROCESSORS:" + str(len(tp._active_span_processor._span_processors)))
print("METRIC_READERS:" + str(len(mp._all_metric_readers)))
# Exit without the SDK's atexit flush: nothing was recorded, and a flush
# would only retry against the unreachable endpoint.
sys.stdout.flush()
os._exit(0)
""",
        env={
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1",
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
        },
    )
    assert out["OUTCOME"] == "configured"
    assert out["TRACES"] == "otlp"
    assert out["METRICS"] == "otlp"
    assert out["SPAN_PROCESSORS"] == "1"
    assert out["METRIC_READERS"] == "1"


def test_embedding_application_provider_is_left_untouched() -> None:
    """An application that set its own provider before the worker starts
    keeps exactly that object: the worker must never shadow it."""
    out = _run_scenario(
        _PREAMBLE
        + """
mine = SdkTracerProvider()
trace.set_tracer_provider(mine)
with structlog.testing.capture_logs() as events:
    outcome = configure_exporters(Settings())
report(outcome, events)
print("SAME_TRACER_PROVIDER:" + str(trace.get_tracer_provider() is mine))
""",
        env={"OTEL_TRACES_EXPORTER": "console"},
    )
    assert out["OUTCOME"] == "preconfigured"
    assert out["SAME_TRACER_PROVIDER"] == "True"
    assert out["METER_IS_SDK"] == "False"
    assert out["CONFIGURED_LINES"] == "0"


def test_metrics_port_serves_the_worker_series_over_http() -> None:
    """``TASKQ_METRICS_PORT`` + the prometheus extra: the installed meter
    provider carries a Prometheus reader bound on the port, and a series
    recorded through the real obs emitter is served by the scrape."""
    pytest.importorskip("opentelemetry.exporter.prometheus")
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    out = _run_scenario(
        _PREAMBLE
        + f"""
import urllib.request
with structlog.testing.capture_logs() as events:
    outcome = configure_exporters(Settings(metrics_port={port}, health_host="127.0.0.1"))
report(outcome, events)
line = [e for e in events if e["event"] == "otel-exporter-configured"][-1]
print("PORT:" + str(line.get("prometheus_port")))
print("HOST:" + str(line.get("prometheus_host")))
from taskq.obs import _otel as obs
obs.record_heartbeat_miss("w1")
text = urllib.request.urlopen("http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
print("SERIES_PRESENT:" + str("taskq_heartbeat_misses_total" in text))
""",
    )
    assert out["OUTCOME"] == "configured"
    assert out["METRICS"] == "prometheus"
    assert out["SOURCE"] == "prometheus"
    assert out["PORT"] == str(port)
    assert out["HOST"] == "127.0.0.1"
    assert out["SERIES_PRESENT"] == "True"


def test_unknown_exporter_fails_startup_loudly() -> None:
    """A misspelled exporter is a configuration error the operator must see
    at deploy time, not a worker that runs and exports nothing."""
    out = _run_scenario(
        _PREAMBLE
        + """
from taskq.obs import OtelExporterConfigurationError
try:
    configure_exporters(Settings())
except OtelExporterConfigurationError as exc:
    print("RAISED:" + type(exc).__name__)
    print("NAMES_VARIABLE:" + str("OTEL_TRACES_EXPORTER" in str(exc)))
""",
        env={"OTEL_TRACES_EXPORTER": "no_such_exporter"},
    )
    assert out["RAISED"] == "OtelExporterConfigurationError"
    assert out["NAMES_VARIABLE"] == "True"


# ── the CLI seam ──────────────────────────────────────────────────────


def test_worker_cli_configures_exporters_before_worker_main(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``taskq worker`` wires the exporters from the loaded settings before
    ``worker_main`` records anything, and a configuration error exits 1
    with the message instead of a traceback."""
    from typer.testing import CliRunner

    from taskq.cli import app
    from taskq.obs import OtelExporterConfigurationError

    order: list[str] = []
    seen: list[object] = []

    def fake_configure(settings: object) -> str:
        order.append("configure")
        seen.append(settings)
        return "none"

    def fake_worker_main(settings: object, **kwargs: object) -> int:
        order.append("worker_main")
        return 0

    monkeypatch.setattr("taskq.cli.configure_exporters", fake_configure)
    monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)
    result = CliRunner().invoke(app, ["worker", "--actors", "tests.test_cli_worker:_REGISTRY"])
    assert result.exit_code == 0, result.output
    assert order == ["configure", "worker_main"]
    assert getattr(seen[0], "otel_autoconfigure", None) is True

    def failing_configure(settings: object) -> str:
        raise OtelExporterConfigurationError("bad exporter: no_such_exporter")

    monkeypatch.setattr("taskq.cli.configure_exporters", failing_configure)
    result = CliRunner().invoke(app, ["worker", "--actors", "tests.test_cli_worker:_REGISTRY"])
    assert result.exit_code == 1
    assert "no_such_exporter" in result.output
