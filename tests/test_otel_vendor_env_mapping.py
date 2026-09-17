"""Vendor env-var mapping tests for the OTLP exporter auto-configuration.

Fast, containerless, and vendor-free: the two vendor setups documented in
``docs/guides/observability.md`` (a connection-string SaaS ingest and an
agent OTLP intake) are pinned as EXACT env-var blocks, and each block is
exercised through the same wiring the ``taskq worker`` CLI runs
(``taskq.obs.configure_exporters``). No vendor SDK exists here or may ever:
TaskQ speaks only the standard OTel environment variables and the OTLP
protocol, and these tests prove the standard path carries each vendor's
documented setup.

Two modes, per the docs' contract that the ``[otel]`` extra is optional:

- with the SDK installed (the dev environment): the block is accepted, the
  exporters are pointed at the block's endpoint, and a real export round
  trip lands on a local stub receiver at the spec'd signal paths;
- without the SDK (simulated by hiding the SDK's modules in-process): the
  worker accepts the same block without error, warns once naming the extra,
  and starts anyway.

The stub receiver is a local HTTP server standing in for the ingest
endpoint: with ``OTEL_EXPORTER_OTLP_PROTOCOL=http/protobuf`` the exporter
POSTs each signal to the base endpoint plus the spec'd path
(``/v1/traces``, ``/v1/metrics``), which is exactly the URL shape both
documented vendor setups expose.
"""

import http.server
import importlib.util
import os
import subprocess
import sys
import threading
import time
from dataclasses import dataclass

import pytest

_HAS_OTEL_SDK = importlib.util.find_spec("opentelemetry.sdk") is not None


def _find_spec(name: str) -> bool:
    """``find_spec`` that answers False instead of raising when a parent
    package is absent (``opentelemetry.exporter`` does not exist at all in
    the extras-isolation legs that install no exporter dist)."""
    try:
        return importlib.util.find_spec(name) is not None
    except ModuleNotFoundError:
        return False


# The full-mode scenarios build the real OTLP exporter inside the subprocess;
# an SDK without the exporter dist (extras-isolation legs that install the
# dev group's SDK transitively but not [otel]) fails at the entry point, so
# the guard below needs both, not the SDK alone.
_HAS_OTLP_EXPORTER = _find_spec("opentelemetry.exporter.otlp")
_FULL_MODE = _HAS_OTEL_SDK and _HAS_OTLP_EXPORTER

_CONNECTION_STRING_ENV = "APPLICATIONINSIGHTS_CONNECTION_STRING"
_AGENT_SITE_ENV = "DD_SITE"
_AGENT_KEY_ENV = "DD_API_KEY"

#: A realistic connection string for the connection-string vendor: an
#: instrumentation key GUID and a regional ingest host. The ``.invalid`` TLD
#: is deliberate; nothing may reach it, and nothing does.
_CONNECTION_STRING = (
    "InstrumentationKey=00000000-0000-0000-0000-000000000000;"
    "IngestionEndpoint=https://otelvalidation.region.in.applicationinsights.example.invalid/"
)


def _ingestion_endpoint(connection_string: str) -> str:
    """Derive the ingest base endpoint from a connection string.

    The documented operator mapping: the OTLP base endpoint is the
    connection string's ``IngestionEndpoint`` field verbatim; the SDK
    appends the per-signal paths.
    """
    fields = dict(part.split("=", 1) for part in connection_string.split(";") if "=" in part)
    return fields["IngestionEndpoint"]


# ── the local OTLP/HTTP stub receiver ────────────────────────────────────

_received_paths: list[str] = []
_received_lock = threading.Lock()


class _StubIngestHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # Why: http.server's dispatch names it.
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        with _received_lock:
            _received_paths.append(self.path)
        self.send_response(200)
        self.send_header("Content-Type", "application/x-protobuf")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        del format, args


@pytest.fixture()
def ingest_endpoint():
    """A local OTLP/HTTP stub receiver standing in for a vendor ingest host."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _StubIngestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    with _received_lock:
        _received_paths.clear()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def _poll_received(paths: tuple[str, ...], deadline_s: float = 10.0) -> list[str]:
    """Wait until every path in *paths* has been POSTed, then return all."""
    deadline = time.monotonic() + deadline_s
    while time.monotonic() < deadline:
        with _received_lock:
            snapshot = list(_received_paths)
        if all(p in snapshot for p in paths):
            return snapshot
        time.sleep(0.1)
    with _received_lock:
        snapshot = list(_received_paths)
    pytest.fail(f"stub ingest never received {paths}; got {snapshot!r}")


# ── the subprocess isolation seam ────────────────────────────────────────


@dataclass(frozen=True)
class _Settings:
    """The slice of WorkerSettings the wiring reads."""

    otel_autoconfigure: bool = True
    metrics_port: int | None = None
    health_host: str = "127.0.0.1"


def _run_scenario(env: dict[str, str]) -> str:
    """Run configure_exporters plus one span and one metric in a subprocess.

    Scenarios install process-global SDK providers behind a set-once guard,
    so they run in a clean child (the seam
    ``tests/test_otel_exporter_autoconfigure.py`` uses). The child force
    flushes both providers, so by exit the export round trip has landed on
    whatever endpoint the scenario's environment pointed at.
    """
    child_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("OTEL_", "TASKQ_", "APPLICATIONINSIGHTS_", "DD_"))
    }
    child_env.update(env)
    result = subprocess.run(  # noqa: S603  # Why: fixed argv of this file's own literal scenario, no shell.
        [sys.executable, "-c", _SCENARIO_BODY],
        capture_output=True,
        text=True,
        timeout=120,
        env=child_env,
    )
    assert result.returncode == 0, (
        f"scenario failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    outcomes = [line for line in result.stdout.splitlines() if line.startswith("OUTCOME:")]
    assert outcomes, f"scenario printed no outcome: stdout={result.stdout!r}"
    return outcomes[0].removeprefix("OUTCOME:")


def _run_sdk_missing_scenario(env: dict[str, str]) -> tuple[str, str]:
    """Run the hidden-SDK variant of a scenario in a fresh child process.

    The missing-SDK outcome requires that no provider is reachable, and other
    tests in this process legitimately install real global providers
    (set-once, never unset), so the SDK's configurator module is hidden
    before taskq is imported in the child. Returns (outcome, unavailable
    extras).
    """
    body = (
        "import sys\n"
        'sys.modules["opentelemetry.sdk._configuration"] = None\n'
        "from dataclasses import dataclass\n\n"
        "import structlog.testing\n\n"
        "from taskq.obs import configure_exporters\n\n\n"
        "@dataclass(frozen=True)\n"
        "class Settings:\n"
        "    otel_autoconfigure: bool = True\n"
        "    metrics_port: int | None = None\n"
        "    health_host: str = '127.0.0.1'\n\n\n"
        "with structlog.testing.capture_logs() as events:\n"
        "    outcome = configure_exporters(Settings())\n"
        'unavailable = [e for e in events if e["event"] == "otel-exporter-unavailable"]\n'
        'print("OUTCOME:" + outcome)\n'
        'print("UNAVAILABLE:" + ",".join(str(e.get("extra")) for e in unavailable))\n'
        'print("WARNINGS:" + str(sum(1 for e in events if e["log_level"] == "warning")))\n'
    )
    child_env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("OTEL_", "TASKQ_", "APPLICATIONINSIGHTS_", "DD_"))
    }
    child_env.update(env)
    result = subprocess.run(  # noqa: S603  # Why: fixed argv of this file's own literal scenario, no shell.
        [sys.executable, "-c", body],
        capture_output=True,
        text=True,
        timeout=120,
        env=child_env,
    )
    assert result.returncode == 0, (
        f"scenario failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    lines = dict(line.split(":", 1) for line in result.stdout.splitlines() if ":" in line)
    assert "OUTCOME" in lines, f"scenario printed no outcome: {result.stdout!r}"
    return lines["OUTCOME"], lines.get("UNAVAILABLE", "")


_SCENARIO_BODY = """\
from dataclasses import dataclass

from opentelemetry import metrics, trace

from taskq.obs import configure_exporters


@dataclass(frozen=True)
class Settings:
    otel_autoconfigure: bool = True
    metrics_port: int | None = None
    health_host: str = "127.0.0.1"


outcome = configure_exporters(Settings())
trace.get_tracer("otel-validation").start_span("probe").end()
metrics.get_meter("otel-validation").create_counter("probe.counter").add(1)
trace.get_tracer_provider().force_flush()
metrics.get_meter_provider().force_flush()
print("OUTCOME:" + outcome)
"""


# ── the connection-string vendor block ────────────────────────────────────


def test_connection_string_ingestion_endpoint_is_the_otlp_base() -> None:
    """The documented mapping: IngestionEndpoint + the spec'd signal paths.

    The operator copies the connection string's IngestionEndpoint into
    ``OTEL_EXPORTER_OTLP_ENDPOINT``; the SDK appends ``/v1/traces`` /
    ``/v1/metrics`` to that base. This pins the derivation the docs describe.
    """
    endpoint = _ingestion_endpoint(_CONNECTION_STRING)
    assert endpoint == "https://otelvalidation.region.in.applicationinsights.example.invalid/"
    # Both vendor blocks' endpoint is a BASE: the per-signal paths are the
    # exporter's job, never spelled into the variable.
    assert endpoint.rstrip("/").count("/") == 2


@pytest.mark.skipif(
    not _FULL_MODE,
    reason="opentelemetry-sdk with the OTLP exporter (the [otel] extra) is not installed; the full-mode assertions need both",
)
def test_connection_string_block_is_accepted_and_exports_to_the_ingest_paths(
    ingest_endpoint: str,
) -> None:
    """The documented connection-string block, accepted without error.

    The worker reads only the standard variables, so the block sets the
    derived endpoint beside the connection string itself (kept set, exactly
    as an operator following the docs would leave it, and asserted harmless:
    nothing in the wiring parses or rejects it). The export round trip must
    land on the block's endpoint at the spec'd per-signal paths.
    """
    outcome = _run_scenario(
        {
            _CONNECTION_STRING_ENV: _CONNECTION_STRING,
            "OTEL_EXPORTER_OTLP_ENDPOINT": ingest_endpoint,
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
            "OTEL_SERVICE_NAME": "otel-validation",
        }
    )
    assert outcome == "configured"
    received = _poll_received(("/v1/traces", "/v1/metrics"))
    assert "/v1/traces" in received
    assert "/v1/metrics" in received


@pytest.mark.skipif(
    not _FULL_MODE,
    reason="opentelemetry-sdk with the OTLP exporter (the [otel] extra) is not installed; the full-mode assertions need both",
)
def test_agent_intake_block_is_accepted_and_exports_to_the_agent_ports(
    ingest_endpoint: str,
) -> None:
    """The documented agent-intake block: DD_SITE and DD_API_KEY beside the
    standard OTLP endpoint, accepted without error.

    The agent's OTLP intake is a standard OTLP receiver on fixed ports
    (4317 gRPC, 4318 HTTP), so the block is the standard variables pointed
    at the agent; the site and API key variables configure the vendor's own
    SDK family, which TaskQ never ships, and must be accepted as inert.
    The stub stands in for the agent's HTTP intake, so the exporter's POSTs
    prove the endpoint was honoured.
    """
    outcome = _run_scenario(
        {
            _AGENT_SITE_ENV: "datadoghq.example.invalid",
            _AGENT_KEY_ENV: "not-a-real-key",
            "OTEL_EXPORTER_OTLP_ENDPOINT": ingest_endpoint,
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
            "OTEL_SERVICE_NAME": "otel-validation",
        }
    )
    assert outcome == "configured"
    received = _poll_received(("/v1/traces", "/v1/metrics"))
    assert "/v1/traces" in received
    assert "/v1/metrics" in received


# ── without the SDK: the same blocks must not fail the worker ────────────


def test_vendor_variables_alone_wire_nothing() -> None:
    """Vendor variables without a standard OTLP endpoint wire nothing.

    Neither the connection string nor the agent variables are trigger
    variables: the wiring asks for exporters only when the standard OTel
    environment asks for one. A deployment that sets the vendor variables
    and forgets the endpoint stays on the no-op proxies, exactly as the docs
    warn, rather than half-configuring.
    """
    import structlog.testing

    from taskq.obs import configure_exporters

    with structlog.testing.capture_logs() as events:
        outcome = configure_exporters(_Settings())
    assert outcome == "none"
    assert not [e for e in events if e["log_level"] == "warning"]


def test_connection_string_block_without_the_sdk_warns_and_starts() -> None:
    """The connection-string block with no ``[otel]`` extra: one warning
    naming the extra, no error, the worker starts.

    Runs in a fresh child with the SDK's configurator module hidden before
    taskq is imported: the missing-SDK outcome requires that no provider is
    reachable, and other tests in this process legitimately install real
    global providers (set-once, never unset).
    """
    outcome, unavailable = _run_sdk_missing_scenario(
        {
            _CONNECTION_STRING_ENV: _CONNECTION_STRING,
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1",
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
        }
    )
    assert outcome == "sdk_missing"
    assert unavailable == "otel"


def test_agent_intake_block_without_the_sdk_warns_and_starts() -> None:
    """The agent-intake block with no ``[otel]`` extra: same contract."""
    outcome, unavailable = _run_sdk_missing_scenario(
        {
            _AGENT_SITE_ENV: "datadoghq.example.invalid",
            _AGENT_KEY_ENV: "not-a-real-key",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4317",
        }
    )
    assert outcome == "sdk_missing"
    assert unavailable == "otel"
