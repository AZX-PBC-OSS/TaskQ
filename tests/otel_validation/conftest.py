"""Fixtures for the OTLP validation lane (``make test-otel``).

One collector container per module (the tests are serial, so there is never
more than one), pinned to a recent collector-contrib release and configured
with the exact topology the vendor OTLP intake ports expose: an OTLP gRPC
receiver on 4317 and traces + metrics + logs pipelines. The exporter is the
collector's ``file`` exporter writing one JSON line per export request, which
is the output the tests poll and assert on; a real vendor backend ingests the
same protocol from the same receiver, only with a different exporter attached.

Postgres comes from the shared integration harness (``pg_dsn`` /
``module_pg_schema`` via ``tests/conftest.py``), not a second container: the
lane's variable is the telemetry path, not the storage backend, and the
shared pair is what every other containerized tier of this suite already
starts.

The container writes its export file through a bind mount, so the mounted
directory is created world-writable: the collector image runs as an
unprivileged uid that cannot write a user-owned pytest tmp directory.
"""

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from taskq.testing._shared_containers import skip_test_without_docker

#: The collector release this lane is validated against. Pinned by tag: the
#: file exporter's output shape and the pipeline config schema are what the
#: assertions read, and both have changed across minor releases.
COLLECTOR_IMAGE = "otel/opentelemetry-collector-contrib:0.161.0"

#: Receiver on the spec'd gRPC port, all three signals exported as JSON lines.
_COLLECTOR_CONFIG = """\
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
exporters:
  file:
    path: /out/telemetry.json
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [file]
    metrics:
      receivers: [otlp]
      exporters: [file]
    logs:
      receivers: [otlp]
      exporters: [file]
"""


class Collector:
    """The running collector's host-reachable endpoints."""

    def __init__(self, endpoint: str, export_file: Path) -> None:
        self.endpoint = endpoint
        """The OTLP gRPC base URL the worker's exporter should be pointed at."""
        self.export_file = export_file
        """The mounted file the collector's file exporter appends JSON lines to."""


@pytest.fixture(scope="module")
def otel_collector(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Collector]:
    """Start the collector container and yield its endpoints.

    Skips the module with a reason (never an error) when the Docker daemon is
    unreachable, per the suite's skip-without-docker discipline.
    """
    skip_test_without_docker()

    from testcontainers.core.container import DockerContainer

    conf_dir = tmp_path_factory.mktemp("collector-conf")
    out_dir = tmp_path_factory.mktemp("collector-out")
    (conf_dir / "otelcol.yaml").write_text(_COLLECTOR_CONFIG, encoding="utf-8")
    # The collector image runs as an unprivileged uid; a user-owned tmp
    # directory denies it the create. World-writable is fine: the file is
    # this test's own scratch output on this machine.
    os.chmod(out_dir, 0o777)  # noqa: S103  # Why: the container's unprivileged uid needs the write bit; the tree holds this test's own scratch output only.

    container = DockerContainer(COLLECTOR_IMAGE)
    container.with_volume_mapping(str(conf_dir / "otelcol.yaml"), "/conf/otelcol.yaml", mode="ro")
    container.with_volume_mapping(str(out_dir), "/out", mode="rw")
    container.with_exposed_ports(4317)
    container.with_command("--config=/conf/otelcol.yaml")
    container.start()
    try:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(4317))
        yield Collector(endpoint=f"http://{host}:{port}", export_file=out_dir / "telemetry.json")
    finally:
        container.stop(force=True)
