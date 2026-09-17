"""OTLP round-trip validation: worker exporter wiring against a real collector.

The lane proves TaskQ's OTLP export path end to end, the way a vendor
backend ingests it: a real ``taskq worker`` process (the CLI, so
``configure_exporters`` runs the production wiring), configured ONLY through
the standard OTel environment variables, exports spans and metrics over gRPC
to a real collector container, and the assertions read the collector's
exported file output. Nothing here touches TaskQ's test doubles: the jobs run
through real Postgres dispatch, real failure handlers, real SDK processors.

What is asserted, per the release gate:

- the ``dispatch`` span arrives with the expected span name and resource
  attributes (``service.name`` from ``OTEL_SERVICE_NAME``),
- the attempt-failure counter arrives as a metrics datapoint with the
  expected instrument name and dimensions,
- the succeeding job's consumer span arrives too,
- and no credential material leaks: the failing actor's message carries a
  password-shaped DSN, and the exported telemetry must hold the masked form,
  never the password.

The same three signals on the same receiver are what every OTLP-ingesting
backend consumes, so a green run here is the protocol proof for the vendor
setups documented in ``docs/guides/observability.md``.
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from taskq.testing.pg import seed_actors

pytestmark = [
    pytest.mark.integration,
    pytest.mark.otel_validation,
]

_ACTORS_PATH = "tests.otel_validation.otel_roundtrip_actors:_REGISTRY"

_SERVICE_NAME = "taskq-otel-validation"

#: The credential seeded into the failing actor's message. The masked form is
#: what the redaction contract promises on every exported surface.
_SEED_PASSWORD = "sup3r-sekret"
_MASKED_DSN_TAIL = "***@db.internal"

_EXPORT_DEADLINE_S = 60.0
_POLL_INTERVAL_S = 0.5

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _flatten_attributes(raw: list[dict[str, Any]]) -> dict[str, Any]:
    """Flatten an OTLP-JSON attribute list into a plain dict."""
    flat: dict[str, Any] = {}
    for attribute in raw:
        value = attribute.get("value", {})
        flat[attribute["key"]] = next(iter(value.values())) if value else None
    return flat


def _read_export(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    """Parse the collector's export file into (spans, metrics, raw text).

    The file exporter writes one JSON object per export request: trace
    requests carry ``resourceSpans``, metric requests ``resourceMetrics``.
    A partially written last line is skipped; the next poll re-reads it.
    """
    spans: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    text = ""
    if not path.exists():
        return spans, metrics, text
    text = path.read_text(encoding="utf-8", errors="replace")
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        for resource_spans in record.get("resourceSpans", []):
            resource = _flatten_attributes(resource_spans.get("resource", {}).get("attributes", []))
            for scope_spans in resource_spans.get("scopeSpans", []):
                for span in scope_spans.get("spans", []):
                    spans.append({"resource": resource, "span": span})
        for resource_metrics in record.get("resourceMetrics", []):
            for scope_metrics in resource_metrics.get("scopeMetrics", []):
                metrics.extend(scope_metrics.get("metrics", []))
    return spans, metrics, text


def _poll_for_export(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    """Poll the export file with a deadline until every expected signal lands."""
    deadline = time.monotonic() + _EXPORT_DEADLINE_S
    spans: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    text = ""
    while time.monotonic() < deadline:
        spans, metrics, text = _read_export(path)
        names = {entry["span"].get("name") for entry in spans}
        metric_names = {metric.get("name") for metric in metrics}
        if (
            "dispatch" in names
            and "process otel_roundtrip_ok" in names
            and "process otel_roundtrip_fail" in names
            and "taskq.jobs.attempt_failures" in metric_names
        ):
            return spans, metrics, text
        time.sleep(_POLL_INTERVAL_S)
    pytest.fail(
        "collector export output never contained the expected signals within "
        f"{_EXPORT_DEADLINE_S}s: spans={sorted(n for n in (e['span'].get('name') for e in spans) if n)!r} "
        f"metrics={sorted(n for n in (m.get('name') for m in metrics) if n)!r}"
    )


def _run_worker(endpoint: str, pg_dsn: str, schema_name: str, cwd: Path) -> None:
    """Run the real worker CLI against the collector and the shared Postgres.

    The ONLY configuration the subprocess gets is the standard OTel
    environment variables, exactly what the documented vendor setups set:
    the worker's own auto-configuration (wp5: the CLI installs SDK exporters
    from the environment before ``worker_main`` records anything) must do the
    rest. ``--until-idle`` makes the worker exit once both jobs are done, and
    the SDK's atexit handlers flush the signals on the way out.
    """
    env = {key: value for key, value in os.environ.items() if not key.startswith("OTEL_")}
    env.update(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema_name,
            "OTEL_EXPORTER_OTLP_ENDPOINT": endpoint,
            "OTEL_SERVICE_NAME": _SERVICE_NAME,
            "OTEL_RESOURCE_ATTRIBUTES": "deployment.environment=otel-validation",
            # Fast pipelines for the test: the metric reader exports every
            # second, the span processor every 200ms. The atexit flushes are
            # the backstop, not the mechanism.
            "OTEL_METRIC_EXPORT_INTERVAL": "1000",
            "OTEL_BSP_SCHEDULE_DELAY": "200",
            "PYTHONUNBUFFERED": "1",
        }
    )
    result = subprocess.run(  # noqa: S603  # Why: fixed argv built from this test's own values, no shell.
        [
            sys.executable,
            "-m",
            "taskq",
            "worker",
            "--actors",
            _ACTORS_PATH,
            "--until-idle",
            "--idle-max-runtime",
            "120",
        ],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )
    # Exit codes in until_idle mode: 0 all succeeded, 3 some failed. Both are
    # the expected outcome here (one actor is designed to fail terminally);
    # anything else is a worker that never drained cleanly.
    assert result.returncode in (0, 3), (
        f"worker exited {result.returncode}\nstdout tail:\n{result.stdout[-2000:]}\n"
        f"stderr tail:\n{result.stderr[-2000:]}"
    )


async def test_worker_exports_dispatch_span_attempt_failure_metric_and_masks_credentials(
    otel_collector: Any,
    module_pg_schema: Any,
) -> None:
    """One worker, one failing job, one succeeding job, through a real collector.

    Enqueue both jobs first, then run the worker with ``--until-idle`` so the
    subprocess drains both and exits; the collector's export file must end up
    holding the dispatch span under the configured service name, the
    attempt-failure counter for the failed actor, the succeeding consumer
    span, and the masked form of the seeded credential.
    """
    import asyncpg

    from taskq import TaskQ
    from tests.otel_validation.otel_roundtrip_actors import Note, fail_job, ok_job

    pg_dsn = module_pg_schema.pg_dsn
    schema_name = module_pg_schema.schema_name

    conn = await asyncpg.connect(pg_dsn)
    try:
        await seed_actors(conn, schema_name, actors=("otel_roundtrip_ok", "otel_roundtrip_fail"))
    finally:
        await conn.close()

    async with TaskQ(dsn=pg_dsn, schema=schema_name) as tq:
        await tq.enqueue(fail_job, Note(text="first attempt"))
        await tq.enqueue(ok_job, Note(text="second attempt"))

    _run_worker(
        endpoint=otel_collector.endpoint,
        pg_dsn=pg_dsn,
        schema_name=schema_name,
        cwd=_REPO_ROOT,
    )

    spans, metrics, text = _poll_for_export(otel_collector.export_file)

    # The dispatch span arrived, under the resource the environment named.
    dispatch_entries = [e for e in spans if e["span"].get("name") == "dispatch"]
    assert dispatch_entries, "no dispatch span in the collector's export output"
    exported_services = {e["resource"].get("service.name") for e in spans}
    assert _SERVICE_NAME in exported_services, (
        f"service.name from OTEL_SERVICE_NAME missing from exported resources: "
        f"{exported_services!r}"
    )

    # Both consumer spans arrived: one failure, one success.
    assert any(e["span"].get("name") == "process otel_roundtrip_fail" for e in spans)
    assert any(e["span"].get("name") == "process otel_roundtrip_ok" for e in spans)

    # The attempt-failure counter arrived as a metrics datapoint, dimensioned
    # by the failed actor and the terminal retryable decision.
    failure_metrics = [m for m in metrics if m.get("name") == "taskq.jobs.attempt_failures"]
    assert failure_metrics, "taskq.jobs.attempt_failures never reached the collector"
    datapoints = [
        dp for metric in failure_metrics for dp in metric.get("sum", {}).get("dataPoints", [])
    ]
    assert datapoints, "attempt-failure counter exported no datapoints"
    failed_actor_points = [
        dp
        for dp in datapoints
        if _flatten_attributes(dp.get("attributes", [])).get("actor") == "otel_roundtrip_fail"
    ]
    assert failed_actor_points, (
        f"no attempt-failure datapoint attributed to the failed actor: "
        f"{[_flatten_attributes(dp.get('attributes', [])) for dp in datapoints]!r}"
    )
    assert all(dp.get("asInt") not in (None, "0", 0) for dp in failed_actor_points), (
        "attempt-failure datapoint counted zero failures"
    )
    assert any(
        _flatten_attributes(dp.get("attributes", [])).get("retryable") == "false"
        for dp in failed_actor_points
    ), "no terminal (retryable=false) attempt-failure datapoint exported"

    # No credential material leaked: the password is nowhere in the export,
    # and the masked form is present where the failure text was rendered.
    assert _SEED_PASSWORD not in text, "the seeded credential leaked into the export"
    assert _MASKED_DSN_TAIL in text, (
        "the failure text reached the export without its credential masked"
    )
