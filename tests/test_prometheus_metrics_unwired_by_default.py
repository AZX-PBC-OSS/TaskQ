"""Pins: the documented "mounted automatically" claim for the Prometheus
metrics endpoint covers the DATA, not just the route.

``docs/guides/admin-ui.md`` §"Health routes" says:

    "The Prometheus metrics endpoint is mounted automatically when
    taskq[prometheus] is installed."

An adopter reads that, installs ``taskq-py[prometheus]``, runs
``taskq ui serve`` exactly as the guide shows, points Prometheus at
``GET /jobs/health/metrics``, and imports the 17-rule alert set that
``docs/guides/observability.md`` §"Ready-made alert rules" tells them to
"import ... instead of writing from scratch". Every one of those alert
expressions (see ``src/taskq/contrib/prometheus/rules.yaml``) references
a ``taskq_*`` / ``messaging_*`` series.

The failure mode these tests pin against regression: the endpoint
answering 200 with valid Prometheus text while serving ZERO ``taskq_*``
series — no error, no warning log, no doctor finding, only absent time
series, so an operator believes they have alerting and has none.

The contract now held:

1. ``create_metrics_router`` — the exact call ``taskq ui serve`` makes
   (src/taskq/cli.py) — wires a ``PrometheusMetricReader``-backed
   ``MeterProvider`` itself at router creation when nothing is
   configured, so the scrape of the documented endpoint contains the
   series real ``taskq.obs`` ``record_*`` calls emit (the second test
   below, run in a clean subprocess). The wiring never replaces an
   operator-configured provider; when an operator's SDK provider has no
   Prometheus bridge into the scraped registry, router creation logs a
   WARNING naming the gap (the startup surface that says the alert rules
   cannot fire). See
   ``src/taskq/contrib/prometheus/_metrics.py::ensure_prometheus_meter_provider``.
2. docs/ shows the wiring by name (the first test below), so an operator
   who configures their own provider knows the reader is theirs to add.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

pytestmark = [pytest.mark.otel]

pytest.importorskip("fastapi")
pytest.importorskip("opentelemetry.exporter.prometheus")


def test_docs_never_show_the_prometheus_metric_reader_wiring_step() -> None:
    """The "mounted automatically" claim in admin-ui.md has no accompanying
    wiring instructions anywhere in docs/, even though the module that
    implements it says in its own docstring that wiring is mandatory and
    caller-owned (src/taskq/contrib/prometheus/_metrics.py:4-5).

    This is the doc-contract gap directly: if this test ever goes green,
    someone added the missing wiring instructions to docs/ and the metrics
    endpoint's "automatic" claim stopped being misleading.
    """
    result = subprocess.run(
        ["grep", "-rl", "PrometheusMetricReader", "docs/"],  # noqa: S607  # Why: grep resolved from PATH, as elsewhere in this suite; fixed literal argv, no shell.
        capture_output=True,
        text=True,
        cwd=".",
    )
    found_in_docs = result.stdout.strip().splitlines()
    assert found_in_docs, (
        "Expected docs/ to show how to wire a PrometheusMetricReader before "
        "process start (the mandatory step named in "
        "src/taskq/contrib/prometheus/_metrics.py's own docstring), given "
        "that docs/guides/admin-ui.md claims the metrics endpoint is "
        '"mounted automatically when taskq[prometheus] is installed" and '
        "docs/guides/observability.md tells operators to import the shipped "
        "17-rule alert set that depends entirely on taskq_* series only "
        "that wiring step produces. Found no such instructions."
    )


def test_metrics_endpoint_should_serve_taskq_series_under_documented_setup() -> None:
    """Pins the behaviour TaskQ's docs promise: under the documented
    no-extra-steps setup, the mounted endpoint serves the ``taskq_*`` /
    ``messaging_*`` series the shipped rules.yaml depends on.

    ``docs/guides/admin-ui.md`` ("Prometheus metrics endpoint is mounted
    automatically when taskq[prometheus] is installed") and
    ``docs/guides/observability.md``'s "Ready-made alert rules" section
    (which tells an operator to import ``rules.yaml`` "instead of writing
    from scratch") together promise a working scrape target once the
    ``[prometheus]`` extra is installed. This mounts the router exactly as
    `taskq ui serve` does (src/taskq/cli.py: `create_metrics_router(None)`,
    no `registry=` override) in a subprocess with NO OTel env vars set —
    the default state after `pip install taskq-py[prometheus]` per
    docs/guides/admin-ui.md's Docker Compose example, which sets only
    TASKQ_PG_DSN / TASKQ_REDIS_URL / TASKQ_ADMIN_HOST / TASKQ_ADMIN_PORT
    and nothing OTel-related.

    The subprocess models the shipped serve path's real boot ORDER: router
    creation happens at process start (``taskq ui serve`` builds its
    routers inside the FastAPI lifespan, before uvicorn accepts a
    connection), and worker activity is recorded afterwards. The order is
    load-bearing, not incidental: OTel's proxy instruments DROP
    measurements recorded before a provider exists (they rebind on
    ``set_meter_provider`` without replaying), so a scrape can only ever
    contain series recorded after startup — which is exactly why the
    wiring lives in router creation and not at first scrape. A regression
    that removes the auto-wiring turns this red: the scrape returns 200
    with valid Prometheus text and none of the four series — the original
    silent-failure shape.
    """
    # Run in a clean subprocess: the module-level instruments in
    # taskq.obs._otel bind to whichever MeterProvider is active at first
    # import (get_meter() called at module load, per obs/_otel.py), and
    # create_metrics_router's auto-wiring sets the process-GLOBAL provider
    # behind OTel's set-once guard — neither may share process state with
    # anything a prior test in this suite configured.
    script = """
from fastapi import FastAPI
from fastapi.testclient import TestClient

# No PrometheusMetricReader constructed by hand. No OTEL_* env vars set.
# This is exactly the state `taskq ui serve` boots into per the documented
# quick-start (docs/guides/admin-ui.md Docker Compose example).
from taskq.obs import _otel as obs

from taskq.contrib.prometheus import create_metrics_router

# Exactly the ui-serve call in src/taskq/cli.py — create_metrics_router(None),
# no registry override. Router creation is PROCESS START on the shipped
# serve path (the FastAPI lifespan), before any request is served or any
# taskq activity records; the provider auto-wiring happens here.
router = create_metrics_router(None)
app = FastAPI()
app.include_router(router, prefix="/jobs/health")
client = TestClient(app)

# Real worker activity, recorded the way the worker records it — after
# startup, the only window a scrape can ever contain (pre-provider proxy
# measurements are dropped by OTel, by design).
obs.record_published_message("compile_digest", "digests")
obs.record_consumed_message("compile_digest", "digests", outcome="succeeded")
obs.record_dispatch_duration("digests", 0.01)
obs.record_heartbeat_miss("w1")

resp = client.get("/jobs/health/metrics")
assert resp.status_code == 200, resp.status_code
text = resp.text

missing = [
    name for name in (
        "messaging_client_published_messages_total",
        "messaging_client_consumed_messages_total",
        "taskq_dispatch_duration_seconds",
        "taskq_heartbeat_misses_total",
    )
    if name not in text
]
# Print for the parent process to assert on.
print("MISSING:" + ",".join(missing))
print("SCRAPE_LEN:" + str(len(text)))
"""
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell — the current interpreter running this file's own literal script; no untrusted input.
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"subprocess failed: stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    missing_line = next(
        (line for line in result.stdout.splitlines() if line.startswith("MISSING:")), None
    )
    assert missing_line is not None, f"unexpected subprocess output: {result.stdout!r}"
    missing = [n for n in missing_line.removeprefix("MISSING:").split(",") if n]

    # The contract this pins: a scrape taken under exactly the setup
    # docs/guides/admin-ui.md describes as producing an automatically
    # mounted, working metrics endpoint must contain the series real
    # obs.record_* calls just emitted.
    assert not missing, (
        f"Expected the documented 'mounted automatically' Prometheus endpoint "
        f"to serve these series after real taskq.obs.record_* calls fired, "
        f"matching docs/guides/admin-ui.md's claim and docs/guides/"
        f"observability.md's instruction to import rules.yaml as-is. Instead "
        f"they were silently absent from the scrape: missing={missing!r}. "
        f"The endpoint returned 200 with valid Prometheus text throughout -- "
        f"there is no error or warning anywhere that would tell an adopter "
        f"their imported alert rules can never fire."
    )
