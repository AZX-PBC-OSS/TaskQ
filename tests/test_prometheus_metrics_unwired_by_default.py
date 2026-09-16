"""Pins: the documented "mounted automatically" claim for the Prometheus
metrics endpoint does not mean the ``taskq_*`` series are populated.

``docs/guides/admin-ui.md`` line ~395-396 says:

    "The Prometheus metrics endpoint is mounted automatically when
    taskq[prometheus] is installed."

An adopter reads that, installs ``taskq-py[prometheus]``, runs
``taskq ui serve`` exactly as ``docs/guides/admin-ui.md`` §"Health routes"
shows, points Prometheus at ``GET /jobs/health/metrics``, and imports the
17-rule alert set that ``docs/guides/observability.md`` §"Ready-made alert
rules" tells them to "import ... instead of writing from scratch". Every one
of those alert expressions (see ``src/taskq/contrib/prometheus/rules.yaml``)
references a ``taskq_*`` / ``messaging_*`` series.

What actually happens: the endpoint returns 200 with valid Prometheus text
the entire time -- but the series never appear, because populating them
requires a step this module's own docstring names but that appears nowhere
in ``docs/``:

    src/taskq/contrib/prometheus/_metrics.py:4-5
    "The operator must configure a MeterProvider with a
    PrometheusMetricReader before process start -- this module does NOT
    configure the provider."

``taskq ui serve`` (src/taskq/cli.py:1831-1835) calls
``create_metrics_router(None)`` with no ``registry=`` and performs no
``PrometheusMetricReader`` / ``MeterProvider`` wiring of its own. A grep of
docs/ for "PrometheusMetricReader" or "MeterProvider(" returns nothing --
confirmed at authoring time via
``grep -rn "PrometheusMetricReader\\|MeterProvider(" docs/``.

The failure mode this pins is exactly the "observability surface fails
silently" shape: an alert rule set an adopter is told to trust, that can
never fire, with the /metrics endpoint reporting healthy (200, valid
Prometheus exposition format) throughout. There is no error, no warning log,
no doctor finding -- only an absent time series, which a
`absent(taskq_jobs_by_status)` meta-alert (which TaskQ does not ship) would
be needed to catch.

Vendor precedent for auto-wired operational visibility with zero extra
config: vendor/sidekiq's Web UI (vendor/sidekiq/web/views/dashboard.html.erb)
and vendor/good_job's dashboard (vendor/good_job/app/controllers/
good_job/metrics_controller.rb:1-34) both render live queue/job state to
their own built-in UI with no external exporter or provider to configure --
neither ships a Prometheus bridge that can be *mounted* without being
*wired*. TaskQ's own admin UI pages (/admin/queues, /admin/jobs) share that
same no-config-needed property; only this one documented "automatic" claim
about the Prometheus bridge is false for the data (true only for the route).

This test drives the real production code paths (the real ``taskq.obs``
``record_*`` emitters used by the worker, and the real
``create_metrics_router`` used by ``taskq ui serve``) under the SAME
un-configured metrics environment those commands run in by default -- no
``PrometheusMetricReader``, no ``OTEL_*`` env vars -- and asserts the
resulting scrape is missing the series the shipped alert rules depend on.

This is a documentation/contract defect, not a call to remove the manual
wiring requirement (that is a legitimate operator-owned config surface in
other observability stacks too). The fix is docs (show the
``PrometheusMetricReader`` wiring next to "mounted automatically", the way
this module's own docstring already does) and/or a startup log warning from
``taskq ui serve`` when the router is mounted but no compatible reader is
registered. Left RED until one of those exists.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

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
        ["grep", "-rl", "PrometheusMetricReader", "docs/"],
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
    """Pins the behaviour TaskQ's docs promise, not the behaviour it has.

    ``docs/guides/admin-ui.md`` ("Prometheus metrics endpoint is mounted
    automatically when taskq[prometheus] is installed") and
    ``docs/guides/observability.md``'s "Ready-made alert rules" section
    (which tells an operator to import ``rules.yaml`` "instead of writing
    from scratch") together promise a working, no-extra-steps scrape target
    once the ``[prometheus]`` extra is installed. Mount the router exactly
    as `taskq ui serve` does (src/taskq/cli.py:1831-1835:
    `create_metrics_router(None)`, no `registry=` override) in a subprocess
    with NO OTel env vars set -- the default state after `pip install
    taskq-py[prometheus]` and `taskq ui serve`, per docs/guides/admin-ui.md's
    Docker Compose example, which sets only TASKQ_PG_DSN / TASKQ_REDIS_URL /
    TASKQ_ADMIN_HOST / TASKQ_ADMIN_PORT and nothing OTel-related.

    Record real taskq.obs metrics the way the worker does at runtime, then
    scrape the router's output and assert the taskq_*/messaging_* series the
    shipped rules.yaml depends on ARE present -- the behaviour "mounted
    automatically" should mean.

    Currently RED: as of this writing, `create_metrics_router` and
    `taskq ui serve` perform no `PrometheusMetricReader`/`MeterProvider`
    wiring (confirmed live: `taskq worker` + `taskq ui serve` run with no
    OTEL_* env vars produced a 200 `text/plain` scrape containing only
    Python/process default-collector series, zero `taskq_*` series, while
    the worker was actively completing and retrying jobs). The module's own
    docstring (src/taskq/contrib/prometheus/_metrics.py:4-5) says wiring a
    `MeterProvider` with a `PrometheusMetricReader` "before process start" is
    the operator's job and that "this module does NOT configure the
    provider" -- which is a reasonable library boundary, but it directly
    contradicts "mounted automatically" as an adopter reads it, and no doc
    shows the wiring step (see the companion test in this file).

    Two acceptable fixes, either one turns this green: (a) `taskq ui serve`
    auto-constructs a `PrometheusMetricReader`-backed `MeterProvider` when
    `taskq[prometheus]` is importable and no MeterProvider has been
    explicitly configured by the caller, matching "mounted automatically"
    literally; or (b) this test seam is wrong and the correct fix is
    doc-only (see the companion test) -- in that case this test should be
    deleted in the same change that closes the docs gap, with a note in the
    commit explaining the API is intentionally BYO-provider like the rest of
    TaskQ's OTel surface (docs/guides/observability.md ​§1 already documents
    OTLP export as fully BYO-collector).
    """
    # Run in a clean subprocess: the module-level instruments in
    # taskq.obs._otel bind to whichever MeterProvider is active at first
    # import (get_meter() called at module load, per obs/_otel.py:171-194),
    # so this must not share process state with anything a prior test in
    # this suite (or _PromEnv's isolated MeterProvider) may have configured.
    script = """
import sys
from fastapi import FastAPI
from fastapi.testclient import TestClient

# No PrometheusMetricReader constructed. No OTEL_* env vars set. This is
# exactly the state `taskq worker` / `taskq ui serve` boot into per the
# documented quick-start (docs/guides/admin-ui.md Docker Compose example).
from taskq.obs import _otel as obs

# Record the way the real worker does on the dispatch/consume path.
obs.record_published_message("compile_digest", "digests")
obs.record_consumed_message("compile_digest", "digests", outcome="succeeded")
obs.record_dispatch_duration("digests", 0.01)
obs.record_heartbeat_miss("w1")

from taskq.contrib.prometheus import create_metrics_router

# Exactly src/taskq/cli.py:1835 -- create_metrics_router(None), no registry override.
router = create_metrics_router(None)
app = FastAPI()
app.include_router(router, prefix="/jobs/health")
client = TestClient(app)

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
    result = subprocess.run(
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
    # obs.record_* calls just emitted. Today it contains none of them --
    # this assertion is expected to fail (RED) until one of the two fixes
    # named in the docstring above ships.
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
