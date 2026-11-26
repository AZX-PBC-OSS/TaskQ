"""The wf-progress alert path, joined end to end: real rows -> the production
leader sampler -> the gauge cache -> a real ``/metrics`` exposition ->
promtool evaluating the SHIPPED rule file.

Every other pin proves one half. ``test_wf_progress_sampler_emits_derived_node_states``
stops at the OTel observation callback; ``test_leg3_the_live_wf_gauge_scrape``
stops at the exposition text, with the sampler read and the ``_other_``
collapse re-run inside the test; ``tests/_prom_review.py``'s promtool harness
starts from series it types by hand (``update_wf_progress_cache({("probe_wf",
"blocked"): 3, ...})``) and binds back to a real producer exposition on the
series NAME and on both label VALUES — ``workflow`` and ``state``, the one
label the alert filters by.

So nothing ever ran the shipped ``TaskQWorkflowBlockedStuck`` rule against a
series the sampler produced, which is the seam the alert's original defect hid
in: the rule evaluated green against a ``state="blocked"`` series the gauge
could not emit. Nothing exercised the sampler's ``_other_`` collapse on the
production path either, and nothing showed the blocked series being REMOVED
once the row clears — the other half of an alert's contract.

These pins cross all three. The rows are the project's own seeded flows, the
read is ``_queue_depth_loop``'s own tick, the expositions are the real
``/jobs/health/metrics`` responses over a real ``PrometheusMetricReader`` (in a
subprocess, the shipped boot order), and the promtool ``input_series`` are
built from THOSE expositions' own label sets and values — nothing about the
series shape is typed by this file.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import asyncpg
import pytest
import yaml

from taskq._ids import new_uuid
from tests._prom_review import docker_available, run_promtool_rule_tests
from tests._wf_fixtures import seed_flow, seed_join

pytest.importorskip("fastapi")
pytest.importorskip("opentelemetry.exporter.prometheus")

pytestmark = [pytest.mark.integration, pytest.mark.otel, pytest.mark.prometheus]

RULES_PATH = (
    Path(__file__).parent.parent / "src" / "taskq" / "contrib" / "prometheus" / "rules.yaml"
)

_ALERT = "TaskQWorkflowBlockedStuck"

#: The scrape leg: the real endpoint, over a real Prometheus reader, with the
#: provider wired BEFORE the first ``taskq.obs`` import (the shipped serve
#: path's boot order — a real-provider meter never rebinds, so a provider
#: installed after the import strands the gauge on the pre-provider proxy).
_SCRAPE_SCRIPT = """
import json
import os

from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry import metrics as otel_metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client import CollectorRegistry

registry = CollectorRegistry()
otel_metrics.set_meter_provider(
    MeterProvider(metric_readers=[PrometheusMetricReader(registry=registry)])
)

from taskq.obs import update_wf_progress_cache  # noqa: E402  # Why: the import ORDER is the pinned boot order.

update_wf_progress_cache(
    {(workflow, state): count for workflow, state, count in json.loads(os.environ["_SAMPLE_WF_CACHE"])}
)

from taskq.contrib.prometheus import create_metrics_router  # noqa: E402  # Why: as above.

app = FastAPI()
app.include_router(create_metrics_router(None, registry=registry), prefix="/jobs/health")
with TestClient(app) as client:
    response = client.get("/jobs/health/metrics")
assert response.status_code == 200, response.status_code
print("SCRAPE_BEGIN")
print(response.text, end="")
"""


# ── the production loop, started and stopped on demand ──────────────


async def _stream_one_tick(
    ctx: Any,
    stop_when: Any,
    description: str,
) -> None:
    """Run the real ``_queue_depth_loop`` until *stop_when()* holds, then stop
    it. The loop's first tick fires immediately; every pin stops it on that
    publication, so later ``queue_depth_interval`` waits never elapse."""
    from taskq.testing.assertions import wait_for_condition
    from taskq.worker._leader_sweeps import (  # pyright: ignore[reportPrivateUsage]  # Why: the production producer is the surface under test.
        _queue_depth_loop,
    )

    shutdown = asyncio.Event()
    task = asyncio.create_task(_queue_depth_loop(ctx, shutdown))
    try:
        await wait_for_condition(stop_when, description=description)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=5.0)


def _make_sweep_ctx(pg_dsn: str, wf_schema: str, wf_pool: asyncpg.Pool) -> Any:
    """A leader ``SweepContext`` whose samplers read through the module pool
    (the same construction every production leader loop receives)."""
    from taskq.backend.clock import SystemClock
    from taskq.settings import WorkerSettings
    from taskq.worker._leader_shared import SweepContext
    from taskq.worker.deps import WorkerDeps

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": wf_schema,
        }
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=wf_pool,
        heartbeat_pool=wf_pool,
        worker_pool=wf_pool,
        notify_conn=None,
        leader_conn=None,
    )
    deps.is_leader.set()
    return SweepContext(
        deps=deps,
        backend=None,  # type: ignore[arg-type]  # Why: the queue-depth loop never touches ctx.backend.
        clock=SystemClock(),
        worker_id=new_uuid(),
    )


async def _delete_seeded_flow(wf_pool: asyncpg.Pool, wf_schema: str, flow_id: Any) -> None:
    """Remove a pin's committed rows after the production loop's own pool
    connections have read them (a committed seed has no rollback; the
    cleanup is the isolation)."""
    async with wf_pool.acquire() as cleanup:
        await cleanup.execute(
            f'DELETE FROM "{wf_schema}".jobs '  # noqa: S608  # Why: the only interpolation is the fixture's validated schema identifier; the row id is a bind parameter.
            "WHERE id = $1 OR (metadata->>'flow_id')::uuid = $1",
            flow_id,
        )


# ── the exposition leg ─────────────────────────────────────────────


def _humanize(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else str(value)


def _expand_annotation(text: str, labels: dict[str, str], value: float) -> str:
    text = re.sub(r"{{\s*\$labels\.(\w+)\s*}}", lambda m: labels.get(m.group(1), ""), text)
    return re.sub(r"{{\s*\$value\s*}}", _humanize(value), text)


def _scrape_wf_series(cache: dict[tuple[str, str], int], workflow: str) -> list[str]:
    """The real endpoint's own lines for one workflow's wf-progress series.

    Filtered to *workflow*: the exposition carries every workflow the cache
    holds, and this pin reads back only the series it seeded. Filtering is
    test isolation, not a re-derivation of the series shape — every label and
    value fed to promtool below comes from the exposition.
    """
    env = {
        key: value for key, value in os.environ.items() if not key.startswith(("OTEL_", "TASKQ_"))
    }
    env["_SAMPLE_WF_CACHE"] = json.dumps(
        [[wf, state, count] for (wf, state), count in cache.items() if wf == workflow]
    )
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell.
        [sys.executable, "-c", _SCRAPE_SCRIPT],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"scrape leg failed:\n{result.stdout}\n{result.stderr}"
    exposition = result.stdout.split("SCRAPE_BEGIN\n", 1)[1]
    lines = [
        line
        for line in exposition.splitlines()
        if line.startswith("taskq_wf_progress_nodes_total{")
    ]
    assert lines, f"the exposition carried no wf-progress series:\n{exposition[:2000]}"
    return lines


def _input_series(series_lines: list[str]) -> list[dict[str, str]]:
    """``input_series`` entries built from the exposition's own lines.

    The series names, label sets and values are the scrape's. The only thing
    this harness contributes is the sample COUNT — the ``0x35`` window a
    duration-bound rule needs and no single scrape can supply.
    """
    entries: list[dict[str, str]] = []
    for line in series_lines:
        match = re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+(\S+)$", line.strip())
        assert match, f"unparseable exposition line: {line!r}"
        name, labels, value = match.group(1), match.group(2) or "", match.group(3)
        # promtool drops empty-valued labels (the OTel bridge renders
        # `otel_scope_schema_url=""`), so the fed set must drop them too or
        # the expected vector carries a label the evaluator never keeps.
        kept = [
            f'{key}="{val}"' for key, val in re.findall(r'(\w+)="([^"]*)"', labels) if val != ""
        ]
        rendered = "{" + ",".join(kept) + "}" if kept else ""
        entries.append({"series": f"{name}{rendered}", "values": f"{_humanize(float(value))}+0x35"})
    return entries


def _shipped_rule() -> dict[str, Any]:
    return next(
        rule
        for rule in yaml.safe_load(RULES_PATH.read_text())["groups"][0]["rules"]
        if rule.get("alert") == _ALERT
    )


def _firing_case(series_lines: list[str], workflow: str) -> dict[str, Any]:
    """The alert FIRES at 32m on the blocked series, with the shipped rule's
    annotations template-expanded against the scrape's own labels and value."""
    inputs = _input_series(series_lines)
    matches = [
        entry
        for entry in inputs
        if 'state="blocked"' in entry["series"] and f'workflow="{workflow}"' in entry["series"]
    ]
    assert matches, (
        "the sampler's own scrape carried no "
        f'state="blocked" series for {workflow}: {[e["series"] for e in inputs]} — '
        f"{_ALERT} selects exactly that series, so the alert is dead again"
    )
    blocked = matches[0]
    labels = dict(re.findall(r'(\w+)="([^"]*)"', blocked["series"]))
    value = float(blocked["values"].split("+")[0])
    shipped = _shipped_rule()
    exp_labels = {**labels, **shipped.get("labels", {})}
    return {
        "interval": "1m",
        "input_series": inputs,
        "alert_rule_test": [
            {
                "eval_time": "32m",
                "alertname": _ALERT,
                "exp_alerts": [
                    {
                        "exp_labels": exp_labels,
                        "exp_annotations": {
                            key: _expand_annotation(text, exp_labels, value)
                            for key, text in shipped.get("annotations", {}).items()
                        },
                    }
                ],
            }
        ],
    }


def _silent_case(series_lines: list[str]) -> dict[str, Any]:
    """The alert stays silent at the same 32m bound on the post-recovery
    exposition — the false-page half, fed from the real scrape."""
    return {
        "interval": "1m",
        "input_series": _input_series(series_lines),
        "alert_rule_test": [{"eval_time": "32m", "alertname": _ALERT, "exp_alerts": []}],
    }


def _own_keys(cache: dict[tuple[str, str], int], workflow: str) -> dict[tuple[str, str], int]:
    return {key: count for key, count in cache.items() if key[0] == workflow}


# ── the pins ───────────────────────────────────────────────────────


@pytest.mark.skipif(
    not docker_available(),
    reason="promtool runs in the prom/prometheus container; Docker unreachable",
)
async def test_the_shipped_blocked_rule_fires_and_resolves_on_the_samplers_own_scrapes(
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
    tmp_path: Any,
) -> None:
    """``TaskQWorkflowBlockedStuck`` fires while a row is blocked and resolves
    once it clears — both on series the production sampler produced, with no
    hand-fed cache and no hand-typed label set.

    The resolution leg is not decoration. The alert can only resolve because
    ``update_wf_progress_cache`` REBINDS the whole cache each tick, so the
    ``(workflow, "blocked")`` key disappears the moment the row stops deriving
    blocked. A merge-instead-of-rebind refactor would pin the count at its last
    non-zero value forever — the alert would hold firing over a condition that
    had cleared — and nothing else in the suite would notice.
    """
    from taskq.obs import (
        _otel as otel_mod,  # pyright: ignore[reportPrivateUsage]  # Why: the loop's only publication surface is the cache.
    )
    from taskq.workflows import Promise, StepContext, WorkflowApp, build, step

    workflow_name = f"wf_alert_path_{new_uuid().hex[:8]}"
    app = WorkflowApp()

    async def body(ctx: StepContext) -> None:
        return None

    @app.workflow(workflow_name)
    def alert_path_flow() -> Promise[None]:
        return build(step(body, key="join_wait"))

    app.get(workflow_name)

    root: Any = None
    try:
        # The production loop reads through its own pool connections: the seed
        # is committed on purpose (clean_pg_conn does not roll back — its
        # isolation is the setup-time truncate) so those connections can see
        # the rows. The finally below removes them for sibling loop pins.
        async with wf_pool.acquire() as seed_conn:
            root = await seed_flow(seed_conn, wf_schema, workflow=workflow_name)
            # A join-wait: pending with an unresolved dependency behind a 'join'
            # stamp — the blocked representation the alert exists for.
            await seed_join(seed_conn, wf_schema, root, step_key="join_wait", deps=1)

        ctx = _make_sweep_ctx(pg_dsn, wf_schema, wf_pool)

        await _stream_one_tick(
            ctx,
            lambda: otel_mod._wf_progress_cache.get((workflow_name, "blocked")) == 1,  # pyright: ignore[reportPrivateUsage]  # Why: as above.
            "the production leader loop to publish the join-wait row",
        )
        blocked_cache = _own_keys(otel_mod._wf_progress_cache, workflow_name)  # pyright: ignore[reportPrivateUsage]

        # Recovery: the join's dependency resolves. The row keeps its 'join'
        # stamp — the fired-join shape the derivation reads as pending.
        async with wf_pool.acquire() as cure_conn:
            await cure_conn.execute(
                f'UPDATE "{wf_schema}".jobs SET deps_pending = 0 '  # noqa: S608  # Why: the only interpolation is the fixture's own schema identifier, which SQL cannot parameterise; the row is a bind parameter.
                "WHERE (metadata->>'flow_id')::uuid = $1 AND step_key = 'join_wait'",
                root,
            )

        await _stream_one_tick(
            ctx,
            lambda: (workflow_name, "blocked") not in otel_mod._wf_progress_cache,  # pyright: ignore[reportPrivateUsage]
            "the loop to drop the resolved row's blocked key",
        )
        resolved_cache = _own_keys(otel_mod._wf_progress_cache, workflow_name)  # pyright: ignore[reportPrivateUsage]

        assert blocked_cache == {(workflow_name, "blocked"): 1}, blocked_cache
        assert (workflow_name, "blocked") not in resolved_cache, (
            f'the resolved row\'s state="blocked" series survived the next successful '
            f"tick: {resolved_cache} — the cache is merging instead of rebinding, so the "
            "gauge pins its last non-zero count and the alert never resolves"
        )
        # Not vacuous: the row is still there, now deriving the fired-join's pending.
        assert resolved_cache == {(workflow_name, "pending"): 1}, resolved_cache

        blocked_series = _scrape_wf_series(blocked_cache, workflow_name)
        resolved_series = _scrape_wf_series(resolved_cache, workflow_name)
        assert not any('state="blocked"' in line for line in resolved_series), (
            f'the post-recovery exposition still carries state="blocked": {resolved_series}'
        )

        test_doc = {
            "rule_files": ["/work/rules.yaml"],
            "evaluation_interval": "1m",
            "tests": [
                _firing_case(blocked_series, workflow_name),
                _silent_case(resolved_series),
            ],
        }
        out = run_promtool_rule_tests(RULES_PATH, yaml.safe_dump(test_doc), Path(tmp_path))
        assert "SUCCESS" in out, out
    finally:
        if root is not None:
            await _delete_seeded_flow(wf_pool, wf_schema, root)


async def test_the_sampler_collapses_an_unregistered_workflow_onto_the_other_bucket(
    wf_schema: str,
    wf_pool: asyncpg.Pool,
    pg_dsn: str,
) -> None:
    """The sampler's other declared dimension behaviour, through the production
    loop: a run whose workflow name is NOT registered in this process
    contributes to the ``_other_`` bucket, never to a series named after
    itself.

    Nothing else crosses this on the real path. ``test_leg3_the_live_wf_gauge_scrape``
    re-implements the collapse inside the test, and the leader-loop pin only
    seeds a REGISTERED workflow — which keeps its name under the correct
    collapse and under no collapse at all. So deleting the collapse reds
    nothing, and the cardinality law the ``_other_`` bucket exists to enforce
    (one series per workflow name is the series that cannot be stored) would be
    re-broken silently.
    """
    from taskq.obs import (
        _otel as otel_mod,  # pyright: ignore[reportPrivateUsage]  # Why: the loop's only publication surface is the cache.
    )

    # A run for a workflow this process has not registered — the shape a
    # deployment produces when a definition is not imported in every worker
    # process (the collapse's whole reason to exist).
    unregistered_name = f"wf_never_registered_{new_uuid().hex[:8]}"
    root: Any = None
    try:
        async with wf_pool.acquire() as seed_conn:
            root = await seed_flow(seed_conn, wf_schema, workflow=unregistered_name)
            await seed_join(seed_conn, wf_schema, root, step_key="join_wait", deps=1)

        ctx = _make_sweep_ctx(pg_dsn, wf_schema, wf_pool)

        await _stream_one_tick(
            ctx,
            lambda: otel_mod._wf_progress_cache.get(("_other_", "blocked"), 0) >= 1,  # pyright: ignore[reportPrivateUsage]  # Why: the loop's only publication surface is the cache.
            "the loop to fold the unregistered run into the _other_ bucket",
        )
        cache = dict(otel_mod._wf_progress_cache)  # pyright: ignore[reportPrivateUsage]

        leaked = sorted(workflow for (workflow, _state) in cache if workflow == unregistered_name)
        assert not leaked, (
            f"the sampler minted series for the unregistered workflow {unregistered_name!r}: "
            f"{leaked} — the registered-names collapse is gone, so the gauge is back to one "
            "series per workflow name (the cardinality law the _other_ bucket enforces)"
        )
        assert cache == {("_other_", "blocked"): 1}, cache
    finally:
        if root is not None:
            await _delete_seeded_flow(wf_pool, wf_schema, root)
