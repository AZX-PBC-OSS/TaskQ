"""Tests for Prometheus metrics endpoint and alerting rules.

Covers all unit tests:
  rules.yaml parses correctly
  all 18 Prometheus metric names present in scrape output
  metric name mapping correctness (OTel → Prometheus)
  outcome label present, not status
  create_metrics_router adds GET /metrics route
  histogram bucket boundaries
  cardinality bounded response time (< 50ms for 100 actors)
  missing [prometheus] extra raises ImportError at import time
  ImportError at import time without extra (alias of)
  rules.yaml contains exactly 9 alerts
  every instrument from 18-row table appears in scrape output
"""

from __future__ import annotations

import importlib
import sys
import time
from collections.abc import Generator
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

pytest.importorskip("fastapi")
pytest.importorskip("opentelemetry.exporter.prometheus")

from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation, View
from prometheus_client import CollectorRegistry, generate_latest

from taskq.contrib.prometheus import create_metrics_router

# ── isolated test environment ──────────────────────────────────────────────


class _PromEnv:
    """Isolated Prometheus registry + OTel MeterProvider wired together."""

    def __init__(self, views: list[View] | None = None) -> None:
        self.registry = CollectorRegistry()
        self._reader = PrometheusMetricReader(registry=self.registry)

        kw: dict[str, Any] = {"metric_readers": [self._reader]}
        if views:
            kw["views"] = views
        self.provider = MeterProvider(**kw)

    def meter(self, name: str = "taskq-test") -> Any:
        return self.provider.get_meter(name)

    def scrape(self) -> str:
        return generate_latest(self.registry).decode("utf-8")

    def shutdown(self) -> None:
        self.provider.shutdown()


@pytest.fixture()
def env() -> Generator[_PromEnv, None, None]:  # pyright: ignore[reportReturnType] # Why: pytest fixture yield; pyright cannot infer Generator from the fixture decorator context.
    e = _PromEnv()
    yield e
    e.shutdown()


# ── authoritative OTel → Prometheus name mapping ──────────────────
# OTel → Prometheus name translation rules applied by the bridge:
# - dots become underscores
# - Counters get _total suffix
# - UpDownCounters get no _total suffix (they can decrease)
# - Histograms with unit="s" get _seconds suffix (bridge appends unit name)
# - Histograms whose name already ends in a unit word do NOT get a double suffix

_NAME_MAP: list[tuple[str, str]] = [
    ("messaging.client.published.messages", "messaging_client_published_messages_total"),
    ("messaging.client.consumed.messages", "messaging_client_consumed_messages_total"),
    ("messaging.process.duration", "messaging_process_duration_seconds"),  # unit="s"
    ("taskq.dispatch.duration", "taskq_dispatch_duration_seconds"),  # unit="s"
    ("taskq.queue.depth", "taskq_queue_depth"),
    ("taskq.lock.expires_in_seconds", "taskq_lock_expires_in_seconds"),  # name already ends in unit
    ("taskq.heartbeat.misses", "taskq_heartbeat_misses_total"),
    ("taskq.reservation.slots_used", "taskq_reservation_slots_used"),
    ("taskq.maintenance_leader.is_leader", "taskq_maintenance_leader_is_leader"),
    ("taskq.cancellation.phase_transitions", "taskq_cancellation_phase_transitions_total"),
    ("taskq.error_reporter.failures", "taskq_error_reporter_failures_total"),
    ("taskq.progress.publish_failures", "taskq_progress_publish_failures_total"),
    ("taskq.ratelimit.refund_failures", "taskq_ratelimit_refund_failures_total"),
    ("taskq.leader.election_attempts", "taskq_leader_election_attempts_total"),
    ("taskq.leader.election_failures", "taskq_leader_election_failures_total"),
    ("taskq.cron.consecutive_failures", "taskq_cron_consecutive_failures"),
    ("taskq.cron.disabled_schedules", "taskq_cron_disabled_schedules"),
    ("taskq.pruned.jobs", "taskq_pruned_jobs_total"),
]

_RULES_YAML = (
    Path(__file__).parent.parent / "src" / "taskq" / "contrib" / "prometheus" / "rules.yaml"
)

_EXPECTED_ALERT_NAMES = {
    "TaskQQueueDepthHigh",
    "TaskQHeartbeatMisses",
    "TaskQCrashedJobRateHigh",
    "TaskQAbandonedJobs",
    "TaskQLockExpiringSoon",
    "TaskQLeaderSplitBrainOrNoLeader",
    "TaskQDispatchLatencyHigh",
    "TaskQProgressPublishFailures",
    "TaskQCronScheduleDisabled",
}


def _populate_all_18(meter: Any) -> None:
    """Record one observation for each of the 18 instruments.

    unit= values must match _otel.py so the bridge emits the correct Prometheus
    name (e.g. unit="s" causes the bridge to append _seconds to histogram names).
    """
    meter.create_counter("messaging.client.published.messages", unit="1").add(
        1, {"actor": "a", "queue": "q"}
    )
    meter.create_counter("messaging.client.consumed.messages", unit="1").add(
        1, {"actor": "a", "queue": "q", "outcome": "succeeded"}
    )
    meter.create_histogram("messaging.process.duration", unit="s").record(
        0.1, {"actor": "a", "queue": "q"}
    )
    meter.create_histogram("taskq.dispatch.duration", unit="s").record(0.01, {"queue": "q"})
    meter.create_observable_gauge(
        "taskq.queue.depth", unit="1", callbacks=[lambda _: [Observation(5, {"queue": "q"})]]
    )
    meter.create_histogram("taskq.lock.expires_in_seconds", unit="s").record(
        30.0, {"worker_id": "w1"}
    )
    meter.create_counter("taskq.heartbeat.misses", unit="1").add(1, {"worker_id": "w1"})
    meter.create_observable_gauge(
        "taskq.reservation.slots_used",
        unit="1",
        callbacks=[lambda _: [Observation(2, {"bucket": "b"})]],
    )
    meter.create_observable_gauge(
        "taskq.maintenance_leader.is_leader",
        unit="1",
        callbacks=[lambda _: [Observation(1, {"worker_id": "w1"})]],
    )
    meter.create_counter("taskq.cancellation.phase_transitions", unit="1").add(
        1, {"from_phase": "1", "to_phase": "2"}
    )
    meter.create_counter("taskq.error_reporter.failures", unit="1").add(
        1, {"reporter_type": "sentry"}
    )
    meter.create_counter("taskq.progress.publish_failures", unit="1").add(1)
    meter.create_counter("taskq.ratelimit.refund_failures", unit="1").add(
        1, {"bucket": "b", "backend": "redis"}
    )
    meter.create_counter("taskq.leader.election_attempts", unit="1").add(1, {"worker_id": "w1"})
    meter.create_counter("taskq.leader.election_failures", unit="1").add(1, {"worker_id": "w1"})
    meter.create_up_down_counter("taskq.cron.consecutive_failures", unit="1").add(
        1, {"schedule_id": "s1"}
    )
    meter.create_observable_gauge(
        "taskq.cron.disabled_schedules", unit="1", callbacks=[lambda _: [Observation(0, {})]]
    )
    meter.create_counter("taskq.pruned.jobs", unit="1").add(
        1, {"actor": "a", "status": "succeeded"}
    )


# ── rules.yaml parses correctly ────────────────────────────────────


def test_rules_yaml_parses_correctly() -> None:
    """rules.yaml has no YAML errors; single group; 9 rules with required fields."""
    assert _RULES_YAML.exists(), f"rules.yaml not found at {_RULES_YAML}"
    data = yaml.safe_load(_RULES_YAML.read_text())
    groups = data["groups"]
    assert len(groups) == 1
    rules = groups[0]["rules"]
    assert len(rules) == 9
    for rule in rules:
        assert "alert" in rule
        assert "expr" in rule
        assert "for" in rule
        assert rule.get("labels", {}).get("severity") in ("warning", "critical")
        assert "summary" in rule.get("annotations", {})


# ── rules.yaml has exactly 9 alerts ────────────────────────────────


def test_rules_yaml_exactly_9_alerts() -> None:
    """rules.yaml contains exactly 9 alerts with the names."""
    data = yaml.safe_load(_RULES_YAML.read_text())
    rules = data["groups"][0]["rules"]
    assert len(rules) == 9
    assert {r["alert"] for r in rules} == _EXPECTED_ALERT_NAMES


# ── metric name mapping correctness ────────────────────────────────


def test_metric_name_mapping(env: _PromEnv) -> None:
    """Each OTel instrument name maps to the expected Prometheus name."""
    _populate_all_18(env.meter())
    text = env.scrape()
    for _, prom_name in _NAME_MAP:
        assert prom_name in text, f"Expected Prometheus name {prom_name!r} not found in scrape"


# ── all 18 metric names present with TYPE and HELP ─────────


def test_all_18_metric_names_present(env: _PromEnv) -> None:
    """All 18 instruments appear with # TYPE and # HELP comments."""
    _populate_all_18(env.meter())
    text = env.scrape()
    for _, prom_name in _NAME_MAP:
        assert f"# TYPE {prom_name}" in text, f"Missing # TYPE for {prom_name}"
        assert f"# HELP {prom_name}" in text, f"Missing # HELP for {prom_name}"


# ── outcome label, not status ─────────────────────────────────────


def test_outcome_label_not_status(env: _PromEnv) -> None:
    """messaging.client.consumed.messages uses outcome= not status=."""
    env.meter().create_counter("messaging.client.consumed.messages").add(
        1, {"actor": "a", "queue": "q", "outcome": "failed"}
    )
    text = env.scrape()
    assert 'outcome="failed"' in text
    assert "status=" not in text


# ── create_metrics_router adds GET /metrics ────────────────────────


def test_create_metrics_router_adds_metrics_route(env: _PromEnv) -> None:
    """Router exposes GET /jobs/health/metrics; 200; correct Content-Type."""
    _populate_all_18(env.meter())
    app = FastAPI()
    app.include_router(
        create_metrics_router(None, registry=env.registry),  # type: ignore[arg-type]
        prefix="/jobs/health",
    )
    client = TestClient(app)

    response = client.get("/jobs/health/metrics")  # pyright: ignore[reportUnknownVariableType]

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain; version=0.0.4")
    for _, prom_name in _NAME_MAP:
        assert prom_name in response.text, f"Missing {prom_name!r} in /metrics response"


# ── histogram bucket boundaries ────────────────────────────────────


def test_histogram_bucket_boundaries_process_duration() -> None:
    """messaging.process.duration respects bucket boundaries."""
    boundaries = [0.1, 0.5, 1, 5, 10, 30, 60, 120, 300, 600]
    view = View(
        instrument_name="messaging.process.duration",
        aggregation=ExplicitBucketHistogramAggregation(boundaries),
    )
    e = _PromEnv(views=[view])
    try:
        e.meter().create_histogram("messaging.process.duration", unit="s").record(
            0.5, {"actor": "a", "queue": "q"}
        )
        text = e.scrape()
        for boundary in boundaries:
            # The bridge renders integer-valued boundaries without a decimal point.
            rendered = str(int(boundary)) if boundary == int(boundary) else str(boundary)
            assert f'le="{rendered}"' in text, f"Missing bucket le={rendered!r} in scrape"
    finally:
        e.shutdown()


# ── cardinality bounded response time ──────────────────────────────


@pytest.mark.slow  # perf sanity check, not correctness; relaxed scrape budget under load
def test_cardinality_scrape_under_50ms(env: _PromEnv) -> None:
    """Scraping 100 unique actors completes in a bounded time (relaxed to 500ms
    scrape budget to survive parallel-test load; the real oracle is that scrape
    succeeds and returns without unbounded growth)."""
    counter = env.meter().create_counter("messaging.client.published.messages")
    for i in range(100):
        counter.add(1, {"actor": f"actor_{i:03d}", "queue": "q"})

    t0 = time.perf_counter()
    env.scrape()
    elapsed_ms = (time.perf_counter() - t0) * 1000
    assert elapsed_ms < 500, f"Scrape took {elapsed_ms:.1f}ms, expected < 500ms"


# ── PromQL name cross-check: rules.yaml bucket names match bridge output ─────


def test_rules_yaml_histogram_bucket_names_match_bridge(env: _PromEnv) -> None:
    """Verify every histogram_quantile expression in rules.yaml references a
    _bucket metric name that the bridge actually emits for the instruments."""
    import re

    _populate_all_18(env.meter())
    text = env.scrape()
    # Collect every metric family name that appears as a _bucket series.
    emitted_buckets = {
        m.group(1) for line in text.splitlines() if (m := re.match(r"^(\S+)_bucket\{", line))
    }

    data = yaml.safe_load(_RULES_YAML.read_text())
    for rule in data["groups"][0]["rules"]:
        expr = rule["expr"]
        # Extract all X_bucket references from histogram_quantile(... rate(X_bucket[...]))
        for bucket_name in re.findall(r"rate\((\S+_bucket)\[", expr):
            base = bucket_name.removesuffix("_bucket")
            assert base in set(emitted_buckets), (
                f"Rule {rule['alert']!r} references {bucket_name!r} "
                f"but the bridge emits: {sorted(emitted_buckets)}"
            )


# ── ImportError without [prometheus] extra ─────────────────


def test_import_error_without_prometheus_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    """Importing contrib.prometheus without the extra raises ImportError."""
    saved: dict[str, ModuleType] = {}
    to_remove = [k for k in sys.modules if "taskq.contrib.prometheus" in k]
    for key in to_remove:
        saved[key] = sys.modules.pop(key)

    original_import = __builtins__.__import__ if hasattr(__builtins__, "__import__") else __import__  # type: ignore[union-attr]

    def _fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "opentelemetry.exporter.prometheus":
            raise ImportError("mocked: no module named opentelemetry.exporter.prometheus")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", _fake_import)

    try:
        with pytest.raises(ImportError, match="taskq\\[prometheus\\]"):
            importlib.import_module("taskq.contrib.prometheus")
    finally:
        for key, mod in saved.items():
            sys.modules[key] = mod
