"""Metrics review: emission truth + alert-rule firing, proven against the
real system.

Two layers, both against REAL emitted series:

**Emission truth.** A subprocess runs the shipped worker bootstrap (the
production boot order: ``WorkerSettings.load`` → ``configure_exporters`` →
``worker._main``) against real Postgres and drives the real behaviors the
metrics document: enqueues (success, terminal failure, retryable failure,
timeout, backpressure refusal, unserved queue, missing actor_config), a
failing cron schedule through the real three-strike auto-disable, the
leader's gauge samplers, the heartbeat loop, the watchdog, and a clean
SIGTERM shutdown. The exposition is captured from BOTH real scrape paths —
the ``/jobs/health/metrics`` bridge router exactly as ``taskq ui serve``
mounts it, and the worker's own ``TASKQ_METRICS_PORT`` pull listener — and
asserted for the documented NAME + LABELS + plausible VALUES of a
representative metric set. A second (hostile) probe drives a real
transient Postgres failure - the server terminating the worker's
connections mid-run - for the heartbeat-miss counter, the one family a
healthy run cannot emit.

**Alert rules fire.** For every alert in the shipped rules.yaml, the
expression is evaluated by promtool (inside the prom/prometheus image)
against input series that carry the metric names and label sets the REAL
probe's exposition actually served, seeded with the pathology the rule
guards. Every rule must fire; a second set of healthy-shape inputs must
NOT fire (the false-page guard). A rule that cannot fire against real
names/labels - or that fires on a healthy fleet - fails here.

Marks: integration (testcontainers PG), otel ([prometheus] extra).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

pytest.importorskip("fastapi")
pytest.importorskip("opentelemetry.exporter.prometheus")

from tests._prom_review import (
    Exposition,
    docker_available,
    parse_exposition,
    run_emitter_probe,
    run_hostile_probe,
    run_promtool_rule_tests,
    run_worker_probe,
)

pytestmark = [pytest.mark.integration, pytest.mark.otel, pytest.mark.prometheus]

RULES_PATH = (
    Path(__file__).parent.parent / "src" / "taskq" / "contrib" / "prometheus" / "rules.yaml"
)

#: Metric-family name tokens the operand scan must never treat as series.
_PROMQL_KEYWORDS = frozenset(
    {
        "rate",
        "sum",
        "max",
        "min",
        "by",
        "without",
        "on",
        "ignoring",
        "group_left",
        "group_right",
        "unless",
        "and",
        "or",
        "histogram_quantile",
        "time",
        "changes",
        "offset",
        "not",
        "le",
        "bool",
        "_other_",
    }
)


# ── fixtures: each probe runs ONCE per module ──────────────────────


@pytest.fixture(scope="module")
def worker_scrapes(pg_dsn: str, module_pg_schema: Any, tmp_path_factory: Any) -> dict[str, str]:
    """Expositions from the real worker probe: keys ``LIVE``/``FINAL``
    (served by the bridge router) and ``LIVE.port``/``FINAL.port`` (served
    by the worker's own TASKQ_METRICS_PORT pull listener)."""
    return run_worker_probe(
        pg_dsn,
        module_pg_schema.schema_name,
        tmp_path_factory.mktemp("prom_worker_probe"),
    )


@pytest.fixture(scope="module")
def live(worker_scrapes: dict[str, str]) -> Exposition:
    return parse_exposition(worker_scrapes["LIVE"])


@pytest.fixture(scope="module")
def final(worker_scrapes: dict[str, str]) -> Exposition:
    return parse_exposition(worker_scrapes["FINAL"])


@pytest.fixture(scope="module")
def follower(worker_scrapes: dict[str, str]) -> Exposition:
    """The second worker's exposition: the fleet shape where a losing
    election candidate records the maintenance-lock contention."""
    return parse_exposition(worker_scrapes["FOLLOWER.port"])


@pytest.fixture(scope="module")
def hostile(pg_dsn: str, module_pg_schema: Any, tmp_path_factory: Any) -> dict[str, str]:
    mid, recovered = run_hostile_probe(
        pg_dsn,
        module_pg_schema.schema_name,
        tmp_path_factory.mktemp("prom_hostile_probe"),
    )
    return {"MID": mid, "RECOVERED": recovered}


@pytest.fixture(scope="module")
def hostile_mid(hostile: dict[str, str]) -> Exposition:
    return parse_exposition(hostile["MID"])


@pytest.fixture(scope="module")
def emitter(tmp_path_factory: Any) -> Exposition:
    """The real sweep-abort emitters' own exposition: the two families the
    live worker probes cannot stage, bound to their real served names."""
    return parse_exposition(run_emitter_probe(tmp_path_factory.mktemp("prom_emitter_probe")))


@pytest.fixture(scope="module")
def hostile_recovered(hostile: dict[str, str]) -> Exposition:
    return parse_exposition(hostile["RECOVERED"])


def _scopeless(labels: dict[str, str]) -> dict[str, str]:
    """Drop the OTel plumbing labels (otel_scope_*) every bridge series
    carries: the documented labels are what the review audits."""
    return {k: v for k, v in labels.items() if not k.startswith("otel_scope_")}


# ── 1. emission truth: name + labels + plausible value ────────────


class TestLeaderGauges:
    """The leader-sampled gauge family, emitted by the real leader loop."""

    def test_is_leader_is_one_with_worker_id(self, live: Exposition) -> None:
        series = live.series("taskq_maintenance_leader_is_leader")
        assert series, "the leader gauge is absent from a live single-worker scrape"
        values = {s.value for s in series}
        assert values <= {0.0, 1.0}, f"is_leader carries non-boolean values: {values}"
        assert all("worker_id" in _scopeless(s.labels) for s in series), (
            "is_leader is documented as labeled by worker_id (the split-brain "
            "alert sums one series per pod); a series without it is anonymous"
        )
        assert any(s.value == 1.0 for s in series), "the single worker never became leader"

    def test_queue_depth_per_queue_with_seeded_counts(self, live: Exposition) -> None:
        series = live.series("taskq_queue_depth")
        assert series, "the queue-depth gauge is absent from the live scrape"
        depth = {_scopeless(s.labels).get("queue"): s.value for s in series}
        assert "ghost_queue" in depth, (
            "the unserved queue the probe seeded (2 pending rows) is missing from the depth gauge"
        )
        assert depth["ghost_queue"] == 2.0, depth

    def test_queue_live_workers_joins_depth(self, live: Exposition) -> None:
        series = live.series("taskq_queue_live_workers")
        assert series, "the live-workers gauge is absent from the live scrape"
        by_queue = {_scopeless(s.labels).get("queue"): s.value for s in series}
        assert by_queue.get("probe_queue") == 1.0, by_queue
        # The TaskQQueueUnserved join contract: a queue with no live worker
        # has NO live_workers series at all, not a 0-valued one.
        assert "ghost_queue" not in by_queue, by_queue

    def test_stranded_by_actor_and_reason(self, live: Exposition) -> None:
        series = live.series("taskq_jobs_stranded")
        assert series, "the stranded gauge is absent despite seeded stranded rows"
        for s in series:
            labels = _scopeless(s.labels)
            assert "actor" in labels and "reason" in labels, labels
            assert labels["reason"] in ("no_actor_config", "unserved_queue"), labels
        reasons = {_scopeless(s.labels)["reason"] for s in series}
        assert "unserved_queue" in reasons, (
            "the ghost-queue rows must surface as stranded{reason=unserved_queue}"
        )
        assert "no_actor_config" in reasons, (
            "the deleted-actor_config rows must surface as "
            f"stranded{{reason=no_actor_config}}: {[_scopeless(s.labels) for s in series]}"
        )

    def test_by_status_counts_seeded_rows(self, live: Exposition) -> None:
        series = live.series("taskq_jobs_by_status")
        assert series, "the by-status gauge is absent from the live scrape"
        by_status = {_scopeless(s.labels).get("status"): s.value for s in series}
        assert by_status.get("pending", 0) >= 2.0, by_status
        for status in by_status:
            assert status in ("pending", "scheduled", "running"), (
                f"terminal status {status} sampled: the doc promises live only"
            )

    def test_sweep_last_success_stamped_per_sweep(self, live: Exposition) -> None:
        series = live.series("taskq_maintenance_leader_sweep_last_success_seconds")
        assert series, "the sweep-last-success gauge is absent from the live scrape"
        sweep_names = {_scopeless(s.labels).get("sweep_name") for s in series}
        assert "scheduled_to_pending" in sweep_names, sweep_names
        start = live.series("process_start_time_seconds")[0].value
        stamps = [s.value for s in series]
        assert all(stamp > start for stamp in stamps), (
            "sweep stamps are not wall-clock epoch seconds (the "
            "promotion-stall alert computes time() - stamp): the series must "
            "carry the same clock domain Prometheus's time() reads"
        )

    def test_sweep_batch_size_pair_emitted_together(self, live: Exposition) -> None:
        for name in (
            "taskq_maintenance_leader_sweep_batch_size",
            "taskq_maintenance_leader_sweep_batch_size_configured",
        ):
            series = live.series(name)
            assert series, f"{name} absent from the live scrape"
            assert all("sweep_name" in _scopeless(s.labels) for s in series), (
                f"{name} lost its sweep_name label"
            )
        actual = {
            _scopeless(s.labels)["sweep_name"]: s.value
            for s in live.series("taskq_maintenance_leader_sweep_batch_size")
        }
        configured = {
            _scopeless(s.labels)["sweep_name"]: s.value
            for s in live.series("taskq_maintenance_leader_sweep_batch_size_configured")
        }
        assert actual.keys() == configured.keys(), (
            "the sweep-degraded alert joins the two series label-matched; "
            f"actual={actual} configured={configured}"
        )
        assert all(0 < v <= configured[k] for k, v in actual.items()), (actual, configured)


class TestDurations:
    """The dispatch/process duration histograms and queue_wait."""

    def test_dispatch_duration_histogram(self, live: Exposition) -> None:
        count = live.series("taskq_dispatch_duration_seconds_count")
        assert count, "dispatch duration histogram absent despite a minute of dispatch rounds"
        assert all(_scopeless(s.labels).get("queue") == "probe_queue" for s in count), (
            "dispatch duration lost its queue label"
        )
        total = count[0].value
        assert total >= 10, (
            f"only {total} dispatch samples for a worker that dispatched for a minute"
        )
        sums = live.series("taskq_dispatch_duration_seconds_sum")
        assert sums[0].value > 0, "dispatch sum is zero with positive counts"
        mean = sums[0].value / total
        assert mean < 1.0, f"mean dispatch latency {mean:.3f}s is not SQL-execution scale"

    def test_process_duration_by_outcome(self, live: Exposition) -> None:
        counts = live.series("messaging_process_duration_seconds_count")
        assert counts, "process duration histogram absent"
        by_outcome: dict[str, float] = {}
        for s in counts:
            labels = _scopeless(s.labels)
            assert {"actor", "queue", "outcome"} <= labels.keys(), labels
            by_outcome[labels["outcome"]] = by_outcome.get(labels["outcome"], 0) + s.value
        assert by_outcome.get("succeeded", 0) >= 3.0, by_outcome
        assert by_outcome.get("failed", 0) >= 1.0, by_outcome
        assert by_outcome.get("scheduled", 0) >= 1.0, by_outcome

    def test_queue_wait_by_actor_and_queue(self, live: Exposition) -> None:
        counts = live.series("taskq_jobs_queue_wait_seconds_count")
        assert counts, "queue_wait histogram absent from the live scrape"
        for s in counts:
            labels = _scopeless(s.labels)
            assert {"actor", "queue"} <= labels.keys(), labels
        actors = {_scopeless(s.labels)["actor"] for s in counts}
        assert {"probe_ok_actor", "probe_fail_actor", "probe_retry_actor"} <= actors, actors
        sums = live.series("taskq_jobs_queue_wait_seconds_sum")
        assert sums, "queue_wait sum absent"
        assert all(s.value > 0 for s in sums), "waited seconds must be positive"


class TestConsumedAndBackpressure:
    def test_published_messages_per_actor(self, live: Exposition) -> None:
        series = live.series("messaging_client_published_messages_total")
        assert series, "published counter absent from the live scrape"
        published = {_scopeless(s.labels)["actor"]: s.value for s in series}
        assert published.get("probe_ok_actor") == 5.0, published  # 3 + 2 post-delete
        assert published.get("probe_ghost_actor") == 2.0, published

    def test_consumed_outcomes(self, live: Exposition) -> None:
        series = live.series("messaging_client_consumed_messages_total")
        assert series, "consumed counter absent from the live scrape"
        seen: set[tuple[str, str]] = set()
        for s in series:
            labels = _scopeless(s.labels)
            assert {"actor", "queue", "outcome"} <= labels.keys(), labels
            seen.add((labels["actor"], labels["outcome"]))
        assert ("probe_ok_actor", "succeeded") in seen, seen
        assert ("probe_fail_actor", "failed") in seen, seen
        assert ("probe_retry_actor", "scheduled") in seen, seen
        assert ("probe_retry_actor", "failed") in seen, seen

    def test_attempt_failures_split_retryable(self, live: Exposition) -> None:
        series = live.series("taskq_jobs_attempt_failures_total")
        assert series, "attempt-failures counter absent from the live scrape"
        seen: set[tuple[str, str, str]] = set()
        for s in series:
            labels = _scopeless(s.labels)
            assert {"actor", "error_type", "retryable"} <= labels.keys(), labels
            seen.add((labels["actor"], labels["error_type"], labels["retryable"]))
        assert ("probe_fail_actor", "ValueError", "false") in seen, seen
        assert ("probe_retry_actor", "ConnectionError", "true") in seen, seen

    def test_backpressure_errors_by_kind(self, live: Exposition) -> None:
        series = live.series("taskq_backpressure_errors_total")
        assert series, "backpressure counter absent despite 3 real refusals"
        for s in series:
            labels = _scopeless(s.labels)
            assert {"actor", "kind"} <= labels.keys(), labels
        max_pending = [
            s
            for s in series
            if _scopeless(s.labels).get("kind") == "max_pending"
            and _scopeless(s.labels).get("actor") == "probe_backpressure_actor"
        ]
        assert max_pending and max_pending[0].value == 3.0

    def test_timeout_kind_start_to_close(self, live: Exposition) -> None:
        series = live.series("taskq_jobs_timeouts_total")
        assert series, "timeouts counter absent despite a real start_to_close hit"
        kinds = {_scopeless(s.labels).get("kind") for s in series}
        assert "start_to_close" in kinds, kinds
        assert any(_scopeless(s.labels).get("actor") == "probe_timeout_actor" for s in series), [
            _scopeless(s.labels) for s in series
        ]


class TestCronCounters:
    """The cron counters, driven through the real three-strike auto-disable."""

    def test_consecutive_failures_updown_by_actor(self, live: Exposition) -> None:
        series = live.series("taskq_cron_consecutive_failures")
        assert series, "cron consecutive-failures up-down counter absent"
        by_actor = {_scopeless(s.labels).get("actor"): s.value for s in series}
        assert by_actor.get("probe_fail_actor") == 3.0, by_actor

    def test_disabled_schedules_after_three_strikes(self, live: Exposition) -> None:
        series = live.series("taskq_cron_disabled_schedules")
        assert series, "disabled-schedules gauge absent"
        assert series[0].value == 1.0, (
            "the failing factory schedule was not auto-disabled by the run's own three-strike rule"
        )

    def test_budget_deferrals_from_the_monopolizer_shape(self, live: Exposition) -> None:
        """Three slow factories drain their backlogs burning the tick's
        funded budget each tick; the fourth schedule's fires defer."""
        series = live.series("taskq_cron_budget_deferrals_total")
        assert series, (
            "the budget-deferral counter is absent: the four backlogged "
            "slow-factory schedules must defer the fourth's fires while "
            "three monopolize the funded budget"
        )
        assert any(s.value >= 1.0 for s in series), series

    def test_cron_lock_contention_during_a_held_cron_lock(self, live: Exposition) -> None:
        """A session holding the cron advisory lock's own key makes every
        leader tick's try-lock lose - the real contention emission."""
        series = live.series("taskq_cron_lock_contention_total")
        assert series, (
            "the held cron advisory lock made leader ticks contend, but the "
            "contention counter never moved"
        )
        assert series[0].value >= 1.0, series[0].value


class TestHeartbeatFamily:
    def test_tick_duration_histogram(self, live: Exposition) -> None:
        counts = live.series("taskq_heartbeat_tick_duration_seconds_count")
        assert counts, "heartbeat tick duration absent from a 70s run at 1s interval"
        assert counts[0].value >= 20, counts[0].value

    def test_consecutive_failures_gauge_resets_to_zero(self, live: Exposition) -> None:
        series = live.series("taskq_heartbeat_consecutive_failures")
        assert series, "the consecutive-failures gauge is absent"
        assert series[0].value == 0.0, series[0].value

    def test_lock_expires_histogram_has_documented_buckets(self, live: Exposition) -> None:
        buckets = live.series("taskq_lock_expires_in_seconds_bucket")
        assert buckets, (
            "lock-expires histogram absent: the probe held a 12s job while "
            "the heartbeat renewed it - renewals must stamp samples"
        )
        les = {s.labels.get("le") for s in buckets}
        assert {0.0, 30.0, 60.0} <= {float(le) for le in les}, (
            f"the documented buckets 0..60s are not served: {sorted(les)}"
        )

    def test_misses_counter_after_real_pg_failure(
        self, hostile_mid: Exposition, hostile_recovered: Exposition
    ) -> None:
        """A real transient Postgres failure (the server terminating the
        worker's connections across several heartbeat ticks) drives the
        miss counter end to end - and the counter OUTLIVES the incident,
        which is what makes TaskQHeartbeatMisses evaluatable after it.

        The consecutive-failures gauge's post-storm value is 0 when the
        worker survived (the reset-on-success contract, pinned by the
        heartbeat unit tests) and frozen above zero when the storm
        tripped the designed isolate/fail-fast exits - both are
        documented outcomes, so only the counter is pinned here.
        """
        series = hostile_mid.series("taskq_heartbeat_misses_total")
        assert series, "heartbeat-misses counter absent after a real transient PG failure"
        assert series[0].value >= 1.0, (
            "the server terminated the worker's connections across several "
            f"heartbeat ticks and no miss was counted: {series}"
        )
        # The counter only ever moved forward: the post-incident scrape
        # carries at least the mid-chaos count.
        final_misses = hostile_recovered.series("taskq_heartbeat_misses_total")
        assert final_misses and final_misses[0].value >= series[0].value


class TestCancellationFamily:
    """The cancel path's counters, through the real abandonment."""

    def test_abandoned_counter_after_graces_lapse(self, live: Exposition) -> None:
        series = live.series("taskq_jobs_abandoned_total")
        assert series, (
            "the abandoned counter is absent: the operator cancel against "
            "the cancellation-swallowing actor must outlast both 1s graces "
            "and land mark_abandoned"
        )
        assert any(
            _scopeless(s.labels).get("actor") == "probe_uncancellable_actor" and s.value == 1.0
            for s in series
        ), [_scopeless(s.labels) for s in series]

    def test_cancellation_requested_per_call(self, live: Exposition) -> None:
        series = live.series("taskq_cancellation_requested_total")
        assert series and series[0].value == 1.0, series

    def test_phase_transitions_recorded(self, live: Exposition) -> None:
        series = live.series("taskq_cancellation_phase_transitions_total")
        assert series, "phase transitions absent from a run with a full cancel escalation"
        assert sum(s.value for s in series) >= 1.0


class TestRateLimitFamily:
    """The rate-limit failure counters, with the limiter's Redis store
    pointed at the probe's closed port."""

    def test_acquire_dependency_failures_failed_closed(self, live: Exposition) -> None:
        series = live.series("taskq_ratelimit_acquire_dependency_failures_total")
        assert series, (
            "the rate-limited actor's acquires against the dead Redis store "
            "must land on the dependency-failure counter"
        )
        assert any(_scopeless(s.labels).get("error_type") for s in series), [
            _scopeless(s.labels) for s in series
        ]

    def test_denials_surfaced_with_source(self, live: Exposition) -> None:
        series = live.series("taskq_reservation_denials_total")
        assert series, "the failed-closed dependency denial must surface as a reservation denial"
        sources = {_scopeless(s.labels).get("source") for s in series}
        assert "rate_limit" in sources, sources


class TestProgressPublishFailures:
    def test_publish_failures_by_channel_and_error(self, live: Exposition) -> None:
        """The progress actor's ctx.progress calls publish over a dead
        Redis (the probe's TASKQ_REDIS_URL): the real catch site records
        one failure per channel per failed round trip."""
        series = live.series("taskq_progress_publish_failures_total")
        assert series, (
            "progress publish failures absent despite a dead Redis and real progress calls"
        )
        channels = {_scopeless(s.labels).get("channel") for s in series}
        assert {"per_job", "global"} <= channels, channels
        assert all(_scopeless(s.labels).get("error_type") for s in series), (
            "a publish-failure series without its error_type label"
        )


class TestWatchdogFamily:
    def test_event_loop_lag_histogram(self, live: Exposition) -> None:
        counts = live.series("taskq_worker_event_loop_lag_seconds_count")
        assert counts, "event-loop lag histogram absent from the live scrape"
        assert counts[0].value >= 10, counts[0].value

    def test_loop_tick_age_by_loop(self, live: Exposition) -> None:
        series = live.series("taskq_worker_loop_tick_age_seconds")
        assert series, "loop tick ages absent from the live scrape"
        loops = {_scopeless(s.labels).get("loop") for s in series}
        assert "heartbeat" in loops, loops

    def test_shutdown_duration_recorded_on_clean_exit(self, final: Exposition) -> None:
        counts = final.series("taskq_worker_shutdown_duration_seconds_count")
        assert counts, "shutdown duration absent after a clean SIGTERM exit"
        assert counts[0].value == 1.0
        assert final.series("taskq_worker_shutdown_duration_seconds_sum")[0].value > 0


class TestFleetHandover:
    def test_lock_contention_recorded_by_the_losing_side(self, follower: Exposition) -> None:
        """A second worker electing while the first holds leadership: the
        losing side's first sight of a holder records the contention."""
        series = follower.series("taskq_leader_lock_contention_total")
        assert series, (
            "the follower worker never recorded maintenance-lock contention "
            "despite losing every election to a live leader"
        )
        assert any(_scopeless(s.labels).get("lock") for s in series), (
            "a contention series without its lock label"
        )
        elections = follower.series("taskq_leader_election_attempts_total")
        assert elections and elections[0].value >= 1.0, elections


class TestScrapePathsAgree:
    def test_port_listener_serves_the_taskq_families_the_bridge_serves(
        self, worker_scrapes: dict[str, str]
    ) -> None:
        """The worker's TASKQ_METRICS_PORT listener and the bridge router
        read the same registry: a family on one and not the other is a
        scrape-path discrepancy an operator's dashboards would inherit."""
        for tag in ("LIVE", "FINAL"):
            bridge = parse_exposition(worker_scrapes[tag])
            port = parse_exposition(worker_scrapes[f"{tag}.port"])
            taskq_bridge = {n for n in bridge.names() if n.startswith(("taskq_", "messaging_"))}
            taskq_port = {n for n in port.names() if n.startswith(("taskq_", "messaging_"))}
            assert taskq_bridge == taskq_port, (
                f"{tag}: family sets diverge between the bridge router and "
                f"the metrics-port listener: bridge-only={sorted(taskq_bridge - taskq_port)} "
                f"port-only={sorted(taskq_port - taskq_bridge)}"
            )


# ── 2. the alert rules fire (promtool, fed by real names/labels) ──


def _humanize(value: float) -> str:
    """PromQL's humanize template function, for the integer-scaled values
    these tests feed (all < 1000, so plain integer rendering)."""
    assert 0 <= abs(value) < 1000, f"test values must stay below 1000: {value}"
    if value == int(value):
        return str(int(value))
    return str(value)


def _expand_templates(text: str, labels: dict[str, str], value: float) -> str:
    expanded = re.sub(r"{{\s*\$labels\.(\w+)\s*}}", lambda m: labels.get(m.group(1), ""), text)
    return re.sub(r"{{\s*\$value\s*}}", _humanize(value), expanded)


def _render_labels(labels: dict[str, str]) -> str:
    if not labels:
        return ""
    inner = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
    return "{" + inner + "}"


#: The result-vector labels each alert's expression produces, and the
#: $value its annotations render (exact only where a template uses it).
_ALERT_RESULT: dict[str, tuple[dict[str, str], float]] = {
    "TaskQQueueDepthHigh": ({"actor": "probe_ok_actor", "queue": "probe_queue"}, 950.0),
    "TaskQHeartbeatMisses": ({}, 0.0167),
    "TaskQFailedJobRateHigh": ({}, 0.5),
    "TaskQRetryRateHigh": ({}, 1.0),
    "TaskQAbandonedJobs": ({"actor": "probe_uncancellable_actor"}, 0.0167),
    "TaskQLockExpiringSoon": ({}, 0.0),
    "TaskQLeaderSplitBrainOrNoLeader": ({}, 2.0),
    "TaskQDispatchLatencyHigh": ({"queue": "probe_queue"}, 0.095),
    "TaskQProgressPublishFailures": (
        {"channel": "per_job", "error_type": "ConnectionError"},
        0.0167,
    ),
    "TaskQCronScheduleDisabled": ({}, 2.0),
    "TaskQCronSkippedSlots": ({"actor": "probe_fail_actor"}, 1.0),
    "TaskQScheduledBacklogGrowing": ({}, 600.0),
    "TaskQPromotionStalled": ({"sweep_name": "scheduled_to_pending"}, 380.0),
    "TaskQSweepTimeouts": ({"sweep_name": "scheduled_to_pending"}, 0.0167),
    "TaskQSweepUnexpectedErrors": ({"sweep_name": "scheduled_to_pending"}, 0.0167),
    "TaskQSweepDegraded": ({"sweep_name": "scheduled_to_pending"}, 25.0),
    "TaskQLeaderLockContention": ({}, 0.0167),
    "TaskQRateLimitDependencyOutage": ({"error_type": "ConnectionError"}, 0.0167),
    "TaskQCronLockContention": ({}, 0.0167),
    "TaskQCronBudgetDeferrals": ({"actor": "probe_fail_actor"}, 0.0167),
    "TaskQQueueUnserved": ({"queue": "ghost_queue"}, 7.0),
    "TaskQStrandedJobs": ({"actor": "probe_ghost_actor", "reason": "unserved_queue"}, 4.0),
    "TaskQRunningLeaseExpired": ({}, 3.0),
    # T08: the blocked-stuck alert ships WITH the gauge (GAPS-ESTATE F4 —
    # a metric nobody alerts on is a decoration). The pathology: a run's
    # blocked-node count pinned above zero past the 30m bound. The rule's
    # expression is a bare selector (no aggregation), so the result
    # vector carries the series' FULL label set — state included.
    "TaskQWorkflowBlockedStuck": ({"workflow": "probe_wf", "state": "blocked"}, 3.0),
}


def _firing_case(
    alert: str, inputs: list[tuple[str, dict[str, str], str]], eval_time: str
) -> dict[str, Any]:
    """One promtool test entry asserting *alert* FIRES, with the rule's own
    annotations template-expanded against the fed pathology."""
    rules_data = yaml.safe_load(RULES_PATH.read_text())
    shipped = next(r for r in rules_data["groups"][0]["rules"] if r.get("alert") == alert)
    result_labels, result_value = _ALERT_RESULT[alert]
    exp_labels = {**result_labels, **shipped.get("labels", {})}
    exp_annotations = {
        key: _expand_templates(text, exp_labels, result_value)
        for key, text in shipped.get("annotations", {}).items()
    }
    return {
        "interval": "1m",
        "input_series": [
            {"series": f"{name}{_render_labels(labels)}", "values": values}
            for name, labels, values in inputs
        ],
        "alert_rule_test": [
            {
                "eval_time": eval_time,
                "alertname": alert,
                "exp_alerts": [{"exp_labels": exp_labels, "exp_annotations": exp_annotations}],
            }
        ],
    }


def _silent_case(
    alert: str, inputs: list[tuple[str, dict[str, str], str]], eval_time: str
) -> dict[str, Any]:
    """One promtool test entry asserting *alert* stays silent on the
    healthy shape (the false-page guard)."""
    return {
        "interval": "1m",
        "input_series": [
            {"series": f"{name}{_render_labels(labels)}", "values": values}
            for name, labels, values in inputs
        ],
        "alert_rule_test": [{"eval_time": eval_time, "alertname": alert, "exp_alerts": []}],
    }


def _histogram_feed(
    live: Exposition,
    base: str,
    *,
    mass: str,
    extra_labels: dict[str, str] | None = None,
) -> list[tuple[str, dict[str, str], str]]:
    """Feed a histogram's REAL bucket label set (the exact ``le`` strings
    the live scrape renders) with all sample mass concentrated per *mass*:

    - ``"high"``: in the last finite bucket (cumulative counts consistent,
      p99 interpolates up to that boundary);
    - ``"first_positive"``: in the first boundary above zero - the bucket
      a healthy sub-unit signal lands in;
    - ``"zero"``: at the zero boundary, the lowest value the histogram
      can represent.
    """
    rendered: list[tuple[float, str]] = sorted(
        (float(s.labels["le"]), s.labels["le"])
        for s in live.series(f"{base}_bucket")
        if "le" in s.labels
    )
    assert rendered, f"no {base}_bucket series in the live scrape"
    if mass == "high":
        mass_le = rendered[-1][0] if rendered[-1][0] != float("inf") else rendered[-2][0]
    elif mass == "first_positive":
        mass_le = next(le for le, _ in rendered if le > 0.0)
    else:  # "zero"
        mass_le = rendered[0][0]
    feeds: list[tuple[str, dict[str, str], str]] = []
    for le, rendered_le in rendered:
        labels = dict(extra_labels or {})
        labels["le"] = rendered_le
        feeds.append((f"{base}_bucket", labels, "0+100x30" if le >= mass_le else "0+0x30"))
    return feeds


def _build_promtool_cases(live: Exposition) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    feed = _histogram_feed

    cases.append(
        _firing_case(
            "TaskQQueueDepthHigh",
            [
                (
                    "taskq_jobs_oldest_pending_age_seconds",
                    {"actor": "probe_ok_actor", "queue": "probe_queue"},
                    "950+0x30",
                ),
            ],
            "8m",
        )
    )
    cases.append(
        _silent_case(
            "TaskQQueueDepthHigh",
            [
                (
                    "taskq_jobs_oldest_pending_age_seconds",
                    {"actor": "probe_ok_actor", "queue": "probe_queue"},
                    "60+0x30",
                ),
            ],
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQHeartbeatMisses",
            [
                ("taskq_heartbeat_misses_total", {}, "0+1x30"),
            ],
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQFailedJobRateHigh",
            [
                (
                    "messaging_client_consumed_messages_total",
                    {"actor": "probe_fail_actor", "queue": "probe_queue", "outcome": "failed"},
                    "0+100x30",
                ),
                (
                    "messaging_client_consumed_messages_total",
                    {"actor": "probe_ok_actor", "queue": "probe_queue", "outcome": "succeeded"},
                    "0+100x30",
                ),
            ],
            "8m",
        )
    )
    cases.append(
        _silent_case(
            "TaskQFailedJobRateHigh",
            [
                (
                    "messaging_client_consumed_messages_total",
                    {"actor": "probe_ok_actor", "queue": "probe_queue", "outcome": "succeeded"},
                    "0+100x30",
                ),
            ],
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQRetryRateHigh",
            [
                (
                    "taskq_jobs_attempt_failures_total",
                    {
                        "actor": "probe_retry_actor",
                        "error_type": "ConnectionError",
                        "retryable": "true",
                    },
                    "0+100x30",
                ),
                (
                    "messaging_client_consumed_messages_total",
                    {"actor": "probe_ok_actor", "queue": "probe_queue", "outcome": "succeeded"},
                    "0+100x30",
                ),
            ],
            "14m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQAbandonedJobs",
            [
                ("taskq_jobs_abandoned_total", {"actor": "probe_uncancellable_actor"}, "0+1x30"),
            ],
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQLockExpiringSoon", feed(live, "taskq_lock_expires_in_seconds", mass="zero"), "8m"
        )
    )
    cases.append(
        _silent_case(
            "TaskQLockExpiringSoon", feed(live, "taskq_lock_expires_in_seconds", mass="high"), "8m"
        )
    )

    cases.append(
        _firing_case(
            "TaskQLeaderSplitBrainOrNoLeader",
            [
                ("taskq_maintenance_leader_is_leader", {"worker_id": "w1"}, "1+0x30"),
                ("taskq_maintenance_leader_is_leader", {"worker_id": "w2"}, "1+0x30"),
            ],
            "5m",
        )
    )
    cases.append(
        _silent_case(
            "TaskQLeaderSplitBrainOrNoLeader",
            [
                ("taskq_maintenance_leader_is_leader", {"worker_id": "w1"}, "1+0x30"),
            ],
            "5m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQDispatchLatencyHigh",
            feed(
                live,
                "taskq_dispatch_duration_seconds",
                mass="high",
                extra_labels={"queue": "probe_queue"},
            ),
            "8m",
        )
    )
    cases.append(
        _silent_case(
            "TaskQDispatchLatencyHigh",
            feed(
                live,
                "taskq_dispatch_duration_seconds",
                mass="first_positive",
                extra_labels={"queue": "probe_queue"},
            ),
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQProgressPublishFailures",
            [
                (
                    "taskq_progress_publish_failures_total",
                    {"channel": "per_job", "error_type": "ConnectionError"},
                    "0+1x30",
                ),
            ],
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQCronScheduleDisabled",
            [
                ("taskq_cron_disabled_schedules", {}, "2+0x30"),
            ],
            "1m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQScheduledBacklogGrowing",
            [
                ("taskq_jobs_oldest_due_age_seconds", {}, "600+0x30"),
                ("taskq_jobs_scheduled_count", {}, "0+5x30"),
            ],
            "14m",
        )
    )
    # Healthy: the count is DRAINING while a straggler ages past the
    # threshold - the exact false-positive shape the rule's own
    # description says must stay silent.
    cases.append(
        _silent_case(
            "TaskQScheduledBacklogGrowing",
            [
                ("taskq_jobs_oldest_due_age_seconds", {}, "600+0x30"),
                ("taskq_jobs_scheduled_count", {}, "50-2x30"),
            ],
            "14m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQPromotionStalled",
            [
                (
                    "taskq_maintenance_leader_sweep_last_success_seconds",
                    {"sweep_name": "scheduled_to_pending"},
                    "100+0x30",
                ),
            ],
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQSweepTimeouts",
            [
                (
                    "taskq_maintenance_leader_sweep_timeouts_total",
                    {"sweep_name": "scheduled_to_pending"},
                    "0+1x30",
                ),
            ],
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQSweepUnexpectedErrors",
            [
                (
                    "taskq_maintenance_leader_sweep_unexpected_errors_total",
                    {"sweep_name": "scheduled_to_pending"},
                    "0+1x30",
                ),
            ],
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQSweepDegraded",
            [
                (
                    "taskq_maintenance_leader_sweep_batch_size",
                    {"sweep_name": "scheduled_to_pending"},
                    "25+0x30",
                ),
                (
                    "taskq_maintenance_leader_sweep_batch_size_configured",
                    {"sweep_name": "scheduled_to_pending"},
                    "100+0x30",
                ),
            ],
            "1m",
        )
    )
    cases.append(
        _silent_case(
            "TaskQSweepDegraded",
            [
                (
                    "taskq_maintenance_leader_sweep_batch_size",
                    {"sweep_name": "scheduled_to_pending"},
                    "100+0x30",
                ),
                (
                    "taskq_maintenance_leader_sweep_batch_size_configured",
                    {"sweep_name": "scheduled_to_pending"},
                    "100+0x30",
                ),
            ],
            "1m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQLeaderLockContention",
            [
                ("taskq_leader_lock_contention_total", {"lock": "maintenance"}, "0+1x30"),
                ("taskq_maintenance_leader_is_leader", {"worker_id": "w1"}, "0+0x30"),
            ],
            "14m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQRateLimitDependencyOutage",
            [
                (
                    "taskq_ratelimit_acquire_dependency_failures_total",
                    {"error_type": "ConnectionError"},
                    "0+1x30",
                ),
            ],
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQCronLockContention",
            [
                ("taskq_cron_lock_contention_total", {}, "0+1x30"),
            ],
            "14m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQCronBudgetDeferrals",
            [
                ("taskq_cron_budget_deferrals_total", {"actor": "probe_fail_actor"}, "0+1x30"),
            ],
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQCronSkippedSlots",
            [
                # The emitter-staged shape: no live run reaches the skip
                # branch (the 1-hour default catch-up window swallows the
                # probes' staged backlog), so the series comes from the
                # emitter probe's real record_cron_skipped_slots call.
                ("taskq_cron_skipped_slots_total", {"actor": "probe_fail_actor"}, "0+1x30"),
            ],
            "14m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQQueueUnserved",
            [
                ("taskq_queue_depth", {"queue": "ghost_queue"}, "7+0x30"),
                # No live_workers series for ghost_queue: the unserved shape.
            ],
            "5m",
        )
    )
    cases.append(
        _silent_case(
            "TaskQQueueUnserved",
            [
                ("taskq_queue_depth", {"queue": "probe_queue"}, "7+0x30"),
                ("taskq_queue_live_workers", {"queue": "probe_queue"}, "1+0x30"),
            ],
            "5m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQStrandedJobs",
            [
                (
                    "taskq_jobs_stranded",
                    {"actor": "probe_ghost_actor", "reason": "unserved_queue"},
                    "4+0x30",
                ),
            ],
            "8m",
        )
    )
    cases.append(
        _silent_case(
            "TaskQStrandedJobs",
            [
                (
                    "taskq_jobs_stranded",
                    {"actor": "probe_ghost_actor", "reason": "unserved_queue"},
                    "0+0x30",
                ),
            ],
            "8m",
        )
    )

    cases.append(
        _firing_case(
            "TaskQRunningLeaseExpired",
            [
                ("taskq_jobs_running_lease_expired", {}, "3+0x30"),
            ],
            "8m",
        )
    )
    cases.append(
        _silent_case(
            "TaskQRunningLeaseExpired",
            [
                ("taskq_jobs_running_lease_expired", {}, "0+0x30"),
            ],
            "8m",
        )
    )

    # T08 — TaskQWorkflowBlockedStuck: the alert suggestion shipped WITH
    # the gauge, evaluated through the honest harness: the pathology
    # (blocked nodes pinned past the 30m bound) staged through promtool;
    # the silent side is the healthy fleet (no blocked nodes).
    cases.append(
        _firing_case(
            "TaskQWorkflowBlockedStuck",
            [
                (
                    "taskq_wf_progress_nodes_total",
                    {"workflow": "probe_wf", "state": "blocked"},
                    "3+0x35",
                ),
            ],
            "32m",
        )
    )
    cases.append(
        _silent_case(
            "TaskQWorkflowBlockedStuck",
            [
                (
                    "taskq_wf_progress_nodes_total",
                    {"workflow": "probe_wf", "state": "blocked"},
                    "0+0x35",
                ),
            ],
            "32m",
        )
    )

    return cases


def _feed_single_bucket(
    live: Exposition,
    base: str,
    target_le: float,
    *,
    extra_labels: dict[str, str] | None = None,
    count: str = "0+100x30",
) -> list[tuple[str, dict[str, str], str]]:
    """Feed a histogram's REAL bucket label set with ALL sample mass inside
    the single bucket whose upper bound is *target_le* (rendered exactly as
    the live scrape renders it): the marginal-threshold shapes - mass in
    the bucket just below or just above an alert's quantile line."""
    rendered: list[tuple[float, str]] = sorted(
        (float(s.labels["le"]), s.labels["le"])
        for s in live.series(f"{base}_bucket")
        if "le" in s.labels
    )
    assert rendered, f"no {base}_bucket series in the live scrape"
    assert any(le == target_le for le, _ in rendered), (
        f"{base}: the live scrape renders no le={target_le} boundary "
        f"(served: {[le for le, _ in rendered]})"
    )
    feeds: list[tuple[str, dict[str, str], str]] = []
    for le, rendered_le in rendered:
        labels = dict(extra_labels or {})
        labels["le"] = rendered_le
        feeds.append((f"{base}_bucket", labels, count if le >= target_le else "0+0x30"))
    return feeds


def _marginal_case(
    alert: str,
    inputs: list[tuple[str, dict[str, str], str]],
    eval_time: str,
    *,
    fires: bool,
    value: float = 0.0,
) -> dict[str, Any]:
    """One promtool test entry for a MARGINAL shape (just under or just
    over a rule's threshold line): asserts the rule crosses the documented
    line exactly - no firing below it, no silence above it."""
    rules_data = yaml.safe_load(RULES_PATH.read_text())
    shipped = next(r for r in rules_data["groups"][0]["rules"] if r.get("alert") == alert)
    result_labels, _ = _ALERT_RESULT[alert]
    exp_labels = {**result_labels, **shipped.get("labels", {})}
    entry: dict[str, Any] = {"eval_time": eval_time, "alertname": alert}
    if fires:
        exp_annotations = {
            key: _expand_templates(text, exp_labels, value)
            for key, text in shipped.get("annotations", {}).items()
        }
        entry["exp_alerts"] = [{"exp_labels": exp_labels, "exp_annotations": exp_annotations}]
    else:
        entry["exp_alerts"] = []
    return {
        "interval": "1m",
        "input_series": [
            {"series": f"{name}{_render_labels(labels)}", "values": values}
            for name, labels, values in inputs
        ],
        "alert_rule_test": [entry],
    }


@pytest.mark.skipif(
    not docker_available(),
    reason="promtool runs in the prom/prometheus container; Docker unreachable",
)
def test_threshold_margins_fire_exactly_at_the_documented_line(
    live: Exposition, tmp_path: Any
) -> None:
    """Marginal shapes, just under and just over three rules' threshold
    lines - the DispatchLatencyHigh lesson applied to the thresholds
    themselves: a threshold that moves when a healthy shape approaches it
    (or sits still when a violating shape crosses it) is a false page or a
    missed page waiting for scale.

    - TaskQDispatchLatencyHigh (> 50 ms): every dispatch in the le=0.05
      bucket - the healthy worst case just UNDER the line - must stay
      SILENT (the served p99 interpolates to 0.04975, not 50 ms); every
      dispatch in the le=0.1 bucket - the first shape OVER the line - must
      FIRE. The bucket scale puts the decision exactly at the 50 ms edge.
    - TaskQLockExpiringSoon (p99 < 30 s): mass in the le=45 bucket (a
      healthy default fleet, remaining ~ lease 60 - interval 10) stays
      silent. AND the found cliff: mass in the le="30" bucket - a HEALTHY
      minimal-valid lease configuration (heartbeat_interval 5s, lock_lease
      33s, the cascade floor for that cadence: remaining ~28s) - FIRES,
      because the served p99 quantiles to 20 + 10 * 0.99 = 29.9 < 30. This
      case PINS the false-page rather than blessing it: the threshold sits
      exactly on a bucket edge, so any healthy cadence whose lease-minus-
      interval lands in (20, 30] pages forever. Operators running such a
      cadence must override the threshold (the runbook documents the math);
      a threshold fix belongs with its own red proof.
    - TaskQFailedJobRateHigh (> 1%): a fleet at EXACTLY 1% (1 failed per
      100 consumed per interval) must stay silent - the comparison is
      strict - and 2 failed per 199 must FIRE.
    - TaskQQueueDepthHigh (> 900 s): 900 stays silent, 901 fires - the
      documented line is the line.
    """
    cases: list[dict[str, Any]] = []

    # DispatchLatencyHigh: the line is the 50ms bucket edge.
    cases.append(
        _marginal_case(
            "TaskQDispatchLatencyHigh",
            _feed_single_bucket(
                live, "taskq_dispatch_duration_seconds", 0.05, extra_labels={"queue": "probe_queue"}
            ),
            "8m",
            fires=False,
        )
    )
    cases.append(
        _marginal_case(
            "TaskQDispatchLatencyHigh",
            _feed_single_bucket(
                live, "taskq_dispatch_duration_seconds", 0.1, extra_labels={"queue": "probe_queue"}
            ),
            "8m",
            fires=True,
            value=0.0995,
        )
    )

    # LockExpiringSoon: healthy default fleet silent; the (20, 30] cadence
    # cliff fires (documented hazard, see above).
    cases.append(
        _marginal_case(
            "TaskQLockExpiringSoon",
            _feed_single_bucket(live, "taskq_lock_expires_in_seconds", 45),
            "8m",
            fires=False,
        )
    )
    cases.append(
        _marginal_case(
            "TaskQLockExpiringSoon",
            _feed_single_bucket(live, "taskq_lock_expires_in_seconds", 30),
            "8m",
            fires=True,
        )
    )

    # FailedJobRateHigh: exactly-1% silent, just-over fires.
    cases.append(
        _marginal_case(
            "TaskQFailedJobRateHigh",
            [
                (
                    "messaging_client_consumed_messages_total",
                    {"actor": "probe_fail_actor", "queue": "probe_queue", "outcome": "failed"},
                    "0+1x30",
                ),
                (
                    "messaging_client_consumed_messages_total",
                    {"actor": "probe_ok_actor", "queue": "probe_queue", "outcome": "succeeded"},
                    "0+99x30",
                ),
            ],
            "8m",
            fires=False,
        )
    )
    cases.append(
        _marginal_case(
            "TaskQFailedJobRateHigh",
            [
                (
                    "messaging_client_consumed_messages_total",
                    {"actor": "probe_fail_actor", "queue": "probe_queue", "outcome": "failed"},
                    "0+2x30",
                ),
                (
                    "messaging_client_consumed_messages_total",
                    {"actor": "probe_ok_actor", "queue": "probe_queue", "outcome": "succeeded"},
                    "0+197x30",
                ),
            ],
            "8m",
            fires=True,
            value=2.0 / 199.0,
        )
    )

    # QueueDepthHigh: the documented 900s line is the line.
    cases.append(
        _marginal_case(
            "TaskQQueueDepthHigh",
            [
                (
                    "taskq_jobs_oldest_pending_age_seconds",
                    {"actor": "probe_ok_actor", "queue": "probe_queue"},
                    "900+0x30",
                )
            ],
            "8m",
            fires=False,
        )
    )
    cases.append(
        _marginal_case(
            "TaskQQueueDepthHigh",
            [
                (
                    "taskq_jobs_oldest_pending_age_seconds",
                    {"actor": "probe_ok_actor", "queue": "probe_queue"},
                    "901+0x30",
                )
            ],
            "8m",
            fires=True,
            value=901.0,
        )
    )

    test_doc = {
        "rule_files": ["/work/rules.yaml"],
        "evaluation_interval": "1m",
        "tests": cases,
    }
    out = run_promtool_rule_tests(RULES_PATH, yaml.safe_dump(test_doc), tmp_path)
    assert "SUCCESS" in out, out


@pytest.mark.skipif(
    not docker_available(),
    reason="promtool runs in the prom/prometheus container; Docker unreachable",
)
def test_harness_series_are_bound_to_the_served_exposition(
    live: Exposition, follower: Exposition, hostile_mid: Exposition, emitter: Exposition
) -> None:
    """A rule-test harness that hand-types series can drift from the
    emitted truth while every case still passes (a wrong-but-consistent
    name evaluates an empty vector and the SILENT guards still pass). The
    binding pin: every input series name the 35 cases feed must be a name
    a real scrape actually served - the worker probes for everything a
    live worker carries, the emitter probe for the four families whose
    pathology cannot be staged live (the sweep-abort pair, the
    skipped-slots counter no live run reaches: the 1-hour default
    catch-up window swallows the probes' staged backlog, and the
    wf-progress gauge whose observable emission is the maintenance
    leader's admin-surface sample, never a worker scrape) - and the case
    counts must be the honest 24 firing + 11 healthy guards covering
    every shipped rule."""
    emitted = live.names() | follower.names() | hostile_mid.names() | emitter.names()
    cases = _build_promtool_cases(live)
    firing = [c for c in cases if c["alert_rule_test"][0]["exp_alerts"]]
    guards = [c for c in cases if not c["alert_rule_test"][0]["exp_alerts"]]
    assert (len(firing), len(guards)) == (24, 11), (
        f"the harness must stay 24 firing + 11 guards, got {len(firing)} + {len(guards)}"
    )
    for case in cases:
        for input_entry in case["input_series"]:
            name = input_entry["series"].split("{")[0]
            assert name in emitted, (
                f"promtool input series {name!r} is not a series the real "
                "scrapes served - the harness has drifted from the emitted truth"
            )
    # The four emitter-bound families: the fed LABEL VALUES must match
    # what the real emitters serve, not just the names.
    for family, bound_label in (
        ("taskq_maintenance_leader_sweep_timeouts_total", "sweep_name"),
        ("taskq_maintenance_leader_sweep_unexpected_errors_total", "sweep_name"),
        ("taskq_cron_skipped_slots_total", "actor"),
        ("taskq_wf_progress_nodes_total", "workflow"),
    ):
        served_label_values = emitter.label_values(family, bound_label)
        for case in cases:
            for input_entry in case["input_series"]:
                if input_entry["series"].split("{")[0] != family:
                    continue
                fed = dict(re.findall(r'(\w+)="([^"]*)"', input_entry["series"]))
                assert fed.get(bound_label) in served_label_values, (
                    f"{family}: fed {bound_label} {fed.get(bound_label)!r} is not "
                    f"one the real emitters served: {sorted(served_label_values)}"
                )
    # And the firing set covers every shipped alert exactly once.
    rules = yaml.safe_load(RULES_PATH.read_text())["groups"][0]["rules"]
    shipped_names = {r["alert"] for r in rules if "alert" in r}
    fired_names = {c["alert_rule_test"][0]["alertname"] for c in firing}
    assert fired_names == shipped_names, (
        f"unfired shipped rules {sorted(shipped_names - fired_names)}, "
        f"unknown alert names {sorted(fired_names - shipped_names)}"
    )


@pytest.mark.skipif(
    not docker_available(),
    reason="promtool runs in the prom/prometheus container; Docker unreachable",
)
def test_every_alert_rule_fires_on_real_names_and_labels(
    live: Exposition,
    follower: Exposition,
    hostile_mid: Exposition,
    emitter: Exposition,
    tmp_path: Any,
) -> None:
    """Every shipped rule must fire against its pathology, built from the
    metric names + label sets the real worker exposition served; the
    healthy shapes must stay silent."""
    rules_data = yaml.safe_load(RULES_PATH.read_text())

    # Gate: every taskq/messaging series the rules reference is one the
    # real probes emitted (the healthy run for every family a healthy
    # worker carries; the hostile run adds the failure-only counters -
    # taskq_heartbeat_misses_total among them; the emitter probe adds the
    # three cannot-stage-live counters - the two sweep-abort counters
    # whose PG-abort pathology cannot be staged against a live shared
    # schema, and the cron skipped-slots counter no live run reaches -
    # whose emission paths are the real public
    # record_sweep_*/record_cron_skipped_slots API). Label values and
    # PromQL syntax are stripped before scanning; a rule operand the
    # bridge never serves would make the rule unevaluatable against real
    # data.
    emitted = live.names() | follower.names() | hostile_mid.names() | emitter.names()
    for rule in rules_data["groups"][0]["rules"]:
        expr = re.sub(r'"[^"]*"', '""', str(rule["expr"]))
        expr = re.sub(r"\{[^}]*\}", "{}", expr)
        # Range selectors ([5m]) carry durations, not series names.
        expr = re.sub(r"\[[^\]]*\]", "[]", expr)
        # ...so do offset durations (`offset 5m`).
        expr = re.sub(r"offset\s+\d+[smhd]", "offset", expr)
        # Grouping clauses (by/without/on/ignoring) name LABELS, not series.
        expr = re.sub(r"\b(by|without|on|ignoring)\s*\([^)]*\)", r"\1 ()", expr)
        for token in set(re.findall(r"[a-zA-Z_][a-zA-Z0-9_:]*", expr)):
            if token in _PROMQL_KEYWORDS:
                continue
            assert token in emitted, (
                f"rule {rule['alert']} references {token}, which the real scrapes never emitted"
            )
        # The label names the rule matches on must be real too (the
        # emitter-bound families' labels are real the same way — the
        # wf-progress gauge's `state` lives only on the emitter's series).
        for label in set(re.findall(r"(\w+)\s*=\"", str(rule["expr"]))):
            assert (
                any(
                    label in s.labels
                    for family in emitted
                    for s in (
                        live.series(family) or hostile_mid.series(family) or emitter.series(family)
                    )
                )
                or label == "le"
            ), f"rule {rule['alert']} filters on label {label!r}, which no emitted series carries"

    test_doc = {
        "rule_files": ["/work/rules.yaml"],
        "evaluation_interval": "1m",
        "tests": _build_promtool_cases(live),
    }
    out = run_promtool_rule_tests(RULES_PATH, yaml.safe_dump(test_doc), tmp_path)
    assert "SUCCESS" in out, out
