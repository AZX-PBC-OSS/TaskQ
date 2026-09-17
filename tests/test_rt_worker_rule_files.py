"""Drift pins for the alerting rule files and their runbook anchors.

The five new alerts (backlog growth, promotion stall, sweep timeouts,
sweep degraded tier, lock contention) live in BOTH rule files, and their
annotations point at anchors in docs/guides/runbooks.md. Two drift
shapes nothing pins today: an annotation pointing at a runbook anchor
that does not exist (the alert pages and the runbook 404s), and an expr
referencing a series name the bridge never emits (the alert never fires
— silently, because a non-matching series selector is not a Prometheus
error).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[1]
_RULES_YAML = _REPO_ROOT / "src" / "taskq" / "contrib" / "prometheus" / "rules.yaml"
_K8S_RULES_YAML = _REPO_ROOT / "src" / "taskq" / "contrib" / "kubernetes" / "prometheus_rule.yaml"
_RUNBOOKS_MD = _REPO_ROOT / "docs" / "guides" / "runbooks.md"

#: The alerts this initiative added; each annotation must name a runbook.
_NEW_ALERTS = (
    "TaskQScheduledBacklogGrowing",
    "TaskQPromotionStalled",
    "TaskQSweepTimeouts",
    "TaskQSweepDegraded",
    "TaskQLeaderLockContention",
)

#: The alerts the observability burn added on top — the denial/outage
#: family (rate-limit store dependency, cron lock contention, the
#: zombie-running lease gauge) — under the same runbook and emitted-series
#: discipline.
_OUTAGE_ALERTS = (
    "TaskQRateLimitDependencyOutage",
    "TaskQCronLockContention",
    "TaskQRunningLeaseExpired",
)

#: The outage family's severities: the first two are degradation signals
#: (work deferred, not lost — warning, like the sweep family); a SUSTAINED
#: non-zero zombie-running count means reclaim is not draining while
#: health probes stay green — that one is the 3am page.
_OUTAGE_SEVERITIES = {
    "TaskQRateLimitDependencyOutage": "warning",
    "TaskQCronLockContention": "warning",
    "TaskQRunningLeaseExpired": "critical",
}

#: The job-outcome family: terminal-failed share, retried-failure share,
#: and real abandonment. Abandonment is critical (an operator cancel the
#: actor never yielded to); the two shares are degradation signals.
_JOB_OUTCOME_ALERTS = (
    "TaskQFailedJobRateHigh",
    "TaskQRetryRateHigh",
    "TaskQAbandonedJobs",
)

_JOB_OUTCOME_SEVERITIES = {
    "TaskQFailedJobRateHigh": "warning",
    "TaskQRetryRateHigh": "warning",
    "TaskQAbandonedJobs": "critical",
}

#: Every runbook-carrying alert, for the checks that apply to both
#: generations alike.
_ALL_RUNBOOKED_ALERTS = _NEW_ALERTS + _OUTAGE_ALERTS + _JOB_OUTCOME_ALERTS


def _rules_from(path: Path) -> list[dict[str, Any]]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    groups = data["groups"] if "groups" in data else data["spec"]["groups"]
    rules: list[dict[str, Any]] = []
    for group in groups:
        rules.extend(group["rules"])
    return rules


def _runbook_anchors(md_path: Path) -> set[str]:
    """Every heading anchor in the runbook, slugified the way the anchors
    in the file's own cross-references are written (lowercase, spaces to
    dashes)."""
    anchors: set[str] = set()
    for line in md_path.read_text(encoding="utf-8").splitlines():
        heading = re.match(r"^#{1,6}\s+(.+?)\s*$", line)
        if heading:
            anchors.add(heading.group(1).strip().lower().replace(" ", "-"))
    return anchors


def test_new_alert_annotations_point_at_existing_runbook_anchors() -> None:
    """Every runbook link in the runbooked alerts' annotations must resolve
    to a real heading anchor in docs/guides/runbooks.md — in BOTH rule
    files. And every such alert must CARRY a runbook link: an annotation
    that lost its link entirely would otherwise pass vacuously."""
    assert _RUNBOOKS_MD.exists(), f"runbooks.md not found at {_RUNBOOKS_MD}"
    anchors = _runbook_anchors(_RUNBOOKS_MD)

    for rules_path in (_RULES_YAML, _K8S_RULES_YAML):
        rules = _rules_from(rules_path)
        for rule in rules:
            if rule.get("alert") not in _ALL_RUNBOOKED_ALERTS:
                continue
            annotations = rule.get("annotations", {})
            text = " ".join(str(v) for v in annotations.values())
            links = list(re.finditer(r"docs/guides/runbooks\.md#([a-z0-9-]+)", text))
            assert links, (
                f"{rules_path.name}: alert {rule['alert']!r} carries no runbook "
                "link at all — an operator following the alert has nowhere to go"
            )
            for match in links:
                anchor = match.group(1)
                assert anchor in anchors, (
                    f"{rules_path.name}: alert {rule['alert']!r} points at "
                    f"runbook anchor #{anchor}, which does not exist — the "
                    "alert pages and the runbook 404s"
                )


def test_new_alert_exprs_reference_series_the_bridge_emits() -> None:
    """Every taskq_* / messaging_* series name in the runbooked alerts'
    exprs must be a Prometheus name the bridge actually emits (per the
    authoritative _NAME_MAP the scrape tests verify). A typo'd series name
    is not a Prometheus error — the alert just silently never fires."""
    from tests.test_prometheus_metrics import _NAME_MAP

    emitted = {prom_name for _, prom_name in _NAME_MAP}

    for rules_path in (_RULES_YAML, _K8S_RULES_YAML):
        rules = _rules_from(rules_path)
        for rule in rules:
            if rule.get("alert") not in _ALL_RUNBOOKED_ALERTS:
                continue
            expr = str(rule["expr"])
            referenced = set(re.findall(r"\b(?:taskq|messaging)_[a-z0-9_]+", expr))
            assert referenced, (
                f"{rules_path.name}: alert {rule['alert']!r} references no "
                "taskq or messaging series at all — the expr is wrong"
            )
            unknown = referenced - emitted
            assert not unknown, (
                f"{rules_path.name}: alert {rule['alert']!r} references series "
                f"the bridge never emits: {sorted(unknown)} — the alert can "
                "never fire"
            )


def test_sweep_degraded_expr_compares_actual_to_configured_series() -> None:
    """TaskQSweepDegraded must compare the sweep's used batch size against
    the configured-size series the worker emits — in BOTH rule files.

    A hardcoded threshold (any literal) is blind whenever a deployment's
    ``event_writer_batch_size`` is not the default: a worker degraded to
    exactly the configured size of a small-batch deployment would never
    fire, and a healthy worker on a large-batch deployment would fire
    forever. Gauge-to-gauge on the two series the same worker emits for
    the same ``sweep_name`` label is the only form that tracks
    per-worker configuration."""
    for rules_path in (_RULES_YAML, _K8S_RULES_YAML):
        rules = _rules_from(rules_path)
        by_name = {rule["alert"]: rule for rule in rules}
        assert "TaskQSweepDegraded" in by_name, (
            f"{rules_path.name} lost the TaskQSweepDegraded alert"
        )
        expr = str(by_name["TaskQSweepDegraded"]["expr"])
        referenced = set(re.findall(r"\btaskq_[a-z0-9_]+", expr))
        assert referenced == {
            "taskq_maintenance_leader_sweep_batch_size",
            "taskq_maintenance_leader_sweep_batch_size_configured",
        }, (
            f"{rules_path.name}: the TaskQSweepDegraded expr must compare exactly the "
            f"used-size and configured-size series (gauge to gauge); saw {sorted(referenced)} "
            "— a literal threshold cannot track per-worker event_writer_batch_size"
        )


@pytest.mark.parametrize("rules_path", [_RULES_YAML, _K8S_RULES_YAML])
def test_scheduled_backlog_growing_asserts_count_growth_not_self_referenced_age(
    rules_path: Path,
) -> None:
    """TaskQScheduledBacklogGrowing must compare a genuine growth signal —
    a job COUNT rising over the window — not join
    taskq_jobs_oldest_due_age_seconds against itself.

    taskq_jobs_oldest_due_age_seconds tracks whichever single job is
    currently oldest-due: its value climbs monotonically toward that one
    job's own promotion regardless of how healthily everything behind it
    is draining. `taskq_jobs_oldest_due_age_seconds >=
    taskq_jobs_oldest_due_age_seconds offset 5m` is therefore satisfied
    by a perfectly healthy, steadily draining backlog for the entire
    5-minute straggler wait — the "growing" half of the check adds
    nothing beyond the bare `age > 300` threshold it is supposed to
    sharpen. The fix compares a count series (taskq_jobs_scheduled_count,
    the label-less twin of taskq_jobs_by_status{status="scheduled"})
    against its own value 5 minutes ago: a draining backlog's scheduled
    count is flat or falling even while one straggler ages past 5
    minutes, so this form does not fire on it.
    """
    rules = _rules_from(rules_path)
    rule = next(r for r in rules if r.get("alert") == "TaskQScheduledBacklogGrowing")
    expr = " ".join(str(rule["expr"]).split())

    assert "taskq_jobs_oldest_due_age_seconds >= taskq_jobs_oldest_due_age_seconds" not in expr, (
        f"{rules_path.name}: TaskQScheduledBacklogGrowing still self-joins the "
        f"oldest-due-age gauge against its own offset value — that pairing is "
        f"satisfied by a healthy draining backlog for the whole straggler wait "
        f"and degenerates to a bare age>300 threshold. expr: {expr}"
    )
    assert "taskq_jobs_scheduled_count" in expr, (
        f"{rules_path.name}: TaskQScheduledBacklogGrowing must reference "
        f"taskq_jobs_scheduled_count (or an equivalent count series) growing "
        f"over the window, not just the oldest item's age. expr: {expr}"
    )
    growth_check = re.search(
        r"taskq_jobs_scheduled_count\s*>\s*\(?\s*taskq_jobs_scheduled_count\s+offset\s+\d+[smh]",
        expr,
    )
    assert growth_check, (
        f"{rules_path.name}: TaskQScheduledBacklogGrowing must compare "
        f"taskq_jobs_scheduled_count against its own value from earlier in the "
        f"window (a `> ... offset ...` growth comparison), not merely reference "
        f"it. expr: {expr}"
    )


def test_both_rule_files_carry_the_five_new_alerts() -> None:
    """The two files must move together: all five new alerts present in
    both, at warning severity (they are degradation signals, not
    data-loss emergencies)."""
    for rules_path in (_RULES_YAML, _K8S_RULES_YAML):
        rules = _rules_from(rules_path)
        by_name = {r["alert"]: r for r in rules}
        for alert in _NEW_ALERTS:
            assert alert in by_name, (
                f"{rules_path.name} is missing the new alert {alert!r} — the "
                "two rule files have drifted"
            )
            assert by_name[alert]["labels"]["severity"] == "warning"


def test_both_rule_files_carry_the_outage_alerts_at_their_severities() -> None:
    """The observability-burn alerts live in BOTH rule files at their own
    severities: the denial-family degradation signals at warning, the
    sustained zombie-running count at critical (work claimed and stuck
    while health probes stay green — the 3am page)."""
    for rules_path in (_RULES_YAML, _K8S_RULES_YAML):
        rules = _rules_from(rules_path)
        by_name = {r["alert"]: r for r in rules}
        for alert, severity in _OUTAGE_SEVERITIES.items():
            assert alert in by_name, (
                f"{rules_path.name} is missing the alert {alert!r} — under a "
                "Redis outage every rate-limited dispatch snoozes silently "
                "and nothing fires"
            )
            assert by_name[alert]["labels"]["severity"] == severity, (
                f"{rules_path.name}: {alert!r} must be {severity!r}"
            )


def test_job_outcome_alerts_read_the_series_that_mean_what_they_say() -> None:
    """The abandoned pager must read the abandonment counter, never the
    consumed-messages outcome (retries and snoozes used to be relabelled
    "abandoned" there, so every retry paged critical); the retry-rate alert
    reads the retried share of attempt failures; and the terminal-failed
    alert is named for what it measures. All at their severities, in both
    files."""
    for rules_path in (_RULES_YAML, _K8S_RULES_YAML):
        by_name = {r["alert"]: r for r in _rules_from(rules_path)}
        assert "TaskQCrashedJobRateHigh" not in by_name, (
            f"{rules_path.name}: the terminal-failed alert measures outcome=failed, "
            "not crashes — it is TaskQFailedJobRateHigh"
        )
        for alert, severity in _JOB_OUTCOME_SEVERITIES.items():
            assert alert in by_name, f"{rules_path.name} is missing {alert!r}"
            assert by_name[alert]["labels"]["severity"] == severity
        abandoned = " ".join(str(by_name["TaskQAbandonedJobs"]["expr"]).split())
        assert "taskq_jobs_abandoned_total" in abandoned
        assert 'outcome="abandoned"' not in abandoned
        retry = " ".join(str(by_name["TaskQRetryRateHigh"]["expr"]).split())
        assert 'taskq_jobs_attempt_failures_total{retryable="true"}' in retry
        failed = " ".join(str(by_name["TaskQFailedJobRateHigh"]["expr"]).split())
        assert 'messaging_client_consumed_messages_total{outcome="failed"}' in failed


def test_dimensionless_series_annotations_carry_no_label_references() -> None:
    """Alerts on series the bridge emits with NO dimensions must not
    reference ``$labels.<dim>`` in their annotations: the rendered alert
    summary would show an empty worker — a 3am page that names nobody.

    taskq.heartbeat.misses and taskq.lock.expires_in_seconds are
    dimensionless by the cardinality rule (obs/_otel.py's worker_id
    note); their summaries must read without a label crutch.
    """
    dimensionless = {
        "taskq_heartbeat_misses_total",
        "taskq_lock_expires_in_seconds",
    }
    for rules_path in (_RULES_YAML, _K8S_RULES_YAML):
        rules = _rules_from(rules_path)
        for rule in rules:
            expr = str(rule["expr"])
            referenced = set(re.findall(r"\btaskq_[a-z0-9_]+", expr))
            if not referenced & dimensionless:
                continue
            annotations = rule.get("annotations", {})
            text = " ".join(str(v) for v in annotations.values())
            offenders = sorted(set(re.findall(r"\$labels\.[a-z_]+", text)))
            assert not offenders, (
                f"{rules_path.name}: alert {rule['alert']!r} fires on a "
                f"dimensionless series but its annotations reference "
                f"{offenders} — the rendered summary carries an empty value"
            )


@pytest.mark.parametrize("rules_path", [_RULES_YAML, _K8S_RULES_YAML])
def test_rule_file_parses_with_expected_alert_count(rules_path: Path) -> None:
    """Both files parse and carry the same alert set — the drift backstop
    the plain/k8s lockstep test in the scrape suite asserts pairwise; this
    pins the count so a sixth alert added to ONE file fails here too."""
    rules = _rules_from(rules_path)
    names = {r["alert"] for r in rules}
    other = _K8S_RULES_YAML if rules_path == _RULES_YAML else _RULES_YAML
    assert names == {r["alert"] for r in _rules_from(other)}, (
        f"{rules_path.name} and its sibling carry different alert sets"
    )


#: Which taskq_* series carry which label names when the bridge emits them
#: (per obs/_otel.py). A series absent from this map carries no labels, the
#: common case for singleton gauges like taskq_jobs_oldest_due_age_seconds.
_SERIES_LABELS: dict[str, frozenset[str]] = {
    "taskq_jobs_by_status": frozenset({"status"}),
    "taskq_jobs_oldest_due_age_seconds": frozenset(),
    "taskq_jobs_running_lease_expired": frozenset(),
    "taskq_maintenance_leader_sweep_last_success_seconds": frozenset({"sweep_name"}),
    "taskq_maintenance_leader_sweep_batch_size": frozenset({"sweep_name"}),
    "taskq_maintenance_leader_sweep_batch_size_configured": frozenset({"sweep_name"}),
    # Label-free leader-lease gauge: one series per pod, present only
    # while that pod holds the lease — pinned label-free so a join
    # against it can never silently go empty.
    "taskq_maintenance_leader_lease_expires_in_seconds": frozenset(),
}

#: Matches `<series_name>{<label filters>}` or a bare `<series_name>`.
_SELECTOR_RE = re.compile(r"\btaskq_[a-z0-9_]+(?:\{([^}]*)\})?")
#: An `ignoring(...)` / `on(...)` vector-matching modifier.
_MODIFIER_RE = re.compile(r"\b(?:ignoring|on)\s*\(")
#: Comparison operators that, between two instant vectors, join the same way
#: `and` does. `>` and friends against a scalar literal are not joins at all.
_COMPARISON_RE = re.compile(r"\s(==|!=|<=|>=|<|>)\s")


def _selector_labels(selector_text: str) -> frozenset[str]:
    """Labels a single `taskq_*{...}` selector carries: the union of its
    inline filter labels and the labels the series is emitted with."""
    match = _SELECTOR_RE.search(selector_text)
    assert match, f"no taskq_* series found in {selector_text!r}"
    name_match = re.match(r"taskq_[a-z0-9_]+", selector_text[match.start() :])
    assert name_match is not None
    name = name_match.group(0)
    inline_filters = match.group(1) or ""
    inline_labels = frozenset(re.findall(r"([a-zA-Z_][a-zA-Z0-9_]*)\s*=", inline_filters))
    base_labels = _SERIES_LABELS.get(name, frozenset())
    return inline_labels | base_labels


def _iter_vector_joins(expr: str, operator_re: re.Pattern[str]):
    """Yield (lhs_text, rhs_text, has_modifier) for each top-level join on
    `operator_re` found in the expression."""
    joined = " ".join(expr.split())
    for m in operator_re.finditer(joined):
        # Crude split at the operator; good enough for these alert
        # expressions, which carry a single join each.
        lhs = joined[: m.start()]
        rest = joined[m.end() :]
        has_modifier = bool(_MODIFIER_RE.match(rest))
        rhs = _MODIFIER_RE.sub("", rest, count=1) if has_modifier else rest
        if has_modifier:
            # Strip the "label1, label2) " tail of ignoring(...)/on(...).
            rhs = re.sub(r"^[^)]*\)\s*", "", rhs)
        yield lhs, rhs, has_modifier


@pytest.mark.parametrize("rules_path", [_RULES_YAML, _K8S_RULES_YAML])
def test_boolean_joins_use_compatible_or_modified_label_sets(rules_path: Path) -> None:
    """Every vector and/or/unless join between two taskq_* series must either
    compare identical label sets, or explicitly carry an ignoring(...)/on(...)
    modifier that reconciles the mismatch.

    Without one of these, Prometheus's `and` is an inner join on identical
    label sets: a left side carrying {status="scheduled"} never matches a
    label-less right side, the joined vector is permanently empty, and the
    alert can never fire. Nothing reports this, because a vector match that
    produces no results is valid PromQL, not an error.
    """
    rules = _rules_from(rules_path)
    violations: list[str] = []
    bool_join_re = re.compile(r"\s(?:and|or|unless)\s")

    for rule in rules:
        expr = str(rule.get("expr", ""))
        if not re.search(r"\btaskq_[a-z0-9_]+.*\b(and|or|unless)\b.*taskq_[a-z0-9_]+", expr):
            continue
        for lhs, rhs, has_modifier in _iter_vector_joins(expr, bool_join_re):
            if not (re.search(r"\btaskq_", lhs) and re.search(r"\btaskq_", rhs)):
                continue
            if has_modifier:
                # An explicit ignoring()/on() modifier is the author's
                # deliberate reconciliation of a label mismatch. Trust it.
                continue
            lhs_labels = _selector_labels(lhs)
            rhs_labels = _selector_labels(rhs)
            if lhs_labels != rhs_labels:
                violations.append(
                    f"{rules_path.name}: alert {rule.get('alert')!r} joins series "
                    f"with mismatched label sets ({sorted(lhs_labels)} vs "
                    f"{sorted(rhs_labels)}) with no ignoring()/on() modifier. "
                    "This join can never produce results, so the alert can "
                    "never fire. expr: " + expr.strip()
                )

    assert not violations, "\n".join(violations)


@pytest.mark.parametrize("rules_path", [_RULES_YAML, _K8S_RULES_YAML])
def test_vector_comparisons_use_compatible_or_modified_label_sets(rules_path: Path) -> None:
    """A comparison between two taskq_* instant vectors joins on label sets
    exactly the way `and` does, so those operands must match too.

    TaskQSweepDegraded compares the used batch size against the configured
    size gauge-to-gauge; that only works because both series carry the same
    sweep_name label. Should either side gain or lose a dimension, the
    comparison silently drops to an empty vector and the alert stops firing
    while still looking well-formed in the rule file. Comparisons against a
    scalar literal carry no such risk and are not joins.
    """
    rules = _rules_from(rules_path)
    violations: list[str] = []

    for rule in rules:
        expr = str(rule.get("expr", ""))
        # Boolean joins are the other test's subject; split on them first so a
        # comparison on one side is never paired with an operand on the other.
        for segment in re.split(r"\s(?:and|or|unless)\s", " ".join(expr.split())):
            for lhs, rhs, has_modifier in _iter_vector_joins(segment, _COMPARISON_RE):
                if not (re.search(r"\btaskq_", lhs) and re.search(r"\btaskq_", rhs)):
                    continue
                if has_modifier:
                    continue
                lhs_labels = _selector_labels(lhs)
                rhs_labels = _selector_labels(rhs)
                if lhs_labels != rhs_labels:
                    violations.append(
                        f"{rules_path.name}: alert {rule.get('alert')!r} compares series "
                        f"with mismatched label sets ({sorted(lhs_labels)} vs "
                        f"{sorted(rhs_labels)}) with no ignoring()/on() modifier. "
                        "The comparison yields an empty vector, so the alert can "
                        "never fire. expr: " + expr.strip()
                    )

    assert not violations, "\n".join(violations)


@pytest.mark.parametrize("rules_path", [_RULES_YAML, _K8S_RULES_YAML])
def test_backlog_alerts_make_an_unconsumed_actor_visible(rules_path: Path) -> None:
    """The backlog-growing alert family must be able to name an actor whose
    jobs accumulate and are never consumed — in BOTH rule files.

    A worker that can do work never refuses to start, so an actor whose
    queue no child of this supervisor consumes boots with a warning and
    then runs silently forever. Monitoring is the only remaining way an
    operator learns that those jobs are piling up. A backlog series
    aggregated fleet-wide, or attributable only to a queue, cannot answer
    "which actor is starving": a busy queue shared by several actors looks
    identical to healthy load while one actor's jobs never move.

    So at least one alert must evaluate backlog depth or oldest-pending age
    at actor granularity — an ``actor`` dimension in a selector, a
    ``by (actor)`` / ``by (queue, actor)`` aggregation, or an ``actor``
    grouping label — and name that actor in its rendered summary so the
    page points at the starving actor rather than at a number.
    """
    rules = _rules_from(rules_path)
    backlog_rules = [
        rule
        for rule in rules
        if re.search(
            r"\btaskq_(?:queue_depth|jobs_by_status|jobs_oldest_due_age_seconds"
            r"|jobs_oldest_pending_age_seconds)\b",
            str(rule.get("expr", "")),
        )
    ]
    assert backlog_rules, (
        f"{rules_path.name} carries no backlog alert at all — an actor whose "
        "jobs are never consumed would be invisible"
    )

    actor_aware = []
    for rule in backlog_rules:
        expr = " ".join(str(rule.get("expr", "")).split())
        has_actor_dimension = bool(
            re.search(r"\{[^}]*\bactor\s*[=!~]", expr)
            or re.search(r"\b(?:by|on|group_left|group_right)\s*\([^)]*\bactor\b", expr)
        )
        if has_actor_dimension:
            actor_aware.append(rule)

    assert actor_aware, (
        f"{rules_path.name}: no backlog alert evaluates depth or oldest-pending "
        "age at actor granularity. Every backlog expr here aggregates away the "
        "actor, so an actor whose queue nothing consumes is indistinguishable "
        "from healthy load on a busy shared queue — and the boot-time warning "
        "is the only other signal an operator ever gets. Exprs seen: "
        + "; ".join(
            f"{r.get('alert')!r}: {' '.join(str(r.get('expr', '')).split())}" for r in backlog_rules
        )
    )

    for rule in actor_aware:
        text = " ".join(str(v) for v in rule.get("annotations", {}).values())
        assert "$labels.actor" in text, (
            f"{rules_path.name}: alert {rule['alert']!r} groups backlog by actor "
            "but its annotations never render $labels.actor — the page reports a "
            "starving actor without naming it"
        )
