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
    """Every runbook link in the new alerts' annotations must resolve to
    a real heading anchor in docs/guides/runbooks.md — in BOTH rule
    files. And every new alert must CARRY a runbook link: an annotation
    that lost its link entirely would otherwise pass vacuously."""
    assert _RUNBOOKS_MD.exists(), f"runbooks.md not found at {_RUNBOOKS_MD}"
    anchors = _runbook_anchors(_RUNBOOKS_MD)

    for rules_path in (_RULES_YAML, _K8S_RULES_YAML):
        rules = _rules_from(rules_path)
        for rule in rules:
            if rule.get("alert") not in _NEW_ALERTS:
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
    """Every taskq_* series name in the new alerts' exprs must be a
    Prometheus name the bridge actually emits (per the authoritative
    _NAME_MAP the scrape tests verify). A typo'd series name is not a
    Prometheus error — the alert just silently never fires."""
    from tests.test_prometheus_metrics import _NAME_MAP

    emitted = {prom_name for _, prom_name in _NAME_MAP}

    for rules_path in (_RULES_YAML, _K8S_RULES_YAML):
        rules = _rules_from(rules_path)
        for rule in rules:
            if rule.get("alert") not in _NEW_ALERTS:
                continue
            expr = str(rule["expr"])
            referenced = set(re.findall(r"\btaskq_[a-z0-9_]+", expr))
            assert referenced, (
                f"{rules_path.name}: alert {rule['alert']!r} references no "
                "taskq series at all — the expr is wrong"
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
