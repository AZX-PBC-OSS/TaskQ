"""The ``TaskQSweepTimeouts`` sampler vocabulary is enumerated in prose, and
the prose has to name every value the counter can actually carry.

``record_sweep_timeout(sampler_name)`` labels
``taskq.maintenance_leader.sweep_timeouts_total``; its own instrument
description, both rule files' ``TaskQSweepTimeouts`` annotations, the runbook
section and the metrics reference all tell the operator which ``sweep_name``
values mean "a gauge sampler's read did not happen" — the only alertable
signal that a sampler is frozen, because the gauge it feeds keeps serving its
last value.

This pin derives the real set from the ``_sampler_read_failed`` call sites and
holds every surface to it, so a new sampler cannot ship a ``sweep_name`` the
paged operator is told does not exist.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import yaml

_ROOT = Path(__file__).parent.parent
_OTEL = _ROOT / "src" / "taskq" / "obs" / "_otel.py"
_WORKER = _ROOT / "src" / "taskq" / "worker"
_PLAIN_RULES = _ROOT / "src" / "taskq" / "contrib" / "prometheus" / "rules.yaml"
_K8S_RULES = _ROOT / "src" / "taskq" / "contrib" / "kubernetes" / "prometheus_rule.yaml"
_RUNBOOKS = _ROOT / "docs" / "guides" / "runbooks.md"
_OBSERVABILITY = _ROOT / "docs" / "guides" / "observability.md"


def _counter_description() -> str:
    """The ``_sweep_timeouts`` counter's registered ``description`` text —
    what the scrape's ``# HELP`` line carries."""
    tree = ast.parse(_OTEL.read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        if not any(isinstance(t, ast.Name) and t.id == "_sweep_timeouts" for t in node.targets):
            continue
        call = node.value
        assert isinstance(call, ast.Call)
        for keyword in call.keywords:
            if keyword.arg == "description":
                return ast.literal_eval(keyword.value)
    raise AssertionError("no `_sweep_timeouts = ... create_counter(..., description=...)` found")


def _sampler_names() -> set[str]:
    """Every literal sampler name ``_sampler_read_failed`` is called with.

    Read from the AST so multi-line calls are covered, and the literal is
    asserted: a non-literal name could never be pinned in prose.
    """
    names: set[str] = set()
    for path in sorted(_WORKER.glob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "_sampler_read_failed"
            ):
                continue
            assert len(node.args) >= 2, f"unexpected call shape in {path}: {ast.dump(node)}"
            sampler = node.args[1]
            assert isinstance(sampler, ast.Constant) and isinstance(sampler.value, str), (
                "a sampler name must be a string literal so this vocabulary pin can "
                f"read it: {path}:{node.lineno}"
            )
            names.add(sampler.value)
    assert names, "no `_sampler_read_failed(...)` call sites found under src/taskq/worker"
    assert "wf_progress" in names, "the AST walk missed the wf-progress sampler: " + repr(
        sorted(names)
    )
    return names


def _rule_description(path: Path, alert: str) -> str:
    doc = yaml.safe_load(path.read_text())
    groups = doc.get("spec", doc).get("groups", [])
    for group in groups:
        for rule in group.get("rules", []):
            if rule.get("alert") == alert:
                return " ".join(str(rule["annotations"]["description"]).split())
    raise AssertionError(f"{alert} not found in {path}")


def _section(path: Path, heading: str) -> str:
    text = path.read_text()
    start = text.index(heading)
    end = text.index("\n## ", start + 1)
    return text[start:end]


def _metrics_row(path: Path, metric: str) -> str:
    for line in path.read_text().splitlines():
        cells = line.split("|")
        if len(cells) > 1 and cells[1].strip() == f"`{metric}`":
            return line
    raise AssertionError(f"{metric} not found in {path}")


def test_every_sampler_name_is_named_in_every_operator_surface() -> None:
    surfaces = {
        "src/taskq/obs/_otel.py (counter description)": _counter_description(),
        "contrib/prometheus/rules.yaml (TaskQSweepTimeouts)": _rule_description(
            _PLAIN_RULES, "TaskQSweepTimeouts"
        ),
        "contrib/kubernetes/prometheus_rule.yaml (TaskQSweepTimeouts)": _rule_description(
            _K8S_RULES, "TaskQSweepTimeouts"
        ),
        "docs/guides/runbooks.md (TaskQSweepTimeouts)": _section(
            _RUNBOOKS, "## TaskQSweepTimeouts"
        ),
        "docs/guides/observability.md (sweep_timeouts row)": _metrics_row(
            _OBSERVABILITY, "taskq.maintenance_leader.sweep_timeouts"
        ),
    }
    names = _sampler_names()
    missing: dict[str, list[str]] = {}
    for surface, text in surfaces.items():
        absent = sorted(name for name in names if not re.search(rf"\b{re.escape(name)}\b", text))
        if absent:
            missing[surface] = absent
    assert not missing, (
        "THE SAMPLER VOCABULARY IN THE OPERATOR TEXT: these sampler names reach "
        "`sweep_name` from `_sampler_read_failed` but a surface that tells the operator "
        f"what the label means does not name them: {missing}. A sampler whose read stops "
        "is the failure this counter exists for; a list that excludes the value sends the "
        "operator chasing a sweep name while the gauge it feeds serves its last value."
    )
