"""The two shipped rule files stay in lockstep on the fields an operator reads.

``contrib/prometheus/rules.yaml`` and ``contrib/kubernetes/prometheus_rule.yaml``
exist so the same rules ship as a plain file or as a PrometheusRule CRD. Two
gates already cover them, and both stop short of the annotation text:

- ``test_alert_rules_match_across_plain_and_k8s_files`` compares each alert's
  ``expr`` / ``for`` / ``severity`` — ``_alerts_from_groups`` returns
  ``dict[str, tuple[str, str, str]]``, a shape that cannot carry annotations;
- ``test_every_runbook_alert_row_names_a_rule_shipped_in_both_files`` compares
  the alert NAME SETS only.

So the ``summary`` and ``description`` annotations — the text the operator
actually reads when paged, and the only part of these files whose prose is
duplicated — have no gate at all: edit one copy and the other deployment
silently pages with the old wording. This pin closes that field.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = [pytest.mark.prometheus]

_ROOT = Path(__file__).parent.parent
_PLAIN = _ROOT / "src" / "taskq" / "contrib" / "prometheus" / "rules.yaml"
_K8S = _ROOT / "src" / "taskq" / "contrib" / "kubernetes" / "prometheus_rule.yaml"


def _alerts(groups: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {rule["alert"]: rule for group in groups for rule in group["rules"] if "alert" in rule}


def _fold(text: Any) -> Any:
    """Whitespace-normalise annotation strings: the two files wrap the same
    prose at different columns, and the wrap is not a behavioural difference."""
    if isinstance(text, str):
        return " ".join(text.split())
    if isinstance(text, dict):
        return {key: _fold(value) for key, value in text.items()}
    return text


def test_both_rule_files_carry_identical_annotation_text() -> None:
    plain = _alerts(yaml.safe_load(_PLAIN.read_text())["groups"])
    k8s = _alerts(yaml.safe_load(_K8S.read_text())["spec"]["groups"])
    assert set(plain) == set(k8s), (
        f"alert name sets differ: plain-only={sorted(set(plain) - set(k8s))}, "
        f"k8s-only={sorted(set(k8s) - set(plain))}"
    )
    drifted = {
        name: {
            field: {
                "plain": _fold(plain[name].get("annotations", {}).get(field)),
                "k8s": _fold(k8s[name].get("annotations", {}).get(field)),
            }
            for field in ("summary", "description")
            if _fold(plain[name].get("annotations", {}).get(field))
            != _fold(k8s[name].get("annotations", {}).get(field))
        }
        for name in plain
    }
    drifted = {name: fields for name, fields in drifted.items() if fields}
    assert not drifted, (
        "THE RULE FILES' ANNOTATION LOCKSTEP: the operator-facing text drifted "
        f"between prometheus/rules.yaml and kubernetes/prometheus_rule.yaml: {drifted} "
        "— whichever copy an operator's deployment loads is the wording they are "
        "paged with, so a one-sided edit makes the other deployment's page lie"
    )
