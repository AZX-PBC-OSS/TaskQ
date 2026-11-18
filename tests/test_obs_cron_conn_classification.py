"""The cron connection-repair family must be CLASSIFIED anomalies.

The ``TASKQ_LOG_EVENTS_LEVEL`` knob's contract (``obs/_structlog``, and
pinned for the shutdown family by
``tests/test_obs_pg_write_failure_classification.py``): the
classification table is TOTAL for the failure families it names. The
table names this one — *``cron-conn-*`` ... ANOMALY ... The cron loop's
failures and conn losses* — so every event the leader emits about its
dedicated cron connection is a member.

Why membership is load-bearing and not bookkeeping: an UNCLASSIFIED
event fails OPEN at every level, so classification is what stops an
anomaly's INFO line from slipping into the ``off`` stream by the
fail-open back door. ``cron-conn-repaired`` is the family's one
INFO-method member — it reports that a tick failed and the connection's
transaction state had to be rolled back — and before the table met the
new names it survived ``off`` through that fail-open arm, while its
WARNING-method siblings (``cron-conn-lost`` among them) passed by level.
Classification is what now drops its INFO line at ``off`` with every
other classified INFO-method anomaly. The WARNING-method members are
inert to classification (WARNING-and-above passes at every level either
way); classifying them is what keeps the family total, so a future
re-level cannot silently change which stream they reach.

The family lives in ``worker/leader.py`` today because the dedicated
cron connection is the leader's alone, but the scan is tree-wide over
``src/taskq`` so an emitter added elsewhere cannot escape it.
"""

from __future__ import annotations

import ast
from pathlib import Path

import structlog

from taskq.obs import _structlog as obs_structlog

_SRC = Path(__file__).resolve().parent.parent / "src" / "taskq"

_LEVELS = {"debug", "info", "warning", "error", "exception", "critical"}

_FAMILY_PREFIX = "cron-conn-"


def _emitted_cron_conn_names() -> dict[str, list[str]]:
    """Every literal ``cron-conn-*`` event name emitted under ``src/taskq``,
    with call-site locations."""
    found: dict[str, list[str]] = {}
    for path in sorted(_SRC.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in _LEVELS
                and node.args
            ):
                continue
            first = node.args[0]
            if not (isinstance(first, ast.Constant) and isinstance(first.value, str)):
                continue
            if first.value.startswith(_FAMILY_PREFIX):
                found.setdefault(first.value, []).append(f"{path.name}:{node.lineno}")
    return found


def test_scan_finds_the_family() -> None:
    """The scan is not vacuous: the family exists and is fully enumerated."""
    names = _emitted_cron_conn_names()
    assert set(names) == {
        "cron-conn-lost",
        "cron-conn-repaired",
        "cron-conn-repair-failed",
        "cron-conn-repair-cut",
        "cron-conn-repair-escalated",
    }, f"the family drifted - update the pin: {names}"


def test_cron_conn_family_is_classified_anomaly() -> None:
    """Every ``cron-conn-*`` event is in the ANOMALY set, so the table stays
    total over the family it names and no member's INFO line reaches the
    ``off`` stream by the fail-open back door."""
    names = _emitted_cron_conn_names()
    assert names, "no cron-conn-* emitters found - the scan is broken"
    unclassified = sorted(n for n in names if n not in obs_structlog._ANOMALY_EVENTS)
    assert not unclassified, (
        "the TASKQ_LOG_EVENTS_LEVEL classification table must stay total over "
        "the cron loop's conn family (obs/_structlog names ``cron-conn-*`` as "
        "ANOMALY): an unclassified member fails OPEN at every level, so its "
        f"INFO-method line survives ``off`` beside siblings that do not: {unclassified}"
    )


def test_the_info_method_member_is_suppressed_at_off_like_its_siblings() -> None:
    """The behavioural half: at ``off`` the family's INFO-method member is
    treated as its classified siblings are - dropped - while the
    WARNING-method members still reach the operator."""
    saved = obs_structlog._events_level
    obs_structlog.set_events_level("off")
    try:
        for name, method, expect_dropped in (
            # The family's one INFO-method member: suppressed at off, as every
            # classified INFO-method anomaly is.
            ("cron-conn-repaired", "info", True),
            # The WARNING-method members: WARNING-and-above survives at off, so
            # the operator is never blinded to the repair, the cut, the failure
            # or the escalation.
            ("cron-conn-lost", "warning", False),
            ("cron-conn-repair-failed", "warning", False),
            ("cron-conn-repair-cut", "warning", False),
            ("cron-conn-repair-escalated", "warning", False),
        ):
            try:
                obs_structlog._events_level_filter(None, method, {"event": name})
                dropped = False
            except structlog.DropEvent:
                dropped = True
            assert dropped is expect_dropped, (
                f"{name} at {method.upper()} method under events_level=off was "
                f"{'dropped' if dropped else 'emitted'}; the family's INFO-method "
                "member must be suppressed at off and its WARNING-method members "
                "must survive"
            )
    finally:
        obs_structlog.set_events_level(saved)
