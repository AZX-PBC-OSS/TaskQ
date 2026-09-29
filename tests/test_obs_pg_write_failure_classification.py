"""The shutdown paths' PG-write-failure events must be CLASSIFIED anomalies.

The ``TASKQ_LOG_EVENTS_LEVEL`` knob's contract (``obs/_structlog``): the
classification table is TOTAL for the failure families it names — the
shutdown orchestrator's last-resort durability failures are the exact
class the ``off`` level must never blind an operator to. Today they are
all emitted at WARNING method, so they survive at every level by the
WARNING-and-above rule alone; classification is what keeps their
treatment deliberate if a call site is ever re-levelled to INFO/DEBUG
(at ``off`` a CLASSIFIED INFO-method anomaly line is suppressed — the
ledger keeps it — while an UNCLASSIFIED event fails OPEN and passes at
every level, so classification is also what stops an anomaly's INFO line
from slipping into the ``off`` stream by the fail-open back door).

The family: every ``*-pg-write-failed`` event in ``worker/shutdown.py``
— the cancel request, force-cancel escalation, and release writes whose
failure leaves a job to lease-expiry reclaim. The family was never
classified — no pin looked at the emitted names, so the gap survived
#596's ``abandon-pg-write-failed`` → ``cancel-pg-write-failed`` rename
unnoticed. The classification is what makes a re-levelled call site
DELIBERATE at ``off`` (classified INFO-method anomaly lines are the ones
``off`` suppresses; unclassified events fail OPEN and pass at every
level — the fail-open rule, not classification, is what carries an
unclassified anomaly through).
"""

from __future__ import annotations

import ast
from pathlib import Path

from taskq.obs import _structlog as obs_structlog

_SHUTDOWN = Path(__file__).resolve().parent.parent / "src" / "taskq" / "worker" / "shutdown.py"

_LEVELS = {"debug", "info", "warning", "error", "exception", "critical"}


def _emitted_pg_write_failed_names() -> dict[str, list[str]]:
    """Every literal event name matching ``*-pg-write-failed`` emitted from
    the shutdown orchestrator, with call-site locations."""
    found: dict[str, list[str]] = {}
    for node in ast.walk(ast.parse(_SHUTDOWN.read_text())):
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
        if first.value.endswith("-pg-write-failed"):
            found.setdefault(first.value, []).append(f"shutdown.py:{node.lineno}")
    return found


def test_sweep_finds_the_family() -> None:
    """The scan is not vacuous: the family exists and is fully enumerated."""
    names = _emitted_pg_write_failed_names()
    assert set(names) == {
        "force-cancel-pg-write-failed",
        "cancel-pg-write-failed",
        "release-pg-write-failed",
    }, f"the family drifted - update the pin: {names}"


def test_pg_write_failure_family_is_classified_anomaly() -> None:
    """Every ``*-pg-write-failed`` event is in the ANOMALY set, so a
    re-levelled call site cannot silently vanish from the ``warning`` /
    ``off`` streams."""
    names = _emitted_pg_write_failed_names()
    assert names, "no pg-write-failed emitters found - the scan is broken"
    unclassified = sorted(n for n in names if n not in obs_structlog._ANOMALY_EVENTS)
    assert not unclassified, (
        "the TASKQ_LOG_EVENTS_LEVEL classification table must stay total over "
        "the shutdown paths' durability failures (a re-levelled unclassified "
        f"anomaly would fail OPEN into the happy path at warning/off): {unclassified}"
    )
