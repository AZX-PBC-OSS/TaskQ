"""`TASKQ_MIGRATE_ON_START` must not be silently ignored by the worker.

The README stated that the worker applies pending migrations at startup when
this is set. It never has: the setting is defined on `TaskQSettings`, so
`WorkerSettings` inherits and validates it, but the only consumer in the
codebase is the `ui serve` path. Setting it on a worker was accepted without
complaint and did nothing -- and there is no schema-version precheck on the
worker path either, so nothing else catches the unmigrated database.

The worker deliberately does not honour it (N replicas racing to migrate is the
hazard the migration advisory lock exists to prevent), so the fix is to say so
loudly rather than to implement it.
"""

from __future__ import annotations

import ast
from pathlib import Path

import structlog.testing

from taskq.settings import WorkerSettings

_ROOT = Path(__file__).resolve().parent.parent


def test_worker_settings_still_accepts_the_setting() -> None:
    """It is inherited and valid -- which is exactly why silence was dangerous."""
    s = WorkerSettings.load_from_dict({"TASKQ_MIGRATE_ON_START": "true"}, validate=False)
    assert s.migrate_on_start is True


def test_worker_bootstrap_warns_when_the_setting_is_set() -> None:
    """A worker started with the setting on must SAY it is being ignored.

    Emitted, not grepped: the warning is now reachable through
    ``_emit_startup_warnings``, so the log line and the fields an operator
    needs are asserted as they are actually produced.
    """
    from taskq.worker._bootstrap import _emit_startup_warnings

    settings = WorkerSettings.load_from_dict({"TASKQ_MIGRATE_ON_START": "true"}, validate=False)
    with structlog.testing.capture_logs() as logs:
        _emit_startup_warnings(settings)

    entry = next(e for e in logs if e["event"] == "migrate-on-start-ignored-by-worker")
    assert entry["log_level"] == "warning"
    assert entry["setting"] == "TASKQ_MIGRATE_ON_START"
    assert "ui serve" in entry["reason"]
    assert "taskq migrate up" in entry["remedy"], "the warning must name the remedy"


def test_worker_bootstrap_is_silent_when_the_setting_is_unset_or_false() -> None:
    """The control: no warning for the default (or an explicit false), or
    it becomes noise operators learn to ignore. Explicit false is its own
    case because a parser that treated any SET value as true would warn on
    a correctly-disabled worker."""
    from taskq.worker._bootstrap import _emit_startup_warnings

    for env in ({}, {"TASKQ_MIGRATE_ON_START": "false"}):
        settings = WorkerSettings.load_from_dict(env, validate=False)
        with structlog.testing.capture_logs() as logs:
            _emit_startup_warnings(settings)

        assert [e for e in logs if e["event"] == "migrate-on-start-ignored-by-worker"] == [], (
            f"no migrate-on-start warning may fire for {env or 'the defaults'}: "
            f"{[e['event'] for e in logs]}"
        )


def test_worker_still_does_not_apply_migrations() -> None:
    """Guards the intent: the fix is a warning, not an implementation.

    A future change that "helpfully" wires apply_pending into the worker would
    reintroduce concurrent migration across replicas.
    """
    worker_pkg = _ROOT / "src" / "taskq" / "worker"
    offenders: list[str] = []
    for path in sorted(worker_pkg.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if (
                (
                    isinstance(node, ast.ImportFrom)
                    and any(a.name.startswith("apply_pending") for a in node.names)
                )
                or (isinstance(node, ast.Attribute) and node.attr.startswith("apply_pending"))
                or (isinstance(node, ast.Name) and node.id.startswith("apply_pending"))
            ):
                offenders.append(f"{path.name}:{node.lineno}")
    assert not offenders, (
        "the worker package must not run migrations — N replicas racing to "
        "migrate is the hazard the advisory lock exists to prevent:\n"
        + "\n".join(f"  - {o}" for o in offenders)
    )


def test_settings_description_scopes_it_to_ui_serve() -> None:
    """The description is what `taskq config` and the docs render, so assert
    the value the field actually carries rather than the source that spells
    it."""
    _type, info = WorkerSettings.get_fields()["migrate_on_start"]
    description = info.description or ""
    assert "ui serve" in description
    assert "the worker ignores it (and warns when it is set)" in description


def test_readme_no_longer_claims_the_worker_migrates() -> None:
    text = (_ROOT / "README.md").read_text()
    assert "The worker applies pending migrations at startup when" not in text
    assert "ignored by the worker" in text


def test_deployment_guide_does_not_claim_drift_detects_a_stale_schema() -> None:
    """Second false claim in the same deploy-order sentence: drift compares
    `queue`/`metadata`, never a schema version."""
    text = (_ROOT / "docs" / "guides" / "deployment.md").read_text()
    assert "fail with `ActorConfigDriftList` if the schema is stale" not in text
    assert "does **not** detect a stale *schema*" in text
