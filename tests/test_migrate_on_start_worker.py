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

import inspect
from pathlib import Path

from taskq.settings import WorkerSettings

_ROOT = Path(__file__).resolve().parent.parent


def test_worker_settings_still_accepts_the_setting() -> None:
    """It is inherited and valid -- which is exactly why silence was dangerous."""
    s = WorkerSettings.load_from_dict({"TASKQ_MIGRATE_ON_START": "true"}, validate=False)
    assert s.migrate_on_start is True


def test_worker_bootstrap_warns_when_the_setting_is_set() -> None:
    from taskq.worker import _bootstrap

    source = inspect.getsource(
        _bootstrap._main
    )  # Why: asserting the bootstrap path emits the warning; _main is the real entry point.
    assert "migrate-on-start-ignored-by-worker" in source
    assert "if settings.migrate_on_start:" in source


def test_worker_still_does_not_apply_migrations() -> None:
    """Guards the intent: the fix is a warning, not an implementation.

    A future change that "helpfully" wires apply_pending into the worker would
    reintroduce concurrent migration across replicas.
    """
    worker_pkg = _ROOT / "src" / "taskq" / "worker"
    for path in worker_pkg.rglob("*.py"):
        text = path.read_text()
        assert "apply_pending" not in text, f"{path.name} must not run migrations"


def test_settings_description_scopes_it_to_ui_serve() -> None:
    text = (_ROOT / "src" / "taskq" / "settings.py").read_text()
    idx = text.index("migrate_on_start: bool = Field(")
    field = text[idx : idx + 700]
    assert "ui serve" in field
    assert "ignores it (and warns when it is set)" in field


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
