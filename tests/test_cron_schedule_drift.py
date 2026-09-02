"""Changing a @cron decorator in code must not deploy silently.

Startup cron registration is create-only (`except UniqueViolationError`), which
is deliberate: an operator's runtime change -- disabling a schedule, retiming
it -- must not be reverted by the next redeploy. The consequence is that
changing a `@cron`'s `cron_expr`, `timezone` or `dst_strategy` in code deploys
"successfully" while the stored row keeps the OLD cadence.

Correcting the original report: it claimed this logged NOTHING. It logged
`cron-schedule-already-registered` at DEBUG. The real defect is narrower and
worse -- that line reported the CODE's values and never compared them to the
stored row, so the mismatch itself was undetectable at any log level. Meanwhile
structural `actor_config` drift raises `ActorConfigDriftList` and refuses to
start. Same class of mistake, opposite loudness.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import structlog.testing

from taskq._ids import new_uuid
from taskq.cron import CronScheduleSpec
from taskq.worker._bootstrap import _warn_on_cron_drift


def _spec(**overrides: Any) -> CronScheduleSpec:
    base: dict[str, Any] = {
        "actor": "nightly_report",
        "name": "",
        "cron_expr": "0 3 * * *",
        "timezone": "UTC",
        "dst_strategy": "skip",
        "payload_factory": None,
        "static_payload": None,
        "identity_key": None,
        "enabled": True,
    }
    base.update(overrides)
    return CronScheduleSpec(**base)


def _stored(**overrides: Any) -> Any:
    class _Row:
        def __init__(self) -> None:
            self.id = new_uuid()
            self.actor = "nightly_report"
            self.name = ""
            self.cron_expr = "0 3 * * *"
            self.timezone = "UTC"
            self.dst_strategy = "skip"
            self.enabled = True

    row = _Row()
    for k, v in overrides.items():
        setattr(row, k, v)
    return row


def _backend(rows: list[Any]) -> Any:
    backend = AsyncMock()
    backend.list_schedules = AsyncMock(return_value=rows)
    return backend


async def test_matching_schedule_does_not_warn() -> None:
    with structlog.testing.capture_logs() as logs:
        await _warn_on_cron_drift(_backend([_stored()]), _spec())
    assert not [e for e in logs if e["event"] == "cron-schedule-drift"]


async def test_changed_cron_expr_warns_with_both_values() -> None:
    """The operator needs to see what will actually run, not just that
    something differs."""
    with structlog.testing.capture_logs() as logs:
        await _warn_on_cron_drift(
            _backend([_stored(cron_expr="0 3 * * *")]), _spec(cron_expr="*/15 * * * *")
        )
    entry = next(e for e in logs if e["event"] == "cron-schedule-drift")
    assert entry["log_level"] == "warning"
    assert entry["drift"]["cron_expr"] == {"stored": "0 3 * * *", "code": "*/15 * * * *"}
    assert "create-only" in entry["remedy"]


async def test_changed_timezone_and_dst_strategy_warn() -> None:
    with structlog.testing.capture_logs() as logs:
        await _warn_on_cron_drift(
            _backend([_stored(timezone="UTC", dst_strategy="skip")]),
            _spec(timezone="Europe/London", dst_strategy="fire_once"),
        )
    drift = next(e for e in logs if e["event"] == "cron-schedule-drift")["drift"]
    assert set(drift) == {"timezone", "dst_strategy"}


async def test_enabled_is_not_treated_as_drift() -> None:
    """`enabled` is operator-controlled at runtime -- the admin UI and CLI
    toggle it. Warning about a deliberately disabled schedule on every startup
    would train operators to ignore this event."""
    with structlog.testing.capture_logs() as logs:
        await _warn_on_cron_drift(_backend([_stored(enabled=False)]), _spec(enabled=True))
    assert not [e for e in logs if e["event"] == "cron-schedule-drift"]


async def test_drift_check_failure_never_breaks_startup() -> None:
    """Unlike actor_config drift, a cron mismatch means the wrong cadence, not
    a broken worker; detection must not be able to stop a worker booting."""
    backend = AsyncMock()
    backend.list_schedules = AsyncMock(side_effect=RuntimeError("pg down"))
    with structlog.testing.capture_logs() as logs:
        await _warn_on_cron_drift(backend, _spec())  # must not raise
    assert [e for e in logs if e["event"] == "cron-schedule-drift-check-failed"]


async def test_missing_row_is_not_reported_as_drift() -> None:
    """Raced with a delete, or the conflict came from another constraint."""
    with structlog.testing.capture_logs() as logs:
        await _warn_on_cron_drift(_backend([]), _spec())
    assert not [e for e in logs if e["event"] == "cron-schedule-drift"]


async def test_named_schedules_are_matched_by_name() -> None:
    """An actor can own several schedules; drift must compare like with like."""
    rows = [
        _stored(name="hourly", cron_expr="0 * * * *"),
        _stored(name="nightly", cron_expr="0 3 * * *"),
    ]
    with structlog.testing.capture_logs() as logs:
        await _warn_on_cron_drift(_backend(rows), _spec(name="nightly", cron_expr="0 4 * * *"))
    entry = next(e for e in logs if e["event"] == "cron-schedule-drift")
    assert entry["name"] == "nightly"
    assert entry["drift"]["cron_expr"]["stored"] == "0 3 * * *"
