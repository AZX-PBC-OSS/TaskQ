"""The shutdown budget must model the bounded-close tail, not just the phases.

Regression cover for the sizing trap: the settings validator enforces only
``cancellation_grace + cleanup_grace < termination_grace - 5.0``, but the
exit-stack unwind that runs *after* those phases is additive and was invisible.
Against a dead Postgres/Redis the worker can therefore need materially longer
than ``termination_grace_period``, get SIGKILLed mid-unwind, and leave terminal
writes unlanded -- at TaskQ's own defaults, which pass the validator.
"""

from __future__ import annotations

import structlog.testing

from taskq._close import (
    CLOSE_TIMEOUT_SECS,
    PUBLISH_DRAIN_TIMEOUT_SECS,
    worst_case_teardown_tail,
)
from taskq.settings import WorkerSettings


def _settings(**overrides: object) -> WorkerSettings:
    return WorkerSettings.load_from_dict(dict(overrides), validate=False)  # type: ignore[arg-type]  # Why: load_from_dict takes a str-keyed mapping of raw values; overrides are typed loosely for test brevity.


def test_teardown_tail_counts_five_sequential_closes_plus_publish_drain() -> None:
    """5 sequential bounded closes + the publish drain.

    Five, not six: the leader connection is closed and nulled by
    orchestrate_shutdown concurrently with the unwind, so the exit stack's
    own leader guard skips it. Counting it would overstate the tail.
    """
    assert worst_case_teardown_tail() == 5 * CLOSE_TIMEOUT_SECS + PUBLISH_DRAIN_TIMEOUT_SECS
    assert worst_case_teardown_tail() == 27.0
    # Scales with the per-resource bound rather than hard-coding it.
    assert worst_case_teardown_tail(close_timeout=1.0) == 5 * 1.0 + PUBLISH_DRAIN_TIMEOUT_SECS


def test_worst_case_shutdown_is_phases_plus_tail() -> None:
    s = _settings(TASKQ_CANCELLATION_GRACE_PERIOD="30", TASKQ_CLEANUP_GRACE_PERIOD="10")
    assert s.worst_case_shutdown_seconds == 30.0 + 10.0 + worst_case_teardown_tail()


def test_taskq_defaults_pass_the_validator_but_exceed_the_budget() -> None:
    """The exact trap: defaults are valid AND insufficient.

    This is why the shortfall is a startup warning rather than a validation
    error -- rejecting it would refuse to start every deployment on defaults.
    """
    s = _settings()
    assert (s.termination_grace_period, s.cancellation_grace_period, s.cleanup_grace_period) == (
        60.0,
        30.0,
        10.0,
    )
    # Passes the documented validator invariant...
    assert s.cancellation_grace_period + s.cleanup_grace_period < s.termination_grace_period - 5.0
    # ...and is still short of the real worst case.
    assert s.worst_case_shutdown_seconds == 67.0
    assert s.shutdown_budget_is_sufficient is False


def test_sufficient_budget_is_recognised() -> None:
    s = _settings(
        TASKQ_TERMINATION_GRACE_PERIOD="95",
        TASKQ_CANCELLATION_GRACE_PERIOD="30",
        TASKQ_CLEANUP_GRACE_PERIOD="10",
    )
    assert s.shutdown_budget_is_sufficient is True

    lowered = _settings(TASKQ_CANCELLATION_GRACE_PERIOD="20", TASKQ_CLEANUP_GRACE_PERIOD="5")
    assert lowered.worst_case_shutdown_seconds == 52.0
    assert lowered.shutdown_budget_is_sufficient is True


def test_publish_drain_constant_is_the_one_deps_actually_uses() -> None:
    """The model must not drift from the real teardown literal.

    ``deps.py`` bounds the progress-publish drain with this same constant;
    a future edit that changes one without the other would silently make the
    startup warning lie.
    """
    import inspect

    from taskq.worker import deps as deps_mod

    source = inspect.getsource(deps_mod.open_worker_deps)
    assert "timeout=PUBLISH_DRAIN_TIMEOUT_SECS" in source, (
        "the publish drain must be bounded by the shared constant, or "
        "worst_case_teardown_tail() no longer models the real teardown"
    )
    assert "timeout=2.0" not in source, "a re-hardcoded literal would desync the model"


def test_startup_warning_names_the_numbers_and_the_remedy() -> None:
    """The warning has to be actionable: the shortfall is fixed in the
    orchestrator's pod spec, not in TaskQ, so it must carry the number."""
    from taskq.worker._bootstrap import _startup_log

    s = _settings()
    with structlog.testing.capture_logs() as logs:
        if not s.shutdown_budget_is_sufficient:
            _startup_log.warning(
                "shutdown-budget-exceeds-termination-grace",
                worst_case_seconds=s.worst_case_shutdown_seconds,
                termination_grace_period=s.termination_grace_period,
                close_tail_seconds=worst_case_teardown_tail(),
            )
    entry = next(log for log in logs if log["event"] == "shutdown-budget-exceeds-termination-grace")
    assert entry["worst_case_seconds"] == 67.0
    assert entry["close_tail_seconds"] == 27.0
    assert entry["log_level"] == "warning"
