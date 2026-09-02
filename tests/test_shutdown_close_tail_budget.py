"""The shutdown budget must model the bounded-close tail, not just the phases.

Regression cover for the sizing trap: the settings validator enforces only
``cancellation_grace + cleanup_grace < termination_grace - 5.0``, but the
exit-stack unwind that runs *after* those phases is additive and was invisible.
Against a dead Postgres/Redis the worker can therefore need materially longer
than ``termination_grace_period``, get SIGKILLed mid-unwind, and leave terminal
writes unlanded. The shipped default (75/30/10) covers the modelled worst case;
custom grace combinations can still fall short, which is why the shortfall is
surfaced as a startup warning rather than a validation error (an operator may
deliberately run a tighter budget than the dead-backend worst case).
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


def test_taskq_defaults_cover_the_modelled_worst_case() -> None:
    """The shipped default must cover the modelled worst case.

    History: the default used to be 60s while the modelled worst case at
    the default graces (30 + 10 + 27s tail) is 67s — every deployment
    running the defaults raised its own ``shutdown-budget-exceeds-
    termination-grace`` boot warning, which made the warning pure noise
    (a downstream redteam finding). 75 keeps 8s of headroom over the 67s
    modelled SIGTERM path and also covers the ~72s sibling-crash path
    (six sequential closes) that the model itself understates — see the
    sibling-crash caveat in ``taskq/_close.py``.
    """
    s = _settings()
    assert (s.termination_grace_period, s.cancellation_grace_period, s.cleanup_grace_period) == (
        75.0,
        30.0,
        10.0,
    )
    # Still passes the documented validator invariant...
    assert s.cancellation_grace_period + s.cleanup_grace_period < s.termination_grace_period - 5.0
    # ...and now covers the real worst case instead of falling short of it.
    assert s.worst_case_shutdown_seconds == 67.0
    assert s.shutdown_budget_is_sufficient is True


def test_sufficient_budget_is_recognised() -> None:
    s = _settings(
        TASKQ_TERMINATION_GRACE_PERIOD="60",
        TASKQ_CANCELLATION_GRACE_PERIOD="30",
        TASKQ_CLEANUP_GRACE_PERIOD="10",
    )
    # The pre-fix default: valid, and short of the 67s modelled worst case.
    assert s.shutdown_budget_is_sufficient is False

    lowered = _settings(TASKQ_CANCELLATION_GRACE_PERIOD="20", TASKQ_CLEANUP_GRACE_PERIOD="5")
    assert lowered.worst_case_shutdown_seconds == 52.0
    assert lowered.shutdown_budget_is_sufficient is True


# The claim "deps.py bounds the publish drain with this same constant, so the
# model cannot drift from the real teardown" used to be a grep of
# open_worker_deps' source for "timeout=PUBLISH_DRAIN_TIMEOUT_SECS". It is now
# executed instead, in
# tests/test_worker_deps_teardown.py::test_teardown_bounds_the_publish_drain_by_the_shared_constant:
# that test shrinks the constant to 50ms, hands the real open_worker_deps a
# publish that never lands, and fails if teardown outlives the bound.
# Re-hardcoding `timeout=2.0` fails it with "teardown took 2.01s with the drain
# bound at 0.05s" — the drift this file cares about, measured rather than
# spelled.


def test_no_startup_warning_at_default_settings() -> None:
    """The control: the shipped defaults must be silent, or the warning is
    noise on every default deployment and operators learn to ignore it
    (the downstream redteam finding that motivated raising the default)."""
    from taskq.worker._bootstrap import _emit_startup_warnings

    s = _settings()
    assert s.shutdown_budget_is_sufficient is True
    with structlog.testing.capture_logs() as logs:
        _emit_startup_warnings(s)
    assert [e for e in logs if e["event"] == "shutdown-budget-exceeds-termination-grace"] == []


def test_startup_warning_names_the_numbers_and_the_remedy() -> None:
    """The warning has to be actionable: the shortfall is fixed in the
    orchestrator's pod spec, not in TaskQ, so it must carry the number."""
    from taskq.worker._bootstrap import _startup_log

    # The pre-fix default shape (60s grace, 30/10 phases): valid, and
    # short of the modelled worst case — the exact configuration the
    # warning exists for.
    s = _settings(
        TASKQ_TERMINATION_GRACE_PERIOD="60",
        TASKQ_CANCELLATION_GRACE_PERIOD="30",
        TASKQ_CLEANUP_GRACE_PERIOD="10",
    )
    assert s.shutdown_budget_is_sufficient is False
    with structlog.testing.capture_logs() as logs:
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
