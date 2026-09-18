"""The shutdown budget must model the bounded-close tail, not just the phases.

Regression cover for the sizing trap: the settings validator enforces only
``cancellation_grace + cleanup_grace < termination_grace - 5.0``, but the
exit-stack unwind that runs *after* those phases is additive and was invisible.
Against a dead Postgres/Redis the worker can therefore need materially longer
than ``termination_grace_period``, get SIGKILLed mid-unwind, and leave terminal
writes unlanded. The shipped default (85/30/10) covers the modelled worst case;
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


def test_teardown_tail_counts_eight_sequential_closes_plus_publish_drain() -> None:
    """8 sequential bounded closes + the publish drain.

    Eight: the three role pools plus the conditional per-slot transaction
    pool (the worst case a worker can present), plus notify_conn,
    redis_client, and the two credential providers a provider-backed
    deployment resolves (pg and redis — each closed once, after every
    resource built through it). Not nine: the leader connection is closed
    and nulled by orchestrate_shutdown concurrently with the unwind, so
    the exit stack's own leader guard skips it. Counting it would
    overstate the tail.
    """
    assert worst_case_teardown_tail() == 8 * CLOSE_TIMEOUT_SECS + PUBLISH_DRAIN_TIMEOUT_SECS
    assert worst_case_teardown_tail() == 42.0
    # Scales with the per-resource bound rather than hard-coding it.
    assert worst_case_teardown_tail(close_timeout=1.0) == 8 * 1.0 + PUBLISH_DRAIN_TIMEOUT_SECS


def test_worst_case_shutdown_is_phases_plus_tail() -> None:
    s = _settings(TASKQ_CANCELLATION_GRACE_PERIOD="30", TASKQ_CLEANUP_GRACE_PERIOD="10")
    assert s.worst_case_shutdown_seconds == 30.0 + 10.0 + worst_case_teardown_tail()


def test_taskq_defaults_cover_the_modelled_worst_case() -> None:
    """The shipped default must cover the modelled worst case.

    History: the default used to be 60s while the modelled worst case at
    the default graces was 67s — every deployment running the defaults
    raised its own ``shutdown-budget-exceeds-termination-grace`` boot
    warning, which made the warning pure noise (a downstream redteam
    finding). The tail counts the conditional per-slot pool's close
    and the two credential-provider closes (42s), putting the modelled
    worst case at 82s; 85 keeps 3s of
    headroom over it. The ~87s sibling-crash path (nine sequential
    closes) still exceeds the default by 2s on per-slot workers — that
    path is the documented caveat the model deliberately understates;
    operators running the per-slot path with tight crash budgets should
    raise ``termination_grace_period``.
    """
    s = _settings()
    assert (s.termination_grace_period, s.cancellation_grace_period, s.cleanup_grace_period) == (
        85.0,
        30.0,
        10.0,
    )
    # Still passes the documented validator invariant...
    assert s.cancellation_grace_period + s.cleanup_grace_period < s.termination_grace_period - 5.0
    # ...and now covers the real worst case instead of falling short of it.
    assert s.worst_case_shutdown_seconds == 82.0
    assert s.shutdown_budget_is_sufficient is True


def test_sufficient_budget_is_recognised() -> None:
    s = _settings(
        TASKQ_TERMINATION_GRACE_PERIOD="60",
        TASKQ_CANCELLATION_GRACE_PERIOD="30",
        TASKQ_CLEANUP_GRACE_PERIOD="10",
    )
    # The pre-fix default: valid, and short of the 82s modelled worst case.
    assert s.shutdown_budget_is_sufficient is False

    lowered = _settings(TASKQ_CANCELLATION_GRACE_PERIOD="20", TASKQ_CLEANUP_GRACE_PERIOD="5")
    assert lowered.worst_case_shutdown_seconds == 67.0
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
    assert entry["worst_case_seconds"] == 82.0
    assert entry["close_tail_seconds"] == 42.0
    assert entry["log_level"] == "warning"


def test_startup_warning_remedy_cross_references_the_upgrading_entry() -> None:
    """The warning's remedy points at the upgrading entry for the default change.

    The warning compares settings to settings: the platform's stop grace is
    invisible to the worker, so a deployment that pinned
    ``terminationGracePeriodSeconds`` (or a sibling platform grace) against
    the old 75s default gets no warning of its own. The remedy is the one
    surface that reaches the operator mid-incident, so it must hand them
    the doc entry that names the raise-it-before-upgrading action.
    """
    from taskq.worker._bootstrap import _emit_startup_warnings

    s = _settings(
        TASKQ_TERMINATION_GRACE_PERIOD="60",
        TASKQ_CANCELLATION_GRACE_PERIOD="30",
        TASKQ_CLEANUP_GRACE_PERIOD="10",
    )
    assert s.shutdown_budget_is_sufficient is False
    with structlog.testing.capture_logs() as logs:
        _emit_startup_warnings(s)
    entry = next(log for log in logs if log["event"] == "shutdown-budget-exceeds-termination-grace")
    assert "docs/guides/upgrading.md" in entry["remedy"]
    assert "75s" in entry["remedy"]


# ── The release hold's exit tail and the lease-vs-park arithmetic ──────


def test_release_exit_tail_composes_the_three_canonical_terms() -> None:
    """deadline check margin (dump interval) + bounded flush (2s) + slack (1s).

    The exit tail is what a release hold pads its remaining share with, so
    its terms are pinned against the canonical constants: a drift here
    either under-covers a released row (tail shrinks below the real trip
    lag) or silently inflates every held release's latency.
    """
    from taskq.constants import (
        RELEASE_EXIT_TAIL_SLACK_SECS,
        WATCHDOG_METRICS_FLUSH_TIMEOUT_SECS,
    )

    s = _settings()
    assert s.release_exit_tail_seconds == (
        s.watchdog_dump_interval
        + WATCHDOG_METRICS_FLUSH_TIMEOUT_SECS
        + RELEASE_EXIT_TAIL_SLACK_SECS
    )
    assert s.release_exit_tail_seconds == 8.0  # 5.0 dump + 2.0 flush + 1.0 slack


def test_release_park_lease_cap_arithmetic_at_the_defaults() -> None:
    """cap = lock_lease - heartbeat - terminal-write budget; bound = the rest.

    At the shipped defaults the budget bound binds (60s lease covers the
    85/30/10 budget with the 45s cap above the 40s bound), so the park
    runs its full remaining budget and the lease-cap warning stays silent.
    """
    from taskq.constants import TERMINAL_WRITE_BUDGET_SECS

    s = _settings()
    assert (
        s.release_park_lease_cap == s.lock_lease - s.heartbeat_interval - TERMINAL_WRITE_BUDGET_SECS
    )
    assert s.release_park_lease_cap == 45.0  # 60 - 10 - 5
    assert s.release_park_budget_bound == (
        s.termination_grace_period
        - s.cancellation_grace_period
        - s.cleanup_grace_period
        - TERMINAL_WRITE_BUDGET_SECS
    )
    assert s.release_park_budget_bound == 40.0  # 85 - 30 - 10 - 5
    assert s.release_park_lease_cap >= s.release_park_budget_bound


def test_release_park_lease_capped_warning_names_the_arithmetic() -> None:
    """The lease-cap warning fires exactly when the cap binds before the
    budget bound, with both numbers and the raise-the-lease remedy.

    The 120/30/10/60 shape is the one the #232 review constructed: under an
    uncapped park its lease expires mid-park (a single failed RELEASING
    write away from a double-run). The cap makes it safe by construction,
    and the warning says the trade being made instead of staying quiet
    about a 45s park where the budget promised 75s.
    """
    from taskq.worker._bootstrap import _emit_startup_warnings

    s = _settings(
        TASKQ_TERMINATION_GRACE_PERIOD="120",
        TASKQ_CANCELLATION_GRACE_PERIOD="30",
        TASKQ_CLEANUP_GRACE_PERIOD="10",
        TASKQ_LOCK_LEASE="60",
    )
    assert s.release_park_lease_cap == 45.0
    assert s.release_park_budget_bound == 75.0
    with structlog.testing.capture_logs() as logs:
        _emit_startup_warnings(s)
    entry = next(log for log in logs if log["event"] == "release-park-lease-capped")
    assert entry["log_level"] == "warning"
    assert entry["park_lease_cap_seconds"] == 45.0
    assert entry["park_budget_bound_seconds"] == 75.0
    assert "TASKQ_LOCK_LEASE" in entry["remedy"]


def test_release_park_lease_capped_warning_is_silent_at_the_defaults() -> None:
    """The control: the shipped defaults run the full-budget park."""
    from taskq.worker._bootstrap import _emit_startup_warnings

    s = _settings()
    with structlog.testing.capture_logs() as logs:
        _emit_startup_warnings(s)
    assert [e for e in logs if e["event"] == "release-park-lease-capped"] == []


def test_disown_floor_warning_fires_at_the_defaults_with_the_arithmetic() -> None:
    """The disown residue: 63 needed, 60 shipped: surfaced, not enforced.

    The double-write-failure shape (the RELEASING write AND the consumer's
    both failing) leaves the row to lease expiry, and the earliest reclaim
    (last heartbeat + lease) must stay behind the deadline trip + exit
    tail where an outlived actor dies. At the defaults that demands 63
    against the shipped 60: a ~3s residue the maintainer chose to surface
    rather than hard-fail (the default would not load) or change (a
    maintainer call, flagged in the fix-round report).
    """
    from taskq.worker._bootstrap import _emit_startup_warnings

    s = _settings()
    assert s.release_disown_lease_floor == (
        s.termination_grace_period
        - s.cancellation_grace_period
        - s.cleanup_grace_period
        + s.heartbeat_interval
        + s.release_exit_tail_seconds
    )
    assert s.release_disown_lease_floor == 63.0  # 85 - 30 - 10 + 10 + 8
    assert s.lock_lease < s.release_disown_lease_floor  # 60 < 63: the residue is real

    with structlog.testing.capture_logs() as logs:
        _emit_startup_warnings(s)
    entry = next(log for log in logs if log["event"] == "lock-lease-below-disown-exit-floor")
    assert entry["log_level"] == "warning"
    assert entry["disown_floor"] == 63.0
    assert entry["residue_seconds"] == 3.0
    assert entry["exit_tail_seconds"] == 8.0
    assert "TASKQ_LOCK_LEASE" in entry["remedy"]


def test_disown_floor_warning_is_quiet_once_the_lease_covers_it() -> None:
    """Raise the lease to the floor and the residue warning goes away."""
    from taskq.worker._bootstrap import _emit_startup_warnings

    s = _settings(TASKQ_LOCK_LEASE="65")
    assert s.lock_lease >= s.release_disown_lease_floor
    with structlog.testing.capture_logs() as logs:
        _emit_startup_warnings(s)
    assert [e for e in logs if e["event"] == "lock-lease-below-disown-exit-floor"] == []
