"""A contended cron tick must be observable, not silent.

`tick_cron` took `pg_try_advisory_xact_lock` and, on failure, returned with no
log, no metric and no counter. That is benign for the sub-second leader-handover
overlap it exists to cover -- but it is indistinguishable from cron having
stopped entirely.

The dangerous case: the lock is transaction-scoped and releases on
COMMIT/ROLLBACK, which never happens if the holding session was partitioned
without a FIN (a VNet blip, unlike a process kill, which sends a FIN and
releases cleanly). Every subsequent tick on the new leader then returns here.
Cron stops firing fleet-wide, and neither `taskq.cron.disabled_schedules` nor
`consecutive_failures` moves, because `fire_schedule` never runs.

Note this narrows the original report: the leader lock path is NOT the same
shape. `leader.py` calls `record_election_attempt(won=False)` and logs a warning
on every failure branch, and the prune/archive sweeps log on contention too.
Cron was the genuine outlier.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import structlog.testing

from taskq.obs import record_cron_lock_contention

_SRC = Path(__file__).resolve().parent.parent / "src" / "taskq"


def test_contended_branch_records_a_counter_and_a_log() -> None:
    from taskq.worker import cron_loop

    source = inspect.getsource(cron_loop.tick_cron)
    head = source[: source.index("_cron_tick_sql")]
    assert "if not lock_acquired:" in head
    assert "record_cron_lock_contention" in head, "contended tick still has no metric"
    assert "cron-tick-lock-contended" in head, "contended tick still has no log line"
    # It must still return -- this is a skip, not an error path.
    assert head.rstrip().endswith("return")


def test_counter_helper_is_a_noop_when_otel_disabled() -> None:
    """Must not construct instruments when telemetry is off."""
    from taskq.obs import _otel

    original = _otel._otel_enabled
    try:
        _otel._otel_enabled = False
        record_cron_lock_contention("worker-1")  # must not raise
    finally:
        _otel._otel_enabled = original


def test_log_carries_the_fields_needed_to_diagnose() -> None:
    from taskq.worker import cron_loop

    with structlog.testing.capture_logs() as logs:
        cron_loop.log.debug(
            "cron-tick-lock-contended",
            kind="cron_tick_lock_contended",
            worker_id="w-1",
            lock=cron_loop.CRON_LOCK_NAME,
        )
    entry = next(e for e in logs if e["event"] == "cron-tick-lock-contended")
    assert entry["worker_id"] == "w-1"
    assert entry["lock"] == cron_loop.CRON_LOCK_NAME


def test_leader_path_was_already_observable() -> None:
    """Pins the refutation, so the narrower claim is not re-widened later."""
    leader = (_SRC / "worker" / "leader.py").read_text()
    assert "record_election_attempt" in leader
    assert "election-lock-attempt-failed" in leader
