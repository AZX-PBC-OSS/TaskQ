"""Red-team storm: the leader's guard-fatal backstop vs the storm that
caused it.

The question: when a slow/overloaded database makes every sweep tick
time out (``statement_timeout`` → ``QueryCanceledError``, client
deadlines → ``TimeoutError``, saturation → ``TooManyConnectionsError``,
restart → ``AdminShutdownError``/``CannotConnectNowError``), does the
``UnexpectedLoopErrorGuard`` deliberately-fatal backstop (5 consecutive
→ re-raise → worker teardown) fire and crash-loop the leader, stopping
maintenance fleet-wide?

Model (from ``worker/_transient.py`` and ``worker/_leader_sweeps.py``
``_sweep_loop``):

* Every error a slow-DB storm produces is in ``TRANSIENT_PG_ERRORS``.
  The sweep loop catches that tuple per sweep call BEFORE its
  ``except Exception`` backstop clause — a storm error is logged as a
  warning, marks the iteration unclean, and NEVER reaches
  ``guard.unexpected``. Crash-looping is NOT the designed answer to a
  slow database: the designed answer is per-tick warnings, drains that
  pause (committed batches stay committed), and — for the prune family —
  the ``SweepBatchSizer`` one-way reduced-tier latch.
* The guard's deliberate fatality is reachable only from OUTSIDE the
  transient set (a code bug, a data error), is bounded at
  ``max_consecutive`` (default 5) CONSECUTIVE non-transient errors with
  only a fully-clean iteration resetting the streak, and re-raises the
  ORIGINAL error. One worker tears down; the leadership advisory lock
  is session-scoped (``pg_try_advisory_lock`` in ``worker/leader.py``),
  so the dead leader's lock releases with its connection and another
  worker wins the next election — fleet-wide maintenance stops only if
  every worker carries the same non-transient bug, which is exactly the
  fail-loud contract.
* Interplay subtlety pinned below: a transient storm does not reset the
  streak either (``guard.ok()`` runs only on a clean iteration), so a
  storm interleaved between two bug hits does not buy the bug more
  budget — but it also never contributes a hit of its own.

Pinned at unit tier with the REAL classification tuple and the REAL
guard object; the fleet-wide re-election sequence is design-tier
(dispositioned above, benchmark-only to observe end-to-end).
"""

from __future__ import annotations

import asyncpg
import pytest

from taskq.worker._transient import (
    DEFAULT_MAX_CONSECUTIVE_UNEXPECTED,
    TRANSIENT_PG_ERRORS,
    UnexpectedLoopErrorGuard,
)


def _storm_error_family() -> list[BaseException]:
    """Every error shape a sustained slow-DB/saturation storm produces,
    instantiated the way the driver raises them."""
    return [
        TimeoutError("client command_timeout / pool acquire deadline"),
        asyncpg.QueryCanceledError("server statement_timeout"),
        asyncpg.TooManyConnectionsError("53300 saturated"),
        asyncpg.CannotConnectNowError("57P03 crash recovery"),
        asyncpg.AdminShutdownError("57P01 restart"),
        asyncpg.DeadlockDetectedError("40P01"),
        asyncpg.SerializationError("40001"),
        asyncpg.IdleSessionTimeoutError("idle session killed"),
        asyncpg.IdleInTransactionSessionTimeoutError("idle in tx killed"),
        asyncpg.InterfaceError("connection unusable"),
        OSError("socket died"),
    ]


async def test_slow_db_storm_errors_never_reach_the_fatal_guard() -> None:
    """Every storm error is transient-classified, so the sweep loop's
    per-sweep ``except TRANSIENT_PG_ERRORS`` clause absorbs it before
    the ``except Exception`` backstop — the guard can record ZERO hits
    from a storm of any length.

    Contract: a database that is merely slow/overloaded must never
    trip the deliberately-fatal 5-consecutive backstop; if any storm
    shape escaped the transient set, a sustained overload would
    crash-loop the leader and stop sweeps fleet-wide — the storm
    amplified into an outage by the very backstop built for bugs.
    """
    guard = UnexpectedLoopErrorGuard("leader.sweep")

    # The loop's exact classification shape, 100 storm iterations:
    # transient errors are caught (logged, iteration unclean) and never
    # handed to the guard.
    family = _storm_error_family()
    for i in range(100):
        exc = family[i % len(family)]
        try:
            raise exc
        except TRANSIENT_PG_ERRORS:
            # The per-sweep catch: warning + iteration_clean = False.
            # guard.unexpected is NOT called — this is the load-bearing
            # asymmetry under test.
            pass

    # The storm bought zero fatalities: the first genuinely-unexpected
    # error after it is strike ONE of five, not strike 101.
    guard.unexpected(RuntimeError("a real bug, not a PG moment"))
    # No raise: streak is 1 < 5. (If the storm had leaked into the
    # guard, this call would have raised the storm's 100th error.)

    # And five consecutive REAL bugs still raise the original — the
    # backstop stays reachable for what it exists for.
    with pytest.raises(RuntimeError, match="a real bug"):
        for _ in range(DEFAULT_MAX_CONSECUTIVE_UNEXPECTED - 1):
            guard.unexpected(RuntimeError("a real bug, not a PG moment"))


async def test_storm_shapes_are_all_transient_classified() -> None:
    """Each storm shape, individually, is caught by the sweep loops'
    ``except TRANSIENT_PG_ERRORS`` tuple.

    Contract: the transient set must keep covering every error a
    degraded database emits mid-storm — one shape dropping out
    (e.g. a new driver version renaming a class) silently converts
    sustained overload into the deliberately-fatal path.
    """
    for exc in _storm_error_family():
        caught = isinstance(exc, TRANSIENT_PG_ERRORS)
        assert caught, (
            f"{type(exc).__name__} is NOT in TRANSIENT_PG_ERRORS — a slow-DB "
            "storm producing it would bypass the sweep loop's transient "
            "catch and count toward the deliberately-fatal 5-consecutive "
            "backstop: the overload itself would crash-loop the leader"
        )


async def test_transient_storm_does_not_reset_the_bug_streak() -> None:
    """The interplay: ``guard.ok()`` runs only on a fully clean
    iteration, so a transient storm interleaved between two bug hits
    neither adds budget-erasing resets nor extra strikes — the streak
    persists across the storm exactly as the docstring promises.

    Contract: only a fully successful work iteration resets the streak;
    an idle or transiently-failing one must not buy the fault more time
    (but must not count toward it either).
    """
    guard = UnexpectedLoopErrorGuard("leader.sweep", max_consecutive=3)

    guard.unexpected(RuntimeError("bug 1"))
    # A long transient storm: the loop never calls guard.ok() (its
    # iterations are unclean) and never calls guard.unexpected().
    for _ in range(50):
        try:
            raise TimeoutError("storm")
        except TRANSIENT_PG_ERRORS:
            pass
    guard.unexpected(RuntimeError("bug 2"))
    # Storm in between bought nothing: streak is 2 of 3, one more hit
    # is fatal.
    with pytest.raises(RuntimeError, match="bug 3"):
        guard.unexpected(RuntimeError("bug 3"))

    # A clean iteration resets; the sequence starts over.
    guard2 = UnexpectedLoopErrorGuard("leader.sweep", max_consecutive=2)
    guard2.unexpected(RuntimeError("x"))
    guard2.ok()  # fully successful iteration: streak cleared
    guard2.unexpected(RuntimeError("y"))  # strike 1 of 2 again — not fatal
    with pytest.raises(RuntimeError, match="z"):
        guard2.unexpected(RuntimeError("z"))  # strike 2 of 2 — fatal
