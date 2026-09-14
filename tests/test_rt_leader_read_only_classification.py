"""Red-team unit pin: a read-only PG failover (SQLSTATE 25006) must be classified survivable.

During a PG failover the surviving primary is briefly read-only: every pure read
succeeds (the leader watchdog's ``SELECT 1`` probes, the election loop's probe —
leader.py ``SELECT 1`` sites) while EVERY leader write (the election upsert, the
cron tick's transaction, the sweeps) fails with ``asyncpg.ReadOnlySQLTransactionError``
(SQLSTATE 25006, read_only_sql_transaction).  The leader loops therefore keep
probing green while all their work fails — the watchdog cannot see the condition,
and the only thing that decides whether the worker survives it is the error
classification in ``taskq.worker._transient``.

Contract under test (RED until fixed): 25006 must be classified as the transient,
retryable condition it is (the module's own docstring: "One tuple, one home: any
shape a site learns, every site learns") — a read-only window resolves when the
failover completes, exactly like 57P03 cannot-connect-now, which the set already
teaches. TODAY it is absent from ``TRANSIENT_PG_ERRORS``, so the write failure rides
the ``except Exception`` backstop of every leader loop: the guard tolerates a few,
then deliberately re-raises and kills the worker's TaskGroup for the whole failover
window — a read-only *server* crash-loops the *worker*.

The companion PG file (``tests/test_rt_leader_read_only_probe_asymmetry.py``) pins
the probe/write asymmetry fact itself on a real read-only session.
"""

import asyncpg
import pytest

from taskq.worker._transient import (
    DEFAULT_MAX_CONSECUTIVE_UNEXPECTED,
    TRANSIENT_PG_ERRORS,
    UnexpectedLoopErrorGuard,
)


def test_read_only_sql_transaction_error_is_classified_transient() -> None:
    exc = asyncpg.ReadOnlySQLTransactionError("cannot execute INSERT in a read-only transaction")
    assert exc.sqlstate == "25006"
    assert isinstance(exc, TRANSIENT_PG_ERRORS), (
        "CONTRACT: a PG failover's read-only window (SQLSTATE 25006) is a survivable, "
        "retryable server state — probes succeed while every leader write fails, so "
        "the classification layer is the only thing that keeps the worker alive "
        "through it. TODAY: ReadOnlySQLTransactionError is absent from "
        "TRANSIENT_PG_ERRORS (_transient.py teaches TimeoutError, "
        "PostgresConnectionError, QueryCanceledError, AdminShutdownError, "
        "CannotConnectNowError, TooManyConnectionsError, DeadlockDetectedError, "
        "SerializationError, IdleSessionTimeoutError, "
        "IdleInTransactionSessionTimeoutError, InterfaceError, OSError — no read-only "
        "shape), so in every leader loop (election upsert, cron tick, sweeps) the "
        "write failure falls to the `except Exception` backstop: guard.unexpected "
        "counts consecutive occurrences and deliberately re-raises, killing the "
        "worker TaskGroup — a read-only server crash-loops the worker for the whole "
        "failover window. The module's own rule is 'one tuple, one home: any shape a "
        "site learns, every site learns' — the read-only shape must be taught."
    )


def test_guard_makes_unclassified_read_only_deliberately_fatal_at_the_cap() -> None:
    """Mechanism pin (GREEN): documents why the missing classification kills.

    An error outside the transient set is tolerated a few times and then
    re-raised by design — the backstop's contract.  With 25006 unclassified,
    five consecutive read-only write failures end the leader loop (and with it
    the worker's TaskGroup).  This pin holds that mechanism visible so the
    classification pin above has an observable blast radius.
    """
    guard = UnexpectedLoopErrorGuard("leader.cron")
    exc = asyncpg.ReadOnlySQLTransactionError("cannot execute INSERT in a read-only transaction")
    with pytest.raises(asyncpg.ReadOnlySQLTransactionError):
        for _ in range(DEFAULT_MAX_CONSECUTIVE_UNEXPECTED):
            guard.unexpected(exc)
