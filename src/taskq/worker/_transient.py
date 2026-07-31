"""Shared transient-PG error classification and the unexpected-error backstop.

Every long-lived worker loop that awaits Postgres treats the same set of
errors as "PG is having a moment; log and retry next tick". That set used
to be re-enumerated at every call site, and it drifted: heartbeat learned
``QueryCanceledError`` (server-side 57014 cancellation) while the leader loops kept only the OSError
flavours, so a degraded PG could throw an uncaught error into the worker
TaskGroup and tear the whole worker down mid-blip. One tuple, one home:
any shape a site learns, every site learns.

The other half of the class: errors OUTSIDE the transient set. Before,
they escaped into the loop's TaskGroup and crashed the worker, which is
right for a contract violation but wrong for a one-off driver surprise,
and silent in both directions (no distinct record before the crash; a
blanket ``except Exception`` like cron's would instead retry a real bug
forever, ticking but doing no work, a functional zombie detector 2 cannot
see because its tick lands at the top of the loop). The guard below makes
the choice explicit and observable.
"""

import asyncpg
import structlog

from taskq.obs import get_logger, get_meter

_log: structlog.stdlib.BoundLogger = get_logger(__name__)

#: Errors that mean "PG is having a moment; retry next tick":
#: - ``TimeoutError``: client-side deadlines (``asyncio.timeout``,
#:   ``command_timeout`` firing locally, pool-acquire timeout).
#: - ``PostgresConnectionError``: the connection is gone (covers
#:   ``ConnectionDoesNotExistError`` / ``ConnectionFailureError``).
#: - ``InterfaceError`` / ``OSError``: the connection is unusable or the
#:   socket died.
#: - ``QueryCanceledError``: server-side 57014 — a DBA ran
#:   pg_cancel_backend, or a server-side ``statement_timeout`` fired. Not
#:   a client-side ``command_timeout`` (that raises ``TimeoutError``); kept
#:   in the tuple because a server-side cancel is equally transient.
#: - ``AdminShutdownError``: 57P01, PG restart/shutdown. An
#:   OperatorInterventionError, NOT a PostgresConnectionError: notify.py
#:   learned this one the hard way.
#: - ``CannotConnectNowError``: 57P03, server in crash recovery or still
#:   starting. Same OperatorInterventionError family.
#: - ``TooManyConnectionsError``: 53300, server saturated; a later tick
#:   can succeed.
#: - ``DeadlockDetectedError`` / ``SerializationError``: 40P01/40001, the
#:   canonical retry-the-transaction pair.
#: - ``IdleSessionTimeoutError`` / ``IdleInTransactionSessionTimeoutError``:
#:   operator-set session timeouts killing an idle (or idle-in-tx)
#:   dedicated conn; the conn-loss path rebuilds on the next tick.
#:
#: Deliberately NOT here: auth failures (``InvalidPasswordError`` et al.)
#: are not transient for static DSNs and must not retry silently (the
#: credential-provider reopen path has its own deliberate broad catch);
#: ``LockNotAvailableError`` needs a lock_timeout TaskQ never sets; data
#: errors (constraint violations, undefined tables) are bugs, and the
#: guard below makes them loud and then deliberately fatal.
TRANSIENT_PG_ERRORS: tuple[type[BaseException], ...] = (
    TimeoutError,
    asyncpg.PostgresConnectionError,
    asyncpg.QueryCanceledError,
    asyncpg.AdminShutdownError,
    asyncpg.CannotConnectNowError,
    asyncpg.TooManyConnectionsError,
    asyncpg.DeadlockDetectedError,
    asyncpg.SerializationError,
    asyncpg.IdleSessionTimeoutError,
    asyncpg.IdleInTransactionSessionTimeoutError,
    asyncpg.InterfaceError,
    OSError,
)

#: Default consecutive-unexpected-failure budget; module-level so tests
#: can shrink it without threading a knob through every loop.
DEFAULT_MAX_CONSECUTIVE_UNEXPECTED = 5

_unexpected_loop_errors = get_meter().create_counter(
    name="taskq.worker.leader_loop_unexpected_errors_total",
    unit="1",
    description="Unexpected (non-transient) errors tolerated by a leader "
    "maintenance loop's backstop, labelled by loop. Anything above zero "
    "warrants investigation: either PG produced a shape the transient set "
    "should learn, or the loop has a bug.",
)


class UnexpectedLoopErrorGuard:
    """Per-loop backstop for errors outside the transient set.

    Tolerates isolated surprises with a loud, distinct, alertable record
    per occurrence, but re-raises after *max_consecutive* in a row, so a
    permanent fault (a code bug, not a PG blip) still kills the worker
    deliberately instead of retrying forever into a zombie that ticks but
    does no work. Only a fully successful work iteration resets the
    streak: an idle or transiently-failing one must not buy the fault
    more time.
    """

    def __init__(
        self,
        loop: str,
        *,
        max_consecutive: int | None = None,
    ) -> None:
        if max_consecutive is None:
            # Resolved at construction (not as a def-time default) so the
            # module constant is the single source, and tests can shrink it.
            max_consecutive = DEFAULT_MAX_CONSECUTIVE_UNEXPECTED
        if max_consecutive < 1:
            raise ValueError(f"max_consecutive must be >= 1, got {max_consecutive}")
        self._loop = loop
        self._max_consecutive = max_consecutive
        self._consecutive = 0

    def ok(self) -> None:
        """A work iteration completed without error: reset the streak."""
        self._consecutive = 0

    def unexpected(self, exc: BaseException) -> None:
        """Log loud + count; re-raise the original error at the cap."""
        self._consecutive += 1
        _unexpected_loop_errors.add(1, {"loop": self._loop})
        _log.error(
            "leader-loop-unexpected-error",
            kind="leader_loop_unexpected_error",
            loop=self._loop,
            error=repr(exc),
            error_type=type(exc).__name__,
            consecutive=self._consecutive,
            max_consecutive=self._max_consecutive,
        )
        if self._consecutive >= self._max_consecutive:
            raise exc
