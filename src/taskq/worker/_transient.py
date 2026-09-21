"""Shared transient-PG error classification and the unexpected-error backstop.

Every long-lived worker loop that awaits Postgres treats the same set of
errors as "PG is having a moment; log and retry next tick". That set used
to be re-enumerated at every call site, and it drifted: heartbeat learned
``QueryCanceledError`` (server-side 57014 cancellation) while the leader loops kept only the OSError
flavours, so a degraded PG could throw an uncaught error into the worker
TaskGroup and tear the whole worker down mid-blip. One tuple, one home:
any shape a site learns, every site learns.

One condition the tuple form cannot express is topology: behind a
transaction-mode pooler (PgBouncer ``pool_mode=transaction``) a prepared
statement can outlive the server connection it was prepared on, and the
server answers the next Bind with a statement-name error that is a
pooling artifact, not a bug. :data:`POOLED_TRANSIENT_PG_ERRORS` and
:func:`is_transient_pg_error` are that extension - gated on the
operator's ``TASKQ_PG_IS_POOLED`` declaration, because no client can
detect a pooler from the wire.

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
#: - ``QueryCanceledError``: server-side 57014, a DBA ran
#:   pg_cancel_backend, or a server-side ``statement_timeout`` fired. Not
#:   a client-side ``command_timeout`` (that raises ``TimeoutError``); kept
#:   in the tuple because a server-side cancel is equally transient.
#: - ``AdminShutdownError``: 57P01, PG restart/shutdown. An
#:   OperatorInterventionError, NOT a PostgresConnectionError: notify.py
#:   learned this one the hard way.
#: - ``CannotConnectNowError``: 57P03, server in crash recovery or still
#:   starting. Same OperatorInterventionError family.
#: - ``ReadOnlySQLTransactionError``: 25006, read_only_sql_transaction ,
#:   a PG failover's read-only window on the surviving primary. Pure
#:   reads keep succeeding (the watchdog's and election loop's ``SELECT
#:   1`` probes) while every leader write fails, so this classification
#:   is the only thing keeping the worker alive through the window; the
#:   condition resolves when the failover completes, exactly like 57P03
#:   resolves when recovery finishes.
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
#: ``LockNotAvailableError`` (55P03) is raised by the bounded advisory
#: acquires' scoped ``lock_timeout``, every acquire site converts it to
#: its own typed outcome (``MaxPendingLockTimeoutError`` /
#: ``UniqueForLockTimeoutError`` / the limiter's fail-closed denial), so
#: one reaching this classifier means a leaked or operator-set
#: ``lock_timeout`` hit an ordinary statement, surfacing loudly beats
#: silently retrying under an unknown bound; data errors (constraint
#: violations, undefined tables) are bugs, and the guard below makes them
#: loud and then deliberately fatal; statement-name errors (26000,
#: 42P05) are bugs on a direct connection but pooler artifacts under
#: transaction-mode pooling - see :data:`POOLED_TRANSIENT_PG_ERRORS`.
TRANSIENT_PG_ERRORS: tuple[type[BaseException], ...] = (
    TimeoutError,
    asyncpg.PostgresConnectionError,
    asyncpg.QueryCanceledError,
    asyncpg.AdminShutdownError,
    asyncpg.CannotConnectNowError,
    asyncpg.ReadOnlySQLTransactionError,
    asyncpg.TooManyConnectionsError,
    asyncpg.DeadlockDetectedError,
    asyncpg.SerializationError,
    asyncpg.IdleSessionTimeoutError,
    asyncpg.IdleInTransactionSessionTimeoutError,
    asyncpg.InterfaceError,
    OSError,
)

#: Errors that mean "PG refused this call and will keep refusing it":
#: the server's answer is a property of the role's grants, not of the
#: moment. Deliberately disjoint from :data:`TRANSIENT_PG_ERRORS`, a
#: refusal retried next tick re-fails identically forever, so classifying
#: it as transient would livelock the retrying loop while logging like a
#: blip. A site catching from this tuple must DEGRADE instead: skip the
#: refused operation, log once at WARN naming what was refused, and keep
#: working without it. Anything from this tuple that escapes a site's own
#: handling still lands in the loop's :class:`UnexpectedLoopErrorGuard`,
#: where a permanent fault is deliberately fatal after the budget, never
#: a silent infinite retry.
#:
#: - ``InsufficientPrivilegeError``: 42501, a managed Postgres
#:   restricting a function to admin/superuser roles (the same posture
#:   class that restricts ``pg_terminate_backend``; here it is the
#:   election loop's courtesy ``pg_try_advisory_lock`` probe). The grant
#:   does not appear on its own: either the operator grants it or the
#:   deployment refuses forever, and the courtesy must never gate the
#:   work it accompanies in the meantime.
PERMANENT_PG_REFUSALS: tuple[type[BaseException], ...] = (asyncpg.InsufficientPrivilegeError,)

#: The pooled-context companion to :data:`TRANSIENT_PG_ERRORS`: errors a
#: transaction-mode pooler (PgBouncer ``pool_mode=transaction``, RDS
#: Proxy, Supavisor) produces when its server-connection remap splits a
#: prepared statement's Parse from its Bind - asyncpg caches prepared
#: statements per connection, so after a remap the cached name names a
#: statement that does not exist on the server connection the Bind
#: landed on:
#:
#: - ``InvalidSQLStatementNameError``: 26000, "unnamed prepared
#:   statement does not exist" - the Parse and the Bind landed on
#:   different server connections.
#: - ``DuplicatePreparedStatementError``: 42P05, "prepared statement
#:   ... already exists" - the re-Prepare hit a server connection that
#:   still holds the original registration under the same name.
#:
#: Deliberately NOT folded into :data:`TRANSIENT_PG_ERRORS`: on a direct
#: connection neither error can be produced by the server without an
#: asyncpg bug (the driver's own cache is the only author of statement
#: names), so classifying them unconditionally as transient would convert
#: a real driver contract violation into a silent forever-retry. The
#: pooled condition is a deployment topology fact no client can detect -
#: a pooler speaks plain Postgres on the wire - so it arrives as the
#: operator's declaration (``TASKQ_PG_IS_POOLED``) and is consumed only
#: through :func:`is_transient_pg_error`.
POOLED_TRANSIENT_PG_ERRORS: tuple[type[BaseException], ...] = (
    asyncpg.InvalidSQLStatementNameError,
    asyncpg.DuplicatePreparedStatementError,
)


def is_transient_pg_error(exc: BaseException, *, pooled: bool = False) -> bool:
    """Whether *exc* means "PG is having a moment; retry next tick".

    The ``except TRANSIENT_PG_ERRORS`` tuple form cannot carry the pooled
    condition (an ``except`` clause has no room for a settings lookup),
    so call sites whose backend can sit behind a transaction-mode pooler
    - and that learn the topology from settings - branch on this
    predicate instead of the bare tuple. ``pooled=True`` extends the
    transient set with :data:`POOLED_TRANSIENT_PG_ERRORS`; the flag is
    the operator's ``TASKQ_PG_IS_POOLED`` declaration, read once per
    loop, not per exception.
    """
    if isinstance(exc, TRANSIENT_PG_ERRORS):
        return True
    return pooled and isinstance(exc, POOLED_TRANSIENT_PG_ERRORS)


#: Default consecutive-unexpected-failure budget; module-level so tests
#: can shrink it without threading a knob through every loop.
DEFAULT_MAX_CONSECUTIVE_UNEXPECTED = 5

_unexpected_loop_errors = get_meter().create_counter(
    name="taskq.worker.loop_unexpected_errors_total",
    unit="1",
    description="Unexpected (non-transient) errors tolerated by a long-lived "
    "loop's backstop, labelled by loop: the leader maintenance loops and "
    "the worker producer loop. Anything above zero warrants investigation: "
    "either PG produced a shape the transient set should learn, or the loop "
    "has a bug.",
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
            "loop-unexpected-error",
            kind="loop_unexpected_error",
            loop=self._loop,
            error=repr(exc),
            error_type=type(exc).__name__,
            consecutive=self._consecutive,
            max_consecutive=self._max_consecutive,
        )
        if self._consecutive >= self._max_consecutive:
            raise exc
