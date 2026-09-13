"""PostgreSQL fallback implementations for sliding-window rate limiter.

All PG-path methods (acquire, peek, reset, refund for both log and GCRA
styles) live here as module-level functions taking ``self: SlidingWindow``
as the first parameter, following the testing-module pattern.

Time domain: every window predicate and TAT epoch runs on the PG server
clock (``clock_timestamp()`` / ``EXTRACT(EPOCH FROM clock_timestamp())``)
read in the same statement or transaction as the state it measures — the
shared window state is server-domain by construction, so callers on nodes
with divergent Python clocks are all measured against the same window.
"""

import asyncio
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from taskq.backend._records import jsonb_param, jsonb_to_dict
from taskq.ratelimit._decision_log import log_decision
from taskq.ratelimit.decision import RateLimitDecision, RateLimitState

if TYPE_CHECKING:
    import asyncpg
    from asyncpg.pool import PoolConnectionProxy

    from taskq.ratelimit.sliding_window import SlidingWindow
    from taskq.settings import WorkerSettings

logger = structlog.get_logger("taskq.ratelimit._sliding_window_pg")

__all__ = [
    "_acquire_pg_gcra",
    "_acquire_pg_log",
    "_peek_pg_gcra",
    "_peek_pg_log",
    "_refund_pg_gcra",
    "_refund_pg_log",
    "_reset_pg_gcra",
    "_reset_pg_log",
]


async def _peek_pg_log(
    self: "SlidingWindow",
    pg_pool: "asyncpg.Pool | None",
    settings: "WorkerSettings | None",
) -> RateLimitState:
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")

    window_ms = int(self._window.total_seconds() * 1000)
    schema = settings.schema_name

    # Why clock_timestamp() in the predicates: the window boundary is
    # measured in the same domain as the stored ``ts`` values (both are the
    # PG server clock), so a skewed caller cannot shrink or stretch the
    # window it is measured against.
    count_sql = (
        f'SELECT count(*) FROM "{schema}".rate_limit_window_entries '  # noqa: S608
        f"WHERE bucket_name = $1 "
        f"AND ts >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond')"
    )
    oldest_sql = (
        f"SELECT ts, clock_timestamp() AS server_now "  # noqa: S608
        f'FROM "{schema}".rate_limit_window_entries '
        f"WHERE bucket_name = $1 "
        f"AND ts >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond') "
        f"ORDER BY ts ASC LIMIT 1"
    )

    async with pg_pool.acquire() as conn:
        count_row = await conn.fetchrow(count_sql, self._name, window_ms)
        count = int(count_row["count"]) if count_row else 0

        is_exhausted = count >= self._limit
        retry_after: timedelta | None = None

        if is_exhausted and count > 0:
            oldest_row = await conn.fetchrow(oldest_sql, self._name, window_ms)
            if oldest_row is not None:
                oldest_ts = oldest_row["ts"]
                server_now = oldest_row["server_now"]
                retry_after = (oldest_ts + timedelta(milliseconds=window_ms)) - server_now
                if retry_after is not None and retry_after <= timedelta(0):
                    retry_after = timedelta(milliseconds=1)

    return RateLimitState(
        bucket_name=self._name,
        backend="postgres",
        is_exhausted=is_exhausted,
        remaining=float(max(0, self._limit - count)),
        retry_after=retry_after,
        limit=self._limit,
        window=self._window,
        style="log",
    )


async def _peek_pg_gcra(
    self: "SlidingWindow",
    pg_pool: "asyncpg.Pool | None",
    settings: "WorkerSettings | None",
) -> RateLimitState:
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")

    window_ms = int(self._window.total_seconds() * 1000)
    window_seconds = window_ms / 1000.0
    emission_interval_seconds = window_seconds / self._limit
    delay_tolerance_seconds = window_seconds
    schema = settings.schema_name

    select_sql = (
        f"SELECT state, EXTRACT(EPOCH FROM clock_timestamp()) AS now_s "  # noqa: S608  # Why: schema_name pre-validated; bucket_name is $1-bound
        f'FROM "{schema}".rate_limit_buckets '
        f"WHERE bucket_name = $1 AND kind = 'gcra'"
    )

    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(select_sql, self._name)
        if row is None:
            now_seconds = float(await conn.fetchval("SELECT EXTRACT(EPOCH FROM clock_timestamp())"))
            current_tat = now_seconds
        else:
            now_seconds = float(row["now_s"])
            state = jsonb_to_dict(row["state"])
            current_tat = float(state.get("tat", now_seconds))  # type: ignore[index]  # Why: state is non-None; fallback to now_seconds for rows missing "tat"

    tat = max(now_seconds, current_tat)
    remaining = float(
        max(0, int((delay_tolerance_seconds - (tat - now_seconds)) / emission_interval_seconds))
    )
    is_exhausted = remaining <= 0
    retry_after: timedelta | None = None
    if is_exhausted:
        new_tat = tat + emission_interval_seconds
        allow_at = new_tat - delay_tolerance_seconds
        retry_after_seconds = allow_at - now_seconds
        if retry_after_seconds <= 0:
            retry_after_seconds = 0.001
        retry_after = timedelta(seconds=retry_after_seconds)

    return RateLimitState(
        bucket_name=self._name,
        backend="postgres",
        is_exhausted=is_exhausted,
        remaining=remaining,
        retry_after=retry_after,
        limit=self._limit,
        window=self._window,
        style="gcra",
    )


async def _reset_pg_log(
    self: "SlidingWindow",
    pg_pool: "asyncpg.Pool | None",
    settings: "WorkerSettings | None",
) -> None:
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")

    schema = settings.schema_name
    delete_sql = (
        f'DELETE FROM "{schema}".rate_limit_window_entries '  # noqa: S608
        f"WHERE bucket_name = $1"
    )
    await pg_pool.execute(delete_sql, self._name)


async def _reset_pg_gcra(
    self: "SlidingWindow",
    pg_pool: "asyncpg.Pool | None",
    settings: "WorkerSettings | None",
) -> None:
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")

    schema = settings.schema_name
    delete_sql = (
        f'DELETE FROM "{schema}".rate_limit_buckets '  # noqa: S608
        f"WHERE bucket_name = $1 AND kind = 'gcra'"
    )
    await pg_pool.execute(delete_sql, self._name)


async def _refund_pg_gcra(
    self: "SlidingWindow",
    decision: RateLimitDecision,
    pg_pool: "asyncpg.Pool | None",
    settings: "WorkerSettings | None",
) -> None:
    if decision.previous_state is None:
        return
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres gcra refund")
    if settings is None:
        raise RuntimeError("settings not injected for postgres gcra refund")

    schema = settings.schema_name
    pre_acquire_tat = float(decision.previous_state["pre_acquire_tat"])  # type: ignore[arg-type]  # Why: dict[str, object] value is float at runtime; type narrowing not possible from generic dict
    post_acquire_tat = float(decision.previous_state["post_acquire_tat"])  # type: ignore[arg-type]  # Why: dict[str, object] value is float at runtime; type narrowing not possible from generic dict

    refund_sql = (
        f'UPDATE "{schema}".rate_limit_buckets '  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name is $1-bound
        f"SET state = jsonb_set(state, '{{tat}}', to_jsonb($2::float)), updated_at = clock_timestamp() "
        f"WHERE bucket_name = $1 "
        f"AND kind = 'gcra' "
        f"AND (state->>'tat')::float = $3::float"
    )

    await pg_pool.execute(refund_sql, self._name, pre_acquire_tat, post_acquire_tat)


async def _refund_pg_log(
    self: "SlidingWindow",
    decision: RateLimitDecision,
    pg_pool: "asyncpg.Pool | None",
    settings: "WorkerSettings | None",
) -> None:
    if decision.request_id is None:
        return
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres log refund")
    if settings is None:
        raise RuntimeError("settings not injected for postgres log refund")

    schema = settings.schema_name
    delete_sql = (
        f'DELETE FROM "{schema}".rate_limit_window_entries '  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name and request_id are $1/$2-bound
        f"WHERE bucket_name = $1 AND request_id = $2::uuid"
    )
    await pg_pool.execute(delete_sql, self._name, decision.request_id)
    logger.debug(
        "ratelimit-refund",
        bucket_name=self._name,
        backend="postgres",
        style="log",
        request_id=decision.request_id,
    )


#: Bounded wait (milliseconds) for the per-bucket log-style advisory
#: lock. The lock is held across a DELETE + count/INSERT pair (a few
#: round trips — low single-digit milliseconds on a healthy pool), so
#: 5 s tolerates a burst of hundreds of queued racers while capping
#: tail latency instead of letting it scale with the racer count, and a
#: black-holed holder (dead TCP, no FIN) blocks its bucket for at most
#: one budget instead of until the server's keepalives reap it. Same
#: default as the enqueue path's DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS. A
#: racer that exhausts the budget gets the limiter's DENIAL outcome —
#: fail closed, never an admission. ``0`` (or less) waits indefinitely,
#: matching the ``lock_timeout`` GUC convention used by migrate.py.
#: A module constant tunable only through the acquire's private kwargs
#: today — settings plumbing is a filed follow-up.
DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS: float = 5000.0

#: Fast-path statement: returns bool (acquired or not) without queueing,
#: so an uncontended racer pays exactly one round trip. hashtextextended
#: keys follow the convention shared with the enqueue path's locks; a
#: collision between two different lock keys costs a little needless
#: serialization and never correctness.
_SLIDING_WINDOW_TRY_LOCK_SQL = "SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))"

#: Contended-tier statement: queues the caller behind the current holder
#: in Postgres' lock scheduler, bounded by the ``lock_timeout`` GUC set
#: inside the surrounding savepoint.
_SLIDING_WINDOW_BLOCKING_LOCK_SQL = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))"

#: Reads the session/transaction's current ``lock_timeout`` so the
#: contended tier can restore it exactly — never clobbers a caller-set
#: bound, never assumes the default is "0".
_SLIDING_WINDOW_LOCK_TIMEOUT_READ_SQL = "SELECT current_setting('lock_timeout')"

#: ``set_config(..., true)`` is SET LOCAL semantics with a bindable
#: parameter (utility ``SET`` statements cannot take extended-protocol
#: parameters), so the budget value never has to be interpolated into
#: SQL text.
_SLIDING_WINDOW_LOCK_TIMEOUT_SET_SQL = "SELECT set_config('lock_timeout', $1, true)"

#: Extra seconds the client-side backstop waits past the budget. Must
#: comfortably cover the contended tier's ~4 short round trips around the
#: blocking acquire plus the savepoint rollback the cancellation path
#: issues, without converting legitimate near-budget grants into client
#: timeouts.
DEFAULT_SLIDING_WINDOW_LOCK_CLIENT_BACKSTOP_SLACK_S: float = 0.5


async def acquire_advisory_xact_lock_bounded(
    conn: "asyncpg.Connection | PoolConnectionProxy",
    lock_key: str,
    *,
    timeout_ms: float,
) -> bool:
    """Acquire the transaction-scoped advisory lock *lock_key*, bounded by
    *timeout_ms* — the module-local mirror of the enqueue branch's
    ``taskq._advisory`` helper, kept shape-identical (same name,
    signature, constants, and behavior) so the integration pass can
    dedupe this copy with an import swap.

    Returns True once the lock is HELD for the rest of the transaction
    (a transaction-scoped advisory lock acquired inside the savepoint
    survives the savepoint's RELEASE); False when the budget expired
    first — the caller layers its own exhaustion outcome on the False.
    A raw driver error never surfaces from contention itself, only from
    non-contention failures (network, SQL) which propagate unchanged.

    ``timeout_ms <= 0`` waits indefinitely: one plain blocking acquire,
    no savepoint and no GUC statements — the ``lock_timeout`` GUC
    convention shared with migrate.py, and the documented opt-out for
    callers that want the pre-bound queueing behavior.

    Two tiers, because the two failure regimes need different owners for
    the wait bound:

    - Uncontended (the overwhelmingly common case): one
      ``pg_try_advisory_xact_lock`` statement — round-trip count
      identical to the pre-bounded era, so the bound costs the common
      case nothing.
    - Contended: a server-side bounded blocking acquire —
      ``pg_advisory_xact_lock`` queued by Postgres' own lock scheduler,
      which hands the lock to the next waiter in ~ms as each holder's
      transaction ends. MEASURED (N same-key racers, ~1.5 ms holder
      critical section, PG 18): the server-side queue drains 128 racers
      in ~0.4 s with zero timeouts, while a client-side try-lock poll
      loop (5 ms->100 ms exponential, no jitter) took ~5.1 s with
      17-59% of racers exhausting their budget — failed pollers sleep
      while the lock sits idle between poll waves, draining ~1-2 racers
      per 100 ms against the queue's ~1 per few ms. A poll only wins
      when the holder is black-holed, and the client-side backstop
      below covers that regime more cheaply.

    Contended-tier mechanics (each step verified against live PG 18):

    1. ``SAVEPOINT`` (asyncpg's nested ``async with conn.transaction()``;
       on a transaction-less connection asyncpg opens a real short
       transaction instead — the lock then releases at its COMMIT,
       matching the bare-connection advisory-only semantics the try-lock
       fast path already has).
    2. Save the prior ``lock_timeout`` (``current_setting``), then
       ``set_config('lock_timeout', '<budget>ms', true)``.
    3. ``SELECT pg_advisory_xact_lock(...)`` — the bounded blocking wait.
       On timeout the statement raises SQLSTATE 55P03
       (:class:`asyncpg.exceptions.LockNotAvailableError`); the savepoint
       context manager rolls back, which (a) restores the transaction to
       a usable state — a raw statement error would otherwise leave
       "current transaction is aborted" behind — and (b) undoes the GUC
       set above. The exception is caught OUTSIDE the context manager and
       converted to the False return.
    4. On success, restore the saved ``lock_timeout`` BEFORE the
       savepoint's RELEASE: ``SET LOCAL``/``set_config(local=true)``
       effects are undone by ROLLBACK TO SAVEPOINT but PERSIST through
       RELEASE, so skipping the restore would leak the wait bound onto
       every later statement of the same transaction (and clobber a
       caller-set bound with the restore happening at all).

    Client-side backstop: the whole contended tier is wrapped in
    ``asyncio.wait_for(..., budget + slack)``. Why BOTH layers: the
    server-side timeout is precise (it fires at the budget even under
    client-side event-loop stalls, keeps the transaction usable via the
    savepoint rollback, and generates no cancel traffic on healthy
    pools), but it cannot fire if the server is UNREACHABLE — a network
    black hole where the acquire statement never returns. The backstop's
    cancellation triggers the same savepoint rollback (asyncpg resyncs
    the connection on cancellation; an advisory xact lock granted inside
    the savepoint is released by its ROLLBACK TO SAVEPOINT, so a cancel
    landing after the grant leaves no phantom holder), and the same
    False is returned. The slack keeps the backstop strictly behind the
    server-side timeout on any healthy network.
    """
    # Why a function-level import: this module is transitively imported
    # by taskq.testing, which must stay importable without the asyncpg
    # driver installed; the acquire only ever runs against a real pool,
    # where asyncpg is guaranteed present.
    from asyncpg.exceptions import LockNotAvailableError

    if await conn.fetchval(_SLIDING_WINDOW_TRY_LOCK_SQL, lock_key):
        return True
    if timeout_ms <= 0:
        await conn.execute(_SLIDING_WINDOW_BLOCKING_LOCK_SQL, lock_key)
        return True

    async def _contended_blocking_acquire() -> None:
        async with conn.transaction():
            prior = await conn.fetchval(_SLIDING_WINDOW_LOCK_TIMEOUT_READ_SQL)
            await conn.execute(_SLIDING_WINDOW_LOCK_TIMEOUT_SET_SQL, f"{round(timeout_ms)}ms")
            await conn.execute(_SLIDING_WINDOW_BLOCKING_LOCK_SQL, lock_key)
            await conn.execute(_SLIDING_WINDOW_LOCK_TIMEOUT_SET_SQL, str(prior))

    try:
        await asyncio.wait_for(
            _contended_blocking_acquire(),
            timeout=timeout_ms / 1000.0 + DEFAULT_SLIDING_WINDOW_LOCK_CLIENT_BACKSTOP_SLACK_S,
        )
    except (LockNotAvailableError, TimeoutError):
        return False
    return True


async def _acquire_pg_log(
    self: "SlidingWindow",
    pg_pool: "asyncpg.Pool | None",
    settings: "WorkerSettings | None",
    request_id: UUID | None,
    *,
    lock_timeout_ms: float = DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS,
) -> RateLimitDecision:
    """Acquire log-style against PG.

    Every window predicate and the inserted ``ts`` are ``clock_timestamp()``
    — the PG server clock owns the shared window state, so nodes with
    divergent Python clocks all get measured against the same window.

    The per-bucket advisory lock is acquired with the two-tier bounded
    acquire (``acquire_advisory_xact_lock_bounded`` above; default
    :data:`DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS`). On budget
    exhaustion the acquire FAILS CLOSED: it returns the limiter's denial
    outcome — ``allowed=False`` with a retry hint, never an exception and
    never an admission — so a racer that could not check the window can
    never over-admit past the limit.
    """
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")
    if request_id is None:
        raise RuntimeError("request_id required for log-style PG acquire")

    window_ms = int(self._window.total_seconds() * 1000)
    schema = settings.schema_name

    delete_sql = (
        f'DELETE FROM "{schema}".rate_limit_window_entries '  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name is $1-bound
        f"WHERE bucket_name = $1 "
        f"AND ts < clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond')"
    )
    insert_sql = (
        f'INSERT INTO "{schema}".rate_limit_window_entries (bucket_name, ts, request_id) '  # noqa: S608  # Why: schema_name pre-validated; values are $1/$3-bound
        f"SELECT $1, clock_timestamp(), $3::uuid "
        f"WHERE ("
        f'SELECT count(*) FROM "{schema}".rate_limit_window_entries '
        f"WHERE bucket_name = $1 "
        f"AND ts >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond')"
        f") < $4::integer "
        f"RETURNING 1"
    )
    retry_select_sql = (
        f"SELECT ts, clock_timestamp() AS server_now "  # noqa: S608  # Why: schema_name pre-validated; bucket_name is $1-bound
        f'FROM "{schema}".rate_limit_window_entries '
        f"WHERE bucket_name = $1 "
        f"AND ts >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond') "
        f"ORDER BY ts ASC LIMIT 1"
    )
    count_sql = (
        f'SELECT count(*) FROM "{schema}".rate_limit_window_entries '  # noqa: S608  # Why: schema_name pre-validated; bucket_name is $1-bound
        f"WHERE bucket_name = $1 "
        f"AND ts >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond')"
    )

    allowed: bool
    retry_after: timedelta
    count_after: int

    # Serialise acquirers per bucket: under READ COMMITTED the DELETE +
    # INSERT ... WHERE count < N pair is not serialised — two concurrent
    # acquires can each count the pre-insert window and both insert,
    # over-admitting past the limit. A transaction-scoped advisory lock
    # makes the whole delete/count/insert sequence atomic per bucket;
    # distinct buckets hash to distinct locks and stay parallel.
    #
    # The key is schema-qualified (same shape as the bucket's Redis key,
    # ``taskq:{schema}:sw:{name}``): advisory locks are database-scoped,
    # so a bare bucket name is shared by every schema in the database —
    # two deployments in one database would serialize on one lock while
    # operating on different ``"{schema}".rate_limit_window_entries``
    # tables. Qualifying keeps the lock's scope identical to the table
    # it serializes access to.
    #
    # Why a bounded TWO-TIER acquire and not the unbounded blocking
    # acquire this path once took (the same machinery as the enqueue
    # path's locks — see acquire_advisory_xact_lock_bounded above for
    # the measured rationale): every racer on this lock holds it across
    # its own DELETE + count/INSERT round trips, so an unbounded
    # blocking acquire makes N concurrent dispatches of a rate-limited
    # actor queue on one lock — tail latency linear in the racer count,
    # and a black-holed holder (dead TCP, no FIN; the server reaps it
    # only via keepalives) blocks the whole bucket's dispatch until
    # then. The two-tier acquire keeps the happy path at exactly one
    # try-lock statement, queues contended racers SERVER-SIDE (Postgres'
    # lock scheduler hands off at holder-release rate, not at a client
    # poll cadence), bounds the wait with a savepoint-scoped
    # lock_timeout, and backstops the network black hole client-side.
    # Once acquired, the lock is transaction-scoped and the
    # delete/count/insert sequence below is unchanged — the window
    # itself stays EXACT.
    #
    # On budget exhaustion the acquire FAILS CLOSED: a racer that could
    # not check the window must never over-admit, so it returns the
    # limiter's denial outcome — RateLimitDecision allowed=False with a
    # retry hint — never an exception, never an admission. A lock-timeout
    # denial is NOT an empty bucket; operators should read it the same
    # way as any other denial (backpressure — the dispatch layer snoozes
    # and re-promotes the job either way), with the distinct
    # ratelimit-lock-timeout warning below as the signal that the bucket
    # (or its holder) is contended or sick rather than merely busy. No
    # counter bump: taskq.backpressure.errors is enqueue-scoped
    # (actor-keyed); the limiter's denial channel is the
    # rate-limit-decision log event, which this denial flows through
    # like any other. retry_after carries one more budget — the holder's
    # critical section is a few round trips, so if this budget expired
    # the honest earliest re-check is after another full one.
    lock_key = f"taskq:{schema}:sw:{self._name}"

    async with pg_pool.acquire() as conn, conn.transaction():
        if not await acquire_advisory_xact_lock_bounded(conn, lock_key, timeout_ms=lock_timeout_ms):
            logger.warning(
                "ratelimit-lock-timeout",
                bucket_name=self._name,
                backend="postgres",
                lock_timeout_ms=lock_timeout_ms,
            )
            result = RateLimitDecision(
                allowed=False,
                remaining=0.0,
                retry_after=timedelta(milliseconds=lock_timeout_ms),
                bucket_name=self._name,
                backend="postgres",
                request_id=str(request_id),
            )
            log_decision(result, style=self._style)
            return result

        await conn.execute(delete_sql, self._name, window_ms)

        inserted = await conn.fetchrow(
            insert_sql,
            self._name,
            window_ms,
            request_id,
            self._limit,
        )

        if inserted is not None:
            allowed = True
            count_row = await conn.fetchrow(count_sql, self._name, window_ms)
            count_after = int(count_row["count"]) if count_row is not None else self._limit
            retry_after = timedelta(0)
        else:
            allowed = False
            oldest_row = await conn.fetchrow(retry_select_sql, self._name, window_ms)
            if oldest_row is not None:
                oldest_ts = oldest_row["ts"]
                server_now = oldest_row["server_now"]
                retry_after = (oldest_ts + timedelta(milliseconds=window_ms)) - server_now
                if retry_after <= timedelta(0):
                    retry_after = timedelta(milliseconds=1)
            else:
                retry_after = timedelta(milliseconds=1)
            count_after = 0

    result = RateLimitDecision(
        allowed=allowed,
        remaining=float(self._limit - count_after) if allowed else 0.0,
        retry_after=retry_after,
        bucket_name=self._name,
        backend="postgres",
        request_id=str(request_id),
    )
    log_decision(result, style=self._style)
    return result


async def _acquire_pg_gcra(
    self: "SlidingWindow",
    pg_pool: "asyncpg.Pool | None",
    settings: "WorkerSettings | None",
) -> RateLimitDecision:
    """Acquire GCRA-style against PG.

    The TAT epoch math runs on ``EXTRACT(EPOCH FROM clock_timestamp())``
    read inside the same locked transaction, so the stored TAT is
    server-domain by construction and a node with a skewed Python clock
    cannot move the shared admission boundary.
    """
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")

    window_ms = int(self._window.total_seconds() * 1000)
    window_seconds = window_ms / 1000.0
    emission_interval_seconds = window_seconds / self._limit
    delay_tolerance_seconds = window_seconds
    schema = settings.schema_name

    select_sql = (
        f"SELECT kind, state, EXTRACT(EPOCH FROM clock_timestamp()) AS now_s "  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name is $1-bound
        f'FROM "{schema}".rate_limit_buckets '
        f"WHERE bucket_name = $1 FOR UPDATE"
    )
    # Cold-start guard mirroring the token-bucket PG path: SELECT ... FOR
    # UPDATE cannot lock a row that does not exist yet, so two concurrent
    # first acquires would each read `row is None`, each admit, and race
    # last-writer-wins on the TAT. Pre-seed a row stamped with the
    # server-clock TAT (idempotent — DO NOTHING on conflict) so first use
    # also serialises on the row lock below.
    preseed_sql = (
        f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at) '  # noqa: S608  # Why: schema_name pre-validated; values are $1-bound
        f"VALUES ($1, 'gcra', "
        f"jsonb_build_object('tat', EXTRACT(EPOCH FROM clock_timestamp())), clock_timestamp()) "
        f"ON CONFLICT (bucket_name) DO NOTHING"
    )
    upsert_sql = (
        f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at) '  # noqa: S608  # Why: schema_name pre-validated; values are $1/$2-bound
        f"VALUES ($1, 'gcra', $2::jsonb, clock_timestamp()) "
        f"ON CONFLICT (bucket_name) DO UPDATE "
        f"SET state = EXCLUDED.state, updated_at = EXCLUDED.updated_at "
        f"WHERE rate_limit_buckets.kind = 'gcra' "
        f"RETURNING 1"
    )

    allowed: bool
    retry_after: timedelta
    remaining_estimate: float
    pg_previous_state: dict[str, object] | None = None

    async with pg_pool.acquire() as conn, conn.transaction():
        await conn.execute(preseed_sql, self._name)
        row = await conn.fetchrow(select_sql, self._name)

        if row is None:
            # Unreachable in the normal path — the preseed above guarantees
            # the row exists before the SELECT. Kept as a defensive fallback
            # (e.g. a concurrent DELETE/reset between preseed and select).
            # First use: no row to fold the server epoch into — take a
            # separate read on the same connection/transaction.
            now_seconds = float(await conn.fetchval("SELECT EXTRACT(EPOCH FROM clock_timestamp())"))
            current_tat = now_seconds
        else:
            existing_kind: str = row["kind"]
            if existing_kind != "gcra":
                raise RuntimeError(
                    f"bucket_name {self._name!r} is already registered with kind != 'gcra'; "
                    f"refusing to corrupt prior state. Rename one of the colliding registrations."
                )
            now_seconds = float(row["now_s"])
            state = jsonb_to_dict(row["state"])
            current_tat = float(state.get("tat", now_seconds))  # type: ignore[index]  # Why: rate_limit_buckets.state is NOT NULL; jsonb_to_dict only returns None for SQL NULL, which cannot occur here; fallback to now_seconds for rows missing "tat" (e.g. from schema migrations or interop writes)

        tat = max(now_seconds, current_tat) + emission_interval_seconds
        pre_acquire_tat = max(now_seconds, current_tat)
        allow_at = tat - delay_tolerance_seconds

        if now_seconds >= allow_at:
            allowed = True
            new_tat = tat
            state_param = jsonb_param({"tat": new_tat})
            returned = await conn.fetchrow(upsert_sql, self._name, state_param)
            if returned is None:
                raise RuntimeError(
                    f"bucket_name {self._name!r} is already registered with kind != 'gcra'; "
                    f"refusing to corrupt prior state. Rename one of the colliding registrations."
                )
            remaining_estimate = float(
                max(
                    0,
                    int(
                        (delay_tolerance_seconds - (new_tat - now_seconds))
                        / emission_interval_seconds
                    ),
                )
            )
            retry_after = timedelta(0)
            pg_previous_state = {
                "pre_acquire_tat": pre_acquire_tat,
                "post_acquire_tat": new_tat,
            }
        else:
            allowed = False
            retry_after_seconds = allow_at - now_seconds
            if retry_after_seconds <= 0:
                retry_after_seconds = 0.001
            retry_after = timedelta(seconds=retry_after_seconds)
            remaining_estimate = 0.0

    result = RateLimitDecision(
        allowed=allowed,
        remaining=remaining_estimate if allowed else 0.0,
        retry_after=retry_after,
        bucket_name=self._name,
        backend="postgres",
        previous_state=pg_previous_state,
    )
    log_decision(result, style=self._style)
    return result
