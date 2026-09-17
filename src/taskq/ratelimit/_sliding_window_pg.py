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

from taskq._advisory import (
    _LOCK_TIMEOUT_SET_SQL,  # pyright: ignore[reportPrivateUsage]  # Why: the one implementation of the set_config statement shared with the enqueue path's bounded locks — a local copy would drift from the machinery it mirrors.
    DEFAULT_ADVISORY_LOCK_CLIENT_BACKSTOP_SLACK_S,
    acquire_advisory_xact_lock_bounded,
)
from taskq.backend._records import jsonb_to_dict
from taskq.exceptions import RateLimitDependencyUnavailable
from taskq.ratelimit._decision_log import log_decision
from taskq.ratelimit._lock_budget import resolve_sliding_window_lock_timeout_ms
from taskq.ratelimit.decision import RateLimitDecision, RateLimitState

if TYPE_CHECKING:
    import asyncpg

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
        raise RateLimitDependencyUnavailable("pg_pool not injected for postgres backend")
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
        raise RateLimitDependencyUnavailable("pg_pool not injected for postgres backend")
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
        raise RateLimitDependencyUnavailable("pg_pool not injected for postgres backend")
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
        raise RateLimitDependencyUnavailable("pg_pool not injected for postgres backend")
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
        raise RateLimitDependencyUnavailable("pg_pool not injected for postgres gcra refund")
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
        raise RateLimitDependencyUnavailable("pg_pool not injected for postgres log refund")
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
#: lock. The lock is held across ONE fused statement (prune + admission
#: insert + count + retry hint in a single round trip, #228), so a
#: holder's critical section is one statement's execution time, and
#: 5 s tolerates a burst of hundreds of queued racers while capping
#: tail latency instead of letting it scale with the racer count, and a
#: black-holed holder (dead TCP, no FIN) blocks its bucket for at most
#: one budget instead of until the server's keepalives reap it. Same
#: default as the enqueue path's DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS. A
#: racer that exhausts the budget gets the limiter's DENIAL outcome —
#: fail closed, never an admission. ``0`` (or less) waits indefinitely,
#: matching the ``lock_timeout`` GUC convention used by migrate.py.
#: The shipped ceiling; an operator retunes it with
#: ``sliding_window_lock_timeout_ms``.
DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS: float = 5000.0

#: The acquire mechanics (try-lock fast path, savepoint + lock_timeout +
#: blocking contended tier, client-side backstop) live in
#: ``taskq._advisory`` — one implementation shared with the enqueue
#: path's locks; this module contributes only the site-specific budget
#: above and the denial-on-exhaustion semantics at the call site.


async def _acquire_pg_log(
    self: "SlidingWindow",
    pg_pool: "asyncpg.Pool | None",
    settings: "WorkerSettings | None",
    request_id: UUID | None,
    *,
    lock_timeout_ms: float | None = None,
) -> RateLimitDecision:
    """Acquire log-style against PG.

    Every window predicate and the inserted ``ts`` are ``clock_timestamp()``
    — the PG server clock owns the shared window state, so nodes with
    divergent Python clocks all get measured against the same window.

    The per-bucket advisory lock is acquired with the two-tier bounded
    acquire (``acquire_advisory_xact_lock_bounded`` from
    ``taskq._advisory``) for the operator's
    ``sliding_window_lock_timeout_ms`` budget, defaulting to
    :data:`DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS`. On budget
    exhaustion the acquire FAILS CLOSED: it returns the limiter's denial
    outcome — ``allowed=False`` with a retry hint, never an exception and
    never an admission — so a racer that could not check the window can
    never over-admit past the limit.
    """
    if pg_pool is None:
        raise RateLimitDependencyUnavailable("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")
    if request_id is None:
        raise RuntimeError("request_id required for log-style PG acquire")
    lock_timeout_ms = resolve_sliding_window_lock_timeout_ms(
        lock_timeout_ms, settings, DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS
    )

    window_ms = int(self._window.total_seconds() * 1000)
    schema = settings.schema_name

    # ONE fused statement for the whole locked critical section (#228):
    # the pre-fused shape spent DELETE + INSERT + COUNT (+ retry SELECT on
    # denial) as four separate round trips under the advisory lock,
    # and lock hold time linear in round trips is exactly the contention tail
    # the two-tier lock machinery exists to bound. The CTEs:
    #
    # * ``pruned``: evicts out-of-window entries (the maintenance the
    #   pre-fused DELETE did; its effect is invisible to the count below
    #   because pruned rows are, by definition, outside the window the
    #   count measures, the two row sets are disjoint under the same
    #   statement snapshot).
    # * ``inserted``: the admission INSERT, guarded by an in-window
    #   count read at the STATEMENT's snapshot. The advisory lock (taken
    #   by a PRIOR statement in this transaction) serializes racers, and
    #   each racer's statement snapshot postdates the previous holder's
    #   commit, the cron-tick lesson (test_round_trip_budgets): a lock
    #   probe folded INTO the work statement would read a snapshot that
    #   predates its own lock grant and re-admit the batch. The lock
    #   stays a separate statement; the WORK is one.
    # * the main SELECT: everything the decision needs, from the same
    #   snapshot: whether the insert landed (a data-modifying CTE's
    #   RETURNING is visible to the parent query), the in-window count
    #   (pre-insert; post-count = pre + inserted, computed below), and
    #   on denial the oldest in-window entry for the retry hint (the
    #   pruned rows were out-of-window, so the oldest in-window entry is
    #   the same one the pre-fused denial's retry SELECT found after the
    #   DELETE).
    fused_sql = (
        f"WITH pruned AS ( "  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; every value is $-bound.
        f'DELETE FROM "{schema}".rate_limit_window_entries '
        f"WHERE bucket_name = $1 "
        f"AND ts < clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond') "
        f"RETURNING 1 "
        f"), inserted AS ( "
        f'INSERT INTO "{schema}".rate_limit_window_entries (bucket_name, ts, request_id) '
        f"SELECT $1, clock_timestamp(), $3::uuid "
        f'WHERE (SELECT count(*) FROM "{schema}".rate_limit_window_entries '
        f"WHERE bucket_name = $1 "
        f"AND ts >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond')) < $4::integer "
        f"RETURNING 1 "
        f") "
        f"SELECT EXISTS(SELECT 1 FROM inserted) AS inserted, "
        f'(SELECT count(*) FROM "{schema}".rate_limit_window_entries '
        f"WHERE bucket_name = $1 "
        f"AND ts >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond')) AS count_in_window, "
        f'(SELECT e.ts FROM "{schema}".rate_limit_window_entries e '
        f"WHERE e.bucket_name = $1 "
        f"AND e.ts >= clock_timestamp() - ($2::bigint * INTERVAL '1 millisecond') "
        f"ORDER BY e.ts ASC LIMIT 1) AS oldest_ts, "
        f"clock_timestamp() AS server_now"
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
    # path's locks — see acquire_advisory_xact_lock_bounded in
    # taskq._advisory for the measured rationale): every racer on this lock holds it across
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
    # Once acquired, the lock is transaction-scoped and the fused
    # statement below is the whole critical section: the window
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

        fused_row = await conn.fetchrow(
            fused_sql,
            self._name,
            window_ms,
            request_id,
            self._limit,
        )
        allowed = fused_row is not None and bool(fused_row["inserted"])

        if allowed:
            # Post-insert in-window count = the pre-insert count the
            # statement read plus this insert (the statement's own
            # snapshot cannot see its CTE's write; the arithmetic is
            # exact either way).
            count_after = int(fused_row["count_in_window"]) + 1 if fused_row else self._limit
            retry_after = timedelta(0)
        else:
            # Denial: the retry hint is the oldest in-window entry's
            # window expiry, the same entry the pre-fused retry SELECT
            # found (the prune removed only out-of-window rows).
            oldest_ts = fused_row["oldest_ts"] if fused_row is not None else None
            server_now = fused_row["server_now"] if fused_row is not None else None
            if oldest_ts is not None and server_now is not None:
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
    *,
    lock_timeout_ms: float | None = None,
) -> RateLimitDecision:
    """Acquire GCRA-style against PG.

    ONE fused ``INSERT … ON CONFLICT DO UPDATE … WHERE … RETURNING``
    (#228): the pre-fused shape spent a preseed, a blocking ``SELECT …
    FOR UPDATE``, and a TAT upsert (BEGIN + set_config + SAVEPOINT
    around them, 8 round trips in bounded mode); the conflict arm now
    advances the TAT server-side under the row lock it takes itself,
    and the ALLOWANCE is the update's WHERE clause, so RETURNING yields
    a row exactly when the acquire was granted (a cold start is always
    granted, emission <= window for limit >= 1). The TAT epoch math
    runs on ``statement_timestamp()`` in the same locked statement, so
    the stored TAT is server-domain by construction and a node with a
    skewed Python clock cannot move the shared admission boundary.

    The bucket row's lock WAIT is bounded by the operator's
    ``sliding_window_lock_timeout_ms`` budget (defaulting to
    :data:`DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS`), mirroring the
    log-style acquire's advisory-lock budget in the same module: with
    ``rate_limit_pg_fallback_enabled`` on, a Redis outage funnels all
    admission through this row lock, so an unbounded wait would let one
    black-holed holder (dead TCP, no FIN) stall its bucket's admission
    until the server's keepalives reap it. On budget exhaustion the
    acquire FAILS CLOSED — the limiter's denial outcome, ``allowed=False``
    with a retry hint of one more budget, never an exception and never
    an admission: a racer that could not read the TAT can never advance
    it. ``lock_timeout_ms <= 0`` waits indefinitely (one autocommit
    statement, no transaction and no GUC), the ``lock_timeout`` GUC
    convention shared with migrate.py and ``taskq._advisory``.
    """
    if pg_pool is None:
        raise RateLimitDependencyUnavailable("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")
    lock_timeout_ms = resolve_sliding_window_lock_timeout_ms(
        lock_timeout_ms, settings, DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS
    )

    window_ms = int(self._window.total_seconds() * 1000)
    window_seconds = window_ms / 1000.0
    emission_interval_seconds = window_seconds / self._limit
    delay_tolerance_seconds = window_seconds
    schema = settings.schema_name

    # ONE fused upsert (#228): the pre-fused shape spent preseed + SELECT
    # FOR UPDATE + upsert (BEGIN + set_config + SAVEPOINT around them):
    # 8 round trips in bounded mode. The conflict arm computes the TAT
    # advance server-side, and the ALLOWANCE is the update's WHERE
    # clause, so RETURNING yields a row exactly when the acquire was
    # granted (or the bucket was cold, the INSERT arm, and a cold start
    # is always allowed: emission <= window for limit >= 1, so the
    # allow_at boundary is at or before now). A denial updates nothing:
    # the pre-fused behavior, preserved exactly (the TAT stands, the
    # row's stamps are untouched), and the retry hint is read by the
    # one follow-up statement below rather than folded into the upsert:
    # a WHERE-gated conflict arm returns NO row on denial, so the hint's
    # inputs (the standing TAT, the server now) must come from a read.
    #
    # The kind guard rides the WHERE too, preserving the loud
    # misconfiguration refusal: a token-bucket row under this name
    # fails the guard exactly as the pre-fused SELECT's kind check
    # raised, and the follow-up read below distinguishes the two
    # no-row causes (kind mismatch -> RuntimeError; otherwise denial).
    #
    # statement_timestamp() (STABLE) is the arithmetic's clock: the
    # WHERE's allowance test and the SET's TAT advance are separate
    # evaluations of the same GREATEST(...) expression, and a stable
    # statement clock keeps them identical, the same doctrine the
    # fused token-bucket acquire documents.
    _now_epoch = "EXTRACT(EPOCH FROM statement_timestamp())"
    _old_tat = f"COALESCE((rate_limit_buckets.state->>'tat')::float8, {_now_epoch})"
    _base_tat = f"GREATEST({_now_epoch}, {_old_tat})"
    fused_sql = (
        f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at) '  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; every value is $-bound.
        f"VALUES ($1, 'gcra', "
        f"jsonb_build_object('tat', {_now_epoch} + $2::float8), "
        f"clock_timestamp()) "
        f"ON CONFLICT (bucket_name) DO UPDATE SET "
        f"state = jsonb_build_object('tat', {_base_tat} + $2::float8), "
        f"updated_at = clock_timestamp() "
        f"WHERE rate_limit_buckets.kind = 'gcra' "
        f"AND {_now_epoch} >= {_base_tat} + $2::float8 - $3::float8 "
        f"RETURNING (state->>'tat')::float8 AS new_tat, "
        f"{_now_epoch}::float8 AS now_s"
    )
    # The denial follow-up: the standing TAT and the server now for the
    # retry hint, plus the kind the WHERE guard may have refused on.
    deny_read_sql = (
        f"SELECT kind, (state->>'tat')::float8 AS tat, "  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name is $1-bound.
        f"EXTRACT(EPOCH FROM clock_timestamp()) AS now_s "
        f'FROM "{schema}".rate_limit_buckets '
        f"WHERE bucket_name = $1"
    )

    allowed: bool
    retry_after: timedelta
    remaining_estimate: float
    pg_previous_state: dict[str, object] | None = None

    async def _fused_acquire(
        conn: "asyncpg.Connection | asyncpg.pool.PoolConnectionProxy[asyncpg.Record]",
    ) -> "asyncpg.Record | None":
        return await conn.fetchrow(fused_sql, self._name, emission_interval_seconds, window_seconds)

    row: asyncpg.Record | None = None

    if lock_timeout_ms > 0:
        # Why a function-level import: this module is imported by
        # taskq.ratelimit, which taskq.testing imports transitively:
        # that boundary must stay importable without the asyncpg driver
        # installed. The acquire only ever runs against a real
        # connection, where asyncpg is guaranteed present.
        from asyncpg.exceptions import LockNotAvailableError

        async with pg_pool.acquire() as conn:
            try:
                async with conn.transaction():
                    await conn.execute(_LOCK_TIMEOUT_SET_SQL, f"{round(lock_timeout_ms)}ms")
                    row = await asyncio.wait_for(
                        _fused_acquire(conn),
                        timeout=lock_timeout_ms / 1000.0
                        + DEFAULT_ADVISORY_LOCK_CLIENT_BACKSTOP_SLACK_S,
                    )
            except (LockNotAvailableError, TimeoutError):
                # Fail closed: the limiter's denial outcome with a retry
                # hint of one more budget: the fused statement is
                # atomic, so the timed-out racer advanced no TAT and
                # admitted nothing. The warning is the operator signal
                # that the bucket (or its holder) is contended or sick
                # rather than merely busy: the same event name the
                # log-style path and the token-bucket path emit for the
                # same condition.
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
                )
                log_decision(result, style=self._style)
                return result
    else:
        # lock_timeout_ms <= 0: the indefinite mode, the GUC
        # convention's opt-out. One autocommit statement; the conflict
        # arm's row lock waits as long as the holder holds.
        async with pg_pool.acquire() as conn:
            row = await _fused_acquire(conn)

    if row is not None:
        # Granted (or cold start, which is always granted): the RETURNING
        # row carries the advanced TAT and the statement clock the
        # arithmetic used. The refund's previous_state pair is derivable
        # exactly: the pre-acquire TAT is the candidate minus one
        # emission interval, the post-acquire TAT is the candidate.
        allowed = True
        new_tat = float(row["new_tat"])
        now_seconds = float(row["now_s"])
        retry_after = timedelta(0)
        remaining_estimate = float(
            max(
                0,
                int(
                    (delay_tolerance_seconds - (new_tat - now_seconds)) / emission_interval_seconds
                ),
            )
        )
        pg_previous_state = {
            "pre_acquire_tat": new_tat - emission_interval_seconds,
            "post_acquire_tat": new_tat,
        }
    else:
        # Denied (or the kind guard refused). One follow-up read carries
        # the retry hint's inputs and discriminates the guard's refusal:
        # the loud misconfiguration error the pre-fused SELECT raised.
        allowed = False
        remaining_estimate = 0.0
        async with pg_pool.acquire() as conn:
            deny_row = await conn.fetchrow(deny_read_sql, self._name)
        if deny_row is not None and deny_row["kind"] != "gcra":
            raise RuntimeError(
                f"bucket_name {self._name!r} is already registered with kind != 'gcra'; "
                f"refusing to corrupt prior state. Rename one of the colliding registrations."
            )
        if deny_row is not None:
            now_seconds = float(deny_row["now_s"])
            current_tat = float(deny_row["tat"]) if deny_row["tat"] is not None else now_seconds
            allow_at = (
                max(now_seconds, current_tat) + emission_interval_seconds - delay_tolerance_seconds
            )
            retry_after_seconds = allow_at - now_seconds
            if retry_after_seconds <= 0:
                retry_after_seconds = 0.001
            retry_after = timedelta(seconds=retry_after_seconds)
        else:
            # The row vanished between the fused statement and this read
            # (a concurrent reset): the pre-fused preseed made this
            # unreachable; keep a bounded defensive hint rather than an
            # admission the upsert never granted.
            retry_after = timedelta(milliseconds=1)

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
