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
from taskq.backend._records import jsonb_param, jsonb_to_dict
from taskq.exceptions import RateLimitDependencyUnavailable
from taskq.ratelimit._decision_log import log_decision
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
    ``taskq._advisory``; default
    :data:`DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS`). On budget
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
    if lock_timeout_ms is None:
        lock_timeout_ms = settings.sliding_window_lock_timeout_ms

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
    *,
    lock_timeout_ms: float | None = None,
) -> RateLimitDecision:
    """Acquire GCRA-style against PG.

    The TAT epoch math runs on ``EXTRACT(EPOCH FROM clock_timestamp())``
    read inside the same locked transaction, so the stored TAT is
    server-domain by construction and a node with a skewed Python clock
    cannot move the shared admission boundary.

    The bucket row's FOR UPDATE WAIT is bounded (default
    :data:`DEFAULT_SLIDING_WINDOW_LOCK_TIMEOUT_MS`), mirroring the
    log-style acquire's advisory-lock budget in the same module: with
    ``rate_limit_pg_fallback_enabled`` on, a Redis outage funnels all
    admission through this row lock, so an unbounded wait would let one
    black-holed holder (dead TCP, no FIN) stall its bucket's admission
    until the server's keepalives reap it. On budget exhaustion the
    acquire FAILS CLOSED — the limiter's denial outcome, ``allowed=False``
    with a retry hint of one more budget, never an exception and never
    an admission: a racer that could not read the TAT can never advance
    it. ``lock_timeout_ms <= 0`` waits indefinitely, the ``lock_timeout``
    GUC convention shared with migrate.py and ``taskq._advisory``.
    """
    if pg_pool is None:
        raise RateLimitDependencyUnavailable("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")
    if lock_timeout_ms is None:
        lock_timeout_ms = settings.sliding_window_lock_timeout_ms

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

        async def _preseed_and_read() -> "asyncpg.Record | None":
            # Cold-start guard mirroring the token-bucket PG path: SELECT
            # ... FOR UPDATE cannot lock a row that does not exist yet, so
            # two concurrent first acquires would each read `row is None`,
            # each admit, and race last-writer-wins on the TAT. Pre-seed a
            # row stamped with the server-clock TAT (idempotent — DO
            # NOTHING on conflict) so first use also serialises on the row
            # lock below.
            await conn.execute(preseed_sql, self._name)
            return await conn.fetchrow(select_sql, self._name)

        row: asyncpg.Record | None = None

        if lock_timeout_ms > 0:
            # Bounded row-lock wait, mechanics mirrored from
            # taskq._advisory's contended tier. set_config(..., true) is
            # SET LOCAL semantics, so the bound covers every lock wait
            # this transaction can take — the preseed's conflict check,
            # the SELECT FOR UPDATE, the upsert's speculative insert — and
            # dies with the transaction's own commit; no save/restore
            # cycle is needed (unlike the enqueue helper, whose caller
            # keeps using the transaction afterwards). The savepoint keeps
            # the transaction committable after a 55P03 (a raw statement
            # error would leave it aborted); the client-side backstop
            # bounds the network black hole the server-side timeout
            # cannot see.
            from asyncpg.exceptions import LockNotAvailableError

            await conn.execute(_LOCK_TIMEOUT_SET_SQL, f"{round(lock_timeout_ms)}ms")

            async def _locked_state_read() -> None:
                nonlocal row
                async with conn.transaction():
                    row = await _preseed_and_read()

            try:
                await asyncio.wait_for(
                    _locked_state_read(),
                    timeout=lock_timeout_ms / 1000.0
                    + DEFAULT_ADVISORY_LOCK_CLIENT_BACKSTOP_SLACK_S,
                )
            except (LockNotAvailableError, TimeoutError):
                # Fail closed: the limiter's denial outcome with a retry
                # hint of one more budget — the timed-out racer wrote
                # nothing (the savepoint rolled the preseed back; the
                # upsert never ran), so the TAT was never advanced. The
                # warning is the operator signal that the bucket (or its
                # holder) is contended or sick rather than merely busy —
                # the same event name the log-style path emits for the
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
            # lock_timeout_ms <= 0: the indefinite mode — the GUC
            # convention's opt-out, and the pre-bound behavior.
            row = await _preseed_and_read()

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
