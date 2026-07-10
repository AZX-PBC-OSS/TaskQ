"""PostgreSQL fallback implementations for sliding-window rate limiter.

All PG-path methods (acquire, peek, reset, refund for both log and GCRA
styles) live here as module-level functions taking ``self: SlidingWindow``
as the first parameter, following the testing-module pattern.
"""

from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import structlog

from taskq.backend._records import jsonb_param, jsonb_to_dict
from taskq.ratelimit._decision_log import log_decision
from taskq.ratelimit.decision import RateLimitDecision, RateLimitState

if TYPE_CHECKING:
    import asyncpg

    from taskq.backend.clock import Clock
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
    now_ms: int,
    pg_pool: "asyncpg.Pool | None",
    clock: "Clock",
    settings: "WorkerSettings | None",
) -> RateLimitState:
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")

    now_dt = clock.now()
    window_ms = int(self._window.total_seconds() * 1000)
    schema = settings.schema_name

    count_sql = (
        f'SELECT count(*) FROM "{schema}".rate_limit_window_entries '  # noqa: S608
        f"WHERE bucket_name = $1 "
        f"AND ts >= $2::timestamptz - ($3::bigint * INTERVAL '1 millisecond')"
    )
    oldest_sql = (
        f'SELECT ts FROM "{schema}".rate_limit_window_entries '  # noqa: S608
        f"WHERE bucket_name = $1 "
        f"AND ts >= $2::timestamptz - ($3::bigint * INTERVAL '1 millisecond') "
        f"ORDER BY ts ASC LIMIT 1"
    )

    async with pg_pool.acquire() as conn:
        count_row = await conn.fetchrow(count_sql, self._name, now_dt, window_ms)
        count = int(count_row["count"]) if count_row else 0

        is_exhausted = count >= self._limit
        retry_after: timedelta | None = None

        if is_exhausted and count > 0:
            oldest_row = await conn.fetchrow(oldest_sql, self._name, now_dt, window_ms)
            if oldest_row is not None:
                oldest_ts = oldest_row["ts"]
                retry_after = (oldest_ts + timedelta(milliseconds=window_ms)) - now_dt
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
    now_ms: int,
    pg_pool: "asyncpg.Pool | None",
    clock: "Clock",
    settings: "WorkerSettings | None",
) -> RateLimitState:
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")

    now_seconds = now_ms / 1000.0
    window_ms = int(self._window.total_seconds() * 1000)
    window_seconds = window_ms / 1000.0
    emission_interval_seconds = window_seconds / self._limit
    delay_tolerance_seconds = window_seconds
    schema = settings.schema_name

    select_sql = (
        f'SELECT state FROM "{schema}".rate_limit_buckets '  # noqa: S608
        f"WHERE bucket_name = $1 AND kind = 'gcra'"
    )

    async with pg_pool.acquire() as conn:
        row = await conn.fetchrow(select_sql, self._name)

    if row is None:
        current_tat = now_seconds
    else:
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
        f"SET state = jsonb_set(state, '{{tat}}', to_jsonb($2::float)), updated_at = now() "
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


async def _acquire_pg_log(
    self: "SlidingWindow",
    pg_pool: "asyncpg.Pool | None",
    clock: "Clock",
    settings: "WorkerSettings | None",
    request_id: UUID | None,
) -> RateLimitDecision:
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")
    if request_id is None:
        raise RuntimeError("request_id required for log-style PG acquire")

    now_dt = clock.now()
    window_ms = int(self._window.total_seconds() * 1000)
    schema = settings.schema_name

    delete_sql = (
        f'DELETE FROM "{schema}".rate_limit_window_entries '  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name is $1-bound
        f"WHERE bucket_name = $1 "
        f"AND ts < $2::timestamptz - ($3::bigint * INTERVAL '1 millisecond')"
    )
    insert_sql = (
        f'INSERT INTO "{schema}".rate_limit_window_entries (bucket_name, ts, request_id) '  # noqa: S608  # Why: schema_name pre-validated; values are $1/$2/$4/$5-bound
        f"SELECT $1, $2::timestamptz, $4::uuid "
        f"WHERE ("
        f'SELECT count(*) FROM "{schema}".rate_limit_window_entries '
        f"WHERE bucket_name = $1 "
        f"AND ts >= $2::timestamptz - ($3::bigint * INTERVAL '1 millisecond')"
        f") < $5::integer "
        f"RETURNING 1"
    )
    retry_select_sql = (
        f'SELECT ts FROM "{schema}".rate_limit_window_entries '  # noqa: S608  # Why: schema_name pre-validated; bucket_name is $1-bound
        f"WHERE bucket_name = $1 "
        f"AND ts >= $2::timestamptz - ($3::bigint * INTERVAL '1 millisecond') "
        f"ORDER BY ts ASC LIMIT 1"
    )
    count_sql = (
        f'SELECT count(*) FROM "{schema}".rate_limit_window_entries '  # noqa: S608  # Why: schema_name pre-validated; bucket_name is $1-bound
        f"WHERE bucket_name = $1 "
        f"AND ts >= $2::timestamptz - ($3::bigint * INTERVAL '1 millisecond')"
    )

    allowed: bool
    retry_after: timedelta
    count_after: int

    async with pg_pool.acquire() as conn, conn.transaction():
        await conn.execute(delete_sql, self._name, now_dt, window_ms)

        inserted = await conn.fetchrow(
            insert_sql,
            self._name,
            now_dt,
            window_ms,
            request_id,
            self._limit,
        )

        if inserted is not None:
            allowed = True
            count_row = await conn.fetchrow(count_sql, self._name, now_dt, window_ms)
            count_after = int(count_row["count"]) if count_row is not None else self._limit
            retry_after = timedelta(0)
        else:
            allowed = False
            oldest_row = await conn.fetchrow(retry_select_sql, self._name, now_dt, window_ms)
            if oldest_row is not None:
                oldest_ts = oldest_row["ts"]
                retry_after = (oldest_ts + timedelta(milliseconds=window_ms)) - now_dt
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
    clock: "Clock",
    settings: "WorkerSettings | None",
) -> RateLimitDecision:
    if pg_pool is None:
        raise RuntimeError("pg_pool not injected for postgres backend")
    if settings is None:
        raise RuntimeError("settings not injected for postgres backend")

    now_dt = clock.now()
    now_seconds = now_dt.timestamp()
    window_ms = int(self._window.total_seconds() * 1000)
    window_seconds = window_ms / 1000.0
    emission_interval_seconds = window_seconds / self._limit
    delay_tolerance_seconds = window_seconds
    schema = settings.schema_name

    select_sql = (
        f'SELECT kind, state FROM "{schema}".rate_limit_buckets '  # noqa: S608  # Why: schema_name is pre-validated against _IDENT_RE at settings load time; bucket_name is $1-bound
        f"WHERE bucket_name = $1 FOR UPDATE"
    )
    upsert_sql = (
        f'INSERT INTO "{schema}".rate_limit_buckets (bucket_name, kind, state, updated_at) '  # noqa: S608  # Why: schema_name pre-validated; values are $1/$2-bound
        f"VALUES ($1, 'gcra', $2::jsonb, now()) "
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
        row = await conn.fetchrow(select_sql, self._name)

        if row is None:
            current_tat = now_seconds
        else:
            existing_kind: str = row["kind"]
            if existing_kind != "gcra":
                raise RuntimeError(
                    f"bucket_name {self._name!r} is already registered with kind != 'gcra'; "
                    f"refusing to corrupt prior state. Rename one of the colliding registrations."
                )
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
