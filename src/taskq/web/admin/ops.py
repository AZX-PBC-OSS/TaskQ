"""Schedules, rate-limits, and reservations admin pages."""

import asyncio
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote_plus
from uuid import UUID

import asyncpg
import structlog
from asyncpg.exceptions import UndefinedTableError
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from jinja2 import Environment

from taskq._ids import new_uuid
from taskq.backend._protocol import (
    Backend,
    EnqueueArgs,
    JobId,
    parse_retry_kind,
)
from taskq.backend.clock import SystemClock
from taskq.cron import (
    compute_next_fire_after,
    resolve_payload,
)
from taskq.settings import TaskQSettings
from taskq.web.admin._factory import (
    get_backend,
    get_base_path,
    get_csrf_token,
    get_pg_pool,
    get_realtime_ctx,
    get_redis_client,
    get_schema,
    get_settings,
    get_templates,
    validate_csrf,
)

logger = structlog.get_logger("taskq.web.admin.ops")

_last_schedule_run: dict[UUID, float] = {}
_SCHEDULE_RUN_COOLDOWN_SECONDS = 10.0

_SCHEDULES_SQL = (
    "SELECT id, actor, cron_expr, timezone, enabled, next_fire_at, "
    "last_fired_at, last_fire_error, consecutive_failures, metadata "
    'FROM "{schema}".cron_schedules ORDER BY next_fire_at'
)

_SCHEDULE_ENABLE_SQL = (
    'UPDATE "{schema}".cron_schedules '
    "SET enabled = true, consecutive_failures = 0, last_fire_error = NULL "
    "WHERE id = $1"
)

_SCHEDULE_DISABLE_SQL = 'UPDATE "{schema}".cron_schedules SET enabled = false WHERE id = $1'

_SCHEDULE_FETCH_FOR_SKIP_SQL = (
    'SELECT cron_expr, timezone, next_fire_at FROM "{schema}".cron_schedules WHERE id = $1'
)

_SCHEDULE_SKIP_SQL = 'UPDATE "{schema}".cron_schedules SET next_fire_at = $2 WHERE id = $1'

_SCHEDULE_FETCH_FOR_RUN_SQL = (
    'SELECT actor, payload_factory, enabled, metadata FROM "{schema}".cron_schedules WHERE id = $1'
)

_ACTOR_CONFIG_SQL = (
    'SELECT queue, max_attempts, retry_kind FROM "{schema}".actor_config WHERE actor = $1'
)

_RATE_LIMITS_SQL = (
    "SELECT bucket_name, kind, state, updated_at "
    'FROM "{schema}".rate_limit_buckets ORDER BY bucket_name'
)

_RESERVATIONS_SQL = (
    "SELECT bucket_name, "
    "count(*) FILTER (WHERE job_id IS NOT NULL) AS held_count, "
    "count(*) FILTER (WHERE job_id IS NULL) AS free_count, "
    "count(*) AS total_slots "
    'FROM "{schema}".reservation_slots '
    "GROUP BY bucket_name ORDER BY bucket_name"
)

_HELD_SLOTS_SQL = (
    "SELECT bucket_name, slot_index, job_id, held_by_worker_id, lease_expires_at "
    'FROM "{schema}".reservation_slots '
    "WHERE job_id IS NOT NULL "
    "ORDER BY bucket_name, slot_index"
)


async def _fetch_redis_rl_state(
    redis_client: Any,
    schema: str,
    names: Sequence[tuple[str, str]],
) -> dict[str, dict[str, str]] | None:
    """Fetch live Redis state for registered rate-limit primitives.

    *names* is a list of ``(bucket_name, kind)`` tuples where kind is
    ``"token_bucket"``, ``"sliding_window_log"``, or ``"sliding_window_gcra"``.
    Redis keys follow the conventions:

    * Token bucket:     ``taskq:{schema}:rl:tb:{name}``   (HGETALL)
    * Sliding window log: ``taskq:{schema}:sw:{name}``    (ZCARD for count)
    * Sliding window GCRA: ``taskq:{schema}:sw_gcra:{name}`` (GET for TAT)

    Returns ``None`` on any Redis failure so the caller can degrade gracefully.
    """
    if redis_client is None:
        return None
    try:
        result: dict[str, dict[str, str]] = {}
        for name, kind in names:
            if kind == "token_bucket":
                redis_key = f"taskq:{schema}:rl:tb:{{{name}}}"
                raw = await redis_client.hgetall(redis_key)
                if raw:
                    decoded: dict[str, str] = {}
                    for k, v in raw.items() if isinstance(raw, dict) else raw:  # pyright: ignore[reportUnknownVariableType]  # Why: redis-py hgetall return type is untyped in the stub; isinstance narrowing at runtime ensures correct types.
                        kk = k.decode() if isinstance(k, bytes) else str(k)  # pyright: ignore[reportUnknownArgumentType]  # Why: redis-py key type is untyped in the stub; isinstance narrowing at runtime ensures correct str conversion.
                        vv = v.decode() if isinstance(v, bytes) else str(v)  # pyright: ignore[reportUnknownArgumentType]  # Why: redis-py value type is untyped in the stub; isinstance narrowing at runtime ensures correct str conversion.
                        decoded[kk] = vv
                    if decoded:
                        result[name] = decoded
            elif kind == "sliding_window_gcra":
                redis_key = f"taskq:{schema}:sw_gcra:{{{name}}}"
                tat_raw = await redis_client.get(redis_key)
                if tat_raw is not None:
                    tat_str = tat_raw.decode() if isinstance(tat_raw, bytes) else str(tat_raw)
                    result[name] = {"tat": tat_str}
            elif kind == "sliding_window_log":
                redis_key = f"taskq:{schema}:sw:{{{name}}}"
                count = await redis_client.zcard(redis_key)
                if count is not None and count > 0:
                    result[name] = {"count": str(count)}
        return result
    except Exception:
        logger.debug("redis-rl-fetch-failed", exc_info=True)
        return None


def register(router: APIRouter) -> None:
    """Attach schedules, rate-limits, and reservations routes to *router*."""

    @router.get("/schedules", response_class=HTMLResponse)
    async def schedules_page(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        error: str | None = None,
        csrf_token: str = Depends(get_csrf_token),
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
    ) -> HTMLResponse:
        schedules_sql = _SCHEDULES_SQL.format(schema=schema)

        cron_installed = True
        rows: list[asyncpg.Record] = []
        async with pool.acquire() as conn:
            try:
                rows = await conn.fetch(schedules_sql)
            except UndefinedTableError:
                logger.debug("cron-schedules-table-missing")
                cron_installed = False

        schedules = [dict(r) for r in rows]
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("schedules.html").render(
            schedules=schedules,
            cron_installed=cron_installed,
            notice_text="cron scheduling not installed — run taskq migrate up to enable",
            error=error,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
            csrf_token=csrf_token,
        )
        return HTMLResponse(content=html)

    @router.post("/schedules/{schedule_id}/enable")
    async def schedule_enable(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        schedule_id: UUID,
        _csrf: None = Depends(validate_csrf),
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        base_path: str = Depends(get_base_path),
    ) -> RedirectResponse:
        enable_sql = _SCHEDULE_ENABLE_SQL.format(schema=schema)

        async with pool.acquire() as conn:
            try:
                result = await conn.execute(enable_sql, schedule_id)
            except UndefinedTableError:
                return RedirectResponse(
                    url=f"{base_path}/schedules?error=cron+scheduling+not+installed",
                    status_code=303,
                )
            if result == "UPDATE 0":
                raise HTTPException(status_code=404, detail="Schedule not found")

        return RedirectResponse(url=f"{base_path}/schedules", status_code=303)

    @router.post("/schedules/{schedule_id}/disable")
    async def schedule_disable(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        schedule_id: UUID,
        _csrf: None = Depends(validate_csrf),
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        base_path: str = Depends(get_base_path),
    ) -> RedirectResponse:
        disable_sql = _SCHEDULE_DISABLE_SQL.format(schema=schema)

        async with pool.acquire() as conn:
            try:
                result = await conn.execute(disable_sql, schedule_id)
            except UndefinedTableError:
                return RedirectResponse(
                    url=f"{base_path}/schedules?error=cron+scheduling+not+installed",
                    status_code=303,
                )
            if result == "UPDATE 0":
                raise HTTPException(status_code=404, detail="Schedule not found")

        return RedirectResponse(url=f"{base_path}/schedules", status_code=303)

    @router.post("/schedules/{schedule_id}/skip")
    async def schedule_skip(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        schedule_id: UUID,
        _csrf: None = Depends(validate_csrf),
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        base_path: str = Depends(get_base_path),
    ) -> RedirectResponse:
        fetch_sql = _SCHEDULE_FETCH_FOR_SKIP_SQL.format(schema=schema)
        skip_sql = _SCHEDULE_SKIP_SQL.format(schema=schema)

        async with pool.acquire() as conn:
            try:
                row = await conn.fetchrow(fetch_sql, schedule_id)
            except UndefinedTableError:
                return RedirectResponse(
                    url=f"{base_path}/schedules?error=cron+scheduling+not+installed",
                    status_code=303,
                )
            if row is None:
                raise HTTPException(status_code=404, detail="Schedule not found")

            cron_expr: str = row["cron_expr"]
            tz_name: str = row["timezone"]
            current_next: datetime = row["next_fire_at"]

            new_next = compute_next_fire_after(cron_expr, tz_name, current_next)[0]
            now = datetime.now(UTC)
            for _ in range(1000):
                if new_next > now:
                    break
                new_next = compute_next_fire_after(cron_expr, tz_name, new_next)[0]
            else:
                raise HTTPException(
                    status_code=400, detail="cron expression produces no future fire time"
                )

            await conn.execute(skip_sql, schedule_id, new_next)

        return RedirectResponse(url=f"{base_path}/schedules", status_code=303)

    @router.post("/schedules/{schedule_id}/run")
    async def schedule_run_now(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        schedule_id: UUID,
        _csrf: None = Depends(validate_csrf),
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        base_path: str = Depends(get_base_path),
        backend: Backend | None = Depends(get_backend),
        settings: TaskQSettings = Depends(get_settings),
    ) -> RedirectResponse:
        if not settings.admin_actions_enabled:
            raise HTTPException(status_code=403, detail="Admin actions are disabled")

        if backend is None:
            raise HTTPException(
                status_code=503, detail="Backend not configured for admin operations"
            )

        now_ts = asyncio.get_running_loop().time()
        last_run = _last_schedule_run.get(schedule_id)
        if last_run is not None and (now_ts - last_run) < _SCHEDULE_RUN_COOLDOWN_SECONDS:
            return RedirectResponse(
                url=f"{base_path}/schedules?error=schedule+run+on+cooldown",
                status_code=303,
            )
        _last_schedule_run[schedule_id] = now_ts

        fetch_sql = _SCHEDULE_FETCH_FOR_RUN_SQL.format(schema=schema)
        actor_config_sql = _ACTOR_CONFIG_SQL.format(schema=schema)

        async with pool.acquire() as conn:
            try:
                row = await conn.fetchrow(fetch_sql, schedule_id)
            except UndefinedTableError:
                return RedirectResponse(
                    url=f"{base_path}/schedules?error=cron+scheduling+not+installed",
                    status_code=303,
                )
            if row is None:
                raise HTTPException(status_code=404, detail="Schedule not found")

            enabled: bool = row["enabled"]
            if not enabled:
                return RedirectResponse(
                    url=f"{base_path}/schedules?error=schedule+is+disabled",
                    status_code=303,
                )

            actor: str = row["actor"]
            payload_factory: str | None = row["payload_factory"]
            raw_metadata: object = row["metadata"]

            payload: dict[str, object]
            try:
                payload = await resolve_payload(payload_factory, raw_metadata)
            except TypeError:
                return RedirectResponse(
                    url=f"{base_path}/schedules?error=factory+returned+unexpected+type",
                    status_code=303,
                )
            except Exception:
                logger.warning("schedule-run-payload-error", exc_info=True)
                return RedirectResponse(
                    url=f"{base_path}/schedules?error=payload+factory+error",
                    status_code=303,
                )

            ac_row = await conn.fetchrow(actor_config_sql, actor)
            if ac_row is None:
                return RedirectResponse(
                    url=f"{base_path}/schedules?error=actor+{quote_plus(actor)}+not+configured",
                    status_code=303,
                )

            args = EnqueueArgs(
                id=JobId(new_uuid()),
                actor=actor,
                queue=ac_row["queue"],
                payload=payload,
                max_attempts=ac_row["max_attempts"],
                retry_kind=parse_retry_kind(ac_row["retry_kind"]),
                scheduled_at=SystemClock().now(),
            )

        await backend.enqueue(args)

        return RedirectResponse(url=f"{base_path}/schedules", status_code=303)

    @router.post("/jobs/{job_id}/retry")
    async def job_retry(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        job_id: UUID,
        _csrf: None = Depends(validate_csrf),
        base_path: str = Depends(get_base_path),
        backend: Backend | None = Depends(get_backend),
        settings: TaskQSettings = Depends(get_settings),
    ) -> RedirectResponse:
        if not settings.admin_actions_enabled:
            raise HTTPException(status_code=403, detail="Admin actions are disabled")

        if backend is None:
            raise HTTPException(
                status_code=503, detail="Backend not configured for admin operations"
            )

        _retryable_statuses: frozenset[str] = frozenset({"failed", "crashed", "cancelled"})

        job = await backend.get(JobId(job_id))
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status not in _retryable_statuses:
            raise HTTPException(status_code=409, detail="Job is not in a retryable state")

        await backend.retry_job(JobId(job_id))

        return RedirectResponse(url=f"{base_path}/jobs/{job_id}", status_code=303)

    @router.get("/rate-limits", response_class=HTMLResponse)
    async def rate_limits_page(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        redis_client: Any | None = Depends(get_redis_client),
        settings: Any = Depends(get_settings),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        csrf_token: str = Depends(get_csrf_token),
    ) -> HTMLResponse:
        from taskq.ratelimit.registry import registry as rl_registry
        from taskq.ratelimit.token_bucket import TokenBucket
        from taskq.worker.deps import WorkerSettings

        allow_reset = getattr(settings, "admin_ui_allow_rate_limit_reset", False)

        configured: list[dict[str, object]] = []
        redis_names: list[tuple[str, str]] = []

        for name, prim in sorted(rl_registry.rate_limits.items()):
            if isinstance(prim, TokenBucket):
                kind = "token_bucket"
                config_summary = f"capacity={prim.capacity}, refill={prim.refill_per_second}/s"
            elif hasattr(prim, "style") and hasattr(prim, "limit") and hasattr(prim, "window"):
                # Duck-type: SlidingWindow and any future rate-limit primitive
                # that exposes style/limit/window attributes.
                kind = f"sliding_window_{prim.style}"
                config_summary = f"limit={prim.limit}, window={prim.window}, style={prim.style}"
            else:
                kind = "unknown"
                config_summary = ""
            backend = prim.backend
            configured.append(
                {
                    "bucket_name": name,
                    "kind": kind,
                    "backend": backend,
                    "config_summary": config_summary,
                }
            )
            if backend in ("redis", "postgres"):
                redis_names.append((name, kind))

        rate_limits_sql = _RATE_LIMITS_SQL.format(schema=schema)

        ratelimit_installed = True
        rows: list[asyncpg.Record] = []
        async with pool.acquire() as conn:
            try:
                rows = await conn.fetch(rate_limits_sql)
            except UndefinedTableError:
                logger.debug("rate-limit-buckets-table-missing")
                ratelimit_installed = False

        pg_state: dict[str, dict[str, object]] = {}
        if ratelimit_installed:
            for r in rows:
                d = dict(r)
                pg_state[str(d["bucket_name"])] = d

        # Attempt live peek for non-memory backends if dependencies are available.
        live_states: dict[str, object] = {}
        try:
            rl_settings = WorkerSettings.load_from_dict(
                {
                    "pg_dsn": str(settings.pg_dsn),
                    "schema_name": schema,
                }
            )
            clock = SystemClock()
            live_states_raw = await rl_registry.peek_all(
                redis_client=redis_client,
                pg_pool=pool,
                clock=clock,
                settings=rl_settings,
            )
            for name, state in live_states_raw.items():
                d: dict[str, object] = {
                    "is_exhausted": state.is_exhausted,
                    "tokens_remaining": state.tokens_remaining,
                    "remaining": state.remaining,
                }
                if state.retry_after is not None:
                    d["retry_after_seconds"] = state.retry_after.total_seconds()
                if state.capacity is not None:
                    d["capacity"] = state.capacity
                if state.limit is not None:
                    d["limit"] = state.limit
                if state.window is not None:
                    d["window_seconds"] = state.window.total_seconds()
                if state.style is not None:
                    d["style"] = state.style
                if state.refill_per_second is not None:
                    d["refill_per_second"] = state.refill_per_second
                live_states[name] = d
        except Exception:
            logger.debug("ratelimit-peek-all-failed", exc_info=True)

        redis_available = False
        redis_configured = redis_client is not None
        redis_state: dict[str, dict[str, str]] | None = None

        if redis_configured:
            redis_available = True
            redis_state = await _fetch_redis_rl_state(redis_client, schema, redis_names)
            if redis_state is None:
                redis_available = False

        buckets: list[dict[str, object]] = []
        for entry in configured:
            name = str(entry["bucket_name"])
            merged: dict[str, object] = dict(entry)
            if name in pg_state:
                merged["pg_state"] = pg_state[name].get("state", "")
                merged["updated_at"] = pg_state[name].get("updated_at", "")
            if name in live_states:
                merged["live_state"] = live_states[name]
            buckets.append(merged)

        for name, pg_row in pg_state.items():
            if not any(str(b["bucket_name"]) == name for b in buckets):
                buckets.append(
                    {
                        "bucket_name": name,
                        "kind": pg_row.get("kind", ""),
                        "backend": "postgres",
                        "config_summary": "",
                        "pg_state": pg_row.get("state", ""),
                        "updated_at": pg_row.get("updated_at", ""),
                    }
                )

        has_memory_buckets = any(str(b["backend"]) == "memory" for b in buckets)
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("rate_limits.html").render(
            allow_reset=allow_reset,
            buckets=buckets,
            csrf_token=csrf_token,
            ratelimit_installed=ratelimit_installed,
            notice_text="rate limiting not installed — run taskq migrate up to enable",
            live_states=live_states,
            redis_state=redis_state,
            redis_available=redis_available,
            redis_configured=redis_configured,
            has_memory_buckets=has_memory_buckets,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)

    @router.post("/rate-limits/{bucket_name}/reset")
    async def rate_limit_reset(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        bucket_name: str,
        _csrf: None = Depends(validate_csrf),
        pool: asyncpg.Pool = Depends(get_pg_pool),
        redis_client: Any | None = Depends(get_redis_client),
        schema: str = Depends(get_schema),
        settings: Any = Depends(get_settings),
        base_path: str = Depends(get_base_path),
    ) -> RedirectResponse:
        from taskq.ratelimit.registry import registry as rl_registry
        from taskq.worker.deps import WorkerSettings

        allow_reset = getattr(settings, "admin_ui_allow_rate_limit_reset", False)
        if not allow_reset:
            raise HTTPException(status_code=403, detail="Rate limit reset is disabled")

        rl_settings = WorkerSettings.load_from_dict(
            {
                "pg_dsn": str(settings.pg_dsn),
                "schema_name": schema,
            }
        )

        await rl_registry.reset(
            bucket_name,
            redis_client=redis_client,
            pg_pool=pool,
            clock=SystemClock(),
            settings=rl_settings,
        )

        return RedirectResponse(url=f"{base_path}/rate-limits", status_code=303)

    @router.get("/reservations", response_class=HTMLResponse)
    async def reservations_page(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        pool: asyncpg.Pool = Depends(get_pg_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
    ) -> HTMLResponse:
        from taskq.ratelimit.registry import registry as rl_registry
        from taskq.ratelimit.reservation import sync_slots

        configured_reservations: list[dict[str, object]] = []
        reservation_primitives = list(rl_registry.reservations.values())

        for name, prim in sorted(rl_registry.reservations.items()):
            configured_reservations.append(
                {
                    "bucket_name": name,
                    "configured_slots": prim.slots,
                    "lease": str(prim.lease),
                }
            )

        reservations_sql = _RESERVATIONS_SQL.format(schema=schema)
        held_slots_sql = _HELD_SLOTS_SQL.format(schema=schema)

        reservations_installed = True
        rows: list[asyncpg.Record] = []
        held_slot_rows: list[asyncpg.Record] = []
        async with pool.acquire() as conn:
            try:
                rows = await conn.fetch(reservations_sql)
                held_slot_rows = await conn.fetch(held_slots_sql)
            except UndefinedTableError:
                logger.debug("reservation-slots-table-missing")
                reservations_installed = False

        if reservations_installed and reservation_primitives:
            try:
                await sync_slots(reservation_primitives, pool, schema=schema)
                async with pool.acquire() as conn:
                    rows = await conn.fetch(reservations_sql)
                    held_slot_rows = await conn.fetch(held_slots_sql)
            except Exception:
                logger.debug("reservation-sync-failed", exc_info=True)

        pg_state: dict[str, dict[str, object]] = {}
        for r in rows:
            d = dict(r)
            pg_state[str(d["bucket_name"])] = d

        reservations: list[dict[str, object]] = []
        for entry in configured_reservations:
            name = str(entry["bucket_name"])
            merged: dict[str, object] = dict(entry)
            if name in pg_state:
                merged["held_count"] = pg_state[name].get("held_count", 0)
                merged["free_count"] = pg_state[name].get("free_count", 0)
                merged["total_slots"] = pg_state[name].get("total_slots", 0)
            else:
                merged["held_count"] = 0
                merged["free_count"] = entry["configured_slots"]
                merged["total_slots"] = entry["configured_slots"]
            reservations.append(merged)

        for name, pg_row in pg_state.items():
            if not any(str(r["bucket_name"]) == name for r in reservations):
                reservations.append(
                    {
                        "bucket_name": name,
                        "configured_slots": pg_row.get("total_slots", 0),
                        "lease": "—",
                        "held_count": pg_row.get("held_count", 0),
                        "free_count": pg_row.get("free_count", 0),
                        "total_slots": pg_row.get("total_slots", 0),
                    }
                )

        realtime_mode, mode_label = realtime_ctx
        held_slots: list[dict[str, object]] = [dict(r) for r in held_slot_rows]
        html = tmpl.get_template("reservations.html").render(
            reservations=reservations,
            reservations_installed=reservations_installed,
            notice_text="reservations not installed — run taskq migrate up to enable",
            held_slots=held_slots,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)
