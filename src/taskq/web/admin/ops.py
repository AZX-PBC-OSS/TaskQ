"""Schedules, rate-limits, and reservations admin pages."""

import asyncio
from collections.abc import Sequence
from datetime import datetime
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
    DST_STRATEGIES,
    DstStrategy,
    compute_next_fire_after,
    resolve_payload,
)
from taskq.exceptions import BackpressureError
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import TaskQSettings
from taskq.web._pool import BoundedPool
from taskq.web.admin._constants import (
    _TERMINAL_STATUSES,  # pyright: ignore[reportPrivateUsage]  # Why: shared constants published by the admin constants module; private prefix scopes them within the admin package.
)
from taskq.web.admin._factory import (
    get_admin_pool,
    get_backend,
    get_base_path,
    get_csrf_token,
    get_realtime_ctx,
    get_redis_client,
    get_rl_registry,
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
    "SET enabled = true, consecutive_failures = 0, last_fire_error = NULL, "
    "disabled_by = NULL "
    "WHERE id = $1"
)

# A disable from the admin UI is operator intent: the ownership marker is
# what keeps the worker's startup registration pass from ever reverting it.
_SCHEDULE_DISABLE_SQL = (
    "UPDATE \"{schema}\".cron_schedules SET enabled = false, disabled_by = 'operator' WHERE id = $1"
)

_SCHEDULE_FETCH_FOR_SKIP_SQL = (
    "SELECT cron_expr, timezone, dst_strategy, next_fire_at, clock_timestamp() AS db_now "
    'FROM "{schema}".cron_schedules WHERE id = $1'
)

_SCHEDULE_SKIP_SQL = 'UPDATE "{schema}".cron_schedules SET next_fire_at = $2 WHERE id = $1'

_SCHEDULE_FETCH_FOR_RUN_SQL = (
    'SELECT actor, payload_factory, enabled, metadata FROM "{schema}".cron_schedules WHERE id = $1'
)

_ACTOR_CONFIG_SQL = (
    "SELECT queue, max_attempts, retry_kind, max_pending, retry_base, retry_cap, "
    'retry_backoff, retry_jitter FROM "{schema}".actor_config WHERE actor = $1'
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

# Log-frame escapes for caller-controlled text. A raw control character in a
# structlog event value forges a whole subsequent line under any
# line-oriented renderer (console/KV), corrupting log parsing and alerting ,
# so a URL-controlled field is escaped before it reaches an event, never
# passed through verbatim. \n/\r/\t keep their letter escapes for
# readability; every other C0 control and DEL renders as its hex escape.
_LOG_CONTROL_ESCAPES: dict[int, str] = {
    **{c: f"\\x{c:02x}" for c in range(0x20)},
    0x7F: "\\x7f",
    ord("\n"): "\\n",
    ord("\r"): "\\r",
    ord("\t"): "\\t",
}


def _log_safe_text(value: str) -> str:
    """Escape control characters so a caller-controlled value cannot forge log lines."""
    return value.translate(_LOG_CONTROL_ESCAPES)


async def _fetch_redis_rl_state(
    redis_client: Any,
    schema: str,
    names: Sequence[tuple[str, str]],
    *,
    read_timeout: float,
) -> dict[str, dict[str, str]] | None:
    """Fetch live Redis state for registered rate-limit primitives.

    *names* is a list of ``(bucket_name, kind)`` tuples where kind is
    ``"token_bucket"``, ``"sliding_window_log"``, or ``"sliding_window_gcra"``.
    Redis keys follow the conventions:

    * Token bucket:     ``taskq:{schema}:rl:tb:{name}``   (HGETALL)
    * Sliding window log: ``taskq:{schema}:sw:{name}``    (ZCARD for count)
    * Sliding window GCRA: ``taskq:{schema}:sw_gcra:{name}`` (GET for TAT)

    All reads run in ONE Redis pipeline, a single round trip for every
    bucket. The name set is unbounded by construction (the page unions the
    in-process registry with every ``rate_limit_buckets`` PG row, and keyed
    buckets accumulate there forever), so one awaited call per name made
    the request handler take O(names) sequential round trips on a page the
    admin UI re-polls, the per-row-round-trip shape.

    The one round trip is bounded by *read_timeout* (``TASKQ_ADMIN_ACQUIRE_TIMEOUT``):
    a black-holed broker degrades the page after that long instead of
    hanging the request. Returns ``None`` on any Redis failure - the
    timeout included - so the caller can degrade visibly (the failure is
    logged here as ``redis-rl-fetch-failed`` with its ``error_type``, and
    the page reports the degradation). An unknown *kind* raises
    :class:`ValueError` instead: that
    is a caller bug (the registry only emits the three kinds), and the
    degrade-to-None path exists for the transport being down, swallowing
    the validation failure would convert a loud programming error into a
    silent whole-page degrade.
    """
    if redis_client is None:
        return None
    if not names:
        return {}
    # The kind check runs while the pipeline is built, OUTSIDE the
    # degrade-to-None guard: queuing the commands is client-side (the
    # first Redis call is pipe.execute() inside the guard), so an unknown
    # kind fails loudly before any round trip.
    pipe = redis_client.pipeline()
    for name, kind in names:
        if kind == "token_bucket":
            pipe.hgetall(f"taskq:{schema}:rl:tb:{{{name}}}")
        elif kind == "sliding_window_gcra":
            pipe.get(f"taskq:{schema}:sw_gcra:{{{name}}}")
        elif kind == "sliding_window_log":
            pipe.zcard(f"taskq:{schema}:sw:{{{name}}}")
        else:
            raise ValueError(f"unknown rate-limit kind: {kind!r}")
    try:
        raw_results: list[Any] = await asyncio.wait_for(pipe.execute(), timeout=read_timeout)

        result: dict[str, dict[str, str]] = {}
        for (name, kind), raw in zip(names, raw_results, strict=True):
            if kind == "token_bucket" and raw:
                decoded: dict[str, str] = {}
                for k, v in raw.items() if isinstance(raw, dict) else raw:  # pyright: ignore[reportUnknownVariableType]  # Why: redis-py hgetall return type is untyped in the stub; isinstance narrowing at runtime ensures correct types.
                    kk = k.decode() if isinstance(k, bytes) else str(k)  # pyright: ignore[reportUnknownArgumentType]  # Why: redis-py key type is untyped in the stub; isinstance narrowing ensures correct str conversion.
                    vv = v.decode() if isinstance(v, bytes) else str(v)  # pyright: ignore[reportUnknownArgumentType]  # Why: redis-py value type is untyped in the stub; isinstance narrowing at runtime ensures correct str conversion.
                    decoded[kk] = vv
                if decoded:
                    result[name] = decoded
            elif kind == "sliding_window_gcra" and raw is not None:
                tat_str = raw.decode() if isinstance(raw, bytes) else str(raw)
                result[name] = {"tat": tat_str}
            elif kind == "sliding_window_log" and raw is not None and raw > 0:
                result[name] = {"count": str(raw)}
        return result
    except Exception as exc:
        logger.warning(
            "redis-rl-fetch-failed",
            error_type=type(exc).__name__,
            error=str(exc),
            buckets=len(names),
            read_timeout=read_timeout,
        )
        return None


def register(router: APIRouter) -> None:
    """Attach schedules, rate-limits, and reservations routes to *router*."""

    @router.get("/schedules", response_class=HTMLResponse)
    async def schedules_page(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        error: str | None = None,
        csrf_token: str = Depends(get_csrf_token),
        pool: BoundedPool = Depends(get_admin_pool),
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
            notice_text="cron scheduling not installed, run taskq migrate up to enable",
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
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        base_path: str = Depends(get_base_path),
        settings: TaskQSettings = Depends(get_settings),
    ) -> RedirectResponse:
        if not settings.admin_actions_enabled:
            raise HTTPException(status_code=403, detail="Admin actions are disabled")

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
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        base_path: str = Depends(get_base_path),
        settings: TaskQSettings = Depends(get_settings),
    ) -> RedirectResponse:
        if not settings.admin_actions_enabled:
            raise HTTPException(status_code=403, detail="Admin actions are disabled")

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
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        base_path: str = Depends(get_base_path),
        settings: TaskQSettings = Depends(get_settings),
    ) -> RedirectResponse:
        if not settings.admin_actions_enabled:
            raise HTTPException(status_code=403, detail="Admin actions are disabled")

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
            # Subscript, not .get(default): the fetch is contracted to
            # provide this column, and a defaulting read is exactly what
            # made the cron loop silently skip for 'allof' schedules
            # whatever they stored (7d7e01c). The coercion mirrors
            # cron_loop's: the column is CHECK-constrained to the three
            # strategies, this only guards a hand-edited row.
            dst_strategy_raw: str = row["dst_strategy"]
            dst_strategy: DstStrategy = (
                dst_strategy_raw if dst_strategy_raw in DST_STRATEGIES else "skip"
            )

            # The advance-until-future test must run in the same clock
            # domain that later decides the schedule is due: the cron loop
            # fires on `next_fire_at <= clock_timestamp()` server-side, so
            # comparing against this process's clock would let a skewed app
            # clock write a `next_fire_at` that PG already considers past --
            # firing immediately, which is the one outcome "skip" exists to
            # prevent. Read from the same row fetch, so it costs no round
            # trip.
            db_now: datetime = row["db_now"]
            new_next = compute_next_fire_after(
                cron_expr, tz_name, current_next, dst_strategy=dst_strategy
            )[0]
            for _ in range(1000):
                if new_next > db_now:
                    break
                new_next = compute_next_fire_after(
                    cron_expr, tz_name, new_next, dst_strategy=dst_strategy
                )[0]
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
        pool: BoundedPool = Depends(get_admin_pool),
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

            # Imported in the handler body: the admin package stays
            # importable without the worker (the boundary
            # test_no_worker_import pins), and the fire paths' shared
            # curve helper is cron_loop's by convention.
            from taskq.worker.cron_loop import (
                _fire_default_curve,  # pyright: ignore[reportPrivateUsage]
            )

            defaults = _fire_default_curve()
            args = EnqueueArgs(
                id=JobId(new_uuid()),
                actor=actor,
                queue=ac_row["queue"],
                payload=payload,
                max_attempts=ac_row["max_attempts"],
                retry_kind=parse_retry_kind(ac_row["retry_kind"]),
                # The actor's declared retry curve (migration 01.00.18):
                # NULL rows keep the enqueue defaults, matching the cron
                # fire path's resolution.
                retry_base=ac_row["retry_base"] or defaults.base,
                retry_cap=ac_row["retry_cap"] or defaults.cap,
                retry_backoff=(
                    ac_row["retry_backoff"]
                    if ac_row["retry_backoff"] in ("exponential", "linear", "fixed")
                    else defaults.backoff
                ),
                retry_jitter=(
                    ac_row["retry_jitter"]
                    if ac_row["retry_jitter"] is not None
                    else defaults.jitter
                ),
                scheduled_at=None,  # Why: "run now" is immediate, the server stamps and decides, immune to app↔DB clock skew.
                # The operator's STORED max_pending cap bounds run-now like
                # every other fire of a schedule: the cron tick resolves
                # _resolve_max_pending(stored, registry literal) and the
                # client path resolves the same rule through the capacity
                # cache, while the single-enqueue path this call reaches
                # enforces only the CARRIED value, so a NULL here would
                # silently exempt run-now from a drain the operator set.
                # A non-NULL stored cap is authoritative over the registry
                # literal on those paths (it tightens or loosens it), so
                # carrying the stored value alone makes run-now answer the
                # same question they answer whenever a stored cap exists.
                # The residual: no stored cap and a registry literal - the
                # literal lives in the worker's actor_registry, not the
                # database, so this path cannot know it and enforces
                # nothing; the tick and client paths still do.
                max_pending=ac_row["max_pending"],
                # Provenance parity with every other fire of a schedule
                # (the tick stamps the same key in _plan_fire): per-schedule
                # attribution and the allof twin-coverage walk scope jobs
                # by this key; an empty metadata dict makes a run-now job
                # invisible to both.
                metadata={"cron_schedule_id": str(schedule_id)},
            )

        try:
            await backend.enqueue(args)
        except BackpressureError:
            # The stored max_pending cap refused the fire: redirect with
            # the reason like the other preflight refusals above. The
            # operator pressed run-now during their own drain.
            return RedirectResponse(
                url=f"{base_path}/schedules?error=actor+{quote_plus(actor)}+at+max_pending+cap",
                status_code=303,
            )

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

        # A job is retryable from every state ``Backend.retry_job`` accepts
        # as a source: every terminal status (see its docstring, an
        # operator re-run is "run this again", and that includes
        # 'succeeded' and 'abandoned', not just the failure statuses).
        # ``_TERMINAL_STATUSES`` (admin/_constants.py) is that same set ,
        # the list/detail pages already use it to mean "this job is done" ,
        # so this gate derives from it rather than hand-maintaining a
        # second, narrower copy that silently falls behind the backend's
        # actual contract.
        job = await backend.get(JobId(job_id))
        if job is None:
            raise HTTPException(status_code=404, detail="Job not found")
        if job.status not in _TERMINAL_STATUSES:
            raise HTTPException(status_code=409, detail="Job is not in a retryable state")

        # The read above is only a pre-check: the write's own guard is the
        # arbiter. A False means the row left a retryable state between the
        # two (a concurrent claim or transition won the race), or the spent
        # attempt sits at the smallint ceiling, so the write applied to
        # nothing and the operator must hear conflict, not success.
        retried = await backend.retry_job(JobId(job_id))
        if not retried:
            raise HTTPException(status_code=409, detail="Job is not in a retryable state")

        return RedirectResponse(url=f"{base_path}/jobs/{job_id}", status_code=303)

    @router.get("/rate-limits", response_class=HTMLResponse)
    async def rate_limits_page(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        redis_client: Any | None = Depends(get_redis_client),
        settings: Any = Depends(get_settings),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        csrf_token: str = Depends(get_csrf_token),
        rl_registry: RateLimitRegistry = Depends(get_rl_registry),
    ) -> HTMLResponse:
        from taskq.ratelimit.token_bucket import TokenBucket
        from taskq.settings import WorkerSettings

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

        # Keyed buckets materialized after worker startup are published to
        # PG by the acquisition path; in a standalone admin process they
        # exist ONLY as pg_state rows (the in-process registry never
        # dispatches jobs). Include them in the live-Redis fetch so their
        # per-key state (tokens / GCRA TAT) is visible too.
        redis_known = {name for name, _kind in redis_names}
        for pg_name, pg_row in pg_state.items():
            if pg_name in redis_known:
                continue
            pg_kind = pg_row["kind"]
            if pg_kind == "token_bucket":
                redis_names.append((pg_name, "token_bucket"))
            elif pg_kind == "gcra":
                redis_names.append((pg_name, "sliding_window_gcra"))

        # Attempt live peek for non-memory backends if dependencies are available.
        live_states: dict[str, object] = {}
        live_peek_error: str | None = None
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
                pg_pool=pool.pool,
                clock=clock,
                settings=rl_settings,
                # The same bound every other backend wait on this page takes
                # (admin_acquire_timeout): each bucket's peek is a separate
                # broker round trip and the registry can hold
                # max_keyed_rate_limits keyed buckets, so an unbounded pass
                # parks the request on a black-holed broker. The TimeoutError
                # degrades the page exactly like any other peek failure.
                timeout=settings.admin_acquire_timeout,
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
        except Exception as exc:
            # The page still renders on PG state alone, but a bucket with no
            # live state must not read as a bucket with nothing in flight:
            # the failure is reported and the page says it is degraded.
            live_peek_error = type(exc).__name__
            logger.warning(
                "ratelimit-peek-all-failed",
                error_type=live_peek_error,
                error=str(exc),
                buckets=len(configured),
            )

        redis_available = False
        redis_configured = redis_client is not None
        redis_state: dict[str, dict[str, str]] | None = None

        if redis_configured:
            redis_available = True
            redis_state = await _fetch_redis_rl_state(
                redis_client, schema, redis_names, read_timeout=settings.admin_acquire_timeout
            )
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
                        "kind": pg_row["kind"],
                        "backend": "postgres",
                        "config_summary": "",
                        "pg_state": pg_row["state"],
                        "updated_at": pg_row["updated_at"],
                    }
                )

        has_memory_buckets = any(str(b["backend"]) == "memory" for b in buckets)
        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("rate_limits.html").render(
            allow_reset=allow_reset,
            buckets=buckets,
            csrf_token=csrf_token,
            ratelimit_installed=ratelimit_installed,
            notice_text="rate limiting not installed, run taskq migrate up to enable",
            live_states=live_states,
            redis_state=redis_state,
            redis_available=redis_available,
            redis_configured=redis_configured,
            live_peek_error=live_peek_error,
            has_memory_buckets=has_memory_buckets,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)

    @router.post("/rate-limits/{bucket_name}/reset")
    async def rate_limit_reset(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        bucket_name: str,
        _csrf: None = Depends(validate_csrf),
        pool: BoundedPool = Depends(get_admin_pool),
        redis_client: Any | None = Depends(get_redis_client),
        schema: str = Depends(get_schema),
        settings: Any = Depends(get_settings),
        base_path: str = Depends(get_base_path),
        rl_registry: RateLimitRegistry = Depends(get_rl_registry),
    ) -> RedirectResponse:
        from taskq.settings import WorkerSettings

        allow_reset = getattr(settings, "admin_ui_allow_rate_limit_reset", False)
        if not allow_reset:
            raise HTTPException(status_code=403, detail="Rate limit reset is disabled")

        rl_settings = WorkerSettings.load_from_dict(
            {
                "pg_dsn": str(settings.pg_dsn),
                "schema_name": schema,
            }
        )

        try:
            await rl_registry.reset(
                bucket_name,
                redis_client=redis_client,
                pg_pool=pool.pool,
                clock=SystemClock(),
                settings=rl_settings,
                # The same bound every other backend wait on the admin UI
                # takes (admin_acquire_timeout): the reset's round trip must
                # time out into a 503, not park the request on a dead store.
                timeout=settings.admin_acquire_timeout,
            )
        except TimeoutError:
            # The reset is a best-effort state change, not a read the page
            # needs to render: answer with the same 503/Retry-After shape
            # the pool checkout uses, naming the bound.
            logger.warning(
                "rate-limit-reset-timed-out",
                bucket_name=_log_safe_text(bucket_name),
                timeout=settings.admin_acquire_timeout,
            )
            raise HTTPException(
                status_code=503,
                detail=(
                    f"Rate limit reset did not complete within "
                    f"{settings.admin_acquire_timeout}s; whether the store "
                    "applied it is unknowable from here - re-check the page "
                    "before retrying."
                ),
                headers={"Retry-After": "2"},
            ) from None
        except KeyError as exc:
            # A keyed bucket a worker published to PG exists ONLY as a PG
            # row in a standalone admin process, the registry has no
            # primitive for it, so a reset is impossible here. The page
            # still renders the reset button for such rows; answer it with
            # an explanatory 404 instead of surfacing the KeyError as a 500.
            logger.warning(
                "rate-limit-reset-bucket-not-registered",
                bucket_name=_log_safe_text(bucket_name),
            )
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Rate limit bucket {bucket_name!r} is not registered in this admin "
                    "process (worker-published PG state only); reset it from a process "
                    "that configures the bucket"
                ),
            ) from exc

        return RedirectResponse(url=f"{base_path}/rate-limits", status_code=303)

    @router.get("/reservations", response_class=HTMLResponse)
    async def reservations_page(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        rl_registry: RateLimitRegistry = Depends(get_rl_registry),
        settings: Any = Depends(get_settings),
    ) -> HTMLResponse:
        from taskq.ratelimit.registry import QUEUE_CONCURRENCY_PREFIX
        from taskq.ratelimit.reservation import sync_slots

        _queue_cap_prefix = QUEUE_CONCURRENCY_PREFIX

        configured_reservations: list[dict[str, object]] = []
        # Filter by the admin's own schema before displaying or syncing ,
        # the process-global registry may carry reservations declared for
        # OTHER schemas (same reason worker/_bootstrap.py filters): syncing
        # a foreign-schema reservation here would insert/delete rows in the
        # local schema's reservation_slots table for a name it does not own.
        reservation_primitives = [
            res for res in rl_registry.reservations.values() if res.schema == schema
        ]

        for name, prim in sorted(rl_registry.reservations.items()):
            if prim.schema != schema:
                continue
            configured_reservations.append(
                {
                    "bucket_name": name,
                    "configured_slots": prim.slots,
                    "lease": str(prim.lease),
                    "is_queue_cap": name.startswith(_queue_cap_prefix),
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

        sync_error: str | None = None
        if reservations_installed and reservation_primitives:
            try:
                await sync_slots(
                    reservation_primitives,
                    pool.pool,
                    schema=schema,
                    # The same bound every other backend wait on the admin
                    # UI takes (admin_acquire_timeout): the sync costs one
                    # connection acquire plus a transaction of statements
                    # PER reservation, so a wedged store must time out into
                    # the degraded page, not park the request.
                    timeout=settings.admin_acquire_timeout,
                )
                async with pool.acquire() as conn:
                    rows = await conn.fetch(reservations_sql)
                    held_slot_rows = await conn.fetch(held_slots_sql)
            except Exception as exc:
                # The rows read before the sync still render, but a table
                # that silently predates a failed sync is a stale table the
                # operator cannot tell from a fresh one: report it and say
                # so on the page.
                sync_error = type(exc).__name__
                logger.warning(
                    "reservation-sync-failed",
                    error_type=sync_error,
                    error=str(exc),
                    primitives=len(reservation_primitives),
                )

        pg_state: dict[str, dict[str, object]] = {}
        for r in rows:
            d = dict(r)
            pg_state[str(d["bucket_name"])] = d

        reservations: list[dict[str, object]] = []
        for entry in configured_reservations:
            name = str(entry["bucket_name"])
            merged: dict[str, object] = dict(entry)
            if name in pg_state:
                merged["held_count"] = pg_state[name]["held_count"]
                merged["free_count"] = pg_state[name]["free_count"]
                merged["total_slots"] = pg_state[name]["total_slots"]
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
                        "configured_slots": pg_row["total_slots"],
                        "lease": ",",
                        "held_count": pg_row["held_count"],
                        "free_count": pg_row["free_count"],
                        "total_slots": pg_row["total_slots"],
                        "is_queue_cap": name.startswith(_queue_cap_prefix),
                    }
                )

        realtime_mode, mode_label = realtime_ctx
        held_slots: list[dict[str, object]] = [dict(r) for r in held_slot_rows]
        html = tmpl.get_template("reservations.html").render(
            reservations=reservations,
            reservations_installed=reservations_installed,
            sync_error=sync_error,
            notice_text="reservations not installed, run taskq migrate up to enable",
            held_slots=held_slots,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)
