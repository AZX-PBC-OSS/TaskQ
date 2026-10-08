"""Trigger FastAPI app - enqueue jobs via TaskQ and view them in the embedded admin UI.

Renders one card per actor at ``GET /``, each with an HTML form for the
actor's payload fields.  ``POST /enqueue/{actor_name}`` parses form data,
validates it through the actor's Pydantic payload model, enqueues via
:class:`~taskq.client.TaskQ`, and redirects to the embedded admin job
detail page at ``/taskq/jobs/{job_id}``.  All configuration is loaded
through :meth:`TaskQSettings.load`; no raw ``os.environ`` access.

The admin UI is mounted at ``/taskq`` using ``create_router`` and
``setup_admin_state`` - no sidecar process required.

Special handling:
- Backpressure errors (:class:`~taskq.SingletonCollisionError`,
  :class:`~taskq.MaxPendingExceededError`) return HTTP 409 JSON so the
  frontend can show a toast notification.
- The ``summer`` actor returns a typed result; enqueuing it returns a
  ``/result/{job_id}`` URL that the frontend opens in a new tab.  That
  page polls ``handle.wait()`` with a short timeout and renders the result
  inline.
- The ``deduplicated`` actor passes an ``identity_key`` derived from the
  payload ``key`` field so ``unique_for`` dedup works correctly.
- Tagged actors (``tagged_lower``, ``tagged_upper``) pass ``tags=`` at
  enqueue.

Additional routes:
- ``POST /batch-fast`` - demonstrates :meth:`TaskQ.enqueue_batch_fast`
  by enqueuing N counter jobs via COPY FROM.
- ``GET /rate-limits`` - JSON peek at all registered rate-limit bucket
  states via :meth:`RateLimitRegistry.peek_all`.
"""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from jinja2 import Environment, FileSystemLoader
from pydantic import BaseModel, TypeAdapter, ValidationError

from examples.actors import (
    batch_counter,
    capped_job,
    count_words,
    counter,
    db_lookup_actor,
    deduplicated,
    deferred,
    fan_out,
    fetch_actor,
    file_processor,
    flaky,
    generate_thumbnail,
    inmemory_rate_limited,
    process_csv_upload,
    reserved,
    send_digest_email,
    singleton_job,
    snoozer,
    step_one,
    summer,
    tagged_lower,
    tagged_upper,
    token_rate_limited,
    window_rate_limited,
)
from examples.actors.advanced import SumResult
from examples.actors.basic import CounterPayload
from taskq import (
    ActorRef,
    EnqueueItem,
    IdentityKey,
    JobId,
    MaxPendingExceededError,
    SingletonCollisionError,
    TaskQ,
)
from taskq.migrate import apply_pending_locked
from taskq.ratelimit import ConcurrencyReservation, RateLimitRegistry, SlidingWindow, TokenBucket
from taskq.settings import TaskQSettings
from taskq.web._routing import HeadForGetRoute
from taskq.web.admin import create_router, setup_admin_state

_SUMMER_RESULT_ADAPTER: TypeAdapter[SumResult] = TypeAdapter(SumResult)
_NONE_RESULT_ADAPTER: TypeAdapter[None] = TypeAdapter(None)

ACTORS: dict[str, ActorRef[Any, Any]] = {
    "counter": counter,
    "flaky": flaky,
    "snoozer": snoozer,
    "deferred": deferred,
    "window_rate_limited": window_rate_limited,
    "token_rate_limited": token_rate_limited,
    "inmemory_rate_limited": inmemory_rate_limited,
    "reserved": reserved,
    # chained actors
    "step_one": step_one,
    "fan_out": fan_out,
    # batch actors
    "batch_counter": batch_counter,
    # DI actors
    "fetch": fetch_actor,
    "db_lookup": db_lookup_actor,
    # advanced actors
    "singleton_job": singleton_job,
    "capped_job": capped_job,
    "deduplicated": deduplicated,
    "summer": summer,
    # progress actors (M5)
    "file_processor": file_processor,
    # tagged actors
    "tagged_lower": tagged_lower,
    "tagged_upper": tagged_upper,
    # sync actor
    "count_words": count_words,
    # real-world scenario actors
    "send_digest_email": send_digest_email,
    "process_csv_upload": process_csv_upload,
    "generate_thumbnail": generate_thumbnail,
}

settings = TaskQSettings.load()

# Owned rate-limit registry for this process. The example's actors reference
# primitives by NAME, so they are registered explicitly here - the
# worker-side pattern is instead to declare primitive INSTANCES on the actor
# (@actor(rate_limits=[TokenBucket(...)])) and let the worker bootstrap's
# collection pass register them automatically. Configs mirror
# examples/actors/ratelimit.py, which still registers on the module singleton
# for the worker process's default resolution path.
rl_registry = RateLimitRegistry()
rl_registry.register(
    SlidingWindow(name="example_window", limit=3, window=timedelta(seconds=15), backend="redis")
)
rl_registry.register(
    TokenBucket(name="example_token", capacity=3, refill_per_second=1.0, backend="redis")
)
rl_registry.register(
    SlidingWindow(name="example_inmemory", limit=2, window=timedelta(seconds=10), backend="memory")
)
rl_registry.register(
    ConcurrencyReservation(name="example_concurrency", slots=2, lease=timedelta(seconds=30))
)

_templates = Environment(
    autoescape=True,
    loader=FileSystemLoader(Path(__file__).parent / "templates"),
)


@asynccontextmanager
async def lifespan(application: FastAPI) -> AsyncGenerator[None]:
    if settings.migrate_on_start:
        # PHASE=None (pre AND post) is the demo's fresh-schema contract
        # (the stranger test's stumble #2): the pre-only default leaves
        # the OLD single-column idempotency index standing (its drop is a
        # post migration, gated for ROLLING deploys — a fresh demo schema
        # has no old workers to protect), and the static workflow nodes'
        # per-flow keys collide on it across runs (the second run's
        # create_flow raised UniqueViolationError on wf:gather).
        await apply_pending_locked(str(settings.pg_dsn), schema=settings.schema_name, phase=None)

    async with AsyncExitStack() as stack:
        pg_pool: asyncpg.Pool = await stack.enter_async_context(
            asyncpg.create_pool(str(settings.pg_dsn), min_size=1, max_size=2),  # type: ignore[arg-type]  # Why: asyncpg.create_pool returns AsyncContextManager[Pool | None]; enter_async_context expects AsyncContextManager[T]; pyright cannot resolve the generic.
        )

        redis_client: Any = None
        if settings.redis_url is not None:
            import redis.asyncio as aioredis

            redis_client = aioredis.from_url(str(settings.redis_url))
            stack.push_async_callback(redis_client.aclose)

        application.state.redis_client = redis_client
        application.state.pg_pool = pg_pool

        tq = await stack.enter_async_context(
            TaskQ(
                pool=pg_pool,
                schema=settings.schema_name,
                redis_client=redis_client,
            ),
        )
        application.state.tq = tq

        # THE WORKFLOW DEMO (T16): the drive loop's task + the mounted
        # WorkflowApp — the admin's typed resolve/deliver doors validate
        # through IT (no untyped deliver surface ships).
        from examples.workflows import drive_loop, wf_app

        application.state.workflow_app = wf_app
        drive_task = asyncio.create_task(drive_loop(pg_pool, settings.schema_name))
        # Task.cancel is a SYNC method (it returns bool) — pushed as a
        # SYNC callback. push_async_callback would await the bool at
        # teardown (TypeError: object bool can't be used in 'await' —
        # CERT2 F-CERT2-1's demo-teardown red; the serve path itself
        # was always fine, the live stack proves it).
        stack.callback(drive_task.cancel)

        # The Backend the client built reaches the admin router too: without
        # it the admin UI's backend-mediated mutation buttons (job cancel,
        # job retry, schedule run-now) render but every one answers 503.
        admin_bundle = create_router(
            pg_pool,
            schema=settings.schema_name,
            redis_client=redis_client,
            base_path="/taskq",
            rate_limit_registry=rl_registry,
            backend=tq.backend,
            workflow_app=wf_app,
        )
        setup_admin_state(application, admin_bundle)
        application.include_router(admin_bundle.router, prefix="/taskq")

        yield


# F6: the monitor's HEAD check against the trigger app's GET routes
# (/, /rate-limits) must not 405. Set BEFORE the routes register.
app = FastAPI(lifespan=lifespan)
app.router.route_class = HeadForGetRoute


@app.get("/")
async def index(request: Request) -> Response:
    actors_desc: list[dict[str, Any]] = []
    for name, ref in ACTORS.items():
        fields: list[tuple[str, str, str, str]] = []
        for field_name, field_info in ref.payload_type.model_fields.items():
            annotation = field_info.annotation
            type_str = "number" if annotation is int else "text"
            default_val = ""
            if not field_info.is_required() and field_info.default is not None:
                default_val = str(field_info.default)
            label = field_info.description or field_name
            fields.append((field_name, type_str, default_val, label))
        description = (ref.fn.__doc__ or "").strip().split("\n")[0]
        actors_desc.append(
            {
                "name": name,
                "description": description,
                "fields": fields,
                "has_result": ref.result_ttl is not None,
                # The demo progress toast subscribes to this actor's
                # stream; the flag rides the CARD, never the response
                # envelope (F3 keeps that {"job_id","url"} alone).
                "has_progress": name == "file_processor",
                "watch_queue_url": ("/taskq/queues/examples" if name == "batch_counter" else None),
            }
        )
    html = _templates.get_template("index.html").render(
        actors=actors_desc,
        admin_url="/taskq",
    )
    return Response(content=html, media_type="text/html")


@app.post("/enqueue/{actor_name}")
async def enqueue_actor(actor_name: str, request: Request) -> Response:
    """Enqueue one job; the ONE success envelope (F3): ``201`` (or ``202``
    for a result-bearing actor's deferred result page) with ``{"job_id",
    "url"}`` — the job id and the page that watches it — for every
    trigger route. Every 4xx is JSON with the FastAPI ``{"detail": ...}``
    shape; nothing answers raw text."""
    ref = ACTORS.get(actor_name)
    if ref is None:
        return JSONResponse({"detail": f"unknown actor: {actor_name}"}, status_code=404)

    form = await request.form()
    try:
        payload = ref.payload_type.model_validate(dict(form))
    except ValidationError as exc:
        return JSONResponse(
            {"detail": f"invalid payload for {actor_name!r}: {_validation_error_detail(exc)}"},
            status_code=400,
        )

    tq: TaskQ = request.app.state.tq
    enqueue_kwargs: dict[str, Any] = {}

    if actor_name == "deferred":
        scheduled_at = datetime.now(UTC) + timedelta(
            seconds=payload.delay_seconds,
        )
        enqueue_kwargs["scheduled_at"] = scheduled_at

    if actor_name == "deduplicated":
        enqueue_kwargs["identity_key"] = IdentityKey(payload.key)

    if actor_name == "tagged_lower":
        enqueue_kwargs["tags"] = ["alpha", "lower"]
    elif actor_name == "tagged_upper":
        enqueue_kwargs["tags"] = ["alpha", "upper"]

    try:
        handle = await tq.enqueue(ref, payload, **enqueue_kwargs)
    except SingletonCollisionError as exc:
        return JSONResponse(
            {
                "detail": (
                    f"A '{actor_name}' job is already active "
                    f"(job {exc.blocking_job_id}). Try again after it completes."
                )
            },
            status_code=409,
        )
    except MaxPendingExceededError as exc:
        return JSONResponse(
            {
                "detail": (
                    f"Too many pending '{actor_name}' jobs "
                    f"({exc.current_count} queued). Try again later."
                )
            },
            status_code=409,
        )

    if ref.result_ttl is not None:
        # 202: the result is not ready - the url is the result page that
        # polls handle.wait().
        return JSONResponse(
            {"job_id": str(handle.job_id), "url": f"/result/{handle.job_id}"},
            status_code=202,
        )

    return JSONResponse(
        {"job_id": str(handle.job_id), "url": f"/taskq/jobs/{handle.job_id}"},
        status_code=201,
    )


# ── the workflow demo (T16) ─────────────────────────────────────────────
#
# The SAME envelope contract (the repo's F3 finding): the workflow
# trigger answers 202 with the RUN id + the run-watch url (the admin's
# workflow page — the graph view with the live SSE). A run-key conflict
# (the same cron slot twice) answers 202 WITH THE EXISTING run id (the
# idempotency shows in the envelope — the SAME id for the same slot; the
# code always answers 202, never a 200 re-read), the run-level
# idempotency is the demo, not an error.


@app.post("/workflows/{workflow_name}/run")
async def run_workflow(workflow_name: str, request: Request) -> Response:
    """Trigger the demo workflow; 202 + {run_id, url} — the F3 envelope."""
    from examples.workflows import trigger_run

    if workflow_name != "doc_ingest":
        raise HTTPException(status_code=404, detail=f"no demo workflow {workflow_name!r}")
    pool: asyncpg.Pool = request.app.state.pg_pool  # type: ignore[attr-defined]  # Why: the lifespan set it.
    settings_obj = TaskQSettings.load()
    run_key: str | None = None
    if request.method == "POST":
        form = await request.form()
        raw_slot = form.get("slot")
        if isinstance(raw_slot, str) and raw_slot:
            run_key = f"doc_ingest:demo:{raw_slot}"
    run_id = await trigger_run(pool, settings_obj.schema_name, run_key)
    return JSONResponse(
        {"run_id": run_id, "url": f"/taskq/workflows/{run_id}"},
        status_code=202,
    )


@app.get("/progress/{job_id}")
async def stream_progress(job_id: UUID, request: Request) -> Response:
    """Stream live progress events for a job as Server-Sent Events.

    Subscribes to the per-job Redis pub/sub channel via
    :meth:`~taskq.JobHandle.progress_stream` and re-encodes each
    :class:`~taskq.progress.ProgressEvent` as an SSE ``data:`` line.
    The stream ends when the job reaches a terminal state
    (``terminal=True`` on the event) or the client disconnects.

    Requires Redis - returns 503 if the handle has no Redis connection.
    """
    tq: TaskQ = request.app.state.tq
    handle = await tq.get(JobId(job_id), result_adapter=_NONE_RESULT_ADAPTER)
    if handle is None:
        return JSONResponse({"detail": "job not found"}, status_code=404)

    async def _event_stream() -> AsyncGenerator[str, None]:
        try:
            async for event in handle.progress_stream():
                payload = event.model_dump_json(exclude_none=True)
                yield f"data: {payload}\n\n"
                if event.terminal:
                    break
        except NotImplementedError:
            yield 'data: {"error": "Redis not configured; progress_stream unavailable"}\n\n'

    return StreamingResponse(
        _event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/result/{job_id}")
async def get_result(job_id: UUID, request: Request) -> Response:
    """Poll for a job result and render it inline; auto-refreshes if not yet ready."""
    tq: TaskQ = request.app.state.tq

    handle = await tq.get(JobId(job_id), result_adapter=_SUMMER_RESULT_ADAPTER)
    if handle is None:
        return JSONResponse({"detail": "job not found"}, status_code=404)

    # The row get() just fetched - no second backend read for the status.
    status = handle.row.status
    if status == "succeeded":
        try:
            result = await handle.wait(timeout=2.0)
        except TimeoutError:
            result = None
        else:
            html = _templates.get_template("result.html").render(
                job_id=job_id,
                status=status,
                result=result.model_dump(),
                admin_url=settings.admin_url,
                done=True,
            )
            return Response(content=html, media_type="text/html")

    html = _templates.get_template("result.html").render(
        job_id=job_id,
        status=status,
        result=None,
        admin_url=settings.admin_url,
        done=False,
    )
    return Response(content=html, media_type="text/html")


# ── Cancel ───────────────────────────────────────────────────────────────


def _validation_error_detail(exc: ValidationError) -> str:
    """The compact one-line validation-error summary (F3).

    Pydantic's str(exc) is a multi-line dump (header + one block per
    error) — unusable in a toast/log line and wildly longer than the
    ask. One line: ``field: message; field: message``.
    """
    return "; ".join(
        f"{'.'.join(str(loc) for loc in error.get('loc', []))}: {error.get('msg', 'invalid')}"
        for error in exc.errors()
    )


@app.post("/cancel/{job_id}")
async def cancel_job(job_id: UUID, request: Request) -> JSONResponse:
    """Cancel a running or pending job by ID.

    Delegates to :meth:`TaskQ.cancel` which writes a cancel request to
    Postgres. The worker observes it on the next heartbeat tick and
    initiates the three-phase cancellation protocol.
    """
    tq: TaskQ = request.app.state.tq
    try:
        result = await tq.cancel(JobId(job_id), reason="user_requested")
    except KeyError:
        return JSONResponse({"detail": "job not found"}, status_code=404)

    return JSONResponse(
        {
            "job_id": str(result.job_id),
            "previous_status": result.previous_status,
            "new_status": result.new_status,
            "cancellation_initiated": result.cancellation_initiated,
        }
    )


# ── Batch-fast enqueue ──────────────────────────────────────────────────


class BatchFastPayload(BaseModel):
    n: int = 5


@app.post("/batch-fast")
async def batch_fast(request: Request) -> JSONResponse:
    """Enqueue N counter jobs via :meth:`TaskQ.enqueue_batch_fast` (COPY FROM).

    Returns ``{"count": N}`` on success. The high-throughput variant uses
    COPY protocol - no per-job handles, no idempotency-key collision
    handling, no max_pending check. Suitable for bulk import with 1K-50K rows.
    """
    try:
        body = await request.json()
        payload = BatchFastPayload.model_validate(body)
    except (ValidationError, ValueError, TypeError):
        payload = BatchFastPayload()

    tq: TaskQ = request.app.state.tq
    items = [EnqueueItem(actor_ref=counter, payload=CounterPayload(n=3)) for _ in range(payload.n)]
    count = await tq.enqueue_batch_fast(items)
    return JSONResponse({"count": count, "actor": "counter"})


# ── Rate-limit peek ─────────────────────────────────────────────────────


@app.get("/rate-limits")
async def peek_rate_limits(request: Request) -> JSONResponse:
    """Peek all registered rate-limit bucket states.

    Returns a JSON dict of ``{bucket_name: RateLimitState}``. Requires
    Redis for full live state; memory-backed buckets show PG-persisted
    metadata only.
    """
    redis_client = getattr(request.app.state, "redis_client", None)
    pg_pool = getattr(request.app.state, "pg_pool", None)

    from taskq.settings import WorkerSettings

    rl_settings = WorkerSettings.load_from_dict(
        {
            "pg_dsn": str(settings.pg_dsn),
            "schema_name": settings.schema_name,
        }
    )

    try:
        live_states = await rl_registry.peek_all(
            redis_client=redis_client,
            pg_pool=pg_pool,
            settings=rl_settings,
        )
    except Exception:
        live_states = {}

    result: dict[str, dict[str, object]] = {}
    for name, state in live_states.items():
        d: dict[str, object] = {
            "bucket_name": state.bucket_name,
            "backend": state.backend,
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
        if state.refill_per_second is not None:
            d["refill_per_second"] = state.refill_per_second
        result[name] = d

    return JSONResponse(result)
