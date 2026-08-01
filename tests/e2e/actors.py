"""E2E actors — deterministic workloads shared by the test process and the worker container.

The test process imports this module as ``tests.e2e.actors`` (ActorRef
handles + payload models); the worker container imports it as
``e2e.actors`` (PYTHONPATH=/app). Imports are limited to stdlib, pydantic,
asyncpg, and the taskq public API so both environments load it cleanly.
The one exception is a relative sibling import (``from .di import
FakeHttpClient``): TaskQ's DI solver keys providers by type identity
(:func:`taskq._di.solver.solve_dependencies`), so the injected type must
resolve in this module's namespace at decoration time — the relative form
resolves under both package roots where an absolute ``e2e.di`` /
``tests.e2e.di`` import would break one of them.

All workloads are deterministic: simulated fetches via ``asyncio.sleep`` +
static data, failure injection only via payload flags. No external
services, no randomness.
"""

import asyncio
import json
import time
from datetime import timedelta
from functools import lru_cache
from typing import Literal
from uuid import UUID, uuid4

import asyncpg
from pydantic import BaseModel, Field

from taskq import EnqueueItem, JobContext, RetryPolicy, Snooze, actor
from taskq.ratelimit import KeyedRateLimitRef, TokenBucket, registry
from taskq.settings import WorkerSettings

from .di import FakeHttpClient


@lru_cache(maxsize=1)
def _effects_schema() -> str:
    """Return the TaskQ schema name from worker env (``TASKQ_SCHEMA_NAME``).

    Cached once per process — the worker container's env is fixed at start.
    """
    return WorkerSettings.load().schema_name


async def _record_effect[P: BaseModel](
    pool: asyncpg.Pool,
    ctx: JobContext[P],
    kind: str,
    detail: dict[str, object],
) -> None:
    """Append one row to the ``e2e_effects`` scratch table (test ground truth).

    The schema identifier is operator-controlled and validated as an
    identifier at settings load; every value is bound as an asyncpg
    parameter. ``detail`` is JSON-encoded for the JSONB column.
    """
    schema = _effects_schema()
    await pool.execute(
        f'INSERT INTO "{schema}".e2e_effects (actor, job_id, attempt, kind, detail) '
        "VALUES ($1, $2, $3, $4, $5::jsonb)",
        ctx.actor,
        ctx.job_id,
        ctx.attempt,
        kind,
        json.dumps(detail),
    )


class WelcomeEmailPayload(BaseModel):
    run_id: str
    user_id: str
    email: str


class WelcomeEmailResult(BaseModel):
    message_id: str
    sent: bool


@actor(
    name="send_welcome_email",
    queue="e2e",
    retry=RetryPolicy(max_attempts=2, base=timedelta(milliseconds=200)),
    result_ttl=timedelta(hours=1),
)
async def send_welcome_email(
    payload: WelcomeEmailPayload,
    ctx: JobContext[WelcomeEmailPayload],
    *,
    pool: asyncpg.Pool,
) -> WelcomeEmailResult:
    """Simulates template render + SMTP send as two progress steps."""
    await ctx.progress(step=1, percent=50.0, detail="render template")
    await asyncio.sleep(0.05)
    await ctx.progress(step=2, percent=100.0, detail="smtp send")
    await asyncio.sleep(0.05)
    message_id = f"msg-{payload.user_id}"
    await _record_effect(
        pool,
        ctx,
        "send",
        {"run_id": payload.run_id, "email": payload.email, "message_id": message_id},
    )
    return WelcomeEmailResult(message_id=message_id, sent=True)


class PermanentSyncError(Exception):
    """Non-retryable sync failure — proves the permanent-failure taxonomy."""


class SyncUserProfilePayload(BaseModel):
    run_id: str
    user_id: str
    fail_times: int = Field(default=0, ge=0)
    fail_kind: Literal["transient", "permanent", "snooze"] = "transient"
    fetch_latency_ms: int = Field(default=50, ge=0)


@actor(
    name="sync_user_profile",
    queue="e2e",
    retry=RetryPolicy(max_attempts=3, base=timedelta(milliseconds=200)),
    non_retryable_exceptions=(PermanentSyncError,),
)
async def sync_user_profile(
    payload: SyncUserProfilePayload,
    ctx: JobContext[SyncUserProfilePayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Simulates an external profile-API sync with scripted failure injection."""
    await _record_effect(
        pool,
        ctx,
        "fetch",
        {"run_id": payload.run_id, "user_id": payload.user_id, "attempt": ctx.attempt},
    )
    if ctx.attempt <= payload.fail_times:
        if payload.fail_kind == "permanent":
            raise PermanentSyncError(f"permanent sync failure for user {payload.user_id}")
        if payload.fail_kind == "snooze":
            raise Snooze(timedelta(milliseconds=200))
        raise RuntimeError("simulated fetch failure")
    await asyncio.sleep(payload.fetch_latency_ms / 1000)
    profile = {"user_id": payload.user_id, "display_name": "E2E User", "timezone": "UTC"}
    await _record_effect(
        pool,
        ctx,
        "synced",
        {"run_id": payload.run_id, "user_id": payload.user_id, "profile": profile},
    )


class GenerateReportPayload(BaseModel):
    run_id: str
    report_id: str
    stages: int = Field(default=4, ge=1)
    stage_latency_ms: int = Field(default=300, ge=0)


class ReportResult(BaseModel):
    stages_completed: int


_STAGE_NAMES = ("fetch", "aggregate", "render", "store")


@actor(name="generate_report", queue="e2e")
async def generate_report(
    payload: GenerateReportPayload,
    ctx: JobContext[GenerateReportPayload],
    *,
    pool: asyncpg.Pool,
) -> ReportResult:
    """Staged pipeline with per-stage progress and cooperative cancellation."""
    for i in range(payload.stages):
        ctx.check_cancelled()
        stage_name = _STAGE_NAMES[i % len(_STAGE_NAMES)]
        await ctx.progress(
            step=i + 1,
            percent=round((i + 1) / payload.stages * 100, 1),
            detail=f"stage {i + 1} {stage_name}",
        )
        await asyncio.sleep(payload.stage_latency_ms / 1000)
        await _record_effect(
            pool,
            ctx,
            "stage",
            {"run_id": payload.run_id, "report_id": payload.report_id, "stage": i + 1},
        )
    await _record_effect(
        pool,
        ctx,
        "done",
        {"run_id": payload.run_id, "report_id": payload.report_id},
    )
    return ReportResult(stages_completed=payload.stages)


class ImportContactsChunkPayload(BaseModel):
    run_id: str
    upload_id: str
    chunk_id: int
    start_row: int
    end_row: int


@actor(
    name="import_contacts_chunk",
    queue="e2e",
    retry=RetryPolicy(max_attempts=2, base=timedelta(milliseconds=200)),
)
async def import_contacts_chunk(
    payload: ImportContactsChunkPayload,
    ctx: JobContext[ImportContactsChunkPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Processes one CSV chunk (deterministic, no I/O)."""
    await asyncio.sleep(0.02)
    await _record_effect(
        pool,
        ctx,
        "chunk_done",
        {
            "run_id": payload.run_id,
            "upload_id": payload.upload_id,
            "chunk_id": payload.chunk_id,
            "rows_processed": payload.end_row - payload.start_row,
        },
    )


class ImportContactsPayload(BaseModel):
    run_id: str
    upload_id: str
    rows: int = Field(default=2500, ge=1)
    chunk_size: int = Field(default=500, ge=1)


@actor(name="import_contacts_csv", queue="e2e")
async def import_contacts_csv(
    payload: ImportContactsPayload,
    ctx: JobContext[ImportContactsPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Simulates CSV parse/validate, then fans out chunk jobs as one batch.

    The explicit ``batch_id`` is recorded on the ``dispatched`` effect so
    tests can correlate with ``wait_for_batch``.
    """
    await ctx.progress(step=1, percent=50.0, detail="parse csv")
    await asyncio.sleep(0.02)
    await ctx.progress(step=2, percent=100.0, detail="validate rows")
    await asyncio.sleep(0.02)

    items = [
        EnqueueItem(
            actor_ref=import_contacts_chunk,
            payload=ImportContactsChunkPayload(
                run_id=payload.run_id,
                upload_id=payload.upload_id,
                chunk_id=index,
                start_row=start_row,
                end_row=min(start_row + payload.chunk_size, payload.rows),
            ),
            metadata={"run_id": payload.run_id, "upload_id": payload.upload_id},
        )
        for index, start_row in enumerate(range(0, payload.rows, payload.chunk_size))
    ]
    batch_id = uuid4()
    await ctx.jobs.enqueue_batch(items, batch_id=batch_id)
    await _record_effect(
        pool,
        ctx,
        "dispatched",
        {
            "run_id": payload.run_id,
            "upload_id": payload.upload_id,
            "chunks": len(items),
            "batch_id": str(batch_id),
        },
    )


# Import-time registration is intentional: the worker syncs rate-limit
# buckets from this registry at startup (mirrors examples/actors/ratelimit.py).
registry.register(
    TokenBucket(
        name="e2e_webhook_delivery",
        capacity=5,
        refill_per_second=5.0,
        backend="redis",
    )
)


class DeliverWebhookPayload(BaseModel):
    run_id: str
    endpoint_id: str


@actor(name="deliver_webhook", queue="e2e", rate_limits=["e2e_webhook_delivery"])
async def deliver_webhook(
    payload: DeliverWebhookPayload,
    ctx: JobContext[DeliverWebhookPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Simulates a webhook POST gated by a Redis token bucket (cap 5, refill 5/s)."""
    await asyncio.sleep(0.03)
    await _record_effect(
        pool,
        ctx,
        "delivered",
        {"run_id": payload.run_id, "endpoint_id": payload.endpoint_id},
    )


class RebuildSearchIndexPayload(BaseModel):
    run_id: str
    index_name: str


@actor(name="rebuild_search_index", queue="e2e", unique_for=timedelta(minutes=10))
async def rebuild_search_index(
    payload: RebuildSearchIndexPayload,
    ctx: JobContext[RebuildSearchIndexPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Simulates a search-index rebuild.

    ``unique_for`` dedup fires only when the enqueue also passes an
    ``identity_key`` — that is the test's enqueue-time responsibility.
    """
    await asyncio.sleep(0.05)
    await _record_effect(
        pool,
        ctx,
        "rebuilt",
        {"run_id": payload.run_id, "index_name": payload.index_name},
    )


class EnrichOrderPayload(BaseModel):
    run_id: str
    order_id: str
    fail_fetch: bool = False


class EnrichResult(BaseModel):
    order_id: str
    enriched: bool


@actor(name="enrich_order", queue="e2e")
async def enrich_order(
    payload: EnrichOrderPayload,
    ctx: JobContext[EnrichOrderPayload],
    *,
    http: FakeHttpClient,
    pool: asyncpg.Pool,
) -> EnrichResult:
    """Simulates order enrichment via the DI-injected fake HTTP client.

    ``http`` resolves from the TRANSIENT-scope provider (fresh instance per
    invocation); ``pool`` from the LOOP-scope provider — together they prove
    DI bootstrap inside a real worker container.
    """
    path = f"/orders/{payload.order_id}/enrichment"
    data = await http.get(path)
    if payload.fail_fetch:
        raise RuntimeError("simulated enrichment fetch failure")
    await _record_effect(
        pool,
        ctx,
        "fetch",
        {"run_id": payload.run_id, "order_id": payload.order_id, "path": path},
    )
    await _record_effect(
        pool,
        ctx,
        "enriched",
        {"run_id": payload.run_id, "order_id": payload.order_id, "status": data["status"]},
    )
    return EnrichResult(order_id=payload.order_id, enriched=True)


# ── KeyedRateLimitRef actor ──────────────────────────────────────────────
# Per-tenant token bucket: each tenant gets an independent capacity-3 /
# 1-refill-per-second bucket materialized lazily on first acquisition.
# The key_fn extracts tenant_id from the validated payload, so two tenants
# share NO token budget — draining tenant A's bucket does not affect
# tenant B's bucket at all.


class DeliverTenantWebhookPayload(BaseModel):
    run_id: str
    tenant_id: str
    endpoint_id: str


@actor(
    name="deliver_tenant_webhook",
    queue="e2e",
    rate_limits=[
        KeyedRateLimitRef(
            base_name="e2e_per_tenant",
            key_fn=lambda p: p["tenant_id"],
            capacity=3,
            refill_per_second=1.0,
            backend="redis",
        )
    ],
)
async def deliver_tenant_webhook(
    payload: DeliverTenantWebhookPayload,
    ctx: JobContext[DeliverTenantWebhookPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Simulates a per-tenant webhook POST gated by a keyed token bucket.

    Each ``tenant_id`` gets its own independent capacity-3 / 1-refill-per-second
    bucket (materialized lazily via :class:`KeyedRateLimitRef`). Draining one
    tenant's bucket has no effect on another tenant's bucket — the e2e test
    proves this independence.
    """
    await asyncio.sleep(0.03)
    await _record_effect(
        pool,
        ctx,
        "tenant_delivered",
        {
            "run_id": payload.run_id,
            "tenant_id": payload.tenant_id,
            "endpoint_id": payload.endpoint_id,
        },
    )


# ── Queue concurrency cap actor ──────────────────────────────────────────
# Uses the "e2e_capped" queue, which is created with max_concurrent=2 in
# the queue-concurrency-cap test module's fixture. The worker reads
# max_concurrent at startup and registers a ConcurrencyReservation; the
# dispatch path transparently prepends the queue-cap reservation name
# via _effective_reservations, so this actor needs no rate_limits or
# reservations declaration — the cap is purely queue-level.


class CappedWorkerPayload(BaseModel):
    run_id: str
    job_index: int


@actor(name="capped_worker", queue="e2e_capped")
async def capped_worker(
    payload: CappedWorkerPayload,
    ctx: JobContext[CappedWorkerPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Simulates work on a queue with a fleet-wide concurrency cap.

    Records "started" and "finished" effects so the test can compute the
    maximum number of simultaneously-running jobs and verify it never
    exceeds the queue's ``max_concurrent``.
    """
    await _record_effect(
        pool,
        ctx,
        "capped_started",
        {"run_id": payload.run_id, "job_index": payload.job_index},
    )
    await asyncio.sleep(1.0)
    await _record_effect(
        pool,
        ctx,
        "capped_finished",
        {"run_id": payload.run_id, "job_index": payload.job_index},
    )


# ── Result TTL actor ─────────────────────────────────────────────────────
# Short result_ttl (2 s) so the e2e result-TTL-expiry test can observe the
# sweep clearing the stored result within a reasonable wall-clock budget.


class QuickResultPayload(BaseModel):
    run_id: str
    value: str


class QuickResultResult(BaseModel):
    value: str


class CronHeartbeatPayload(BaseModel):
    run_id: str
    beat: int = 0


@actor(name="cron_heartbeat", queue="e2e")
async def cron_heartbeat(
    payload: CronHeartbeatPayload,
    ctx: JobContext[CronHeartbeatPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Cron-fired marker: records one effect per schedule fire."""
    await _record_effect(
        pool,
        ctx,
        "cron-tick",
        {"run_id": payload.run_id, "beat": payload.beat, "attempt": ctx.attempt},
    )


@actor(
    name="quick_result",
    queue="e2e",
    result_ttl=timedelta(seconds=2),
)
async def quick_result(
    payload: QuickResultPayload,
    ctx: JobContext[QuickResultPayload],
    *,
    pool: asyncpg.Pool,
) -> QuickResultResult:
    """Minimal actor with a short result_ttl for sweep-expiry verification."""
    await asyncio.sleep(0.05)
    await _record_effect(
        pool,
        ctx,
        "quick_result",
        {"run_id": payload.run_id, "value": payload.value},
    )
    return QuickResultResult(value=payload.value)


# ── Slow deliver webhook actor (shutdown drain test) ─────────────────────


class SlowDeliverPayload(BaseModel):
    run_id: str
    endpoint_id: str


@actor(
    name="slow_deliver_webhook",
    queue="e2e",
    retry=RetryPolicy(max_attempts=2, base=timedelta(milliseconds=200)),
)
async def slow_deliver_webhook(
    payload: SlowDeliverPayload,
    ctx: JobContext[SlowDeliverPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Long-running webhook delivery that outlives a SIGTERM grace period.

    Records ``started`` immediately, sleeps 3 s (longer than the shutdown
    drain window), then records ``finished`` — proving whether the worker
    drained or killed the in-flight job.
    """
    await _record_effect(
        pool,
        ctx,
        "started",
        {"run_id": payload.run_id, "endpoint_id": payload.endpoint_id},
    )
    await asyncio.sleep(3.0)
    await _record_effect(
        pool,
        ctx,
        "finished",
        {"run_id": payload.run_id, "endpoint_id": payload.endpoint_id},
    )


# ── Loop-blocker actor (watchdog detector-4 e2e) ─────────────────────────


class LoopBlockerPayload(BaseModel):
    run_id: str
    block_seconds: float = 600.0


@actor(name="loop_blocker_job", queue="e2e")
async def loop_blocker_job(
    payload: LoopBlockerPayload,
    ctx: JobContext[LoopBlockerPayload],
) -> None:
    """Blocks the entire event loop with a synchronous sleep: the only way
    to prove the loop-lag watchdog (detector 4) fires. Never enqueued by
    any module except the watchdog e2e."""
    time.sleep(payload.block_seconds)  # noqa: ASYNC251 # Why: deliberately blocks the event loop to trip the loop-lag watchdog.


# ── Long-running job actor (crash recovery test) ─────────────────────────


class LongRunningPayload(BaseModel):
    run_id: str


@actor(
    name="long_running_job",
    queue="e2e",
    retry=RetryPolicy(max_attempts=3, base=timedelta(seconds=5)),
)
async def long_running_job(
    payload: LongRunningPayload,
    ctx: JobContext[LongRunningPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Job that outlives the 3 s lock lease so a worker crash mid-run
    triggers the expired-lock recovery sweep and a clean re-dispatch.
    """
    await _record_effect(
        pool,
        ctx,
        "started",
        {"run_id": payload.run_id, "attempt": ctx.attempt},
    )
    await asyncio.sleep(30.0)
    await _record_effect(
        pool,
        ctx,
        "finished",
        {"run_id": payload.run_id, "attempt": ctx.attempt},
    )


# ── Short-lived job actor (shutdown drain short-job test) ─────────────────


class ShortJobPayload(BaseModel):
    run_id: str
    label: str = "short"


@actor(name="short_lived_job", queue="e2e")
async def short_lived_job(
    payload: ShortJobPayload,
    ctx: JobContext[ShortJobPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Quick job (~0.5 s) that completes within the shutdown drain grace
    window (cancellation_grace=1.0 + cleanup_grace=1.0 = 2.0 s).

    Records ``started`` immediately, sleeps 0.5 s, then records ``finished``
    — proving the worker drains (not cancels) a short in-flight job on
    SIGTERM.
    """
    await _record_effect(
        pool,
        ctx,
        "started",
        {"run_id": payload.run_id, "label": payload.label},
    )
    await asyncio.sleep(0.5)
    await _record_effect(
        pool,
        ctx,
        "finished",
        {"run_id": payload.run_id, "label": payload.label},
    )


# ── Actor-level max_concurrent test actor ─────────────────────────────────


class ConcurrentTrackedPayload(BaseModel):
    run_id: str
    job_index: int


@actor(name="concurrent_tracked_worker", queue="e2e", max_concurrent=2)
async def concurrent_tracked_worker(
    payload: ConcurrentTrackedPayload,
    ctx: JobContext[ConcurrentTrackedPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Actor with ``max_concurrent=2`` that records started/finished
    effects so the test can compute the maximum number of simultaneously
    running jobs and verify it never exceeds 2.
    """
    await _record_effect(
        pool,
        ctx,
        "ct_started",
        {"run_id": payload.run_id, "job_index": payload.job_index},
    )
    await asyncio.sleep(1.0)
    await _record_effect(
        pool,
        ctx,
        "ct_finished",
        {"run_id": payload.run_id, "job_index": payload.job_index},
    )


# ── Batch abort test actor ───────────────────────────────────────────────


class BatchAbortPayload(BaseModel):
    run_id: str
    should_fail: bool = True


@actor(
    name="batch_abort_worker",
    queue="e2e",
    retry=RetryPolicy(max_attempts=1, base=timedelta(milliseconds=100)),
)
async def batch_abort_worker(
    payload: BatchAbortPayload,
    ctx: JobContext[BatchAbortPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Always fails — used to test batch abort policy."""
    await _record_effect(pool, ctx, "attempt", {"run_id": payload.run_id})
    raise RuntimeError("intentional failure for batch abort test")


# ── Batch finalizer actor ────────────────────────────────────────────────


class FinalizerPayload(BaseModel):
    run_id: str
    batch_id: UUID


@actor(
    name="batch_finalizer",
    queue="e2e",
    retry=RetryPolicy(max_attempts=50, base=timedelta(seconds=1)),
)
async def batch_finalizer(
    payload: FinalizerPayload,
    ctx: JobContext[FinalizerPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Waits for batch children via wait_for_batch, then records a 'finalized' effect."""
    from taskq import wait_for_batch

    status = await wait_for_batch(
        pool,
        payload.batch_id,
        schema=_effects_schema(),
        snooze_interval=timedelta(seconds=2),
    )
    await _record_effect(
        pool,
        ctx,
        "finalized",
        {
            "run_id": payload.run_id,
            "batch_id": str(payload.batch_id),
            "total": status.total,
            "succeeded": status.succeeded,
            "failed": status.failed,
        },
    )


class AbortFinalizerPayload(BaseModel):
    run_id: str
    batch_id: UUID


@actor(
    name="batch_abort_finalizer",
    queue="e2e",
    retry=RetryPolicy(max_attempts=50, base=timedelta(seconds=1)),
    non_retryable_exceptions=(),  # BatchAbortedError is caught, not retried
)
async def batch_abort_finalizer(
    payload: AbortFinalizerPayload,
    ctx: JobContext[AbortFinalizerPayload],
    *,
    pool: asyncpg.Pool,
) -> None:
    """Finalizer for abort-policy batches.

    Catches :class:`BatchAbortedError` from ``wait_for_batch`` so the
    finalizer reaches a terminal *succeeded* state (not a retry-storm)
    even when the batch is aborted.  Records either a ``finalized`` or
    ``aborted`` effect.
    """
    from taskq import wait_for_batch
    from taskq.exceptions import BatchAbortedError

    try:
        status = await wait_for_batch(
            pool,
            payload.batch_id,
            schema=_effects_schema(),
            snooze_interval=timedelta(seconds=2),
        )
        await _record_effect(
            pool,
            ctx,
            "finalized",
            {
                "run_id": payload.run_id,
                "batch_id": str(payload.batch_id),
                "total": status.total,
                "succeeded": status.succeeded,
                "failed": status.failed,
            },
        )
    except BatchAbortedError:
        await _record_effect(
            pool,
            ctx,
            "aborted",
            {
                "run_id": payload.run_id,
                "batch_id": str(payload.batch_id),
            },
        )
