# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team: lone-surrogate strings at the progress and result boundaries.

A lone surrogate (e.g. ``"\\udcff"``, exactly what ``os.fsdecode`` of a
non-UTF-8 filename byte produces) is a legal Python ``str`` that can NEVER
be encoded to UTF-8 -- so it can never reach a PG ``text``/``jsonb`` value.
The NUL guard family (``sanitize_nul_str`` / ``check_no_nul_str`` /
``dumps_jsonb_str``) rejects NUL at every boundary; NOTHING rejects
surrogates. Three defects follow, pinned here in the in-memory tier (the
PG tier twins live in tests/test_rt_payload_pg_jsonb.py):

1. ``ctx.progress(detail=...)`` bypasses the publish-time serialization
   that ``data=`` gets (context.py guards only ``data``), so an
   unencodable detail poisons the coalesce buffer and blows up the
   terminal write's ``jsonb_param`` as an unclassifiable TypeError.
2. An actor result dict containing a lone surrogate fails through the
   generic retryable path (orjson TypeError), burning every attempt --
   the exact burn ResultTooLarge exists to prevent (retry.py: "the actor
   already ran -- a re-run returns the same value").
3. An actor exception whose MESSAGE carries a surrogate stores cleanly
   in the in-memory mirror but strands the job on PG (asyncpg DataError
   is a PostgresError subclass, so the terminal-write classification
   misreads it as transient infra) -- the mirror divergence is pinned
   here as the desired observable; the PG failure is the RED twin.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import UUID

import pytest
import structlog
from pydantic import BaseModel

from taskq._ids import new_job_id, new_uuid
from taskq.context import JobContext
from taskq.obs import bind_job_context
from taskq.progress._buffer import _ProgressBuffer
from taskq.settings import WorkerSettings
from taskq.testing.actor import default_actor_config
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend, PassthroughPayload
from taskq.testing.jobs import make_enqueue_args
from taskq.worker._consumer import consume_one_job

if TYPE_CHECKING:
    from taskq.backend._protocol import JobRow

_SURROGATE = "\udcff"
"""A lone low surrogate: legal Python str, impossible to UTF-8 encode,
and exactly what ``os.fsdecode(b'\\xff')`` yields on a surrogateescape
filesystem."""


class _BufDeps:
    """Duck-typed WorkerDeps carrying only the progress-buffer wiring the
    consumer reads (worker pools/redis left unset so no flush/publish
    path runs -- the terminal write alone must decide the outcome)."""

    def __init__(self, buffers: dict[UUID, _ProgressBuffer]) -> None:
        self.progress_buffers = buffers
        self.worker_pool: object | None = None
        self.heartbeat_pool: object | None = None
        self.dispatcher_pool: object | None = None
        self.settings: WorkerSettings | None = None
        self.redis_client: object | None = None
        self.disowned_jobs: set[UUID] = set()


def _make_ctx(
    buffers: dict[UUID, _ProgressBuffer],
) -> JobContext[BaseModel]:
    job_id = new_job_id()
    settings = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "taskq_rt_test"})
    ctx: JobContext[BaseModel] = JobContext(
        job_id=job_id,
        actor="rt_actor",
        queue="default",
        attempt=1,
        worker_id=new_uuid(),
        payload=PassthroughPayload(),  # pyright: ignore[arg-type]  # Why: JobContext is generic over the payload model; PassthroughPayload accepts anything and is never read here.
        jobs=None,  # pyright: ignore[arg-type]  # Why: no sub-enqueuer is wired; progress() never touches it before the publish guard under test.
        log=bind_job_context(
            structlog.get_logger("test"),
            job_id=job_id,
            actor="rt_actor",
            queue="default",
            attempt=1,
            identity_key=None,
            trace_id="",
        ),
        _progress_buffers=buffers,
        _redis_client=None,
        _worker_settings=settings,
        _pending_publish_tasks=None,
    )
    return ctx


# ── Defect 1: progress(detail=...) bypasses the publish-time guard ──────


async def test_progress_data_with_surrogate_raises_at_publish() -> None:
    """GREEN pin of the sibling contract: ``data=`` carrying a lone
    surrogate is rejected by the publish-time serialization (the same
    dumps() call that enforces progress_data_max_bytes), so the poison
    never enters the coalesce buffer."""
    buf = _ProgressBuffer(job_id=new_job_id(), base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {buf.job_id: buf}
    ctx = _make_ctx(buffers)

    with pytest.raises(TypeError, match="surrogates not allowed"):
        await ctx.progress(data={"m": _SURROGATE})

    assert buf.pending_seq_delta == 0, "contract: a rejected publish leaves the buffer untouched"
    assert buf.dirty is False
    assert buf.pending_state == {}


async def test_progress_detail_with_surrogate_must_be_rejected_at_publish() -> None:
    """RED: ``detail=`` with the SAME unencodable string must be rejected
    at publish exactly like ``data=`` -- context.py serializes only
    ``data`` before buffering, so a surrogate detail silently enters
    pending_state and detonates later at the terminal write's jsonb_param
    (an unclassifiable TypeError, not a job failure)."""
    buf = _ProgressBuffer(job_id=new_job_id(), base_seq=0)
    buffers: dict[UUID, _ProgressBuffer] = {buf.job_id: buf}
    ctx = _make_ctx(buffers)

    with pytest.raises(TypeError, match="surrogates not allowed"):
        await ctx.progress(detail=f"step {_SURROGATE} failed")

    assert buf.pending_state == {}, (
        f"contract: an unencodable detail must never reach the buffer; got {buf.pending_state!r}"
    )


async def test_surrogate_detail_must_not_break_the_terminal_write() -> None:
    """RED (end-to-end): an actor that publishes a surrogate detail and
    returns a storable result must still land 'succeeded' with the defect
    visible in the stored progress -- today the poisoned pending_state
    makes mark_succeeded's jsonb_param raise a bare TypeError that
    escapes the terminal-write classification (PostgresError/OSError/
    TimeoutError only), so the job is left running."""
    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))
    # Register the actor so dispatch_batch finds it (mirrors PG's
    # actor_config requirement — candidates come FROM the registry).
    backend.register_actor_config(actor="rt_detail_actor")
    args = make_enqueue_args(
        actor="rt_detail_actor", payload={"value": 1}, scheduled_at=backend._clock.now()
    )  # pyright: ignore[reportPrivateUsage]  # Why: test-only access to the FakeClock-backed InMemoryBackend, the established runner pattern.
    await backend.enqueue(args)
    dispatched = await backend.dispatch_batch(
        backend._worker_id,  # pyright: ignore[reportPrivateUsage]  # Why: test-only access; the in-memory worker id owns the dispatch lease.
        ["default"],
        limit=1,
        lock_lease=timedelta(seconds=60),
    )
    assert len(dispatched) == 1
    job: JobRow = dispatched[0]
    buffers: dict[UUID, _ProgressBuffer] = {}

    async def run_actor(_job: object, ctx: JobContext[BaseModel]) -> dict[str, object]:
        await ctx.progress(detail=f"processing {_SURROGATE}")
        return {"ok": True}

    outcome = await consume_one_job(
        backend,
        job,
        backend._worker_id,  # pyright: ignore[reportPrivateUsage]  # Why: test-only access; matches the worker that holds the dispatch lease.
        run_actor=run_actor,
        actor_config=default_actor_config(),
        payload_type=PassthroughPayload,
        clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)),
        deps=_BufDeps(buffers),  # pyright: ignore[arg-type]  # Why: duck-typed WorkerDeps; the consumer reads only progress_buffers/pool/settings/redis/disowned_jobs off it.
    )
    assert outcome == "succeeded", (
        f"contract: a surrogate progress detail must not break the terminal write; outcome={outcome!r}"
    )
    stored = await backend.get(job.id)
    assert stored is not None
    assert stored.status == "succeeded", (
        f"contract: the job must land succeeded with the detail made visible, not strand; status={stored.status!r}"
    )


# ── Defect 2: unencodable actor result burns every retry attempt ────────


async def test_unencodable_result_is_non_retryable_like_result_too_large() -> None:
    """RED: an actor result containing a lone surrogate is a
    DETERMINISTIC serialization defect -- the actor already ran and a
    re-run returns the same unencodable value -- so it must fail
    non-retryably after exactly ONE execution (ResultTooLarge's exact
    contract, retry.py:351-357). Today the orjson TypeError classifies as
    a generic retryable failure and burns all three attempts."""
    runs: list[int] = []
    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))

    def stub(
        payload: object, ctx: object
    ) -> dict[str, object]:  # Why: runner stub signature is fixed; both params are unused here.
        runs.append(1)
        return {"filename": _SURROGATE}

    backend.register_stub("rt_result_actor", stub)
    args = make_enqueue_args(
        actor="rt_result_actor",
        payload={"value": 1},
        max_attempts=3,
        scheduled_at=backend._clock.now(),  # pyright: ignore[reportPrivateUsage]  # Why: test-only access; the established runner pattern.
    )
    row = await backend.enqueue(args)
    await backend.run_until_drained()

    stored = await backend.get(row.id)
    assert stored is not None
    assert len(runs) == 1, (
        f"contract: a deterministic unencodable result is non-retryable -- one execution, like ResultTooLarge; actor ran {len(runs)} times"
    )
    assert stored.status == "failed", (
        f"contract: the job must be terminally failed, not stranded; status={stored.status!r}"
    )


# ── Defect 3's mirror half: surrogate exception MESSAGE ─────────────────


async def test_mirror_marks_failed_for_surrogate_exception_message() -> None:
    """GREEN pin (in-memory half): an actor raising an exception whose
    message carries a lone surrogate still lands 'failed' with the
    message preserved -- the mirror never UTF-8-encodes, so no strand.
    (The PG twin is RED: the same write raises asyncpg DataError, a
    PostgresError subclass the terminal-write classification misreads as
    transient infra -- see test_rt_payload_pg_jsonb.py.)"""
    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))

    def stub(
        payload: object, ctx: object
    ) -> object:  # Why: runner stub signature is fixed; both params are unused here.
        raise ValueError(f"cannot open file {_SURROGATE}")

    backend.register_stub("rt_exc_actor", stub)
    args = make_enqueue_args(
        actor="rt_exc_actor",
        payload={"value": 1},
        max_attempts=1,
        scheduled_at=backend._clock.now(),
    )  # pyright: ignore[reportPrivateUsage]  # Why: test-only access; the established runner pattern.
    row = await backend.enqueue(args)
    await backend.run_until_drained()

    stored = await backend.get(row.id)
    assert stored is not None
    assert stored.status == "failed", (
        f"contract: a surrogate-bearing exception message must still produce a terminal failure; status={stored.status!r}"
    )
    assert stored.error_class == "ValueError"
    assert stored.error_message is not None and _SURROGATE in stored.error_message, (
        "contract: the mirror preserves the unencodable message verbatim (the PG tier must sanitize instead of stranding)"
    )
