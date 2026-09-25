"""Liveness-resume attack pins: the loop x error-class resumption matrix.

The campaign hypothesis (dramatiq #207/#258/#445, bullmq #2466/#2964/#3289/
#3516): the most common production bug in a queue is not a drop or a double
run, it is a loop that STOPS after an error and nobody notices. This module
pins the resumption contract for the loops the matrix audit found UNGUARDED,
in the exact class-4 shape (an exception outside every loop's transient
family: a malformed row's KeyError, a partial migration's AttributeError):

1. ``progress_flush_loop``: before the fix, ``_flush_dirty_set`` was
   awaited with NO backstop at the loop level, so any non-transient shape
   escaping it killed the flush loop for the life of the process. The
   worker's progress telemetry then went silently dark (every later
   ``ctx.progress`` call still buffered in memory, never flushed) while
   every other sibling kept ticking: a functional zombie the stale-loop
   detector cannot see, because the dead loop's tick stops with its death.
   The fix gives the loop the same ``UnexpectedLoopErrorGuard`` backstop
   every other long-lived loop carries: tolerate with a loud, alertable
   record, resume next tick (the dirty buffers are intact, nothing is
   lost), and re-raise deliberately at the cap rather than swallowing a
   real bug forever.

2. ``_prune_loop`` / ``_archive_expiry_loop``: the once-a-day sweeps'
   outer error boundary caught only ``TRANSIENT_PG_ERRORS``, but the pool
   acquire and the ``pg_try_advisory_lock`` probe sit OUTSIDE the attempt
   body's broad failure half. A non-transient shape there (a revoked grant
   on the advisory-lock probe, the classic class-4 surprise) escaped into
   the leader's TaskGroup and tore down the whole worker for a once-a-day
   sweep's blip. The fix adds the same guard backstop, arms the retry
   ladder on it (the attempt re-runs on the backoff cadence, not
   tomorrow's fire), and resets the streak only on a successful attempt.

3. The dispatch poll on a read-only standby window (class 1): the claim
   writes fail with 25006 while reads succeed. The poll must KEEP POLLING
   (drain nothing, retry next tick), keep its liveness ticks fresh, and
   never feed the unexpected-error budget; the heartbeat's failure budget
   (pinned in the heartbeat chaos suite) is the clean worker-level exit.

Every pin drives the REAL loop over doubles at the established seams
(pool stand-ins, scripted backends, module-attribute patches), bounds
every wait, and reads the public alert surface
(``taskq.worker.loop_unexpected_errors_total``) the way an operator would.
"""

import asyncio
import contextlib
import random
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import asyncpg
import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskq._ids import new_uuid
from taskq.progress import _flush as flush_mod
from taskq.progress._buffer import _ProgressBuffer
from taskq.settings import WorkerSettings
from taskq.worker import _leader_sweeps
from taskq.worker import run as run_mod
from taskq.worker._leader_shared import SweepContext
from taskq.worker._leader_sweeps import _archive_expiry_loop, _prune_loop
from taskq.worker.cancel import ActiveJobRegistry
from taskq.worker.deps import WorkerDeps

_UNEXPECTED_COUNTER = "taskq.worker.loop_unexpected_errors_total"

_PG_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"

_WORKER_ID = new_uuid()


def _patch_counter(monkeypatch: pytest.MonkeyPatch) -> InMemoryMetricReader:
    """Swap the guard's counter for a test-metered one; return the reader."""
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    meter = provider.get_meter("attack-liveness-resume")
    import taskq.worker._transient as transient_mod

    monkeypatch.setattr(
        transient_mod,
        "_unexpected_loop_errors",
        meter.create_counter(_UNEXPECTED_COUNTER, unit="1"),
    )
    return reader


def _unexpected_by_loop(reader: InMemoryMetricReader) -> dict[str, int]:
    from taskq.testing.otel import counter_data_points

    return {
        str(dict(p.attributes or {})["loop"]): int(p.value)
        for p in counter_data_points(reader, _UNEXPECTED_COUNTER)
    }


def _patch_budget(monkeypatch: pytest.MonkeyPatch, budget: int) -> None:
    import taskq.worker._transient as transient_mod

    monkeypatch.setattr(transient_mod, "DEFAULT_MAX_CONSECUTIVE_UNEXPECTED", budget)


# ─────────────────────────────────────────────────────────────────────────
# 1. progress_flush_loop: class 4 resumption and the cap.
# ─────────────────────────────────────────────────────────────────────────


def _flushing_pool() -> Any:
    """Pool double whose one conn answers the flush UPDATE with one RETURN
    row per bound job id: a clean successful flush every statement."""

    class _Conn:
        async def fetch(self, *args: object) -> list[dict[str, object]]:
            bound = args[1] if len(args) > 1 else None
            if not isinstance(bound, list):
                return []
            return [{"id": job_id, "progress_seq": 7} for job_id in cast("list[UUID]", bound)]

    class _Acquire:
        async def __aenter__(self) -> _Conn:
            return _Conn()

        async def __aexit__(self, *exc: object) -> None:
            return None

    class _Pool:
        def acquire(self, *, timeout: float | None = None) -> _Acquire:
            return _Acquire()

    return _Pool()


def _dirty_buffer(job_id: UUID) -> _ProgressBuffer:
    buf = _ProgressBuffer(job_id=job_id, base_seq=0, attempt=1)
    buf.pending_seq_delta = 1
    buf.pending_state["step"] = 1
    buf.dirty = True
    return buf


async def _stop_task_bounded(task: asyncio.Task[None]) -> None:
    """Stop a driven loop task: bounded wait, cancel on overrun."""
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_progress_flush_loop_survives_an_unexpected_tick_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-transient shape escaping _flush_dirty_set must NOT kill the
    loop: the guard counts it loudly, the next tick RESUMES (the real
    flush runs, the buffer comes back clean), all within a cadence bound.

    The injected shape is deliberately outside every family the flush
    machinery handles internally (statement errors and pool errors are
    per-batch handled; this AttributeError stands in for the class-4
    surprise: a partial migration's attribute rename meeting a buffer
    built by the previous deployment).
    """
    reader = _patch_counter(monkeypatch)
    job_id = new_uuid()
    buffers: dict[UUID, _ProgressBuffer] = {job_id: _dirty_buffer(job_id)}
    pool = _flushing_pool()

    real_flush = flush_mod._flush_dirty_set
    first_tick_raised = asyncio.Event()

    async def _chaos_flush(*args: Any, **kwargs: Any) -> None:
        if not first_tick_raised.is_set():
            first_tick_raised.set()
            raise AttributeError(
                "'dict' object has no attribute 'pending_seq_delta' (simulated "
                "partial-migration buffer defect)"
            )
        await real_flush(*args, **kwargs)

    monkeypatch.setattr(flush_mod, "_flush_dirty_set", _chaos_flush)

    shutdown = asyncio.Event()
    task = asyncio.create_task(
        flush_mod.progress_flush_loop(
            lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown
        )
    )
    error: BaseException | None = None
    try:
        # The resumed flush is bounded by the loop's own cadence x a small
        # factor: coalesce_interval 0.01, budget 1s (100x).
        async with asyncio.timeout(1.0):
            while buffers[job_id].dirty:  # noqa: ASYNC110  # Why: polling observable buffer state (buffer.dirty) that carries no event to await; bounded by the surrounding asyncio.timeout.
                await asyncio.sleep(0.005)
    finally:
        shutdown.set()
        try:
            # No cancel: the loop must exit CLEANLY through its own while
            # condition - a cancel here would mask the death assertion
            # below.
            async with asyncio.timeout(2.0):
                await task
        except TimeoutError:
            error = TimeoutError("the loop did not exit on shutdown")
        except BaseException as exc:
            error = exc

    assert error is None, f"the loop died on the unexpected shape (pre-fix behavior: {error!r})"
    by_loop = _unexpected_by_loop(reader)
    assert by_loop.get("progress_flush", 0) == 1, (
        "the tolerated unexpected tick must surface exactly once on the "
        f"alertable counter labelled loop=progress_flush, got {by_loop}"
    )
    assert buffers[job_id].dirty is False, "the resumed tick must actually flush the buffer"
    assert buffers[job_id].base_seq == 7, "the resumed tick must adopt the authoritative seq"


async def test_progress_flush_loop_is_fatal_at_the_unexpected_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The backstop must not be so broad it swallows a programming bug
    forever: at the consecutive cap the loop re-raises the ORIGINAL error,
    deliberately fatal (the count-and-surface contract)."""
    _patch_budget(monkeypatch, 3)
    _patch_counter(monkeypatch)
    job_id = new_uuid()
    buffers: dict[UUID, _ProgressBuffer] = {job_id: _dirty_buffer(job_id)}
    pool = _flushing_pool()

    async def _always_broken(*args: Any, **kwargs: Any) -> None:
        raise AttributeError("simulated permanent programming bug in the flush path")

    monkeypatch.setattr(flush_mod, "_flush_dirty_set", _always_broken)

    shutdown = asyncio.Event()
    loop_task = asyncio.create_task(
        flush_mod.progress_flush_loop(
            lambda: pool, "taskq_test", _WORKER_ID, buffers, 0.01, shutdown
        )
    )

    outcome: BaseException | None = None
    try:
        async with asyncio.timeout(5.0):
            await loop_task
    except TimeoutError:
        outcome = TimeoutError("the loop never reached the unexpected-error cap")
    except AttributeError as exc:
        outcome = exc
    finally:
        shutdown.set()
        if not loop_task.done():
            await _stop_task_bounded(loop_task)

    assert isinstance(outcome, AttributeError), (
        f"the guard must re-raise the original error at the cap (a blanket "
        f"log-and-continue would swallow a real bug forever); got {outcome!r}"
    )
    assert buffers[job_id].dirty is True, "the buffers stay dirty; nothing is lost"


# ─────────────────────────────────────────────────────────────────────────
# 2. The once-a-day sweeps: class 4 resumption at the lock-attempt frames.
# ─────────────────────────────────────────────────────────────────────────


def _sweep_settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": _PG_DSN, "TASKQ_SCHEMA_NAME": "liveness_resume"}, validate=False
    )


class _AcquireBrokenPool:
    """Pool double whose acquire raises a NON-transient shape: a managed PG
    revoking the advisory-lock grant (the revoked-grant class-4 surprise).
    Statement/pool errors the loops already classify transient are NOT this
    shape; InsufficientPrivilegeError is permanent until the grant lands."""

    def __init__(self, attempts: list[int]) -> None:
        self._attempts = attempts

    def acquire(self, *, timeout: float | None = None) -> Any:
        self._attempts.append(1)
        raise asyncpg.InsufficientPrivilegeError(
            "permission denied for function pg_try_advisory_lock"
        )


def _sweep_ctx(pool: Any) -> SweepContext:
    deps = WorkerDeps(
        settings=_sweep_settings(),
        dispatcher_pool=pool,
        heartbeat_pool=pool,
        worker_pool=pool,
        notify_conn=None,
        leader_conn=None,
    )
    deps.is_leader.set()
    return SweepContext(
        deps=deps,
        backend=cast("Any", SimpleNamespace()),
        clock=cast("Any", SimpleNamespace()),
        worker_id=new_uuid(),
    )


async def _immediate_sleep(shutdown: asyncio.Event, seconds: float) -> None:
    await asyncio.sleep(0.01)


async def _fast_fire_sleep(
    shutdown: asyncio.Event, next_fire: object, retry_backoff: float | None
) -> bool:
    await asyncio.sleep(0.01)
    return retry_backoff is not None


def _patch_fast_clocks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Collapse the once-a-day waits to the test cadence (timing, not
    behavior): the cron fire wait and the backoff ladder's sleep."""
    monkeypatch.setattr(_leader_sweeps, "_sleep_until_next_attempt", _fast_fire_sleep)
    monkeypatch.setattr(_leader_sweeps, "_sleep_interruptible", _immediate_sleep)


_SWEEP_PARAMS = [
    (_prune_loop, "leader.prune"),
    (_archive_expiry_loop, "leader.archive_expiry"),
]


@pytest.mark.parametrize(("loop_fn", "label"), _SWEEP_PARAMS, ids=["prune", "archive_expiry"])
async def test_daily_sweep_survives_an_unexpected_lock_attempt_error(
    monkeypatch: pytest.MonkeyPatch,
    loop_fn: Any,
    label: str,
) -> None:
    """A non-transient error from the pool acquire / lock-probe frames must
    be counted on the alertable surface and the loop must RESUME (further
    acquire attempts on the retry ladder), not tear down the leader.

    Before the fix the outer boundary caught only TRANSIENT_PG_ERRORS, so
    this shape escaped into the leader TaskGroup and killed the whole
    worker for a once-a-day sweep's blip.
    """
    _patch_fast_clocks(monkeypatch)
    reader = _patch_counter(monkeypatch)
    _patch_budget(monkeypatch, 50)  # the pin stops the loop long before the cap

    attempts: list[int] = []
    ctx = _sweep_ctx(_AcquireBrokenPool(attempts))
    shutdown = asyncio.Event()

    task = asyncio.create_task(loop_fn(ctx, shutdown))

    async with asyncio.timeout(5.0):
        while len(attempts) < 3:  # noqa: ASYNC110  # Why: polling the pool double's attempt counter, which carries no event to await; bounded by the surrounding asyncio.timeout.
            await asyncio.sleep(0.005)
    shutdown.set()
    error: BaseException | None = None
    try:
        async with asyncio.timeout(2.0):
            await task
    except TimeoutError:
        error = TimeoutError("the loop did not stop on shutdown after resuming")
    except BaseException as exc:
        error = exc

    assert error is None, (
        f"the {label} loop died on a non-transient lock-attempt error ({error!r}) "
        "- a once-a-day sweep's blip must not tear down the leader"
    )
    assert len(attempts) >= 3, (
        f"the {label} loop must RESUME after an unexpected lock-attempt error "
        f"(further attempts on the retry ladder); attempts={len(attempts)}"
    )
    by_loop = _unexpected_by_loop(reader)
    assert by_loop.get(label, 0) >= 3, (
        f"every tolerated unexpected error must surface on the alertable "
        f"counter labelled loop={label}, got {by_loop}"
    )


@pytest.mark.parametrize(("loop_fn", "label"), _SWEEP_PARAMS, ids=["prune", "archive_expiry"])
async def test_daily_sweep_is_fatal_at_the_unexpected_budget(
    monkeypatch: pytest.MonkeyPatch,
    loop_fn: Any,
    label: str,
) -> None:
    """The sweep backstop must not swallow a permanent fault forever: at
    the consecutive cap the loop re-raises the ORIGINAL error (the
    deliberate-death contract the other leader loops share)."""
    _patch_fast_clocks(monkeypatch)
    _patch_counter(monkeypatch)
    _patch_budget(monkeypatch, 2)

    ctx = _sweep_ctx(_AcquireBrokenPool([]))
    shutdown = asyncio.Event()

    with pytest.raises(asyncpg.InsufficientPrivilegeError):
        async with asyncio.timeout(5.0):
            await loop_fn(ctx, shutdown)
    # Reaching here IS the assertion: the guard re-raised at the cap
    # instead of retrying the permanent fault forever.


# ─────────────────────────────────────────────────────────────────────────
# 3. Class 1: the dispatch poll on a read-only standby window.
# ─────────────────────────────────────────────────────────────────────────


class _ScriptedBackend:
    """dispatch_batch pops one script item per round: an exception instance
    (raised verbatim, the loop classifies it) or None (a clean empty
    round). Exhausted script = clean empty rounds forever."""

    def __init__(self, script: list[Any]) -> None:
        self.script = list(script)
        self.rounds = 0

    async def dispatch_batch(self, **_kwargs: Any) -> list[Any]:
        self.rounds += 1
        item = self.script.pop(0) if self.script else None
        if item is None:
            return []
        raise item


class _TickRecorder:
    """Liveness stand-in recording every tick: the poll's resumption is
    observable as ticks CONTINUING past the failure window."""

    def __init__(self) -> None:
        self.count = 0

    def tick(self, name: str, *, period: float) -> None:
        self.count += 1

    def forget(self, name: str) -> None:
        return


def _producer_deps(ticks: _TickRecorder) -> SimpleNamespace:
    settings = SimpleNamespace(
        queues=["default"],
        lock_lease=30.0,
        notify_enabled=False,
        poll_interval=0.005,
        notify_poll_interval=0.005,
        max_concurrency=1,
        schema_name="taskq",
        pg_is_pooled=False,
    )
    return SimpleNamespace(
        settings=settings,
        liveness=ticks,
        active_jobs=SimpleNamespace(
            count=lambda: 0,
            intent_count=lambda: 0,
            mark_enqueued=ActiveJobRegistry().mark_enqueued,
        ),
        disowned_jobs=set(),
        dispatcher_pool=SimpleNamespace(),
    )


async def test_dispatch_poll_keeps_polling_through_a_read_only_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """25006 (a standby answering the claim writes during a failover) is
    transient: the producer loop retries next tick, KEEPS TICKING liveness
    through the window, feeds no unexpected-error budget, and claims again
    once the window clears (a clean round lands). Bounded by the loop's
    own cadence x a small factor."""
    reader = _patch_counter(monkeypatch)  # zero increments is the assertion
    read_only = asyncpg.ReadOnlySQLTransactionError(
        "cannot execute INSERT in a read-only transaction"
    )
    # Three failover-window rounds, then the window clears.
    backend = _ScriptedBackend([read_only, read_only, read_only])
    ticks = _TickRecorder()

    local_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=1)
    shutdown = asyncio.Event()
    stop = asyncio.Event()

    async def _stop_after_window() -> None:
        # 3 failing rounds + 1 clean round at a ~5ms cadence: 1s is 200x.
        await asyncio.sleep(1.0)
        stop.set()

    async with asyncio.timeout(10.0):
        await asyncio.gather(
            run_mod.producer_loop(
                _producer_deps(ticks),
                local_queue,
                shutdown,
                stop,
                backend=backend,
                worker_id=_WORKER_ID,
                # Why: uniform is for poll-timing jitter, not cryptography; the same
                # non-crypto use the production module RNG has.
                rng=random.Random(1234),  # noqa: S311
            ),
            _stop_after_window(),
        )

    assert backend.rounds >= 4, (
        f"the poll must keep polling THROUGH the read-only window and claim "
        f"again once it clears; rounds={backend.rounds}"
    )
    assert ticks.count > 3, (
        "liveness ticks must continue past the failure window: a poll that "
        "keeps running but stops ticking is a detector-2 blind spot"
    )
    by_loop = _unexpected_by_loop(reader)
    assert by_loop.get("worker.producer", 0) == 0, (
        "a read-only standby window is transient and must never feed the unexpected-error budget"
    )
