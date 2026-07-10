"""Unit tests for producer_loop_stub and consumer_loop_stub.

— producer exits on producer_stop_event / shutdown_event.
— consumer deregister-ordering and exception paths.
— except-chain ordering (CancelledError not routed to generic Exception).
Race-winner cleanup — no pending-task leaks.
"""

import asyncio
import contextlib
from typing import cast
from unittest.mock import create_autospec
from uuid import UUID

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend, JobId, JobRow
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import consumer_loop_stub, producer_loop_stub


def _stub_deps() -> WorkerDeps:
    settings = WorkerSettings.load_from_dict({})
    pool: object = object()
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )


class _BackendMethods:
    async def mark_succeeded(self, job_id: object, worker_id: object, result: object) -> bool:
        return True

    async def mark_succeeded_with_conn(
        self, conn: object, job_id: object, worker_id: object, result: object
    ) -> bool:
        return True

    async def mark_cancelled(self, job_id: object, worker_id: object) -> bool:
        return True


def _make_backend_mock() -> Backend:
    raw = create_autospec(_BackendMethods, instance=True)
    raw.mark_succeeded.return_value = True  # type: ignore[attr-defined] # Why: create_autospec returns Any; attribute correctly typed at runtime.
    raw.mark_cancelled.return_value = True  # type: ignore[attr-defined]
    return cast(Backend, raw)


def _make_job(
    *,
    job_id: UUID | None = None,
    actor: str = "test_actor",
    queue: str = "default",
    attempt: int = 1,
) -> JobRow:
    if job_id is None:
        job_id = new_uuid()
    return JobRow(
        id=JobId(job_id),
        actor=actor,
        queue=queue,
        identity_key=None,
        fairness_key=None,
        payload={},
        payload_schema_ver=1,
        status="running",
        priority=0,
        attempt=attempt,
        max_attempts=3,
        retry_kind="transient",
        schedule_to_close=None,
        start_to_close=None,
        heartbeat_timeout=None,
        created_at=None,  # type: ignore[arg-type] # Why: not read by stub; None satisfies runtime check.
        scheduled_at=None,  # type: ignore[arg-type]
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=None,  # type: ignore[arg-type] # Why: not read by stub.
        error_class=None,
        error_message=None,
        error_traceback=None,
        progress_state={},
        progress_seq=0,
        result=None,
        result_size_bytes=None,
        result_expires_at=None,
        idempotency_key=None,
        trace_id=None,
        span_id=None,
        metadata={},
        tags=(),
    )


def _install_call_tracker(
    deps: WorkerDeps,
    backend: Backend,
    call_seq: list[str],
    shutdown_event: asyncio.Event | None = None,
) -> None:
    """Wrap backend methods and deregister to record call order into call_seq.

    When shutdown_event is provided, each backend call also sets it so the
    consumer's outer while loop exits after the job's finally runs.
    """

    original_dereg = deps.active_jobs.deregister

    async def _tracked_dereg(entry_id: JobId) -> None:
        await original_dereg(entry_id)
        call_seq.append("deregister")

    deps.active_jobs.deregister = _tracked_dereg  # type: ignore[method-assign] # Why: injecting tracking wrapper for call-order assertion.

    def _tracked_succeeded(*args: object, **kwargs: object) -> bool | None:
        call_seq.append("mark_succeeded")
        if shutdown_event is not None:
            shutdown_event.set()
        return None

    backend.mark_succeeded.side_effect = _tracked_succeeded  # type: ignore[attr-defined]

    def _tracked_cancelled(*args: object, **kwargs: object) -> bool | None:
        call_seq.append("mark_cancelled")
        if shutdown_event is not None:
            shutdown_event.set()
        return None

    backend.mark_cancelled.side_effect = _tracked_cancelled  # type: ignore[attr-defined]


# ── producer exits on events ───────────────────────────────────────


async def test_producer_exits_on_producer_stop_event() -> None:
    """Producer returns when producer_stop_event is set."""
    deps = _stub_deps()
    queue: asyncio.Queue[JobRow] = asyncio.Queue()
    shutdown_event = asyncio.Event()
    producer_stop_event = asyncio.Event()
    backend = _make_backend_mock()
    worker_id = new_uuid()

    producer_stop_event.set()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            producer_loop_stub(
                deps,
                queue,
                shutdown_event,
                producer_stop_event,
                backend=backend,
                worker_id=worker_id,
            ),
        )

    assert not queue.qsize()


async def test_producer_exits_on_shutdown_event() -> None:
    """Producer returns when shutdown_event is set."""
    deps = _stub_deps()
    queue: asyncio.Queue[JobRow] = asyncio.Queue()
    shutdown_event = asyncio.Event()
    producer_stop_event = asyncio.Event()
    backend = _make_backend_mock()
    worker_id = new_uuid()

    shutdown_event.set()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            producer_loop_stub(
                deps,
                queue,
                shutdown_event,
                producer_stop_event,
                backend=backend,
                worker_id=worker_id,
            ),
        )

    assert not queue.qsize()


# ── natural completion ───────────────────────────────────────────


async def test_consumer_natural_completion_deregisters_in_finally() -> None:
    """Natural completion: mark_succeeded shielded before deregister."""
    deps = _stub_deps()
    queue: asyncio.Queue[JobRow] = asyncio.Queue()
    job = _make_job()
    await queue.put(job)

    shutdown_event = asyncio.Event()
    backend = _make_backend_mock()
    call_seq: list[str] = []
    _install_call_tracker(deps, backend, call_seq, shutdown_event)

    worker_id = new_uuid()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            consumer_loop_stub(
                deps,
                queue,
                shutdown_event,
                backend=backend,
                worker_id=worker_id,
                stub_work_timeout=0.01,
            ),
        )

    assert call_seq == ["mark_succeeded", "deregister"]
    assert deps.active_jobs.count() == 0


# ── cooperative cancel ───────────────────────────────────────────


async def test_consumer_cooperative_cancel_deregisters_in_finally() -> None:
    """Cooperative cancel: mark_cancelled shielded before deregister."""
    deps = _stub_deps()
    queue: asyncio.Queue[JobRow] = asyncio.Queue()
    job = _make_job()
    await queue.put(job)

    shutdown_event = asyncio.Event()
    backend = _make_backend_mock()
    call_seq: list[str] = []
    _install_call_tracker(deps, backend, call_seq, shutdown_event)

    worker_id = new_uuid()

    async def _set_cancel() -> None:
        for _ in range(20):
            await asyncio.sleep(0)
        entry = deps.active_jobs.get(job.id)
        if entry is not None:
            entry.ctx.cancel_event.set()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            consumer_loop_stub(
                deps,
                queue,
                shutdown_event,
                backend=backend,
                worker_id=worker_id,
                stub_work_timeout=60.0,
            ),
        )
        tg.create_task(_set_cancel())

    assert call_seq == ["mark_cancelled", "deregister"]


# ── task.cancel() ────────────────────────────────────────────────


async def test_consumer_task_cancel_deregisters_in_finally() -> None:
    """task.cancel() marks cancelled before deregister; CancelledError re-raised."""
    deps = _stub_deps()
    registered: asyncio.Event = asyncio.Event()
    original_register = deps.active_jobs.register

    async def _gate_register(job_id: object, task: object, ctx: object) -> None:
        await original_register(job_id, task, ctx)  # type: ignore[arg-type]
        registered.set()

    deps.active_jobs.register = _gate_register  # type: ignore[method-assign] # Why: injecting gate event for synchronised test cancellation.

    queue: asyncio.Queue[JobRow] = asyncio.Queue()
    job = _make_job()
    await queue.put(job)

    shutdown_event = asyncio.Event()
    backend = _make_backend_mock()
    call_seq: list[str] = []
    _install_call_tracker(deps, backend, call_seq)

    worker_id = new_uuid()

    consumer_task = asyncio.create_task(
        consumer_loop_stub(
            deps,
            queue,
            shutdown_event,
            backend=backend,
            worker_id=worker_id,
            stub_work_timeout=60.0,
        ),
    )

    await registered.wait()
    consumer_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer_task

    assert "mark_cancelled" in call_seq
    assert "deregister" in call_seq
    assert "mark_succeeded" not in call_seq
    assert call_seq.index("mark_cancelled") < call_seq.index("deregister")


# ── except-chain ordering ──────────────────────────────────────


async def test_consumer_except_chain_ordering() -> None:
    """CancelledError triggers mark_cancelled + re-raise, not generic branch."""
    deps = _stub_deps()
    registered: asyncio.Event = asyncio.Event()
    original_register = deps.active_jobs.register

    async def _gate_register(job_id: object, task: object, ctx: object) -> None:
        await original_register(job_id, task, ctx)  # type: ignore[arg-type]
        registered.set()

    deps.active_jobs.register = _gate_register  # type: ignore[method-assign] # Why: injecting gate event for synchronised test cancellation.

    queue: asyncio.Queue[JobRow] = asyncio.Queue()
    job = _make_job()
    await queue.put(job)

    shutdown_event = asyncio.Event()
    backend = _make_backend_mock()
    call_seq: list[str] = []
    _install_call_tracker(deps, backend, call_seq)

    worker_id = new_uuid()

    consumer_task = asyncio.create_task(
        consumer_loop_stub(
            deps,
            queue,
            shutdown_event,
            backend=backend,
            worker_id=worker_id,
            stub_work_timeout=60.0,
        ),
    )

    await registered.wait()
    consumer_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer_task

    assert "mark_cancelled" in call_seq
    assert "deregister" in call_seq
    assert "mark_succeeded" not in call_seq
    assert call_seq.index("mark_cancelled") < call_seq.index("deregister")


# ── Generic exception path ─────────────────────────────────────────────────


async def test_consumer_generic_exception_deregisters_in_finally() -> None:
    """Generic exception from mark_succeeded: deregister still runs in finally."""
    deps = _stub_deps()
    queue: asyncio.Queue[JobRow] = asyncio.Queue()
    job = _make_job()
    await queue.put(job)

    shutdown_event = asyncio.Event()
    backend = _make_backend_mock()
    call_seq: list[str] = []
    _install_call_tracker(deps, backend, call_seq, shutdown_event)

    # Override the mark_succeeded side effect to raise AFTER tracking and setting shutdown.
    def _boom_and_track(*args: object, **kwargs: object) -> None:
        call_seq.append("mark_succeeded")
        shutdown_event.set()
        raise RuntimeError("mark_succeeded failure")

    backend.mark_succeeded.side_effect = _boom_and_track  # type: ignore[attr-defined]

    worker_id = new_uuid()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            consumer_loop_stub(
                deps,
                queue,
                shutdown_event,
                backend=backend,
                worker_id=worker_id,
                stub_work_timeout=0.01,
            ),
        )

    assert call_seq == ["mark_succeeded", "deregister"]
    assert deps.active_jobs.count() == 0


# ── Race-winner cleanup ────────────────────────────────────────────────────


async def test_no_pending_task_leak_producer() -> None:
    """Producer race-winner cleanup: no pending tasks left behind."""
    deps = _stub_deps()
    queue: asyncio.Queue[JobRow] = asyncio.Queue()
    shutdown_event = asyncio.Event()
    producer_stop_event = asyncio.Event()
    backend = _make_backend_mock()
    worker_id = new_uuid()

    loop = asyncio.get_running_loop()
    before = len(asyncio.all_tasks(loop))

    async def _set_later() -> None:
        await asyncio.sleep(0.01)
        producer_stop_event.set()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            producer_loop_stub(
                deps,
                queue,
                shutdown_event,
                producer_stop_event,
                backend=backend,
                worker_id=worker_id,
            ),
        )
        tg.create_task(_set_later())

    after = len(asyncio.all_tasks(loop))
    assert before == after


async def test_no_pending_task_leak_consumer() -> None:
    """Consumer race-winner cleanup: no pending tasks left behind after shutdown."""
    deps = _stub_deps()
    queue: asyncio.Queue[JobRow] = asyncio.Queue()
    shutdown_event = asyncio.Event()
    backend = _make_backend_mock()
    call_seq: list[str] = []
    _install_call_tracker(deps, backend, call_seq, shutdown_event)
    worker_id = new_uuid()

    loop = asyncio.get_running_loop()
    before = len(asyncio.all_tasks(loop))

    job = _make_job()
    await queue.put(job)

    async with asyncio.TaskGroup() as tg:
        tg.create_task(
            consumer_loop_stub(
                deps,
                queue,
                shutdown_event,
                backend=backend,
                worker_id=worker_id,
                stub_work_timeout=0.01,
            ),
        )

    after = len(asyncio.all_tasks(loop))
    assert before == after


async def test_no_pending_task_leak_consumer_cancelled_before_dequeue() -> None:
    """Cancel consumer while blocked on empty queue: no pending tasks leaked."""
    deps = _stub_deps()
    queue: asyncio.Queue[JobRow] = asyncio.Queue()
    shutdown_event = asyncio.Event()
    backend = _make_backend_mock()
    worker_id = new_uuid()

    loop = asyncio.get_running_loop()
    before = len(asyncio.all_tasks(loop))

    consumer_task = asyncio.create_task(
        consumer_loop_stub(
            deps,
            queue,
            shutdown_event,
            backend=backend,
            worker_id=worker_id,
            stub_work_timeout=60.0,
        ),
    )

    await asyncio.sleep(0.01)
    consumer_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer_task

    after = len(asyncio.all_tasks(loop))
    assert before == after
