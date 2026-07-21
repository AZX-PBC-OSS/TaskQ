"""Unit tests for worker_main, _main, register_worker, and deregister_worker.

Wiring shape — all siblings called with correct args.
Single orchestrator entry point — _main MUST NOT call orchestrate_shutdown.
Exit code retrieval from orchestrator_holder.
Signal handler installation order.
Failure isolation — ExceptionGroup propagation.
register_worker / deregister_worker SQL shape and timeout uniformity.
Try/finally cleanup paths.
Test seam — _local_queue_seed injects jobs into consumer stubs.
"""

import asyncio
from collections.abc import Callable, Generator
from contextlib import ExitStack, contextmanager
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend, JobId, JobRow
from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import (
    _main,
    deregister_worker,
    register_worker,
    worker_main,
)

# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture
def settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "30.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "10.0",
            "TASKQ_TERMINATION_GRACE_PERIOD": "60.0",
            "TASKQ_LOCK_LEASE": "45.0",
            "TASKQ_HEARTBEAT_INTERVAL": "5.0",
            "TASKQ_MAX_CONCURRENCY": "2",
        }
    )


# ── Helpers ─────────────────────────────────────────────────────────────


def _stub_deps(settings: WorkerSettings) -> WorkerDeps:
    pool: object = object()
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    return deps


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
        created_at=None,  # type: ignore[arg-type]
        scheduled_at=None,  # type: ignore[arg-type]
        started_at=None,
        finished_at=None,
        last_heartbeat_at=None,
        locked_by_worker=None,
        lock_expires_at=None,
        cancel_requested_at=None,
        cancel_phase=None,  # type: ignore[arg-type]
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


class Harness:
    """Mutable state carrier shared between wiring tests and the harness
    context manager.

    Create via ``_use_test_harness(settings)``, set override attributes
    on the instance, then call ``await _main(settings)``. All patched
    sibling functions update counters and captures on this instance.
    """

    def __init__(
        self,
        settings: WorkerSettings,
        *,
        exit_value: int = 0,
        set_shutdown: bool = True,
    ) -> None:
        self.settings = settings
        self.backend: Backend = cast(Backend, FakeBackend())
        self.worker_id: UUID = new_uuid()
        self.exit_value = exit_value
        self.set_shutdown = set_shutdown

        # ── Call counters ──────────────────────────────────────────────
        self.heartbeat_count = 0
        self.notify_count = 0
        self.leader_count = 0
        self.producer_count = 0
        self.consumer_count = 0
        self.register_count = 0
        self.install_count = 0
        self.dereg_count = 0

        # ── Override points (set after creation, before _main) ─────────
        self.heartbeat_exc: BaseException | None = None
        self.dereg_side_effect: Callable[..., object] | None = None
        self.consumer_side_effect: Callable[..., object] | None = None
        self.install_fn: Callable[..., object] | None = None

        # ── Captures ───────────────────────────────────────────────────
        self.captured_backends: list[Backend] = []
        self.captured_holder: list[object] = []
        self.probe: list[str] = []


def _fake_install_with_holder(
    loop: asyncio.AbstractEventLoop,
    shutdown_event: asyncio.Event,
    holder: list[asyncio.Task[int]],
    exit_value: int = 0,
) -> None:
    """Simulate signal handler: set shutdown_event and populate holder.

    The holder receives a pre-resolved asyncio.Future so
    ``await orchestrator_holder[0]`` completes synchronously.
    """
    shutdown_event.set()
    fut: asyncio.Future[int] = loop.create_future()
    fut.set_result(exit_value)
    holder.append(fut)  # type: ignore[arg-type]


@contextmanager
def _use_test_harness(
    settings: WorkerSettings,
    *,
    exit_value: int = 0,
    set_shutdown: bool = True,
) -> Generator["Harness", None, None]:
    """Shared context manager that patches _main's sibling functions.

    Yields a :class:`Harness` instance with FakeBackend, call counters,
    and override points. All patches are applied on enter and restored
    on exit.
    """
    h = Harness(settings, exit_value=exit_value, set_shutdown=set_shutdown)

    # ── Fake sibling functions (closures over h) ────────────────────────

    async def _fake_register(pool: object, s: WorkerSettings) -> UUID:
        h.register_count += 1
        return h.worker_id

    def _fake_install(
        loop: asyncio.AbstractEventLoop,
        deps: WorkerDeps,
        wid: UUID,
        sh_ev: asyncio.Event,
        esc_ev: asyncio.Event,
        backend: Backend,
        holder: list[asyncio.Task[int]],
    ) -> None:
        if h.install_fn is not None:
            h.install_fn(loop, deps, wid, sh_ev, esc_ev, backend, holder)
            return
        h.install_count += 1
        h.captured_backends.append(backend)
        h.captured_holder.append(holder)
        if h.set_shutdown:
            _fake_install_with_holder(loop, sh_ev, holder, exit_value=h.exit_value)

    async def _fake_heartbeat(
        deps: WorkerDeps, wid: UUID, shutdown: asyncio.Event, **kwargs: object
    ) -> None:
        h.heartbeat_count += 1
        if h.heartbeat_exc is not None:
            raise h.heartbeat_exc

    async def _fake_notify(
        deps: WorkerDeps, backend: Backend, shutdown: asyncio.Event, worker_id: UUID
    ) -> None:
        h.notify_count += 1

    async def _fake_leader_run(shutdown: asyncio.Event) -> None:
        h.leader_count += 1

    async def _fake_producer(*args: object, **kwargs: object) -> None:
        h.producer_count += 1

    async def _fake_consumer(*args: object, **kwargs: object) -> None:
        h.consumer_count += 1
        if h.consumer_side_effect is not None:
            await h.consumer_side_effect(*args, **kwargs)  # pyright: ignore[reportGeneralTypeIssues] # Why: consumer_side_effect is Callable[..., object] (sync or async); runtime callable is async.

    async def _fake_dereg(pool: object, s: WorkerSettings, wid: UUID) -> None:
        h.dereg_count += 1
        if h.dereg_side_effect is not None:
            await h.dereg_side_effect(pool, s, wid)  # pyright: ignore[reportGeneralTypeIssues] # Why: dereg_side_effect is Callable[..., object] (sync or async); runtime callable is async.

    deps = _stub_deps(settings)

    with ExitStack() as stack:
        stack.enter_context(
            patch("taskq.worker._bootstrap.PostgresBackend", return_value=h.backend)
        )

        mock_open = stack.enter_context(patch("taskq.worker._bootstrap.open_worker_deps"))
        mock_open.return_value.__aenter__ = AsyncMock(return_value=deps)
        mock_open.return_value.__aexit__ = AsyncMock(return_value=None)

        stack.enter_context(patch("taskq.worker.run.register_worker", side_effect=_fake_register))
        stack.enter_context(
            patch("taskq.worker._bootstrap.install_signal_handlers", side_effect=_fake_install)
        )
        stack.enter_context(
            patch("taskq.worker._bootstrap.heartbeat_loop", side_effect=_fake_heartbeat)
        )
        stack.enter_context(
            patch("taskq.worker._bootstrap.notify_listener_loop", side_effect=_fake_notify)
        )

        mock_leader_cls = stack.enter_context(patch("taskq.worker._bootstrap.MaintenanceLeader"))
        mock_leader_instance = MagicMock()
        mock_leader_instance.run.side_effect = _fake_leader_run
        mock_leader_cls.return_value = mock_leader_instance

        stack.enter_context(patch("taskq.worker.run.producer_loop", side_effect=_fake_producer))
        stack.enter_context(
            patch("taskq.worker.run.consumer_loop_stub", side_effect=_fake_consumer)
        )
        stack.enter_context(patch("taskq.worker.run.deregister_worker", side_effect=_fake_dereg))

        yield h


# ── wiring shape ────────────────────────────────────────────


async def test_wiring_shape_siblings_called_once(settings: WorkerSettings) -> None:
    """All siblings called exactly once with documented arguments."""
    with _use_test_harness(settings) as h:
        result = await _main(settings)

    assert result == 0
    assert h.register_count == 1
    assert h.install_count == 1
    assert len(h.captured_holder) == 1 and len(h.captured_holder[0]) > 0  # type: ignore[arg-type]
    assert h.heartbeat_count == 1
    assert h.notify_count == 1
    assert h.leader_count == 1
    assert h.producer_count == 1
    assert h.consumer_count == settings.max_concurrency


async def test_wiring_shape_backend_instance_identity(settings: WorkerSettings) -> None:
    """Same backend instance is passed to every sibling (single-source-of-truth DoD)."""
    backend: Backend = cast(Backend, FakeBackend())
    worker_id_val = new_uuid()
    captured_backends: list[Backend] = []

    def _capture_install(
        loop: asyncio.AbstractEventLoop,
        deps: WorkerDeps,
        wid: UUID,
        sh_ev: asyncio.Event,
        esc_ev: asyncio.Event,
        _backend: Backend,
        holder: list[asyncio.Task[int]],
    ) -> None:
        captured_backends.append(_backend)
        _fake_install_with_holder(loop, sh_ev, holder)

    async def _capture_notify(
        deps: WorkerDeps, _backend: Backend, shutdown: asyncio.Event, worker_id: UUID
    ) -> None:
        captured_backends.append(_backend)

    async def _capture_producer(*args: object, **kwargs: object) -> None:
        if "backend" in kwargs:
            captured_backends.append(kwargs["backend"])  # type: ignore[arg-type]

    async def _capture_consumer(*args: object, **kwargs: object) -> None:
        if "backend" in kwargs:
            captured_backends.append(kwargs["backend"])  # type: ignore[arg-type]

    async def _fake_register(pool: object, s: WorkerSettings) -> UUID:
        return worker_id_val

    async def _fake_leader_run(shutdown: asyncio.Event) -> None:
        pass

    deps = _stub_deps(settings)

    with ExitStack() as stack:
        stack.enter_context(patch("taskq.worker._bootstrap.PostgresBackend", return_value=backend))
        mock_open = stack.enter_context(patch("taskq.worker._bootstrap.open_worker_deps"))
        mock_open.return_value.__aenter__ = AsyncMock(return_value=deps)
        mock_open.return_value.__aexit__ = AsyncMock(return_value=None)
        stack.enter_context(patch("taskq.worker.run.register_worker", side_effect=_fake_register))
        stack.enter_context(
            patch("taskq.worker._bootstrap.install_signal_handlers", side_effect=_capture_install)
        )
        stack.enter_context(patch("taskq.worker._bootstrap.heartbeat_loop", new_callable=AsyncMock))
        stack.enter_context(
            patch("taskq.worker._bootstrap.notify_listener_loop", side_effect=_capture_notify)
        )
        mock_leader_cls = stack.enter_context(patch("taskq.worker._bootstrap.MaintenanceLeader"))
        mock_leader_instance = MagicMock()
        mock_leader_instance.run.side_effect = _fake_leader_run
        mock_leader_cls.return_value = mock_leader_instance
        stack.enter_context(patch("taskq.worker.run.producer_loop", side_effect=_capture_producer))
        stack.enter_context(
            patch("taskq.worker.run.consumer_loop_stub", side_effect=_capture_consumer)
        )
        stack.enter_context(patch("taskq.worker.run.deregister_worker", new_callable=AsyncMock))

        result = await _main(settings)

    assert result == 0
    assert len(captured_backends) == 5  # install, notify, producer, 2x consumer
    for i in range(1, len(captured_backends)):
        assert captured_backends[0] is captured_backends[i]


# ── Single orchestrator entry point ─────────────────────────────────


async def test_main_does_not_call_orchestrate_shutdown(settings: WorkerSettings) -> None:
    """_main MUST NOT call orchestrate_shutdown directly."""
    with ExitStack() as stack:
        stack.enter_context(_use_test_harness(settings))
        mock_orch = stack.enter_context(patch("taskq.worker.shutdown.orchestrate_shutdown"))
        result = await _main(settings)

    assert result == 0
    assert mock_orch.call_count == 0


# ── Exit code retrieval from holder ─────────────────────────────────


async def test_exit_code_zero_and_one_from_holder(settings: WorkerSettings) -> None:
    """_main returns holder's task value."""

    async def _run_with_exit(value: int) -> int:
        with _use_test_harness(settings, exit_value=value):
            return await _main(settings)

    assert await _run_with_exit(0) == 0
    assert await _run_with_exit(1) == 1


async def test_exit_code_zero_when_holder_empty(settings: WorkerSettings) -> None:
    """_main returns 0 when orchestrator_holder is empty."""
    with _use_test_harness(settings, set_shutdown=True) as h:

        def _install_no_holder(
            loop: asyncio.AbstractEventLoop,
            deps: WorkerDeps,
            wid: UUID,
            sh_ev: asyncio.Event,
            esc_ev: asyncio.Event,
            backend: Backend,
            holder: list[asyncio.Task[int]],
        ) -> None:
            h.install_count += 1
            h.captured_backends.append(backend)
            h.captured_holder.append(holder)
            sh_ev.set()  # signal shutdown but do NOT append to holder

        h.install_fn = _install_no_holder  # type: ignore[assignment]
        result = await _main(settings)

    assert result == 0


# ── Signal handler installation order ───────────────────────────────


async def test_install_signal_handlers_called_after_deps_and_backend(
    settings: WorkerSettings,
) -> None:
    """install_signal_handlers called after register_worker."""
    import taskq.worker._bootstrap as bootstrap_mod
    import taskq.worker.run as run_mod

    with _use_test_harness(settings) as h:

        def _probe_register(pool: object, s: WorkerSettings) -> UUID:
            h.probe.append("register")
            return h.worker_id

        def _probe_install(
            loop: asyncio.AbstractEventLoop,
            deps: WorkerDeps,
            wid: UUID,
            sh_ev: asyncio.Event,
            esc_ev: asyncio.Event,
            backend: Backend,
            holder: list[asyncio.Task[int]],
        ) -> None:
            h.probe.append("install")
            _fake_install_with_holder(loop, sh_ev, holder)

        # Override the harness-level patches with probe functions
        with ExitStack() as inner:
            inner.enter_context(
                patch.object(run_mod, "register_worker", side_effect=_probe_register)
            )
            inner.enter_context(
                patch.object(bootstrap_mod, "install_signal_handlers", side_effect=_probe_install)
            )
            result = await _main(settings)

    assert result == 0
    assert h.probe == ["register", "install"]


# ── Failure isolation ───────────────────────────────────────────────


async def test_sibling_raises_exceptiongroup_propagates(settings: WorkerSettings) -> None:
    """Sibling raises mid-run: ExceptionGroup propagates."""
    fake_exc = RuntimeError("heartbeat boom")

    with _use_test_harness(settings, set_shutdown=False) as h:
        h.heartbeat_exc = fake_exc

        with pytest.raises(ExceptionGroup) as exc_info:  # type: ignore[reportGeneralTypeIssues]
            await _main(settings)

        eg = exc_info.value
        assert len(eg.exceptions) == 1
        assert eg.exceptions[0] is fake_exc

    assert h.install_count >= 1
    assert h.dereg_count == 1


# ── register_worker SQL shape ───────────────────────────────────────


async def test_register_worker_insert_shape(settings: WorkerSettings) -> None:
    """register_worker issues INSERT with validation, returns UUID4, timeout 2.0."""
    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

    result = await register_worker(mock_pool, settings)

    assert isinstance(result, UUID)
    mock_pool.acquire.assert_called_once_with(timeout=2.0)
    mock_conn.execute.assert_called_once()
    call_args = mock_conn.execute.call_args
    sql: str = call_args[0][0]
    assert 'INSERT INTO "taskq".workers' in sql
    params = call_args[0][1:]
    assert params[0] == result
    assert isinstance(params[1], str)  # hostname
    assert isinstance(params[2], int)  # pid
    assert params[3] == ["default"]


async def test_register_worker_mocked_timeout_raises(settings: WorkerSettings) -> None:
    """register_worker raises on pool acquire timeout."""
    mock_pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(side_effect=TimeoutError("pool exhausted"))
    cm.__aexit__ = AsyncMock()
    mock_pool.acquire.return_value = cm

    with pytest.raises(TimeoutError):
        await register_worker(mock_pool, settings)


async def test_register_worker_oserror_raises(settings: WorkerSettings) -> None:
    """register_worker raises on OSError."""
    mock_pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(side_effect=OSError("connection lost"))
    cm.__aexit__ = AsyncMock()
    mock_pool.acquire.return_value = cm

    with pytest.raises(OSError):
        await register_worker(mock_pool, settings)


# ── deregister_worker runs in try/finally ────────────────────────────


async def test_deregister_worker_called_on_happy_path(settings: WorkerSettings) -> None:
    """deregister_worker called exactly once with registered UUID."""
    with _use_test_harness(settings) as h:
        result = await _main(settings)

    assert result == 0
    assert h.dereg_count == 1


async def test_deregister_worker_called_after_sibling_raises(
    settings: WorkerSettings,
) -> None:
    """deregister_worker STILL called when a sibling raises."""
    with _use_test_harness(settings, set_shutdown=False) as h:
        h.heartbeat_exc = RuntimeError("boom")

        with pytest.raises(ExceptionGroup):
            await _main(settings)

    assert h.dereg_count == 1


# ── deregister_worker timeout uniformity ────────────────────────────


async def test_deregister_worker_delete_shape(settings: WorkerSettings) -> None:
    """deregister_worker uses pool.acquire with timeout 2.0, issues DELETE."""
    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

    wid = new_uuid()
    await deregister_worker(mock_pool, settings, wid)

    mock_pool.acquire.assert_called_once_with(timeout=2.0)
    mock_conn.execute.assert_called_once()
    call_args = mock_conn.execute.call_args
    sql: str = call_args[0][0]
    assert 'DELETE FROM "taskq".workers' in sql
    assert call_args[0][1] == wid


async def test_deregister_worker_returns_on_timeout(settings: WorkerSettings) -> None:
    """deregister_worker logs warning and returns cleanly on timeout."""
    mock_pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(side_effect=TimeoutError("pool exhausted"))
    cm.__aexit__ = AsyncMock()
    mock_pool.acquire.return_value = cm

    # Should not raise.
    await deregister_worker(mock_pool, settings, new_uuid())


async def test_deregister_worker_returns_on_oserror(settings: WorkerSettings) -> None:
    """deregister_worker logs warning and returns cleanly on OSError."""
    mock_pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(side_effect=OSError("connection lost"))
    cm.__aexit__ = AsyncMock()
    mock_pool.acquire.return_value = cm

    # Should not raise.
    await deregister_worker(mock_pool, settings, new_uuid())


# ── Test seam ───────────────────────────────────────────────────────


async def test_local_queue_seed_jobs_consumed(settings: WorkerSettings) -> None:
    """_local_queue_seed jobs are consumed by consumer stubs before _main returns."""
    consumed_jobs: list[UUID] = []
    stop_after: int = 2
    job_a = _make_job(actor="actor_a")
    job_b = _make_job(actor="actor_b")

    async def _consumer_recording(
        deps: WorkerDeps,
        local_queue: asyncio.Queue[JobRow],
        shutdown_event: asyncio.Event,
        *,
        backend: Backend,
        worker_id: UUID,
        stub_work_timeout: float = 60.0,
    ) -> None:
        while not shutdown_event.is_set():
            try:
                job: JobRow = local_queue.get_nowait()
            except asyncio.QueueEmpty:
                shutdown_event.set()
                return
            consumed_jobs.append(job.id)
            nonlocal stop_after
            stop_after -= 1
            if stop_after <= 0:
                shutdown_event.set()
                return

    with _use_test_harness(settings, set_shutdown=False) as h:
        h.consumer_side_effect = _consumer_recording  # type: ignore[assignment]
        result = await _main(settings, _local_queue_seed=[job_a, job_b])

    assert result == 0
    assert len(consumed_jobs) == 2
    assert consumed_jobs == [job_a.id, job_b.id]


# ── Cleanup failure does not mask exit code ─────────────────────────


async def test_cleanup_failure_does_not_mask_shutdown_outcome(
    settings: WorkerSettings,
) -> None:
    """deregister_worker raises internally; _main still returns holder's exit code."""
    with _use_test_harness(settings) as h:

        async def _failing_dereg(pool: object, s: WorkerSettings, wid: UUID) -> None:
            raise OSError("cleanup failed")

        h.dereg_side_effect = _failing_dereg  # type: ignore[assignment]
        result = await _main(settings)

    assert result == 0
    assert h.dereg_count == 1


# ── worker_main process entry point ──────────────────────────────────


def test_worker_main_runs_under_asyncio_runner_and_returns_exit_code(
    settings: WorkerSettings,
) -> None:
    """worker_main wraps _main in an asyncio.Runner and returns its result.

    _main itself is patched out — this test exercises the ``worker_main``
    wrapper (Runner construction/teardown, cron_registry resolution,
    setup_logging call) in isolation from the real bootstrap sequence.
    """
    captured_kwargs: dict[str, object] = {}

    async def _fake_main(
        s: WorkerSettings,
        *,
        actor_registry: object = None,
        _registry: object = None,
        _cron_registry: object = None,
        connections: object = None,
    ) -> int:
        captured_kwargs["actor_registry"] = actor_registry
        captured_kwargs["_registry"] = _registry
        captured_kwargs["_cron_registry"] = _cron_registry
        captured_kwargs["connections"] = connections
        return 7

    with patch("taskq.worker._bootstrap._main", side_effect=_fake_main):
        result = worker_main(settings, cron_registry=[])

    assert result == 7
    assert captured_kwargs["_cron_registry"] == []


def test_worker_main_uses_get_registered_crons_when_cron_registry_omitted(
    settings: WorkerSettings,
) -> None:
    """worker_main falls back to get_registered_crons() when cron_registry is None."""

    async def _fake_main(
        s: WorkerSettings,
        *,
        actor_registry: object = None,
        _registry: object = None,
        _cron_registry: object = None,
        connections: object = None,
    ) -> int:
        return 0

    with (
        patch("taskq.worker._bootstrap._main", side_effect=_fake_main),
        patch("taskq.scheduler.get_registered_crons", return_value=[]) as mock_get_registered_crons,
    ):
        result = worker_main(settings)

    assert result == 0
    mock_get_registered_crons.assert_called_once()


def test_worker_main_forwards_connections_to_main(settings: WorkerSettings) -> None:
    """worker_main(connections=…) forwards the exact WorkerConnections object
    to _main — identity is the contract (managed-identity hook point)."""
    sentinel = WorkerConnections()
    captured: dict[str, object] = {}

    async def _fake_main(
        s: WorkerSettings,
        *,
        actor_registry: object = None,
        _registry: object = None,
        _cron_registry: object = None,
        connections: object = None,
    ) -> int:
        captured["connections"] = connections
        return 0

    with patch("taskq.worker._bootstrap._main", side_effect=_fake_main):
        result = worker_main(settings, cron_registry=[], connections=sentinel)

    assert result == 0
    assert captured["connections"] is sentinel
