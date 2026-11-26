"""Unit tests for worker_main, _main, register_worker, and deregister_worker.

Wiring shape - all siblings called with correct args.
Single orchestrator entry point - _main MUST not call orchestrate_shutdown.
Exit code retrieval from orchestrator_holder.
Signal handler installation order.
Failure isolation - ExceptionGroup propagation.
register_worker / deregister_worker SQL shape and timeout uniformity.
Try/finally cleanup paths.
Test seam - _local_queue_seed injects jobs into consumer stubs.
"""

import asyncio
import contextlib
import json
import signal
from collections.abc import Callable, Generator
from contextlib import AsyncExitStack, ExitStack, contextmanager
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

import pytest
import structlog.testing

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend, JobRow
from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend
from taskq.testing.health import unique_health_sock_path
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import (
    _main,
    deregister_worker,
    register_worker,
    worker_main,
    worker_main_async,
)
from tests._stub_job_row import make_job as _make_job
from tests.conftest import _FakePool

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
            # _main starts a real HealthServer - never the shared default path.
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("worker_main"),
        }
    )


# ── Helpers ─────────────────────────────────────────────────────────────


def _sigterm_mask() -> set[int | signal.Signals] | None:
    """Read the host mask where POSIX exposes it (Windows has no mask API)."""
    sigmask = getattr(signal, "pthread_sigmask", None)
    return sigmask(signal.SIG_BLOCK, set()) if sigmask is not None else None


def _stub_deps(settings: WorkerSettings) -> WorkerDeps:
    pool = _FakePool()
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]
        heartbeat_pool=pool,  # type: ignore[arg-type]
        worker_pool=pool,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
        # The bootstrap asserts the incremental stack exists after
        # open_worker_deps yields (it pushes the boot-failure deregister
        # backstop onto it); the real open_worker_deps owns that stack,
        # the harness fake below owns this one the same way.
        _exit_stack=AsyncExitStack(),
    )
    return deps


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
        # Captured from install_signal_handlers so the sibling fakes can
        # park on it (see _park_until_shutdown).
        self.shutdown_event: asyncio.Event | None = None
        # The WorkerDeps instance _main runs against, so tests can assert
        # on state _main configures (e.g. watchdog liveness budgets).
        self.deps: WorkerDeps | None = None
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


def _fake_open_worker_deps_exit(deps: WorkerDeps) -> Callable[[object, object, object], Any]:
    """Build an ``open_worker_deps.__aexit__`` stand-in for a stub deps.

    Mirrors open_worker_deps' real exit: unwind the incremental stack
    (the boot-failure deregister backstop rides it) and null the attribute
    so a late reload_credentials fails fast on a dead stack (see
    open_worker_deps' own finally).
    """

    async def _exit(*args: object) -> None:
        stack = deps._exit_stack
        if stack is not None:
            await stack.aclose()
        deps._exit_stack = None

    return _exit


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

    async def _fake_register(pool: object, s: WorkerSettings, **kwargs: object) -> UUID:
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
        h.shutdown_event = sh_ev
        if h.set_shutdown:
            _fake_install_with_holder(loop, sh_ev, holder, exit_value=h.exit_value)

    async def _park_until_shutdown() -> None:
        """Model a long-lived sibling: keep running until shutdown fires.

        The real siblings never return while the worker is healthy, and the
        sibling contract (watchdog detector 3) treats a clean return while
        no shutdown is in progress as a bug. A fake that returned
        immediately would trip that contract and tear down the TaskGroup,
        so the fakes must park exactly as production loops do.
        """
        if h.shutdown_event is not None:
            await h.shutdown_event.wait()

    async def _fake_heartbeat(
        deps: WorkerDeps, wid: UUID, shutdown: asyncio.Event, **kwargs: object
    ) -> None:
        h.heartbeat_count += 1
        if h.heartbeat_exc is not None:
            raise h.heartbeat_exc
        await _park_until_shutdown()

    async def _fake_notify(
        deps: WorkerDeps, backend: Backend, shutdown: asyncio.Event, worker_id: UUID
    ) -> None:
        h.notify_count += 1
        await _park_until_shutdown()

    async def _fake_leader_run(shutdown: asyncio.Event) -> None:
        h.leader_count += 1
        await _park_until_shutdown()

    async def _fake_producer(*args: object, **kwargs: object) -> None:
        h.producer_count += 1
        await _park_until_shutdown()

    async def _fake_consumer(*args: object, **kwargs: object) -> None:
        h.consumer_count += 1
        if h.consumer_side_effect is not None:
            await h.consumer_side_effect(*args, **kwargs)  # pyright: ignore[reportGeneralTypeIssues] # Why: consumer_side_effect is Callable[..., object] (sync or async); runtime callable is async.
        await _park_until_shutdown()

    async def _fake_dereg(pool: object, s: WorkerSettings, wid: UUID) -> object:
        h.dereg_count += 1
        if h.dereg_side_effect is not None:
            # Propagate the outcome: the TaskGroup's finally gate reads
            # deregister_worker's return (True / False / None), so an
            # override that reports an outcome must reach the gate.
            return await h.dereg_side_effect(pool, s, wid)  # pyright: ignore[reportGeneralTypeIssues] # Why: dereg_side_effect is Callable[..., object] (sync or async); runtime callable is async.
        return None

    deps = _stub_deps(settings)
    h.deps = deps

    with ExitStack() as stack:
        stack.enter_context(
            patch("taskq.worker._bootstrap.PostgresBackend", return_value=h.backend)
        )

        mock_open = stack.enter_context(patch("taskq.worker._bootstrap.open_worker_deps"))
        mock_open.return_value.__aenter__ = AsyncMock(return_value=deps)
        mock_open.return_value.__aexit__ = _fake_open_worker_deps_exit(deps)

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


async def test_maintenance_leader_unconditionally_started_regardless_of_settings() -> None:
    """MaintenanceLeader.run is always started inside _main's TaskGroup -
    there is no settings flag that gates it.  This test varies several
    settings (health_enabled=False, max_concurrency=1) that might plausibly
    be suspected of short-circuiting the leader, and asserts leader_count
    is still exactly 1 - proving the unconditional wiring in
    ``src/taskq/worker/_bootstrap.py``.
    """
    varied_settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "30.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "10.0",
            "TASKQ_TERMINATION_GRACE_PERIOD": "60.0",
            "TASKQ_LOCK_LEASE": "45.0",
            "TASKQ_HEARTBEAT_INTERVAL": "5.0",
            "TASKQ_MAX_CONCURRENCY": "1",
            "TASKQ_HEALTH_ENABLED": "false",
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("worker_main_leader"),
        }
    )
    with _use_test_harness(varied_settings) as h:
        await _main(varied_settings)

    assert h.leader_count == 1


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

    async def _fake_register(pool: object, s: WorkerSettings, **kwargs: object) -> UUID:
        return worker_id_val

    async def _fake_leader_run(shutdown: asyncio.Event) -> None:
        pass

    deps = _stub_deps(settings)

    with ExitStack() as stack:
        stack.enter_context(patch("taskq.worker._bootstrap.PostgresBackend", return_value=backend))
        mock_open = stack.enter_context(patch("taskq.worker._bootstrap.open_worker_deps"))
        mock_open.return_value.__aenter__ = AsyncMock(return_value=deps)
        mock_open.return_value.__aexit__ = _fake_open_worker_deps_exit(deps)
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
    """_main MUST not call orchestrate_shutdown directly."""
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
            sh_ev.set()  # signal shutdown but do not append to holder

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

        def _probe_register(pool: object, s: WorkerSettings, **kwargs: object) -> UUID:
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


async def test_register_worker_metadata_carries_binding_concurrency(
    settings: WorkerSettings,
) -> None:
    """The workers-row metadata reports the capacity the worker runs at.

    ``settings.max_concurrency`` sizes ``local_queue`` and bounds every
    dispatch, so the registration write carries it in the row's metadata
    next to ``notify_enabled`` - a fleet's effective parallelism stays
    queryable from the database without introspection into worker code.
    """
    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

    await register_worker(mock_pool, settings)

    call_args = mock_conn.execute.call_args
    params = call_args[0][1:]
    metadata: dict[str, object] = json.loads(params[6])
    assert metadata["max_concurrency"] == settings.max_concurrency == 2, (
        "the metadata must track settings.max_concurrency - the number that "
        f"sizes local_queue and bounds every dispatch; wrote {metadata}"
    )
    assert "notify_enabled" in metadata
    # the metadata carries `workflow_execution` — the value read (`::boolean`),
    # never a key-existence probe: the key is stamped on every worker's row,
    # the value is the capability.
    assert metadata["workflow_execution"] is False


async def test_register_worker_metadata_carries_workflow_execution_stamp(
    settings: WorkerSettings,
) -> None:
    """The capable boot's stamp: `workflow_execution=True` rides the
    workers row's metadata — the claim fence's execution leg reads it as
    data (one pkey probe per round), so a flow row is claimable only by a
    worker whose boot ran the F3 projection (the definitions imported)."""
    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

    await register_worker(mock_pool, settings, workflow_execution=True)

    params = mock_conn.execute.call_args[0][1:]
    metadata: dict[str, object] = json.loads(params[6])
    assert metadata["workflow_execution"] is True, (
        f"the capable boot must stamp workflow_execution=True; wrote {metadata}"
    )


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


async def test_main_records_the_workflow_capability_on_deps(
    settings: WorkerSettings,
    monkeypatch: Any,
) -> None:
    """_main propagates the boot-resolved workflow capability onto deps.

    The live re-registration repairs restore the row from
    ``deps.workflow_execution_capable`` - if the boot never records the
    resolved capability there, a reaped workflow-capable worker would be
    silently restored as vanilla (the claim fence would park its flow
    rows fail-closed forever) with every other check green. The pin is on
    the boot's assignment, so dropping or mis-scoping it fails here
    instead of downgrading the worker in production.
    """
    import taskq.workflows._worker_execution as _wf_exec

    # Pin the resolution itself, so the assignment is what's under test:
    # a capable boot (the real projection/collectors are the flows tests'
    # subject; empty stubs keep this file's DB-free harness intact) and a
    # vanilla boot must land on deps as the value register_worker saw.
    monkeypatch.setattr(_wf_exec, "workflow_execution_capable", lambda: True)
    monkeypatch.setattr(_wf_exec, "project_workflow_actor_configs", lambda: [])
    monkeypatch.setattr(_wf_exec, "collect_workflow_rate_limits", lambda registry: ([], []))
    monkeypatch.setattr(_wf_exec, "collect_workflow_reservations", lambda registry: ([], []))
    with _use_test_harness(settings) as h:
        await _main(settings)
    assert h.deps is not None
    assert h.deps.workflow_execution_capable is True

    monkeypatch.setattr(_wf_exec, "workflow_execution_capable", lambda: False)
    with _use_test_harness(settings) as h:
        await _main(settings)
    assert h.deps is not None
    assert h.deps.workflow_execution_capable is False


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
    """deregister_worker still called when a sibling raises."""
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
    """deregister_worker logs warning and returns False (unconfirmed) on
    timeout - the row's fate stays unknown, the backstop decides."""
    mock_pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(side_effect=TimeoutError("pool exhausted"))
    cm.__aexit__ = AsyncMock()
    mock_pool.acquire.return_value = cm

    with structlog.testing.capture_logs() as captured:
        outcome = await deregister_worker(mock_pool, settings, new_uuid())
    assert outcome is False, "a swallowed transient failure must report unconfirmed, not None/True"
    warnings = [e for e in captured if e["event"] == "deregister_worker_failed"]
    assert len(warnings) == 1, f"the timeout warning did not fire exactly once: {captured!r}"


async def test_deregister_worker_returns_on_oserror(settings: WorkerSettings) -> None:
    """deregister_worker logs warning and returns False (unconfirmed) on
    a connection loss - never raising into shutdown."""
    mock_pool = MagicMock()
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(side_effect=OSError("connection lost"))
    cm.__aexit__ = AsyncMock()
    mock_pool.acquire.return_value = cm

    with structlog.testing.capture_logs() as captured:
        outcome = await deregister_worker(mock_pool, settings, new_uuid())
    assert outcome is False, "a swallowed connection loss must report unconfirmed"
    warnings = [e for e in captured if e["event"] == "deregister_worker_failed"]
    assert len(warnings) == 1, (
        f"the connection-loss warning did not fire exactly once: {captured!r}"
    )


# ── deregister outcome contract: the finally gate and the backstop retry ──


class _DeleteReportingPool:
    """Stub asyncpg.Pool that drives the REAL deregister_worker.

    Every acquire hands out a conn whose DELETE reports ``DELETE 1``,
    unless a transient outage is armed for the first execute (the
    dominant transient class: pool-acquire timeout / conn loss, a
    ``TimeoutError`` is a TRANSIENT_PG_ERRORS member).
    """

    def __init__(self, *, transient_first_execute: bool = False) -> None:
        self.delete_calls = 0
        self.transient_first_execute = transient_first_execute

    def acquire(self, timeout: float | None = None) -> "_DeleteReportingAcquire":
        return _DeleteReportingAcquire(self)


class _DeleteReportingAcquire:
    """``pool.acquire(timeout=...)`` stand-in: the async-conn context."""

    def __init__(self, pool: _DeleteReportingPool) -> None:
        self._pool = pool

    async def __aenter__(self) -> "_DeleteReportingConn":
        return _DeleteReportingConn(self._pool)

    async def __aexit__(self, *exc: object) -> None:
        return None


class _DeleteReportingConn:
    """Stub asyncpg.Connection whose execute speaks asyncpg's DELETE status.

    The dispatcher pool is read elsewhere in the bootstrap too (the
    migration-ledger probe, the seed clock); those reads degrade on the
    empty answers a duck-typed stub gives, so ``fetch``/``fetchval``
    return them and only the DELETE status is crafted.
    """

    def __init__(self, pool: _DeleteReportingPool) -> None:
        self._pool = pool

    async def execute(self, *args: object, **kwargs: object) -> str:
        sql = args[0] if args else ""
        # Count only the workers DELETE; any other statement (the cron
        # re-enable UPDATE, a seed clock read) is not a deregister
        # attempt and answers with its own benign tag.
        if not (isinstance(sql, str) and "DELETE FROM" in sql and ".workers" in sql):
            return "UPDATE 0"
        self._pool.delete_calls += 1
        if self._pool.transient_first_execute and self._pool.delete_calls == 1:
            raise TimeoutError("pool-acquire timeout simulated at the first deregister")
        return "DELETE 1"

    async def fetch(self, *args: object, **kwargs: object) -> list[object]:
        return []

    async def fetchval(self, *args: object, **kwargs: object) -> None:
        return None


async def _real_deregister(pool: object, s: WorkerSettings, wid: UUID) -> object:
    """Route the harness's deregister seam through the REAL
    deregister_worker (the test module's import is bound before the
    harness patches ``taskq.worker.run.deregister_worker``), so the pins
    below exercise the production swallow-and-report contract, not a
    stand-in for it."""
    return await deregister_worker(pool, s, wid)  # type: ignore[arg-type]


async def test_clean_shutdown_deregisters_once_confirmed_and_clears_flag(
    settings: WorkerSettings,
) -> None:
    """Full clean boot-then-shutdown: the row is deleted exactly once.

    The finally's own deregister is the only attempt on the clean path:
    it reports the DELETE confirmed (``DELETE 1`` -> ``True``), the flag
    clears in the finally, and the deps-teardown backstop must not fire
    at all. A mutation that lets the backstop fire on top of a confirmed
    delete doubles the DELETE for nothing; a mutation that leaves the
    flag set on the clean path is caught by the same two asserts.
    """
    pool = _DeleteReportingPool()
    with _use_test_harness(settings) as h:
        assert h.deps is not None
        h.deps.dispatcher_pool = pool  # type: ignore[assignment]
        h.dereg_side_effect = _real_deregister
        result = await _main(settings)

    assert result == 0
    assert pool.delete_calls == 1
    assert h.deps is not None
    assert h.deps.registered_worker_id is None


async def test_transient_failed_deregister_is_retried_by_the_backstop(
    settings: WorkerSettings,
) -> None:
    """A transient-failed finally-deregister leaves the delete to the backstop.

    The finally's deregister hits a transient failure; the real
    deregister_worker swallows it internally and reports ``False`` without
    raising (the dominant failure class would otherwise clear the flag in
    the finally and strand the row until the staleness sweep). The flag
    stays set, so the deps-teardown backstop genuinely retries the DELETE
    against a pool that has recovered: the second execute is that retry's
    delete, and the retry's confirmation clears the flag.
    """
    pool = _DeleteReportingPool(transient_first_execute=True)
    with _use_test_harness(settings) as h:
        assert h.deps is not None
        h.deps.dispatcher_pool = pool  # type: ignore[assignment]
        h.dereg_side_effect = _real_deregister
        result = await _main(settings)

    assert result == 0
    # The finally's failed attempt, then the backstop's retry - not one
    # attempt whose outcome was discarded, and not a third attempt.
    assert pool.delete_calls == 2
    assert h.deps is not None
    assert h.deps.registered_worker_id is None


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
        slot_freed_event: asyncio.Event | None = None,
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
    """deregister_worker raising in the cleanup never masks the exit code.

    The fake raises on every attempt (it bypasses the real
    deregister_worker's internal transient swallow), so this pins the
    whole log-and-suppress chain: the TaskGroup's finally suppresses the
    first raise, the flag stays set, and the deps-teardown backstop
    retries - which suppresses the identical re-raise. Two attempts, no
    crash, the holder's exit code untouched.
    """
    with _use_test_harness(settings) as h:

        async def _failing_dereg(pool: object, s: WorkerSettings, wid: UUID) -> None:
            raise OSError("cleanup failed")

        h.dereg_side_effect = _failing_dereg  # type: ignore[assignment]
        result = await _main(settings)

    assert result == 0
    assert h.dereg_count == 2


# ── worker_main process entry point ──────────────────────────────────


def test_worker_main_runs_under_asyncio_runner_and_returns_exit_code(
    settings: WorkerSettings,
) -> None:
    """worker_main wraps _main in an asyncio.Runner and returns its result.

    _main itself is patched out - this test exercises the ``worker_main``
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
        pg_credential_provider: object = None,
        rate_limit_registry: object = None,
        until_idle: bool = False,
        idle_settle_window: float | None = None,
        idle_poll_interval: float | None = None,
        idle_max_runtime: float | None = None,
    ) -> int:
        captured_kwargs["actor_registry"] = actor_registry
        captured_kwargs["_registry"] = _registry
        captured_kwargs["_cron_registry"] = _cron_registry
        captured_kwargs["connections"] = connections
        captured_kwargs["rate_limit_registry"] = rate_limit_registry
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
        pg_credential_provider: object = None,
        rate_limit_registry: object = None,
        until_idle: bool = False,
        idle_settle_window: float | None = None,
        idle_poll_interval: float | None = None,
        idle_max_runtime: float | None = None,
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
    to _main - identity is the contract (managed-identity hook point)."""
    sentinel = WorkerConnections()
    captured: dict[str, object] = {}

    async def _fake_main(
        s: WorkerSettings,
        *,
        actor_registry: object = None,
        _registry: object = None,
        _cron_registry: object = None,
        connections: object = None,
        pg_credential_provider: object = None,
        rate_limit_registry: object = None,
        until_idle: bool = False,
        idle_settle_window: float | None = None,
        idle_poll_interval: float | None = None,
        idle_max_runtime: float | None = None,
    ) -> int:
        captured["connections"] = connections
        return 0

    with patch("taskq.worker._bootstrap._main", side_effect=_fake_main):
        result = worker_main(settings, cron_registry=[], connections=sentinel)

    assert result == 0
    assert captured["connections"] is sentinel


# ── Watchdog settings wiring ────────────────────────────────────────


async def test_main_applies_every_documented_watchdog_knob() -> None:
    """Every watchdog knob must reach the object that honours it.

    The guide documents eight tunables. A knob that is documented but not
    plumbed is worse than a missing one: an operator raising a budget on a
    starved host would believe they had widened a terminal detector when
    they had not. Deliberately non-default values, so a hardcoded constant
    cannot pass by coincidence.
    """
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("watchdog_knobs"),
            "TASKQ_MAX_CONCURRENCY": "1",
            "TASKQ_TERMINATION_GRACE_PERIOD": "77.0",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "1.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "1.0",
            # Lease sized so the deliberately-large 111s lag budget sits
            # inside it (111 + 10 < 150): the lag-lease invariant must not
            # reject the non-default knob values under test.
            "TASKQ_LOCK_LEASE": "150.0",
            "TASKQ_WATCHDOG_TICK_GRACE_FACTOR": "9.0",
            "TASKQ_WATCHDOG_STALE_FLOOR": "42.0",
            "TASKQ_WATCHDOG_CHECK_INTERVAL": "3.5",
            "TASKQ_WATCHDOG_LOOP_LAG_BUDGET": "111.0",
            "TASKQ_WATCHDOG_LOOP_LAG_WARN_BUDGET": "4.5",
            "TASKQ_WATCHDOG_LOOP_LAG_STARTUP_GRACE": "222.0",
            "TASKQ_WATCHDOG_DUMP_INTERVAL": "6.5",
        }
    )
    lag_kwargs: dict[str, object] = {}
    shutdown_kwargs: dict[str, object] = {}
    watchdog_loop_kwargs: dict[str, object] = {}

    class _SpyLag:
        def __init__(self, *args: object, **kwargs: object) -> None:
            lag_kwargs.update(kwargs)

        def start(self) -> None:
            pass

        def stop(self) -> None:
            pass

    class _SpyShutdown:
        def __init__(self, *args: object, **kwargs: object) -> None:
            shutdown_kwargs.update(kwargs)

        def start(self) -> None:
            pass

        async def cancel(self) -> None:
            pass

    async def _spy_watchdog_loop(*args: object, **kwargs: object) -> None:
        watchdog_loop_kwargs.update(kwargs)

    with _use_test_harness(settings) as h, ExitStack() as stack:
        stack.enter_context(patch("taskq.worker._bootstrap.LoopLagWatchdog", _SpyLag))
        stack.enter_context(patch("taskq.worker._bootstrap.ShutdownWatchdog", _SpyShutdown))
        stack.enter_context(patch("taskq.worker._bootstrap.loop_watchdog_loop", _spy_watchdog_loop))
        result = await _main(settings)

    assert result == 0
    assert h.deps is not None
    # Staleness budget inputs land on the shared LoopLiveness.
    assert h.deps.liveness.grace_factor == 9.0
    assert h.deps.liveness.stale_floor == 42.0
    # Detector 4 (thread).
    assert lag_kwargs["budget"] == 111.0
    assert lag_kwargs["warn_budget"] == 4.5
    assert lag_kwargs["startup_grace"] == 222.0
    assert lag_kwargs["poll_interval"] == 3.5
    # Detector 1 reuses termination_grace_period as its deadline.
    assert shutdown_kwargs["deadline"] == 77.0
    assert shutdown_kwargs["dump_interval"] == 6.5
    # Detector 2 sweep cadence.
    assert watchdog_loop_kwargs["check_interval"] == 3.5


# ── Watchdog disarm ordering ────────────────────────────────────────


async def test_watchdogs_disarmed_only_after_taskgroup_drains(
    settings: WorkerSettings,
) -> None:
    """The watchdogs must outlive the TaskGroup's exit.

    Detector 1 exists to bound exactly that exit: a sibling that swallows
    its cancellation wedges ``TaskGroup.__aexit__``, which is where the
    original 120 s silent hang happened. Disarming the watchdogs before
    the group drains makes the detector dead code on the only path that
    matters, so the drain window must fall strictly between arming and
    disarming.
    """
    order: list[str] = []

    class _SpyShutdownWatchdog:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def start(self) -> None:
            order.append("armed")

        async def cancel(self) -> None:
            order.append("disarmed")

    class _SpyLagWatchdog:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        def start(self) -> None:
            pass

        def stop(self) -> None:
            order.append("lag-disarmed")

    with _use_test_harness(settings, set_shutdown=False) as h:

        async def _slow_winddown(*args: object, **kwargs: object) -> None:
            # Signal shutdown, then take time to actually wind down - the
            # window in which __aexit__ is waiting and detector 1 must
            # still be armed.
            assert h.shutdown_event is not None
            h.shutdown_event.set()
            await asyncio.sleep(0.05)
            order.append("sibling-drained")

        h.consumer_side_effect = _slow_winddown  # type: ignore[assignment]
        with ExitStack() as stack:
            stack.enter_context(
                patch("taskq.worker._bootstrap.ShutdownWatchdog", _SpyShutdownWatchdog)
            )
            stack.enter_context(patch("taskq.worker._bootstrap.LoopLagWatchdog", _SpyLagWatchdog))
            result = await _main(settings)

    assert result == 0
    assert order.index("sibling-drained") < order.index("disarmed"), (
        f"watchdog disarmed before the group drained: {order}"
    )
    assert order.index("sibling-drained") < order.index("lag-disarmed"), (
        f"lag watchdog disarmed before the group drained: {order}"
    )


# ── until_idle drain monitor wiring ────────────────────────────────────


@contextlib.asynccontextmanager
async def _patch_orchestrate_for_drain(work_seconds: float = 0.0):
    """Patch orchestrate_shutdown in both namespaces.

    drain.py and shutdown.py each import orchestrate_shutdown at module
    level, so both references must be patched for the drain monitor's
    create_task to invoke the mock.
    """

    async def _mock_orchestrate(
        deps: object,
        settings: object,
        worker_id: object,
        shutdown_event: asyncio.Event,
        escalate_event: object,
        *,
        backend: object,
    ) -> int:
        try:
            await asyncio.sleep(work_seconds)
        finally:
            shutdown_event.set()
        return 0

    with (
        patch("taskq.worker.drain.orchestrate_shutdown", side_effect=_mock_orchestrate),
        patch("taskq.worker.shutdown.orchestrate_shutdown", side_effect=_mock_orchestrate),
    ):
        yield


async def test_until_idle_spawns_drain_monitor(settings: WorkerSettings) -> None:
    """_main with until_idle=True spawns the drain monitor as a sibling."""
    with _use_test_harness(settings, set_shutdown=False) as h:
        h.backend.count_active_jobs = AsyncMock(return_value=0)  # type: ignore[attr-defined]
        async with _patch_orchestrate_for_drain():
            result = await _main(
                settings,
                until_idle=True,
                idle_settle_window=0.1,
                idle_poll_interval=0.1,
                idle_max_runtime=5.0,
            )
    assert result == 0
    assert len(h.captured_holder[0]) == 1  # type: ignore[arg-type]


async def test_until_idle_exit_code_3_on_failures(settings: WorkerSettings) -> None:
    """_main with until_idle=True and drain_failures > 0 exits 3."""
    with _use_test_harness(settings, set_shutdown=False) as h:
        h.backend.count_active_jobs = AsyncMock(return_value=0)  # type: ignore[attr-defined]
        assert h.deps is not None
        h.deps.drain_failures = 1
        async with _patch_orchestrate_for_drain():
            result = await _main(
                settings,
                until_idle=True,
                idle_settle_window=0.1,
                idle_poll_interval=0.1,
                idle_max_runtime=5.0,
            )
    assert result == 3


async def test_until_idle_exit_code_4_on_timeout(settings: WorkerSettings) -> None:
    """_main with until_idle=True and idle_max_runtime exceeded exits 4."""
    with _use_test_harness(settings, set_shutdown=False) as h:
        h.backend.count_active_jobs = AsyncMock(return_value=10)  # type: ignore[attr-defined]
        async with _patch_orchestrate_for_drain():
            result = await _main(
                settings,
                until_idle=True,
                idle_settle_window=10.0,
                idle_poll_interval=0.1,
                idle_max_runtime=0.3,
            )
    assert result == 4
    assert len(h.captured_holder[0]) == 1  # type: ignore[arg-type]


async def test_without_until_idle_no_drain_monitor(settings: WorkerSettings) -> None:
    """_main without until_idle does not spawn the drain monitor."""
    with _use_test_harness(settings, set_shutdown=True):
        result = await _main(settings)
    assert result == 0


def test_worker_main_leaves_the_host_signal_disposition_alone(
    settings: WorkerSettings,
) -> None:
    """In-process ``worker_main`` must not mutate the host's SIGTERM
    disposition.

    Why this pin exists: ``worker_main`` must end with a HANDLED
    SIGTERM disposition - an ignored one is correct only for the entrypoints that
    ``sys.exit`` immediately, poison for every other caller. An ignored
    disposition is inherited by every process the host forks from then on
    (exec preserves "ignored", unlike handlers), and the proven failure is
    exact: a test spawning a sleeping child after an in-process
    ``worker_main`` saw that child ignore the helper's SIGTERM, eat the
    full 10s escalation budget, and die to SIGKILL - ``rc == -9`` - on
    every CI lane whose xdist worker had already run one of these
    in-process calls. Reproduced deterministically (the child itself
    reported ``SIG_IGN`` and the 10.0s ladder) before the fix; the drain's
    verdict guard now lives at the entrypoints (the taskq worker command,
    the e2e and system-e2e worker entries), where the next act is the
    process's own death, and this pin holds the library honest.
    """
    disposition_before = signal.getsignal(signal.SIGTERM)
    blocked_before = _sigterm_mask()

    async def _fake_main(*args: object, **kwargs: object) -> int:
        return 0

    with patch("taskq.worker._bootstrap._main", side_effect=_fake_main):
        assert worker_main(settings, cron_registry=[]) == 0

    assert signal.getsignal(signal.SIGTERM) is disposition_before
    if blocked_before is not None:
        assert _sigterm_mask() == blocked_before, (
            "a library caller must come back with its signal mask untouched too: a "
            "blocked SIGTERM is inherited across fork/exec exactly as an ignored "
            "disposition is, leaving a spawned child unable to receive its stop signal"
        )


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="requires POSIX signal masks and asyncio's Unix event loop",
)
def test_worker_main_ignore_sigterm_on_exit_leaves_sigterm_ignored_and_unblocked(
    settings: WorkerSettings,
) -> None:
    """``ignore_sigterm_on_exit=True`` ends with SIGTERM ignored, not blocked.

    The flag's contract for a dedicated worker process: a stop signal landing
    anywhere between the worker returning and ``sys.exit`` - the loop's close,
    the interpreter's exit path, its thread joins - must not replace the
    drain's verdict with ``-15``. Blocking alone would defer that signal into
    the exit and still kill the process, the mask must be restored, and it must
    be restored *after* the ``SIG_IGN`` install or a pending blocked SIGTERM is
    delivered on unblock.
    """

    async def _fake_main(*args: object, **kwargs: object) -> int:
        # The worker's real loop installs a SIGTERM handler; the close is what
        # rewrites the disposition, and this pin is about the ordering around
        # that rewrite.
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, lambda: None)
        return 0

    with patch("taskq.worker._bootstrap._main", side_effect=_fake_main):
        assert worker_main(settings, cron_registry=[], ignore_sigterm_on_exit=True) == 0

    assert signal.getsignal(signal.SIGTERM) is signal.SIG_IGN, (
        "a redundant stop signal after the drain must not kill the process: the "
        "entrypoint's remaining work is its own exit, and the drain's verdict is "
        "the exit status"
    )
    blocked_after = _sigterm_mask()
    if blocked_after is not None:
        assert signal.SIGTERM not in blocked_after, (
            "the guard must unblock SIGTERM before returning: a leaked block is "
            "inherited across fork/exec and the child never sees its stop signal"
        )


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="requires POSIX signal masks and asyncio's Unix event loop",
)
async def test_worker_main_async_ignore_sigterm_on_exit_hands_back_an_ignored_sigterm(
    settings: WorkerSettings,
) -> None:
    """``worker_main_async(..., ignore_sigterm_on_exit=True)`` guards its handover.

    The loop belongs to the caller, so the guard runs where the coroutine
    returns: this loop's own SIGTERM handler is removed (a later ``loop.close()``
    finds nothing to reset to ``SIG_DFL``), the disposition is set to
    ``SIG_IGN``, and the mask is restored. What the caller does next - closing a
    pool, dumping a profile, exiting - then runs with SIGTERM ignored.
    """
    loop = asyncio.get_running_loop()

    async def _fake_main(*args: object, **kwargs: object) -> int:
        loop.add_signal_handler(signal.SIGTERM, lambda: None)
        return 0

    with patch("taskq.worker._bootstrap._main", side_effect=_fake_main):
        assert await worker_main_async(settings, cron_registry=[], ignore_sigterm_on_exit=True) == 0

    assert signal.getsignal(signal.SIGTERM) is signal.SIG_IGN
    blocked_after = _sigterm_mask()
    if blocked_after is not None:
        assert signal.SIGTERM not in blocked_after
    assert loop.remove_signal_handler(signal.SIGTERM) is False, (
        "the loop must no longer hold a SIGTERM handler after the handover: "
        "loop.close() rewrites the disposition of every handler it still holds, "
        "which would put SIG_DFL back over the guard's SIG_IGN"
    )


async def test_worker_main_async_ignore_sigterm_tolerates_unsupported_loop_handlers(
    settings: WorkerSettings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The no-signal-handler loop fallback must still install SIG_IGN.

    Windows' event loops implement ``add_signal_handler`` /
    ``remove_signal_handler`` by raising ``NotImplementedError``. The worker's
    normal installer already treats that as an unsupported optional feature;
    the exit guard must not turn the same platform limitation into a failed
    worker exit before its documented SIG_IGN fallback runs.
    """

    async def _fake_main(*args: object, **kwargs: object) -> int:
        return 0

    class _NoSignalHandlerLoop:
        def remove_signal_handler(self, sig: signal.Signals) -> bool:
            raise NotImplementedError

    monkeypatch.setattr("taskq.worker._bootstrap._main", _fake_main)
    monkeypatch.setattr(
        "taskq.worker._bootstrap.asyncio.get_running_loop",
        lambda: _NoSignalHandlerLoop(),
    )
    # Model the platform without pthread_sigmask as well as the loop without
    # signal handlers. The production fallback explicitly supports both.
    monkeypatch.setattr("taskq.worker._bootstrap.signal.pthread_sigmask", None, raising=False)

    assert await worker_main_async(settings, cron_registry=[], ignore_sigterm_on_exit=True) == 0
    assert signal.getsignal(signal.SIGTERM) is signal.SIG_IGN


# ── Entrypoint verdicts and the guard's documented scope ───────────────


def test_aad_worker_mode_propagates_the_drain_verdict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``examples/fastapi_app/aad.py``'s worker mode must exit with the verdict.

    The example is the Azure/AAD deployment entrypoint operators copy. A
    verdict that is discarded cannot be preserved: ``worker_main``'s ``3``
    (failed jobs in drain mode) and ``4`` (idle-max-runtime exceeded) must
    reach the process exit status.
    """
    import sys
    import types

    from examples.fastapi_app import aad

    class _StubSettings:
        pg_dsn_direct = "postgresql://u:p@localhost/db"
        pg_dsn_pooled = "postgresql://u:p@localhost/db"
        dispatcher_pool_size = 1
        heartbeat_pool_size = 1
        worker_pool_size = 1
        redis_url = None
        schema_name = "public"

    class _StubCredential:
        pass

    azure_aio = types.ModuleType("azure.identity.aio")
    azure_aio.DefaultAzureCredential = _StubCredential  # type: ignore[attr-defined]  # Why: the stub module stands in for the optional azure package.
    azure_pkg = types.ModuleType("azure")
    azure_identity = types.ModuleType("azure.identity")
    monkeypatch.setitem(sys.modules, "azure", azure_pkg)
    monkeypatch.setitem(sys.modules, "azure.identity", azure_identity)
    monkeypatch.setitem(sys.modules, "azure.identity.aio", azure_aio)
    monkeypatch.setattr(aad, "WorkerSettings", types.SimpleNamespace(load=lambda: _StubSettings()))
    monkeypatch.setattr(aad, "worker_main", lambda *a, **kw: 3)
    monkeypatch.setattr(sys, "argv", ["aad.py", "worker"])

    with pytest.raises(SystemExit) as exited:
        aad.main()

    assert exited.value.code == 3, (
        "the worker mode discarded worker_main's exit code: a drain that failed "
        f"jobs (3) exits {exited.value.code!r}, so the container reports success"
    )


_PROCESS_SIGTERM_WITH_MASKED_MAIN = """
import signal, sys, threading, time

signal.signal(signal.SIGTERM, signal.SIG_DFL)


def _straggler() -> None:
    time.sleep(30.0)


threading.Thread(target=_straggler, daemon=True).start()
signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM})
sys.stdout.write("ready\\n")
sys.stdout.flush()
time.sleep(30.0)
"""


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="requires POSIX signal masks",
)
def test_the_exit_guard_mask_is_per_thread() -> None:
    """The documented guard scope is real: a straggler can still take SIGTERM.

    ``ignore_sigterm_on_exit`` masks SIGTERM in the calling thread while
    ``asyncio`` has put ``SIG_DFL`` back on the process-global disposition. A
    process-directed SIGTERM in that window is delivered to any thread that
    does not block it, so the guarantee is scoped to a process with no other
    live threads at exit.
    """
    import subprocess
    import sys
    import time

    child = subprocess.Popen(  # noqa: S603  # Why: fixed interpreter + literal script, no shell, no external input.
        [sys.executable, "-c", _PROCESS_SIGTERM_WITH_MASKED_MAIN],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        assert child.stdout.readline().strip() == "ready", "child never reached its masked state"
        child.send_signal(signal.SIGTERM)
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and child.poll() is None:
            time.sleep(0.01)
        rc = child.poll()
        assert rc is not None, "the child outlived its SIGTERM: no thread took it"
        assert rc == -signal.SIGTERM, (
            f"expected the default action (rc == {int(-signal.SIGTERM)}), got {rc}: a "
            "blocked main thread shielded the process while an unblocked thread "
            "was live, so the guard's per-thread scope claim would be wrong"
        )
    finally:
        if child.poll() is None:  # pragma: no cover - only on a failed expectation
            child.kill()
        child.wait()


#: Drive an entrypoint's own ``__main__`` with the worker bootstrap faked out,
#: and deliver a SIGTERM at the exact instant ``asyncio`` rewrites SIGTERM's
#: disposition to SIG_DFL (``remove_signal_handler`` is what does it). The
#: signal is process-directed: only the guard's mask can keep it from landing
#: on the default disposition.
_LATE_SIGTERM_PRELUDE = """
import asyncio, asyncio.unix_events, os, signal, sys

_sent = False
_orig_remove = asyncio.unix_events._UnixSelectorEventLoop.remove_signal_handler


def _hooked_remove(self, sig):
    global _sent
    res = _orig_remove(self, sig)
    if sig == signal.SIGTERM and not _sent:
        _sent = True
        os.kill(os.getpid(), signal.SIGTERM)
    return res


asyncio.unix_events._UnixSelectorEventLoop.remove_signal_handler = _hooked_remove

import taskq.worker._bootstrap as _bootstrap
import taskq.worker.run as _run


async def _fake_main(*args, **kwargs):
    asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, lambda: None)
    return 0


_bootstrap._main = _fake_main
_run._main = _fake_main

import runpy

try:
    runpy.run_module({module!r}, run_name="__main__")
except SystemExit as exc:
    sys.exit(exc.code)
"""

#: The rewired entrypoints that drive the worker in this process and whose
#: module is importable outside the e2e container image.
_ENTRYPOINT_MODULES = [
    "tests._worker_harness",
    "tests._worker_harness_consv",
    "tests._worker_harness_consv2",
    "tests.system_e2e._kill_entry",
    "tests.system_e2e._wf_entry",
    "tests.system_e2e._worker_entry",
    "tests.system_e2e._p2_saturation_worker",
    "tests.system_e2e._wf_diag_entry",
]


@pytest.mark.skipif(
    not hasattr(signal, "pthread_sigmask"),
    reason="requires POSIX signal masks and asyncio's Unix event loop",
)
@pytest.mark.parametrize("module", _ENTRYPOINT_MODULES)
def test_entrypoint_keeps_its_verdict_when_sigterm_lands_in_the_close(module: str) -> None:
    """A SIGTERM inside the loop's close must not replace the drain verdict.

    Every rewired ``run_worker_to_exit`` /
    ``worker_main_async(ignore_sigterm_on_exit=True)`` entrypoint must survive
    a signal delivered after the drain returned and exit with that verdict
    instead of ``-15``.
    """
    import os
    import subprocess
    import sys

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env = {
        **os.environ,
        # Prefer this worktree's ``src`` so the child imports the checkout
        # under test, not an unrelated parent checkout's installed package.
        "PYTHONPATH": os.pathsep.join([os.path.join(repo_root, "src"), repo_root]),
        "TASKQ_PG_DSN": "postgresql://u:p@localhost/db",
        "TASKQ_ENVIRONMENT": "dev",
        "TASKQ_WATCHDOG_ENABLED": "false",
    }
    proc = subprocess.run(  # noqa: S603  # Why: fixed interpreter + generated script naming a module in this repo.
        [sys.executable, "-c", _LATE_SIGTERM_PRELUDE.format(module=module)],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, (
        f"{module} exited rc={proc.returncode} on a SIGTERM delivered inside the loop "
        f"close: the guard did not preserve the drain verdict. stderr={proc.stderr!r}"
    )
