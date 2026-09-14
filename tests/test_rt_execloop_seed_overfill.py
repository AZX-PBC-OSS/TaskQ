"""Red-team: an oversized ``_local_queue_seed`` parks bootstrap forever (E5).

Contract under attack: an oversized seed (``len(seed) > max_concurrency``)
must fail loudly (raise) or be bounded — never park ``_main``'s bootstrap
forever on a full ``local_queue`` with zero consumers started.

Hypothesis (verified against the current tree): ``_main`` seeds the queue
with ``for job in _local_queue_seed: await local_queue.put(job)`` BEFORE the
consumer TaskGroup exists (src/taskq/worker/_bootstrap.py:1225-1231), and
``local_queue`` is ``asyncio.Queue(maxsize=settings.max_concurrency)`` — so
an oversized seed blocks on ``put`` forever, silently, with no validation
anywhere on the path.

Both tests drive the REAL ``_main`` through the same fake-sibling harness
shape used by tests/test_worker_main.py (patched backend / open_worker_deps /
signal install / sibling loops) with ``TASKQ_MAX_CONCURRENCY=1``. The fitted
control (seed of 1) proves the harness reaches the seed put and completes
bootstrap; the oversized attack (seed of 2) then proves the park.
"""

import asyncio
import contextlib
from collections.abc import Generator
from contextlib import ExitStack, contextmanager
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID

from taskq._ids import new_uuid
from taskq.backend._protocol import Backend
from taskq.settings import WorkerSettings
from taskq.testing.actor import FakeBackend, as_backend
from taskq.testing.jobs import make_job_row
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import _main


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_CANCELLATION_GRACE_PERIOD": "30.0",
            "TASKQ_CLEANUP_GRACE_PERIOD": "10.0",
            "TASKQ_TERMINATION_GRACE_PERIOD": "60.0",
            "TASKQ_LOCK_LEASE": "45.0",
            "TASKQ_HEARTBEAT_INTERVAL": "5.0",
            "TASKQ_MAX_CONCURRENCY": "1",
            "TASKQ_HEALTH_ENABLED": "false",
        }
    )


class _StubConn:
    """No-op connection mirroring the conftest _FakePool/_FakeConn shape.

    Why: _main's bootstrap issues real conn calls before the seed put (e.g.
    the queue-cap ``fetch`` at _bootstrap.py:1100); returning empty results
    keeps bootstrap running without I/O, exactly like conftest's _FakeConn.
    """

    async def execute(self, *args: object, **kwargs: object) -> str:
        return "OK"

    async def fetch(self, *args: object, **kwargs: object) -> list[object]:
        return []

    async def fetchrow(self, *args: object, **kwargs: object) -> object | None:
        return None

    async def fetchval(self, *args: object, **kwargs: object) -> object | None:
        return None

    def transaction(self) -> "_StubConnCtx":
        return _StubConnCtx()

    def is_in_transaction(self) -> bool:
        return True


class _StubConnCtx:
    def __init__(self) -> None:
        self._conn = _StubConn()

    async def __aenter__(self) -> _StubConn:
        return self._conn

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class _StubPool:
    """Duck-typed asyncpg.Pool: acquire() yields the no-op connection."""

    def __init__(self) -> None:
        self._conn = _StubConn()

    def acquire(self, timeout: float | None = None) -> _StubConnCtx:
        return _StubConnCtx()


def _stub_deps(settings: WorkerSettings) -> WorkerDeps:
    pool = _StubPool()
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=pool,  # type: ignore[arg-type]  # Why: duck-typed pool stub — the harness never performs I/O; the pinned contract is the seed-put path, not pool fidelity.
        heartbeat_pool=pool,  # type: ignore[arg-type]  # Why: duck-typed pool stub, same as above.
        worker_pool=pool,  # type: ignore[arg-type]  # Why: duck-typed pool stub, same as above.
        notify_conn=None,
        leader_conn=None,
    )


@contextmanager
def _fake_main_harness(
    settings: WorkerSettings,
) -> Generator[None, None, None]:
    """Patch _main's sibling seams so bootstrap runs without real I/O.

    Yields the shutdown_event the fake signal-install sets immediately, so
    once bootstrap gets past the seed put the fake siblings all return and
    _main completes — making "completed within the bound" the observable for
    a bounded/raising fix.
    """
    backend: Backend = as_backend(FakeBackend())
    deps = _stub_deps(settings)
    # The REAL shutdown_event _main creates is only available once
    # install_signal_handlers runs (before the TaskGroup opens); the fake
    # siblings park on it via this holder, exactly like the reference
    # harness in tests/test_worker_main.py.
    captured_shutdown: dict[str, asyncio.Event] = {}
    worker_id = new_uuid()

    def _fake_install(
        loop: asyncio.AbstractEventLoop,
        deps: object,
        worker_id: object,
        sh_ev: asyncio.Event,
        esc_ev: object,
        backend: object,
        holder: list[object],
    ) -> None:
        captured_shutdown["event"] = sh_ev
        sh_ev.set()
        fut: asyncio.Future[int] = loop.create_future()
        fut.set_result(0)
        holder.append(fut)

    async def _park_until_shutdown(*args: object, **kwargs: object) -> None:
        # Accepts any sibling signature: the real siblings take positional and
        # keyword args (deps, worker_id, backend, cancel_controller, ...) that
        # this harness discards — only the park-until-shutdown shape matters.
        await captured_shutdown["event"].wait()

    async def _fake_register(pool: object, s: object) -> UUID:
        return worker_id

    async def _fake_dereg(pool: object, s: object, wid: object) -> None:
        return None

    with ExitStack() as stack:
        stack.enter_context(patch("taskq.worker._bootstrap.PostgresBackend", return_value=backend))
        mock_open = stack.enter_context(patch("taskq.worker._bootstrap.open_worker_deps"))
        mock_open.return_value.__aenter__ = AsyncMock(return_value=deps)
        mock_open.return_value.__aexit__ = AsyncMock(return_value=None)
        stack.enter_context(patch("taskq.worker.run.register_worker", side_effect=_fake_register))
        stack.enter_context(
            patch("taskq.worker._bootstrap.install_signal_handlers", side_effect=_fake_install)
        )
        stack.enter_context(
            patch("taskq.worker._bootstrap.heartbeat_loop", side_effect=_park_until_shutdown)
        )
        stack.enter_context(
            patch("taskq.worker._bootstrap.notify_listener_loop", side_effect=_park_until_shutdown)
        )
        leader_cls = stack.enter_context(patch("taskq.worker._bootstrap.MaintenanceLeader"))
        leader_instance = MagicMock()
        leader_instance.run = AsyncMock(side_effect=_park_until_shutdown)
        leader_cls.return_value = leader_instance
        stack.enter_context(
            patch("taskq.worker.run.producer_loop", side_effect=_park_until_shutdown)
        )
        stack.enter_context(
            patch("taskq.worker.run.consumer_loop_stub", side_effect=_park_until_shutdown)
        )
        stack.enter_context(patch("taskq.worker.run.deregister_worker", side_effect=_fake_dereg))
        yield


async def test_fitted_seed_completes_bootstrap() -> None:
    """Control: seed length == max_concurrency — _main completes bootstrap.

    Proves the harness reaches the seed-put path and that _main returns once
    the seed does not park, so the oversized attack's timeout can only be the
    seed over-fill, not harness breakage."""
    settings = _settings()
    job_a = make_job_row(status="running")
    with _fake_main_harness(settings):
        result = await asyncio.wait_for(
            asyncio.create_task(_main(settings, _local_queue_seed=[job_a])),
            timeout=5.0,
        )
    assert result == 0, (
        f"control: a fitted seed (len == max_concurrency) must let _main "
        f"complete bootstrap and return 0; got {result}"
    )


async def test_oversized_seed_fails_loudly_or_is_bounded() -> None:
    """Seed length > max_concurrency must not park bootstrap forever.

    Today the second ``await local_queue.put(job)`` parks forever — the
    consumers that could drain the queue are created only AFTER the seed
    loop — so _main hangs silently (no validation, no log)."""
    settings = _settings()
    job_a = make_job_row(status="running")
    job_b = make_job_row(status="running")
    with _fake_main_harness(settings):
        task = asyncio.create_task(_main(settings, _local_queue_seed=[job_a, job_b]))
        outcome: str
        try:
            await asyncio.wait_for(task, timeout=3.0)
            outcome = "completed"
        except TimeoutError:
            outcome = "hung"
            with contextlib.suppress(asyncio.CancelledError):
                await task  # let the cancelled bootstrap unwind before asserting
        except Exception as exc:
            # A LOUD raise is the accepted fix shape ("fails loudly"); the
            # fitted-seed control in this file proves the harness itself
            # completes, so a raise here is the seed validation, not noise.
            outcome = f"raised {type(exc).__name__}"
    assert outcome != "hung", (
        "CONTRACT: an oversized _local_queue_seed (len > max_concurrency) must "
        "fail loudly (raise) or be bounded — never park bootstrap forever. "
        "VIOLATION: _main seeds the queue with `for job in seed: "
        "await local_queue.put(job)` BEFORE any consumer exists "
        "(src/taskq/worker/_bootstrap.py:1225-1231) and the queue is "
        "asyncio.Queue(maxsize=max_concurrency), so the second put parked "
        "forever with zero consumers running and no validation on the path — "
        "the wait_for timeout is the proof of the silent hang."
    )
