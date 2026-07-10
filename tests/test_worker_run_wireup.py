"""Unit tests for ``make_heartbeat_kwargs`` — the production wire-up.

Tests the heartbeat-spawn wiring surface in isolation: constructs a synthetic
WorkerDeps + InMemoryBackend, calls make_heartbeat_kwargs, and verifies the
returned dict carries a CancelController that satisfies the protocol contract.
"""

import inspect
from datetime import UTC, datetime

import pytest

from taskq._ids import new_uuid
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker.cancel import CancelController
from taskq.worker.deps import WorkerDeps
from taskq.worker.run import make_heartbeat_kwargs
from tests.conftest import _FakePool

_FAKE_DSN = "postgresql://fake:fake@fake:5432/fake"


def _ws(**overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"TASKQ_PG_DSN": _FAKE_DSN}
    for k, v in overrides.items():
        data[f"TASKQ_{k}" if not k.startswith("TASKQ_") else k] = v
    return WorkerSettings.load_from_dict(data)


# ── make_heartbeat_kwargs returns correct shape ────────────────────


@pytest.mark.asyncio
async def test_returns_dict_with_cancel_controller_key() -> None:
    """Returns a dict with exactly one key ``cancel_controller``."""
    worker_id = new_uuid()
    ws = _ws()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    backend = InMemoryBackend(clock=FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC)))

    result = make_heartbeat_kwargs(deps, worker_id, backend)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"cancel_controller", "cancel_wake_event"}


# ── cancel controller satisfies protocol contract ──────────────────


@pytest.mark.asyncio
async def test_value_matches_cancel_controller_protocol() -> None:
    """The ``cancel_controller`` value satisfies the CancelController Protocol.

    Verified via runtime_checkable isinstance check and async-method
    introspection — confirms the returned object has ``run_in_tx`` and
    ``run_post_tx`` as coroutine methods.
    """
    worker_id = new_uuid()
    ws = _ws()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    backend = InMemoryBackend(clock=FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC)))

    result = make_heartbeat_kwargs(deps, worker_id, backend)
    controller = result["cancel_controller"]

    assert isinstance(controller, CancelController)
    assert inspect.iscoroutinefunction(controller.run_in_tx)
    assert inspect.iscoroutinefunction(controller.run_post_tx)


# ── smoke test — wiring did not swap factory arguments ─────────────


class _Recorder:
    def __init__(self) -> None:
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        self.fetch_calls.append((sql, args))
        return []

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return "UPDATE 0"

    def transaction(self) -> "_TransactionCtx":
        return _TransactionCtx()


class _TransactionCtx:
    async def __aenter__(self) -> "_Recorder":
        return _Recorder()

    async def __aexit__(self, *args: object) -> None:
        pass


@pytest.mark.asyncio
async def test_cancel_controller_run_in_tx_polls_cancel_flags() -> None:
    """Smoke test: ``run_in_tx`` issues the SELECT against the mock connection.

    Constructs a WorkerDeps with an empty ActiveJobRegistry so no per-job
    processing occurs — only the initial cancel-flag poll is asserted.
    Verifies the factory arguments (worker_id, deps) were not accidentally
    swapped during wiring.
    """
    worker_id = new_uuid()
    ws = _ws()
    deps = WorkerDeps(  # type: ignore[call-arg]
        settings=ws,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )
    backend = InMemoryBackend(clock=FakeClock(start=datetime(2025, 1, 1, tzinfo=UTC)))

    result = make_heartbeat_kwargs(deps, worker_id, backend)
    controller = result["cancel_controller"]

    recorder = _Recorder()
    await controller.run_in_tx(recorder)  # type: ignore[arg-type] # Why: _Recorder satisfies asyncpg.Connection's fetch/execute/transaction surface at duck-type level; asyncpg Connection is not a Protocol.

    assert len(recorder.fetch_calls) >= 1, (
        "run_in_tx must issue at least one fetch for cancel-flag poll"
    )
    _sql, args = recorder.fetch_calls[0]
    assert args == (worker_id,), "fetch must be parameterized with worker_id"
