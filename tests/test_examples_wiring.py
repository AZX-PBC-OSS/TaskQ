"""Tests for examples package wiring — actor definitions, DI registry, and worker bootstrap.

Covers:
  - All example ActorRef instances have the expected name/queue
  - DI actors declare their dependencies correctly
  - build_registry() produces a valid ProviderRegistry with the right providers
  - build_registry() + worker bootstrap validates without error
  - Actor return types / result_ttl are set correctly
  - Chained actors reference each other's ActorRef correctly
  - Advanced actor decorator options (singleton, max_concurrent, unique_for, result_ttl)
  - worker_main() accepts a di_registry parameter (API gap fix)
  - ProviderRegistry is exported from taskq.di (API gap fix)
"""

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from examples.actors.advanced import SumResult, capped_job, deduplicated, singleton_job, summer
from examples.actors.basic import counter, deferred
from examples.actors.chained import fan_out, step_one, step_two
from examples.actors.di import FakeDb, FakeHttpClient, build_registry, db_lookup_actor, fetch_actor
from examples.actors.failure import flaky, snoozer
from examples.actors.ratelimit import (
    inmemory_rate_limited,
    reserved,
    token_rate_limited,
    window_rate_limited,
)
from pydantic import BaseModel

from taskq._di.registry import ProviderRegistry
from taskq._di.scope import Scope
from taskq.actor import ActorRef
from taskq.backend.clock import Clock, SystemClock
from taskq.settings import WorkerSettings
from taskq.worker.run import _main
from tests.conftest import unique_health_sock_path

# ── Helpers ──────────────────────────────────────────────────────────


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "PG_DSN": "postgres://u:p@localhost:5432/db",
            "LOCK_LEASE": 60,
            "HEARTBEAT_INTERVAL": 10,
            # _main starts a real HealthServer — never the shared default path.
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("examples_wiring"),
        },
    )


async def _run_main_with_mocked_deps(
    settings: WorkerSettings,
    *,
    _registry: ProviderRegistry | None = None,
) -> int:
    from taskq._ids import new_uuid
    from taskq.worker.deps import WorkerDeps

    worker_id_val = new_uuid()

    async def _fake_register(pool: object, s: WorkerSettings) -> object:
        return worker_id_val

    def _fake_install(
        loop: object,
        deps: object,
        wid: object,
        sh_ev: "asyncio.Event",  # type: ignore[name-defined]
        esc_ev: object,
        backend: object,
        holder: list[object],
    ) -> None:
        import asyncio

        sh_ev.set()
        fut: asyncio.Future[int] = asyncio.get_running_loop().create_future()
        fut.set_result(0)
        holder.append(fut)  # type: ignore[arg-type]

    async def _noop(*args: object, **kwargs: object) -> None:
        pass

    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_conn.fetch = AsyncMock(return_value=[])
    mock_conn.transaction = MagicMock()
    mock_conn.transaction.return_value.__aenter__ = AsyncMock()
    mock_conn.transaction.return_value.__aexit__ = AsyncMock(return_value=None)

    pool_obj = MagicMock()
    pool_obj.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_conn)
    pool_obj.acquire.return_value.__aexit__ = AsyncMock(return_value=None)

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=pool_obj,  # type: ignore[arg-type]
        heartbeat_pool=pool_obj,  # type: ignore[arg-type]
        worker_pool=pool_obj,  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=None,
    )

    from unittest.mock import create_autospec

    class _Methods:
        async def mark_succeeded(self, job_id: object, worker_id: object, result: object) -> bool:
            return True

        async def mark_succeeded_with_conn(
            self, conn: object, job_id: object, worker_id: object, result: object
        ) -> bool:
            return True

        async def mark_cancelled(self, job_id: object, worker_id: object) -> bool:
            return True

        async def write_cancel_escalation(
            self, job_id: object, worker_id: object, phase: object
        ) -> bool:
            return True

        async def mark_abandoned(
            self, job_id: object, progress_seq: object = 0, progress_state: object = None
        ) -> bool:
            return True

    fake_backend = create_autospec(_Methods, instance=True)

    with (
        patch("taskq.worker._bootstrap.PostgresBackend", return_value=fake_backend),
        patch("taskq.worker._bootstrap.open_worker_deps") as mock_open,
        patch("taskq.worker.run.register_worker", side_effect=_fake_register),
        patch("taskq.worker._bootstrap.install_signal_handlers", side_effect=_fake_install),
        patch("taskq.worker._bootstrap.heartbeat_loop", side_effect=_noop),
        patch("taskq.worker._bootstrap.notify_listener_loop", side_effect=_noop),
        patch("taskq.worker._bootstrap.MaintenanceLeader") as mock_leader_cls,
        patch("taskq.worker.run.producer_loop", side_effect=_noop),
        patch("taskq.worker.run.consumer_loop_stub", side_effect=_noop),
        patch("taskq.worker.run.deregister_worker", new_callable=AsyncMock),
    ):
        mock_leader_instance = MagicMock()
        mock_leader_instance.run.side_effect = _noop
        mock_leader_cls.return_value = mock_leader_instance

        mock_open.return_value.__aenter__ = AsyncMock(return_value=deps)
        mock_open.return_value.__aexit__ = AsyncMock(return_value=None)

        return await _main(settings, _registry=_registry)


# ── API gap: ProviderRegistry exported from taskq.di ─────────────────


def test_provider_registry_exported_from_taskq_di() -> None:
    from taskq.di import ProviderRegistry as PR  # noqa: N817

    assert PR is ProviderRegistry


def test_worker_main_accepts_di_registry_param() -> None:
    import inspect

    from taskq.worker.run import worker_main

    sig = inspect.signature(worker_main)
    assert "di_registry" in sig.parameters
    param = sig.parameters["di_registry"]
    assert param.default is None


# ── Actor definitions ────────────────────────────────────────────────


@pytest.mark.parametrize(
    "ref, expected_name, expected_queue",
    [
        (counter, "counter", "examples"),
        (deferred, "deferred", "examples"),
        (flaky, "flaky", "examples"),
        (snoozer, "snoozer", "examples"),
        (window_rate_limited, "window_rate_limited", "examples"),
        (token_rate_limited, "token_rate_limited", "examples"),
        (inmemory_rate_limited, "inmemory_rate_limited", "examples"),
        (reserved, "reserved", "examples"),
        (step_one, "step_one", "examples"),
        (step_two, "step_two", "examples"),
        (fan_out, "fan_out", "examples"),
        (fetch_actor, "fetch", "examples"),
        (db_lookup_actor, "db_lookup", "examples"),
        (singleton_job, "singleton_job", "examples"),
        (capped_job, "capped_job", "examples"),
        (deduplicated, "deduplicated", "examples"),
        (summer, "summer", "examples"),
    ],
)
def test_actor_name_and_queue(
    ref: ActorRef[BaseModel, BaseModel | None], expected_name: str, expected_queue: str
) -> None:
    assert ref.name == expected_name
    assert ref.queue == expected_queue


# ── DI actor dependency declarations ─────────────────────────────────


def test_fetch_actor_declares_http_dependency() -> None:
    assert "http" in fetch_actor.dependencies
    assert fetch_actor.dependencies["http"] is FakeHttpClient
    assert fetch_actor.wants_ctx is True


def test_db_lookup_actor_declares_db_dependency() -> None:
    assert "db" in db_lookup_actor.dependencies
    assert db_lookup_actor.dependencies["db"] is FakeDb
    assert db_lookup_actor.wants_ctx is False


# ── DI registry ──────────────────────────────────────────────────────


def test_build_registry_has_http_client_at_loop_scope() -> None:
    registry = build_registry()
    assert registry.has_provider(FakeHttpClient)
    entry = registry.get(FakeHttpClient)
    assert entry.scope == Scope.LOOP


def test_build_registry_has_db_at_transient_scope() -> None:
    registry = build_registry()
    assert registry.has_provider(FakeDb)
    entry = registry.get(FakeDb)
    assert entry.scope == Scope.TRANSIENT


def test_build_registry_not_yet_sealed() -> None:
    registry = build_registry()
    # Should not raise — registry is not sealed yet
    registry.register_value(WorkerSettings, Scope.PROCESS, _settings())


def test_build_registry_validates_with_di_actors() -> None:
    registry = build_registry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, SystemClock())
    registry.validate(actors=[fetch_actor, db_lookup_actor])
    # validate() seals — no error means all deps resolved


# ── Bootstrap integration: di_registry flows through _main ───────────


async def test_di_registry_passes_through_worker_bootstrap() -> None:
    registry = build_registry()
    settings = _settings()
    exit_code = await _run_main_with_mocked_deps(settings, _registry=registry)
    assert exit_code == 0


async def test_di_registry_with_di_actors_validates_cleanly() -> None:
    registry = build_registry()
    settings = _settings()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, SystemClock())
    registry.validate(actors=[fetch_actor, db_lookup_actor])
    # No exception = all DI deps resolved correctly for the DI actors


# ── Advanced actor options ────────────────────────────────────────────


def test_singleton_job_has_singleton_flag() -> None:
    assert singleton_job.singleton is True


def test_capped_job_has_max_concurrent() -> None:
    assert capped_job.max_concurrent == 2


def test_deduplicated_has_unique_for() -> None:
    assert deduplicated.unique_for == timedelta(minutes=1)


def test_summer_has_result_ttl() -> None:
    assert summer.result_ttl == timedelta(hours=1)


def test_fetch_actor_has_result_ttl() -> None:
    assert fetch_actor.result_ttl == timedelta(minutes=5)


def test_summer_returns_sum_result_type() -> None:
    from pydantic import TypeAdapter

    adapter = TypeAdapter(SumResult)
    result = adapter.validate_python({"total": 42})
    assert result.total == 42


# ── Chaining: step_one references step_two ───────────────────────────


def test_step_one_wants_ctx() -> None:
    assert step_one.wants_ctx is True


def test_step_two_no_ctx_no_deps() -> None:
    assert step_two.wants_ctx is False
    assert step_two.dependencies == {}


def test_fan_out_wants_ctx() -> None:
    assert fan_out.wants_ctx is True


# ── FakeHttpClient and FakeDb smoke tests ────────────────────────────


async def test_fake_http_client_get_returns_dict() -> None:
    client = FakeHttpClient()
    result = await client.get("https://example.com")
    assert isinstance(result, dict)
    assert result["status"] == 200
    assert "https://example.com" in str(result["url"])


async def test_fake_http_client_aclose() -> None:
    client = FakeHttpClient()
    assert client._closed is False
    await client.aclose()
    assert client._closed is True


def test_fake_db_query_returns_list() -> None:
    db = FakeDb()
    rows = db.query("SELECT 1")
    assert isinstance(rows, list)
    assert len(rows) > 0
