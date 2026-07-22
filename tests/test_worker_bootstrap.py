"""Integration tests for worker bootstrap config sync.

pytestmark = pytest.mark.integration — all tests use the real PG container
via the ``jobs_app`` fixture (session-scoped ``pg_container``).

The "sync ordering" (dispatch-after-bootstrap visibility) assertion is
owned by integration tests.
"""

import asyncio
import contextlib
import json
from collections.abc import Mapping
from typing import Any, cast

import asyncpg
import pytest
import structlog
from pydantic import BaseModel

from taskq._di import ProviderRegistry, Scope
from taskq._ids import new_base62
from taskq.actor import ActorRef, actor
from taskq.backend.clock import Clock
from taskq.cron import CronScheduleSpec
from taskq.exceptions import ActorConfigDriftList, MissingProvider
from taskq.migrate import apply_pending
from taskq.ratelimit.registry import registry as rl_registry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.settings import WorkerSettings
from taskq.worker.run import _main
from tests.conftest import unique_health_sock_path

pytestmark = pytest.mark.integration

_SCHEMA_LABEL = f"twb_{new_base62()}".lower()


# ── Generic per-test schema helpers (unique label per new test) ─────────


async def _prepare_schema_for(pg_dsn: str, schema: str) -> None:
    """Drop and recreate *schema*, apply migrations."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()


async def _cleanup_schema_for(pg_dsn: str, schema: str) -> None:
    """Drop *schema*."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


def _settings_for(pg_dsn: str, schema: str, **overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"pg_dsn": pg_dsn, "schema_name": schema}
    # _main starts a real HealthServer — never the shared default path.
    data.setdefault("health_socket_path", unique_health_sock_path("worker_bootstrap"))
    data.update(overrides)
    return WorkerSettings.load_from_dict(data)


async def _run_and_cancel(coro_factory: Any, *, sleep: float = 1.5) -> None:
    """Run ``_main``-wrapping coroutine briefly, then cancel it cleanly."""

    async def _runner() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await coro_factory()

    task = asyncio.create_task(_runner())
    await asyncio.sleep(sleep)
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    else:
        await task


class _Payload(BaseModel):
    x: int


# ── Schema setup helpers ─────────────────────────────────────────────


async def _prepare_schema(pg_dsn: str) -> None:
    """Drop and recreate the test schema, apply migrations."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA_LABEL}" CASCADE')
    finally:
        await conn.close()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=_SCHEMA_LABEL)
    finally:
        await conn.close()


async def _cleanup_schema(pg_dsn: str) -> None:
    """Drop the test schema."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA_LABEL}" CASCADE')
    finally:
        await conn.close()


def _settings(pg_dsn: str, **overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"pg_dsn": pg_dsn, "schema_name": _SCHEMA_LABEL}
    # _main starts a real HealthServer — never the shared default path.
    data.setdefault("health_socket_path", unique_health_sock_path("worker_bootstrap"))
    data.update(overrides)
    return WorkerSettings.load_from_dict(data)


# ── Happy path ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_bootstrap_populates_actor_config(pg_dsn: str) -> None:
    await _prepare_schema(pg_dsn)

    @actor(name="actor_a", max_concurrent=2)  # type: ignore[call-overload] # Why: test-only stub; ActorHandler protocol with *args/**kwargs is stricter than runtime.
    async def actor_a(payload: _Payload) -> None: ...

    @actor(name="actor_b", max_concurrent=None)  # type: ignore[call-overload] # Why: test-only stub.
    async def actor_b(payload: _Payload) -> None: ...

    registry: Mapping[str, ActorRef[Any, Any]] = {
        "actor_a": actor_a,  # type: ignore[dict-item] # Why: registry holds heterogeneous ActorRef types.
        "actor_b": actor_b,  # type: ignore[dict-item] # Why: registry holds heterogeneous ActorRef types.
    }

    settings = _settings(pg_dsn)

    async def _runner() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await _main(settings, actor_registry=registry)

    task = asyncio.create_task(_runner())
    await asyncio.sleep(2.0)
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    else:
        await task

    conn = await asyncpg.connect(pg_dsn)
    try:
        rows = await conn.fetch(
            f"SELECT actor, max_concurrent, queue FROM {_SCHEMA_LABEL}.actor_config ORDER BY actor"
        )
        assert len(rows) == 2
        assert rows[0]["actor"] == "actor_a"
        assert rows[0]["max_concurrent"] == 2
        assert rows[0]["queue"] == "default"
        assert rows[1]["actor"] == "actor_b"
        assert rows[1]["max_concurrent"] is None
        assert rows[1]["queue"] == "default"
    finally:
        await conn.close()

    await _cleanup_schema(pg_dsn)


# ── Drift refusal (default) ─────────────────────────────────────


@pytest.mark.asyncio
async def test_drift_refuses_start(pg_dsn: str) -> None:
    await _prepare_schema(pg_dsn)

    @actor(name="X", max_concurrent=3)  # type: ignore[call-overload] # Why: test-only stub.
    async def actor_x(payload: _Payload) -> None: ...

    registry: Mapping[str, ActorRef[Any, Any]] = {
        "X": actor_x,  # type: ignore[dict-item] # Why: registry holds heterogeneous ActorRef types.
    }

    settings = _settings(pg_dsn)

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f"INSERT INTO {_SCHEMA_LABEL}.actor_config (actor, max_concurrent, queue, metadata) "
            "VALUES ('X', 5, 'default', '{}'::jsonb)"
        )
    finally:
        await conn.close()

    with pytest.raises(ActorConfigDriftList) as exc_info:
        await _main(settings, actor_registry=registry)

    drift_list = exc_info.value
    assert len(drift_list.drifts) == 1
    assert drift_list.drifts[0].actor == "X"
    assert drift_list.drifts[0].field == "max_concurrent"
    assert drift_list.drifts[0].registered == 3
    assert drift_list.drifts[0].stored == 5

    conn = await asyncpg.connect(pg_dsn)
    try:
        row = await conn.fetchrow(
            f"SELECT max_concurrent FROM {_SCHEMA_LABEL}.actor_config WHERE actor = 'X'"
        )
        assert row is not None
        assert row["max_concurrent"] == 5
    finally:
        await conn.close()

    await _cleanup_schema(pg_dsn)


# ── Drift force overwrite (force=True) ──────────────────────────


@pytest.mark.asyncio
async def test_drift_force_overwrites(pg_dsn: str) -> None:
    await _prepare_schema(pg_dsn)

    @actor(name="X", max_concurrent=3)  # type: ignore[call-overload] # Why: test-only stub.
    async def actor_x(payload: _Payload) -> None: ...

    registry: Mapping[str, ActorRef[Any, Any]] = {
        "X": actor_x,  # type: ignore[dict-item] # Why: registry holds heterogeneous ActorRef types.
    }

    settings = _settings(pg_dsn, force_update_actor_config="true")

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f"INSERT INTO {_SCHEMA_LABEL}.actor_config (actor, max_concurrent, queue, metadata) "
            "VALUES ('X', 5, 'default', '{}'::jsonb)"
        )
    finally:
        await conn.close()

    async def _runner() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await _main(settings, actor_registry=registry)

    task = asyncio.create_task(_runner())
    await asyncio.sleep(2.0)
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    else:
        await task

    conn = await asyncpg.connect(pg_dsn)
    try:
        row = await conn.fetchrow(
            f"SELECT max_concurrent FROM {_SCHEMA_LABEL}.actor_config WHERE actor = 'X'"
        )
        assert row is not None
        assert row["max_concurrent"] == 3
    finally:
        await conn.close()

    await _cleanup_schema(pg_dsn)


# ── Empty registry ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_empty_registry_starts_cleanly(pg_dsn: str) -> None:
    await _prepare_schema(pg_dsn)

    registry: Mapping[str, ActorRef[Any, Any]] = {}

    settings = _settings(pg_dsn)

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f"INSERT INTO {_SCHEMA_LABEL}.actor_config (actor, max_concurrent, queue, metadata) "
            "VALUES ('legacy', 1, 'default', '{}'::jsonb)"
        )
    finally:
        await conn.close()

    async def _runner() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await _main(settings, actor_registry=registry)

    task = asyncio.create_task(_runner())
    await asyncio.sleep(2.0)
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    else:
        await task

    conn = await asyncpg.connect(pg_dsn)
    try:
        rows = await conn.fetch(
            f"SELECT actor, max_concurrent FROM {_SCHEMA_LABEL}.actor_config ORDER BY actor"
        )
        assert len(rows) == 1
        assert rows[0]["actor"] == "legacy"
        assert rows[0]["max_concurrent"] == 1
    finally:
        await conn.close()

    await _cleanup_schema(pg_dsn)


# ── Pool provider pre-registered — skip auto-registration ───────────


@pytest.mark.asyncio
async def test_pool_provider_preregistered_skips_registration(pg_dsn: str) -> None:
    """When asyncpg.Pool is already registered, _main must NOT attempt to
    register it again — ``register_value`` raises ``ValueError`` on a
    duplicate registration, so a clean bootstrap here proves the guard
    at ``if not registry.has_provider(asyncpg.Pool)`` took the skip path."""
    schema = f"twb_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    settings = _settings_for(pg_dsn, schema)

    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    sentinel_pool = object()
    registry.register_value(asyncpg.Pool, Scope.LOOP, cast(asyncpg.Pool, sentinel_pool))

    await _run_and_cancel(lambda: _main(settings, _registry=registry))

    await _cleanup_schema_for(pg_dsn, schema)


# ── sync_rate_limit_buckets / sync_slots failures — warn and continue ──


@pytest.mark.asyncio
async def test_ratelimit_sync_failures_logged_and_bootstrap_continues(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both ``sync_rate_limit_buckets`` and ``sync_slots`` raising during
    startup are caught, logged as warnings, and do not abort bootstrap."""
    schema = f"twb_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    settings = _settings_for(pg_dsn, schema)

    async def _raise_buckets(*args: object, **kwargs: object) -> None:
        raise RuntimeError("buckets sync boom")

    async def _raise_slots(*args: object, **kwargs: object) -> None:
        raise RuntimeError("slots sync boom")

    monkeypatch.setattr("taskq.ratelimit.sync_rate_limit_buckets", _raise_buckets)
    monkeypatch.setattr("taskq.ratelimit.sync_slots", _raise_slots)

    with structlog.testing.capture_logs() as captured:
        await _run_and_cancel(lambda: _main(settings))

    events = {e.get("event") for e in captured}
    assert "sync_rate_limit_buckets_failed" in events
    assert "sync_slots_failed" in events

    await _cleanup_schema_for(pg_dsn, schema)


# ── Clock type guard — non-Clock value registered at PROCESS scope ───


@pytest.mark.asyncio
async def test_clock_type_guard_raises_missing_provider(pg_dsn: str) -> None:
    """A pre-registered ``Clock`` provider whose value is not actually a
    ``Clock`` instance trips the defensive type-guard and raises
    ``MissingProvider`` before the TaskGroup starts."""
    schema = f"twb_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    settings = _settings_for(pg_dsn, schema)

    registry = ProviderRegistry()
    registry.register_value(WorkerSettings, Scope.PROCESS, settings)
    registry.register_value(Clock, Scope.PROCESS, cast(Clock, object()))

    with pytest.raises(MissingProvider) as exc_info:
        await _main(settings, _registry=registry)

    assert exc_info.value.type_name == "Clock"

    await _cleanup_schema_for(pg_dsn, schema)


# ── ensure_slots failure during reservation sync — warn and continue ──


@pytest.mark.asyncio
async def test_ensure_slots_failure_logged_and_bootstrap_continues(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A reservation's ``ensure_slots`` raising is caught, logged as a
    warning (with ``bucket_name``), and does not abort bootstrap."""
    schema = f"twb_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    reservation_name = f"res_{new_base62()}".lower()
    reservation = ConcurrencyReservation(name=reservation_name, slots=1, lease=30.0, schema=schema)
    rl_registry.register(reservation)

    @actor(name="ensure_slots_actor")  # type: ignore[call-overload] # Why: test-only stub.
    async def ensure_slots_actor(payload: _Payload) -> None: ...

    registry: Mapping[str, ActorRef[Any, Any]] = {
        "ensure_slots_actor": ensure_slots_actor,  # type: ignore[dict-item]
    }

    settings = _settings_for(pg_dsn, schema)

    async def _raise_ensure_slots(self: ConcurrencyReservation, pool: object) -> None:
        raise RuntimeError("ensure_slots boom")

    monkeypatch.setattr(ConcurrencyReservation, "ensure_slots", _raise_ensure_slots)

    async def _run() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await _main(settings, actor_registry=registry)

    try:
        with structlog.testing.capture_logs() as captured:
            task = asyncio.create_task(_run())
            # Poll for the expected log instead of a fixed sleep — under a
            # loaded container, bootstrap may need more than a fixed window
            # to reach the ensure_slots loop.
            deadline = asyncio.get_running_loop().time() + 30.0
            while asyncio.get_running_loop().time() < deadline:
                if any(e.get("event") == "ensure_slots_failed" for e in captured):
                    break
                if task.done():
                    break
                await asyncio.sleep(0.05)
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
    finally:
        rl_registry._reservations.pop(reservation_name, None)  # pyright: ignore[reportPrivateUsage]

    matches = [e for e in captured if e.get("event") == "ensure_slots_failed"]
    assert len(matches) >= 1
    assert matches[0]["bucket_name"] == reservation_name

    await _cleanup_schema_for(pg_dsn, schema)


# ── Cron registry auto-registration ──────────────────────────────────


@pytest.mark.asyncio
async def test_cron_registry_creates_schedule_with_metadata(pg_dsn: str) -> None:
    """A ``CronScheduleSpec`` with ``name`` and ``static_payload`` set is
    registered via ``backend.create_schedule``; the row's ``name`` column
    carries the schedule name and the metadata jsonb carries the static payload."""
    schema = f"twb_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    settings = _settings_for(pg_dsn, schema)

    spec = CronScheduleSpec(
        actor="cron_actor_a",
        cron_expr="0 0 * * *",
        timezone="UTC",
        static_payload={"x": 1},
        name="nightly-a",
    )

    await _run_and_cancel(lambda: _main(settings, _cron_registry=[spec]))

    conn = await asyncpg.connect(pg_dsn)
    try:
        row = await conn.fetchrow(
            f"SELECT actor, cron_expr, name, metadata FROM {schema}.cron_schedules WHERE actor = 'cron_actor_a'"
        )
        assert row is not None
        assert row["cron_expr"] == "0 0 * * *"
        assert row["name"] == "nightly-a"
        metadata = (
            json.loads(row["metadata"]) if isinstance(row["metadata"], str) else row["metadata"]
        )
        assert metadata["static_payload"] == {"x": 1}
    finally:
        await conn.close()

    await _cleanup_schema_for(pg_dsn, schema)


@pytest.mark.asyncio
async def test_cron_registry_duplicate_skipped_on_unique_violation(pg_dsn: str) -> None:
    """Registering the same cron spec twice: the second pass hits
    ``UniqueViolationError`` on the actor UNIQUE constraint, is logged at
    debug level, and does not raise or duplicate the row."""
    schema = f"twb_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    settings = _settings_for(pg_dsn, schema)

    spec = CronScheduleSpec(
        actor="cron_actor_b",
        cron_expr="*/5 * * * *",
        timezone="UTC",
    )

    await _run_and_cancel(lambda: _main(settings, _cron_registry=[spec]))
    await _run_and_cancel(lambda: _main(settings, _cron_registry=[spec]))

    conn = await asyncpg.connect(pg_dsn)
    try:
        rows = await conn.fetch(
            f"SELECT actor FROM {schema}.cron_schedules WHERE actor = 'cron_actor_b'"
        )
        assert len(rows) == 1
    finally:
        await conn.close()

    await _cleanup_schema_for(pg_dsn, schema)
