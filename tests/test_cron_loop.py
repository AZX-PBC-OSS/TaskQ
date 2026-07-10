"""Unit tests for cron fire logic — :mod:`taskq.worker.cron_loop`.

through fire_schedule, resolve_payload, miss-handling,
consecutive_failures tracking, and auto-disable.

Also covers regression: PRODUCER span is linked (not parented)
to the ambient trace context.
Pure-Python, no PG required.
"""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from opentelemetry import trace

from taskq._ids import new_uuid
from taskq.cron import _factory_cache
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.otel import setup_tracer
from taskq.worker.cron_loop import _ActorConfig, fire_schedule

from .test_leader import FakeConn, _worker_settings


@pytest.fixture(autouse=True)
def _restore_factory_cache() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: pytest autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Snapshot and restore _factory_cache.

    Tests that exercise ``_resolve_factory`` (via ``fire_schedule`` with
    a ``payload_factory``) may populate the module-level cache; this
    fixture ensures every test starts clean. File-scope autouse is
    justified because the majority of tests in this file call
    ``fire_schedule`` which may invoke ``_resolve_factory``.
    """
    original_cache = dict(_factory_cache)
    try:
        yield
    finally:
        _factory_cache.clear()
        _factory_cache.update(original_cache)


class _FakeCronRecord:
    """Mimics asyncpg.Record for cron schedule rows in unit tests."""

    def __init__(self, data: dict[str, object]) -> None:
        self._data = data

    def __getitem__(self, key: str) -> object:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def get(self, key: str, default: object = None) -> object:
        return self._data.get(key, default)


class _FakeCronConn(FakeConn):
    """FakeConn extended with fetchrow support for actor_config queries."""

    def __init__(
        self,
        *,
        fetchval_result: object = None,
        actor_config_row: _FakeCronRecord | None = None,
        disabled_count: int = 0,
    ) -> None:
        super().__init__(fetchval_result=fetchval_result)
        self._actor_config_row = actor_config_row
        self._disabled_count = disabled_count
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        self.fetchrow_calls.append((sql, args))
        if "actor_config" in sql:
            return self._actor_config_row
        return None

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        if "COUNT" in sql:
            return self._disabled_count
        return self._fetchval_result


def _make_schedule_row(
    *,
    actor: str = "test_actor",
    cron_expr: str = "*/5 * * * *",
    timezone: str = "UTC",
    payload_factory: str | None = None,
    metadata: dict[str, object] | None = None,
    consecutive_failures: int = 0,
    next_fire_at: datetime | None = None,
    last_fired_at: datetime | None = None,
    schedule_id: UUID | None = None,
    identity_key: str | None = None,
) -> _FakeCronRecord:
    from uuid import uuid4

    return _FakeCronRecord(
        {
            "id": schedule_id or uuid4(),
            "actor": actor,
            "cron_expr": cron_expr,
            "timezone": timezone,
            "payload_factory": payload_factory,
            "metadata": metadata or {},
            "last_fired_at": last_fired_at,
            "consecutive_failures": consecutive_failures,
            "next_fire_at": next_fire_at or datetime.now(UTC),
            "identity_key": identity_key,
        }
    )


def _make_actor_config_row(
    *,
    queue: str = "default",
    max_attempts: int = 3,
    retry_kind: str = "transient",
) -> _FakeCronRecord:
    return _FakeCronRecord(
        {
            "queue": queue,
            "max_attempts": max_attempts,
            "retry_kind": retry_kind,
        }
    )


def _cron_settings(**overrides: object) -> WorkerSettings:
    return _worker_settings(
        "postgresql://x:x@localhost/x",
        CRON_CATCH_UP_WINDOW="3600",
        CRON_AUTO_DISABLE_THRESHOLD="3",
        **overrides,  # type: ignore[arg-type] # Why: test helper forwards overrides to settings constructor.
    )


pytestmark = pytest.mark.asyncio


# ── consecutive_failures increments on factory error ────────────────


async def test_cron_fire_failure_increments_consecutive_failures() -> None:
    """Factory raising → consecutive_failures increments from 0 to 1;
    last_fire_error is set; last_fired_at unchanged."""
    schedule_id = new_uuid()
    now = datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC)
    row = _make_schedule_row(
        actor="failing_actor",
        payload_factory="nonexistent.module.fn",
        consecutive_failures=0,
        next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
        schedule_id=schedule_id,
    )
    conn = _FakeCronConn(actor_config_row=_make_actor_config_row())
    settings = _cron_settings()
    backend = InMemoryBackend(clock=FakeClock(now))
    actor_config_cache: dict[str, _ActorConfig] = {}

    await fire_schedule(conn, row, now, settings, backend, "taskq", new_uuid(), actor_config_cache)

    update_calls = conn.execute_calls
    assert len(update_calls) >= 1
    sql, args = update_calls[0]
    assert "consecutive_failures" in sql
    assert args[0] == schedule_id
    assert args[2] == 1

    last_fired_at_update = [(s, a) for s, a in update_calls if "last_fired_at = now()" in s]
    assert not last_fired_at_update


# ── 3-strike auto-disable ──────────────────────────────────────────


async def test_cron_fire_auto_disable_after_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """3 consecutive factory failures → enabled=False, OTel error event
    cron.auto_disabled emitted with failure_count=3."""
    _, exporter = setup_tracer(monkeypatch)

    schedule_id = new_uuid()
    now = datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC)
    conn = _FakeCronConn(
        actor_config_row=_make_actor_config_row(),
        disabled_count=1,
    )
    settings = _cron_settings()
    backend = InMemoryBackend(clock=FakeClock(now))

    for i in range(3):
        row = _make_schedule_row(
            actor="failing_actor",
            payload_factory="nonexistent.module.fn",
            consecutive_failures=i,
            next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
            schedule_id=schedule_id,
        )
        actor_config_cache: dict[str, _ActorConfig] = {}
        await fire_schedule(
            conn, row, now, settings, backend, "taskq", new_uuid(), actor_config_cache
        )

    auto_disable_calls = [
        (sql, args) for sql, args in conn.execute_calls if "enabled = false" in sql
    ]
    assert len(auto_disable_calls) == 1
    _, args = auto_disable_calls[0]
    assert args[0] == schedule_id
    assert args[2] == 3

    auto_disabled_spans = [
        s
        for s in exporter.spans_named("cron fire")
        if any(ev.name == "cron.auto_disabled" for ev in s.events)
    ]
    assert len(auto_disabled_spans) >= 1
    event_attrs = dict(auto_disabled_spans[0].events[0].attributes or {})
    assert event_attrs.get("failure_count") == 3
    assert event_attrs.get("schedule_name") == "failing_actor"
    assert isinstance(event_attrs.get("last_error"), str)
    assert event_attrs["last_error"]


# ── consecutive_failures resets on success ─────────────────────────


async def test_cron_fire_success_resets_consecutive_failures() -> None:
    """2 failures then success → consecutive_failures=0,
    last_fire_error=None, last_fired_at updated."""
    schedule_id = new_uuid()
    now = datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC)
    settings = _cron_settings()
    backend = InMemoryBackend(clock=FakeClock(now))
    backend.register_actor_config(actor="recovering_actor", queue="default")

    for i in range(2):
        conn = _FakeCronConn(actor_config_row=_make_actor_config_row())
        row = _make_schedule_row(
            actor="recovering_actor",
            payload_factory="nonexistent.module.fn",
            consecutive_failures=i,
            next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
            schedule_id=schedule_id,
        )
        actor_config_cache: dict[str, _ActorConfig] = {}
        await fire_schedule(
            conn, row, now, settings, backend, "taskq", new_uuid(), actor_config_cache
        )

    success_conn = _FakeCronConn(actor_config_row=_make_actor_config_row())
    success_row = _make_schedule_row(
        actor="recovering_actor",
        consecutive_failures=2,
        next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
        schedule_id=schedule_id,
    )
    actor_config_cache_success: dict[str, _ActorConfig] = {}
    await fire_schedule(
        success_conn,
        success_row,
        now,
        settings,
        backend,
        "taskq",
        new_uuid(),
        actor_config_cache_success,
    )

    success_updates = [
        (sql, args) for sql, args in success_conn.execute_calls if "consecutive_failures = 0" in sql
    ]
    assert len(success_updates) >= 1
    assert "last_fire_error = NULL" in success_updates[0][0]
    assert "last_fired_at = now()" in success_updates[0][0]


# ── last_fired_at NOT updated on factory failure ────────────────────


async def test_cron_fire_failure_does_not_update_last_fired_at() -> None:
    """last_fired_at unchanged after factory failure."""
    schedule_id = new_uuid()
    now = datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC)
    row = _make_schedule_row(
        actor="failing_actor",
        payload_factory="nonexistent.module.fn",
        consecutive_failures=0,
        next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
        schedule_id=schedule_id,
    )
    conn = _FakeCronConn(actor_config_row=_make_actor_config_row())
    settings = _cron_settings()
    backend = InMemoryBackend(clock=FakeClock(now))
    actor_config_cache: dict[str, _ActorConfig] = {}

    await fire_schedule(conn, row, now, settings, backend, "taskq", new_uuid(), actor_config_cache)

    for sql, _args in conn.execute_calls:
        assert "last_fired_at = now()" not in sql


# ── Miss within catch-up window — not skipped ─────────────────────


async def test_cron_fire_miss_within_catch_up_window_not_skipped() -> None:
    """next_fire_at = now() - 30min, cron_catch_up_window = 1h —
    _fire_schedule does not skip the slot."""
    schedule_id = new_uuid()
    now = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    next_fire = now - timedelta(minutes=30)
    row = _make_schedule_row(
        actor="late_actor",
        next_fire_at=next_fire,
        schedule_id=schedule_id,
    )
    backend = InMemoryBackend(clock=FakeClock(now))
    backend.register_actor_config(actor="late_actor", queue="default")
    conn = _FakeCronConn(actor_config_row=_make_actor_config_row())
    settings = _cron_settings()
    actor_config_cache: dict[str, _ActorConfig] = {}

    await fire_schedule(conn, row, now, settings, backend, "taskq", new_uuid(), actor_config_cache)

    success_updates = [
        (sql, args) for sql, args in conn.execute_calls if "last_fired_at = now()" in sql
    ]
    assert len(success_updates) >= 1


# ── Miss beyond catch-up window — skipped ─────────────────────────


async def test_cron_fire_miss_beyond_catch_up_window_skipped() -> None:
    """next_fire_at = now() - 90min, cron_catch_up_window = 1h —
    _fire_schedule skips old slot, fires at current-slot next_fire_at."""
    schedule_id = new_uuid()
    now = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    next_fire = now - timedelta(minutes=90)
    row = _make_schedule_row(
        actor="very_late_actor",
        cron_expr="0 * * * *",
        next_fire_at=next_fire,
        schedule_id=schedule_id,
    )
    backend = InMemoryBackend(clock=FakeClock(now))
    backend.register_actor_config(actor="very_late_actor", queue="default")
    conn = _FakeCronConn(actor_config_row=_make_actor_config_row())
    settings = _cron_settings()
    actor_config_cache: dict[str, _ActorConfig] = {}

    await fire_schedule(conn, row, now, settings, backend, "taskq", new_uuid(), actor_config_cache)

    success_updates = [
        (sql, args) for sql, args in conn.execute_calls if "last_fired_at = now()" in sql
    ]
    assert len(success_updates) >= 1
    _, args = success_updates[0]
    next_fire_arg: object = args[1]
    assert isinstance(next_fire_arg, datetime)
    assert next_fire_arg > now


# ── PRODUCER span is linked, not parented ──────────────────────────


async def test_cron_fire_producer_span_linked_not_parented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """regression. The "cron fire" PRODUCER span has no parent and
    carries a link to the ambient trace context when one exists."""
    import taskq.obs._otel as otel_mod

    _, exporter = setup_tracer(monkeypatch)
    tracer = otel_mod.get_tracer()

    schedule_id = new_uuid()
    now = datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC)
    row = _make_schedule_row(
        actor="linked_actor",
        next_fire_at=now,
        schedule_id=schedule_id,
    )
    backend = InMemoryBackend(clock=FakeClock(now))
    backend.register_actor_config(actor="linked_actor", queue="default")
    conn = _FakeCronConn(actor_config_row=_make_actor_config_row())
    settings = _cron_settings()
    actor_config_cache: dict[str, _ActorConfig] = {}

    with tracer.start_as_current_span("ambient") as ambient:
        ambient_ctx = ambient.get_span_context()
        await fire_schedule(
            conn, row, now, settings, backend, "taskq", new_uuid(), actor_config_cache
        )

    cron_span = exporter.span_named("cron fire")
    assert cron_span is not None
    assert cron_span.kind == trace.SpanKind.PRODUCER
    assert cron_span.parent is None, "PRODUCER span must not be parented to ambient trace"
    assert cron_span.links is not None, "PRODUCER span must carry a link"
    assert len(cron_span.links) >= 1
    linked_ctx = cron_span.links[0].context
    assert linked_ctx.trace_id == ambient_ctx.trace_id
    assert linked_ctx.span_id == ambient_ctx.span_id


async def test_cron_auto_disable_producer_span_linked_not_parented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """regression on the auto-disable error path. The "cron fire"
    PRODUCER span is linked (not parented) and carries the auto_disabled event."""
    import taskq.obs._otel as otel_mod

    _, exporter = setup_tracer(monkeypatch)
    tracer = otel_mod.get_tracer()

    schedule_id = new_uuid()
    now = datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC)
    conn = _FakeCronConn(
        actor_config_row=_make_actor_config_row(),
        disabled_count=1,
    )
    settings = _cron_settings()
    backend = InMemoryBackend(clock=FakeClock(now))

    with tracer.start_as_current_span("ambient") as ambient:
        ambient_ctx = ambient.get_span_context()
        for i in range(3):
            row = _make_schedule_row(
                actor="failing_linked_actor",
                payload_factory="nonexistent.module.fn",
                consecutive_failures=i,
                next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                schedule_id=schedule_id,
            )
            actor_config_cache: dict[str, _ActorConfig] = {}
            await fire_schedule(
                conn, row, now, settings, backend, "taskq", new_uuid(), actor_config_cache
            )

    cron_spans = exporter.spans_named("cron fire")
    auto_disable_span = None
    for s in cron_spans:
        if any(ev.name == "cron.auto_disabled" for ev in (s.events or [])):
            auto_disable_span = s
            break
    assert auto_disable_span is not None
    assert auto_disable_span.kind == trace.SpanKind.PRODUCER
    assert auto_disable_span.parent is None, (
        "auto-disable PRODUCER span must not be parented to ambient trace"
    )
    assert auto_disable_span.links is not None
    assert len(auto_disable_span.links) >= 1
    linked_ctx = auto_disable_span.links[0].context
    assert linked_ctx.trace_id == ambient_ctx.trace_id
    assert linked_ctx.span_id == ambient_ctx.span_id


# ── cron fire propagates schedule identity_key to the enqueued job ──


async def test_cron_fire_passes_identity_key_to_enqueued_job() -> None:
    """fire_schedule sets identity_key from the schedule row on the
    EnqueueArgs so cron-fired jobs dedup against on-demand jobs for the
    same business key."""
    from taskq.backend._protocol import IdentityKey

    schedule_id = new_uuid()
    now = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    row = _make_schedule_row(
        actor="identity_actor",
        next_fire_at=now,
        schedule_id=schedule_id,
        identity_key="sync:entity:123",
    )
    backend = InMemoryBackend(clock=FakeClock(now))
    backend.register_actor_config(actor="identity_actor", queue="default")
    conn = _FakeCronConn(actor_config_row=_make_actor_config_row())
    settings = _cron_settings()
    actor_config_cache: dict[str, _ActorConfig] = {}

    await fire_schedule(conn, row, now, settings, backend, "taskq", new_uuid(), actor_config_cache)

    enqueued = [j for j in backend._jobs.values() if j.actor == "identity_actor"]
    assert len(enqueued) == 1
    assert enqueued[0].identity_key == IdentityKey("sync:entity:123")


async def test_cron_fire_without_identity_key_leaves_it_none() -> None:
    """When the schedule row has no identity_key, the enqueued job's
    identity_key stays None (no dedup) — preserves pre-existing behaviour."""
    now = datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC)
    row = _make_schedule_row(actor="plain_actor", next_fire_at=now)
    backend = InMemoryBackend(clock=FakeClock(now))
    backend.register_actor_config(actor="plain_actor", queue="default")
    conn = _FakeCronConn(actor_config_row=_make_actor_config_row())
    settings = _cron_settings()
    actor_config_cache: dict[str, _ActorConfig] = {}

    await fire_schedule(conn, row, now, settings, backend, "taskq", new_uuid(), actor_config_cache)

    enqueued = [j for j in backend._jobs.values() if j.actor == "plain_actor"]
    assert len(enqueued) == 1
    assert enqueued[0].identity_key is None
