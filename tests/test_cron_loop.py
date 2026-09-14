"""Unit tests for the cron tick — :mod:`taskq.worker.cron_loop`.

Drives ``tick_cron`` end-to-end against a recording fake connection (no
PG): lock probe, server clock, due-schedule select, batched actor_config
lookup, planning (miss-handling, payload resolution, identity keys),
batched enqueue through the in-memory backend, and the batched
success/failure UPDATE statements — plus consecutive_failures tracking,
auto-disable, and the PRODUCER-span link contract.

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
from taskq.worker.cron_loop import tick_cron

from .test_leader import FakeConn, _worker_settings

# The fake connection's server clock: every croniter seed in a tick comes
# from this single domain, so the tests seed schedule rows relative to it.
_NOW = datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _restore_factory_cache() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: pytest autouse fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Snapshot and restore _factory_cache.

    Tests that exercise ``_resolve_factory`` (via ``tick_cron`` with
    a ``payload_factory``) may populate the module-level cache; this
    fixture ensures every test starts clean. File-scope autouse is
    justified because the majority of tests in this file drive ticks
    that may invoke ``_resolve_factory``.
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
    """FakeConn extended to drive one ``tick_cron`` without PG.

    ``fetchval`` answers the three scalar reads a tick makes — the
    advisory-lock probe (always acquired; contention is pinned in
    ``test_cron_lock_contention_obs.py``), the server clock, and the
    disabled-schedule COUNT. ``fetch`` answers the due-schedule SELECT
    from *schedule_rows* (recording that it was read) and the batched
    ``actor_config`` ``ANY($1)`` SELECT from *actor_config_rows*.
    ``execute`` records ``(sql, args)`` and returns an UPDATE tag whose
    rowcount satisfies every statement. ``transaction()`` is the
    passthrough the FakeConn base already provides, so tests can honor
    the contract that a tick runs inside a caller-owned open transaction.
    """

    def __init__(
        self,
        *,
        schedule_rows: list[_FakeCronRecord] | None = None,
        actor_config_rows: list[_FakeCronRecord] | None = None,
        disabled_count: int = 0,
    ) -> None:
        super().__init__()
        self.schedule_rows = schedule_rows if schedule_rows is not None else []
        self.actor_config_rows = actor_config_rows if actor_config_rows is not None else []
        self._disabled_count = disabled_count
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.read_due_schedules = False

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        if "pg_try_advisory_xact_lock" in sql:
            return True
        if "clock_timestamp" in sql:
            return _NOW
        if "COUNT" in sql:
            return self._disabled_count
        raise AssertionError(f"unexpected fetchval: {sql}")

    async def fetch(self, sql: str, *args: object) -> list[_FakeCronRecord]:
        self.fetch_calls.append((sql, args))
        if "actor_config" in sql:
            return self.actor_config_rows
        if "cron_schedules" in sql:
            self.read_due_schedules = True
            return self.schedule_rows
        raise AssertionError(f"unexpected fetch: {sql}")


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
    dst_strategy: str = "skip",
) -> _FakeCronRecord:
    return _FakeCronRecord(
        {
            "id": schedule_id or new_uuid(),
            "actor": actor,
            "cron_expr": cron_expr,
            "timezone": timezone,
            "payload_factory": payload_factory,
            "metadata": metadata or {},
            "last_fired_at": last_fired_at,
            "consecutive_failures": consecutive_failures,
            "next_fire_at": next_fire_at or _NOW,
            "identity_key": identity_key,
            # Every column the tick's SELECT names: the planner subscripts
            # them, so a row double that omits one is not a row.
            "dst_strategy": dst_strategy,
        }
    )


def _make_actor_config_row(
    *,
    actor: str = "test_actor",
    queue: str = "default",
    max_attempts: int = 3,
    retry_kind: str = "transient",
) -> _FakeCronRecord:
    """One row of the batched ``actor_config`` ``ANY($1)`` SELECT result."""
    return _FakeCronRecord(
        {
            "actor": actor,
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


async def _tick(
    conn: _FakeCronConn,
    settings: WorkerSettings,
    backend: InMemoryBackend,
    worker_id: UUID | None = None,
) -> int:
    """Drive one tick inside the caller-owned transaction the contract requires."""
    async with conn.transaction():
        return await tick_cron(
            conn, settings, backend, "taskq", worker_id if worker_id is not None else new_uuid()
        )


def _failure_updates(
    conn: _FakeCronConn,
) -> list[tuple[str, tuple[object, ...]]]:
    """The recorded failure-branch UPDATEs, identified by their SET spelling."""
    return [
        (sql, args)
        for sql, args in conn.execute_calls
        if "consecutive_failures = f.consecutive" in sql
    ]


def _success_updates(
    conn: _FakeCronConn,
) -> list[tuple[str, tuple[object, ...]]]:
    """The recorded success-branch UPDATEs, identified by their SET spelling."""
    return [
        (sql, args)
        for sql, args in conn.execute_calls
        if "last_fired_at = clock_timestamp()" in sql
    ]


pytestmark = pytest.mark.asyncio


# ── consecutive_failures increments on factory error ────────────────


async def test_cron_fire_failure_increments_consecutive_failures() -> None:
    """Factory raising → the batched failure UPDATE carries the schedule id,
    the raw error text, consecutive=1, disable=False; no success UPDATE
    runs, so last_fired_at is never stamped."""
    schedule_id = new_uuid()
    row = _make_schedule_row(
        actor="failing_actor",
        payload_factory="nonexistent.module.fn",
        consecutive_failures=0,
        next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
        schedule_id=schedule_id,
    )
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="failing_actor")],
    )
    settings = _cron_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    fired = await _tick(conn, settings, backend)

    assert fired == 0, "a tick whose only schedule failed to plan fires nothing"
    failure_updates = _failure_updates(conn)
    assert len(failure_updates) == 1
    sql, args = failure_updates[0]
    assert "consecutive_failures" in sql
    assert args[0] == [schedule_id]
    error_texts: object = args[1]
    assert isinstance(error_texts, list)
    assert "nonexistent" in str(error_texts[0]), "the raw error text reaches last_fire_error"
    assert args[2] == [1]
    assert args[3] == [False]

    assert _success_updates(conn) == [], "a failed fire must not stamp last_fired_at"
    for sql, _args in conn.execute_calls:
        assert "last_fired_at = clock_timestamp()" not in sql


# ── 3-strike auto-disable ──────────────────────────────────────────


async def test_cron_fire_auto_disable_after_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """3 consecutive factory failures → the third tick's failure UPDATE
    carries disable=True and consecutive=3; the OTel span event
    cron.auto_disabled carries failure_count=3; the disabled-schedule
    count is re-read after the disabling tick."""
    _, exporter = setup_tracer(monkeypatch)

    schedule_id = new_uuid()
    conn = _FakeCronConn(
        actor_config_rows=[_make_actor_config_row(actor="failing_actor")],
        disabled_count=1,
    )
    settings = _cron_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    for i in range(3):
        conn.schedule_rows = [
            _make_schedule_row(
                actor="failing_actor",
                payload_factory="nonexistent.module.fn",
                consecutive_failures=i,
                next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                schedule_id=schedule_id,
            )
        ]
        await _tick(conn, settings, backend)

    failure_updates = _failure_updates(conn)
    assert len(failure_updates) == 3, "one failure UPDATE per tick"
    disable_flags = [args[3] for _sql, args in failure_updates]
    assert disable_flags == [[False], [False], [True]]
    _, third_args = failure_updates[2]
    assert third_args[0] == [schedule_id]
    assert third_args[2] == [3]

    assert any("COUNT" in sql for sql, _args in conn.fetchval_calls), (
        "the disabling tick must refresh the disabled-schedules count"
    )

    auto_disabled_spans = [
        s
        for s in exporter.spans_named("cron fire")
        if any(ev.name == "cron.auto_disabled" for ev in s.events)
    ]
    assert len(auto_disabled_spans) == 1
    event_attrs = dict(auto_disabled_spans[0].events[0].attributes or {})
    assert event_attrs.get("failure_count") == 3
    assert event_attrs.get("schedule_name") == "failing_actor"
    assert isinstance(event_attrs.get("last_error"), str)
    assert event_attrs["last_error"]


# ── consecutive_failures resets on success ─────────────────────────


async def test_cron_fire_success_resets_consecutive_failures() -> None:
    """2 failure ticks then a success tick → the success tick issues the
    batched success UPDATE resetting consecutive_failures to 0, clearing
    last_fire_error, stamping last_fired_at, and advancing next_fire_at
    strictly into the future."""
    schedule_id = new_uuid()
    settings = _cron_settings()
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    for i in range(2):
        conn = _FakeCronConn(
            schedule_rows=[
                _make_schedule_row(
                    actor="recovering_actor",
                    payload_factory="nonexistent.module.fn",
                    consecutive_failures=i,
                    next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                    schedule_id=schedule_id,
                )
            ],
            actor_config_rows=[_make_actor_config_row(actor="recovering_actor")],
        )
        await _tick(conn, settings, backend)

    success_conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(
                actor="recovering_actor",
                consecutive_failures=2,
                next_fire_at=_NOW,
                schedule_id=schedule_id,
            )
        ],
        actor_config_rows=[_make_actor_config_row(actor="recovering_actor")],
    )
    fired = await _tick(success_conn, settings, backend)

    assert fired == 1
    assert success_conn.read_due_schedules, "the tick must read the due-schedule set"
    success_updates = _success_updates(success_conn)
    assert len(success_updates) >= 1
    sql, args = success_updates[0]
    assert "consecutive_failures = 0" in sql
    assert "last_fire_error = NULL" in sql
    assert "last_fired_at = clock_timestamp()" in sql
    next_fires: object = args[1]
    assert isinstance(next_fires, list)
    next_fire_arg: object = next_fires[0]
    assert isinstance(next_fire_arg, datetime)
    assert next_fire_arg > _NOW, "next_fire_at must advance strictly into the future"


# ── last_fired_at NOT updated on factory failure ────────────────────


async def test_cron_fire_failure_does_not_update_last_fired_at() -> None:
    """last_fired_at unchanged after factory failure."""
    row = _make_schedule_row(
        actor="failing_actor",
        payload_factory="nonexistent.module.fn",
        consecutive_failures=0,
        next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
    )
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="failing_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    await _tick(conn, _cron_settings(), backend)

    for sql, _args in conn.execute_calls:
        assert "last_fired_at = clock_timestamp()" not in sql


# ── Miss within catch-up window — not skipped ─────────────────────


async def test_cron_fire_miss_within_catch_up_window_not_skipped() -> None:
    """next_fire_at = server_now - 30min, cron_catch_up_window = 1h —
    the tick fires the overdue slot instead of skipping it."""
    row = _make_schedule_row(
        actor="late_actor",
        next_fire_at=_NOW - timedelta(minutes=30),
    )
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="late_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    fired = await _tick(conn, _cron_settings(), backend)

    assert fired == 1
    assert len(_success_updates(conn)) >= 1


# ── Miss beyond catch-up window — skipped ─────────────────────────


async def test_cron_fire_miss_beyond_catch_up_window_skipped() -> None:
    """next_fire_at = server_now - 90min, cron_catch_up_window = 1h —
    the tick skips the missed slot: the next_fire_at it writes is
    recomputed from the server clock, so it lands strictly in the
    future."""
    row = _make_schedule_row(
        actor="very_late_actor",
        cron_expr="0 * * * *",
        next_fire_at=_NOW - timedelta(minutes=90),
    )
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="very_late_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    fired = await _tick(conn, _cron_settings(), backend)

    assert fired == 1
    success_updates = _success_updates(conn)
    assert len(success_updates) >= 1
    _, args = success_updates[0]
    next_fires: object = args[1]
    assert isinstance(next_fires, list)
    next_fire_arg: object = next_fires[0]
    assert isinstance(next_fire_arg, datetime)
    assert next_fire_arg > _NOW, "the recompute seed is the server clock, not the stale slot"


# ── PRODUCER span is linked, not parented ──────────────────────────


async def test_cron_fire_producer_span_linked_not_parented(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """regression. The "cron fire" PRODUCER span has no parent and
    carries a link to the ambient trace context when one exists."""
    import taskq.obs._otel as otel_mod

    _, exporter = setup_tracer(monkeypatch)
    tracer = otel_mod.get_tracer()

    row = _make_schedule_row(actor="linked_actor", next_fire_at=_NOW)
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="linked_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    with tracer.start_as_current_span("ambient") as ambient:
        ambient_ctx = ambient.get_span_context()
        await _tick(conn, _cron_settings(), backend)

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
    PRODUCER span carrying cron.auto_disabled is linked (not parented)
    to the ambient trace context."""
    import taskq.obs._otel as otel_mod

    _, exporter = setup_tracer(monkeypatch)
    tracer = otel_mod.get_tracer()

    conn = _FakeCronConn(
        actor_config_rows=[_make_actor_config_row(actor="failing_linked_actor")],
        disabled_count=1,
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    with tracer.start_as_current_span("ambient") as ambient:
        ambient_ctx = ambient.get_span_context()
        for i in range(3):
            conn.schedule_rows = [
                _make_schedule_row(
                    actor="failing_linked_actor",
                    payload_factory="nonexistent.module.fn",
                    consecutive_failures=i,
                    next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                )
            ]
            await _tick(conn, _cron_settings(), backend)

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
    """The tick puts the schedule row's identity_key on the EnqueueArgs
    so cron-fired jobs dedup against on-demand jobs for the same
    business key."""
    from taskq.backend._protocol import IdentityKey

    row = _make_schedule_row(
        actor="identity_actor",
        next_fire_at=_NOW,
        identity_key="sync:entity:123",
    )
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="identity_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    fired = await _tick(conn, _cron_settings(), backend)

    assert fired == 1
    enqueued = [j for j in backend._jobs.values() if j.actor == "identity_actor"]
    assert len(enqueued) == 1
    assert enqueued[0].identity_key == IdentityKey("sync:entity:123")


async def test_cron_fire_without_identity_key_leaves_it_none() -> None:
    """When the schedule row has no identity_key, the enqueued job's
    identity_key stays None (no dedup) — preserves pre-existing behaviour."""
    row = _make_schedule_row(actor="plain_actor", next_fire_at=_NOW)
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="plain_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    await _tick(conn, _cron_settings(), backend)

    enqueued = [j for j in backend._jobs.values() if j.actor == "plain_actor"]
    assert len(enqueued) == 1
    assert enqueued[0].identity_key is None


# ── cron fire events carry the firing worker's id ──────────────────────
#
# The tick receives `worker_id` and dropped it from every event it
# logs, while the 13 sibling leader-loop events in `_leader_sweeps.py` all
# carry `worker_id=`. Cron runs only on the leader and leadership moves
# between workers across a rolling deploy, so "which worker fired (or
# failed) this schedule" is exactly the attribution needed to debug one.


async def test_cron_fired_event_carries_worker_id() -> None:
    """The success event identifies the worker that fired the schedule."""
    import structlog

    worker_id = new_uuid()
    row = _make_schedule_row(actor="attributed_actor", next_fire_at=_NOW)
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="attributed_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    with structlog.testing.capture_logs() as captured:
        await _tick(conn, _cron_settings(), backend, worker_id=worker_id)

    fired = [e for e in captured if e["event"] == "cron fired"]
    assert len(fired) == 1
    assert fired[0]["worker_id"] == str(worker_id)


async def test_cron_fire_failed_event_carries_worker_id() -> None:
    """The failure event identifies the worker too — the case where the
    attribution actually matters."""
    import structlog

    worker_id = new_uuid()
    row = _make_schedule_row(
        actor="failing_actor",
        payload_factory="nonexistent.module.fn",
        consecutive_failures=0,
        next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
    )
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="failing_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    with structlog.testing.capture_logs() as captured:
        await _tick(conn, _cron_settings(), backend, worker_id=worker_id)

    failed = [e for e in captured if e["event"] == "cron fire failed"]
    assert len(failed) == 1
    assert failed[0]["worker_id"] == str(worker_id)


# ── a failed batched enqueue fails every planned fire ──────────────────
#
# One enqueue statement covers the whole batch, so its failure converts
# every planned success into a per-schedule failure with the same
# consecutive-failure and auto-disable handling a planning error gets.
# Without this pin, a regression that lets the exception escape (or drops
# the failures UPDATE) would leave auto-disable dead on the enqueue-error
# path.


async def test_cron_enqueue_failure_counts_and_autodisables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """enqueue_batch raising → every planned fire becomes a failure record:
    consecutive increments, the third tick disables, and the tick returns 0."""
    from taskq.backend._protocol import EnqueueArgs, JobRow

    class _EnqueueFailsBackend(InMemoryBackend):
        """Real in-memory backend whose batched enqueue fails."""

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            raise RuntimeError("queue backend unavailable")

    _, exporter = setup_tracer(monkeypatch)

    schedule_id = new_uuid()
    conn = _FakeCronConn(
        actor_config_rows=[_make_actor_config_row(actor="enqueue_failure_actor")],
        disabled_count=1,
    )
    settings = _cron_settings()
    backend = _EnqueueFailsBackend(clock=FakeClock(_NOW))

    fired: int = -1
    for i in range(3):
        conn.schedule_rows = [
            _make_schedule_row(
                actor="enqueue_failure_actor",
                consecutive_failures=i,
                next_fire_at=_NOW,
                schedule_id=schedule_id,
            )
        ]
        fired = await _tick(conn, settings, backend)

    assert fired == 0, "a tick whose enqueue failed fires nothing"
    failure_updates = _failure_updates(conn)
    assert len(failure_updates) == 3
    disable_flags = [args[3] for _sql, args in failure_updates]
    assert disable_flags == [[False], [False], [True]]
    _, third_args = failure_updates[2]
    third_error_texts: object = third_args[1]
    assert isinstance(third_error_texts, list)
    assert "queue backend unavailable" in str(third_error_texts[0])

    auto_disabled = [
        ev
        for span in exporter.spans_named("cron fire")
        for ev in span.events
        if ev.name == "cron.auto_disabled"
    ]
    assert len(auto_disabled) == 1
