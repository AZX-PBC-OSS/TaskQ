"""Unit tests for the cron tick - :mod:`taskq.worker.cron_loop`.

Drives ``tick_cron`` end-to-end against a recording fake connection (no
PG): lock probe, server clock, due-schedule select, batched actor_config
lookup, planning (miss-handling, payload resolution, identity keys),
batched enqueue through the in-memory backend, and the batched
success/failure UPDATE statements - plus consecutive_failures tracking,
auto-disable, and the PRODUCER-span link contract.

Also covers regression: PRODUCER span is linked (not parented)
to the ambient trace context.
Pure-Python, no PG required.
"""

import asyncio
import re
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from asyncpg.exceptions import InterfaceError, UniqueViolationError
from opentelemetry import trace
from opentelemetry.trace import StatusCode

from taskq._ids import new_uuid
from taskq.constants import cron_commit_gate_channel
from taskq.cron import _factory_cache
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.otel import setup_tracer
from taskq.worker import cron_loop
from taskq.worker.cron_loop import ActorFirePolicy, tick_cron

from .test_leader import FakeConn, _FakeTransaction, _worker_settings

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

    @property
    def data(self) -> dict[str, object]:
        return self._data

    def __getitem__(self, key: str) -> object:
        return self._data[key]

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def get(self, key: str, default: object = None) -> object:
        return self._data.get(key, default)


class _FakeCronConn(FakeConn):
    """FakeConn extended to drive one ``tick_cron`` without PG.

    ``fetchval`` answers the three scalar reads a tick makes - the
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
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.read_due_schedules = False

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        if "COUNT" in sql:
            return self._disabled_count
        raise AssertionError(f"unexpected fetchval: {sql}")

    async def fetchrow(self, sql: str, *args: object) -> object:
        """Answer the tick's one single-row read: the per-actor failure
        totals aggregate (an empty totals object - this fake holds no
        failing rows). Any other ``fetchrow`` is a new read the fake does
        not model."""
        self.fetchrow_calls.append((sql, args))
        if "jsonb_object_agg" in sql:
            return _FakeCronRecord({"totals": "{}"})
        raise AssertionError(f"unexpected fetchrow: {sql}")

    async def fetch(self, sql: str, *args: object) -> list[_FakeCronRecord]:
        self.fetch_calls.append((sql, args))
        if "actor_config" in sql:
            return self.actor_config_rows
        if "pg_try_advisory_xact_lock" in sql:
            # The tick's first statement: the lock probe, nothing else.
            return [_FakeCronRecord({"got": True})]
        if "cron_schedules" in sql:
            # The tick's second statement: the due read. The planning clock
            # rides every row; an empty due set is no rows at all.
            self.read_due_schedules = True
            if not self.schedule_rows:
                return []
            return [_FakeCronRecord({**row.data, "server_now": _NOW}) for row in self.schedule_rows]
        if '"taskq".jobs' in sql:
            # The policy preflights (singleton blockers, max_pending counts)
            # and the DST overlap-twin probe read the jobs table; this fake
            # holds no jobs, so the preflights see none.
            return []
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
    max_pending: int | None = None,
    retry_base: timedelta | None = None,
    retry_cap: timedelta | None = None,
    retry_backoff: str | None = None,
    retry_jitter: float | None = None,
) -> _FakeCronRecord:
    """One row of the batched ``actor_config`` ``ANY($1)`` SELECT result.

    The curve scalars default to NULL: a row predating migration
    01.00.18, so the tick resolves them to the enqueue defaults.  A real
    asyncpg Record carries every column the SELECT names, curve columns
    included, so the fake must too even when it stores NULL."""
    return _FakeCronRecord(
        {
            "actor": actor,
            "queue": queue,
            "max_attempts": max_attempts,
            "retry_kind": retry_kind,
            "max_pending": max_pending,
            "retry_base": retry_base,
            "retry_cap": retry_cap,
            "retry_backoff": retry_backoff,
            "retry_jitter": retry_jitter,
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
    actor_policies: Mapping[str, ActorFirePolicy] | None = None,
) -> int:
    """Drive one tick inside the caller-owned transaction the contract requires."""
    async with conn.transaction():
        return await tick_cron(
            conn,
            settings,
            backend,
            "taskq",
            worker_id if worker_id is not None else new_uuid(),
            actor_policies=actor_policies,
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


pytestmark = [pytest.mark.asyncio]


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


# ── Miss within catch-up window - not skipped ─────────────────────


async def test_cron_fire_miss_within_catch_up_window_not_skipped() -> None:
    """next_fire_at = server_now - 30min, cron_catch_up_window = 1h -
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


# ── Miss beyond catch-up window - skipped ─────────────────────────


async def test_cron_fire_miss_beyond_catch_up_window_skipped() -> None:
    """next_fire_at = server_now - 90min, cron_catch_up_window = 1h -
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
    identity_key stays None (no dedup) - preserves pre-existing behaviour."""
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
    """The failure event identifies the worker too - the case where the
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


# ── batched-enqueue failures are attributed per schedule, not per tick ─
#
# The policy preflights are advisory: a client enqueue can commit in the
# window between the preflight SELECT and the batched INSERT (the tick's
# transaction is READ COMMITTED, so the INSERT's own statement snapshot
# sees the newly committed row).  The batched INSERT then violates
# ``jobs_singleton_uniq`` - and Postgres aborts the WHOLE statement, not
# just the offending row.  The strike must land only on the schedule whose
# fire actually collided; unrelated schedules in the same tick must fire
# (or at worst keep their counters untouched), because their only defect
# was sharing a tick with a busy actor.  Three such ticks striking everyone
# is the auto-disable trap: every unrelated schedule in the fleet goes
# dark because one actor was busy.
#
# A transient infra failure of the batched INSERT (statement timeout,
# connection drop, server shutdown) is not a schedule defect at all and
# must not strike ANY schedule - the leader's transient handling retries
# the whole tick.


def _singleton_violation(actor: str) -> UniqueViolationError:
    """A faithful ``jobs_singleton_uniq`` violation as asyncpg surfaces it.

    The partial unique index is keyed on ``(actor)`` (see the initial
    migration), so Postgres' detail line names the colliding ACTOR - the
    one fact needed to attribute the violation to a plan without
    re-inserting anything.
    """
    exc = UniqueViolationError(
        'duplicate key value violates unique constraint "jobs_singleton_uniq"'
    )
    exc.constraint_name = "jobs_singleton_uniq"
    exc.detail = f"Key (actor)=({actor}) already exists."
    return exc


async def test_singleton_race_between_preflight_and_insert_strikes_only_the_racer() -> None:
    """One plan collides with a singleton blocker that commits between the
    preflight and the batched INSERT: the colliding schedule takes exactly
    one strike and the unrelated schedule in the same tick still fires -
    no tick-wide strike, no auto-disable of the healthy schedule, and the
    colliding fire is not enqueued."""
    racer_id = new_uuid()
    peer_id = new_uuid()
    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(
                actor="busy_actor",
                next_fire_at=_NOW,
                schedule_id=racer_id,
            ),
            _make_schedule_row(
                actor="healthy_actor",
                next_fire_at=_NOW,
                schedule_id=peer_id,
            ),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="busy_actor"),
            _make_actor_config_row(actor="healthy_actor"),
        ],
    )
    settings = _cron_settings()

    from taskq.backend._protocol import EnqueueArgs, JobRow

    class _SingletonRacedBackend(InMemoryBackend):
        """First batched INSERT hits the newly-committed blocker; the retry
        (the survivors only) lands - the blocker persists for the tick."""

        def __init__(self) -> None:
            super().__init__(clock=FakeClock(_NOW))
            self._raced = False

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            if not self._raced:
                self._raced = True
                raise _singleton_violation("busy_actor")
            return await super().enqueue_batch(
                args_list, connection=connection, enforce_max_pending=enforce_max_pending
            )

    backend = _SingletonRacedBackend()

    fired = await _tick(
        conn,
        settings,
        backend,
        actor_policies={"busy_actor": ActorFirePolicy(singleton=True)},
    )

    assert fired == 1, (
        "the healthy schedule in the colliding tick must still fire - a busy "
        "singleton actor is not a defect of every schedule in the batch"
    )
    failure_updates = _failure_updates(conn)
    assert len(failure_updates) == 1, "exactly one schedule takes a strike"
    _sql, args = failure_updates[0]
    assert args[0] == [racer_id], (
        f"the strike must land only on the colliding schedule, got {args[0]}"
    )
    error_texts: object = args[1]
    assert isinstance(error_texts, list)
    assert "jobs_singleton_uniq" in str(error_texts[0]), (
        "the strike must carry the real constraint name as the reason"
    )
    assert args[2] == [1], "a first race loss is one strike, not three"
    assert args[3] == [False], "a first race loss must not auto-disable"
    success_updates = _success_updates(conn)
    assert len(success_updates) == 1
    _sql, success_args = success_updates[0]
    assert success_args[0] == [peer_id]
    assert not [j for j in backend._jobs.values() if j.actor == "busy_actor"], (
        "the colliding fire must not be enqueued - the client's job won the slot"
    )


async def test_transient_enqueue_failure_raises_without_striking_schedules() -> None:
    """A TimeoutError from the batched enqueue (statement timeout, conn
    blip) is PG weather, not a schedule defect: the tick re-raises for the
    leader's transient handling (retry next tick) and NO schedule takes a
    strike - the caller's rollback discards the tick and no
    consecutive_failures bookkeeping may commit."""
    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(actor="timeout_actor", next_fire_at=_NOW),
        ],
        actor_config_rows=[_make_actor_config_row(actor="timeout_actor")],
    )
    settings = _cron_settings()

    from taskq.backend._protocol import EnqueueArgs, JobRow

    class _TimeoutBackend(InMemoryBackend):
        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            raise TimeoutError()

    with pytest.raises(TimeoutError):
        await _tick(conn, settings, _TimeoutBackend(clock=FakeClock(_NOW)))

    assert _failure_updates(conn) == [], (
        "a transient infra failure of the batched INSERT must not increment "
        "consecutive_failures - three seconds of PG weather would auto-disable "
        "every healthy schedule in the fleet"
    )
    assert _success_updates(conn) == []


async def test_unattributable_server_error_is_isolated_per_plan() -> None:
    """A non-transient error the tick cannot attribute from the error alone
    (no constraint violation, no detail) falls back to enqueuing each plan
    in its own savepoint, so the failure lands only on the plan that
    actually fails and the healthy plans in the same tick still fire."""
    racer_id = new_uuid()
    peer_id = new_uuid()
    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(actor="defect_actor", next_fire_at=_NOW, schedule_id=racer_id),
            _make_schedule_row(actor="healthy_actor", next_fire_at=_NOW, schedule_id=peer_id),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="defect_actor"),
            _make_actor_config_row(actor="healthy_actor"),
        ],
    )
    settings = _cron_settings()

    from taskq.backend._protocol import EnqueueArgs, JobRow

    class _DefectBackend(InMemoryBackend):
        """Only the defect actor's rows fail (server-side, per plan)."""

        def __init__(self) -> None:
            super().__init__(clock=FakeClock(_NOW))

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            if any(a.actor == "defect_actor" for a in args_list):
                raise ValueError("payload defect: NUL in text")
            return await super().enqueue_batch(
                args_list, connection=connection, enforce_max_pending=enforce_max_pending
            )

    backend = _DefectBackend()
    fired = await _tick(conn, settings, backend)

    assert fired == 1, "the plan without the defect must still fire"
    failure_updates = _failure_updates(conn)
    assert len(failure_updates) == 1
    _sql, args = failure_updates[0]
    assert args[0] == [racer_id], f"only the actually-failing plan takes the strike, got {args[0]}"
    success_updates = _success_updates(conn)
    assert len(success_updates) == 1
    _sql, success_args = success_updates[0]
    assert success_args[0] == [peer_id]


async def test_pkey_violation_strikes_only_the_colliding_row() -> None:
    """A ``jobs_pkey`` violation carries the collided id in its detail
    line: the plan minting that id is struck and the rest of the batch
    retries and fires."""
    racer_id = new_uuid()
    peer_id = new_uuid()
    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(actor="collide_actor", next_fire_at=_NOW, schedule_id=racer_id),
            _make_schedule_row(actor="healthy_actor", next_fire_at=_NOW, schedule_id=peer_id),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="collide_actor"),
            _make_actor_config_row(actor="healthy_actor"),
        ],
    )
    settings = _cron_settings()

    from taskq.backend._protocol import EnqueueArgs, JobRow

    class _PkeyCollisionBackend(InMemoryBackend):
        """The first batched INSERT carries one already-committed id."""

        def __init__(self) -> None:
            super().__init__(clock=FakeClock(_NOW))
            self._collided: UUID | None = None

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            if self._collided is None:
                self._collided = args_list[0].id
                exc = UniqueViolationError(
                    'duplicate key value violates unique constraint "jobs_pkey"'
                )
                exc.constraint_name = "jobs_pkey"
                exc.detail = f"Key (id)=({self._collided}) already exists."
                raise exc
            return await super().enqueue_batch(
                args_list, connection=connection, enforce_max_pending=enforce_max_pending
            )

    backend = _PkeyCollisionBackend()
    fired = await _tick(conn, settings, backend)

    assert fired == 1, "only the colliding plan fails; its peer fires"
    failure_updates = _failure_updates(conn)
    assert len(failure_updates) == 1
    _sql, args = failure_updates[0]
    assert args[0] == [racer_id]
    error_texts: object = args[1]
    assert isinstance(error_texts, list)
    assert "jobs_pkey" in str(error_texts[0])


# ── a transient error AFTER strikes rolls back the strikes - and must
#    not have exported their telemetry ─────────────────────────────────
#
# A strike persists only if the tick's failures UPDATE executes AND the
# caller's transaction commits.  A TRANSIENT error from any LATER
# statement of the tick (the successes UPDATE here) re-raises correctly -
# zero strikes persist, the leader retries - but the strike spans used to
# be opened (and exported) at strike time, inside _strike_plans: the
# trace backend claimed schedule failures and auto-disables that the
# rollback erased.  Trace says schedule X auto-disabled; the DB row says
# enabled with count 0.  This pins the buffered emission: no failure
# span, no auto-disable event, no metric delta, unless ALL of the tick's
# SQL ran.


async def test_transient_after_strikes_emits_no_failure_telemetry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The batched enqueue strikes one plan (attributed singleton race),
    the survivors land, and THEN the successes UPDATE raises TimeoutError:
    the tick re-raises with zero persisted strikes and zero EXPORTED
    failure telemetry - no error span, no cron.auto_disabled event, no
    record_cron_failure delta."""
    from taskq.backend._protocol import EnqueueArgs, JobRow

    _, exporter = setup_tracer(monkeypatch)
    cron_failure_calls: list[tuple[str, int]] = []
    monkeypatch.setattr(
        cron_loop,
        "record_cron_failure",
        lambda sid, delta: cron_failure_calls.append((sid, delta)),
    )

    racer_id = new_uuid()
    peer_id = new_uuid()

    class _SuccessUpdateTransientConn(_FakeCronConn):
        """The successes UPDATE - the statement AFTER the strikes were
        computed - dies with a transient error (server timeout)."""

        async def execute(self, sql: str, *args: object) -> str:
            if "last_fired_at = clock_timestamp()" in sql:
                raise TimeoutError("successes UPDATE timed out")
            return await super().execute(sql, *args)

    class _RacedBackend(InMemoryBackend):
        """First batched INSERT hits the raced blocker; the survivors'
        retry lands."""

        def __init__(self) -> None:
            super().__init__(clock=FakeClock(_NOW))
            self._raced = False

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            if not self._raced:
                self._raced = True
                raise _singleton_violation("telemetry_racer")
            return await super().enqueue_batch(
                args_list, connection=connection, enforce_max_pending=enforce_max_pending
            )

    conn = _SuccessUpdateTransientConn(
        schedule_rows=[
            _make_schedule_row(actor="telemetry_racer", next_fire_at=_NOW, schedule_id=racer_id),
            _make_schedule_row(actor="telemetry_peer", next_fire_at=_NOW, schedule_id=peer_id),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="telemetry_racer"),
            _make_actor_config_row(actor="telemetry_peer"),
        ],
    )

    with pytest.raises(TimeoutError):
        await _tick(
            conn,
            _cron_settings(),
            _RacedBackend(),
            actor_policies={"telemetry_racer": ActorFirePolicy(singleton=True)},
        )

    assert _failure_updates(conn) == [], (
        "the failures UPDATE never ran - the transient re-raise must precede it"
    )
    assert cron_failure_calls == [], (
        "a strike the rollback erased must not move the cron failure counter"
    )
    error_spans = [
        s for s in exporter.spans_named("cron fire") if s.status.status_code == StatusCode.ERROR
    ]
    assert error_spans == [], (
        "failure spans were exported for strikes the transient rollback "
        "erased - telemetry must be emitted only after all of the tick's "
        "SQL has executed"
    )
    auto_disabled = [
        ev
        for s in exporter.spans_named("cron fire")
        for ev in (s.events or [])
        if ev.name == "cron.auto_disabled"
    ]
    assert auto_disabled == [], "no auto-disable event for a strike that never persisted"


async def test_operator_index_violation_is_not_attributed() -> None:
    """An operator-added non-partial unique index on (actor) raises the
    same ``Key (actor)=(x) already exists.`` detail shape as
    ``jobs_singleton_uniq`` - but a DIFFERENT constraint name.

    Attribution gated on the detail alone would strike the
    singleton-stamped plan of that actor (the only pending plan the
    stamp-verification finds) even though the violator was an unstamped
    row the operator's index - not TaskQ's - rejected: a wrong strike
    toward auto-disable for a schedule whose fire broke none of TaskQ's
    own constraints.  The gate on ``constraint_name`` sends anything but
    ``jobs_pkey`` / ``jobs_singleton_uniq`` down the per-plan fallback;
    here the raced collision clears before the per-plan retry (READ
    COMMITTED fresh snapshot), so both schedules fire and NO strike is
    written."""
    from taskq.backend._protocol import EnqueueArgs, JobRow

    stamped_id = new_uuid()
    peer_id = new_uuid()

    class _OperatorIndexBackend(InMemoryBackend):
        """First batched INSERT violates an operator-added non-partial
        unique index on (actor): jobs_singleton_uniq's detail shape, a
        foreign constraint name."""

        def __init__(self) -> None:
            super().__init__(clock=FakeClock(_NOW))
            self._raised = False

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            if not self._raised:
                self._raised = True
                exc = UniqueViolationError(
                    'duplicate key value violates unique constraint "jobs_actor_operator_uniq"'
                )
                exc.constraint_name = "jobs_actor_operator_uniq"
                exc.detail = "Key (actor)=(op_index_actor) already exists."
                raise exc
            return await super().enqueue_batch(
                args_list, connection=connection, enforce_max_pending=enforce_max_pending
            )

    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(actor="op_index_actor", next_fire_at=_NOW, schedule_id=stamped_id),
            _make_schedule_row(actor="op_index_peer", next_fire_at=_NOW, schedule_id=peer_id),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="op_index_actor"),
            _make_actor_config_row(actor="op_index_peer"),
        ],
    )

    fired = await _tick(
        conn,
        _cron_settings(),
        _OperatorIndexBackend(),
        actor_policies={"op_index_actor": ActorFirePolicy(singleton=True)},
    )

    assert fired == 2, (
        "an operator-index violation must not strike the singleton-stamped "
        "plan of the named actor - the per-plan fallback retried both and "
        "both landed"
    )
    assert _failure_updates(conn) == [], (
        "attribution from the detail line alone would have written a "
        "failure UPDATE striking the stamped schedule for a constraint "
        "TaskQ does not own"
    )
    success_updates = _success_updates(conn)
    assert len(success_updates) == 1
    _sql, success_args = success_updates[0]
    assert success_args[0] == [stamped_id, peer_id]


# ── attribution failure modes: every unattributable shape falls back ──
#
# The pre-fix bug class was the WRONG-SCHEDULE strike: an error the parser
# could not safely map to a plan was either attributed anyway (operator
# index, above) or never exercised with the shapes that actually reach
# the parser.  These close the remaining traps: an actor-named detail
# whose only pending plan is NOT singleton-stamped, and the three
# detail-line degenerate shapes (None, garbage, truncated).


async def test_actor_detail_without_singleton_stamp_falls_back() -> None:
    """``cols == "actor"`` naming a plan that is NOT singleton-stamped:
    the stamp-verification guard (the partial index only covers stamped
    rows) must find no offender, so the violation is unattributable -
    per-plan fallback, no wrong strike.

    Without actor_policies the plans carry no ``metadata["singleton"]``
    stamp at all, so a parser that trusted the detail line alone would
    strike the named actor's schedule for a violation its rows cannot
    have produced (jobs_singleton_uniq never covers unstamped rows)."""
    from taskq.backend._protocol import EnqueueArgs, JobRow

    named_id = new_uuid()
    peer_id = new_uuid()

    class _UnstampedActorBackend(InMemoryBackend):
        """Batched INSERT violates jobs_singleton_uniq (faithful
        constraint name) naming an actor whose pending plans are
        unstamped; the per-plan retries land."""

        def __init__(self) -> None:
            super().__init__(clock=FakeClock(_NOW))
            self._raised = False

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            if not self._raised:
                self._raised = True
                raise _singleton_violation("unstamped_actor")
            return await super().enqueue_batch(
                args_list, connection=connection, enforce_max_pending=enforce_max_pending
            )

    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(actor="unstamped_actor", next_fire_at=_NOW, schedule_id=named_id),
            _make_schedule_row(actor="unstamped_peer", next_fire_at=_NOW, schedule_id=peer_id),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="unstamped_actor"),
            _make_actor_config_row(actor="unstamped_peer"),
        ],
    )

    fired = await _tick(conn, _cron_settings(), _UnstampedActorBackend())

    assert fired == 2, "the fallback retried both plans and both landed"
    assert _failure_updates(conn) == [], (
        "an actor-named detail whose only pending plan is unstamped must "
        "not strike it - jobs_singleton_uniq cannot have been violated by "
        "an unstamped row"
    )
    success_updates = _success_updates(conn)
    assert len(success_updates) == 1
    _sql, success_args = success_updates[0]
    assert success_args[0] == [named_id, peer_id]


@pytest.mark.parametrize(
    "detail",
    [
        pytest.param(None, id="detail-none"),
        pytest.param("some garbage the server never sends", id="detail-garbage"),
        pytest.param(
            "Key (actor)=(aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            id="detail-truncated",
        ),
    ],
)
async def test_degenerate_detail_lines_fall_back(detail: str | None) -> None:
    """``UniqueViolationError`` with a detail that is None, garbage, or
    truncated (PG cuts long detail values - the tail `` already
    exists.`` is gone) is unparsable: per-plan fallback, no strike.

    The constraint name is faithful (``jobs_singleton_uniq``) so the
    constraint gate passes and ONLY the detail parser stands between the
    violation and a wrong strike - every existing "unattributable" test
    used a ValueError that never reached the parser at all."""
    from taskq.backend._protocol import EnqueueArgs, JobRow

    stamped_id = new_uuid()
    peer_id = new_uuid()

    class _DegenerateDetailBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__(clock=FakeClock(_NOW))
            self._raised = False

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            if not self._raised:
                self._raised = True
                exc = UniqueViolationError(
                    'duplicate key value violates unique constraint "jobs_singleton_uniq"'
                )
                exc.constraint_name = "jobs_singleton_uniq"
                exc.detail = detail
                raise exc
            return await super().enqueue_batch(
                args_list, connection=connection, enforce_max_pending=enforce_max_pending
            )

    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(actor="degenerate_actor", next_fire_at=_NOW, schedule_id=stamped_id),
            _make_schedule_row(actor="degenerate_peer", next_fire_at=_NOW, schedule_id=peer_id),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="degenerate_actor"),
            _make_actor_config_row(actor="degenerate_peer"),
        ],
    )

    fired = await _tick(
        conn,
        _cron_settings(),
        _DegenerateDetailBackend(),
        actor_policies={"degenerate_actor": ActorFirePolicy(singleton=True)},
    )

    assert fired == 2, "an unparsable detail must not strike the stamped plan"
    assert _failure_updates(conn) == []


async def test_operator_index_column_detail_falls_back() -> None:
    """An operator-index-shaped detail naming a column TaskQ never keys
    on (``Key (tenant_id)=(x) already exists.``): neither the constraint
    gate nor the column parser can attribute it - per-plan fallback, no
    strike."""
    from taskq.backend._protocol import EnqueueArgs, JobRow

    tenant_id_: UUID = new_uuid()
    peer_id = new_uuid()

    class _TenantIndexBackend(InMemoryBackend):
        def __init__(self) -> None:
            super().__init__(clock=FakeClock(_NOW))
            self._raised = False

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            if not self._raised:
                self._raised = True
                exc = UniqueViolationError(
                    'duplicate key value violates unique constraint "jobs_tenant_operator_uniq"'
                )
                exc.constraint_name = "jobs_tenant_operator_uniq"
                exc.detail = "Key (tenant_id)=(tenant-x) already exists."
                raise exc
            return await super().enqueue_batch(
                args_list, connection=connection, enforce_max_pending=enforce_max_pending
            )

    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(actor="tenant_actor", next_fire_at=_NOW, schedule_id=tenant_id_),
            _make_schedule_row(actor="tenant_peer", next_fire_at=_NOW, schedule_id=peer_id),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="tenant_actor"),
            _make_actor_config_row(actor="tenant_peer"),
        ],
    )

    fired = await _tick(conn, _cron_settings(), _TenantIndexBackend())

    assert fired == 2
    assert _failure_updates(conn) == []


# ── the retry loop terminates when the RETRY also violates ────────────
#
# Attribution strikes at least one plan per pass, so the loop is bounded
# by the batch size - but every existing fake succeeded on the retry.
# This pins the second-violation pass: two singleton actors raced
# externally, the first pass strikes actor A's plan, the survivors' retry
# violates for actor B, and the THIRD pass lands the survivor.


async def test_second_violation_on_retry_strikes_both_and_exits() -> None:
    """Two externally-raced singleton actors in one batch: pass 1 strikes
    A, pass 2 (the survivors) strikes B, pass 3 lands the healthy peer -
    two strikes both persisted, survivors fire, loop exits."""
    from taskq.backend._protocol import EnqueueArgs, JobRow

    a_id = new_uuid()
    b_id = new_uuid()
    c_id = new_uuid()

    class _TwoRacedSingletonBackend(InMemoryBackend):
        """Each raced actor's rows violate exactly once, on whichever
        pass first carries them - A on pass 1, B on pass 2, C never."""

        def __init__(self) -> None:
            super().__init__(clock=FakeClock(_NOW))
            self._raised: set[str] = set()
            self.batch_calls = 0

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            self.batch_calls += 1
            actors = {a.actor for a in args_list}
            for actor in ("raced_a", "raced_b"):
                if actor in actors and actor not in self._raised:
                    self._raised.add(actor)
                    raise _singleton_violation(actor)
            return await super().enqueue_batch(
                args_list, connection=connection, enforce_max_pending=enforce_max_pending
            )

    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(actor="raced_a", next_fire_at=_NOW, schedule_id=a_id),
            _make_schedule_row(actor="raced_b", next_fire_at=_NOW, schedule_id=b_id),
            _make_schedule_row(actor="raced_healthy", next_fire_at=_NOW, schedule_id=c_id),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="raced_a"),
            _make_actor_config_row(actor="raced_b"),
            _make_actor_config_row(actor="raced_healthy"),
        ],
    )
    backend = _TwoRacedSingletonBackend()

    fired = await _tick(
        conn,
        _cron_settings(),
        backend,
        actor_policies={
            "raced_a": ActorFirePolicy(singleton=True),
            "raced_b": ActorFirePolicy(singleton=True),
        },
    )

    assert fired == 1, "the never-raced survivor fires after both strikes"
    assert backend.batch_calls == 3, (
        f"the loop must take exactly three passes (violate A, violate B, "
        f"land C); took {backend.batch_calls} - an unbounded loop would hang "
        "the tick, and a single-pass fallback would strike all three"
    )
    failure_updates = _failure_updates(conn)
    assert len(failure_updates) == 1, "one batched failures UPDATE carries both strikes"
    _sql, args = failure_updates[0]
    assert args[0] == [a_id, b_id], f"both raced schedules struck, in strike order; got {args[0]}"
    assert args[2] == [1, 1], "each raced schedule takes exactly one strike"
    assert args[3] == [False, False], "a first race loss must not auto-disable"
    success_updates = _success_updates(conn)
    assert len(success_updates) == 1
    _sql, success_args = success_updates[0]
    assert success_args[0] == [c_id]


# ── cancellation mid-savepoint ────────────────────────────────────────
#
# CancelledError is a BaseException: it passes through every ``except
# Exception`` in the enqueue path (neither TRANSIENT_PG_ERRORS retry
# semantics nor a per-schedule strike) - but it must still roll the
# batched enqueue back to ITS savepoint on the way out, leaving the
# caller's transaction (the thing the leader rolls back) intact, and
# with buffered emission it must export nothing.


async def test_cancelled_error_mid_savepoint_rolls_back_and_writes_no_strikes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Cancellation while the batched enqueue holds the savepoint open:
    the savepoint rolls back, the CancelledError propagates, zero
    strikes are written and zero failure telemetry exported."""
    import asyncio

    from taskq.backend._protocol import EnqueueArgs, JobRow

    _, exporter = setup_tracer(monkeypatch)
    cron_failure_calls: list[tuple[str, int]] = []
    monkeypatch.setattr(
        cron_loop,
        "record_cron_failure",
        lambda sid, delta: cron_failure_calls.append((sid, delta)),
    )

    entered = asyncio.Event()

    class _WedgeInsideSavepointBackend(InMemoryBackend):
        """The batched enqueue wedges after entering the savepoint -
        cancellation can only land inside it."""

        def __init__(self) -> None:
            super().__init__(clock=FakeClock(_NOW))

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            entered.set()
            await asyncio.Event().wait()  # wedge until the test cancels
            raise AssertionError("unreachable: the wedge must be cancelled")

    class _RecordingTransaction(_FakeTransaction):
        """asyncpg-shaped nested-transaction double: records the
        savepoint begin/rollback order a real connection would issue
        (nested transaction on an in-transaction connection = SAVEPOINT;
        exception on exit = ROLLBACK TO SAVEPOINT, exception propagates)."""

        def __init__(self, log: list[str]) -> None:
            self._log = log

        async def __aenter__(self) -> None:
            self._log.append("savepoint-begin")
            return None

        async def __aexit__(self, *args: object) -> None:
            self._log.append("savepoint-rollback" if args[0] is not None else "savepoint-release")

    class _SavepointConn(_FakeCronConn):
        def __init__(self, **kwargs: object) -> None:
            super().__init__(**kwargs)  # type: ignore[arg-type]  # Why: test-only double; kwargs are the _FakeCronConn constructor's.
            self.savepoint_log: list[str] = []

        def transaction(self) -> _RecordingTransaction:
            return _RecordingTransaction(self.savepoint_log)

    conn = _SavepointConn(
        schedule_rows=[
            _make_schedule_row(actor="cancel_actor", next_fire_at=_NOW),
        ],
        actor_config_rows=[_make_actor_config_row(actor="cancel_actor")],
    )

    task = asyncio.create_task(_tick(conn, _cron_settings(), _WedgeInsideSavepointBackend()))
    await entered.wait()  # the tick is wedged INSIDE the savepoint
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert conn.savepoint_log == [
        "savepoint-begin",  # the caller-owned transaction (from _tick)
        "savepoint-begin",  # the batched enqueue's savepoint
        "savepoint-rollback",  # cancellation rolled back TO the savepoint
        "savepoint-rollback",  # and the caller's transaction rolls back too
    ], (
        "cancellation must roll the savepoint back on its way out - the enqueue's partial write cannot survive in the caller's transaction"
    )
    assert _failure_updates(conn) == [], "a cancelled tick writes zero strikes"
    assert cron_failure_calls == [], "and exports no failure telemetry"
    error_spans = [
        s for s in exporter.spans_named("cron fire") if s.status.status_code == StatusCode.ERROR
    ]
    assert error_spans == []
    assert _success_updates(conn) == []


# ── the factory deadline's budget math ─────────────────────────────────
#
# The per-factory deadline is ``min(cron_payload_factory_timeout, what the
# tick has left of its whole-tick budget after the write reserve)``.  Any
# floor under that clamp lets each factory wait exceed the budget the tick
# actually has - and a batch of hung factories sums those floors past the
# leader's whole-tick ``asyncio.timeout``: the outer deadline wins, the
# tick's transaction rolls back every strike, and the identical batch is
# re-selected next tick.  A tick whose budget is spent must therefore
# fund NO further factory wait (None), and the planning loop turns that
# into an immediate, named per-schedule failure.


class TestFactoryDeadlineMath:
    """The clamp/reserve/exhaustion edges of the per-factory deadline."""

    def test_configured_budget_applies_when_the_tick_has_room(self) -> None:
        settings = _cron_settings(
            DISPATCHER_COMMAND_TIMEOUT="5.0", CRON_PAYLOAD_FACTORY_TIMEOUT="2.0"
        )
        assert cron_loop._factory_deadline(settings, 0.0) == 2.0

    def test_the_write_reserve_stays_unspent(self) -> None:
        settings = _cron_settings(
            DISPATCHER_COMMAND_TIMEOUT="5.0", CRON_PAYLOAD_FACTORY_TIMEOUT="5.0"
        )
        # A fresh tick at the matching defaults: the clamp holds the 10%
        # write reserve back from the factory.
        assert cron_loop._factory_deadline(settings, 0.0) == 4.5
        # Two seconds in, the clamp tracks what the tick actually has left.
        assert cron_loop._factory_deadline(settings, 2.0) == 2.5

    def test_a_spent_tick_funds_no_factory_wait(self) -> None:
        settings = _cron_settings(
            DISPATCHER_COMMAND_TIMEOUT="5.0", CRON_PAYLOAD_FACTORY_TIMEOUT="5.0"
        )
        assert cron_loop._factory_deadline(settings, 4.5) is None, (
            "a tick at the edge of its funded budget must not grant even a "
            "minimal wait - per-factory floors are what sum a hung batch "
            "past the whole-tick deadline"
        )
        assert cron_loop._factory_deadline(settings, 100.0) is None

    def test_a_leftover_below_the_minimum_fundable_grant_funds_no_wait(self) -> None:
        """A micro-grant is a lottery ticket, not a budget: factories faster
        than it fire, factories slower than it take a manufactured
        ``TimeoutError`` strike.  At the defaults the threshold is a
        quarter of the 4.5s funded budget (1.125s)."""
        settings = _cron_settings(
            DISPATCHER_COMMAND_TIMEOUT="5.0", CRON_PAYLOAD_FACTORY_TIMEOUT="5.0"
        )
        # 0.5s left of the funded budget: below the 1.125s threshold.
        assert cron_loop._factory_deadline(settings, 4.0) is None, (
            "a leftover too small to honour the declared factory scale must "
            "fund no call - granting it strikes, via a plain TimeoutError, "
            "any factory slower than itself: evidence manufactured against "
            "a schedule whose only defect was planning behind a monopolizer"
        )
        # Exactly at the threshold: fundable (>=, not >).
        assert cron_loop._factory_deadline(settings, 3.375) == 1.125
        # Above it: the leftover is the grant, as before.
        assert cron_loop._factory_deadline(settings, 3.0) == 1.5

    def test_a_tight_factory_budget_needs_only_its_full_declared_grant(self) -> None:
        """The threshold is capped by the configured timeout: a fleet whose
        factories are declared fast (timeout well under a quarter of the
        funded budget) must not be starved of every grant: its rule is
        "call a factory only with its FULL declared budget", never a
        partial one."""
        settings = _cron_settings(
            DISPATCHER_COMMAND_TIMEOUT="5.0", CRON_PAYLOAD_FACTORY_TIMEOUT="0.5"
        )
        # 0.4s left: a partial grant below the declared 0.5s budget,
        # refused (a 0.45s factory would strike on it).
        assert cron_loop._factory_deadline(settings, 4.1) is None
        # 0.5s left: the full declared budget fits, granted.
        assert cron_loop._factory_deadline(settings, 4.0) == 0.5
        # Room to spare: the configured budget is the grant, floor or no
        # floor.
        assert cron_loop._factory_deadline(settings, 0.0) == 0.5

    def test_a_tiny_tick_still_funds_its_first_factory(self) -> None:
        """At the smallest whole-tick deadline the funded budget is 0.9s and
        the threshold 0.225s (capped by a 1.0s configured timeout): a fresh
        tick always funds its first factory, and the monopolizer-scale
        leftover the review reproduced (0.077s) funds nothing."""
        settings = _cron_settings(
            DISPATCHER_COMMAND_TIMEOUT="1.0", CRON_PAYLOAD_FACTORY_TIMEOUT="1.0"
        )
        assert cron_loop._factory_deadline(settings, 0.0) == pytest.approx(0.9)
        # 0.077s left, the review's micro-grant: refused.
        assert cron_loop._factory_deadline(settings, 0.823) is None
        # 0.3s left: above the 0.225s threshold, granted.
        assert cron_loop._factory_deadline(settings, 0.6) == pytest.approx(0.3)


# ── tick-budget exhaustion defers the schedule instead of striking it ──
#
# The funded factory budget is FIRST-COME: a schedule whose factory
# consumed the whole grant (it ran, it timed out, it struck) leaves
# nothing for the factory-backed schedules after it in next_fire_at
# order.  Those schedules' factories NEVER RAN.  Striking them was the defect:
# the failure UPDATE never advances next_fire_at, so the identical batch
# returned in the identical order every tick and healthy schedules rode
# a hung neighbour's strikes to auto-disable.  The boundary is crisp:
# a factory that RAN and raised strikes (its own evidence); a factory
# the tick could not fund is DEFERRED: next_fire_at advances one leader
# cadence (not the next cron slot: the slot is still owed, only its
# funding was missing), consecutive_failures and last_fire_error are
# untouched, and the schedule retries on the very next tick.


def _suppression_updates(
    conn: _FakeCronConn,
) -> list[tuple[str, tuple[object, ...]]]:
    """The recorded suppression-branch UPDATEs, identified by their SET
    spelling (the success UPDATE shares the ``next_fire_at = f.next_fire``
    tail but begins its SET with ``last_fired_at``)."""
    return [
        (sql, args) for sql, args in conn.execute_calls if "SET next_fire_at = f.next_fire" in sql
    ]


class _SteppableMonotonic:
    """A ``time`` module double whose ``monotonic()`` the test advances in
    steps, the same seam the integration tier's skewed-datetime shims
    use (``test_cron_integration.py``), applied to the tick's elapsed
    clock so the funded-budget boundary is reached deterministically
    without a real factory hang on every tick."""

    def __init__(self, t0: float) -> None:
        self._now = t0

    def monotonic(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


async def test_budget_exhausted_factory_schedule_is_deferred_not_struck(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A factory that consumes its whole grant legitimately, and fires,
    leaves nothing for the next factory-backed schedule in next_fire_at
    order; that schedule rides the suppression UPDATE (next_fire_at + one
    leader cadence, nothing else): no failure UPDATE carries it, no
    failure span is exported for it, its factory is never called."""
    _, exporter = setup_tracer(monkeypatch)

    fired_id = new_uuid()
    deferred_id = new_uuid()
    # The deferred schedule's owed slot is 10:00:30 on an HOURLY expr, so
    # a skip-to-next-slot regression would write 11:00.  The contract is
    # the one-cadence retry at _NOW + 1s.
    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(
                actor="full_grant_actor",
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._full_grant_unit_factory",
                next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                schedule_id=fired_id,
            ),
            _make_schedule_row(
                actor="deferred_factory_actor",
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._fast_unit_factory",
                next_fire_at=datetime(2025, 1, 1, 10, 0, 30, tzinfo=UTC),
                schedule_id=deferred_id,
            ),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="full_grant_actor"),
            _make_actor_config_row(actor="deferred_factory_actor"),
        ],
    )
    settings = _cron_settings()  # 5.0s whole tick → 4.5s funded
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    fake_time = _SteppableMonotonic(100.0)
    resolver_calls: list[str | None] = []

    async def _grant_consuming_resolver(
        row: object, *, timeout_s: float | None = None
    ) -> dict[str, object]:
        """Stand-in for resolve_payload: the full-grant factory consumes
        exactly its granted deadline of the tick's elapsed budget and
        succeeds; no other factory is reached in this tick."""
        assert isinstance(row, _FakeCronRecord)
        resolver_calls.append(row["payload_factory"])
        fake_time.advance(timeout_s or 0.0)
        return {}

    monkeypatch.setattr(cron_loop, "resolve_payload", _grant_consuming_resolver)
    monkeypatch.setattr(cron_loop, "time", fake_time)

    import structlog.testing

    with structlog.testing.capture_logs() as captured:
        fired = await _tick(conn, settings, backend)

    assert fired == 1, "the full-grant schedule fires; the deferred slot is not a fire"
    assert resolver_calls == ["tests.test_cron_loop._full_grant_unit_factory"], (
        "the deferred schedule's factory must never be called - there was "
        "no funded wait left to grant it"
    )
    success_updates = _success_updates(conn)
    assert len(success_updates) == 1
    assert success_updates[0][1][0] == [fired_id], (
        "only the full-grant schedule advanced as a success"
    )

    suppression_updates = _suppression_updates(conn)
    assert len(suppression_updates) == 1, (
        "the never-funded schedule must ride the suppression UPDATE - the "
        "one branch that advances next_fire_at without touching failure "
        "accounting"
    )
    _, args = suppression_updates[0]
    assert args[0] == [deferred_id]
    next_fires: object = args[1]
    assert isinstance(next_fires, list)
    assert next_fires[0] == _NOW + timedelta(seconds=1.0), (
        "the deferral advance is ONE leader cadence, not the schedule's "
        "next cron slot - the owed slot is still landable, only its "
        "funding was missing, so skipping to 11:00 would drop a healthy "
        f"schedule's fire because a neighbour hogged the budget; got {next_fires[0]}"
    )

    assert _failure_updates(conn) == [], (
        "a factory that never ran is no evidence against the schedule - "
        "the strike path must stay reserved for factories that ran"
    )
    error_spans = [
        s for s in exporter.spans_named("cron fire") if s.status.status_code == StatusCode.ERROR
    ]
    assert error_spans == [], "no failure claim may be exported for a deferral"

    deferred_events = [
        e
        for e in captured
        if e["event"] == "cron-fire-budget-deferred" and e.get("schedule_id") == str(deferred_id)
    ]
    assert len(deferred_events) == 1, (
        "the deferral must leave a named log trail - an operator seeing it "
        "every tick knows a neighbour is eating the tick's factory budget"
    )
    assert deferred_events[0]["log_level"] == "info"
    assert not [e for e in captured if e["event"] == "cron fire failed"], (
        "the deferred schedule must not be logged as a failure"
    )


async def test_one_hung_factory_strikes_itself_and_defers_its_healthy_factory_peer() -> None:
    """The shape against the REAL resolver at the smallest real
    budget: one due schedule whose factory hangs consumes the tick's
    whole funded budget, so the healthy factory-backed schedule after it
    in next_fire_at order is deferred, while the hung one takes its own
    named strike and stays un-advanced at the front of the order."""
    hung_id = new_uuid()
    peer_id = new_uuid()
    settings = _cron_settings(
        DISPATCHER_COMMAND_TIMEOUT="1.0", CRON_PAYLOAD_FACTORY_TIMEOUT="1.0"
    )  # funded budget: 0.9s
    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(
                actor="hung_unit_actor",
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._hung_unit_factory",
                next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                schedule_id=hung_id,
            ),
            _make_schedule_row(
                actor="healthy_unit_actor",
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._fast_unit_factory",
                next_fire_at=datetime(2025, 1, 1, 10, 0, 30, tzinfo=UTC),
                schedule_id=peer_id,
            ),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="hung_unit_actor"),
            _make_actor_config_row(actor="healthy_unit_actor"),
        ],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    fired = await _tick(conn, settings, backend)

    assert fired == 0, "the hung schedule fails, the healthy one defers - nothing fires"

    failure_updates = _failure_updates(conn)
    assert len(failure_updates) == 1
    _, failure_args = failure_updates[0]
    assert failure_args[0] == [hung_id], "only the schedule whose factory RAN is struck"
    assert failure_args[2] == [1]
    assert failure_args[3] == [False]
    error_texts: object = failure_args[1]
    assert isinstance(error_texts, list)
    error_text = str(error_texts[0])
    assert "_hung_unit_factory" in error_text, (
        "the strike must name the factory that hung - the dotted path is "
        "the only thing that distinguishes it from every other schedule"
    )
    granted_match = re.search(r"timed out after (\d+(?:\.\d+)?)s", error_text)
    assert granted_match is not None, (
        f"the strike must name the effective deadline; got {error_text!r}"
    )
    assert float(granted_match.group(1)) == pytest.approx(0.9, abs=0.1), (
        "the strike must name the EFFECTIVE granted deadline - the funded "
        "clamp (whole-tick x 0.9 minus elapsed), not the configured 1.0s - "
        f"so an operator reads which budget fired; got {error_text!r}"
    )

    suppression_updates = _suppression_updates(conn)
    assert len(suppression_updates) == 1
    _, suppression_args = suppression_updates[0]
    assert suppression_args[0] == [peer_id], (
        "the healthy factory-backed schedule AFTER the hung one must be "
        "deferred, not struck - its factory was never called"
    )
    peer_next_fires: object = suppression_args[1]
    assert isinstance(peer_next_fires, list)
    assert peer_next_fires[0] == _NOW + timedelta(seconds=1.0)

    assert _success_updates(conn) == []


async def test_a_slow_successful_monopolizer_defers_its_peer_instead_of_striking_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The review shape against the REAL resolver at the smallest
    real budget: a monopolizing factory that consumes most of the funded
    budget and SUCCEEDS (0.70s of 0.9s) leaves only a micro-grant for the
    healthy factory-backed peer behind it.  The peer must be DEFERRED,
    its factory never called, not struck with a manufactured timeout: a
    slow-SUCCESSFUL monopolizer never strikes, never auto-disables and
    never frees the budget, so a strike here marched the peer to
    auto-disable in three ticks with nothing to stop the march."""
    monopolizer_id = new_uuid()
    peer_id = new_uuid()
    settings = _cron_settings(
        DISPATCHER_COMMAND_TIMEOUT="1.0", CRON_PAYLOAD_FACTORY_TIMEOUT="1.0"
    )  # funded 0.9s, minimum fundable grant 0.225s
    conn = _FakeCronConn(
        schedule_rows=[
            _make_schedule_row(
                actor="slow_monopolizer_actor",
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._slow_monopolizer_unit_factory",
                next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                schedule_id=monopolizer_id,
            ),
            _make_schedule_row(
                actor="marched_peer_actor",
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._marched_peer_unit_factory",
                next_fire_at=datetime(2025, 1, 1, 10, 0, 30, tzinfo=UTC),
                schedule_id=peer_id,
            ),
        ],
        actor_config_rows=[
            _make_actor_config_row(actor="slow_monopolizer_actor"),
            _make_actor_config_row(actor="marched_peer_actor"),
        ],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))
    _MARCHED_PEER_FACTORY_CALLS.clear()
    deferral_actors: list[str] = []
    monkeypatch.setattr(cron_loop, "record_cron_budget_deferral", deferral_actors.append)

    fired = await _tick(conn, settings, backend)

    assert fired == 1, "the monopolizer SUCCEEDS - it fires; that is what makes it never drain"
    success_updates = _success_updates(conn)
    assert len(success_updates) == 1
    assert success_updates[0][1][0] == [monopolizer_id], (
        "the monopolizer's fire is the tick's one success"
    )

    assert _failure_updates(conn) == [], (
        "a micro-grant is a lottery ticket, not a budget: granted the "
        "~0.2s leftover, the peer's 0.30s factory was cut by wait_for into "
        "a plain TimeoutError - a STRIKE with a manufactured 'timed out "
        "after 0.2s' reason, evidence against a schedule whose only defect "
        "was planning behind a slow neighbour.  The monopolizer never "
        "strikes and never frees the budget, so that strike marched the "
        "peer to auto-disable in three ticks"
    )
    assert _MARCHED_PEER_FACTORY_CALLS == [], (
        "the peer's factory must never be called on a leftover below the "
        "minimum fundable grant - there was no grant that could honour it"
    )

    suppression_updates = _suppression_updates(conn)
    assert len(suppression_updates) == 1
    _, suppression_args = suppression_updates[0]
    assert suppression_args[0] == [peer_id]
    peer_next_fires: object = suppression_args[1]
    assert isinstance(peer_next_fires, list)
    assert peer_next_fires[0] == _NOW + timedelta(seconds=1.0)
    assert deferral_actors == ["marched_peer_actor"], (
        "each deferral must record the taskq.cron.budget_deferrals counter "
        "under the starving schedule's actor - the sustained-rate signal "
        "that exposes a monopolizer no strike will ever name"
    )


async def test_the_march_arc_defers_the_peer_quietly_until_the_operator_knob_frees_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The full slow-monopolizer arc, and the operator lever that ends it.
    Through three ticks the monopolizer SUCCEEDS every time (its factory
    fits its grant) and the peer defers every tick, never struck, never
    auto-disabled, one budget_deferrals count per tick: the quiet-
    starvation shape the counter exists to expose.  Then the operator
    tightens TASKQ_CRON_PAYLOAD_FACTORY_TIMEOUT below the monopolizer's
    real duration: that same tick the monopolizer takes its FIRST strike
    (the knob's intended consequence: its drain begins) while the peer
    is funded a 0.4s grant and fires."""
    monopolizer_id = new_uuid()
    peer_id = new_uuid()
    # 1.0s whole tick → 0.9s funded; monopolizer duration 0.70, peer 0.30.
    march_settings = _cron_settings(
        DISPATCHER_COMMAND_TIMEOUT="1.0", CRON_PAYLOAD_FACTORY_TIMEOUT="1.0"
    )
    # The fairness lever: declared budget 0.5s < the monopolizer's 0.70s.
    knob_settings = _cron_settings(
        DISPATCHER_COMMAND_TIMEOUT="1.0", CRON_PAYLOAD_FACTORY_TIMEOUT="0.5"
    )

    fake_time = _SteppableMonotonic(100.0)
    granted: list[float | None] = []
    durations = {
        "tests.test_cron_loop._knob_monopolizer_factory": 0.70,
        "tests.test_cron_loop._knob_peer_factory": 0.30,
    }

    async def _duration_resolver(
        row: object, *, timeout_s: float | None = None
    ) -> dict[str, object]:
        """Stand-in for resolve_payload with per-factory DURATIONS: a
        factory consumes exactly its duration and returns when the grant
        covers it; under a grant too small it consumes the grant and
        raises the resolver's own timeout shape (what wait_for does)."""
        assert isinstance(row, _FakeCronRecord)
        factory = row["payload_factory"]
        assert isinstance(factory, str)
        duration = durations[factory]
        granted.append(timeout_s)
        if timeout_s is not None and duration > timeout_s:
            fake_time.advance(timeout_s)
            raise TimeoutError(f"cron payload factory {factory!r} timed out after {timeout_s:g}s")
        fake_time.advance(duration)
        return {}

    monkeypatch.setattr(cron_loop, "resolve_payload", _duration_resolver)
    monkeypatch.setattr(cron_loop, "time", fake_time)
    deferral_actors: list[str] = []
    monkeypatch.setattr(cron_loop, "record_cron_budget_deferral", deferral_actors.append)

    backend = InMemoryBackend(clock=FakeClock(_NOW))
    actor_rows = [
        _make_actor_config_row(actor="knob_monopolizer_actor"),
        _make_actor_config_row(actor="knob_peer_actor"),
    ]

    def _rows() -> list[_FakeCronRecord]:
        # The monopolizer stays due at the front of the order (an
        # at-cadence or catch-up-crawl shape): its next_fire_at is always
        # older than the peer's owed slot.
        return [
            _make_schedule_row(
                actor="knob_monopolizer_actor",
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._knob_monopolizer_factory",
                next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                schedule_id=monopolizer_id,
            ),
            _make_schedule_row(
                actor="knob_peer_actor",
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._knob_peer_factory",
                next_fire_at=datetime(2025, 1, 1, 10, 0, 30, tzinfo=UTC),
                schedule_id=peer_id,
            ),
        ]

    for tick_no in range(3):
        conn = _FakeCronConn(schedule_rows=_rows(), actor_config_rows=actor_rows)
        fired = await _tick(conn, settings=march_settings, backend=backend)

        assert fired == 1, (
            f"tick {tick_no + 1}: the slow monopolizer SUCCEEDS every tick - "
            "that is exactly what makes it never drain"
        )
        assert _failure_updates(conn) == [], (
            f"tick {tick_no + 1}: nobody may strike - the pre-fix march put "
            "the peer in this UPDATE with a manufactured micro-grant timeout "
            "and auto-disabled it on tick 3"
        )
        suppression_updates = _suppression_updates(conn)
        assert len(suppression_updates) == 1
        assert suppression_updates[0][1][0] == [peer_id], (
            f"tick {tick_no + 1}: the peer defers, unstruck, unfired"
        )
        success_ids = _success_updates(conn)[0][1][0]
        assert success_ids == [monopolizer_id]

    assert deferral_actors == ["knob_peer_actor"] * 3, (
        "one budget_deferrals count per deferred tick, under the starving "
        "schedule's actor - three ticks of quiet starvation are three "
        "counts an operator can alert on"
    )

    # The operator lever: declared budget 0.5s, below the monopolizer's
    # 0.70s real duration.  Same rows, same durations: only the setting
    # changed.
    conn = _FakeCronConn(schedule_rows=_rows(), actor_config_rows=actor_rows)
    fired = await _tick(conn, settings=knob_settings, backend=backend)

    assert fired == 1, (
        "the PEER is the tick's one fire - the monopolizer's first strike begins its drain"
    )
    failure_updates = _failure_updates(conn)
    assert len(failure_updates) == 1
    _, failure_args = failure_updates[0]
    assert failure_args[0] == [monopolizer_id], (
        "the knob's intended consequence: a factory slower than the "
        "declared budget takes the strike it has earned"
    )
    assert failure_args[2] == [1], "the monopolizer's drain starts at strike one"
    assert failure_args[3] == [False]
    success_ids = _success_updates(conn)[0][1][0]
    assert success_ids == [peer_id], "the starved peer fires the very tick the knob tightens"
    assert granted[-1] == pytest.approx(0.4), (
        "the peer's grant under the knob is min(configured 0.5, remaining "
        "0.4) - above the 0.225s minimum fundable grant, so the 0.30s "
        "factory fits it"
    )
    assert _suppression_updates(conn) == []


async def test_healthy_factory_peer_survives_the_hung_neighbours_drain_and_fires_when_budget_frees(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The full arc, tick by tick: through three hang-ticks the
    monopolizer accumulates ITS OWN strikes (auto-disable flips on the
    third) while the healthy factory-backed peer behind it is deferred
    every tick and never struck; the tick the monopolizer is gone the
    peer's factory is funded at the full clamp and it fires."""
    hung_id = new_uuid()
    peer_id = new_uuid()
    settings = _cron_settings()  # 5.0s whole tick → 4.5s funded, 3-strike threshold

    fake_time = _SteppableMonotonic(100.0)
    granted: list[float | None] = []

    async def _grant_consuming_resolver(
        row: object, *, timeout_s: float | None = None
    ) -> dict[str, object]:
        """Stand-in for resolve_payload: a factory whose call outlives its
        granted deadline consumes exactly that grant of the tick's
        elapsed budget and raises the resolver's own timeout shape; a
        healthy factory returns instantly."""
        assert isinstance(row, _FakeCronRecord)
        factory = row["payload_factory"]
        granted.append(timeout_s)
        if factory == "tests.test_cron_loop._never_resolves_unit":
            assert timeout_s is not None
            fake_time.advance(timeout_s)
            raise TimeoutError(f"cron payload factory {factory!r} timed out after {timeout_s:g}s")
        return {}

    monkeypatch.setattr(cron_loop, "resolve_payload", _grant_consuming_resolver)
    monkeypatch.setattr(cron_loop, "time", fake_time)

    backend = InMemoryBackend(clock=FakeClock(_NOW))

    def _rows_for_tick(hung_consecutive: int, include_hung: bool) -> list[_FakeCronRecord]:
        rows = []
        if include_hung:
            # Strikes never advance next_fire_at: the monopolizer stays
            # due at the front of the order every tick until disabled.
            rows.append(
                _make_schedule_row(
                    actor="drain_hung_actor",
                    cron_expr="0 * * * *",
                    payload_factory="tests.test_cron_loop._never_resolves_unit",
                    consecutive_failures=hung_consecutive,
                    next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                    schedule_id=hung_id,
                )
            )
        rows.append(
            _make_schedule_row(
                actor="drain_peer_actor",
                cron_expr="0 * * * *",
                payload_factory="tests.test_cron_loop._fast_unit_factory",
                next_fire_at=datetime(2025, 1, 1, 10, 0, 30, tzinfo=UTC),
                schedule_id=peer_id,
            )
        )
        return rows

    actor_rows = [
        _make_actor_config_row(actor="drain_hung_actor"),
        _make_actor_config_row(actor="drain_peer_actor"),
    ]

    for tick_no in range(3):
        conn = _FakeCronConn(
            schedule_rows=_rows_for_tick(tick_no, include_hung=True),
            actor_config_rows=actor_rows,
        )
        await _tick(conn, settings, backend)

        failure_updates = _failure_updates(conn)
        assert len(failure_updates) == 1, (
            f"tick {tick_no + 1}: exactly the monopolizer may be struck"
        )
        _, failure_args = failure_updates[0]
        assert failure_args[0] == [hung_id]
        assert failure_args[2] == [tick_no + 1], "the monopolizer's own strikes accumulate"
        assert failure_args[3] == [tick_no + 1 == 3], (
            "auto-disable must fire for the genuinely-hung-every-time "
            "factory on ITS third strike - never on the neighbour's"
        )

        suppression_updates = _suppression_updates(conn)
        assert len(suppression_updates) == 1
        _, suppression_args = suppression_updates[0]
        assert suppression_args[0] == [peer_id], (
            f"tick {tick_no + 1}: the healthy peer is deferred every "
            "hang-tick - suppressed, never struck"
        )
        assert _success_updates(conn) == []

    # The monopolizer is disabled: the next tick's batch holds only the
    # peer, whose factory is funded at the full clamp and fires.
    conn = _FakeCronConn(
        schedule_rows=_rows_for_tick(3, include_hung=False),
        actor_config_rows=actor_rows,
    )
    fired = await _tick(conn, settings, backend)

    assert fired == 1, "the deferred schedule fires the very tick the budget frees"
    success_updates = _success_updates(conn)
    assert len(success_updates) == 1
    assert success_updates[0][1][0] == [peer_id]
    assert _failure_updates(conn) == []
    assert _suppression_updates(conn) == []
    assert granted[-1] == pytest.approx(4.5), (
        "with the monopolizer gone the peer's factory is granted the full "
        "clamp - min(cron_payload_factory_timeout, remaining funded "
        "budget); a fairness cap below the configured deadline would have "
        "shrunk this grant and struck a factory that fits comfortably"
    )


async def _hung_unit_factory() -> dict[str, object]:  # pyright: ignore[reportUnusedFunction]  # Why: resolved at runtime via its dotted path (payload_factory), never imported; pyright cannot see the string reference.
    """Payload factory (dotted-path resolvable) that never returns in
    time: the resolver's per-factory wait_for is what cuts it."""
    await asyncio.sleep(30)
    return {}


def _fast_unit_factory() -> dict[str, object]:  # pyright: ignore[reportUnusedFunction]  # Why: resolved at runtime via its dotted path (payload_factory), never imported; pyright cannot see the string reference.
    """Payload factory that returns instantly: the healthy peer shape."""
    return {}


_MARCHED_PEER_FACTORY_CALLS: list[float] = []
"""Call log of _marched_peer_unit_factory: module scope because the
factory is reached only via its dotted path; the test that consumes it
clears and reads it around one awaited tick (the _HANG_STATE convention
in the integration tier)."""


async def _slow_monopolizer_unit_factory() -> dict[str, object]:  # pyright: ignore[reportUnusedFunction]  # Why: resolved at runtime via its dotted path (payload_factory), never imported; pyright cannot see the string reference.
    """Payload factory that consumes most of the smallest funded budget
    (0.70s of 0.9s) and SUCCEEDS: the slow-successful monopolizer that
    never strikes, never auto-disables and never frees the budget."""
    await asyncio.sleep(0.70)
    return {}


async def _marched_peer_unit_factory() -> dict[str, object]:  # pyright: ignore[reportUnusedFunction]  # Why: resolved at runtime via its dotted path (payload_factory), never imported; pyright cannot see the string reference.
    """Payload factory needing 0.30s: fits any fundable grant at the
    smallest budget, but never the micro-grant a monopolizer leaves."""
    _MARCHED_PEER_FACTORY_CALLS.append(asyncio.get_running_loop().time())
    await asyncio.sleep(0.30)
    return {}


# ── the commit-gate fallback is loud ───────────────────────────────────
#
# A connection that cannot carry the gate's session-scoped LISTEN (a
# transaction-pooling proxy shape) gets its telemetry emitted inline -
# pre-commit precision lost.  That degradation must be a named warning:
# emitted silently, telemetry that can describe an uncommitted
# transaction is indistinguishable from the commit-gated kind, and a
# failure has come to look exactly like a success.


class _GatelessCronConn(_FakeCronConn):
    """A connection that cannot carry the commit gate's LISTEN: the server
    pid reads fine but listener registration fails, the shape a
    transaction-pooling proxy in front of Postgres presents."""

    def get_server_pid(self) -> int:
        # Implausible backend pid: the armed-emission map is keyed by pid
        # and shares this process with real-PG tests, so the fake must not
        # collide with a real session's entry.
        return 2**30

    async def remove_listener(self, *_args: object) -> None:
        return None

    async def add_listener(self, *_args: object) -> None:
        raise InterfaceError("cannot LISTEN through a transaction-pooling proxy")


async def test_commit_gate_fallback_warns_and_still_emits_inline() -> None:
    """add_listener raising InterfaceError: the tick still fires, the
    fallback logs one warning naming the cause, and the tick's own
    telemetry is still emitted (inline) rather than lost."""
    import structlog.testing

    row = _make_schedule_row(actor="gateless_actor", next_fire_at=_NOW)
    conn = _GatelessCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="gateless_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    with structlog.testing.capture_logs() as captured:
        fired = await _tick(conn, _cron_settings(), backend)

    assert fired == 1
    fallback_warnings = [e for e in captured if e["event"] == "cron-commit-gate-unavailable"]
    assert len(fallback_warnings) == 1, (
        "the commit gate's failure must be a loud, named warning - a silent "
        "fallback makes telemetry that can describe an uncommitted "
        "transaction indistinguishable from the gated kind"
    )
    assert fallback_warnings[0]["log_level"] == "warning"
    assert "transaction-pooling proxy" in str(fallback_warnings[0].get("error", "")), (
        "the warning must name the cause"
    )
    assert len([e for e in captured if e["event"] == "cron fired"]) == 1, (
        "the tick's telemetry must still be emitted inline - losing the "
        "whole failure trail costs more than the commit-time precision the "
        "gate buys"
    )


# ── the failure-totals round trip is gated on telemetry being on ───────
#
# The per-tick per-actor failure aggregate has exactly one consumer: the
# failure-gauge reconcile in the emission.  With telemetry disabled that
# reconcile is a no-op, so the aggregate is one wasted round trip on every
# non-empty tick.


async def test_failure_totals_round_trip_only_when_telemetry_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-empty tick with telemetry disabled must not issue the totals
    query at all; re-enabled, exactly one totals read runs per tick."""
    import taskq.obs._otel as otel_mod

    row = _make_schedule_row(
        actor="quiet_actor",
        payload_factory="nonexistent.module.fn",
        next_fire_at=_NOW,
    )
    conn = _FakeCronConn(
        schedule_rows=[row],
        actor_config_rows=[_make_actor_config_row(actor="quiet_actor")],
    )
    backend = InMemoryBackend(clock=FakeClock(_NOW))

    monkeypatch.setattr(otel_mod, "_otel_enabled", False)
    await _tick(conn, _cron_settings(), backend)
    assert conn.fetchrow_calls == [], (
        "the failure-totals aggregate ran with telemetry disabled - one "
        "wasted round trip on every non-empty tick"
    )

    monkeypatch.setattr(otel_mod, "_otel_enabled", True)
    await _tick(conn, _cron_settings(), backend)
    totals_reads = [sql for sql, _args in conn.fetchrow_calls if "jsonb_object_agg" in sql]
    assert len(totals_reads) == 1, (
        "with telemetry enabled the reconcile's source query runs exactly once per non-empty tick"
    )


# ── commit-gate session state retires with its connection ────────────
#
# ``_armed_commit_emits`` and ``_confirmed_listening`` are keyed by backend
# pid. A tick that arms an emission and then loses its connection leaves
# the entry unanswered forever (the server rolled the NOTIFY back with the
# session), and a confirmed LISTEN outlives its session - under cron
# connection churn both maps would grow without bound, and a pid the
# server recycles would inherit a dead session's "confirmed listening"
# proof. The gate hooks the connection's termination signal to retire all
# of it.


class _GateSession:
    """A connection that CAN carry the commit gate: records the channel
    listener, captures the arming ``pg_notify``, and fires termination
    listeners on death (asyncpg's ``Connection._cleanup`` behavior).

    Implausible pids, like ``_GatelessCronConn``'s, so the process-global
    gate maps never collide with a real session's entry.
    """

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.channel_listeners: dict[str, Callable[..., None]] = {}
        self.remove_listener_calls: list[str] = []
        self.notify_calls: list[tuple[str, str]] = []
        self.termination_listeners: list[Callable[[object], None]] = []
        self.dead = False

    def get_server_pid(self) -> int:
        return self.pid

    async def remove_listener(self, channel: str, callback: object) -> None:
        self.remove_listener_calls.append(channel)
        self.channel_listeners.pop(channel, None)

    async def add_listener(self, channel: str, callback: Callable[..., None]) -> None:
        self.channel_listeners[channel] = callback

    def add_termination_listener(self, callback: Callable[[object], None]) -> None:
        self.termination_listeners.append(callback)

    async def execute(self, sql: str, *args: object) -> str:
        assert "pg_notify" in sql, f"unexpected execute on the gate seam: {sql}"
        self.notify_calls.append((str(args[0]), str(args[1])))
        return "SELECT 1"

    def deliver_commit_notify(self) -> None:
        """The server's answer when the arming tick's transaction COMMITs:
        the session's own NOTIFY rides back, addressed to its own pid."""
        channel, nonce = self.notify_calls[-1]
        self.channel_listeners[channel](self, self.pid, channel, nonce)

    def die(self) -> None:
        """What asyncpg does from ``_cleanup`` on close/terminate/loss."""
        self.dead = True
        for callback in self.termination_listeners:
            callback(self)


@pytest.fixture
def _commit_gate_maps() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction] # Why: pytest fixture consumed implicitly by the test runner; pyright does not track fixture usage.
    """Give each commit-gate test a clean slate, and hand one back.

    The maps are process-global (keyed by backend pid); these tests fill
    them deliberately with their own fake pids. Under xdist, a module that
    ran earlier in the same worker can leave a real backend pid behind
    (an entry whose connection closed without retiring it); snapshot-
    and-restore would faithfully preserve that foreign entry and the
    trio would assert on state it does not own (observed as
    ``assert {6125} == set()`` under full-suite CI ordering). So the
    fixture clears at setup and at teardown: the trio asserts only on
    what its own connections do. The underlying question, whether a
    closed connection can leave its confirmed-listening entry behind,
    is tracked separately and is not masked here: these tests still pin
    retirement for their own closes.
    """
    cron_loop._armed_commit_emits.clear()
    cron_loop._confirmed_listening.clear()
    cron_loop._termination_hooked.clear()
    try:
        yield
    finally:
        cron_loop._armed_commit_emits.clear()
        cron_loop._confirmed_listening.clear()
        cron_loop._termination_hooked.clear()


async def test_commit_gate_retires_session_state_on_close(
    _commit_gate_maps: None,
) -> None:
    """Full gated cycle: arm, COMMIT delivers the NOTIFY (the emission
    runs), then the connection dies - every per-pid entry is retired."""
    conn = _GateSession(pid=2**30 + 1)
    emitted: list[bool] = []

    await cron_loop._emit_on_commit(conn, lambda: emitted.append(True), schema="taskq")
    conn.deliver_commit_notify()
    assert emitted == [True], "a committed tick's emission must run"
    assert 2**30 + 1 in cron_loop._confirmed_listening

    conn.die()

    assert cron_loop._armed_commit_emits == {}
    assert cron_loop._confirmed_listening == set()
    assert cron_loop._termination_hooked == set()


async def test_commit_gate_retires_unanswered_emission_on_close(
    _commit_gate_maps: None,
) -> None:
    """The churn leak: a tick armed an emission, then its transaction
    rolled back (no NOTIFY can ever answer) and the connection died. The
    armed entry must not outlive the session - and the emission must
    never run for a commit that did not happen."""
    conn = _GateSession(pid=2**30 + 2)
    emitted: list[bool] = []

    await cron_loop._emit_on_commit(conn, lambda: emitted.append(True), schema="taskq")
    assert 2**30 + 2 in cron_loop._armed_commit_emits

    conn.die()

    assert emitted == [], "no commit, no emission - the gate's core contract"
    assert cron_loop._armed_commit_emits == {}
    assert cron_loop._confirmed_listening == set()


async def test_commit_gate_state_stays_bounded_under_connection_churn(
    _commit_gate_maps: None,
) -> None:
    """Fifty rotate-and-tick cycles (a leader losing and rebuilding its
    cron connection) must leave the gate maps empty, not holding one entry
    per dead session."""
    for i in range(50):
        conn = _GateSession(pid=2**30 + 100 + i)
        emitted: list[bool] = []
        await cron_loop._emit_on_commit(
            conn, lambda emitted=emitted: emitted.append(True), schema="taskq"
        )
        conn.deliver_commit_notify()
        assert emitted == [True]
        conn.die()

    assert cron_loop._armed_commit_emits == {}, (
        "armed emissions for dead sessions never got an answer and never retired"
    )
    assert cron_loop._confirmed_listening == set(), (
        "confirmed LISTEN entries outlived their sessions"
    )
    assert cron_loop._termination_hooked == set()


async def test_commit_gate_channel_is_scoped_to_the_schema(
    _commit_gate_maps: None,
) -> None:
    """NOTIFY channels share one database-wide namespace: two schemas'
    cron sessions in one database would otherwise deliver each other's
    commit signals - and, since the gate records the SENDER's pid as
    confirmed-listening, a foreign schema's notification would mark this
    session confirmed without its own LISTEN ever having survived a
    commit. The channel carries the schema's tag like every other
    channel, so the two schemas never share it."""
    from taskq.constants import cron_commit_gate_channel

    session = _GateSession(pid=2**30 + 400)
    await cron_loop._emit_on_commit(session, lambda: None, schema="tenant_a")
    channel, _nonce = session.notify_calls[-1]
    assert channel == cron_commit_gate_channel("tenant_a")
    assert channel in session.channel_listeners
    assert channel != cron_commit_gate_channel("tenant_b")


async def test_commit_gate_relistens_for_a_recycled_pid(
    _commit_gate_maps: None,
) -> None:
    """A pid the server hands to a fresh session must not inherit the dead
    session's confirmed LISTEN: the arm must force the defensive
    re-LISTEN (remove then add) on the new session, exactly as for a
    never-seen pid."""
    first = _GateSession(pid=2**30 + 200)
    await cron_loop._emit_on_commit(first, lambda: None, schema="taskq")
    first.deliver_commit_notify()
    assert 2**30 + 200 in cron_loop._confirmed_listening
    first.die()

    second = _GateSession(pid=2**30 + 200)
    await cron_loop._emit_on_commit(second, lambda: None, schema="taskq")

    assert cron_commit_gate_channel("taskq") in second.remove_listener_calls, (
        "a recycled pid inherited its dead predecessor's confirmed-listening "
        "proof - the fresh session skipped the defensive re-LISTEN"
    )


async def test_commit_gate_hooks_termination_once_per_connection(
    _commit_gate_maps: None,
) -> None:
    """Re-arming every tick must not stack termination listeners on a
    long-lived connection - the bound is one hook per session."""
    conn = _GateSession(pid=2**30 + 300)

    await cron_loop._emit_on_commit(conn, lambda: None, schema="taskq")
    conn.deliver_commit_notify()
    await cron_loop._emit_on_commit(conn, lambda: None, schema="taskq")
    conn.deliver_commit_notify()
    await cron_loop._emit_on_commit(conn, lambda: None, schema="taskq")

    assert len(conn.termination_listeners) == 1
