"""Schedule ``next_fire_at`` must seed in the due-check's clock domain.

The cron loop's due-check is server-side (``next_fire_at <=
clock_timestamp()``) and its NORMAL path recomputes every subsequent
fire from the STORED fire time — only a miss beyond
``cron_catch_up_window`` re-anchors on the server clock. A Python-clock
seed therefore phase-shifts every fire for the schedule's LIFE whenever
|app↔DB skew| stays inside the catch-up window (default 1h): a
"03:00 daily" schedule fires persistently at 03:00±skew.

These tests shim ``taskq.client._jobs.datetime`` with a skewed app clock
(the repo's skew-injection pattern — cf. ``_SkewedCronDatetime`` in
test_cron_integration.py, TI9-TI12) and drive the REAL public client
methods. The persisted ``next_fire_at`` must be anchored to the server
clock (pool-backed clients) or the client's injected Clock (in-memory
clients) — never to the skewed Python clock.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import ClassVar
from uuid import UUID, uuid4

import pytest

import taskq.client._jobs as jobs_mod
from taskq.backend._protocol import (
    ScheduleCreateArgs,
    ScheduleRecord,
    ScheduleUpdateArgs,
)
from taskq.client import JobsClient
from taskq.cron import compute_next_fire_after
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp
from taskq.testing.in_memory import InMemoryBackend

# 12:01 — the next */5 fire from the server clock is 12:05; from the
# app clock skewed +10m (12:11) it is 12:15. The two domains are
# distinguishable at a glance, which is the point.
_SERVER_NOW = datetime(2026, 1, 1, 12, 1, tzinfo=UTC)
_APP_SKEWED_NOW = _SERVER_NOW + timedelta(minutes=10)


class _FixedJobsDatetime:
    """Shim for ``taskq.client._jobs.datetime`` — ``now()`` returns a
    FIXED skewed app-clock time, so whichever clock domain seeded
    ``next_fire_at`` is observable in the persisted value."""

    def __init__(self, now: datetime) -> None:
        self._now = now

    def now(self, tz: object = None) -> datetime:
        return self._now


class _SkewedJobsDatetime:
    """Shim offsetting ``now()`` by *skew* off the real clock (the
    end-to-end PG variant, where the server clock is real)."""

    def __init__(self, skew: timedelta) -> None:
        self._skew = skew

    def now(self, tz: object = None) -> datetime:
        return datetime.now(tz if tz is not None else UTC) + self._skew


class _FakeConn:
    """Connection stand-in answering ``SELECT clock_timestamp()``."""

    def __init__(self, server_now: datetime) -> None:
        self._server_now = server_now
        self.queries: list[str] = []

    async def fetchval(self, sql: str, *args: object) -> datetime:
        self.queries.append(sql)
        return self._server_now


class _AcquireCtx:
    """``async with pool.acquire()`` context for the fake pool."""

    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _AcquireCtx:
        return _AcquireCtx(self._conn)


class _FakePgBackend:
    """Pool-backed stand-in driving the REAL JobsClient schedule path.

    Exposes ``worker_pool`` — the attribute shape JobsClient probes for
    its server-clock read — and records every create/update args it
    receives, so the test asserts on the value that would be persisted.
    Only the schedule surface the client touches is implemented.
    """

    supports_transactional_simulation: ClassVar[bool] = False

    def __init__(self, server_now: datetime) -> None:
        self.conn = _FakeConn(server_now)
        self.worker_pool = _FakePool(self.conn)
        self.created: list[ScheduleCreateArgs] = []
        self.updated: list[tuple[UUID, ScheduleUpdateArgs]] = []
        self._schedule_id = uuid4()
        self._records: dict[UUID, ScheduleRecord] = {}

    async def create_schedule(self, args: ScheduleCreateArgs) -> ScheduleRecord:
        self.created.append(args)
        record = ScheduleRecord(
            id=self._schedule_id,
            actor=args.actor,
            name=args.name,
            cron_expr=args.cron_expr,
            timezone=args.timezone,
            dst_strategy=args.dst_strategy,
            payload_factory=args.payload_factory,
            identity_key=args.identity_key,
            enabled=args.enabled,
            last_fired_at=None,
            last_fire_error=None,
            consecutive_failures=0,
            next_fire_at=args.next_fire_at,
            metadata=dict(args.metadata),
        )
        self._records[record.id] = record
        return record

    async def list_schedules(
        self,
        *,
        actor: str | None = None,
        enabled: bool | None = None,
    ) -> list[ScheduleRecord]:
        return list(self._records.values())

    async def update_schedule(
        self,
        schedule_id: UUID,
        args: ScheduleUpdateArgs,
    ) -> ScheduleRecord:
        self.updated.append((schedule_id, args))
        existing = self._records[schedule_id]
        record = existing.model_copy(
            update={
                "cron_expr": args.cron_expr or existing.cron_expr,
                "next_fire_at": args.next_fire_at or existing.next_fire_at,
            }
        )
        self._records[schedule_id] = record
        return record


def _expected(expr: str, after: datetime) -> datetime:
    return compute_next_fire_after(expr, "UTC", after)[0]


async def test_create_seeds_next_fire_from_server_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """create_schedule on a pool-backed client seeds ``next_fire_at``
    from the SERVER clock, not the (skewed) Python clock."""
    backend = _FakePgBackend(_SERVER_NOW)
    client = JobsClient(backend)  # type: ignore[arg-type]  # Why: deliberately not a full Backend — only the schedule surface the client touches is implemented, and the guardrails under test fire before any other backend call.
    monkeypatch.setattr(jobs_mod, "datetime", _FixedJobsDatetime(_APP_SKEWED_NOW))

    handle = await client.create_schedule("seeded_actor", "*/5 * * * *")

    assert _expected("*/5 * * * *", _APP_SKEWED_NOW) != _expected("*/5 * * * *", _SERVER_NOW), (
        "test precondition: the two clock domains must be distinguishable"
    )
    assert backend.created, "the backend create must have been reached"
    assert backend.created[0].next_fire_at == _expected("*/5 * * * *", _SERVER_NOW), (
        "the PERSISTED next_fire_at must be anchored to the server clock — "
        "a skewed app clock must not phase-shift the fire chain"
    )
    assert handle.next_fire_at == _expected("*/5 * * * *", _SERVER_NOW)
    assert any("clock_timestamp()" in q for q in backend.conn.queries), (
        "the server clock must actually have been read from the pool"
    )


async def test_update_seeds_next_fire_from_server_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """update_schedule with a changed cron_expr re-seeds ``next_fire_at``
    from the SERVER clock, not the (skewed) Python clock."""
    backend = _FakePgBackend(_SERVER_NOW)
    client = JobsClient(backend)  # type: ignore[arg-type]  # Why: see test_create_seeds_next_fire_from_server_clock.
    handle = await client.create_schedule("seeded_actor", "0 3 * * *")

    monkeypatch.setattr(jobs_mod, "datetime", _FixedJobsDatetime(_APP_SKEWED_NOW))
    record = await client.update_schedule(handle.schedule_id, cron_expr="*/5 * * * *")

    assert backend.updated, "the backend update must have been reached"
    assert backend.updated[0][1].next_fire_at == _expected("*/5 * * * *", _SERVER_NOW), (
        "the PERSISTED next_fire_at must be anchored to the server clock"
    )
    assert record.next_fire_at == _expected("*/5 * * * *", _SERVER_NOW)


async def test_in_memory_seeds_from_client_injected_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On the in-memory backend there is no second clock domain: the seed
    comes from the client's injected Clock (wired to the backend's clock
    in tests), and a skewed module-level ``datetime`` must not move it."""
    clock = FakeClock(_SERVER_NOW)
    backend = InMemoryBackend(clock=clock)
    client = JobsClient(backend, clock=clock)
    monkeypatch.setattr(jobs_mod, "datetime", _FixedJobsDatetime(_APP_SKEWED_NOW))

    handle = await client.create_schedule("mem_actor", "*/5 * * * *")

    assert handle.next_fire_at == _expected("*/5 * * * *", clock.now()), (
        "the in-memory seed must come from the client's injected Clock, "
        "not the skewed module-level datetime"
    )


@pytest.mark.integration
async def test_pg_create_seeds_from_server_clock(
    clean_jobs_app: JobsApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end on real Postgres: a Python clock skewed +10 minutes
    cannot move the persisted ``next_fire_at`` — it is anchored to
    ``clock_timestamp()`` read through the backend's pool.

    The expectation is bracketed by server-clock reads taken before and
    after the create (a */5 boundary can be crossed mid-test); the
    skewed seed would land strictly after both brackets, so membership
    in the bracket excludes it.
    """

    async def _server_now() -> datetime:
        async with clean_jobs_app.deps.worker_pool.acquire() as conn:
            now: datetime = await conn.fetchval("SELECT clock_timestamp()")
            return now

    client = JobsClient(clean_jobs_app.backend)
    monkeypatch.setattr(jobs_mod, "datetime", _SkewedJobsDatetime(timedelta(minutes=10)))

    before = await _server_now()
    handle = await client.create_schedule("pg_seed_actor", "*/5 * * * *")
    after = await _server_now()

    bracket = (
        _expected("*/5 * * * *", before),
        _expected("*/5 * * * *", after),
    )
    assert handle.next_fire_at in bracket, (
        f"next_fire_at {handle.next_fire_at!r} must be anchored to the PG "
        f"server clock (bracket {bracket!r}); a Python clock skewed +10m "
        "would seed a later */5 slot"
    )
