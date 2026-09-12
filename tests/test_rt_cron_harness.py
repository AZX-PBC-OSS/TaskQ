"""Shared real-PG attack harness for the bounded cron tick red-team files.

No tests live here.  The ``tests/test_rt_cron_*.py`` files drive
:func:`taskq.worker.cron_loop.tick_cron` against a real Postgres schema
(the ``module_pg_schema`` / ``clean_pg_conn`` fixtures) and share this
module's seeding helpers, connection wrappers and backend subclasses so
each attack file stays focused on its assertions.

Two pieces here are load-bearing beyond convenience:

* the wedge — :func:`wedge_then_fail` / :func:`wedge_then_succeed` are
  importable async ``payload_factory`` dotted paths that hold one
  schedule's PLANNING open on an :class:`asyncio.Event` while the tick's
  transaction (and its advisory lock) stays open.  That is the only
  deterministic way to act on the database from a second connection
  strictly between the tick's due SELECT and its UPDATEs.
* :class:`_JobIdCollisionBackend` — makes the tick's batched enqueue fail
  with a genuine server-side ``UniqueViolationError`` (``jobs_pkey``) on
  the caller's connection, without mocking asyncpg: the first planned
  row's id is swapped for one already committed, and the real
  ``enqueue_batch`` INSERT runs and fails for real.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Generator
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

from taskq._ids import new_uuid
from taskq.backend._protocol import ConnLike, EnqueueArgs, JobRow
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.settings import WorkerSettings

_HOURLY = "0 * * * *"
"""Hourly cadence: a schedule seeded at the current hour boundary is due
and advances to a strictly future boundary on its next fire."""

_TEN_MINUTELY = "*/10 * * * *"
"""Ten-minute cadence for catch-up and alignment pins (grid-exact)."""


class _StubBackendDeps:
    """Minimal duck-typed ``BackendDeps`` for :class:`PostgresBackend`.

    ``PostgresBackend.__init__`` reads only ``deps.settings.schema_name``
    (plus the identifier check on it); pools are resolved lazily through
    properties and the only backend method the cron tick calls is
    ``enqueue_batch(connection=...)``, which runs entirely on the
    caller-supplied connection and never touches a pool.
    """

    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.worker_pool: object | None = None
        self.heartbeat_pool: object | None = None
        self.dispatcher_pool: object | None = None


def cron_settings(schema: str, **overrides: str) -> WorkerSettings:
    """Settings for one attack: 1h catch-up window, 3-strike auto-disable."""
    values: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
        "TASKQ_SCHEMA_NAME": schema,
        "TASKQ_CRON_CATCH_UP_WINDOW": "3600",
        "TASKQ_CRON_AUTO_DISABLE_THRESHOLD": "3",
    }
    values.update(overrides)
    return WorkerSettings.load_from_dict(values, validate=False)


def make_backend(settings: WorkerSettings) -> PostgresBackend:
    """A ``PostgresBackend`` bound to the settings' schema, pool-free."""
    return PostgresBackend(
        _StubBackendDeps(settings),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; __init__ reads only settings.schema_name and no pool is acquired on the tick path.
        clock=SystemClock(),
        cancellation_grace_period=_ZERO,
        cleanup_grace_period=_ZERO,
    )


def pool_backend(
    settings: WorkerSettings, pool: asyncpg.Connection | asyncpg.Pool
) -> PostgresBackend:
    """A real pool-backed ``PostgresBackend`` for paths that acquire.

    ``make_backend`` suffices where the caller supplies the connection
    (the cron tick, single enqueues, caller-conn batches), but pool-touching
    paths — ``enqueue_batch_atomic`` acquiring its own transaction, pool-level
    ``enqueue`` — need real pools. Same duck-typed deps shape, wired to the
    module pool fixture instead of ``None``.
    """
    deps = _StubBackendDeps(settings)
    deps.worker_pool = pool
    deps.heartbeat_pool = pool
    deps.dispatcher_pool = pool
    return PostgresBackend(
        deps,  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; __init__ reads only settings.schema_name plus the pools set above.
        clock=SystemClock(),
        cancellation_grace_period=_ZERO,
        cleanup_grace_period=_ZERO,
    )


_ZERO = timedelta(seconds=0)


class GatedEnqueueBackend(PostgresBackend):
    """Backend whose batched enqueue pauses on a gate before writing.

    The pause lands after the tick took the advisory lock, read the due
    set and planned every fire — the exact state a second leader ticks
    through during handover.  ``entered`` is set the moment the enqueue
    is reached, so the test can start the competing tick while this one
    is provably mid-transaction.
    """

    def __init__(
        self,
        settings: WorkerSettings,
        *,
        gate: asyncio.Event,
        entered: asyncio.Event,
    ) -> None:
        super().__init__(
            _StubBackendDeps(settings),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; see make_backend.
            clock=SystemClock(),
            cancellation_grace_period=_ZERO,
            cleanup_grace_period=_ZERO,
        )
        self._gate = gate
        self.entered = entered

    async def enqueue_batch(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: ConnLike | None = None,
        enforce_max_pending: bool = True,
    ) -> list[JobRow]:
        self.entered.set()
        await self._gate.wait()
        return await super().enqueue_batch(
            args_list, connection=connection, enforce_max_pending=enforce_max_pending
        )


class JobIdCollisionBackend(PostgresBackend):
    """Backend whose batched enqueue fails with a genuine server-side
    ``UniqueViolationError`` on the caller's connection.

    The first planned row's id is swapped for one already committed to
    ``jobs``; the real ``enqueue_batch`` INSERT then runs and violates
    ``jobs_pkey`` for real — the identity-collision infra failure the
    tick's enqueue-failure branch exists for, with no asyncpg mocking.
    """

    def __init__(self, settings: WorkerSettings, *, collide_id: UUID) -> None:
        super().__init__(
            _StubBackendDeps(settings),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; see make_backend.
            clock=SystemClock(),
            cancellation_grace_period=_ZERO,
            cleanup_grace_period=_ZERO,
        )
        self._collide_id = collide_id

    async def enqueue_batch(
        self,
        args_list: list[EnqueueArgs],
        *,
        connection: ConnLike | None = None,
        enforce_max_pending: bool = True,
    ) -> list[JobRow]:
        sabotaged = [replace(args_list[0], id=self._collide_id), *args_list[1:]]
        return await super().enqueue_batch(
            sabotaged, connection=connection, enforce_max_pending=enforce_max_pending
        )


class CountingConn:
    """Delegates to a real connection, recording every awaited statement.

    The statement SEQUENCE is the property under test in the lock-ordering
    and empty-tick attacks; ``execute``/``executemany``/``fetch``/
    ``fetchrow``/``fetchval`` are recorded because they are the only
    awaited protocol methods the tick (and the batched enqueue running on
    the same connection) issues.  No ``**kwargs`` passthrough: none of
    those call sites passes keyword arguments, and a typed ``object``
    mapping cannot satisfy asyncpg's typed ``timeout``/``record_class``
    parameters.
    """

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn
        self.statements: list[str] = []

    def _record(self, sql: str) -> None:
        self.statements.append(" ".join(sql.split())[:200])

    @property
    def count(self) -> int:
        return len(self.statements)

    def matching(self, needle: str) -> int:
        """How many recorded statements contain *needle* (case-sensitive)."""
        return sum(1 for s in self.statements if needle in s)

    async def execute(self, sql: str, *args: object) -> object:
        self._record(sql)
        return await self._conn.execute(sql, *args)

    async def executemany(self, sql: str, args: object) -> object:
        self._record(sql)
        return await self._conn.executemany(sql, args)

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        self._record(sql)
        return await self._conn.fetch(sql, *args)

    async def fetchrow(self, sql: str, *args: object) -> asyncpg.Record | None:
        self._record(sql)
        return await self._conn.fetchrow(sql, *args)

    async def fetchval(self, sql: str, *args: object) -> object | None:
        self._record(sql)
        return await self._conn.fetchval(sql, *args)

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)


class FrozenClockConn:
    """Delegates to a real connection except the tick's planning clock.

    ``SELECT clock_timestamp()`` returns a pinned instant so the catch-up
    boundary comparison (``fire_at < frozen_now - window``) is EXACT —
    the only quantity in the tick that cannot be made deterministic with
    a live server clock, because equality requires hitting a microsecond
    that is only known once the tick itself reads it.  Every other
    statement — the advisory-lock probe, the due SELECT (whose
    ``statement_timestamp()`` bound is evaluated server-side), the
    enqueue and the UPDATEs — runs against real PG.
    """

    def __init__(self, conn: asyncpg.Connection, frozen: datetime) -> None:
        self._conn = conn
        self._frozen = frozen

    async def fetchval(self, sql: str, *args: object) -> object | None:
        if sql.strip() == "SELECT clock_timestamp()":
            return self._frozen
        return await self._conn.fetchval(sql, *args)

    async def fetch(self, sql: str, *args: object) -> list[asyncpg.Record]:
        return await self._conn.fetch(sql, *args)

    async def fetchrow(self, sql: str, *args: object) -> asyncpg.Record | None:
        return await self._conn.fetchrow(sql, *args)

    async def execute(self, sql: str, *args: object) -> object:
        return await self._conn.execute(sql, *args)

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)


# ── Seeding helpers ────────────────────────────────────────────────────


async def seed_actor_config(
    conn: asyncpg.Connection,
    schema: str,
    actor: str,
    *,
    queue: str = "rt_queue",
    max_attempts: int = 5,
    retry_kind: str = "transient",
) -> None:
    await conn.execute(
        f'INSERT INTO "{schema}".actor_config (actor, queue, max_attempts, retry_kind) '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
        "VALUES ($1, $2, $3, $4) "
        "ON CONFLICT (actor) DO UPDATE SET queue = EXCLUDED.queue, "
        "max_attempts = EXCLUDED.max_attempts, retry_kind = EXCLUDED.retry_kind",
        actor,
        queue,
        max_attempts,
        retry_kind,
    )


async def seed_schedule(
    conn: asyncpg.Connection,
    schema: str,
    *,
    actor: str,
    name: str,
    cron_expr: str,
    next_fire_at: datetime,
    timezone: str = "UTC",
    dst_strategy: str = "skip",
    payload_factory: str | None = None,
    identity_key: str | None = None,
    consecutive_failures: int = 0,
    enabled: bool = True,
) -> UUID:
    """Insert one cron_schedules row and return its id."""
    schedule_id = new_uuid()
    await conn.execute(
        f'INSERT INTO "{schema}".cron_schedules '  # noqa: S608  # Why: schema is a test-fixture identifier; values are $-bound.
        "(id, actor, name, cron_expr, timezone, dst_strategy, payload_factory, "
        "enabled, next_fire_at, metadata, identity_key, consecutive_failures) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, '{}'::jsonb, $10, $11)",
        schedule_id,
        actor,
        name,
        cron_expr,
        timezone,
        dst_strategy,
        payload_factory,
        enabled,
        next_fire_at,
        identity_key,
        consecutive_failures,
    )
    return schedule_id


async def schedule_row(conn: asyncpg.Connection, schema: str, schedule_id: UUID) -> dict[str, Any]:
    """The full schedule row as a dict, for bit-for-bit before/after compares."""
    row = await conn.fetchrow(
        f'SELECT * FROM "{schema}".cron_schedules WHERE id = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; id is $-bound.
        schedule_id,
    )
    assert row is not None
    return dict(row)


async def jobs_for_identity(
    conn: asyncpg.Connection,
    schema: str,
    identity_key: str,
) -> list[dict[str, Any]]:
    return [
        dict(r)
        for r in await conn.fetch(
            f"SELECT id, actor, queue, status::text AS status, identity_key, payload, "  # noqa: S608  # Why: schema is a test-fixture identifier; identity is $-bound.
            f"scheduled_at, max_attempts, retry_kind::text AS retry_kind "
            f'FROM "{schema}".jobs WHERE identity_key = $1 ORDER BY scheduled_at, id',
            identity_key,
        )
    ]


async def count_jobs(conn: asyncpg.Connection, schema: str, actor: str) -> int:
    total: int | None = await conn.fetchval(
        f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1',  # noqa: S608  # Why: schema is a test-fixture identifier; actor is $-bound.
        actor,
    )
    assert total is not None
    return total


async def server_now(conn: asyncpg.Connection) -> datetime:
    now: datetime | None = await conn.fetchval("SELECT clock_timestamp()")
    assert now is not None
    return now


def hour_floor(now: datetime) -> datetime:
    """The current hour boundary (on-grid for ``0 * * * *``)."""
    return now.replace(minute=0, second=0, microsecond=0)


def ten_min_floor(now: datetime) -> datetime:
    """The current 10-minute boundary (on-grid for ``*/10`` and ``*/5``)."""
    return now.replace(minute=now.minute - now.minute % 10, second=0, microsecond=0)


def next_ten_min_boundary(after: datetime) -> datetime:
    """The first 10-minute grid point strictly after *after*."""
    floored = ten_min_floor(after)
    return floored if floored > after else floored + timedelta(minutes=10)


# ── The wedge: planning-time pause inside the tick's transaction ───────


_WEDGE_STATE: dict[str, object] = {}
"""Set by :func:`wedge_events` before the tick starts; the factories below
read it.  Module-scope mutable state is safe here: tests run sequentially
on the module-scoped event loop and always consume the wedge inside the
awaited tick that set it."""


async def wedge_then_fail() -> dict[str, object]:
    """Payload factory: hold planning open, then fail the schedule.

    Dotted path: ``tests.test_rt_cron_harness.wedge_then_fail``.
    """
    entered = _WEDGE_STATE["entered"]
    gate = _WEDGE_STATE["gate"]
    assert isinstance(entered, asyncio.Event)
    assert isinstance(gate, asyncio.Event)
    entered.set()
    await gate.wait()
    raise RuntimeError("wedge: planned failure after the gate opened")


async def wedge_then_succeed() -> dict[str, object]:
    """Payload factory: hold planning open, then return a payload.

    Dotted path: ``tests.test_rt_cron_harness.wedge_then_succeed``.
    """
    entered = _WEDGE_STATE["entered"]
    gate = _WEDGE_STATE["gate"]
    assert isinstance(entered, asyncio.Event)
    assert isinstance(gate, asyncio.Event)
    entered.set()
    await gate.wait()
    return {"wedged": True}


async def hang_past_factory_timeout() -> dict[str, object]:
    """Payload factory that never returns in time.

    Dotted path: ``tests.test_rt_cron_harness.hang_past_factory_timeout``.
    ``resolve_payload`` wraps async factories in ``asyncio.wait_for(…,
    timeout=5.0)``; this sleeps past it so the tick must cut the planning
    off or hold its transaction (and the cron advisory lock) open forever.
    """
    await asyncio.sleep(30)
    return {}


@contextlib.contextmanager
def wedge_events() -> Generator[tuple[asyncio.Event, asyncio.Event], None, None]:
    """Install the wedge state; yields ``(entered, gate)``.

    ``entered`` fires when the tick reaches the wedged schedule's payload
    resolution (its transaction and advisory lock are open, the due set
    is read); open ``gate`` to let the tick continue.
    """
    entered = asyncio.Event()
    gate = asyncio.Event()
    _WEDGE_STATE["entered"] = entered
    _WEDGE_STATE["gate"] = gate
    try:
        yield entered, gate
    finally:
        _WEDGE_STATE.clear()
