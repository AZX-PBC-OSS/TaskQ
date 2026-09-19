"""Queue-mode resolution cache pins: TTL pickup and explicit invalidation.

Every ``dispatch_batch`` opened its transaction with a queue-mode resolve
round trip (``SELECT name, mode FROM queues WHERE name = ANY($1)``) for
data that changes only through queue ops. The worker-side
:class:`~taskq.backend._dispatch.QueueModeCache` collapses that per-round
statement to a per-TTL resolve. These pins hold the two halves that make
the collapse safe:

* TTL - a mode change made out of band (another process, raw SQL) is
  picked up once the entry expires: the next dispatch re-resolves through
  the existing query, never through a cached stale mode.
* invalidation - a mode change made in THIS process through the queue-ops
  seam (``set_queue_mode``) is picked up by the very next dispatch: the
  seam clears every live cache, so no TTL window applies to the process
  that made the change.
* the miss path - unknown queues still resolve through the query with the
  ``strict_fifo`` fallback, and an absent row is cached with the same TTL
  bound (a queue configured mid-flight is picked up within the TTL, the
  same propagation bound a mode change gets).

The dispatch-level pins drive the real ``_dispatch_batch`` against a
counting connection: the resolve statement count IS the property under
test, exactly as in ``tests/test_dispatch_event_batching.py``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

from taskq._ids import new_uuid
from taskq.backend._dispatch import (
    QUEUE_MODE_CACHE_TTL_SECONDS,
    QueueModeCache,
    _dispatch_batch,
)
from taskq.backend._sql_templates import render
from taskq.testing.clock import FakeClock
from taskq.worker.queue_ops import set_queue_mode

_CLOCK_START = datetime(2025, 1, 1, tzinfo=UTC)


def _fake_clock() -> FakeClock:
    return FakeClock(_CLOCK_START)


class _FakeDispatchConn:
    """Serves the queue-mode resolve from a mutable row map and records
    every dispatch-CTE fetch, distinguishing the three statements by shape:
    the resolve query reads the queues *table* (``.queues WHERE``); the
    dispatch CTE's ``queues`` is a params column - qualified references
    like ``p.queues`` in the idle-actor prefilter must NOT match; the
    empty-round claimable-rows probe (``ac.queue = ANY($1...)``) is the
    window-expansion gate - this fake's claim returns empty, so the probe
    fires each round and answers "nothing remains" (idle), matching the
    claim's own result.
    """

    def __init__(self, queue_rows: dict[str, str]) -> None:
        self.queue_rows = queue_rows
        self.resolve_fetches = 0
        self.probe_fetches = 0
        self.dispatch_sqls: list[str] = []

    async def fetch(self, sql: str, *args: object) -> list[dict[str, str]]:
        if ".queues WHERE" in sql:
            self.resolve_fetches += 1
            return [{"name": name, "mode": mode} for name, mode in self.queue_rows.items()]
        if "ac.queue = ANY($1::text[])" in sql:
            self.probe_fetches += 1
            return []
        self.dispatch_sqls.append(sql)
        return []

    async def execute(self, sql: str, *args: object) -> str:
        return "INSERT 0 1"

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *args: object) -> None:
        return None


class _FakeDispatchPool:
    """Pool stand-in yielding one fake connection, asyncpg-shaped."""

    def __init__(self, conn: _FakeDispatchConn) -> None:
        self._conn = conn

    @asynccontextmanager
    async def acquire(
        self,
        *,
        timeout: float | None = None,  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire, which _dispatch_batch calls with timeout=.
    ) -> AsyncGenerator[_FakeDispatchConn]:
        yield self._conn


async def _dispatch(
    conn: _FakeDispatchConn,
    cache: QueueModeCache | None,
    *,
    queues: list[str],
) -> None:
    """One _dispatch_batch round through the fake pool."""
    await _dispatch_batch(
        _FakeDispatchPool(conn),  # type: ignore[arg-type]  # Why: duck-typed pool; only acquire() is used.
        render("taskq"),
        2,
        5.0,
        "taskq",
        new_uuid(),
        queues,
        10,
        timedelta(seconds=30),
        queue_mode_cache=cache,
    )


# ── The collapse: one resolve per TTL, not one per round ────────────────


async def test_resolve_runs_once_per_ttl_not_once_per_round() -> None:
    """Within the TTL every dispatch round skips the resolve statement;
    the round still dispatches (the CTE fetch happens each time)."""
    clock = _fake_clock()
    conn = _FakeDispatchConn({"default": "strict_fifo"})
    cache = QueueModeCache(clock=clock.monotonic)

    for _ in range(5):
        await _dispatch(conn, cache, queues=["default"])

    assert conn.resolve_fetches == 1, (
        f"expected ONE queue-mode resolve for 5 dispatch rounds inside the "
        f"TTL, got {conn.resolve_fetches} - the per-round statement is back"
    )
    assert len(conn.dispatch_sqls) == 5, "every round must still dispatch"
    assert conn.probe_fetches == 5, (
        "every round here comes back empty (the fake's claim returns no rows), "
        "so each must pay exactly one claimable-rows probe and never a widened "
        "re-claim - the idle-round cost contract of window expansion"
    )


async def test_mode_change_is_picked_up_within_the_ttl() -> None:
    """An out-of-band mode change (raw SQL / another process) is picked up
    once the TTL expires: the entry goes stale, the next dispatch
    re-resolves, and the newly-read mode selects the SQL variant."""
    clock = _fake_clock()
    conn = _FakeDispatchConn({"default": "strict_fifo"})
    cache = QueueModeCache(clock=clock.monotonic)
    tmpl = render("taskq")

    await _dispatch(conn, cache, queues=["default"])
    assert conn.dispatch_sqls[-1] == tmpl.dispatch_strict_fifo

    # Out-of-band flip - invisible to this process until the TTL expires.
    conn.queue_rows["default"] = "round_robin"
    clock.advance(timedelta(seconds=QUEUE_MODE_CACHE_TTL_SECONDS + 0.1))
    await _dispatch(conn, cache, queues=["default"])

    assert conn.resolve_fetches == 2, "an expired entry must re-resolve"
    assert conn.dispatch_sqls[-1] == tmpl.dispatch_round_robin, (
        "the re-resolved mode must select the round-robin SQL variant - a "
        "stale cached mode after TTL expiry silently changes dispatch "
        "ordering"
    )


# ── Explicit invalidation at the queue-ops seam ─────────────────────────


class _FakeQueueOpsConn:
    """Serves set_queue_mode's UPSERT (RETURNING row) and remembers it."""

    def __init__(self) -> None:
        self.upserts: list[tuple[str, str]] = []

    async def fetchrow(self, sql: str, *args: object) -> dict[str, str | None]:
        name, mode = args[0], args[1]  # type: ignore[index]  # Why: the upsert binds (name, mode) positionally; the row's shape is the test's own fixture.
        assert isinstance(name, str) and isinstance(mode, str)
        self.upserts.append((name, mode))
        return {"name": name, "mode": mode, "max_concurrent": None}


async def test_mode_change_is_picked_up_immediately_after_queue_ops_invalidation() -> None:
    """A mode change made in this process through the queue-ops seam is
    visible to the very next dispatch - no TTL window applies to the
    process that made the change, because set_queue_mode clears every
    live cache."""
    clock = _fake_clock()
    conn = _FakeDispatchConn({"default": "strict_fifo"})
    cache = QueueModeCache(clock=clock.monotonic)
    tmpl = render("taskq")

    await _dispatch(conn, cache, queues=["default"])
    assert conn.dispatch_sqls[-1] == tmpl.dispatch_strict_fifo

    ops_conn = _FakeQueueOpsConn()
    row = await set_queue_mode(ops_conn, "default", "round_robin", schema="taskq")
    assert row.mode == "round_robin"
    assert ops_conn.upserts == [("default", "round_robin")]
    # Both fakes stand in for one database: the upsert's effect is what
    # the dispatch connection's next resolve reads.
    conn.queue_rows["default"] = "round_robin"

    # Clock NOT advanced: only the seam's invalidation can make this miss.
    await _dispatch(conn, cache, queues=["default"])
    assert conn.resolve_fetches == 2, (
        "set_queue_mode must clear the live cache - a same-process mode "
        "change served stale for a full TTL is a self-inflicted "
        "propagation delay"
    )
    assert conn.dispatch_sqls[-1] == tmpl.dispatch_round_robin


# ── The miss path and the fallback ─────────────────────────────────────


async def test_unknown_queue_resolves_via_the_query_and_caches_the_fallback() -> None:
    """A queue with no row resolves through the existing query with the
    strict_fifo fallback (first round), and the absent row is cached with
    the same TTL bound (second round issues no resolve)."""
    clock = _fake_clock()
    conn = _FakeDispatchConn({})  # no queues rows at all
    cache = QueueModeCache(clock=clock.monotonic)
    tmpl = render("taskq")

    await _dispatch(conn, cache, queues=["default"])
    assert conn.resolve_fetches == 1
    assert conn.dispatch_sqls[-1] == tmpl.dispatch_strict_fifo

    await _dispatch(conn, cache, queues=["default"])
    assert conn.resolve_fetches == 1, "an absent row is cached, not re-read"


async def test_without_a_cache_every_round_resolves() -> None:
    """queue_mode_cache=None preserves the helper's standalone contract:
    resolve per call, exactly the pre-cache behaviour."""
    conn = _FakeDispatchConn({"default": "strict_fifo"})

    for _ in range(3):
        await _dispatch(conn, None, queues=["default"])

    assert conn.resolve_fetches == 3


# ── Per-backend-instance ownership ─────────────────────────────────────


async def test_backend_instances_cache_independently() -> None:
    """Two PostgresBackend instances never share mode state: a resolve
    that filled one backend's cache does nothing for the other backend's
    next dispatch - each pays its own one resolve per TTL."""
    from taskq.backend.postgres import PostgresBackend

    clock = _fake_clock()
    mock_clock = Mock()
    mock_clock.monotonic = clock.monotonic

    conn_a = _FakeDispatchConn({"default": "strict_fifo"})
    conn_b = _FakeDispatchConn({"default": "strict_fifo"})
    settings = Mock()
    settings.dispatch_oversample = 2
    settings.dispatcher_command_timeout = 5.0
    settings.schema_name = "taskq"

    def _deps(conn: _FakeDispatchConn) -> Mock:
        deps = Mock()
        deps.settings = settings
        deps.dispatcher_pool = _FakeDispatchPool(conn)
        return deps

    backend_a = PostgresBackend(  # type: ignore[arg-type]  # Why: Mock deps satisfies BackendDeps structurally at runtime; pyright cannot verify the protocol through Mock.
        deps=_deps(conn_a),
        clock=mock_clock,
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )
    backend_b = PostgresBackend(  # type: ignore[arg-type]  # Why: same as above.
        deps=_deps(conn_b),
        clock=mock_clock,
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=30),
    )

    for backend in (backend_a, backend_b):
        for _ in range(2):
            await backend.dispatch_batch(
                worker_id=new_uuid(),
                queues=["default"],
                limit=10,
                lock_lease=timedelta(seconds=30),
            )

    assert conn_a.resolve_fetches == 1
    assert conn_b.resolve_fetches == 1, (
        "backend B must pay its own resolve - a cache shared across "
        "backend instances couples workers that share a process (two "
        "workers, two schemas) into one mode view"
    )
