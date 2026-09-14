"""Red-team attacks on the sweep breaker wiring (``_run_bounded_sweep``).

``PostgresBackend._run_bounded_sweep`` wraps every sweep call: it records
the batch size the call will actually use, runs the sweep, and routes
the two timeout shapes an aborted batch produces — server-side
``statement_timeout`` cancellation (``asyncpg.QueryCanceledError``,
SQLSTATE 57014) and client-side ``command_timeout``
(``TimeoutError``) — into the per-sweep ``SweepBatchSizer`` breaker while
re-raising for the caller's transient-error handling.

The sizer's state machine itself is unit-pinned in
``test_sweep_backend_parity.py`` (latch at threshold, one-way latch,
rolling window, reduced-tier floor).  This file attacks the WIRING,
which nothing pins:

* a REAL server-side cancellation (a 50 ms statement_timeout aborting a
  pg_sleep, on a real connection) counts — and the reduced size is what
  the NEXT call actually runs with, with the gauge reporting it;
* a non-timeout error does NOT count (the breaker must not latch on
  ordinary sweep failures — those belong to the caller's retry policy);
* a client-side ``TimeoutError`` counts;
* an explicit ``batch_size`` override bypasses the latched tier for that
  call only, and the gauge reports the override (reality, not
  configuration);
* concurrent failing calls sharing one sizer all count — the failure
  accounting must not lose updates when two drains overlap.

The unit pins for the latch semantics live elsewhere, so every
assertion here is at the observable seam: the size argument the run
callable receives, and the recorded gauge pairs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TYPE_CHECKING

import asyncpg
import pytest

from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.settings import WorkerSettings
from taskq.testing.fixtures import ModulePgSchema

if TYPE_CHECKING:
    from pytest import MonkeyPatch

pytestmark = pytest.mark.integration

_Run = Callable[[int, int], Awaitable[int]]

# Settings defaults this file relies on (from WorkerSettings):
# event_writer_batch_size=100, reduced divisor=4 -> 25, failure
# threshold=3, window=600s, statement timeout=1750ms.
_DEFAULT_SIZE = 100
_REDUCED_SIZE = 25
_THRESHOLD = 3


class _StubBackendDeps:
    """Minimal BackendDeps: _run_bounded_sweep reads only ``settings``."""

    def __init__(self, settings: WorkerSettings) -> None:
        self.settings = settings
        self.worker_pool = None
        self.heartbeat_pool = None
        self.dispatcher_pool = None


def _make_backend(schema: str) -> PostgresBackend:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SCHEMA_NAME": schema,
        },
        validate=False,
    )
    return PostgresBackend(
        _StubBackendDeps(settings),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps; _run_bounded_sweep reads only settings.
        clock=SystemClock(),
        cancellation_grace_period=timedelta(seconds=0),
        cleanup_grace_period=timedelta(seconds=0),
    )


def _gauge_spy(monkeypatch: MonkeyPatch) -> list[tuple[str, int]]:
    """Intercept the sweep-batch-size gauge feed, returning what it saw."""
    recorded: list[tuple[str, int]] = []
    monkeypatch.setattr(
        "taskq.backend.postgres.record_sweep_batch_size",
        lambda sweep_name, size: recorded.append((sweep_name, size)),
    )
    return recorded


def _cancelled_run(conn: asyncpg.Connection) -> _Run:
    """A run callable whose batch REALLY is cancelled server-side."""

    async def run(size: int, timeout_ms: int) -> int:
        async with conn.transaction():
            # The sweep's own SET LOCAL shape: the timeout binds the very
            # next statement, which sleeps well past it.
            await conn.execute("SET LOCAL statement_timeout = 50")
            await conn.execute("SELECT pg_sleep(0.5)")
        return 0

    return run


async def test_real_server_cancellation_reduces_the_next_call_and_feeds_the_gauge(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    monkeypatch: MonkeyPatch,
) -> None:
    """Three real 57014 aborts latch the breaker; the NEXT call runs at
    the reduced tier and the gauge reports it.

    The gauge must report the size the call actually used — the reduced
    tier after latching — because a worker reporting the reduced tier is
    reporting an unhealthy database and must not be silent about it.
    """
    schema = module_pg_schema.schema_name
    backend = _make_backend(schema)
    gauge = _gauge_spy(monkeypatch)
    failing = _cancelled_run(clean_pg_conn)

    for i in range(_THRESHOLD):
        with pytest.raises(asyncpg.QueryCanceledError):
            await backend._run_bounded_sweep("expired_locks", None, failing)  # pyright: ignore[reportPrivateUsage]  # Why: the wiring under attack is the private helper itself.
        assert gauge[-1] == ("expired_locks", _DEFAULT_SIZE), (
            f"failure {i + 1} must still run at the default tier (the latch needs the full streak)"
        )

    sizes_seen: list[int] = []

    async def ok_run(size: int, timeout_ms: int) -> int:
        sizes_seen.append(size)
        return 0

    count = await backend._run_bounded_sweep("expired_locks", None, ok_run)  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.
    assert count == 0
    assert sizes_seen == [_REDUCED_SIZE], (
        f"after {_THRESHOLD} cancellations the next call must run at the "
        f"reduced tier {_REDUCED_SIZE}, ran with {sizes_seen}"
    )
    assert gauge[-1] == ("expired_locks", _REDUCED_SIZE), (
        "the gauge must report the reduced size the latched call actually used"
    )

    # A success does not unlatch: the tier stays reduced for the rest of
    # the backend's lifetime.
    await backend._run_bounded_sweep("expired_locks", None, ok_run)  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.
    assert sizes_seen == [_REDUCED_SIZE, _REDUCED_SIZE]


async def test_non_timeout_errors_do_not_count_toward_the_breaker(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    monkeypatch: MonkeyPatch,
) -> None:
    """Ordinary sweep failures must not latch the breaker.

    The breaker's control signal is a cancelled batch (the database
    refusing a full-size bite); an ordinary error belongs to the
    caller's retry policy, and latching on it would degrade batch sizes
    for reasons that have nothing to do with batch duration.
    """
    schema = module_pg_schema.schema_name
    backend = _make_backend(schema)
    _gauge_spy(monkeypatch)

    async def broken_run(size: int, timeout_ms: int) -> int:
        raise RuntimeError("ordinary sweep failure")

    for _ in range(_THRESHOLD + 2):
        with pytest.raises(RuntimeError, match="ordinary sweep failure"):
            await backend._run_bounded_sweep("deadline_exceeded", None, broken_run)  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.

    sizes_seen: list[int] = []

    async def ok_run(size: int, timeout_ms: int) -> int:
        sizes_seen.append(size)
        return 0

    await backend._run_bounded_sweep("deadline_exceeded", None, ok_run)  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.
    assert sizes_seen == [_DEFAULT_SIZE], (
        "non-timeout errors must leave the breaker unlatched — the next call "
        f"still runs at {_DEFAULT_SIZE}, ran with {sizes_seen}"
    )


async def test_client_side_timeout_counts_toward_the_breaker(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    monkeypatch: MonkeyPatch,
) -> None:
    """A client-side command_timeout (TimeoutError) is the other shape an
    aborted batch produces and must count like the server-side one."""
    schema = module_pg_schema.schema_name
    backend = _make_backend(schema)
    _gauge_spy(monkeypatch)

    async def client_timeout_run(size: int, timeout_ms: int) -> int:
        raise TimeoutError("client command_timeout")

    for _ in range(_THRESHOLD):
        with pytest.raises(TimeoutError):
            await backend._run_bounded_sweep(  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.
                "scheduled_to_pending", None, client_timeout_run
            )

    sizes_seen: list[int] = []

    async def ok_run(size: int, timeout_ms: int) -> int:
        sizes_seen.append(size)
        return 0

    await backend._run_bounded_sweep(  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.
        "scheduled_to_pending", None, ok_run
    )
    assert sizes_seen == [_REDUCED_SIZE], (
        f"three client-side timeouts must latch; next call ran with {sizes_seen}"
    )


async def test_explicit_batch_size_override_bypasses_the_tier_for_that_call(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    monkeypatch: MonkeyPatch,
) -> None:
    """An explicit batch_size overrides the latched tier for one call.

    The override is the operator's manual escape hatch from a latched
    breaker; it must reach BOTH the sweep (the run's size argument) and
    the gauge (which reports reality — the override — not the tier).
    """
    schema = module_pg_schema.schema_name
    backend = _make_backend(schema)
    gauge = _gauge_spy(monkeypatch)
    failing = _cancelled_run(clean_pg_conn)

    for _ in range(_THRESHOLD):
        with pytest.raises(asyncpg.QueryCanceledError):
            await backend._run_bounded_sweep("expired_locks", None, failing)  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.

    sizes_seen: list[int] = []

    async def ok_run(size: int, timeout_ms: int) -> int:
        sizes_seen.append(size)
        return 0

    await backend._run_bounded_sweep(  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.
        "expired_locks", 7, ok_run
    )
    assert sizes_seen == [7], "the override must be the size the call runs with"
    assert gauge[-1] == ("expired_locks", 7), "the gauge must report the override"


async def test_concurrent_failing_calls_all_count(
    clean_pg_conn: asyncpg.Connection,
    module_pg_schema: ModulePgSchema,
    monkeypatch: MonkeyPatch,
) -> None:
    """Overlapping failing calls sharing one sizer must not lose counts.

    Two drains can overlap on the leader (each batch its own connection);
    the sizer's failure accounting is the shared state.  A lost update
    here would need a longer streak to latch, quietly widening the
    window in which full-size batches keep being attempted against a
    database that is already refusing them.
    """
    schema = module_pg_schema.schema_name
    backend = _make_backend(schema)
    _gauge_spy(monkeypatch)
    # One connection per concurrent call: asyncpg connections execute one
    # operation at a time, so two overlapping runs on one connection raise
    # InterfaceError before any batch statement is even attempted — a
    # caller bug, not the concurrency the breaker must account for.
    conn2 = await asyncpg.connect(module_pg_schema.pg_dsn)
    conn3 = await asyncpg.connect(module_pg_schema.pg_dsn)
    failing1 = _cancelled_run(clean_pg_conn)
    failing2 = _cancelled_run(conn2)
    failing3 = _cancelled_run(conn3)
    try:
        results = await asyncio.gather(
            backend._run_bounded_sweep("expired_locks", None, failing1),  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.
            backend._run_bounded_sweep("expired_locks", None, failing2),  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.
            backend._run_bounded_sweep("expired_locks", None, failing3),  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.
            return_exceptions=True,
        )
        assert all(isinstance(r, asyncpg.QueryCanceledError) for r in results), (
            f"every concurrent call must surface its cancellation, got {results!r}"
        )

        sizes_seen: list[int] = []

        async def ok_run(size: int, timeout_ms: int) -> int:
            sizes_seen.append(size)
            return 0

        await backend._run_bounded_sweep("expired_locks", None, ok_run)  # pyright: ignore[reportPrivateUsage]  # Why: drives the private wrapper whose wiring is under attack.
        assert sizes_seen == [_REDUCED_SIZE], (
            f"three concurrent cancellations must latch with no lost counts; "
            f"next call ran with {sizes_seen}"
        )
    finally:
        await conn2.close()
        await conn3.close()
