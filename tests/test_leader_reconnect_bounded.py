"""Contract: the leader election loop's connection rebuilds are bounded.

``MaintenanceLeader._open_leader_conn`` and ``_open_dedicated_conn`` await
``deps.leader_conn_factory()`` - a caller-supplied callable - when rebuilding
the advisory-lock, monitor, and cron connections. The notify listener's
identical rebuild factory call is bounded with ``settings.reload_factory_timeout``
(``taskq/worker/notify.py``); unbounded, a hung token endpoint parks the
election loop past every staleness budget, and the in-worker watchdog's
stale-loop detector force-exits the whole worker instead of the loop's own
retry/backoff handling it. The bound's exhaustion is the loop's ordinary
factory-failure path: logged, heartbeat-interval backoff, retry - never a
crash and never a stall.

Docker-free: hand-rolled fakes driving the REAL ``_election_loop`` (fake
conventions mirror ``tests/test_leader.py``). No ``pytestmark`` - must run
under ``pytest -m "not integration"``.
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.worker.deps import WorkerDeps
from taskq.worker.leader import MaintenanceLeader

# ── Test helpers ───────────────────────────────────────────────────────

# Production's documented bound for factory calls, shrunk so a
# production-side bound fires well inside the test budget below.
_PROD_BOUND_SECS = 0.05
# The loop's retry cadence, shrunk so several retry cycles fit in the budget.
_HEARTBEAT_SECS = 0.01
# Generous vs the production bound: this budget firing is the red result.
_TEST_BUDGET_SECS = 5.0

_WORKER_ID = UUID("00000000-0000-0000-0000-000000000002")


def _make_settings(**overrides: str) -> WorkerSettings:
    base: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://fake:fake@fake:5432/fake",
        "TASKQ_PG_DSN_DIRECT": "postgresql://fake:fake@fake:5432/fake",
        "TASKQ_PG_DSN_POOLED": "postgresql://fake:fake@fake:5432/fake",
        "TASKQ_HEARTBEAT_INTERVAL": str(_HEARTBEAT_SECS),
        "reload_factory_timeout": str(_PROD_BOUND_SECS),
    }
    base.update(overrides)
    return WorkerSettings.load_from_dict(base, validate=False)


class _FakeConn:
    """Fake asyncpg.Connection for the election loop (mirrors test_leader.py)."""

    def __init__(self, *, fetchval_result: object = None) -> None:
        self.fetchval_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self._closed = False
        self.close_calls = 0
        self.terminated = False
        self.close_wait = asyncio.Event()
        self.close_wait.set()
        self.closed_event = asyncio.Event()
        self._fetchval_result = fetchval_result

    async def fetchval(self, sql: str, *args: object) -> object:
        self.fetchval_calls.append((sql, args))
        if "maintenance_leader" in sql:
            # The lease statement answers with the won term's elected_at, or
            # no row at all; the generic result models the advisory-lock
            # probe and would report a win to a pod that lost.
            return datetime.now(UTC) if self._fetchval_result else None
        return self._fetchval_result

    async def execute(self, sql: str, *args: object) -> str:
        self.execute_calls.append((sql, args))
        return "UPDATE 1"

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        return None

    async def fetch(self, sql: str, *args: object) -> list[object]:
        return []

    async def close(self) -> None:
        self.close_calls += 1
        await self.close_wait.wait()
        self._closed = True
        self.closed_event.set()

    def terminate(self) -> None:
        self.terminated = True
        self._closed = True
        self.closed_event.set()
        self.close_wait.set()

    def is_closed(self) -> bool:
        return self._closed


class _FakePool:
    """Fake asyncpg.Pool placeholder - the election loop never acquires."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _make_deps(
    *,
    settings: WorkerSettings,
    leader_conn: _FakeConn | None,
    leader_conn_factory: Any,
) -> WorkerDeps:
    return WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]  # Why: fake stands in for the asyncpg C-extension type in a docker-free test.
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]
        worker_pool=_FakePool(),  # type: ignore[arg-type]
        notify_conn=None,
        leader_conn=leader_conn,  # type: ignore[arg-type]
        leader_conn_factory=leader_conn_factory,
    )


def _make_leader(deps: WorkerDeps) -> MaintenanceLeader:
    clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    backend = InMemoryBackend(clock=clock)
    return MaintenanceLeader(deps, _WORKER_ID, backend, clock=clock)


async def _factory_calls_reach(factory_calls: list[int], n: int) -> None:
    """Bounded wait until the hanging factory has been entered n times.

    RED pre-fix: the loop is parked inside the FIRST factory() call - no
    retry ever happens and this bounded wait fails by name.
    """
    deadline = asyncio.get_running_loop().time() + _TEST_BUDGET_SECS
    while factory_calls[0] < n:
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail(
                f"the election loop is parked inside factory() "
                f"(entered {factory_calls[0]} time(s), needed {n}): a hung "
                "leader_conn_factory stalls the rebuild unboundedly, so the "
                "loop's own retry/backoff never takes over and the in-worker "
                "watchdog force-exits the whole worker instead."
            )
        await asyncio.sleep(0.01)


# ── The rebuilds must be bounded, with the loop retrying ───────────────


async def test_election_leader_conn_rebuild_factory_hang_is_bounded_and_retried() -> None:
    """A leader_conn_factory that never returns is bounded by
    reload_factory_timeout: the timeout flows into the election loop's
    existing open-failure handling (logged, heartbeat backoff, retry) -
    the factory is entered again - and the loop stays responsive to
    shutdown instead of parking forever."""
    settings = _make_settings()
    factory_calls = [0]
    factory_entered = asyncio.Event()
    never = asyncio.Event()  # never set: the credential provider hangs

    async def hung_factory() -> asyncpg.Connection:
        factory_calls[0] += 1
        factory_entered.set()
        await never.wait()
        raise AssertionError("unreachable: the hang gate is never set")

    deps = _make_deps(settings=settings, leader_conn=None, leader_conn_factory=hung_factory)
    leader = _make_leader(deps)
    shutdown = asyncio.Event()

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the production election loop IS the test.
    try:
        await asyncio.wait_for(factory_entered.wait(), timeout=_TEST_BUDGET_SECS)
        # Two entries prove the first call's bound fired and the loop's own
        # retry took over - pre-fix the loop parks inside call one.
        await _factory_calls_reach(factory_calls, 2)
        assert not deps.is_leader.is_set(), "no conn, no leadership"

        # The loop must stay responsive: shutdown set between retries ends
        # it on the next while-check - a loop still parked in factory()
        # would hang here.
        shutdown.set()
        await asyncio.wait_for(task, timeout=_TEST_BUDGET_SECS)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_election_dedicated_conn_rebuild_factory_hang_is_bounded_and_retried() -> None:
    """After winning the lock, the monitor/cron dedicated connections are
    rebuilt through the same leader_conn_factory; a factory that never
    returns is bounded by reload_factory_timeout: the timeout flows into
    the loop's existing dedicated-conn-failure handling (conn dropped,
    logged, heartbeat backoff, retry), is_leader is never set on a partial
    win, and the loop stays responsive to shutdown."""
    settings = _make_settings()
    # Wins the advisory lock and survives the upsert; the hang is in the
    # factory the _open_dedicated_conn rebuild path calls.
    leader_conn = _FakeConn(fetchval_result=True)
    factory_calls = [0]
    dedicated_entered = asyncio.Event()
    never = asyncio.Event()  # never set: the credential provider hangs

    async def hung_factory() -> asyncpg.Connection:
        factory_calls[0] += 1
        dedicated_entered.set()
        await never.wait()
        raise AssertionError("unreachable: the hang gate is never set")

    deps = _make_deps(settings=settings, leader_conn=leader_conn, leader_conn_factory=hung_factory)
    leader = _make_leader(deps)
    shutdown = asyncio.Event()

    task = asyncio.create_task(leader._election_loop(shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the production election loop IS the test.
    try:
        await asyncio.wait_for(dedicated_entered.wait(), timeout=_TEST_BUDGET_SECS)
        assert any("pg_try_advisory_lock" in sql for sql, _ in leader_conn.fetchval_calls), (
            "the test must reach the dedicated-conn open via a won election"
        )
        # The failed dedicated open drops the leader conn, so the next
        # cycle reopens through the same factory: a second entry proves the
        # bound fired and the loop's own retry took over.
        await _factory_calls_reach(factory_calls, 2)
        assert not deps.is_leader.is_set(), (
            "a win whose dedicated conns never opened must not claim leadership"
        )

        shutdown.set()
        await asyncio.wait_for(task, timeout=_TEST_BUDGET_SECS)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
