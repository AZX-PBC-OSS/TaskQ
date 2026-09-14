"""Contract: ``open_worker_deps`` bounds its bootstrap notify establishment.

When a caller supplies ``WorkerConnections.notify_conn_factory``, the
bootstrap open of the notify connection — the first ``factory()`` call and
the ``LISTEN`` execute that puts the factory-built connection into
subscription state — completes or raises within a bounded time. The notify
listener's reconnect path bounds the same two operations with
``settings.reload_factory_timeout`` and
``settings.notify_listener_setup_timeout``; the bootstrap open runs
before any watchdog is armed, so an unbounded hang there wedges worker
startup with nothing to detect or recover it. A black-holed credential
provider, or a factory-built connection that completes the handshake and
then stalls on the ``LISTEN`` execute, must fail the open within the
configured bounds — not park it forever.

Docker-free: hand-rolled fakes wired through the REAL ``open_worker_deps``
via ``WorkerConnections`` factories (asyncpg types are C-extensions — no
MagicMock for pool/conn). Fake conventions mirror
``tests/test_worker_deps_teardown.py``.

No ``pytestmark`` — must run under ``pytest -m "not integration"``.
"""

from __future__ import annotations

import asyncio
from typing import Any, Self

import asyncpg
import pytest

from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.worker.deps import open_worker_deps

# ── Test helpers ───────────────────────────────────────────────────────

# Production's documented bounds for the notify factory call and the
# LISTEN execute, shrunk so a production-side bound fires well inside the
# test budget below: an open that applies them raises within ~0.5s, while
# an open that does not parks forever and the TEST budget is what fires.
_PROD_BOUND_SECS = "0.5"
# 10x the configured production bound — generous. This budget firing is
# the red result: production never bounded the bootstrap open on its own.
_TEST_BUDGET_SECS = 5.0


def _make_settings(**overrides: str) -> WorkerSettings:
    """Build WorkerSettings from a dict, bypassing .env discovery."""
    base: dict[str, str] = {
        "TASKQ_PG_DSN": "postgresql://fake:fake@fake:5432/fake",
        "TASKQ_PG_DSN_DIRECT": "postgresql://fake:fake@fake:5432/fake",
        "TASKQ_PG_DSN_POOLED": "postgresql://fake:fake@fake:5432/fake",
        "TASKQ_HEALTH_ENABLED": "false",
        "TASKQ_NOTIFY_ENABLED": "true",
        "notify_listener_setup_timeout": _PROD_BOUND_SECS,
        "reload_factory_timeout": _PROD_BOUND_SECS,
    }
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


class _FakePool:
    """Fake asyncpg.Pool: instant bounded close, no hang gates."""

    def __init__(self, name: str = "") -> None:
        self.name = name
        self.closed = False
        self.terminated = False
        self.close_calls = 0

    async def close(self) -> None:
        self.close_calls += 1
        if self.closed or self.terminated:
            return
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True

    def is_closing(self) -> bool:
        return self.closed

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        pass


class _FakeConn:
    """Fake asyncpg.Connection whose ``LISTEN`` execute parks forever.

    The ``listen_gate`` event is never set by the test: a connection that
    completes the factory handshake and then black-holes on the LISTEN
    execute. ``terminate`` opens the gate so a bounded-close fallback
    cannot wedge an unwind either.
    """

    def __init__(self, name: str = "") -> None:
        self.name = name
        self.closed = False
        self.terminated = False
        self.close_calls = 0
        self.executed: list[str] = []
        self.listen_gate = asyncio.Event()

    async def execute(self, sql: str, *_args: object) -> str:
        self.executed.append(sql)
        if sql.startswith("LISTEN"):
            await self.listen_gate.wait()
        return "OK"

    async def close(self) -> None:
        self.close_calls += 1
        if self.closed or self.terminated:
            return
        self.closed = True

    def terminate(self) -> None:
        self.terminated = True
        self.closed = True
        self.listen_gate.set()

    def is_closed(self) -> bool:
        return self.closed


def _make_pool_factory(fake: _FakePool) -> Any:
    """Wrap a fake pool in the zero-arg async factory shape WorkerConnections takes."""

    async def factory() -> asyncpg.Pool:
        return fake  # type: ignore[return-value]

    return factory


def _make_conn_factory(fake: _FakeConn) -> Any:
    """Wrap a fake conn in the zero-arg async factory shape WorkerConnections takes."""

    async def factory() -> asyncpg.Connection:
        return fake  # type: ignore[return-value]

    return factory


# ── The bootstrap notify open must be bounded ──────────────────────────


async def test_bootstrap_notify_factory_call_is_bounded() -> None:
    """The FIRST notify factory() call during the bootstrap open completes
    or raises within the configured bound. A credential provider that
    accepts the call and never returns must fail the open — the reconnect
    loop already bounds its identical factory call with
    reload_factory_timeout — not park worker startup forever before any
    watchdog is armed."""
    settings = _make_settings()
    factory_entered = asyncio.Event()
    never = asyncio.Event()  # never set: the credential provider hangs

    async def black_hole_factory() -> asyncpg.Connection:
        factory_entered.set()
        await never.wait()
        raise AssertionError("unreachable: the hang gate is never set")

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory(_FakePool("dispatcher")),
        heartbeat_pool_factory=_make_pool_factory(_FakePool("heartbeat")),
        worker_pool_factory=_make_pool_factory(_FakePool("worker")),
        notify_conn_factory=black_hole_factory,
        leader_conn_factory=_make_conn_factory(_FakeConn("leader")),
    )

    cm = open_worker_deps(settings, connections=conns)
    budget = asyncio.timeout(_TEST_BUDGET_SECS)
    try:
        async with budget:
            deps = await cm.__aenter__()
    except TimeoutError:
        # The factory was entered (asserted below) and never returns, so a
        # TimeoutError here can only be a bound production applied to the
        # hung call. If it is the TEST budget that fired instead, the open
        # never returns on its own: bootstrap hangs.
        assert factory_entered.is_set(), "open failed before reaching the notify factory call"
        if budget.expired():
            pytest.fail(
                "open_worker_deps parks forever in the bootstrap notify "
                f"factory() call: still suspended {_TEST_BUDGET_SECS:.0f}s in "
                f"with reload_factory_timeout={_PROD_BOUND_SECS}s and "
                f"notify_listener_setup_timeout={_PROD_BOUND_SECS}s configured. "
                "The reconnect loop bounds the same factory call with "
                "reload_factory_timeout; the bootstrap open runs before any "
                "watchdog is armed, so a black-holed user-supplied "
                "notify_conn_factory wedges worker startup undetected."
            )
        return  # production raised its own typed timeout: bounded
    assert factory_entered.is_set()
    assert deps.notify_conn is not None, "the open must yield deps with the notify conn set"
    async with asyncio.timeout(_TEST_BUDGET_SECS):
        await cm.__aexit__(None, None, None)


async def test_bootstrap_notify_listen_execute_is_bounded() -> None:
    """The bootstrap LISTEN execute completes or raises within the
    configured bound. A factory-built connection that completes the
    handshake and then stalls on LISTEN must fail the open — the reconnect
    loop already bounds its identical LISTEN execute with
    notify_listener_setup_timeout — not park worker startup forever before
    any watchdog is armed."""
    settings = _make_settings()
    notify = _FakeConn("notify")  # LISTEN execute parks forever

    conns = WorkerConnections(
        dispatcher_pool_factory=_make_pool_factory(_FakePool("dispatcher")),
        heartbeat_pool_factory=_make_pool_factory(_FakePool("heartbeat")),
        worker_pool_factory=_make_pool_factory(_FakePool("worker")),
        notify_conn_factory=_make_conn_factory(notify),
        leader_conn_factory=_make_conn_factory(_FakeConn("leader")),
    )

    cm = open_worker_deps(settings, connections=conns)
    budget = asyncio.timeout(_TEST_BUDGET_SECS)
    try:
        async with budget:
            deps = await cm.__aenter__()
    except TimeoutError:
        # LISTEN was issued (asserted below) and never returned, so a
        # TimeoutError here can only be a bound production applied to the
        # hung execute. If it is the TEST budget that fired instead, the
        # open never returns on its own: bootstrap hangs.
        assert any(sql.startswith("LISTEN") for sql in notify.executed), (
            "open failed before reaching the bootstrap LISTEN execute"
        )
        if budget.expired():
            pytest.fail(
                "open_worker_deps parks forever in the bootstrap notify "
                "LISTEN execute: the factory-built connection completed the "
                f"handshake and LISTEN never returned {_TEST_BUDGET_SECS:.0f}s "
                f"in, with notify_listener_setup_timeout={_PROD_BOUND_SECS}s "
                "configured. The reconnect loop bounds the same execute with "
                "notify_listener_setup_timeout; the bootstrap LISTEN has no "
                "bound, and the open runs before any watchdog is armed."
            )
        return  # production raised its own typed timeout: bounded
    assert deps.notify_conn is notify
    async with asyncio.timeout(_TEST_BUDGET_SECS):
        await cm.__aexit__(None, None, None)
