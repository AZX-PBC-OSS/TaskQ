"""Contract: ``open_worker_deps`` bounds its bootstrap factory opens, and
``reload_credentials`` bounds its no-listener LISTEN.

The notify seam is already bounded (``tests/test_notify_bootstrap_bounded.py``).
This file pins the remaining bootstrap opens that await a caller-supplied
callable before any watchdog is armed — the leader connection factory, the
redis client factory, and the role pool factories — plus the bare LISTEN
execute on ``reload_credentials``' no-listener fallback path. Every one of
them runs where no detector can see a hang, so each must complete or raise
within the configured bound instead of parking worker startup (or a SIGHUP
reload) forever. The bounds are the SAME settings the already-bounded sibling
paths use — ``reload_factory_timeout`` for factory calls (the notify factory
open, the slot-pool open, the reload path, the notify reconnect loop) and
``notify_listener_setup_timeout`` for LISTEN executes (the bootstrap open,
the listener setup, the reconnect loop) — not second mechanisms.

Docker-free: hand-rolled fakes wired through the REAL ``open_worker_deps`` /
``reload_credentials`` via ``WorkerConnections`` factories (asyncpg types are
C-extensions — no MagicMock for pool/conn). Fake conventions mirror
``tests/test_notify_bootstrap_bounded.py``.

No ``pytestmark`` — must run under ``pytest -m "not integration"``.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import Any, Self

import asyncpg
import pytest

from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.worker.deps import WorkerDeps, open_worker_deps, reload_credentials

# ── Test helpers ───────────────────────────────────────────────────────

# Production's documented bounds for factory calls and LISTEN executes,
# shrunk so a production-side bound fires well inside the test budget
# below: an open that applies them raises within ~0.5s, while an open that
# does not parks forever and the TEST budget is what fires.
_PROD_BOUND_SECS = "0.5"
# 10x the configured production bound — generous. This budget firing is
# the red result: production never bounded the call on its own.
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
    """Fake asyncpg.Connection.

    ``hang_listen`` parks every LISTEN execute forever (a connection that
    completes the factory handshake and then black-holes on LISTEN);
    otherwise every execute answers instantly. ``terminate`` opens the
    gate so a bounded-close fallback cannot wedge an unwind either.
    """

    def __init__(self, name: str = "", *, hang_listen: bool = False) -> None:
        self.name = name
        self.closed = False
        self.terminated = False
        self.close_calls = 0
        self.executed: list[str] = []
        self.hang_listen = hang_listen
        self.listen_gate = asyncio.Event()

    async def execute(self, sql: str, *_args: object) -> str:
        self.executed.append(sql)
        if sql.startswith("LISTEN") and self.hang_listen:
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


def _all_ok_except(**overrides: Any) -> WorkerConnections:
    """WorkerConnections with every PG role factory working, minus overrides.

    Overrides replace the named factory slot (or set it to a hanging one);
    every other role answers instantly so the open reaches the seam under
    test instead of failing on an earlier step.
    """
    slots: dict[str, Any] = {
        "dispatcher_pool_factory": _make_pool_factory(_FakePool("dispatcher")),
        "heartbeat_pool_factory": _make_pool_factory(_FakePool("heartbeat")),
        "worker_pool_factory": _make_pool_factory(_FakePool("worker")),
        "notify_conn_factory": _make_conn_factory(_FakeConn("notify")),
        "leader_conn_factory": _make_conn_factory(_FakeConn("leader")),
        "redis_client_factory": None,
    }
    slots.update(overrides)
    return WorkerConnections(**slots)


# ── Bootstrap factory opens must be bounded ────────────────────────────


async def test_bootstrap_leader_factory_call_is_bounded() -> None:
    """The FIRST leader factory() call during the bootstrap open completes
    or raises within the configured bound. A credential provider that
    accepts the call and never returns must fail the open — the notify
    seam's identical factory call is already bounded with
    reload_factory_timeout — not park worker startup forever before any
    watchdog is armed."""
    settings = _make_settings()
    factory_entered = asyncio.Event()
    never = asyncio.Event()  # never set: the credential provider hangs

    async def black_hole_factory() -> asyncpg.Connection:
        factory_entered.set()
        await never.wait()
        raise AssertionError("unreachable: the hang gate is never set")

    conns = _all_ok_except(leader_conn_factory=black_hole_factory)

    cm = open_worker_deps(settings, connections=conns)
    budget = asyncio.timeout(_TEST_BUDGET_SECS)
    try:
        async with budget:
            await cm.__aenter__()
    except TimeoutError as exc:
        # The factory was entered (asserted below) and never returns, so a
        # TimeoutError here can only be a bound production applied to the
        # hung call. If it is the TEST budget that fired instead, the open
        # never returns on its own: bootstrap hangs.
        assert factory_entered.is_set(), "open failed before reaching the leader factory call"
        if budget.expired():
            pytest.fail(
                "open_worker_deps parks forever in the bootstrap leader "
                f"factory() call: still suspended {_TEST_BUDGET_SECS:.0f}s in "
                f"with reload_factory_timeout={_PROD_BOUND_SECS}s configured. "
                "The notify seam's identical factory call is bounded with "
                "reload_factory_timeout; the bootstrap leader open runs before "
                "any watchdog is armed, so a black-holed user-supplied "
                "leader_conn_factory wedges worker startup undetected."
            )
        assert "leader connection factory" in str(exc), (
            f"the bootstrap timeout must name the operation; got: {exc!r}"
        )
        return  # production raised its own typed timeout: bounded
    pytest.fail("the open must not succeed while the leader factory hangs")


async def test_bootstrap_redis_factory_call_is_bounded() -> None:
    """The redis client factory() call during the bootstrap open completes
    or raises within the configured bound — the same reload_factory_timeout
    treatment as the notify and leader factories — not park worker startup
    forever before any watchdog is armed."""
    settings = _make_settings()
    factory_entered = asyncio.Event()
    never = asyncio.Event()  # never set: the credential provider hangs

    async def black_hole_factory() -> object:
        factory_entered.set()
        await never.wait()
        raise AssertionError("unreachable: the hang gate is never set")

    conns = _all_ok_except(redis_client_factory=black_hole_factory)

    cm = open_worker_deps(settings, connections=conns)
    budget = asyncio.timeout(_TEST_BUDGET_SECS)
    try:
        async with budget:
            await cm.__aenter__()
    except TimeoutError as exc:
        assert factory_entered.is_set(), "open failed before reaching the redis factory call"
        if budget.expired():
            pytest.fail(
                "open_worker_deps parks forever in the bootstrap redis "
                f"client factory() call: still suspended {_TEST_BUDGET_SECS:.0f}s "
                f"in with reload_factory_timeout={_PROD_BOUND_SECS}s configured. "
                "The notify and leader seam factories are bounded with "
                "reload_factory_timeout; a black-holed user-supplied "
                "redis_client_factory wedges worker startup undetected."
            )
        assert "redis client factory" in str(exc), (
            f"the bootstrap timeout must name the operation; got: {exc!r}"
        )
        return  # production raised its own typed timeout: bounded
    pytest.fail("the open must not succeed while the redis factory hangs")


async def test_bootstrap_pool_factory_call_is_bounded() -> None:
    """A user-supplied role pool factory() call during the bootstrap open
    completes or raises within the configured bound — the same
    reload_factory_timeout treatment the slot-pool open already applies —
    not park worker startup forever. (The DSN fallback needs none of this:
    asyncpg's own connect timeout bounds create_pool.)"""
    settings = _make_settings()
    factory_entered = asyncio.Event()
    never = asyncio.Event()  # never set: the credential provider hangs

    async def black_hole_factory() -> asyncpg.Pool:
        factory_entered.set()
        await never.wait()
        raise AssertionError("unreachable: the hang gate is never set")

    conns = _all_ok_except(dispatcher_pool_factory=black_hole_factory)

    cm = open_worker_deps(settings, connections=conns)
    budget = asyncio.timeout(_TEST_BUDGET_SECS)
    try:
        async with budget:
            await cm.__aenter__()
    except TimeoutError as exc:
        assert factory_entered.is_set(), "open failed before reaching the pool factory call"
        if budget.expired():
            pytest.fail(
                "open_worker_deps parks forever in the bootstrap dispatcher "
                f"pool factory() call: still suspended {_TEST_BUDGET_SECS:.0f}s "
                f"in with reload_factory_timeout={_PROD_BOUND_SECS}s configured. "
                "The slot-pool open already bounds its factory with "
                "reload_factory_timeout; a black-holed user-supplied "
                "role pool factory wedges worker startup undetected."
            )
        assert "dispatcher pool factory" in str(exc), (
            f"the bootstrap timeout must name the pool; got: {exc!r}"
        )
        return  # production raised its own typed timeout: bounded
    pytest.fail("the open must not succeed while the pool factory hangs")


# ── reload_credentials' no-listener LISTEN must be bounded ─────────────


async def test_reload_no_listener_listen_execute_is_bounded() -> None:
    """On the no-listener fallback path (listener not started / already
    stopped), the LISTEN execute on the freshly built notify conn completes
    or raises within notify_listener_setup_timeout — the same bound the
    bootstrap open and the reconnect loop apply to the identical execute.
    A conn that completes the factory handshake and then black-holes on
    LISTEN must be reported as a failed resource, not park the reload
    (and with it the reload coordinator) forever."""
    settings = _make_settings()
    old_notify = _FakeConn("notify-old")
    # Completes the factory handshake, then black-holes on LISTEN.
    new_notify = _FakeConn("notify-new", hang_listen=True)

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool("dispatcher"),  # type: ignore[arg-type]  # Why: fake stands in for the asyncpg C-extension type in a docker-free test.
        heartbeat_pool=_FakePool("heartbeat"),  # type: ignore[arg-type]
        worker_pool=_FakePool("worker"),  # type: ignore[arg-type]
        notify_conn=old_notify,  # type: ignore[arg-type]
        leader_conn=None,
        notify_conn_factory=_make_conn_factory(new_notify),
        _exit_stack=AsyncExitStack(),
    )

    reloaded, failed = await asyncio.wait_for(reload_credentials(deps), timeout=_TEST_BUDGET_SECS)
    assert "notify_conn" in failed, (
        f"a LISTEN that never completes must mark the resource failed; "
        f"got reloaded={reloaded!r} failed={failed!r}"
    )
    assert "notify_conn" not in reloaded
    assert deps.notify_conn is old_notify, "an exhausted LISTEN bound must not swap the conn"


async def test_reload_no_listener_happy_path_still_swaps() -> None:
    """The bound must not break the working fallback path: a factory-built
    conn whose LISTEN answers is swapped in and reported reloaded."""
    settings = _make_settings()
    old_notify = _FakeConn("notify-old")
    new_notify = _FakeConn("notify-new")

    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool("dispatcher"),  # type: ignore[arg-type]  # Why: fake stands in for the asyncpg C-extension type in a docker-free test.
        heartbeat_pool=_FakePool("heartbeat"),  # type: ignore[arg-type]
        worker_pool=_FakePool("worker"),  # type: ignore[arg-type]
        notify_conn=old_notify,  # type: ignore[arg-type]
        leader_conn=None,
        notify_conn_factory=_make_conn_factory(new_notify),
        _exit_stack=AsyncExitStack(),
    )

    reloaded, failed = await asyncio.wait_for(reload_credentials(deps), timeout=_TEST_BUDGET_SECS)
    assert "notify_conn" in reloaded
    assert failed == []
    assert deps.notify_conn is new_notify
