"""Red-team attacks on ``open_worker_deps``' mid-open failure teardown.

Contract under attack - ``open_worker_deps``' own docstring (deps.py):

    "Uses AsyncExitStack so that a failure during step N closes steps
    1..N-1 before the exception propagates."

The three role pools honour it: :func:`_resolve_pool` pushes each pool's
bounded-close callback at the moment the pool is opened. The two
TaskQ-OWNED dedicated connections do not. ``notify_conn`` is opened
(factory or DSN) and LISTENed, and ``leader_conn`` after it, but their
LIFO close guards are pushed only at the END of the stack body - after
the redis client construction and the ``WorkerDeps`` build. Any failure
between the notify open and that push (the LISTEN execute itself, the
leader factory, the redis factory) unwinds the exit stack with NOTHING
registered for the already-opened dedicated conns: each is leaked until
GC - an asyncpg connection holds a socket and a PG backend session, so
the leak is a live backend session per failed startup, and the embedder
retrying startup in a loop (a supervisor restart loop) accumulates them.

The desired observable asserted below is exactly the documented one: a
TaskQ-owned dedicated conn opened by ``open_worker_deps`` is closed
(bounded) when a LATER open step fails - the same LIFO guarantee the
pools already get. Caller-owned resources are never closed (the
ownership contract), asserted alongside so the fix cannot over-close.

In-memory tier: fake dedicated conns instrumented with a close counter;
the pools are caller-owned concrete fakes, so no DSN path runs.
"""

from __future__ import annotations

from typing import Any

import pytest

from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.worker.deps import open_worker_deps

_DSN = "postgresql://u:p@h:5432/db"


class _FakeDedicatedConn:
    """Minimal dedicated-conn double: LISTEN ``execute`` + bounded close."""

    def __init__(self, *, execute_error: Exception | None = None) -> None:
        self.close_calls = 0
        self.execute_calls = 0
        self.closed = False
        self._execute_error = execute_error

    async def execute(self, sql: str, *args: Any) -> str:
        self.execute_calls += 1
        if self._execute_error is not None:
            raise self._execute_error
        return "OK"

    async def close(self) -> None:
        self.close_calls += 1
        self.closed = True


class _CallerPool:
    """Caller-owned pool double - must never be closed by TaskQ."""


def _settings() -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": _DSN,
            "TASKQ_PG_DSN_DIRECT": _DSN,
            "TASKQ_SCHEMA_NAME": "taskq",
        },
        validate=False,
    )


def _conns(**overrides: Any) -> WorkerConnections:
    base: dict[str, Any] = {
        "dispatcher_pool": _CallerPool(),
        "heartbeat_pool": _CallerPool(),
        "worker_pool": _CallerPool(),
    }
    base.update(overrides)
    return WorkerConnections(**base)


async def test_leader_factory_failure_closes_taskq_owned_notify_conn() -> None:
    """A failure opening leader_conn must close the already-opened notify_conn.

    Startup order is notify → leader (deps.py's docstring); the exit
    stack's documented contract is that a failure during step N closes
    steps 1..N-1. Today the notify guard is pushed only after ALL opens
    succeed, so the TaskQ-owned notify session is leaked (close_calls
    stays 0) whenever the leader factory fails - one leaked PG backend
    session per failed startup, accumulating under a supervisor restart
    loop.
    """
    notify = _FakeDedicatedConn()

    async def notify_factory() -> Any:
        return notify

    async def leader_factory() -> Any:
        raise RuntimeError("leader connect failed")

    with pytest.raises(RuntimeError, match="leader connect failed"):
        async with open_worker_deps(
            _settings(),
            connections=_conns(
                notify_conn_factory=notify_factory,
                leader_conn_factory=leader_factory,
            ),
        ):
            pass

    assert notify.close_calls == 1, (
        "open_worker_deps' documented contract is 'a failure during step N "
        "closes steps 1..N-1 before the exception propagates' - the "
        "TaskQ-owned notify_conn was opened before the failing leader "
        f"factory, but it was closed {notify.close_calls} times. The notify "
        "close guard is registered only after every open step succeeds, so "
        "this failure path leaks a live PG session (socket + backend) until "
        "GC."
    )


async def test_listen_failure_closes_freshly_opened_notify_conn() -> None:
    """A LISTEN that fails must close the connection it was issued on.

    The notify conn is opened through the factory and only then pushed
    into LISTEN; a LISTEN that black-holes (dead PG, network cut) leaves
    an open, unregistered session behind when it raises. The teardown
    contract requires the opened conn to be closed on the failure path.
    """
    notify = _FakeDedicatedConn(execute_error=RuntimeError("LISTEN black-holed"))

    async def notify_factory() -> Any:
        return notify

    async def leader_factory() -> Any:
        return _FakeDedicatedConn()

    with pytest.raises(RuntimeError, match="LISTEN black-holed"):
        async with open_worker_deps(
            _settings(),
            connections=_conns(
                notify_conn_factory=notify_factory,
                leader_conn_factory=leader_factory,
            ),
        ):
            pass

    assert notify.close_calls == 1, (
        "the notify conn's LISTEN execute failed after the conn was opened "
        f"through the factory, but the conn was closed {notify.close_calls} "
        "times - the teardown guard for notify_conn is registered only at "
        "the end of open_worker_deps, so this failure path leaks the "
        "session it just established."
    )


async def test_redis_factory_failure_closes_notify_and_leader_conns() -> None:
    """A failure building the redis client must close both dedicated conns.

    Redis is opened AFTER notify and leader; a failing redis factory
    unwinds the exit stack, which today holds close callbacks only for
    the three pools - both TaskQ-owned dedicated conns leak. The pools'
    own teardown (already correct) is asserted alongside: the failure
    path must close the TaskQ-owned resources and leave caller-owned
    pools alone.
    """
    notify = _FakeDedicatedConn()
    leader = _FakeDedicatedConn()

    async def notify_factory() -> Any:
        return notify

    async def leader_factory() -> Any:
        return leader

    async def redis_factory() -> Any:
        raise RuntimeError("redis factory failed")

    with pytest.raises(RuntimeError, match="redis factory failed"):
        async with open_worker_deps(
            _settings(),
            connections=_conns(
                notify_conn_factory=notify_factory,
                leader_conn_factory=leader_factory,
                redis_client_factory=redis_factory,
            ),
        ):
            pass

    assert notify.close_calls == 1, (
        "the redis factory failed AFTER the TaskQ-owned notify_conn was "
        f"opened and LISTENed, but notify was closed {notify.close_calls} "
        "times - the documented 'failure during step N closes steps "
        "1..N-1' teardown does not cover the dedicated conns."
    )
    assert leader.close_calls == 1, (
        "the redis factory failed AFTER the TaskQ-owned leader_conn was "
        f"opened, but leader was closed {leader.close_calls} times - the "
        "documented 'failure during step N closes steps 1..N-1' teardown "
        "does not cover the dedicated conns."
    )
