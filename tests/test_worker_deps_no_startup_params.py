"""Contract: no TaskQ-built role pool sends ``server_settings`` at boot.

``server_settings`` entries ride asyncpg's STARTUP PACKET, and a pooler
that rejects unknown startup parameters (PgBouncer: ``unsupported startup
parameter: jit``) fails every connect through it — with a single
``TASKQ_PG_DSN`` pointed at the pooler, the worker's eagerly-opened boot
pools (``min_size=1``) never come up (#247). The dispatcher pool carried
``server_settings={"jit": "off"}`` for exactly that reason's opposite: a
JIT guard whose measured win had already moved into the statement itself
(perf-evidence-dispatch.md — the depth oracle passes with JIT enabled on
a plain connection), leaving the startup parameter pure pooler hazard.
The guard remains available server-side without any startup packet
(``ALTER ROLE ... SET jit = off`` / ``?options=-c jit=off`` — see
docs/guides/ops.md §"Database performance knobs").

This pins the boot shape against a quiet re-add: every
``asyncpg.create_pool`` call the DSN bootstrap path makes must omit
``server_settings`` entirely. The slot pool's inherited
``search_path``/``role`` (``worker/_bootstrap.py``, built only when a
LOOP-scope connection is registered) are the one deliberate exception and
are NOT reached by this harness.

Docker-free: ``asyncpg.create_pool`` is monkeypatched with a recorder and
the notify/leader roles are faked through ``WorkerConnections`` factories
(the ``tests/test_deps_bootstrap_bounded.py`` convention — asyncpg types
are C-extensions, no MagicMock for pools), so the real
``open_worker_deps`` runs its whole DSN sequence against fakes.

No ``pytestmark`` — must run under ``pytest -m "not integration"``.
"""

from __future__ import annotations

from typing import Any

import asyncpg
import pytest

from taskq.connections import WorkerConnections
from taskq.settings import WorkerSettings
from taskq.worker.deps import open_worker_deps


class _FakePool:
    """Fake asyncpg.Pool: instant close, records nothing but that."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True

    def terminate(self) -> None:
        self.closed = True


class _FakeConn:
    """Fake asyncpg.Connection: answers LISTEN and anything else instantly."""

    async def execute(self, sql: str, *_args: object) -> str:
        return "OK"

    async def close(self) -> None:
        return None

    def terminate(self) -> None:
        return None

    def is_closed(self) -> bool:
        return False


def _make_settings() -> WorkerSettings:
    """WorkerSettings for the DSN bootstrap path, bypassing .env discovery."""
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://fake:fake@fake:5432/fake",
            "TASKQ_PG_DSN_DIRECT": "postgresql://fake:fake@fake:5432/fake",
            "TASKQ_PG_DSN_POOLED": "postgresql://fake:fake@fake:5432/fake",
            "TASKQ_HEALTH_ENABLED": "false",
            "TASKQ_NOTIFY_ENABLED": "true",
        }
    )


async def test_worker_dsn_pools_boot_without_any_startup_params(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every TaskQ-built role pool opens with no ``server_settings`` kwarg.

    The startup packet is the only channel a pooler can reject before
    authentication, and the boot error it produces names neither the
    parameter nor the pool — an operator behind such a pooler sees workers
    that fail to start for no stated reason. A pool that needs a session
    GUC has server-side channels that never touch the packet (role
    defaults, DSN ``options``), so nothing here needs re-adding.
    """

    calls: list[dict[str, Any]] = []

    async def _recording_create_pool(*args: Any, **kwargs: Any) -> _FakePool:
        calls.append({"args": args, "kwargs": kwargs})
        return _FakePool()

    monkeypatch.setattr(asyncpg, "create_pool", _recording_create_pool)

    async def _notify_factory() -> asyncpg.Connection:
        return _FakeConn()  # type: ignore[return-value]  # Why: the fake satisfies the conn surface the bootstrap LISTEN/close path touches; the C-extension type can't be constructed without a server.

    async def _leader_factory() -> asyncpg.Connection:
        return _FakeConn()  # type: ignore[return-value]  # Why: same as the notify fake above.

    conns = WorkerConnections(
        notify_conn_factory=_notify_factory,
        leader_conn_factory=_leader_factory,
    )

    async with open_worker_deps(_make_settings(), connections=conns):
        pass

    # Three DSN pools: dispatcher, heartbeat, worker (the notify/leader
    # dedicated conns and the redis client are factory-faked or unset above,
    # and the slot pool is not built — no LOOP-scope registration exists).
    assert len(calls) == 3, f"expected the three DSN role pools; got {len(calls)}"
    for call in calls:
        assert "server_settings" not in call["kwargs"], (
            f"asyncpg.create_pool received server_settings={call['kwargs']['server_settings']!r}: "
            "the entry rides the startup packet, which a pooler that rejects "
            "unknown startup parameters (PgBouncer) refuses before auth — "
            "worker boot fails behind it with an error naming neither the "
            "parameter nor the pool (#247). Session GUCs belong server-side "
            "(ALTER ROLE ... SET / DSN options); see "
            "docs/guides/ops.md §'Database performance knobs'."
        )
