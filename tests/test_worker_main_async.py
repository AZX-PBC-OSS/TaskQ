"""``worker_main_async`` — the supported async worker entrypoint.

The gap this guards: ``worker_main`` owns its ``asyncio.Runner``, and asyncpg
pools are loop-bound. A consumer that must build dependencies on the running
loop *before* its actors are constructed (actors close over those deps) could
not use ``worker_main`` at all, and imported ``taskq.worker._bootstrap._main``
instead — a private name, with the private ``_cron_registry`` /``_registry``
test seams in its signature.

``worker_main_async`` is that entrypoint with ``worker_main``'s exact public
signature, and ``worker_main`` is now the thin sync wrapper over it.
"""

from __future__ import annotations

import asyncio
import inspect

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.health import unique_health_sock_path
from taskq.worker import worker_main_async as worker_main_async_pkg
from taskq.worker.run import worker_main, worker_main_async

_SCHEMA_LABEL = f"twma_{new_base62()}".lower()


def _settings(pg_dsn: str, **overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"pg_dsn": pg_dsn, "schema_name": _SCHEMA_LABEL}
    data.setdefault("health_socket_path", unique_health_sock_path("worker_main_async"))
    data.update(overrides)
    return WorkerSettings.load_from_dict(data)


# ── Public surface ────────────────────────────────────────────────────


def test_worker_main_async_is_exported() -> None:
    """Reachable from both the module and the package, and in both ``__all__``."""
    import taskq.worker as worker_pkg
    import taskq.worker.run as run_mod

    assert worker_main_async_pkg is worker_main_async
    assert "worker_main_async" in run_mod.__all__
    assert "worker_main_async" in worker_pkg.__all__


def test_worker_main_async_is_a_coroutine_function() -> None:
    assert inspect.iscoroutinefunction(worker_main_async)


def test_signature_matches_worker_main() -> None:
    """Same parameters as the sync entrypoint — no private seams, no divergence.

    A divergent signature would push every caller back to ``_main`` for
    whatever the async form dropped, which is the bug this closes.
    """
    sync_params = inspect.signature(worker_main).parameters
    async_params = inspect.signature(worker_main_async).parameters
    assert list(sync_params) == list(async_params)
    for name, sync_param in sync_params.items():
        assert async_params[name].kind == sync_param.kind, name
        assert async_params[name].default == sync_param.default, name


def test_signature_exposes_no_private_seams() -> None:
    """``_registry`` / ``_cron_registry`` / ``_local_queue_seed`` stay private."""
    params = inspect.signature(worker_main_async).parameters
    assert not [p for p in params if p.startswith("_")]
    assert "cron_registry" in params
    assert "connections" in params
    assert "actor_registry" in params


# ── Runs to completion on a caller-owned loop ─────────────────────────


@pytest.mark.integration
async def test_runs_to_completion_on_a_caller_owned_loop(pg_dsn: str) -> None:
    """The consumer's real shape: a pool built on THIS loop, then the worker.

    ``worker_main`` cannot express this — it would drive the worker under its
    own ``Runner``, on a different loop from the pool the actors close over.
    """
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA_LABEL}" CASCADE')
        await apply_pending(conn, schema=_SCHEMA_LABEL)
    finally:
        await conn.close()

    running_loop = asyncio.get_running_loop()
    # Built BEFORE the worker starts, bound to the caller's loop — exactly the
    # ordering constraint that forced the private import.
    caller_pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    assert caller_pool is not None
    try:
        async with caller_pool.acquire() as c:
            assert await c.fetchval("SELECT 1") == 1

        code = await worker_main_async(
            _settings(pg_dsn),
            actor_registry={},
            cron_registry=[],
            until_idle=True,
            idle_settle_window=0.2,
            idle_poll_interval=0.1,
            idle_max_runtime=30.0,
        )
        assert code == 0
        # The pool outlives the worker and is still usable on THIS loop —
        # an asyncpg pool bound to a different (or finished) loop cannot be.
        assert asyncio.get_running_loop() is running_loop
        assert not caller_pool.is_closing()
        async with caller_pool.acquire() as c:
            assert await c.fetchval("SELECT 1") == 1
    finally:
        await caller_pool.close()
        conn = await asyncpg.connect(pg_dsn)
        try:
            await conn.execute(f'DROP SCHEMA IF EXISTS "{_SCHEMA_LABEL}" CASCADE')
        finally:
            await conn.close()
