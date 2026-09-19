# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team locks: backend sweeps acquire the notify/dispatcher pool with no timeout.

``backend/postgres.py``'s sweep entrypoints acquire with
``async with self._notify_pool.acquire() as conn:`` - no ``timeout=``
(``scheduled_to_pending``, ``deadline_sweep``, ``reclaim_expired_locks``)
- and ``_notify_pool`` delegates to the DISPATCHER pool, the same pool a
prune drain holds for its whole multi-batch drain
(``worker/_leader_sweeps.py`` keeps its dispatcher conn across
``prune_terminal_jobs``). With that pool exhausted, a sweep queues
indefinitely: no bound, no classified failure.

The RED contract: the sweep entrypoints must bound their notify-pool
acquire (and classify the failure) instead of queueing indefinitely.
Every wait in this test is bounded so the RED failure can never hang the
suite.
"""

import asyncio
import contextlib
import time
from datetime import timedelta

import asyncpg
import pytest

from taskq._ids import new_base62
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.migrate import apply_pending
from taskq.settings import WorkerSettings
from taskq.testing.settings import make_integration_settings

pytestmark = pytest.mark.integration

_TEST_BOUND_S = 8.0
_UNBOUNDED_MARGIN_S = 7.5


class _NotifyOnlyDeps:
    """BackendDeps stand-in: the sweep path touches only settings and the
    dispatcher pool (the notify pool delegates to it, postgres.py
    ``_notify_pool``)."""

    def __init__(self, settings: WorkerSettings, dispatcher_pool: asyncpg.Pool) -> None:
        self.settings = settings
        self.worker_pool = dispatcher_pool
        self.heartbeat_pool = dispatcher_pool
        self.dispatcher_pool = dispatcher_pool


async def test_sweep_notify_pool_acquire_must_be_bounded_under_held_dispatcher_conn(
    pg_dsn: str,
) -> None:
    """RED: with the single-conn dispatcher pool held (the prune-drain
    shape), the sweep entrypoint must fail (or succeed) within a bound -
    not queue indefinitely.

    Contract: backend sweeps must bound their notify/dispatcher pool
    acquire and classify the failure. Today the contract is violated:
    postgres.py's ``scheduled_to_pending`` acquires with
    ``async with self._notify_pool.acquire() as conn:`` - no timeout=
    (same shape at ``deadline_sweep`` and ``reclaim_expired_locks``) - so
    with the pool's only connection held by a prune drain, the sweep
    queued past the test's own 8 s bound; the ONLY thing that ended the
    wait was this test's wait_for, proving the acquire itself carries no
    bound. A production-side bound that fires inside the window (e.g.
    dispatcher_command_timeout, the prune loop's own acquire convention)
    satisfies the contract.
    """
    schema = f"tlck_{new_base62()}".lower()
    pool: asyncpg.Pool | None = None
    try:
        setup = await asyncpg.connect(pg_dsn)
        try:
            await apply_pending(setup, schema=schema)
        finally:
            await setup.close()
        settings = make_integration_settings(pg_dsn, schema_name=schema)
        pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=1)
        backend = PostgresBackend(
            _NotifyOnlyDeps(settings, pool),
            clock=SystemClock(),
            cancellation_grace_period=timedelta(0),
            cleanup_grace_period=timedelta(0),
        )
        control = await asyncio.wait_for(backend.scheduled_to_pending(), timeout=_TEST_BOUND_S)
        assert control == 0, (
            "control: a fresh schema has nothing scheduled - the sweep entrypoint "
            "itself works when a connection is free, guarding the starved case below "
            "against wrong-reason failure"
        )
        held = await pool.acquire()
        try:
            started = time.monotonic()
            try:
                await asyncio.wait_for(backend.scheduled_to_pending(), timeout=_TEST_BOUND_S)
            except TimeoutError:
                elapsed = time.monotonic() - started
                if elapsed >= _UNBOUNDED_MARGIN_S:
                    pytest.fail(
                        "Contract: the backend sweep entrypoints must bound their "
                        "notify/dispatcher pool acquire and classify the failure - a "
                        "held dispatcher conn (the prune drain holds one for its "
                        "whole multi-batch drain) must not queue a sweep indefinitely. "
                        "Today the contract is violated: postgres.py's "
                        "scheduled_to_pending acquires with `async with "
                        "self._notify_pool.acquire() as conn:` - no timeout= (same "
                        "shape at deadline_sweep and reclaim_expired_locks) - and the "
                        f"sweep was still queued at the test's own {elapsed:.1f} s "
                        "bound; the only thing that ended the wait was this test's "
                        "wait_for, proving the acquire carries no bound of its own"
                    )
        finally:
            await pool.release(held)
        after = await asyncio.wait_for(backend.scheduled_to_pending(), timeout=_TEST_BOUND_S)
        assert after == 0, (
            "Contract: releasing the holder must let the sweep run again - the "
            "starve is an unbounded queue, not a break"
        )
    finally:
        if pool is not None:
            with contextlib.suppress(Exception):
                await pool.close()
        with contextlib.suppress(Exception):
            conn = await asyncpg.connect(pg_dsn)
            try:
                await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
            finally:
                with contextlib.suppress(Exception):
                    await conn.close()
