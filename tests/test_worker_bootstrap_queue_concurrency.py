"""Integration tests for the fleet-wide per-queue concurrency cap bootstrap
(Part 1, issue #24).

Mirrors the exact fixture/assertion conventions of
``tests/test_worker_bootstrap.py`` — all tests use the real PG container via
the ``pg_dsn`` fixture (session-scoped ``pg_container``).

Each test seeds the ``queues`` table with a ``max_concurrent`` value via
direct SQL, runs the ``_main`` bootstrap sequence, and asserts:
  - The global ``rl_registry`` contains a ``ConcurrencyReservation`` named
    ``queue_concurrency_reservation_name(<queue>)`` with matching slots.
  - The ``reservation_slots`` table in PG has the expected slot rows for
    that bucket name (proving ``sync_slots`` actually ran).
  - Queues with ``max_concurrent IS NULL`` or not in ``settings.queues``
    produce NO registry entry and NO slot rows.
  - A failing ``sync_slots`` for the queue-cap path is caught, logged,
    and does not crash worker startup.
  - Lowering ``max_concurrent`` and re-running bootstrap (simulating a
    worker restart) shrinks the slot rows — the core regression test for
    the ``sync_slots`` fix that replaced the purely-additive
    ``ensure_slots`` call.
"""

import asyncio
import contextlib
from typing import Any

import asyncpg
import pytest
import structlog

from taskq._ids import new_base62
from taskq.ratelimit.registry import (
    queue_concurrency_reservation_name,
)
from taskq.ratelimit.registry import (
    registry as rl_registry,
)
from taskq.settings import WorkerSettings
from taskq.worker.run import _main
from tests.conftest import unique_health_sock_path

pytestmark = pytest.mark.integration


# ── Helpers (mirroring test_worker_bootstrap.py) ─────────────────────


async def _prepare_schema_for(pg_dsn: str, schema: str) -> None:
    """Drop and recreate *schema*, apply migrations."""
    from taskq.migrate import apply_pending

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()


async def _cleanup_schema_for(pg_dsn: str, schema: str) -> None:
    """Drop *schema*."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


def _settings_for(pg_dsn: str, schema: str, **overrides: str) -> WorkerSettings:
    data: dict[str, str] = {"pg_dsn": pg_dsn, "schema_name": schema}
    data.setdefault("health_socket_path", unique_health_sock_path("worker_bootstrap_qc"))
    data.update(overrides)
    return WorkerSettings.load_from_dict(data)


async def _run_and_cancel(coro_factory: Any, *, sleep: float = 2.0) -> None:
    """Run ``_main``-wrapping coroutine briefly, then cancel it cleanly."""

    async def _runner() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await coro_factory()

    task = asyncio.create_task(_runner())
    await asyncio.sleep(sleep)
    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    else:
        await task


# ── Happy path: queue with max_concurrent set and in settings.queues ──


@pytest.mark.asyncio
async def test_queue_cap_registered_and_slots_created(pg_dsn: str) -> None:
    """A queue with ``max_concurrent`` set and included in
    ``settings.queues`` produces a ``ConcurrencyReservation`` in the
    registry with matching slots, and ``reservation_slots`` rows exist
    in PG for that bucket name — proving ``ensure_slots`` actually ran."""
    schema = f"twbqc_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    queue_name = "orders"
    max_concurrent = 3

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, max_concurrent) VALUES ($1, $2)',
            queue_name,
            max_concurrent,
        )
    finally:
        await conn.close()

    settings = _settings_for(pg_dsn, schema, queues=queue_name)

    await _run_and_cancel(lambda: _main(settings))

    cap_name = queue_concurrency_reservation_name(queue_name)

    # (a) The registry has the ConcurrencyReservation with matching slots.
    reservations = rl_registry.reservations
    assert cap_name in reservations
    cap_res = reservations[cap_name]
    assert cap_res.slots == max_concurrent

    # (b) reservation_slots rows actually exist in PG for that bucket name.
    conn = await asyncpg.connect(pg_dsn)
    try:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".reservation_slots WHERE bucket_name = $1',
            cap_name,
        )
        assert count == max_concurrent
    finally:
        await conn.close()

    await _cleanup_schema_for(pg_dsn, schema)


# ── Shrink/grow: sync_slots correctly adjusts slot count on restart ──


@pytest.mark.asyncio
async def test_queue_cap_shrink_slots_on_restart(pg_dsn: str) -> None:
    """Lowering ``max_concurrent`` and re-running bootstrap shrinks the
    ``reservation_slots`` row count — the core regression test for the
    ``sync_slots`` fix.  ``ensure_slots`` (the old code) was purely
    additive (INSERT ... ON CONFLICT DO NOTHING) and could never remove
    excess slots, so lowering a cap was a silent no-op.  ``sync_slots``
    deletes excess free slots, so a worker restart after lowering the
    cap actually takes effect."""
    schema = f"twbqc_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    queue_name = "shrinkable"
    cap_name = queue_concurrency_reservation_name(queue_name)

    # ── First boot: max_concurrent = 5 → 5 slot rows ───────────────
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, max_concurrent) VALUES ($1, $2)',
            queue_name,
            5,
        )
    finally:
        await conn.close()

    settings = _settings_for(pg_dsn, schema, queues=queue_name)
    await _run_and_cancel(lambda: _main(settings))

    conn = await asyncpg.connect(pg_dsn)
    try:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".reservation_slots WHERE bucket_name = $1',
            cap_name,
        )
        assert count == 5
    finally:
        await conn.close()

    # Simulate a worker restart: clear the reservation from the
    # in-memory registry (a fresh process starts with an empty registry).
    rl_registry._reservations.pop(cap_name, None)  # pyright: ignore[reportPrivateUsage]

    # ── Second boot: lower to max_concurrent = 2 → 2 slot rows ──────
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'UPDATE "{schema}".queues SET max_concurrent = $2 WHERE name = $1',
            queue_name,
            2,
        )
    finally:
        await conn.close()

    await _run_and_cancel(lambda: _main(settings))

    conn = await asyncpg.connect(pg_dsn)
    try:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".reservation_slots WHERE bucket_name = $1',
            cap_name,
        )
        assert count == 2, (
            f"expected 2 slots after shrinking from 5→2, got {count} "
            f"(sync_slots should have deleted 3 excess free slots)"
        )
    finally:
        await conn.close()

    rl_registry._reservations.pop(cap_name, None)  # pyright: ignore[reportPrivateUsage]
    await _cleanup_schema_for(pg_dsn, schema)


@pytest.mark.asyncio
async def test_queue_cap_grow_slots_on_restart(pg_dsn: str) -> None:
    """Raising ``max_concurrent`` and re-running bootstrap grows the
    ``reservation_slots`` row count.  While the old ``ensure_slots`` code
    could also grow (it was additive), this test confirms the
    ``sync_slots`` replacement preserves the grow capability — a strict
    superset of ``ensure_slots`` must not regress on the insert path."""
    schema = f"twbqc_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    queue_name = "growable"
    cap_name = queue_concurrency_reservation_name(queue_name)

    # ── First boot: max_concurrent = 2 → 2 slot rows ───────────────
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, max_concurrent) VALUES ($1, $2)',
            queue_name,
            2,
        )
    finally:
        await conn.close()

    settings = _settings_for(pg_dsn, schema, queues=queue_name)
    await _run_and_cancel(lambda: _main(settings))

    conn = await asyncpg.connect(pg_dsn)
    try:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".reservation_slots WHERE bucket_name = $1',
            cap_name,
        )
        assert count == 2
    finally:
        await conn.close()

    # Simulate a worker restart: clear the reservation from the
    # in-memory registry (a fresh process starts with an empty registry).
    rl_registry._reservations.pop(cap_name, None)  # pyright: ignore[reportPrivateUsage]

    # ── Second boot: raise to max_concurrent = 5 → 5 slot rows ──────
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'UPDATE "{schema}".queues SET max_concurrent = $2 WHERE name = $1',
            queue_name,
            5,
        )
    finally:
        await conn.close()

    await _run_and_cancel(lambda: _main(settings))

    conn = await asyncpg.connect(pg_dsn)
    try:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".reservation_slots WHERE bucket_name = $1',
            cap_name,
        )
        assert count == 5, (
            f"expected 5 slots after growing from 2→5, got {count} "
            f"(sync_slots should have inserted 3 missing slots)"
        )
    finally:
        await conn.close()

    rl_registry._reservations.pop(cap_name, None)  # pyright: ignore[reportPrivateUsage]
    await _cleanup_schema_for(pg_dsn, schema)


@pytest.mark.asyncio
async def test_queue_cap_null_max_concurrent_produces_no_reservation(pg_dsn: str) -> None:
    """A queue with ``max_concurrent IS NULL`` produces NO registry entry
    and NO ``reservation_slots`` rows — the cap is opt-in per queue."""
    schema = f"twbqc_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    queue_name = "uncapped"

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name) VALUES ($1)',
            queue_name,
        )
    finally:
        await conn.close()

    # The queue IS in settings.queues, but max_concurrent is NULL.
    settings = _settings_for(pg_dsn, schema, queues=queue_name)

    await _run_and_cancel(lambda: _main(settings))

    cap_name = queue_concurrency_reservation_name(queue_name)
    assert cap_name not in rl_registry.reservations

    conn = await asyncpg.connect(pg_dsn)
    try:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".reservation_slots WHERE bucket_name = $1',
            cap_name,
        )
        assert count == 0
    finally:
        await conn.close()

    await _cleanup_schema_for(pg_dsn, schema)


@pytest.mark.asyncio
async def test_queue_cap_not_in_settings_queues_produces_no_reservation(pg_dsn: str) -> None:
    """A queue with ``max_concurrent`` set but NOT in ``settings.queues``
    produces NO registry entry and NO ``reservation_slots`` rows — only
    queues this worker consumes are capped."""
    schema = f"twbqc_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    other_queue = "other_queue"
    worker_queue = "default"

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, max_concurrent) VALUES ($1, $2)',
            other_queue,
            5,
        )
    finally:
        await conn.close()

    # Worker only consumes "default", not "other_queue".
    settings = _settings_for(pg_dsn, schema, queues=worker_queue)

    await _run_and_cancel(lambda: _main(settings))

    other_cap_name = queue_concurrency_reservation_name(other_queue)
    assert other_cap_name not in rl_registry.reservations

    conn = await asyncpg.connect(pg_dsn)
    try:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".reservation_slots WHERE bucket_name = $1',
            other_cap_name,
        )
        assert count == 0
    finally:
        await conn.close()

    await _cleanup_schema_for(pg_dsn, schema)


# ── sync_slots failure: caught, logged, bootstrap continues ──────────


@pytest.mark.asyncio
async def test_queue_cap_sync_slots_failure_logged_and_bootstrap_continues(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing queue-cap ``sync_slots`` is caught, logged as
    ``sync_slots_failed``, and does not crash worker startup — mirroring
    ``test_sync_rate_limit_buckets_and_sync_slots_failure_logged`` for the
    actor-config path.  After the fix that replaced ``ensure_slots`` with
    ``sync_slots``, slot creation failures are caught by the
    ``sync_slots`` try/except, not the per-row
    ``queue_concurrency_cap_setup_failed`` handler."""
    schema = f"twbqc_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    queue_name = "failing_queue"

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name, max_concurrent) VALUES ($1, $2)',
            queue_name,
            2,
        )
    finally:
        await conn.close()

    settings = _settings_for(pg_dsn, schema, queues=queue_name)

    async def _raise_sync_slots(reservations: object, pool: object, *, schema: str) -> None:
        raise RuntimeError("sync_slots boom")

    monkeypatch.setattr("taskq.ratelimit.sync_slots", _raise_sync_slots)

    cap_name = queue_concurrency_reservation_name(queue_name)

    async def _run() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await _main(settings)

    try:
        with structlog.testing.capture_logs() as captured:
            task = asyncio.create_task(_run())
            deadline = asyncio.get_running_loop().time() + 30.0
            while asyncio.get_running_loop().time() < deadline:
                if any(e.get("event") == "sync_slots_failed" for e in captured):
                    break
                if task.done():
                    break
                await asyncio.sleep(0.05)
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
    finally:
        rl_registry._reservations.pop(cap_name, None)  # pyright: ignore[reportPrivateUsage]

    matches = [e for e in captured if e.get("event") == "sync_slots_failed"]
    assert len(matches) >= 1
    assert "sync_slots boom" in matches[0]["error"]

    await _cleanup_schema_for(pg_dsn, schema)
