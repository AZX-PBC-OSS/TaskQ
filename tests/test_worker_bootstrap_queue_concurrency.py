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
  - A failing ``sync_slots`` for the queue-cap path CRASHES worker startup
    loudly (a registered-but-unslotted cap would deny every dispatch on
    the queue until a manual restart).
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

from taskq._ids import new_base62
from taskq.ratelimit.registry import (
    QUEUE_CONCURRENCY_PREFIX,
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


# ── sync_slots failure on the queue-cap path: crash loudly ───────────


@pytest.mark.asyncio
async def test_queue_cap_sync_slots_failure_crashes_bootstrap(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failing queue-cap ``sync_slots`` CRASHES worker startup with a
    ``RuntimeError`` naming the affected queue-cap reservations.

    The reservations are registered BEFORE ``sync_slots`` runs, and
    dispatch prepends the cap name as a plain string (no ensure_slots
    retry on the acquire path) — so warn-and-continue would leave the cap
    registered with zero slot rows and every dispatch on that queue
    snoozed with ``ReservationUnavailable`` until a manual restart.
    Crash-and-let-the-supervisor-retry self-heals, because ``sync_slots``
    is idempotent.

    This pins the QUEUE-CAP sync_slots block specifically — a previous
    version of this test patched ``sync_slots`` to always raise and
    asserted on the FIRST ``sync_slots_failed`` log event, which always
    came from the actor-path call ~100 lines earlier in ``_main``;
    coverage showed the queue-cap block never executed. The patched
    ``sync_slots`` below raises ONLY when called with queue-cap
    reservations (names under the reserved prefix) and delegates
    otherwise, so the actor-path warn-and-continue behavior cannot shadow
    the block under test.
    """
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

    import taskq.ratelimit as ratelimit_mod

    real_sync_slots = ratelimit_mod.sync_slots

    async def _raise_for_queue_caps(reservations: list[Any], pool: Any, *, schema: str) -> Any:
        if any(r.name.startswith(QUEUE_CONCURRENCY_PREFIX) for r in reservations):
            raise RuntimeError("sync_slots boom")
        return await real_sync_slots(reservations, pool, schema=schema)

    monkeypatch.setattr("taskq.ratelimit.sync_slots", _raise_for_queue_caps)

    cap_name = queue_concurrency_reservation_name(queue_name)

    async def _run() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await _main(settings)

    task = asyncio.create_task(_run())
    deadline = asyncio.get_running_loop().time() + 30.0
    while asyncio.get_running_loop().time() < deadline:
        if task.done():
            break
        await asyncio.sleep(0.05)

    try:
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            pytest.fail(
                "bootstrap kept running despite the queue-cap sync_slots "
                "failure — it must crash loudly instead of leaving a "
                "registered-but-unslotted cap that denies every dispatch"
            )

        exc = task.exception()
        assert exc is not None, "bootstrap should have raised on queue-cap sync_slots failure"
        assert isinstance(exc, RuntimeError), (
            f"expected RuntimeError, got {type(exc).__name__}: {exc}"
        )
        assert "failed to sync slot rows for queue-cap reservations" in str(exc)
        assert cap_name in str(exc)
        assert "sync_slots boom" in str(exc)
    finally:
        rl_registry._reservations.pop(cap_name, None)  # pyright: ignore[reportPrivateUsage]
        await _cleanup_schema_for(pg_dsn, schema)


# ── Missing migration: bootstrap must crash, not silently continue ──


@pytest.mark.asyncio
async def test_bootstrap_raises_when_max_concurrent_column_missing(pg_dsn: str) -> None:
    """When migration ``01.00.04_01_pre_queue_concurrency.sql`` has not been
    applied (the ``queues.max_concurrent`` column is absent), bootstrap MUST
    raise a clear error identifying the missing migration — not silently
    continue with no queue-cap reservation registered.

    Simulates the missing column by applying all migrations then dropping
    the ``max_concurrent`` column, which is equivalent to having stopped
    one migration earlier.
    """
    schema = f"twbqc_{new_base62()}".lower()
    await _prepare_schema_for(pg_dsn, schema)

    queue_name = "orders"

    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'INSERT INTO "{schema}".queues (name) VALUES ($1)',
            queue_name,
        )
        await conn.execute(
            f'ALTER TABLE "{schema}".queues DROP COLUMN max_concurrent',
        )
    finally:
        await conn.close()

    settings = _settings_for(pg_dsn, schema, queues=queue_name)

    cap_name = queue_concurrency_reservation_name(queue_name)

    async def _run() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await _main(settings)

    task = asyncio.create_task(_run())
    raised_exc: Exception | None = None
    deadline = asyncio.get_running_loop().time() + 30.0
    while asyncio.get_running_loop().time() < deadline:
        if task.done():
            exc = task.exception()
            if isinstance(exc, Exception):
                raised_exc = exc
            break
        await asyncio.sleep(0.05)

    if not task.done():
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    try:
        assert raised_exc is not None, (
            "bootstrap should have raised RuntimeError about missing "
            "queues.max_concurrent column, but did not raise"
        )
        assert isinstance(raised_exc, RuntimeError), (
            f"expected RuntimeError, got {type(raised_exc).__name__}: {raised_exc}"
        )
        assert "max_concurrent" in str(raised_exc)
        assert "01.00.04" in str(raised_exc)

        # The queue-cap reservation must NOT have been registered.
        assert cap_name not in rl_registry.reservations
    finally:
        await _cleanup_schema_for(pg_dsn, schema)
