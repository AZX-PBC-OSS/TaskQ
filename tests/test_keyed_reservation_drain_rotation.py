# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.

"""Pins for the pending-reclaim drain's core invariants.

The drain is the second DELETE path for ``reservation_slots`` rows (the
first is ``sync_slots``, which iterates only currently-registered
reservations and is therefore blind to an evicted bucket by
construction). Four invariants keep it safe and live:

- **Fairness** - the per-schema pending set is insertion-ordered and the
  drain takes the FRONT batch, re-appending that tick's survivors (names
  whose rows were all still held by a live lease) to the BACK. A held
  bucket waits at most one full rotation; a sorted head of held buckets
  can never starve the pending tail behind it (starvation strands idle
  rows forever, fills the pending set to its cap, and vetoes every
  later eviction - a fail-closed availability cliff).
- **Held-row guard** - the drain's DELETE removes only free or
  lease-expired rows. A slot genuinely held by a live lease survives
  every drain, its bucket stays pending, and the holder's release (or
  lease expiry) is what finally frees the row for the next drain.
- **Slice bound** - one drain call's DELETE touches at most
  ``batch_names`` buckets per schema, so one tick's write set is
  constant-size against any evicted-key backlog.
- **No lingering empty schema keys** - a drain pass that empties a
  schema's pending set (every name re-registered, or every row
  reclaimed) pops the schema key, so a later drain with nothing pending
  acquires no connection at all.
"""

from datetime import timedelta
from time import monotonic
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.migrate import apply_pending
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.settings import WorkerSettings

pytestmark = pytest.mark.integration

_HEAD_LEASE = timedelta(minutes=10)


class _SessionPayload(BaseModel):
    session_id: str


def _schema() -> str:
    """This file's dedicated schema (local, per the suite-hygiene rule
    against module-level schema constants)."""
    return "taskq_keyed_drain_rotation_test"


def _settings(pg_dsn: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {"TASKQ_PG_DSN": pg_dsn, "TASKQ_SCHEMA_NAME": _schema()},
        validate=False,
    )


async def _fresh_schema(pg_dsn: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_schema()}" CASCADE')
        await apply_pending(conn, schema=_schema())
    finally:
        await conn.close()


async def _slot_rows(pool: asyncpg.Pool, bucket: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*) FROM "{_schema()}".reservation_slots WHERE bucket_name = $1',
            bucket,
        )


async def _held_rows(pool: asyncpg.Pool, bucket: str) -> int:
    """Rows genuinely held: a holder is set and the lease is still live."""
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*) FROM "{_schema()}".reservation_slots '
            "WHERE bucket_name = $1 AND job_id IS NOT NULL "
            "AND lease_expires_at > clock_timestamp()",
            bucket,
        )


def _ref(base_name: str, *, slots: int = 1, lease: timedelta = _HEAD_LEASE) -> KeyedReservationRef:
    return KeyedReservationRef.typed(
        _SessionPayload,
        base_name=base_name,
        key_fn=lambda p: p.session_id,
        slots=slots,
        lease=lease,
    )


async def _acquire(
    reg: RateLimitRegistry,
    ref: KeyedReservationRef,
    pool: asyncpg.Pool,
    settings: WorkerSettings,
):
    return await reg.acquire_for_actor(
        rate_limits=[],
        reservations=[ref],
        job_id=new_uuid(),
        worker_id=new_uuid(),
        payload=_SessionPayload(session_id="s1"),
        pg_pool=pool,
        settings=settings,
    )


async def _materialize(
    reg: RateLimitRegistry,
    ref: KeyedReservationRef,
    pool: asyncpg.Pool,
    settings: WorkerSettings,
) -> str:
    """Register a keyed bucket and create its rows WITHOUT acquiring a slot."""
    return await reg._resolve_reservation_name(  # pyright: ignore[reportPrivateUsage]  # Why: materializing a keyed bucket without acquiring, matching test_keyed_reservation_self_heal.py's pattern.
        ref, payload=_SessionPayload(session_id="s1"), pg_pool=pool, settings=settings
    )


def _seed_idle(reg: RateLimitRegistry, *buckets: str) -> None:
    """Stamp every bucket's tracking entry far past the idle threshold."""
    for bucket in buckets:
        reg._keyed_reservation_last_used[bucket] = monotonic() - 7200.0  # pyright: ignore[reportPrivateUsage]  # Why: seeding entries idle for eviction, matching test_leader_sweep_rl_registry.py's pattern.


async def test_held_head_does_not_starve_the_pending_tail(pg_dsn: str) -> None:
    """Three held buckets at the head of the pending queue plus one
    fully-idle bucket behind them: with ``batch_names=2``, repeated drains
    must reclaim the IDLE bucket's rows even while every head stays held -
    a held bucket waits at most one full rotation, never blocks the tail."""
    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        settings = _settings(pg_dsn)

        # Three held heads, materialized + acquired in this order so they
        # occupy the head of the pending queue; each job holds its only
        # slot for the rest of the test (lease well past the drain window).
        heads = ["a-held:s1", "b-held:s1", "c-held:s1"]
        for base in ("a-held", "b-held", "c-held"):
            await _acquire(reg, _ref(base), pool, settings)
        # The fully-idle tail bucket: rows materialized, never acquired.
        idle = await _materialize(reg, _ref("z-idle", slots=2), pool, settings)

        for bucket in (*heads, idle):
            assert await _slot_rows(pool, bucket) >= 1, f"fixture broken: {bucket} has no rows"
        for bucket in heads:
            assert await _held_rows(pool, bucket) == 1, f"fixture broken: {bucket} is not held"

        _seed_idle(reg, *heads, idle)
        assert reg.evict_idle_keyed_reservations(idle_for=timedelta(0)) == 4

        for _ in range(3):
            await reg.drain_pending_reservation_reclaims(pool, batch_names=2)

        assert await _slot_rows(pool, idle) == 0, (
            f"the fully-idle bucket {idle!r} was never drained: held buckets at "
            "the head of the pending queue starve the tail behind them, so its "
            "idle rows are stranded forever, pending fills to its cap, and "
            "every later eviction is vetoed"
        )
        for bucket in heads:
            assert await _slot_rows(pool, bucket) == 1, (
                f"a held bucket's rows must survive every drain: {bucket}"
            )
            assert await _held_rows(pool, bucket) == 1, (
                f"a held bucket must still be held after every drain: {bucket}"
            )
        assert reg.has_pending_reservation_reclaims, (
            "the held heads must stay pending (their rows survive by design), "
            "waiting for their leases to expire or their holders to release"
        )
    finally:
        await pool.close()


async def test_held_slot_survives_drain_until_release(pg_dsn: str) -> None:
    """A slot genuinely held by a live lease survives the drain's
    idle-guarded DELETE - the bucket stays pending, a re-acquire against
    the still-held row is denied, and only the holder's release lets the
    next drain reclaim the row."""
    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        settings = _settings(pg_dsn)
        ref = _ref("held-guard")
        bucket = "held-guard:s1"

        acquired = await _acquire(reg, ref, pool, settings)
        assert await _held_rows(pool, bucket) == 1, "fixture broken: the slot is not held"

        # The entry is idle-evictable while its row is held: the idle
        # threshold tracks the last acquire, not the row's holder.
        reservation = reg.get_reservation(bucket)
        _seed_idle(reg, bucket)
        assert reg.evict_idle_keyed_reservations(idle_for=timedelta(0)) == 1

        await reg.drain_pending_reservation_reclaims(pool)

        assert await _slot_rows(pool, bucket) == 1, (
            "the drain's DELETE must skip a row held by a live lease - deleting "
            "it would let a second job acquire the slot the holder still owns"
        )
        assert await _held_rows(pool, bucket) == 1
        assert reg.has_pending_reservation_reclaims, (
            "a held survivor must stay pending for a later drain"
        )

        # The still-held row denies a second acquirer - the PG row enforces
        # the cap regardless of the registry entry's lifecycle.
        from taskq.exceptions import ReservationUnavailable

        with pytest.raises(ReservationUnavailable):
            await reservation.acquire(new_uuid(), new_uuid(), pool)

        # The holder releases; the next drain reclaims the now-free row.
        await reg.release_for_actor(acquired)
        await reg.drain_pending_reservation_reclaims(pool)

        assert await _slot_rows(pool, bucket) == 0, (
            "a released row must be reclaimed by the next drain"
        )
        assert not reg.has_pending_reservation_reclaims
    finally:
        await pool.close()


async def test_drain_delete_is_bounded_by_batch_names(pg_dsn: str) -> None:
    """One drain call's DELETE touches at most ``batch_names`` buckets: with
    five pending idle buckets and ``batch_names=2``, one drain reclaims
    exactly two buckets' rows, the next drain two more, the last the rest -
    never the whole backlog in one statement."""
    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        settings = _settings(pg_dsn)

        buckets = [await _materialize(reg, _ref(f"p{i}"), pool, settings) for i in range(1, 6)]
        for bucket in buckets:
            assert await _slot_rows(pool, bucket) == 1, f"fixture broken: {bucket} has no rows"

        _seed_idle(reg, *buckets)
        assert reg.evict_idle_keyed_reservations(idle_for=timedelta(0)) == 5

        await reg.drain_pending_reservation_reclaims(pool, batch_names=2)
        assert [await _slot_rows(pool, b) for b in buckets] == [0, 0, 1, 1, 1], (
            "one drain must delete only the front slice of pending buckets, not "
            "the whole backlog - an unbounded DELETE is the backlog-ricochet "
            "defect the bounded-write guard exists for"
        )

        await reg.drain_pending_reservation_reclaims(pool, batch_names=2)
        assert [await _slot_rows(pool, b) for b in buckets] == [0, 0, 0, 0, 1], (
            "the next drain must continue where the slice bound stopped the last"
        )

        await reg.drain_pending_reservation_reclaims(pool, batch_names=2)
        assert [await _slot_rows(pool, b) for b in buckets] == [0, 0, 0, 0, 0]
        assert not reg.has_pending_reservation_reclaims
    finally:
        await pool.close()


class _AcquireRaisingPool:
    """A pool whose ``acquire`` raises - proves a drain touches no connection."""

    def acquire(self, *, timeout: float | None = None) -> Any:
        raise asyncpg.PostgresConnectionError("no drain should reach the pool")


async def test_drain_emptied_by_re_registration_pops_the_schema_key(pg_dsn: str) -> None:
    """A drain pass whose every pending name re-registered must pop the
    schema key: the next drain is a no-op that acquires no connection, a
    re-activated key owns its rows again."""
    await _fresh_schema(pg_dsn)

    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        settings = _settings(pg_dsn)
        ref = _ref("reregistered")
        bucket = "reregistered:s1"

        await _materialize(reg, ref, pool, settings)
        _seed_idle(reg, bucket)
        assert reg.evict_idle_keyed_reservations(idle_for=timedelta(0)) == 1

        # The key re-activates before any drain ran: it is registered and
        # owns its rows again, so the drain must drop the pending name
        # without touching PG.
        await _materialize(reg, ref, pool, settings)
        assert await reg.drain_pending_reservation_reclaims(pool) == 0
        assert await _slot_rows(pool, bucket) == 1, "a re-activated key's rows must be left alone"

        # Nothing pending: the next drain must return before acquiring a
        # connection - an empty-set schema key lingering after the pass
        # would make every later drain pay a pool acquire for nothing.
        assert await reg.drain_pending_reservation_reclaims(_AcquireRaisingPool()) == 0  # type: ignore[arg-type]  # Why: a minimal stand-in for asyncpg.Pool; only acquire() is reached, and only if the bug is present.
    finally:
        await pool.close()
