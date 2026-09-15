# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.

"""Red-team pins: fleet-wide reclamation of keyed rate-limit/reservation rows.

The #139 residual these pins close: keyed ``reservation_slots`` /
``rate_limit_buckets`` rows orphan when the worker that materialised them
DIES. The in-process reclamation machinery (the registry's idle eviction
plus ``drain_pending_reservation_reclaims``) only ever names rows its OWN
process evicted — the pending-reclaim set, the tracked-key stamps, and
the row-schema captures all live inside the dead process's registry, so
no survivor can name the rows. A live worker that happens to re-resolve
the same concrete key re-materialises over them, but the key space is
caller-controlled: steady-state cardinality after enough worker deaths is
one bucket (or one bucket's ``slots`` rows) per key ever materialised by
any process that later died — unbounded, and unreachable by every
deletion path in the package.

The fleet-reclaim design: per-key rows carry their own staleness through a
``keyed`` flag that marks rows FLEET-reclaimable (keyed-materialised; for
``rate_limit_buckets`` only when the bucket's state is PG-resident, because
only then does the acquire path touch the row and keep ``last_used_at``
truthful). The ``last_used_at`` timestamp is refreshed by the
acquire/release/upsert statements that already touch the row (no extra round
trips). The maintenance leader then runs ``sweep_idle_keyed_rows`` — a
bounded, committed batch per tick per table — deleting fleet-reclaimable
rows unused past the horizon. Static rows (``keyed`` false by construction)
and redis-backend keyed rows (state in Redis; the PG row is outage-fallback
+ admin metadata whose ``last_used_at`` cannot track Redis-side use) are
never deleted by it.

What each pin asserts:

- **orphaned-by-death** — a keyed row whose creating worker died (no
  registry anywhere can name it) is gone after one sweep tick.
- **static immortality** — a static bucket's rows survive the sweep at
  any age, past any horizon: ``keyed`` false excludes them from the
  sweep's predicate entirely.
- **redis-backend survival** — a redis-backend keyed bucket's published
  PG row is never swept (its liveness signal lives in Redis).
- **the stamp rides the acquire/release writes** — the existing
  statements refresh ``last_used_at``; no dedicated stamping round trip
  exists.
- **the held-bucket veto** — a keyed bucket with one live-held slot
  keeps ALL its rows (a partial delete would silently shrink the
  bucket's configured capacity; the acquire-path heal only fires at
  zero rows).
- **the disable sentinel** — ``timedelta(0)`` is the settings-level
  disable, rejected at the sweep's own boundary as a caller wiring bug.
- **the batch bound** — one call deletes at most ``batch_size`` buckets
  (rows, for ``rate_limit_buckets``), oldest first; repeated calls drain
  the backlog a committed batch at a time.
"""

from datetime import timedelta

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._sweeps import sweep_idle_keyed_rows
from taskq.migrate import apply_pending
from taskq.ratelimit.refs import KeyedRateLimitRef, KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry, sync_rate_limit_buckets
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings

pytestmark = pytest.mark.integration

#: The production horizon (``WorkerSettings.keyed_row_reclaim_period``
#: default) — the pins age rows past it rather than shrinking it, so the
#: sweep predicate is exercised exactly as the leader runs it.
_HORIZON = timedelta(hours=1)

#: The production batch bound (``WorkerSettings.keyed_row_reclaim_batch_size``
#: default's magnitude) — the bound pin shrinks it to make the cap bite.
_BATCH = 256


class _TenantPayload(BaseModel):
    tenant_id: str


class _SessionPayload(BaseModel):
    session_id: str


def _schema() -> str:
    """This file's dedicated schema (local, per the suite-hygiene rule
    against module-level schema constants)."""
    return "taskq_keyed_fleet_reclaim"


def _settings(pg_dsn: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": _schema(),
        },
        validate=False,
    )


def _pg_ref(base_name: str) -> KeyedRateLimitRef:
    """A keyed rate limit on the PG backend — the acquire path's own
    preseed/upsert creates and stamps the ``rate_limit_buckets`` row."""
    return KeyedRateLimitRef.typed(
        _TenantPayload,
        base_name=base_name,
        key_fn=lambda p: p.tenant_id,
        capacity=5,
        refill_per_second=0.5,
        backend="postgres",
    )


def _redis_ref(base_name: str) -> KeyedRateLimitRef:
    return KeyedRateLimitRef.typed(
        _TenantPayload,
        base_name=base_name,
        key_fn=lambda p: p.tenant_id,
        capacity=5,
        refill_per_second=0.5,
        backend="redis",
    )


def _res_ref(base_name: str, slots: int = 2) -> KeyedReservationRef:
    return KeyedReservationRef.typed(
        _SessionPayload,
        base_name=base_name,
        key_fn=lambda p: p.session_id,
        slots=slots,
        lease=timedelta(seconds=30),
    )


async def _fresh_schema(pg_dsn: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_schema()}" CASCADE')
        await apply_pending(conn, schema=_schema())
    finally:
        await conn.close()


async def _drop_schema(pg_dsn: str) -> None:
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_schema()}" CASCADE')
    finally:
        await conn.close()


async def _age_bucket_row_setup(pool: asyncpg.Pool, bucket: str, older_than: timedelta) -> None:
    """Stand up one fleet-reclaimable ``rate_limit_buckets`` row directly
    — the empty state document the keyed publish path writes before any
    acquire — aged past the horizon, for the batch-bound pin's
    population."""
    async with pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{_schema()}".rate_limit_buckets '
            f"(bucket_name, kind, state, keyed, last_used_at) "
            f"VALUES ($1, 'token_bucket', '{{}}'::jsonb, true, clock_timestamp() - $2::interval)",
            bucket,
            older_than,
        )


async def _insert_bucket_row_with_tokens(
    pool: asyncpg.Pool,
    bucket: str,
    *,
    tokens: float,
    capacity: float,
    refill: float,
    older_than: timedelta,
) -> None:
    """Stand up one fleet-reclaimable ``rate_limit_buckets`` row directly,
    in the exact shape the keyed PG-backend acquire's preseed/upsert
    writes, with an explicit token count and aged past the horizon."""
    async with pool.acquire() as conn:
        await conn.execute(
            f'INSERT INTO "{_schema()}".rate_limit_buckets '
            f"(bucket_name, kind, state, keyed, last_used_at) "
            f"VALUES ($1, 'token_bucket', "
            f"jsonb_build_object('tokens', $2::float8, 'ts', "
            f"EXTRACT(EPOCH FROM clock_timestamp()), 'capacity', $3::float8, "
            f"'refill', $4::float8), "
            f"true, clock_timestamp() - $5::interval)",
            bucket,
            tokens,
            capacity,
            refill,
            older_than,
        )


async def _sweep(pg_dsn: str, *, horizon: timedelta = _HORIZON, batch_size: int = _BATCH) -> int:
    """One leader-tick's worth of the fleet reclaim sweep, on a bare
    connection — the seam the maintenance leader drives."""
    conn = await asyncpg.connect(pg_dsn)
    try:
        return await sweep_idle_keyed_rows(
            conn, schema=_schema(), horizon=horizon, batch_size=batch_size
        )
    finally:
        await conn.close()


async def _slot_rows(pool: asyncpg.Pool, bucket: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*) FROM "{_schema()}".reservation_slots WHERE bucket_name = $1',
            bucket,
        )


async def _bucket_rows(pool: asyncpg.Pool, bucket: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*) FROM "{_schema()}".rate_limit_buckets WHERE bucket_name = $1',
            bucket,
        )


async def _slots_keyed(pool: asyncpg.Pool, bucket: str) -> bool | None:
    """Whether EVERY slot row of *bucket* carries the fleet-reclaimable
    mark (None when the bucket has no rows)."""
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT bool_and(keyed) FROM "{_schema()}".reservation_slots WHERE bucket_name = $1',
            bucket,
        )


async def _bucket_keyed(pool: asyncpg.Pool, bucket: str) -> bool | None:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT keyed FROM "{_schema()}".rate_limit_buckets WHERE bucket_name = $1',
            bucket,
        )


async def _max_slot_last_used(pool: asyncpg.Pool, bucket: str) -> object:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT max(last_used_at) FROM "{_schema()}".reservation_slots WHERE bucket_name = $1',
            bucket,
        )


async def _age_slots(pool: asyncpg.Pool, bucket: str, older_than: timedelta) -> None:
    """Row-timestamp mutation standing in for clock advance (the
    differential harness's doctrine) — ages *bucket*'s staleness stamps
    without waiting out a real horizon."""
    async with pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{_schema()}".reservation_slots '
            f"SET last_used_at = clock_timestamp() - $1::interval WHERE bucket_name = $2",
            older_than,
            bucket,
        )


async def _fresh_slot_count(pool: asyncpg.Pool, bucket: str) -> int:
    """How many of *bucket*'s slot rows carry a ``last_used_at`` inside the
    horizon, i.e. how many read as live to the sweep predicate."""
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*) FROM "{_schema()}".reservation_slots '
            "WHERE bucket_name = $1 AND last_used_at > clock_timestamp() - $2::interval",
            bucket,
            _HORIZON,
        )


async def _age_bucket_row(pool: asyncpg.Pool, bucket: str, older_than: timedelta) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{_schema()}".rate_limit_buckets '
            f"SET last_used_at = clock_timestamp() - $1::interval WHERE bucket_name = $2",
            older_than,
            bucket,
        )


async def test_orphaned_keyed_rows_are_reclaimed_after_worker_death(pg_dsn: str) -> None:
    """Pin (a): keyed rows whose creating worker died are gone after one
    sweep tick.

    The worker-death shape, exactly: the registry that materialised these
    rows is gone with the process (no pending-reclaim set, no tracked-key
    stamp, no row-schema capture survives it), and no live worker ever
    re-resolves the same concrete key — so the fleet sweep's row-borne
    staleness is the ONLY signal left that can name them."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        res_handles = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[_res_ref("orphan-res")],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s1"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        rl_handles = await reg.acquire_for_actor(
            rate_limits=[_pg_ref("orphan-rl")],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="acme"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        assert len(res_handles) == 1 and len(rl_handles) == 1
        await reg.release_for_actor(res_handles)
        # The worker dies: the registry (and every in-process reclaim
        # structure inside it) ceases to exist. Simulated by dropping the
        # reference entirely — nothing below consults it.
        del reg, res_handles, rl_handles

        res_bucket = "orphan-res:s1"
        rl_bucket = "orphan-rl:acme"
        assert await _slot_rows(pool, res_bucket) == 2, "fixture broken: ensure_slots did not run"
        assert await _bucket_rows(pool, rl_bucket) == 1, "fixture broken: no rate_limit_buckets row"
        assert await _slots_keyed(pool, res_bucket) is True, (
            "fixture broken: keyed-materialised reservation rows are not marked fleet-reclaimable"
        )
        assert await _bucket_keyed(pool, rl_bucket) is True, (
            "fixture broken: a PG-backend keyed bucket's row is not marked fleet-reclaimable"
        )

        # The horizon elapses with no acquire touching either bucket.
        await _age_slots(pool, res_bucket, _HORIZON * 2)
        await _age_bucket_row(pool, rl_bucket, _HORIZON * 2)

        deleted = await _sweep(pg_dsn)

        assert deleted == 3, f"expected 2 slot rows + 1 bucket row deleted, got {deleted}"
        assert await _slot_rows(pool, res_bucket) == 0, (
            f"the keyed reservation rows of {res_bucket!r} survived the fleet sweep although "
            "their creating worker died a horizon ago: no registry anywhere can name them "
            "(the pending-reclaim drain died with the process), so this sweep is the only "
            "deletion path left — steady-state cardinality is slots x every key ever "
            "materialised by a process that later died"
        )
        assert await _bucket_rows(pool, rl_bucket) == 0, (
            f"the keyed rate-limit row of {rl_bucket!r} survived the fleet sweep although "
            "its creating worker died a horizon ago — same orphan shape, one "
            "rate_limit_buckets row per dead-worker key, unbounded in the caller-controlled "
            "key space"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


async def test_static_bucket_rows_survive_the_sweep_forever(pg_dsn: str) -> None:
    """Pin (b): static buckets are never deleted — not at any age, past
    any horizon.

    A static reservation has no keyed lifecycle and no acquire-path heal
    (the registered-bucket-with-zero-rows trap is keyed-only), so a sweep
    that deleted its rows would leave a permanently denying limiter. The
    ``keyed`` mark excludes static rows from the sweep's predicate
    entirely: horizon-independent immortality, pinned here by ageing the
    rows a horizon wider than their staleness."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        # The two static shapes: startup ensure_slots for a declared
        # reservation, and the startup sync publish for a declared
        # rate limit.
        static_res = ConcurrencyReservation(
            name="fleet-static-res",
            slots=2,
            lease=timedelta(seconds=30),
            schema=_schema(),
        )
        await static_res.ensure_slots(pool)
        reg = RateLimitRegistry()
        reg.register(
            TokenBucket(
                name="fleet-static-rl",
                capacity=5,
                refill_per_second=0.5,
                backend="postgres",
            )
        )
        await sync_rate_limit_buckets(reg, pool, schema=_schema())

        assert await _slot_rows(pool, "fleet-static-res") == 2
        assert await _bucket_rows(pool, "fleet-static-rl") == 1
        assert await _slots_keyed(pool, "fleet-static-res") is False, (
            "fixture broken: a statically declared reservation's rows must never carry "
            "the fleet-reclaimable mark"
        )
        assert await _bucket_keyed(pool, "fleet-static-rl") is False, (
            "fixture broken: the startup sync publish must never mark a row fleet-reclaimable"
        )

        # 30 days stale, swept at the production horizon AND at a horizon
        # narrower than their age — no horizon may touch them.
        await _age_slots(pool, "fleet-static-res", timedelta(days=30))
        await _age_bucket_row(pool, "fleet-static-rl", timedelta(days=30))
        await _sweep(pg_dsn)
        await _sweep(pg_dsn, horizon=timedelta(days=29))

        assert await _slot_rows(pool, "fleet-static-res") == 2, (
            "a STATIC reservation's slot rows were deleted by the fleet sweep: static "
            "buckets have no keyed lifecycle, no heal, and no re-materialisation — the "
            "deletion is a permanently denying limiter, not a reclamation"
        )
        assert await _bucket_rows(pool, "fleet-static-rl") == 1, (
            "a STATIC rate limit's rate_limit_buckets row was deleted by the fleet sweep "
            "at an age any keyed row would have been reclaimed at — the keyed mark, not "
            "the horizon, is what must gate this sweep"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


async def test_redis_backend_keyed_row_is_never_swept(pg_dsn: str) -> None:
    """Pin (c): a redis-backend keyed bucket's PG row survives — never
    swept.

    The redis-backend keyed bucket's state lives in Redis (self-bounding
    via the Lua script's EXPIRE TTL); its PG row is admin-UI metadata
    plus the Redis-outage fallback's state carrier. Its ``last_used_at``
    cannot track Redis-side use — the healthy acquire path never touches
    PG — so a staleness sweep would reclaim the row of an actively-used
    bucket and reset the fallback state of a fixed-quota one mid-outage.
    The row is therefore never marked fleet-reclaimable."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        # The resolution publishes the row (best-effort, idempotent);
        # the acquire then needs a Redis client this test does not
        # inject, so it raises AFTER the publish — the RuntimeError is
        # the fixture's publish trigger, not a failure under test.
        with pytest.raises(RuntimeError, match="redis_client not injected"):
            await reg.acquire_for_actor(
                rate_limits=[_redis_ref("fleet-redis")],
                reservations=[],
                job_id=new_uuid(),
                worker_id=new_uuid(),
                payload=_TenantPayload(tenant_id="zeta"),
                pg_pool=pool,
                settings=_settings(pg_dsn),
            )

        bucket = "fleet-redis:zeta"
        assert await _bucket_rows(pool, bucket) == 1, (
            "fixture broken: the keyed materialisation publish did not create the admin row"
        )
        assert await _bucket_keyed(pool, bucket) is False, (
            "a redis-backend keyed bucket's PG row was marked fleet-reclaimable: its "
            "liveness signal lives in Redis (the healthy acquire never touches PG), so "
            "the mark would let the sweep reclaim the row of an actively-used bucket"
        )

        await _age_bucket_row(pool, bucket, timedelta(days=30))
        await _sweep(pg_dsn)
        await _sweep(pg_dsn, horizon=timedelta(days=29))

        assert await _bucket_rows(pool, bucket) == 1, (
            f"the redis-backend keyed row {bucket!r} was swept: redis-backend rows are "
            "never fleet-reclaimable — the PG row is outage-fallback state plus admin "
            "metadata, and no PG-side staleness signal can speak for Redis-side use"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


async def test_stale_keyed_row_is_refreshed_by_acquire_and_release(pg_dsn: str) -> None:
    """Pin (d): ``last_used_at`` rides the existing acquire/release
    writes — the statements that already touch the row refresh the
    staleness stamp; no dedicated stamping round trip exists.

    Aged rows that are acquired again come back fresh on BOTH tables
    (the reservation acquire CTE and the token bucket's preseed/upsert),
    and the reservation release stamps the row it frees."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        # Reservation side: materialise, age, re-acquire, age, release.
        handles = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[_res_ref("stamp-res")],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s1"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        res_bucket = "stamp-res:s1"
        assert await _max_slot_last_used(pool, res_bucket) is not None
        await _age_slots(pool, res_bucket, _HORIZON * 2)
        reacquired = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[_res_ref("stamp-res")],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s1"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        assert len(reacquired) == 1
        async with pool.acquire() as conn:
            fresh_slot_stamp = await conn.fetchval(
                f"SELECT now() - max(last_used_at) < interval '5 seconds' "
                f'FROM "{_schema()}".reservation_slots WHERE bucket_name = $1',
                res_bucket,
            )
        assert fresh_slot_stamp is True, (
            "the reservation acquire did not refresh last_used_at: the stamp must ride "
            "the acquire CTE's existing UPDATE that already touches the row, not as a "
            "separate round trip — a re-activated key must not look stale the instant "
            "it is used, or the next sweep tick reclaims a live bucket's rows"
        )
        await _age_slots(pool, res_bucket, _HORIZON * 2)
        await reg.release_for_actor(reacquired)
        async with pool.acquire() as conn:
            released_stamp = await conn.fetchval(
                f"SELECT now() - max(last_used_at) < interval '5 seconds' "
                f'FROM "{_schema()}".reservation_slots WHERE bucket_name = $1',
                res_bucket,
            )
        assert released_stamp is True, (
            "the reservation release did not refresh last_used_at: the release UPDATE "
            "already touches the row it frees, and an unstamped release lets a "
            "just-freed bucket look idle past the horizon mid-workflow"
        )
        await reg.release_for_actor(handles)

        # Rate-limit side: materialise, age, re-acquire.
        rl_handles = await reg.acquire_for_actor(
            rate_limits=[_pg_ref("stamp-rl")],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="acme"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        assert len(rl_handles) == 1
        rl_bucket = "stamp-rl:acme"
        await _age_bucket_row(pool, rl_bucket, _HORIZON * 2)
        again = await reg.acquire_for_actor(
            rate_limits=[_pg_ref("stamp-rl")],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="acme"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        assert len(again) == 1
        async with pool.acquire() as conn:
            fresh_bucket_stamp = await conn.fetchval(
                f"SELECT now() - last_used_at < interval '5 seconds' "
                f'FROM "{_schema()}".rate_limit_buckets WHERE bucket_name = $1',
                rl_bucket,
            )
        assert fresh_bucket_stamp is True, (
            "the token-bucket acquire did not refresh last_used_at: the preseed/upsert "
            "pair already writes the row on every PG acquire, and the stamp must ride "
            "those statements — a dedicated stamping round trip on the hot path is the "
            "cost this design exists to avoid"
        )

        # The refreshed rows are not swept at the production horizon:
        # a re-activated key is live, whatever its age was.
        deleted = await _sweep(pg_dsn)
        assert deleted == 0, (
            f"freshly-acquired keyed rows were swept ({deleted} deleted): the acquire "
            "stamp is the liveness signal the sweep trusts — a sweep that deletes "
            "fresh rows deletes live buckets"
        )
        assert await _slot_rows(pool, res_bucket) == 2
        assert await _bucket_rows(pool, rl_bucket) == 1
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


async def test_live_held_slot_vetoes_the_whole_bucket(pg_dsn: str) -> None:
    """A keyed bucket with one live-held slot keeps ALL its rows.

    The fleet sweep reclaims buckets whole or not at all: deleting a
    stale bucket's free rows while a live lease holds one would shrink
    the bucket's configured capacity (the acquire-path heal only fires
    at ZERO rows — a partially-deleted bucket denies with a silently
    smaller slot count forever). The veto predicate is whole-bucket:
    every row free-or-lease-expired, every row keyed, every row's stamp
    past the horizon."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        # Acquire one of the two slots and KEEP it held (live lease).
        reg = RateLimitRegistry()
        handles = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[_res_ref("held-res", slots=2)],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s1"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        assert len(handles) == 1
        bucket = "held-res:s1"
        await _age_slots(pool, bucket, _HORIZON * 2)

        deleted = await _sweep(pg_dsn)

        assert deleted == 0, f"the sweep deleted {deleted} rows of a live-held bucket"
        assert await _slot_rows(pool, bucket) == 2, (
            "a keyed bucket with one live-held slot lost rows to the fleet sweep: the "
            "held row survives (lease guard) but its FREE sibling was deleted — the "
            "bucket now runs at half its configured capacity with no heal path (the "
            "heal fires only at zero rows)"
        )
        # After the lease dies (job gone, heartbeat stopped) the whole
        # bucket becomes reclaimable — the veto is about liveness, not
        # permanence.
        await reg.release_for_actor(handles)
        await _age_slots(pool, bucket, _HORIZON * 2)
        deleted = await _sweep(pg_dsn)
        assert deleted == 2, (
            f"after the hold ended the stale bucket was not fully reclaimed ({deleted} "
            "rows deleted, expected 2) — the veto must track liveness, not latch"
        )
        assert await _slot_rows(pool, bucket) == 0
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


async def test_zero_horizon_is_rejected_at_the_sweep_boundary(pg_dsn: str) -> None:
    """``timedelta(0)`` is the settings-level disable sentinel, never a
    sweep argument: at the function boundary zero would read as "delete
    every keyed row older than now", the dangerous misreading, so it is
    rejected as a caller wiring bug — the same contract
    ``sweep_expired_events`` enforces for the event retention."""
    await _fresh_schema(pg_dsn)
    try:
        conn = await asyncpg.connect(pg_dsn)
        try:
            with pytest.raises(ValueError, match="disable sentinel"):
                await sweep_idle_keyed_rows(
                    conn, schema=_schema(), horizon=timedelta(0), batch_size=_BATCH
                )
        finally:
            await conn.close()
    finally:
        await _drop_schema(pg_dsn)


async def test_consumed_fixed_quota_bucket_survives_the_fleet_sweep(pg_dsn: str) -> None:
    """A PG-backend keyed bucket that has consumed part of a fixed quota
    keeps its row and its remaining quota across the fleet sweep.

    A fixed quota (``refill_per_second == 0``) is drained forever by
    design, so it must never appear to regain capacity just because
    nobody acquired it for an hour. The ``rate_limit_buckets`` arm of the
    sweep needs the same kind of state check the ``reservation_slots``
    arm gets from its whole-bucket veto: idleness alone is not evidence
    that a row is safe to delete. Deleting it lets the next acquire
    re-preseed via ``ON CONFLICT DO NOTHING`` at full capacity,
    over-admitting against a budget that was already spent. A
    fixed-quota bucket has no lease concept at all, so no lease or hold
    guard covers this case.
    """
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        bucket = TokenBucket(
            name="quota-drain",
            capacity=5,
            refill_per_second=0.0,  # fixed-quota: never refills on its own
            backend="postgres",
            keyed=True,
        )
        settings = _settings(pg_dsn)

        # Consume 4 of 5 tokens for real, through the acquire path (not a
        # hand-inserted row) — the row now genuinely carries consumed
        # quota state, exactly like the issue's scenario.
        for _ in range(4):
            decision = await bucket.acquire(1.0, pg_pool=pool, settings=settings)
            assert decision.allowed

        row_name = bucket.name  # non-keyed-ref direct TokenBucket use: bucket_name == name
        state_before = await bucket.peek(pg_pool=pool, settings=settings)
        assert state_before.tokens_remaining == pytest.approx(1.0), (
            "fixture broken: expected 1 of 5 tokens remaining after 4 acquires"
        )
        assert await _bucket_rows(pool, row_name) == 1
        assert await _bucket_keyed(pool, row_name) is True, (
            "fixture broken: a keyed=True TokenBucket must publish a fleet-reclaimable row"
        )

        # The bucket goes idle past the horizon with no acquire touching
        # it — no lease, no hold, nothing but ordinary idleness. The
        # sibling reservation_slots sweep vetoes a live-held bucket even
        # PAST the horizon; this bucket has consumed quota but nothing
        # analogous protects it.
        await _age_bucket_row(pool, row_name, _HORIZON * 2)

        deleted = await _sweep(pg_dsn)

        assert deleted == 0, (
            f"the fleet sweep deleted {deleted} row(s) of a PG keyed bucket that had "
            f"consumed 4/5 of its FIXED quota (refill_per_second=0): the "
            "rate_limit_buckets sweep predicate (keyed AND last_used_at < horizon) has "
            "no quota-state check analogous to the reservation_slots sweep's whole-bucket "
            "veto, so ordinary idleness, not worker death, silently deletes a row that "
            "still carries live consumed state. The next acquire re-preseeds via "
            "ON CONFLICT DO NOTHING at full capacity, resetting the quota."
        )
        assert await _bucket_rows(pool, row_name) == 1, (
            f"the rate_limit_buckets row for {row_name!r} was deleted by the fleet sweep "
            "although it carried consumed fixed-quota state with no lease/hold guard of "
            "any kind protecting it"
        )

        state_after = await bucket.peek(pg_pool=pool, settings=settings)
        assert state_after.tokens_remaining == pytest.approx(1.0), (
            f"the bucket's remaining quota changed from 1.0 to {state_after.tokens_remaining} "
            "across the fleet sweep: a state-safe sweep must never alter a live bucket's "
            "consumed quota, whether by deleting the row (reset to full on re-preseed) or "
            "any other means"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


async def test_legacy_bucket_row_without_quota_keys_survives_the_fleet_sweep(
    pg_dsn: str,
) -> None:
    """A ``rate_limit_buckets`` row whose state predates the quota keys
    survives the fleet sweep past the horizon.

    The write shape before ``capacity``/``refill`` rode along in ``state``
    was ``{"tokens", "ts"}`` and nothing else: a real token count with
    nothing stored to evaluate it against. That row could be a refilling
    bucket mid-window (safe to delete — its state converges back to full)
    or a fixed quota partly spent (deleting it lets the next acquire
    re-preseed at full capacity, handing back a budget the tenant already
    used). The sweep cannot tell the two apart from the row, so the row
    is kept: the veto is fail-closed on missing keys, the same "cannot
    prove safe to delete" rule the consumed-fixed-quota case already
    states. A kept legacy row is not a leak — the first acquire after the
    keys existed rewrites the state with the full document, so the kept
    population is bounded by rows nothing has touched since.

    The companion never-acquired row (``{}`` state — what the keyed
    publish path writes before any acquire) carries no token count at
    all, so the same sweep must still delete it: the veto protects
    unprovable state, not the absence of state.
    """
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        legacy = "taskq:ratelimit:pg:tenant:legacy-state"
        never_acquired = "taskq:ratelimit:pg:tenant:published-never-acquired"
        async with pool.acquire() as conn:
            await conn.execute(
                f'INSERT INTO "{_schema()}".rate_limit_buckets '
                f"(bucket_name, kind, state, keyed, last_used_at) "
                f"VALUES ($1, 'token_bucket', "
                f"jsonb_build_object('tokens', $2::float8, 'ts', "
                f"EXTRACT(EPOCH FROM clock_timestamp())), "
                f"true, clock_timestamp() - $3::interval)",
                legacy,
                2.0,
                _HORIZON + timedelta(minutes=5),
            )
        await _age_bucket_row_setup(pool, never_acquired, _HORIZON + timedelta(minutes=5))

        deleted = await _sweep(pg_dsn)

        assert await _bucket_rows(pool, never_acquired) == 0, (
            "the never-acquired published row survived the sweep although its "
            "state carries no token count at all: the veto protects unprovable "
            "state, not the absence of state — sparing empty rows keeps one "
            "permanent row per key ever published, the unbounded growth this "
            "sweep exists to bound"
        )
        assert deleted == 1, (
            f"the sweep should have deleted exactly the provably-safe row; got {deleted}"
        )
        assert await _bucket_rows(pool, legacy) == 1, (
            f"the fleet sweep deleted the legacy row {legacy!r} although its state "
            "carries a stored token count without the capacity/refill keys needed "
            "to prove deletion safe: if that count is a partly spent fixed quota, "
            "the next acquire re-preseeds at full capacity and hands back a budget "
            "the tenant already used — a row the sweep cannot prove safe to delete "
            "is one it must keep"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


async def _stand_up_bursty_live_bucket(pool: asyncpg.Pool, bucket: str) -> None:
    """Materialise *bucket* as a keyed 5-slot reservation in the shape of a
    bursty tenant: configured for 5 concurrent, but only ever running 1.

    Materialise once (mirroring real bootstrap), age every row past the
    horizon, then perform one real acquire/release pair. ``acquire`` and
    ``release`` stamp only the single slot row each one acts on, never the
    bucket's siblings, so exactly one row comes back fresh and the other
    four keep their aged stamps. That leaves the bucket live but carrying
    an old per-row ``min(last_used_at)``, the precondition this class of
    starvation depends on. The acquire is driven through
    ``ConcurrencyReservation`` directly rather than through the registry,
    because a registry acquire on a not-yet-materialised key re-runs
    ``ensure_slots``, whose ON CONFLICT arm refreshes ``last_used_at`` on
    every existing row and would undo the aging step.
    """
    res = ConcurrencyReservation(
        name=bucket,
        slots=5,
        lease=timedelta(seconds=30),
        schema=_schema(),
        keyed=True,
    )
    await res.ensure_slots(pool)
    await _age_slots(pool, bucket, _HORIZON * 3)
    lease = await res.acquire(new_uuid(), new_uuid(), pool=pool)
    await res.release(lease, new_uuid(), pool=pool)


async def _stand_up_orphan_bucket(pool: asyncpg.Pool, bucket: str) -> None:
    """Materialise *bucket* as a genuine dead-worker orphan: a keyed 2-slot
    reservation whose every row is stale and which nothing ever touches
    again."""
    res = ConcurrencyReservation(
        name=bucket,
        slots=2,
        lease=timedelta(seconds=30),
        schema=_schema(),
        keyed=True,
    )
    await res.ensure_slots(pool)
    await _age_slots(pool, bucket, _HORIZON * 3)


async def test_live_bursty_buckets_do_not_starve_a_real_orphan_out_of_the_window(
    pg_dsn: str,
) -> None:
    """A real dead-worker orphan is reclaimed even when live buckets could
    fill the entire candidate window.

    The sweep picks its candidates by per-row ``min(last_used_at)`` under a
    ``batch_size`` LIMIT. A bucket configured for more concurrency than it
    ever uses keeps permanently idle high-index slot rows, so its per-row
    minimum stays old however live the bucket actually is. Such a bucket
    sorts into the candidate window, consumes one of its LIMIT slots, and
    is then correctly spared by the whole-bucket veto. With enough of them,
    a genuine orphan sitting behind them in the oldest-first ordering never
    enters the window at all, and the tick nets zero deletions while
    unreclaimed rows accumulate without bound.

    Fixture: 3 live bursty buckets (5 slots each, one fresh row apiece)
    plus 1 orphan (2 slots, all stale), horizon 1h, ``batch_size=3`` --
    wide enough to admit exactly the 3 live buckets and nothing else if
    there is no per-bucket liveness pre-filter on candidate selection.
    """
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=6)
    try:
        live_buckets = [f"starve-res:tenant{i}" for i in range(3)]
        for bucket in live_buckets:
            await _stand_up_bursty_live_bucket(pool, bucket)

        orphan_bucket = "starve-res:orphan"
        await _stand_up_orphan_bucket(pool, orphan_bucket)

        # Sanity: each live bucket reads as live through exactly one fresh
        # slot row while keeping an old per-row minimum from the rest.
        for bucket in live_buckets:
            assert await _fresh_slot_count(pool, bucket) == 1, (
                f"fixture broken: {bucket!r} must have exactly one fresh slot row "
                "(the live acquire/release) with the rest stale"
            )
            assert await _slot_rows(pool, bucket) == 5
        assert await _slot_rows(pool, orphan_bucket) == 2

        deleted = await _sweep(pg_dsn, batch_size=3)

        assert deleted > 0 and await _slot_rows(pool, orphan_bucket) == 0, (
            "the real dead-worker orphan bucket was not reclaimed this tick: the "
            "candidate selection orders and limits purely on per-row "
            "min(last_used_at) with no per-bucket liveness pre-filter, so the 3 "
            "live-but-bursty buckets (each with an old minimum from its idle "
            "high-index slots) fill the batch_size=3 window, are correctly vetoed, "
            "and starve the real orphan out of the window entirely -- net 0 "
            "deletions even though a genuine orphan exists and is older than the "
            "horizon"
        )
        for bucket in live_buckets:
            assert await _slot_rows(pool, bucket) == 5, (
                f"live bucket {bucket!r} must keep all 5 configured slot rows: the "
                "whole-bucket veto still applies, and a partial delete would "
                "silently shrink its configured capacity"
            )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


async def test_orphan_reclaim_makes_progress_within_a_bounded_number_of_ticks(
    pg_dsn: str,
) -> None:
    """Repeated sweep ticks reclaim an orphan that live buckets crowd out
    of a single tick's candidate window.

    Even where one tick cannot be guaranteed to reach the orphan, the sweep
    must make monotonic progress: a bounded number of ticks, no more than
    the number of live buckets able to occupy the window, must be enough. A
    candidate selection dominated by live buckets can instead loop at zero
    net deletions forever, because those buckets re-sort to the front every
    tick. Their per-row ``min(last_used_at)`` never advances, since the
    idle high-index rows that supply the minimum are never touched, so no
    amount of waiting lets the orphan through.
    """
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=6)
    try:
        for i in range(3):
            await _stand_up_bursty_live_bucket(pool, f"starve-res:tenant{i}")

        orphan_bucket = "starve-res:orphan"
        await _stand_up_orphan_bucket(pool, orphan_bucket)

        # Generously more ticks than the 3 live buckets that could occupy
        # the window, so a sweep making any progress at all gets there.
        reclaimed = False
        for _ in range(5):
            await _sweep(pg_dsn, batch_size=3)
            if await _slot_rows(pool, orphan_bucket) == 0:
                reclaimed = True
                break

        assert reclaimed, (
            "the real orphan bucket was still unreclaimed after 5 sweep ticks "
            "(batch_size=3, 3 live bursty buckets ahead of it): the candidate "
            "window keeps re-selecting the same live buckets tick after tick, "
            "since their per-row min(last_used_at) never advances while their "
            "idle high-index slots go untouched, so the sweep never makes any "
            "progress toward the real orphan"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


async def test_sweep_is_bounded_oldest_first_and_drains_per_call(pg_dsn: str) -> None:
    """One call deletes at most ``batch_size`` buckets (rows, for
    ``rate_limit_buckets``) — the #120 doctrine's constant-size
    committed batch per tick — oldest first, and repeated calls drain
    the backlog."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        # Three stale keyed buckets per table with distinct ages, so the
        # oldest-first ordering is deterministic.
        for key, age in (("old", 3.0), ("mid", 2.0), ("new", 1.5)):
            res_bucket = f"bound-res:{key}"
            rl_bucket = f"bound-rl:{key}"
            res = ConcurrencyReservation(
                name=res_bucket,
                slots=1,
                lease=timedelta(seconds=30),
                schema=_schema(),
                keyed=True,
            )
            await res.ensure_slots(pool)
            await _age_slots(pool, res_bucket, timedelta(hours=age))
            await _age_bucket_row_setup(pool, rl_bucket, timedelta(hours=age))

        first = await _sweep(pg_dsn, batch_size=2)
        second = await _sweep(pg_dsn, batch_size=2)
        third = await _sweep(pg_dsn, batch_size=2)

        assert first == 4, (
            f"batch_size=2 must cap one call at 2 buckets' rows (2 reservation buckets "
            f"x 1 slot + 2 rate-limit rows = 4 rows); got {first}"
        )
        assert second == 2, f"the second call must drain the remaining bucket; got {second}"
        assert third == 0, "the drained steady state must be a clean no-op"
        assert await _slot_rows(pool, "bound-res:old") == 0
        assert await _bucket_rows(pool, "bound-rl:old") == 0
        assert await _slot_rows(pool, "bound-res:new") == 0
        assert await _bucket_rows(pool, "bound-rl:new") == 0
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


async def test_refilling_keyed_bucket_below_capacity_remains_reclaimable(pg_dsn: str) -> None:
    """A refilling PG-backend keyed bucket is still reclaimed once idle
    past the horizon, even with tokens below capacity at the moment the
    sweep runs.

    The state a refilling bucket carries is not a spent budget: a
    positive ``refill_per_second`` returns it to full on its own, and an
    hour of idleness past the reclaim horizon is far longer than any
    sane refill window, so the row's stored token count is already stale
    by the time it is eligible. Protecting it would make every key ever
    materialised keep its row forever — the unbounded growth this sweep
    exists to bound — while buying no correctness at all.

    The protection belongs strictly to the fixed-quota case
    (``refill_per_second == 0``), where consumed tokens never come back
    and the row IS the record of what was spent. Drawing the line at
    "any bucket not at full capacity" instead of at "zero refill"
    collapses that distinction and trades over-admission for an
    unbounded table.
    """
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        bucket = "taskq:ratelimit:pg:tenant:partial-refill"
        # Capacity 5, only 1 token stored: below capacity, but refilling.
        await _insert_bucket_row_with_tokens(
            pool,
            bucket,
            tokens=1.0,
            capacity=5.0,
            refill=0.5,
            older_than=_HORIZON + timedelta(minutes=5),
        )

        await _sweep(pg_dsn)

        assert await _bucket_rows(pool, bucket) == 0, (
            f"the idle refilling bucket row {bucket!r} was not reclaimed by the "
            "fleet sweep: a below-capacity token count on a bucket that refills "
            "is not consumed quota worth preserving, and sparing every such row "
            "leaves one permanent row per key ever materialised — the state "
            "check on this arm must key on a zero refill rate, not on tokens "
            "being short of capacity"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


async def test_keyed_bucket_at_full_capacity_remains_reclaimable(pg_dsn: str) -> None:
    """A keyed bucket genuinely back at full capacity is still reclaimed
    once idle past the horizon.

    It carries no consumed state to lose, so protecting it would buy
    nothing and would trade the over-admission bug for unbounded row
    growth: every key ever materialised would keep its row forever.
    """
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        bucket = "taskq:ratelimit:pg:tenant:full-capacity"
        await _insert_bucket_row_with_tokens(
            pool,
            bucket,
            tokens=5.0,
            capacity=5.0,
            refill=0.0,
            older_than=_HORIZON + timedelta(minutes=5),
        )

        await _sweep(pg_dsn)

        assert await _bucket_rows(pool, bucket) == 0, (
            "a bucket genuinely at full capacity was not reclaimed by the fleet "
            "sweep: protecting consumed state must not sacrifice bounded row "
            "growth for buckets that carry no state to protect"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


def _pg_fixed_quota_ref(base_name: str) -> KeyedRateLimitRef:
    """A keyed PG-backend fixed-quota rate limit: no refill, so every
    consumed token is consumed for good."""
    return KeyedRateLimitRef.typed(
        _TenantPayload,
        base_name=base_name,
        key_fn=lambda p: p.tenant_id,
        capacity=5,
        refill_per_second=0.0,
        backend="postgres",
    )


async def test_eviction_drain_preserves_consumed_pg_fixed_quota(pg_dsn: str) -> None:
    """The per-worker idle eviction and its reclaim drain leave a
    PG-backend keyed fixed-quota bucket's consumed state intact.

    Idle eviction exists to bound registry growth, not to reset quotas.
    It exempts a memory-backed fixed-quota bucket that still holds
    consumed quota, because evicting it would destroy state that lives
    only on the instance. A PG-backed bucket keeps its state in the
    ``rate_limit_buckets`` row instead, so the equivalent damage is done
    by the drain that deletes that row: the next acquire re-preseeds at
    full capacity and re-admits a budget that was already spent. Whatever
    the mechanism, an eviction cycle on an idle key must never hand back
    capacity the tenant already used.
    """
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        settings = _settings(pg_dsn)

        # Spend 3 of 5 tokens for real, through the acquire path, so the
        # published row genuinely carries consumed quota.
        for _ in range(3):
            handles = await reg.acquire_for_actor(
                rate_limits=[_pg_fixed_quota_ref("evict-quota")],
                reservations=[],
                job_id=new_uuid(),
                worker_id=new_uuid(),
                payload=_TenantPayload(tenant_id="acme"),
                pg_pool=pool,
                settings=settings,
            )
            assert len(handles) == 1, "fixture broken: the keyed acquire was not admitted"

        rl_bucket = "evict-quota:acme"
        assert await _bucket_rows(pool, rl_bucket) == 1, (
            "fixture broken: no rate_limit_buckets row was published"
        )
        probe = TokenBucket(
            name=rl_bucket,
            capacity=5,
            refill_per_second=0.0,
            backend="postgres",
            keyed=True,
        )
        before = await probe.peek(pg_pool=pool, settings=settings)
        assert before.tokens_remaining == pytest.approx(2.0), (
            "fixture broken: expected 2 of 5 tokens remaining after 3 acquires"
        )

        # The key goes idle, so the worker's own sweep evicts it and the
        # drain runs against its pending set.
        reg.evict_idle_keyed_rate_limits(timedelta(0))
        await reg.drain_pending_reservation_reclaims(pool)

        assert await _bucket_rows(pool, rl_bucket) == 1, (
            f"the rate_limit_buckets row for {rl_bucket!r} was deleted by the "
            "eviction drain although it carried consumed fixed-quota state: the "
            "eviction exemption only covers memory-backed buckets, so a PG-backed "
            "bucket loses the row that IS its state, and the next acquire "
            "re-preseeds at full capacity"
        )
        after = await probe.peek(pg_pool=pool, settings=settings)
        assert after.tokens_remaining == pytest.approx(2.0), (
            f"the bucket's remaining fixed quota changed from 2.0 to "
            f"{after.tokens_remaining} across an idle eviction cycle: evicting a "
            "registry entry to bound memory must never restore spent quota"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)
