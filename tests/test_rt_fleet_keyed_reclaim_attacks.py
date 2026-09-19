# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.

"""Red-team attacks on the fleet keyed-row reclamation wave (e5d2154).

The feature under attack: keyed ``reservation_slots`` /
``rate_limit_buckets`` rows carry their own staleness (the ``keyed``
mark plus ``last_used_at``, refreshed by the acquire / release / upsert
statements that already touch them - piggybacking freshness on statements
already in flight), and the maintenance leader's ``sweep_idle_keyed_rows``
deletes marked rows unused past ``keyed_row_reclaim_period``, one bounded
committed batch per tick per table. The author's pins
(``tests/test_keyed_row_fleet_reclaim.py`` and the wiring/capture
siblings) cover the steady states: orphan reclamation, static
immortality, the redis-backend publish, the acquire/release stamps, the
live-lease veto, the disable sentinel, the batch bound, the leader
wiring. This file attacks the INTERLEAVINGS and the mark's writers
those pins cannot reach - each test names the hypothesis it drives:

* **the sweep-vs-live-use races** - a write landing between the sweep's
  candidate window and its DELETE (an acquire, a release). The
  hypothesis: the DELETE must re-check eligibility under its row lock
  (EvalPlanQual), or a live row dies / a live bucket is partially
  deleted. The slots arm's guard re-checks only ``job_id``/lease; the
  buckets arm re-checks the stamp too - the asymmetry is probed from
  both sides.
* **refresh-on-use completeness** - every statement that touches a
  keyed row must stamp it. The author pins the acquire/release and the
  token bucket's acquire; unpinned are the token bucket's REFUND and
  the re-materialisation ``ensure_slots`` over rows that SURVIVED (the
  conflict arm - the registry's re-resolve path after an eviction whose
  rows outlived the drain).
* **the mark's birth and death** - whether any path can flip a STATIC
  bucket's rows fleet-reclaimable (the migration's own deny-forever
  trap; the in-process collision guard cannot see across processes),
  and whether the mark and stamp are reborn correctly when a fleet
  sweep deletion is healed by the next acquire.
* **rolling-deploy skew** - the previous release's unstamped acquire
  against the new sweep (is the horizon/mark shape safe for a live-held
  row?), and the pre-migration ``UndefinedColumnError`` the leader
  block claims to tolerate per tick.
* **the redis-backend carve-out through the outage fallback** - the one
  path where a redis-backend keyed bucket's PG row IS written: the
  fallback's preseed/upsert must keep the row unmarked, and the sweep
  must never reset mid-outage fallback state.

An attack that lands red stays red - it is the finding, reported with
the design's own words it contradicts. A green attack is a refutation
the suite keeps as a pin.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
import redis.asyncio as redis_async
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.backend._sweeps import sweep_idle_keyed_rows
from taskq.backend.clock import Clock
from taskq.migrate import apply_pending
from taskq.ratelimit.composition import RateLimitHandle
from taskq.ratelimit.refs import KeyedRateLimitRef, KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import (
    _ACQUIRE_SQL_TEMPLATE,  # pyright: ignore[reportPrivateUsage]  # Why: the race tests must run the production acquire statement itself, held open in a transaction - a paraphrase would prove nothing about the real interleaving.
    _RELEASE_SQL_TEMPLATE,  # pyright: ignore[reportPrivateUsage]  # Why: same - the production release statement, held open, is the interleaving under test.
    ConcurrencyReservation,
)
from taskq.ratelimit.token_bucket import TokenBucket
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.worker._leader_shared import SweepContext
from taskq.worker.deps import WorkerDeps

#: The production horizon (``WorkerSettings.keyed_row_reclaim_period``
#: default) - rows are aged past it rather than shrinking it, so the
#: sweep predicate runs exactly as the leader drives it.
_HORIZON = timedelta(hours=1)

#: The production batch bound's magnitude - wide enough that every test
#: below is capped by eligibility, not by the batch.
_BATCH = 256


class _TenantPayload(BaseModel):
    tenant_id: str


class _SessionPayload(BaseModel):
    session_id: str


def _schema() -> str:
    """This file's dedicated schema (local, per the suite-hygiene rule
    against module-level schema constants)."""
    return "taskq_fleet_reclaim_attack"


def _settings(pg_dsn: str) -> WorkerSettings:
    return WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": _schema(),
        },
        validate=False,
    )


def _pg_ref(base_name: str) -> KeyedRateLimitRef:
    """A keyed rate limit on the PG backend - the acquire path's own
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


async def _sweep(pg_dsn: str, *, horizon: timedelta = _HORIZON, batch_size: int = _BATCH) -> int:
    """One leader-tick's worth of the fleet reclaim sweep, on a bare
    connection - the seam the maintenance leader drives. Safe to run as
    a ``asyncio`` task: the connection's lifecycle is self-contained."""
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


async def _slots_stamp_is_fresh(pool: asyncpg.Pool, bucket: str) -> bool | None:
    """Whether *bucket*'s newest slot stamp is within 5 seconds of now."""
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f"SELECT now() - max(last_used_at) < interval '5 seconds' "
            f'FROM "{_schema()}".reservation_slots WHERE bucket_name = $1',
            bucket,
        )


async def _bucket_stamp_is_fresh(pool: asyncpg.Pool, bucket: str) -> bool | None:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f"SELECT now() - last_used_at < interval '5 seconds' "
            f'FROM "{_schema()}".rate_limit_buckets WHERE bucket_name = $1',
            bucket,
        )


async def _bucket_state_tokens(pool: asyncpg.Pool, bucket: str) -> float | None:
    """The stored token count of *bucket*'s ``rate_limit_buckets`` row -
    the outage-fallback state a redis-backend keyed bucket carries."""
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f"SELECT (state->>'tokens')::float8 "
            f'FROM "{_schema()}".rate_limit_buckets WHERE bucket_name = $1',
            bucket,
        )


async def _age_slots(pool: asyncpg.Pool, bucket: str, older_than: timedelta) -> None:
    """Row-timestamp mutation standing in for clock advance (the
    differential harness's doctrine) - ages *bucket*'s staleness stamps
    without waiting out a real horizon."""
    async with pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{_schema()}".reservation_slots '
            f"SET last_used_at = clock_timestamp() - $1::interval WHERE bucket_name = $2",
            older_than,
            bucket,
        )


async def _age_bucket_row(pool: asyncpg.Pool, bucket: str, older_than: timedelta) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            f'UPDATE "{_schema()}".rate_limit_buckets '
            f"SET last_used_at = clock_timestamp() - $1::interval WHERE bucket_name = $2",
            older_than,
            bucket,
        )


async def _await_lock_waiter(pool: asyncpg.Pool) -> None:
    """Block until some OTHER session in this database waits on a row lock.

    The race tests hold a row-writing transaction open (the acquire, the
    release, the upsert) and start the sweep as a concurrent task: the
    sweep's DELETE takes its statement snapshot under READ COMMITTED
    (the uncommitted writer is invisible, so the rows still look
    stale/free), then blocks on the writer's row lock. Only once that
    wait is OBSERVED may the writer commit - that ordering is the race
    under test: the refresh lands strictly between the candidate window
    and the DELETE's row lock, which is exactly the interleaving
    EvalPlanQual exists for.
    """
    async with pool.acquire() as conn:
        for _ in range(250):
            waiting = await conn.fetchval(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE state = 'active' AND wait_event_type = 'Lock' "
                "AND datname = current_database()"
            )
            if waiting:
                return
            await asyncio.sleep(0.02)
    raise AssertionError(
        "no session ever blocked on a row lock: the sweep never reached its "
        "DELETE against the held transaction (fixture failure, not a finding)"
    )


# ── Attack 1: the sweep-vs-live-acquire race (hypothesis 3) ────────────


@pytest.mark.integration
async def test_acquire_landing_between_window_and_delete_partially_deletes_a_live_bucket(
    pg_dsn: str,
) -> None:
    """A keyed acquire that commits between the sweep's candidate window
    and its DELETE must keep the bucket WHOLE.

    The sweep's own contract (``_SWEEP_IDLE_KEYED_SLOTS_SQL``): "a bucket
    must be reclaimed WHOLE or not at all - a partial delete would
    silently shrink the bucket's configured capacity, and the
    acquire-path heal only fires at ZERO rows (a partially-deleted
    bucket denies with a smaller slot count forever, no code path ever
    names it again)". The drive: the real acquire statement runs inside
    a held-open transaction (row locked, uncommitted) while the sweep
    snapshots the bucket as all-free/all-stale and enters its DELETE;
    the acquire then commits between the window and the row lock.
    """
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=4)
    try:
        bucket = "race-acquire:s1"
        res = ConcurrencyReservation(
            name=bucket,
            slots=4,
            lease=timedelta(seconds=30),
            schema=_schema(),
            keyed=True,
        )
        await res.ensure_slots(pool)
        await _age_slots(pool, bucket, _HORIZON * 2)

        job_id, worker_id = new_uuid(), new_uuid()
        acquire_sql = _ACQUIRE_SQL_TEMPLATE.format(schema=_schema())
        conn_a = await asyncpg.connect(pg_dsn)
        try:
            async with conn_a.transaction():
                # The real acquire statement, held open: slot 0 is taken
                # and locked, uncommitted - invisible to the sweep's
                # snapshot, blocking to its DELETE.
                row = await conn_a.fetchrow(acquire_sql, bucket, job_id, worker_id, 30.0)
                assert row is not None and row["slot_index"] == 0, (
                    "fixture broken: the held acquire did not take slot 0"
                )
                sweep_task = asyncio.create_task(_sweep(pg_dsn))
                await _await_lock_waiter(pool)
                # Exiting the transaction block commits the acquire -
                # the refresh lands between the sweep's window and its
                # row lock.
            deleted: int = await sweep_task
        finally:
            await conn_a.close()

        rows = await _slot_rows(pool, bucket)
        assert rows == 4, (
            f"the fleet sweep partially deleted a LIVE keyed bucket: an acquire committed "
            f"between the sweep's candidate window and its DELETE, the per-row guard "
            f"(job_id/lease only - no last_used_at re-check, unlike the buckets arm) "
            f"spared the acquired row but its DELETE removed the {4 - rows} free "
            f"siblings ({deleted} deleted, {rows} of 4 rows remain). The sweep's own "
            "comment calls this 'the one accepted partial ... its acquire stamped it "
            "fresh, so the bucket is live again and re-enters eligibility a horizon "
            "later' - but a bucket in continuous use never goes idle past the horizon, "
            "so it never re-enters eligibility, never returns to zero rows (the heal "
            "fires only at zero), and permanently runs at reduced capacity: the "
            "whole-bucket contract stated ten lines above the guard is violated by "
            "the guard itself"
        )

        # The green path's aftermath: the acquired slot releases and the
        # bucket still admits its full configured concurrency.
        await res.release(0, worker_id, pool)
        leases = [await res.acquire(new_uuid(), worker_id, pool) for _ in range(4)]
        assert len(leases) == 4, (
            "fixture aftermath broken: 4 free rows must admit 4 concurrent leases"
        )
        for lease in leases:
            await res.release(int(lease), worker_id, pool)
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


# ── Attack 2: the sweep-vs-late-release race (hypothesis 3) ────────────


@pytest.mark.integration
async def test_release_landing_between_window_and_delete_loses_the_freshly_released_row(
    pg_dsn: str,
) -> None:
    """A keyed release that commits between the sweep's candidate window
    and its DELETE must keep its row: the release's own stamp is the
    mid-workflow signal.

    ``_RELEASE_SQL_TEMPLATE``'s comment: "the row the release frees is
    the row whose staleness must reset (a just-released bucket is
    mid-workflow, not idle)". The buckets arm's DELETE re-checks
    ``keyed AND last_used_at < horizon`` under its row lock; the slots
    arm's DELETE guard re-checks only ``job_id``/lease. The drive: a
    slot held by an EXPIRED lease (free-or-expired passes the window),
    released by a held-open real release statement while the sweep
    enters its DELETE; the release commits between window and row lock.
    """
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=4)
    try:
        bucket = "race-release:s1"
        res = ConcurrencyReservation(
            name=bucket,
            slots=2,
            lease=timedelta(seconds=30),
            schema=_schema(),
            keyed=True,
        )
        await res.ensure_slots(pool)
        worker_id = new_uuid()
        # Slot 0: held by a worker whose lease expired two hours ago (the
        # shape sweep 4 has not yet cleared) - eligible through the
        # free-or-expired arm of every window in the sweep.
        async with pool.acquire() as conn:
            await conn.execute(
                f'UPDATE "{_schema()}".reservation_slots '
                f"SET job_id = $1, held_by_worker_id = $2, "
                f"acquired_at = clock_timestamp() - $3::interval, "
                f"lease_expires_at = clock_timestamp() - $3::interval "
                f"WHERE bucket_name = $4 AND slot_index = 0",
                new_uuid(),
                worker_id,
                timedelta(hours=2),
                bucket,
            )
        await _age_slots(pool, bucket, _HORIZON * 2)

        conn_a = await asyncpg.connect(pg_dsn)
        try:
            async with conn_a.transaction():
                # The real release statement, held open: it frees slot 0
                # and stamps it fresh, uncommitted.
                await conn_a.execute(
                    _RELEASE_SQL_TEMPLATE.format(schema=_schema()), bucket, 0, worker_id
                )
                sweep_task = asyncio.create_task(_sweep(pg_dsn))
                await _await_lock_waiter(pool)
                # Commit: the freed, freshly-stamped row is now visible -
                # the sweep's DELETE re-evaluates it under its row lock.
            await sweep_task
        finally:
            await conn_a.close()

        rows = await _slot_rows(pool, bucket)
        assert rows == 2, (
            f"the fleet sweep deleted the row a release had just freed and stamped "
            f"({rows} of 2 rows remain): the release landed between the sweep's "
            "candidate window and its DELETE, so EvalPlanQual re-checked the row "
            "against the slots arm's guard - job_id/lease only, never the "
            "last_used_at the release had just refreshed - and the freshly-stamped, "
            "mid-workflow row died with its stale free sibling. The buckets arm's "
            "own DELETE re-checks the stamp ('AND b.keyed AND b.last_used_at < ...'); "
            "the slots arm's asymmetry with it is the hole this attack drives"
        )
        _ = res  # the reservation is fixture context; the rows are the finding
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


# ── Attack 3: re-materialisation does not restamp (hypothesis 1) ───────


@pytest.mark.integration
async def test_rematerialisation_over_surviving_rows_leaves_the_bucket_fully_reclaimable(
    pg_dsn: str,
) -> None:
    """A keyed bucket re-materialised over rows that SURVIVED (the
    registry's re-resolve path) must not be fleet-reclaimable before its
    first acquire.

    ``ConcurrencyReservation.ensure_slots``'s contract: "Inserts the
    bucket's full slot row set with this reservation's fleet-reclaimable
    mark and a fresh ``last_used_at``". The INSERT arm delivers that;
    the conflict arm (``ON CONFLICT ... DO UPDATE SET keyed =
    EXCLUDED.keyed``) refreshes ONLY the mark. Re-materialisation over
    surviving rows is reachable: an idle-evicted entry whose rows
    outlived the pending-reclaim drain (held by another worker's live
    leases) is re-resolved by the next job for that key, and the
    ensure's conflict arm runs over the survivors' pre-eviction stamps.
    """
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=4)
    try:
        bucket = "remat:s1"
        first = ConcurrencyReservation(
            name=bucket,
            slots=2,
            lease=timedelta(seconds=30),
            schema=_schema(),
            keyed=True,
        )
        await first.ensure_slots(pool)
        # The rows outlive their registry entry: held through the
        # pending-reclaim drain, idle past the horizon.
        await _age_slots(pool, bucket, _HORIZON * 2)

        # Re-materialisation - exactly what _resolve_reservation_name's
        # fresh-registration branch runs for the same concrete name.
        remat = ConcurrencyReservation(
            name=bucket,
            slots=2,
            lease=timedelta(seconds=30),
            schema=_schema(),
            keyed=True,
        )
        await remat.ensure_slots(pool)

        fresh = await _slots_stamp_is_fresh(pool, bucket)
        assert fresh is True, (
            "re-materialisation left the bucket's pre-eviction staleness in place: "
            "ensure_slots' contract promises 'a fresh last_used_at', but the ON "
            "CONFLICT arm (DO UPDATE SET keyed = EXCLUDED.keyed) refreshes only the "
            "keyed mark - a bucket that just came back into a live registry still "
            "reads as idle past the horizon, so the leader tick between the "
            "re-materialisation's ensure and the acquire's first stamp deletes the "
            "whole bucket out from under the acquiring worker (one denial plus the "
            "heal round trip on a live path)"
        )

        deleted = await _sweep(pg_dsn)
        rows = await _slot_rows(pool, bucket)
        assert rows == 2, (
            f"a just-re-materialised keyed bucket was fully reclaimed before its "
            f"first acquire ({deleted} rows deleted, {rows} remain): the bucket is "
            "in a live registry, mid-resolution, and the fleet sweep deleted it on "
            "staleness that predates its re-materialisation"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


# ── Attack 4: the static-name flip (hypothesis 2) ──────────────────────


@pytest.mark.integration
async def test_keyed_materialisation_over_a_static_name_flips_its_rows_fleet_reclaimable(
    pg_dsn: str,
) -> None:
    """No path may flip a STATIC bucket's rows fleet-reclaimable.

    Migration 01.00.10_02's invariant: "a statically declared bucket's
    rows are born false and must NEVER be flipped true (a static
    reservation has no acquire-path heal, so deleted rows would deny
    forever)". The registry's concrete-name collision guard sees only
    ITS OWN process's entries - two workers with different actor sets
    (different registries) can legally hold a static declaration and a
    keyed ref that resolve to the SAME concrete name, and the keyed
    materialisation's ensure_slots conflict arm re-marks the static
    rows fleet-reclaimable.
    """
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=4)
    try:
        name = "collide:s1"
        # Worker B: the static declaration - the bootstrap's startup
        # ensure, rows born keyed=false.
        static = ConcurrencyReservation(
            name=name,
            slots=2,
            lease=timedelta(seconds=30),
            schema=_schema(),
        )
        await static.ensure_slots(pool)
        assert await _slots_keyed(pool, name) is False, (
            "fixture broken: a statically declared reservation's rows must be born "
            "without the fleet-reclaimable mark"
        )

        # Worker A: a DIFFERENT process's registry (its actor set does
        # not declare the static name - the in-process collision guard
        # cannot see worker B's entry) resolves the keyed ref whose
        # concrete name IS the static name.
        reg_a = RateLimitRegistry()
        handles = await reg_a.acquire_for_actor(
            rate_limits=[],
            reservations=[_res_ref("collide", slots=2)],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s1"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        assert len(handles) == 1

        marked = await _slots_keyed(pool, name)
        assert marked is False, (
            "a keyed materialisation flipped a STATIC declaration's rows "
            "fleet-reclaimable: the concrete name 'collide:s1' equals the static "
            "name, worker A's registry cannot see worker B's static entry (the "
            "collision guard is process-local), and ensure_slots' conflict arm "
            "(DO UPDATE SET keyed = EXCLUDED.keyed) re-marked the rows true - "
            "migration 01.00.10_02's own invariant says static rows 'must NEVER be "
            "flipped true (a static reservation has no acquire-path heal, so "
            "deleted rows would deny forever)'"
        )

        # The consequence: once the keyed user goes quiet, the fleet
        # sweep reclaims the STATIC reservation's rows, and worker B's
        # limiter denies forever.
        await reg_a.release_for_actor(handles)
        await _age_slots(pool, name, _HORIZON * 2)
        await _sweep(pg_dsn)
        rows = await _slot_rows(pool, name)
        assert rows == 2, (
            f"the fleet sweep deleted a STATIC reservation's rows ({rows} of 2 "
            "remain) after a cross-registry keyed materialisation marked them: the "
            "static limiter has no keyed lifecycle, no heal, and no "
            "re-materialisation - every acquisition on worker B now denies forever"
        )
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


# ── Pin 1: the refund stamp (hypothesis 1) ─────────────────────────────


@pytest.mark.integration
async def test_refund_refreshes_the_keyed_bucket_row_stamp(pg_dsn: str) -> None:
    """The token bucket's PG refund is a release-path USE of the row -
    its UPDATE refreshes ``last_used_at`` (token_bucket.py: "a bucket
    whose token was just refunded is mid-workflow"), so a refunded
    bucket must not be swept even when its stamp had aged past the
    horizon."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=4)
    try:
        reg = RateLimitRegistry()
        handles = await reg.acquire_for_actor(
            rate_limits=[_pg_ref("refund-rl")],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="acme"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        bucket = "refund-rl:acme"
        assert len(handles) == 1
        handle = handles[0]
        assert isinstance(handle, RateLimitHandle)
        assert handle.decision.backend == "postgres" and handle.decision.allowed

        await _age_bucket_row(pool, bucket, _HORIZON * 2)
        prim = reg.get_rate_limit(bucket)
        assert isinstance(prim, TokenBucket)
        await prim.refund(handle.decision, pg_pool=pool, settings=_settings(pg_dsn))

        fresh = await _bucket_stamp_is_fresh(pool, bucket)
        assert fresh is True, (
            "the PG refund did not refresh last_used_at: the refund UPDATE is a "
            "release-path use of the row, and an unstamped refund lets the fleet "
            "sweep catch the row idle past the horizon between the acquiring "
            "worker's last acquire and its next one"
        )

        deleted = await _sweep(pg_dsn)
        assert deleted == 0, f"the sweep deleted {deleted} rows of a just-refunded bucket"
        assert await _bucket_rows(pool, bucket) == 1
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


# ── Pin 2: the buckets arm re-checks the stamp (hypothesis 3) ──────────


@pytest.mark.integration
async def test_buckets_arm_delete_rechecks_the_stamp_under_a_late_acquire(pg_dsn: str) -> None:
    """The buckets arm's DELETE re-checks ``keyed AND last_used_at <
    horizon`` under its row lock - an acquire (the real upsert from
    ``token_bucket._acquire_pg``) committing between the sweep's window
    and the DELETE spares the row. The mirror of the slots arm's race:
    there the guard checks only job_id/lease; here the stamp itself is
    the guard."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=4)
    try:
        reg = RateLimitRegistry()
        handles = await reg.acquire_for_actor(
            rate_limits=[_pg_ref("race-bucket")],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="acme"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        bucket = "race-bucket:acme"
        assert len(handles) == 1
        await _age_bucket_row(pool, bucket, _HORIZON * 2)

        # The real acquire-path upsert (token_bucket._acquire_pg's
        # upsert_sql, verbatim), held open inside a transaction: it
        # locks the row and stamps it fresh, uncommitted. The state is the
        # refilling bucket's full document (this fixture's ref is
        # capacity=5, refill_per_second=0.5): provably safe to delete, so
        # only the fresh stamp can spare the row - anything the quota veto
        # keeps would make this pin blind to a stamp re-check regression.
        upsert_sql = (
            f'INSERT INTO "{_schema()}".rate_limit_buckets '
            f"(bucket_name, kind, state, updated_at, keyed, last_used_at) "
            f"VALUES ($1, 'token_bucket', $2::jsonb, clock_timestamp(), $3, clock_timestamp()) "
            f"ON CONFLICT (bucket_name) DO UPDATE SET state=EXCLUDED.state, "
            f"updated_at=clock_timestamp(), last_used_at=clock_timestamp(), "
            f"keyed=EXCLUDED.keyed"
        )
        conn_a = await asyncpg.connect(pg_dsn)
        try:
            async with conn_a.transaction():
                await conn_a.execute(
                    upsert_sql,
                    bucket,
                    '{"tokens": 4.0, "ts": 0, "capacity": 5.0, "refill": 0.5}',
                    True,
                )
                sweep_task = asyncio.create_task(_sweep(pg_dsn))
                await _await_lock_waiter(pool)
                # Commit: the acquire's fresh stamp is visible - the
                # DELETE re-evaluates the row under its row lock.
            deleted: int = await sweep_task
        finally:
            await conn_a.close()

        assert deleted == 0, (
            f"the buckets arm's DELETE removed a row whose stamp an acquire had "
            f"just refreshed ({deleted} deleted): the arm's own guard re-checks "
            "'b.keyed AND b.last_used_at < horizon' under EvalPlanQual, and a "
            "fresh stamp must spare the row"
        )
        assert await _bucket_rows(pool, bucket) == 1
        assert await _bucket_stamp_is_fresh(pool, bucket) is True
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


# ── Pin 3: the mark's rebirth through the heal (hypothesis 2) ──────────


@pytest.mark.integration
async def test_swept_keyed_bucket_rematerialises_marked_and_fresh_on_the_next_acquire(
    pg_dsn: str,
) -> None:
    """After a fleet sweep deletes a tracked keyed bucket's rows, the
    next acquire's denial-path heal re-materialises them - and the
    re-born rows carry the mark and fresh stamps (the
    self-heal's interplay with the new fleet sweep: deletion is a
    transient denial, not a wedge)."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=4)
    try:
        reg = RateLimitRegistry()
        handles = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[_res_ref("heal-res", slots=2)],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s1"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        bucket = "heal-res:s1"
        assert len(handles) == 1
        await reg.release_for_actor(handles)
        await _age_slots(pool, bucket, _HORIZON * 2)

        deleted = await _sweep(pg_dsn)
        assert deleted == 2 and await _slot_rows(pool, bucket) == 0, (
            "fixture broken: the stale tracked bucket was not fully swept"
        )

        # The tracked entry survives the sweep; the acquire denies on
        # zero rows, the heal probes, re-materialises, and the retry
        # succeeds - the re-born rows marked and freshly stamped.
        again = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[_res_ref("heal-res", slots=2)],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s1"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        assert len(again) == 1, (
            "the post-sweep acquire did not heal: a tracked keyed bucket whose rows "
            "the fleet sweep deleted must re-materialise on the denial path and "
            "admit the retry"
        )
        assert await _slot_rows(pool, bucket) == 2
        assert await _slots_keyed(pool, bucket) is True, (
            "the heal's re-materialised rows lost the fleet-reclaimable mark: rows "
            "re-born through ensure_slots' INSERT arm must carry it or the bucket "
            "reverts to the orphan-row shape if this worker later dies"
        )
        assert await _slots_stamp_is_fresh(pool, bucket) is True, (
            "the heal's re-materialised rows were not freshly stamped: the horizon "
            "must restart at re-materialisation, or the next sweep tick deletes the "
            "just-healed bucket again (a denial/heal loop)"
        )
        await reg.release_for_actor(again)
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


# ── Pin 4: the redis carve-out through the outage fallback (hypothesis 6) ──


@pytest.mark.integration
async def test_redis_backend_keyed_row_stays_unmarked_through_the_outage_fallback(
    pg_dsn: str,
) -> None:
    """A redis-backend keyed bucket's PG row must stay ``keyed=false``
    through the ONE path that writes it - the Redis-outage fallback.

    The row is outage-fallback state plus admin metadata; its stamp
    cannot speak for Redis-side use, so the fleet sweep must never
    delete it (sweeping would "reset the outage-fallback state of a
    fixed-quota bucket mid-outage" - the migration's words). The drive:
    a redis-backend keyed ref resolved with a dead Redis (ConnectionError
    → PG fallback), so the fallback's preseed/upsert consumes real PG
    tokens; the row then ages past the horizon and the sweep runs."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=4)
    dead_redis = redis_async.Redis(
        host="127.0.0.1", port=1, socket_connect_timeout=0.25, socket_timeout=0.25
    )
    try:
        reg = RateLimitRegistry()
        handles = await reg.acquire_for_actor(
            rate_limits=[_redis_ref("outage-rl")],
            reservations=[],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_TenantPayload(tenant_id="zeta"),
            redis_client=dead_redis,
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        bucket = "outage-rl:zeta"
        assert len(handles) == 1
        handle = handles[0]
        assert isinstance(handle, RateLimitHandle)
        assert handle.decision.backend == "postgres", (
            "fixture broken: the acquire did not fall back to PG against the dead Redis client"
        )
        assert handle.decision.allowed

        # The fallback paid in PG: the row exists, unmarked, holding the
        # consumed-token state an outage rides on.
        assert await _bucket_rows(pool, bucket) == 1
        assert await _bucket_keyed(pool, bucket) is False, (
            "the outage fallback's upsert marked a redis-backend keyed bucket's PG "
            "row fleet-reclaimable: only backend='postgres' keyed buckets may carry "
            "the mark - this row's staleness can never speak for Redis-side use"
        )
        assert await _bucket_state_tokens(pool, bucket) == 4.0, (
            "fixture broken: the fallback acquire did not consume a PG token "
            "(capacity 5, one acquire)"
        )

        await _age_bucket_row(pool, bucket, _HORIZON * 2)
        deleted = await _sweep(pg_dsn)
        assert deleted == 0, f"the sweep deleted {deleted} rows of a redis-backend keyed bucket"
        assert await _bucket_rows(pool, bucket) == 1, (
            "a redis-backend keyed bucket's PG row was swept: the row is "
            "outage-fallback state plus admin metadata - deleting it mid-outage "
            "resets the fallback state of a fixed-quota bucket (a fresh full-capacity "
            "row on the next fallback acquire silently re-admits over quota)"
        )
        assert await _bucket_keyed(pool, bucket) is False
        assert await _bucket_state_tokens(pool, bucket) == 4.0, (
            "the sweep or its aftermath reset the outage-fallback token state"
        )
    finally:
        await dead_redis.aclose()
        await pool.close()
        await _drop_schema(pg_dsn)


# ── Pin 5: the previous release's acquire against the new sweep (hypothesis 5) ──


@pytest.mark.integration
async def test_old_generation_acquire_with_a_live_lease_vetoes_the_sweep(pg_dsn: str) -> None:
    """Rolling-deploy skew, old-against-new: the previous release's
    acquire does not stamp ``last_used_at`` (the stamp is new in
    e5d2154), so a row it holds looks stale forever - the horizon/mark
    shape must still protect it. The live-lease veto is that protection:
    a bucket with one live-held slot is not reclaimable whatever its
    stamps say. (The free-between-acquires window under old-code churn
    IS swept - the designed transient; the held-row case must not be.)"""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=4)
    try:
        bucket = "skew-old:s1"
        res = ConcurrencyReservation(
            name=bucket,
            slots=2,
            lease=timedelta(seconds=30),
            schema=_schema(),
            keyed=True,
        )
        await res.ensure_slots(pool)
        await _age_slots(pool, bucket, _HORIZON * 2)

        # The previous release's acquire arm - the current
        # _ACQUIRE_SQL_TEMPLATE verbatim MINUS the last_used_at stamp
        # (the one line e5d2154 added to it).
        old_acquire_sql = f"""\
WITH free_slot AS (
    SELECT slot_index FROM "{_schema()}".reservation_slots
    WHERE bucket_name = $1
      AND (job_id IS NULL OR lease_expires_at < clock_timestamp())
    ORDER BY slot_index
    LIMIT 1
    FOR UPDATE SKIP LOCKED
),
acquired AS (
    UPDATE "{_schema()}".reservation_slots
    SET job_id            = $2,
        held_by_worker_id = $3,
        acquired_at       = clock_timestamp(),
        lease_expires_at  = clock_timestamp() + $4 * INTERVAL '1 second'
    WHERE (bucket_name, slot_index) IN (SELECT $1, slot_index FROM free_slot)
    RETURNING slot_index
)
SELECT a.slot_index FROM acquired a"""
        worker_id = new_uuid()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(old_acquire_sql, bucket, new_uuid(), worker_id, 30.0)
        assert row is not None and row["slot_index"] == 0, (
            "fixture broken: the old-generation acquire did not take slot 0"
        )

        deleted = await _sweep(pg_dsn)
        assert deleted == 0, (
            f"the sweep deleted {deleted} rows of a bucket whose slot an "
            "old-generation (unstamping) acquire holds under a live lease"
        )
        assert await _slot_rows(pool, bucket) == 2, (
            "a live-held bucket lost rows to the fleet sweep under rolling-deploy "
            "skew: the old release's acquire never stamps last_used_at, so the "
            "lease veto - not the stamp - is what must keep the held bucket whole"
        )
        async with pool.acquire() as conn:
            still_stale = await conn.fetchval(
                f"SELECT now() - max(last_used_at) > interval '1 hour' "
                f'FROM "{_schema()}".reservation_slots WHERE bucket_name = $1',
                bucket,
            )
        assert still_stale is True, (
            "fixture broken: the stamps were refreshed somewhere - the point of "
            "this pin is that the LEASE, not a fresh stamp, protected the bucket"
        )
        _ = res
    finally:
        await pool.close()
        await _drop_schema(pg_dsn)


# ── Pin 6: the pre-migration failure mode (hypothesis 5) ───────────────


@pytest.mark.integration
async def test_pre_migration_schema_raises_undefined_column_at_the_sweep(pg_dsn: str) -> None:
    """A schema without the keyed/last_used_at columns makes the sweep
    raise ``UndefinedColumnError`` - the exact failure mode the leader
    block's pre-migration tolerance is built around (the wiring pins it
    per tick below). Pins that the failure mode IS the named exception,
    not something else escaping the except tuple."""
    await _fresh_schema(pg_dsn)
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(
            f'ALTER TABLE "{_schema()}".reservation_slots '
            f"DROP COLUMN IF EXISTS keyed, DROP COLUMN IF EXISTS last_used_at"
        )
        await conn.execute(
            f'ALTER TABLE "{_schema()}".rate_limit_buckets '
            f"DROP COLUMN IF EXISTS keyed, DROP COLUMN IF EXISTS last_used_at"
        )
        with pytest.raises(asyncpg.exceptions.UndefinedColumnError):
            await sweep_idle_keyed_rows(conn, schema=_schema(), horizon=_HORIZON, batch_size=_BATCH)
    finally:
        await conn.close()
        await _drop_schema(pg_dsn)


# ── Pin 7: the leader loop's per-tick pre-migration tolerance (hypothesis 5) ──


class _FakeConn:
    async def execute(self, sql: str, *args: object) -> str:
        return "DELETE 0"

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        return []

    async def fetchval(self, sql: str, *args: object) -> object:
        return None

    async def fetchrow(self, sql: str, *args: object) -> object | None:
        return None

    async def close(self) -> None:
        pass

    def is_closed(self) -> bool:
        return False


class _FakePool:
    @asynccontextmanager
    async def acquire(self, *, timeout: float | None = None) -> AsyncGenerator[_FakeConn]:  # noqa: ASYNC109  # Why: mirrors asyncpg.Pool.acquire signature.
        yield _FakeConn()


class _PreMigrationBackend:
    """Backend double exposing the PG-only sweep surface the leader
    section gates on, whose keyed-row reclaim always raises the
    pre-migration ``UndefinedColumnError`` - the rolling-deploy state
    between code rollout and migration 01.00.10_02 landing. Sibling
    sweeps return 0 so no drain runs and the hasattr gate is entered
    (same shape as the author's wiring double)."""

    def __init__(self) -> None:
        self.keyed_reclaim_calls: list[dict[str, str]] = []

    async def reclaim_expired_locks(self, cancel_grace: timedelta, cleanup_grace: timedelta) -> int:
        return 0

    async def deadline_sweep(self) -> int:
        return 0

    async def sweep_leaked_reservation_slots(
        self, conn: object, *, schema: str, batch_size: int
    ) -> int:
        return 0

    async def sweep_expired_results(self, conn: object, *, schema: str, batch_size: int) -> int:
        return 0

    async def sweep_expired_events(
        self,
        conn: object,
        *,
        schema: str,
        retention: timedelta,
        batch_size: int,
    ) -> int:
        return 0

    async def sweep_idle_keyed_rows(
        self,
        conn: object,
        *,
        schema: str,
        horizon: timedelta,
        batch_size: int,
    ) -> int:
        self.keyed_reclaim_calls.append({"schema": schema, "horizon": repr(horizon)})
        raise asyncpg.exceptions.UndefinedColumnError(
            'column "keyed" of relation "reservation_slots" does not exist'
        )


def _wiring_deps() -> WorkerDeps:
    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": "postgresql://x:x@localhost/x",
            "TASKQ_SWEEP_INTERVAL": "0.05",
            "TASKQ_KEYED_ROW_RECLAIM_PERIOD": "2h",
        },
        validate=False,
    )
    deps = WorkerDeps(
        settings=settings,
        dispatcher_pool=_FakePool(),  # type: ignore[arg-type]  # Why: test double for asyncpg.Pool
        heartbeat_pool=_FakePool(),  # type: ignore[arg-type]  # Why: same.
        worker_pool=_FakePool(),  # type: ignore[arg-type]  # Why: same.
        notify_conn=None,
        leader_conn=_FakeConn(),  # type: ignore[arg-type]  # Why: test double for asyncpg.Connection
    )
    # The keyed-reclaim block lives in the leader-gated section of the tick.
    deps.is_leader.set()
    return deps


def _wiring_ctx(deps: WorkerDeps, backend: _PreMigrationBackend) -> SweepContext:
    clock: Clock = FakeClock(datetime(2025, 1, 1, tzinfo=UTC))
    return SweepContext(
        deps=deps,
        backend=backend,  # type: ignore[arg-type]  # Why: test double for the Backend protocol
        clock=clock,
        worker_id=new_uuid(),
        rate_limit_registry=None,
    )


async def _run_wiring_loop_until(ctx: SweepContext, done: Callable[[], bool]) -> None:
    import taskq.worker._leader_sweeps as sweeps_mod

    shutdown = asyncio.Event()
    task = asyncio.create_task(sweeps_mod._sweep_loop(ctx, shutdown))  # pyright: ignore[reportPrivateUsage]  # Why: driving the sweep loop directly, matching the author's wiring-test pattern.
    try:
        for _ in range(200):
            if done():
                break
            await asyncio.sleep(0.01)
    finally:
        shutdown.set()
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def test_leader_loop_tolerates_pre_migration_undefined_column_per_tick() -> None:
    """The leader sweep block tolerates ``UndefinedColumnError`` PER TICK
    (the pre-migration rolling-deploy pattern the stale-batches block
    set): a new-code leader against a schema whose keyed/last_used_at
    columns have not landed warns and keeps ticking - the loop neither
    dies (leaving the fleet with no sweeper at all) nor stops calling
    (the tolerance must be a per-tick warn until the migration lands,
    not a one-shot swallow)."""
    deps = _wiring_deps()
    backend = _PreMigrationBackend()
    ctx = _wiring_ctx(deps, backend)

    await _run_wiring_loop_until(ctx, lambda: len(backend.keyed_reclaim_calls) >= 3)

    assert len(backend.keyed_reclaim_calls) >= 3, (
        f"the leader loop did not tolerate the pre-migration UndefinedColumnError "
        f"per tick ({len(backend.keyed_reclaim_calls)} calls recorded): the block's "
        "except tuple is built around this exact exception so a rolling deploy "
        "warns each tick until migration 01.00.10_02 lands - a loop that tore down "
        "would leave the fleet with no sweeper, and one that stopped calling would "
        "silently drop the reclaim feature the moment the migration landed"
    )
