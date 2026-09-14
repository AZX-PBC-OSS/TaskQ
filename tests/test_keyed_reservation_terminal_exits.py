# ruff: noqa: S608  # Why: schema is a fixed test identifier, not user input; every value is $-bound.

"""Terminal-exit pins for keyed ``reservation_slots`` rows: every flow that
creates rows for a ``base_name:key`` bucket must leave those rows with a
deletion exit — the idle eviction's pending-reclaim drain.

The inventory these pins complete (each row kind against each terminal
path of the job that caused it):

* **success / failure / retry / snooze-in-actor / cancel (cooperative)** —
  the consumer's outer ``finally`` releases the held slot
  (lease-fenced), so the row is FREE and the eviction+drain reclaims it.
  Pinned by ``tests/test_keyed_reservation_slot_reclamation.py``.
* **pre-actor denial** (the #139 headline flow) — the acquire is denied
  after the keyed bucket's rows were already materialised; nothing is
  held, the job snoozes. The rows must still reclaim: pinned here.
* **abandon** — the holder's consumer dies without releasing; the lease
  lapses; the (surviving) sweeper's drain treats an expired lease as
  free. Pinned here.
* **worker death** — the registry that tracks the key is gone with the
  process, so no eviction ever records the rows: the one terminal exit
  with NO deletion path today (fleet-wide, only a row-carried staleness
  marker + fleet sweep can close it — the solid_queue
  ``Semaphore.expired.in_batches(&:delete_all)`` shape). NOT pinned
  red here; it is the documented residual gap.

The static-bucket control in the denial pin (a statically pre-registered
bucket's rows SURVIVE the eviction+drain) guards the invariant the
reclamation must never break: static capacity is config-bounded and its
rows are ensured at startup, never evicted.
"""

import asyncio
from datetime import timedelta

import asyncpg
import pytest
from pydantic import BaseModel

from taskq._ids import new_uuid
from taskq.exceptions import ReservationUnavailable
from taskq.migrate import apply_pending
from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.settings import WorkerSettings

pytestmark = pytest.mark.integration


class _SessionPayload(BaseModel):
    session_id: str


def _schema() -> str:
    """This file's dedicated schema (local, per the suite-hygiene rule
    against module-level schema constants)."""
    return "taskq_keyed_terminal_exits"


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
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f'SELECT count(*) FROM "{_schema()}".reservation_slots '
            "WHERE bucket_name = $1 AND job_id IS NOT NULL",
            bucket,
        )


async def test_denied_keyed_reservation_leaves_zero_rows_after_eviction_drain(
    pg_dsn: str,
) -> None:
    """A keyed bucket materialised by a DENIED acquire must reclaim.

    The #139 headline flow: an actor declares a keyed reservation plus a
    static cap; the static cap is full, so the acquire is denied after
    the keyed bucket's slot rows were already ensured — the job snoozes
    (``mark_snoozed`` with ``outcome="reservation_denied"``) having
    never run. Those denial-materialised rows are exactly the growth the
    eviction-drain exists to bound: once the key goes idle, the eviction
    must record the bucket and the drain must leave ZERO rows — and the
    static bucket's capacity rows must survive untouched."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        settings = _settings(pg_dsn)

        static = ConcurrencyReservation(
            name="denial-exit-static", slots=1, lease=timedelta(seconds=30), schema=_schema()
        )
        reg.register(static)
        await static.ensure_slots(pool)

        # The static cap's only slot is held, so the denied job below can
        # never acquire it.
        holder = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[static],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=None,
            pg_pool=pool,
            settings=settings,
        )
        assert len(holder) == 1

        keyed = KeyedReservationRef.typed(
            _SessionPayload,
            base_name="denial-exit",
            key_fn=lambda p: p.session_id,
            slots=2,
            lease=timedelta(seconds=30),
        )
        denied_bucket = "denial-exit:s-denied"

        # The denied attempt: the keyed ref resolves FIRST (materialising
        # its slot rows), then the full static cap denies — the rollback
        # releases the keyed slot and the job snoozes with no actor run.
        with pytest.raises(ReservationUnavailable):
            await reg.acquire_for_actor(
                rate_limits=[],
                reservations=[keyed, static],
                job_id=new_uuid(),
                worker_id=new_uuid(),
                payload=_SessionPayload(session_id="s-denied"),
                pg_pool=pool,
                settings=settings,
            )
        assert await _slot_rows(pool, denied_bucket) == 2, (
            "fixture broken: the denied attempt did not materialise the keyed bucket's rows"
        )

        # The holder's job terminates; its release frees the static slot.
        await reg.release_for_actor(holder)

        # The denied key goes idle; the worker's sweep evicts the entry
        # and the drain reclaims the rows the denial created.
        evicted = reg.evict_idle_keyed_reservations(idle_for=timedelta(0))
        assert evicted == 1, "fixture broken: the idle keyed bucket was not evicted"
        await reg.drain_pending_reservation_reclaims(pool)

        assert await _slot_rows(pool, denied_bucket) == 0, (
            f"the denial-materialised bucket {denied_bucket!r} left rows behind after "
            "its key went idle and the eviction+drain ran — the #139 denial flow "
            "would accrue one bucket of rows per distinct denied key with no "
            "deletion exit"
        )
        assert await _slot_rows(pool, "denial-exit-static") == 1, (
            "the eviction+drain must not touch a STATIC bucket's capacity rows — "
            "static cardinality is config-bounded, its rows are ensured at "
            "startup, and no worker re-ensures them mid-flight"
        )
    finally:
        await pool.close()


async def test_abandoned_keyed_reservation_leaves_zero_rows_after_lease_expiry(
    pg_dsn: str,
) -> None:
    """A keyed reservation abandoned by its holder must reclaim once the
    lease lapses.

    The abandon terminal exit: the holder's consumer dies without
    releasing (the job is reclaimed by the lock-expiry machinery, the
    slot row stays ``job_id``-held). The drain's idle guard deliberately
    treats an expired lease as deletable — a live lease survives (the
    over-admission invariant, pinned by
    ``test_keyed_reservation_drain_rotation``), a lapsed one does not —
    so the sweeper's next drain must leave ZERO rows for the bucket."""
    await _fresh_schema(pg_dsn)
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=2)
    try:
        reg = RateLimitRegistry()
        ref = KeyedReservationRef.typed(
            _SessionPayload,
            base_name="abandon-exit",
            key_fn=lambda p: p.session_id,
            slots=1,
            lease=timedelta(milliseconds=60),
        )
        bucket = "abandon-exit:s-abandoned"

        acquired = await reg.acquire_for_actor(
            rate_limits=[],
            reservations=[ref],
            job_id=new_uuid(),
            worker_id=new_uuid(),
            payload=_SessionPayload(session_id="s-abandoned"),
            pg_pool=pool,
            settings=_settings(pg_dsn),
        )
        assert len(acquired) == 1
        assert await _held_rows(pool, bucket) == 1, (
            "fixture broken: the acquire did not hold the slot"
        )

        # Abandon: no release — the consumer is gone. The lease (60 ms,
        # stamped server-side at acquire) lapses; the acquire CTE would
        # already hand this slot to a new job.
        await asyncio.sleep(0.4)

        evicted = reg.evict_idle_keyed_reservations(idle_for=timedelta(0))
        assert evicted == 1, "fixture broken: the idle keyed bucket was not evicted"
        await reg.drain_pending_reservation_reclaims(pool)

        assert await _slot_rows(pool, bucket) == 0, (
            f"the abandoned bucket {bucket!r} left rows behind after its lease "
            "lapsed and the eviction+drain ran — the drain's idle guard treats "
            "an expired lease as deletable precisely so a dead holder cannot "
            "pin its bucket's rows forever"
        )
    finally:
        await pool.close()
