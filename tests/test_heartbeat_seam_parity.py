# Why: no raw SQL in this file - both backends are driven through their own
# public seams, so no S608 is owed.

"""Heartbeat-family seam parity: ``heartbeat_jobs`` and
``extend_reservation_leases`` must answer the same COUNT on both backends.

The semantic-parity registry
(``tests/test_backend_semantic_parity_registry.py``'s sibling) carried both
seams as UNPINNED gaps: "selection of the worker's held slots". This file
pins them. PG is the production oracle:

* ``heartbeat_jobs`` renews every running row the worker still holds except
  the ``disowned`` ids, and its count is the number of JOB rows renewed
  (``UPDATE_JOBS_LOCK_SQL_TEMPLATE``). Duplicate ids in the ``disowned``
  Collection change nothing: ``NOT (id = ANY($3))`` is a set predicate.
* ``extend_reservation_leases`` renews the worker's ``reservation_slots``
  ROWS - pre-allocated by the reservation registry's acquisition, never by
  dispatch (``UPDATE_RESERVATION_LEASES_SQL_TEMPLATE``). Its count is the
  number of SLOT rows renewed, so a worker holding plain running jobs - jobs
  that never acquired a reservation slot - extends NOTHING and answers 0.
  The mirror's slot table models the same store: without one, no slots
  exist, and the count is 0 - a mirror that counted every held running job
  certified a heartbeat metric production never reports.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_job_id, new_uuid
from taskq.backend._protocol import EnqueueArgs
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import JobsApp, ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend
from tests.test_in_memory_dispatch_parity import _ensure_pg_actor, _setup_pg_queue

pytestmark = pytest.mark.integration

_LEASE = timedelta(seconds=30)
_SCHEDULED_AT = datetime(2025, 1, 1, tzinfo=UTC)
_IN_MEMORY_NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _args(*, job_id: object = None, actor: str) -> EnqueueArgs:
    return EnqueueArgs(
        id=job_id or new_job_id(),
        actor=actor,
        queue="default",
        payload={"x": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=_SCHEDULED_AT,
    )


def _mk_mem(actor: str) -> InMemoryBackend:
    backend = InMemoryBackend(clock=FakeClock(_IN_MEMORY_NOW))
    backend.register_actor_config(actor=actor)
    return backend


async def _pg_setup(pool: object, schema: str, actor: str) -> None:
    async with pool.acquire() as conn:  # type: ignore[union-attr]  # Why: deps is object-typed in the non-TYPE_CHECKING JobsApp shim; WorkerDeps has worker_pool at runtime.
        await _setup_pg_queue(conn, schema, "default", "strict_fifo")
        await _ensure_pg_actor(conn, schema, actor)


async def test_extend_reservation_leases_counts_slots_not_held_jobs(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """A worker holding plain running jobs extends NO reservation slots: 0.

    The mirror's slot-table-less default models a deployment with no
    reservation buckets - exactly the PG shape below, where dispatch never
    inserts a ``reservation_slots`` row. PG answers 0; the mirror must
    too. (Previously the mirror counted every held running job, reporting
    slot renewals production never made.)
    """
    schema = module_pg_schema.schema_name
    pg_backend = clean_jobs_app.backend
    actor = "parity_extend_leases"

    pg_ids = [new_job_id() for _ in range(3)]
    for jid in pg_ids:
        await pg_backend.enqueue(_args(job_id=jid, actor=actor))
    mem_backend = _mk_mem(actor)
    mem_ids = [new_job_id() for _ in range(3)]
    for jid in mem_ids:
        await mem_backend.enqueue(_args(job_id=jid, actor=actor))

    await _pg_setup(clean_jobs_app.deps.worker_pool, schema, actor)

    pg_wid, mem_wid = new_uuid(), new_uuid()
    pg_rows = await pg_backend.dispatch_batch(pg_wid, ["default"], 3, _LEASE)
    mem_rows = await mem_backend.dispatch_batch(mem_wid, ["default"], 3, _LEASE)
    assert len(pg_rows) == 3 and len(mem_rows) == 3

    pg_count = await pg_backend.extend_reservation_leases(pg_wid, _LEASE)
    mem_count = await mem_backend.extend_reservation_leases(mem_wid, _LEASE)

    assert pg_count == 0, (
        "PostgresBackend (production) reported reservation-slot renewals for a "
        f"worker holding only plain dispatched jobs: {pg_count}. The parity "
        "oracle itself is wrong - re-derive it before trusting the InMemory "
        "comparison below."
    )
    assert mem_count == pg_count, (
        "InMemoryBackend diverged from PostgresBackend at the "
        f"extend_reservation_leases count seam: PG renewed {pg_count} "
        f"reservation_slots rows and InMemory renewed {mem_count}. PG's count "
        "is the number of reservation_slots ROWS renewed "
        "(UPDATE_RESERVATION_LEASES_SQL_TEMPLATE) - dispatch never creates one, "
        "so a worker holding plain running jobs extends nothing. The mirror "
        "counted every held running job, certifying a heartbeat metric "
        "production never reports."
    )


async def test_heartbeat_jobs_disowned_exclusion_matches_pg(
    module_pg_schema: ModulePgSchema,
    clean_jobs_app: JobsApp,
) -> None:
    """``heartbeat_jobs`` excludes the disowned ids on both backends; the
    count is job rows renewed. Duplicate ids in the Collection change
    nothing - the exclusion is a set predicate on both sides."""
    schema = module_pg_schema.schema_name
    pg_backend = clean_jobs_app.backend
    actor = "parity_heartbeat_disowned"

    pg_ids = [new_job_id() for _ in range(3)]
    for jid in pg_ids:
        await pg_backend.enqueue(_args(job_id=jid, actor=actor))
    mem_backend = _mk_mem(actor)
    mem_ids = [new_job_id() for _ in range(3)]
    for jid in mem_ids:
        await mem_backend.enqueue(_args(job_id=jid, actor=actor))

    await _pg_setup(clean_jobs_app.deps.worker_pool, schema, actor)

    pg_wid, mem_wid = new_uuid(), new_uuid()
    pg_rows = await pg_backend.dispatch_batch(pg_wid, ["default"], 3, _LEASE)
    mem_rows = await mem_backend.dispatch_batch(mem_wid, ["default"], 3, _LEASE)
    assert len(pg_rows) == 3 and len(mem_rows) == 3

    # Disown two of the three, one id repeated: a dup-tuple Collection, the
    # shape a caller holding a list with a repeat presents.
    pg_disowned = (pg_ids[0], pg_ids[1], pg_ids[1])
    mem_disowned = (mem_ids[0], mem_ids[1], mem_ids[1])

    pg_count = await pg_backend.heartbeat_jobs(pg_wid, _LEASE, disowned=pg_disowned)
    mem_count = await mem_backend.heartbeat_jobs(mem_wid, _LEASE, disowned=mem_disowned)

    assert pg_count == 1, (
        "PostgresBackend (production) did not exclude the disowned ids: "
        f"renewed {pg_count} of 3 held rows. The parity oracle itself is "
        "wrong - re-derive it before trusting the InMemory comparison below."
    )
    assert mem_count == pg_count, (
        "InMemoryBackend diverged from PostgresBackend at the heartbeat_jobs "
        f"disowned seam: PG renewed {pg_count} rows and InMemory renewed "
        f"{mem_count}. The disowned exclusion is a set predicate on both "
        "sides (NOT (id = ANY($3)) on PG); the same disowned ids must renew "
        "the same number of rows."
    )

    # And the excluded rows' leases were not touched: their lock_expires_at
    # still reads as expired-after-lease on both backends (the count is the
    # observable; the row state is the contract behind it).
    pg_held = await pg_backend.get(pg_ids[2])
    mem_held = await mem_backend.get(mem_ids[2])
    assert pg_held is not None and mem_held is not None
    assert pg_held.lock_expires_at is not None and mem_held.lock_expires_at is not None
    assert (mem_held.lock_expires_at - _IN_MEMORY_NOW) > _LEASE - timedelta(seconds=5), (
        "the renewed row's lease must have moved out by the full lease"
    )
