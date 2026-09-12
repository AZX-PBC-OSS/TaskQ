"""Red-team attacks on ``max_pending`` backpressure overshoot (real PG + unit pin).

Hypothesis: ``max_pending`` is a check-then-insert count
(``src/taskq/backend/_enqueue.py``) with no serializing lock and no
slot-index backstop — unlike ``unique_for``, which takes a
transaction-scoped advisory lock, and unlike singleton, which has the
``jobs_singleton_uniq`` backstop with a typed ``UniqueViolation``
catch (upstream precedent: pgqueuer v1.4.0 fixed count-then-insert
overshoot with capacity-slot partial unique indexes).

Every batch tier below must refuse an oversized aggregate loudly
(``MaxPendingExceededError``, nothing admitted) rather than admit it
silently:

1. ``PostgresBackend.enqueue_batch`` admission-checks the carried
   per-item caps as one aggregate before writing.
2. ``enqueue_batch_streaming`` chunks, ``enqueue_batch_fast`` COPY,
   and ``SubJobEnqueuer.enqueue_batch(connection=...)`` all funnel
   through that tier and inherit the refusal.
3. Concurrent single enqueues serialize per capped actor on a
   transaction-scoped advisory lock, so overlapping counts cannot
   each see room.

The one clean path is pinned at unit tier: ``JobsClient.enqueue_batch``
runs an aggregated ``existing + batch_count > limit`` check before the
INSERT and rejects an oversized batch outright.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import asyncpg
import pytest
from pydantic import BaseModel

from taskq import actor
from taskq.actor import ActorRef
from taskq.backend._protocol import JobRow
from taskq.batch import EnqueueItem
from taskq.client._args import build_batch_args, build_enqueue_args
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.client._jobs import JobsClient
from taskq.exceptions import MaxPendingExceededError
from taskq.testing.clock import FakeClock
from taskq.testing.fixtures import ModulePgSchema
from taskq.testing.in_memory import InMemoryBackend

from .test_rt_cron_harness import count_jobs, cron_settings, make_backend, pool_backend

_START = datetime(2025, 1, 1, tzinfo=UTC)

_CAP = 2
_BATCH_N = 5


class _Payload(BaseModel):
    value: int = 1


@actor(name="rt_overshoot_capped", max_pending=_CAP)
async def _capped(payload: _Payload) -> None:
    pass


@actor(name="rt_overshoot_uncapped")
async def _uncapped(payload: _Payload) -> None:
    pass


def _items(ref: ActorRef[_Payload, None], n: int = _BATCH_N) -> list[EnqueueItem]:
    return [EnqueueItem(actor_ref=ref, payload=_Payload(value=i)) for i in range(n)]


def _batch_id() -> UUID:
    from taskq._ids import new_job_id

    return UUID(bytes=new_job_id().bytes)


# ── PG attack 1: backend batch tier ignores the resolved per-item cap ──


@pytest.mark.integration
async def test_backend_enqueue_batch_ignores_per_item_max_pending(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """``PostgresBackend.enqueue_batch`` admits a batch whose aggregate exceeds
    the cap even though every item carries the resolved ``max_pending``.

    The client resolves the effective (operator-owned) cap per actor and
    stamps it onto each item via ``build_batch_args(max_pending_by_actor=...)``;
    the batch tier admission-checks the aggregate before writing anything:
    an oversized batch raises ``MaxPendingExceededError`` with zero rows
    admitted (all-or-nothing, same contract as the single-enqueue path).
    Deterministic: one call, no timing.
    """
    schema = module_pg_schema.schema_name
    backend = make_backend(cron_settings(schema))
    actor_name = "rt_overshoot_backend_batch"
    args_list = build_batch_args(
        _items(_uncapped),
        _batch_id(),
        max_pending_by_actor={_uncapped.name: _CAP},
    )
    # Rewrite to the dedicated actor name so the count query is unambiguous,
    # keeping the carried cap on every item.
    from dataclasses import replace

    args_list = [replace(a, actor=actor_name) for a in args_list]
    assert all(a.max_pending == _CAP for a in args_list), (
        "setup: every item must carry the resolved cap for this to attack the batch tier"
    )

    with pytest.raises(MaxPendingExceededError):
        await backend.enqueue_batch(args_list, connection=clean_pg_conn)

    assert await count_jobs(clean_pg_conn, schema, actor_name) == 0, (
        "a refused batch must admit nothing: partial admission would be silent over-cap delivery"
    )


# ── PG attack 2: streaming batches bypass max_pending ─────────────────


@pytest.mark.integration
async def test_streaming_enqueue_batch_bypasses_max_pending(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """``JobsClient.enqueue_batch_streaming`` admits N > cap jobs with no error.

    Unlike ``enqueue_batch`` (aggregated pre-check), the streaming path
    discloses "max_pending NOT enforced" — this test proves the disclosure
    wrong if enforcement lands, or the bypass if it does not. The cap on
    the actor is real (``@actor(max_pending=2)``); an oversized stream
    must raise ``MaxPendingExceededError`` admitting nothing.
    """
    schema = module_pg_schema.schema_name
    backend = make_backend(cron_settings(schema))
    client = JobsClient(backend)

    with pytest.raises(MaxPendingExceededError):
        await client.enqueue_batch_streaming(iter(_items(_capped)), connection=clean_pg_conn)

    assert await count_jobs(clean_pg_conn, schema, _capped.name) == 0, (
        "a refused stream must admit nothing: partial admission would be silent over-cap delivery"
    )


# ── PG attack 3: COPY-fast batches bypass max_pending ─────────────────


@pytest.mark.integration
async def test_batch_fast_bypasses_max_pending(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """``JobsClient.enqueue_batch_fast`` must refuse N > cap jobs with no error.

    Same unchecked tier as the streaming path (COPY has no preflight at
    all); the actor's ``max_pending=2`` literal is carried onto the args by
    ``build_batch_args`` and the pre-COPY aggregate check must refuse the
    batch admitting nothing.
    """
    schema = module_pg_schema.schema_name
    backend = make_backend(cron_settings(schema))
    client = JobsClient(backend)

    with pytest.raises(MaxPendingExceededError):
        await client.enqueue_batch_fast(_items(_capped), connection=clean_pg_conn)

    assert await count_jobs(clean_pg_conn, schema, _capped.name) == 0, (
        "a refused COPY must admit nothing: partial admission would be silent over-cap delivery"
    )


# ── PG attack 4: sub-enqueuer conn batches bypass max_pending ─────────


@pytest.mark.integration
async def test_sub_enqueuer_conn_batch_bypasses_max_pending(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """``SubJobEnqueuer.enqueue_batch(connection=...)`` must refuse N > cap jobs.

    This path resolves the effective cap per actor but — unlike
    ``JobsClient.enqueue_batch`` — runs NO aggregated
    ``existing + batch_count > limit`` check before delegating to
    ``backend.enqueue_batch``. The backend tier admission-checks the
    carried caps itself, so the oversized batch raises
    ``MaxPendingExceededError`` admitting nothing.
    """
    schema = module_pg_schema.schema_name
    backend = make_backend(cron_settings(schema))
    enqueuer = SubJobEnqueuer(None, None, backend)

    with pytest.raises(MaxPendingExceededError):
        await enqueuer.enqueue_batch(_items(_capped), connection=clean_pg_conn)

    assert await count_jobs(clean_pg_conn, schema, _capped.name) == 0, (
        "a refused batch must admit nothing: partial admission would be silent over-cap delivery"
    )


# ── PG attack 5: concurrent single enqueues race the count ────────────


@pytest.mark.integration
async def test_concurrent_singles_overshoot_max_pending(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """Twenty concurrent single enqueues against ``max_pending=2`` overshoot.

    Each ``enqueue_with_conn`` counts ``pending + scheduled`` then INSERTs
    with no serializing lock; overlapping counts each see room and all
    INSERT. The per-actor jobs table must never hold more than the cap —
    anything above it is admitted load the operator explicitly shed.
    """
    import asyncio

    schema = module_pg_schema.schema_name
    backend = make_backend(cron_settings(schema))
    actor_name = "rt_overshoot_race"
    ref = _uncapped

    conns: list[asyncpg.Connection] = [
        await asyncpg.connect(module_pg_schema.pg_dsn) for _ in range(20)
    ]
    try:
        from dataclasses import replace

        from taskq._ids import new_job_id

        template = build_enqueue_args(ref, _Payload())
        template = replace(template, actor=actor_name, max_pending=_CAP)

        async def _one(i: int, conn: asyncpg.Connection) -> object:
            from dataclasses import replace as _replace

            args = _replace(template, payload={"value": i}, id=new_job_id())
            try:
                return await backend.enqueue_with_conn(conn, args)
            except MaxPendingExceededError as exc:
                return exc

        results = await asyncio.gather(
            *(_one(i, conn) for i, conn in enumerate(conns)),
            return_exceptions=True,
        )
        assert not any(
            isinstance(r, BaseException) and not isinstance(r, MaxPendingExceededError)
            for r in results
        ), (
            f"unexpected error type: {[type(r).__name__ for r in results if isinstance(r, BaseException)]}"
        )
        # Serialization is exact, not merely bounded: the per-actor lock
        # orders the twenty transactions, so precisely the first two see
        # room and the other eighteen are refused loudly — a silent drop
        # would pass the count below while hiding the missing refusal.
        admitted = sum(isinstance(r, JobRow) for r in results)
        refused = sum(isinstance(r, MaxPendingExceededError) for r in results)
        assert (admitted, refused) == (2, 18), (
            f"expected exactly 2 admissions and 18 loud refusals, got "
            f"{admitted} admissions and {refused} refusals"
        )
    finally:
        for conn in conns:
            await conn.close()

    pending = await count_jobs(clean_pg_conn, schema, actor_name)
    assert pending <= _CAP, (
        f"concurrent singles overshot max_pending={_CAP}: "
        f"{pending} pending jobs admitted for actor={actor_name!r}"
    )


# ── PG attack 6: autonomous atomic batches enforce per chunk ─────────


@pytest.mark.integration
async def test_atomic_batch_enforces_cap_per_chunk(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
    module_pg_pool: asyncpg.Pool,
) -> None:
    """``enqueue_batch_atomic`` chunks must admission-check against the shared txn.

    The autonomous atomic path consumes the iterable lazily in chunks
    inside ONE transaction. Each chunk's carried caps are checked on the
    transaction's connection, so chunk 1 (fits) commits nothing yet and
    chunk 2 (existing 2 + 2 new > cap 2) raises with the whole
    transaction rolled back: zero rows admitted, not a partial stream.
    """
    schema = module_pg_schema.schema_name
    backend = pool_backend(cron_settings(schema), module_pg_pool)
    actor_name = "rt_overshoot_atomic"
    args_list = build_batch_args(
        _items(_uncapped),
        _batch_id(),
        max_pending_by_actor={_uncapped.name: _CAP},
    )
    from dataclasses import replace

    args_list = [replace(a, actor=actor_name) for a in args_list]
    assert all(a.max_pending == _CAP for a in args_list), (
        "setup: every item must carry the resolved cap for this to attack the atomic tier"
    )

    with pytest.raises(MaxPendingExceededError):
        await backend.enqueue_batch_atomic(
            iter(args_list),
            batch_id=_batch_id(),
            queue="default",
            batch_row=None,
            finalizer_args=None,
            chunk_size=2,
        )

    assert await count_jobs(clean_pg_conn, schema, actor_name) == 0, (
        "a refused atomic batch must admit nothing: the single transaction rolls back every chunk"
    )


# ── PG pin: idempotency duplicates do not consume cap ───────────────


@pytest.mark.integration
async def test_batch_idempotency_duplicates_do_not_consume_cap(
    module_pg_schema: ModulePgSchema,
    clean_pg_conn: asyncpg.Connection,
) -> None:
    """Items that will dedupe must not count toward the cap.

    One stored row plus a batch of five where four share the stored
    (scope, key) pair: the four dedupe via ``ON CONFLICT`` and write
    nothing, so the aggregate admits (1 existing + 1 new <= cap 2).
    Counting them would refuse a batch that consumes a single slot —
    the single-enqueue path returns idempotency hits before any cap
    accounting, and the batch tier must match.
    """
    from taskq._ids import new_job_id

    schema = module_pg_schema.schema_name
    backend = make_backend(cron_settings(schema))
    actor_name = "rt_overshoot_idem"
    template = build_enqueue_args(
        _uncapped,
        _Payload(),
        max_pending=_CAP,
        identity_key=None,
        idempotency_key="idem-cap-discount",
        idempotency_scope="rt-overshoot",
    )
    from dataclasses import replace

    first = replace(template, actor=actor_name, id=new_job_id())
    await backend.enqueue_with_conn(clean_pg_conn, first)

    rest = [
        replace(template, actor=actor_name, id=new_job_id()) for _ in range(4)
    ]
    rows = await backend.enqueue_batch([first, *rest], connection=clean_pg_conn)

    assert len(rows) == 5
    assert await count_jobs(clean_pg_conn, schema, actor_name) == 1


# ── Unit pin: the one clean batch path ────────────────────────────────


async def test_regular_enqueue_batch_rejects_oversized_aggregate() -> None:
    """PIN (passes): ``JobsClient.enqueue_batch`` rejects a batch whose
    aggregate exceeds the cap via its aggregated pre-check.

    Documents the boundary of the finding: the non-streaming client batch
    path counts once (``existing + batch_count > limit``) and raises
    ``MaxPendingExceededError`` before the INSERT, so attacks 1-4 are gaps
    in the OTHER batch paths, not in this one.
    """
    backend = InMemoryBackend(clock=FakeClock(_START))
    client = JobsClient(backend)

    with pytest.raises(MaxPendingExceededError):
        await client.enqueue_batch(_items(_capped))
