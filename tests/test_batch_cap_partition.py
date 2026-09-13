"""#149 — bulk-tier cap enforcement partitions admission per actor.

``_enforce_batch_max_pending`` (PG) / ``_check_batch_max_pending``
(InMemory) refused the ENTIRE enqueue_batch / enqueue_batch_fast call
when ANY single actor's group exceeded its cap — all-or-nothing across
actors. A parent enqueuing a mixed-actor sub-batch where ONE child actor
is capped lost the whole call: nothing enqueued for anyone.

The evolved contract (vendor prior art: pgqueuer's dequeue capacity gates
bind per entrypoint — the multi-entrypoint statement admits the
entrypoints with free room instead of failing wholesale; river's
InsertMany shows whole-batch atomicity is the norm only where no
per-actor constraint exists): an over-cap actor's items are refused as a
group (never partially filled — the single path refuses a capped enqueue
outright, and filling "up to" the cap would admit items whose ordering
the caller never chose), every other actor's items are admitted, and the
refusal surfaces AFTER the admitted items are inserted as
:class:`~taskq.exceptions.BatchMaxPendingExceededError` — the bulk-tier
sibling of :class:`~taskq.exceptions.PartialBatchError`, which is the
house pattern for partial batch admission (succeeded count + failed
indices + per-failure exceptions).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import asyncpg
import pytest
from pydantic import BaseModel

from taskq import actor
from taskq.backend._enqueue import _enqueue_batch
from taskq.batch import EnqueueItem
from taskq.client._args import build_enqueue_args
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.client._jobs import JobsClient
from taskq.exceptions import (
    BatchMaxPendingExceededError,
    MaxPendingExceededError,
    PartialBatchError,
)
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

from .test_enqueue_coverage import (
    _SCHEMA_LABEL,
    _SQL,
    _FakeEnqueueConn,
    _full_record,
    _Record,
)

_START = datetime(2025, 1, 1, tzinfo=UTC)


class _Payload(BaseModel):
    value: int = 0


@actor(name="cap_partition_healthy")
async def _healthy_ref(_payload: _Payload) -> None:
    pass


@actor(name="cap_partition_capped", max_pending=1)
async def _capped_ref(_payload: _Payload) -> None:
    pass


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


def _args(ref: Any, value: int = 0, *, max_pending: int | None = None) -> Any:
    return build_enqueue_args(
        ref,
        _Payload(value=value),
        max_pending=max_pending if max_pending is not None else ref.max_pending,
    )


async def _seed_one_pending(backend: InMemoryBackend, ref: Any) -> None:
    """One existing pending job for *ref*'s actor (its first slot)."""
    await backend.enqueue(_args(ref, 99))


def _stored_count(backend: InMemoryBackend, actor_name: str) -> int:
    return sum(1 for row in backend._jobs.values() if row.actor == actor_name)


# ── Backend tier: enqueue_batch partitions per actor ────────────────────


async def test_backend_enqueue_batch_partitions_admission_per_actor() -> None:
    """Mixed batch, one capped actor: healthy actors' items are inserted,
    the capped actor's items are refused with attribution, and the refusal
    raises AFTER the admitted rows are stored."""
    backend = _make_backend()
    await _seed_one_pending(backend, _capped_ref)

    args_list = [
        _args(_healthy_ref, 0),
        _args(_capped_ref, 1),
        _args(_healthy_ref, 2),
        _args(_capped_ref, 3),
        _args(_healthy_ref, 4),
    ]

    with pytest.raises(BatchMaxPendingExceededError) as exc_info:
        await backend.enqueue_batch(args_list)

    err = exc_info.value
    # Attribution: one refusal per over-cap actor, indices into the
    # caller's list, and the admitted count — everything a targeted
    # retry of only the refused items needs.
    assert [r.actor for r in err.refusals] == [_capped_ref.name]
    assert err.refusals[0].current_count == 1
    assert err.refusals[0].max_pending == 1
    assert err.refused_indices == {_capped_ref.name: [1, 3]}
    assert err.admitted_count == 3
    # The healthy actor's items were admitted despite the sibling refusal.
    assert _stored_count(backend, _healthy_ref.name) == 3
    # The capped actor admitted nothing past its single stored slot.
    assert _stored_count(backend, _capped_ref.name) == 1


async def test_backend_enqueue_batch_all_refused_admits_nothing() -> None:
    """A batch where every item belongs to over-cap actors admits nothing
    and still raises the typed batch refusal (not the single-path
    MaxPendingExceededError)."""
    backend = _make_backend()
    await _seed_one_pending(backend, _capped_ref)

    with pytest.raises(BatchMaxPendingExceededError) as exc_info:
        await backend.enqueue_batch([_args(_capped_ref, 0), _args(_capped_ref, 1)])

    assert exc_info.value.admitted_count == 0
    assert exc_info.value.refused_indices == {_capped_ref.name: [0, 1]}
    assert _stored_count(backend, _capped_ref.name) == 1


async def test_backend_enqueue_batch_exact_fill_is_admitted() -> None:
    """M1 boundary survives the partition: a batch that fills an actor
    exactly to its cap is admitted (existing + batch > cap refuses)."""
    backend = _make_backend()
    # Cap 2 with one stored row: one more item fills it exactly (1 + 1 > 2
    # is False), so the batch is admitted.
    await backend.enqueue(_args(_capped_ref, 99, max_pending=2))

    rows = await backend.enqueue_batch([_args(_capped_ref, 0, max_pending=2)])

    assert len(rows) == 1
    assert _stored_count(backend, _capped_ref.name) == 2


async def test_backend_enqueue_batch_discounts_stored_idempotency_pairs() -> None:
    """Pure-retry batches at a full cap are admitted: items whose
    (scope, key) pair is already stored dedupe instead of writing, so
    the aggregate admission discounts them — parity with the PG tier's
    ON CONFLICT discount. The old in-memory mirror re-checked caps per
    item WITHOUT the discount and refused mid-batch."""
    backend = _make_backend()
    seed = build_enqueue_args(
        _capped_ref,
        _Payload(value=99),
        idempotency_key="retry-key",
        idempotency_scope="",
    )
    seeded = await backend.enqueue(seed)
    assert _stored_count(backend, _capped_ref.name) == 1

    retries = [
        build_enqueue_args(
            _capped_ref,
            _Payload(value=i),
            idempotency_key="retry-key",
            idempotency_scope="",
        )
        for i in range(2)
    ]

    rows = await backend.enqueue_batch(retries)

    assert [row.id for row in rows] == [seeded.id, seeded.id]
    assert _stored_count(backend, _capped_ref.name) == 1


# ── Backend tier: enqueue_batch_fast partitions per actor ───────────────


async def test_backend_enqueue_batch_fast_partitions_admission_per_actor() -> None:
    """The COPY tier gets the same per-actor partition: within-cap actors'
    rows are written, the over-cap actor's items raise the typed refusal."""
    backend = _make_backend()
    await _seed_one_pending(backend, _capped_ref)

    args_list = [
        _args(_healthy_ref, 0),
        _args(_capped_ref, 1),
        _args(_healthy_ref, 2),
    ]

    with pytest.raises(BatchMaxPendingExceededError) as exc_info:
        await backend.enqueue_batch_fast(args_list)

    assert exc_info.value.refused_indices == {_capped_ref.name: [1]}
    assert exc_info.value.admitted_count == 2
    assert _stored_count(backend, _healthy_ref.name) == 2
    assert _stored_count(backend, _capped_ref.name) == 1


# ── Client tier: JobsClient.enqueue_batch ───────────────────────────────


async def test_client_enqueue_batch_mixed_actor_partition() -> None:
    """JobsClient.enqueue_batch no longer aborts the whole call at its
    pre-check: the backend is the single enforcement point, the healthy
    actors' items are committed, and the typed refusal reaches the caller
    with item indices into the caller's list."""
    backend = _make_backend()
    client = JobsClient(backend=backend, clock=FakeClock(start=_START))
    await _seed_one_pending(backend, _capped_ref)

    items = [
        EnqueueItem(actor_ref=_healthy_ref, payload=_Payload(value=0)),
        EnqueueItem(actor_ref=_capped_ref, payload=_Payload(value=1)),
        EnqueueItem(actor_ref=_healthy_ref, payload=_Payload(value=2)),
    ]

    with pytest.raises(BatchMaxPendingExceededError) as exc_info:
        await client.enqueue_batch(items)

    assert exc_info.value.refused_indices == {_capped_ref.name: [1]}
    assert exc_info.value.admitted_count == 2
    assert _stored_count(backend, _healthy_ref.name) == 2


# ── Client tier: SubJobEnqueuer converts to the house partial error ────


async def test_sub_enqueuer_conn_batch_converts_to_partial_batch_error() -> None:
    """The parent-facing sub-job API surfaces the partition through its
    established PartialBatchError shape (the same type its no-connection
    fallback already raises), so actor code handles one error contract
    across all connection modes."""
    backend = _make_backend()
    enqueuer = SubJobEnqueuer(None, None, backend)
    await _seed_one_pending(backend, _capped_ref)

    items = [
        EnqueueItem(actor_ref=_healthy_ref, payload=_Payload(value=0)),
        EnqueueItem(actor_ref=_capped_ref, payload=_Payload(value=1)),
        EnqueueItem(actor_ref=_healthy_ref, payload=_Payload(value=2)),
        EnqueueItem(actor_ref=_capped_ref, payload=_Payload(value=3)),
    ]

    # Why a dummy connection: InMemoryBackend ignores it; the point is to
    # route through the connection arm of SubJobEnqueuer.enqueue_batch.
    conn = cast_asyncpg_conn(object())

    with pytest.raises(PartialBatchError) as exc_info:
        await enqueuer.enqueue_batch(items, connection=conn)

    err = exc_info.value
    assert err.succeeded_count == 2
    assert err.total == 4
    assert [idx for idx, _ in err.failed_items] == [1, 3]
    for _, item_exc in err.failed_items:
        assert isinstance(item_exc, MaxPendingExceededError)
        assert item_exc.actor == _capped_ref.name
        assert item_exc.current_count == 1
        assert item_exc.max_pending == 1
    assert _stored_count(backend, _healthy_ref.name) == 2
    assert _stored_count(backend, _capped_ref.name) == 1


def cast_asyncpg_conn(obj: object) -> asyncpg.Connection:
    return obj  # type: ignore[return-value]  # Why: InMemoryBackend ignores the connection object; the cast only satisfies the typed signature.


# ── PG module tier: partition on a caller-owned open transaction ────────


class _CapturingConn(_FakeEnqueueConn):
    """Records every fetch's (sql, args) so a test can assert exactly
    which items reached the batch INSERT."""

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args: object) -> list[_Record]:
        self.fetch_calls.append((sql, args))
        return await super().fetch(sql, *args)


def _pg_mixed_args() -> list[Any]:
    from taskq._ids import new_job_id

    healthy = replace(_args(_healthy_ref, 0), actor="cap_partition_pg_healthy")
    capped = replace(_args(_capped_ref, 1), actor="cap_partition_pg_capped")
    healthy2 = replace(_args(_healthy_ref, 2), actor="cap_partition_pg_healthy")
    # Distinct ids so the returning-records join is unambiguous.
    healthy = replace(healthy, id=new_job_id())
    capped = replace(capped, id=new_job_id())
    healthy2 = replace(healthy2, id=new_job_id())
    return [healthy, capped, healthy2]


async def test_pg_enqueue_batch_inserts_admitted_items_only() -> None:
    """The PG bulk tier partitions on the inserting connection: the
    unnest INSERT receives ONLY the within-cap actors' arrays, and the
    typed refusal raises after those items are inserted."""
    args_list = _pg_mixed_args()
    admitted_ids = [args_list[0].id, args_list[2].id]

    def _rec(args: Any) -> _Record:
        return _Record({**_full_record(job_id=args.id), "actor": args.actor})

    conn = _CapturingConn(
        fetch_map={
            # count_pending_jobs: the capped actor holds one row.
            "GROUP BY actor": [_Record({"actor": args_list[1].actor, "cnt": 1})],
            # The batch INSERT's RETURNING + the follow-up full-row fetch
            # both see only the admitted items.
            "RETURNING id, actor": [_rec(args_list[0]), _rec(args_list[2])],
            "id = ANY($1::uuid[])": [_rec(args_list[0]), _rec(args_list[2])],
        }
    )

    with pytest.raises(BatchMaxPendingExceededError) as exc_info:
        await _enqueue_batch(
            None,
            _SQL,
            _SCHEMA_LABEL,
            args_list,
            connection=conn,  # type: ignore[arg-type]  # Why: fake conn models a caller-owned open transaction
        )

    err = exc_info.value
    assert err.refused_indices == {args_list[1].actor: [1]}
    assert err.admitted_count == 2
    # One aggregated count query for the whole (mixed-actor) batch — the
    # per-actor admission never degrades to one count per actor (the
    # unit-tier pin for the aggregation TI7's integration test used to
    # cover at the client layer).
    count_calls = [c for c in conn.fetch_calls if "GROUP BY actor" in c[0]]
    assert len(count_calls) == 1
    # The INSERT statement ran exactly once and carried only the admitted
    # items' ids — the refused actor's item never reached the statement.
    insert_calls = [c for c in conn.fetch_calls if "RETURNING id, actor" in c[0]]
    assert len(insert_calls) == 1
    assert list(insert_calls[0][1][0]) == admitted_ids


async def test_pg_enqueue_batch_abort_mode_refuses_whole_call() -> None:
    """refuse_whole_batch_on_cap=True (the enqueue_batch_atomic chunk
    arm) keeps the legacy all-or-nothing contract: MaxPendingExceededError
    raises BEFORE any INSERT, so the caller's transaction rolls back
    everything."""
    args_list = _pg_mixed_args()

    conn = _CapturingConn(
        fetch_map={
            "GROUP BY actor": [_Record({"actor": args_list[1].actor, "cnt": 1})],
        }
    )

    with pytest.raises(MaxPendingExceededError) as exc_info:
        await _enqueue_batch(
            None,
            _SQL,
            _SCHEMA_LABEL,
            args_list,
            connection=conn,  # type: ignore[arg-type]  # Why: fake conn models a caller-owned open transaction
            refuse_whole_batch_on_cap=True,
        )

    assert exc_info.value.actor == args_list[1].actor
    # Nothing reached the INSERT: the refusal fired at admission time.
    assert not any("RETURNING id, actor" in c[0] for c in conn.fetch_calls)
