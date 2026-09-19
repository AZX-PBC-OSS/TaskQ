"""Differential attacks on max_pending enforcement (single, batch, cap groups).

The cap semantics that must not drift: the single path refuses when the
stored pending+scheduled count has REACHED the carried cap; the batch path
admits when ``existing + admitted-items`` does not EXCEED the effective cap
(M1 semantics), partitions refusals per actor, and discounts idempotency
pairs that will dedupe instead of writing.
"""

from __future__ import annotations

import pytest

from taskq._ids import new_job_id
from taskq.backend._protocol import EnqueueArgs, IdempotencyKey
from taskq.exceptions import BatchMaxPendingExceededError, MaxPendingExceededError

from .test_rt_diff_harness import DiffSide, assert_mirror, run_differential

pytestmark = pytest.mark.integration


def _capped_args(
    side: DiffSide,
    token: str,
    actor: str,
    *,
    max_pending: int,
    idempotency_key: str | None = None,
) -> EnqueueArgs:
    """One capped EnqueueArgs in the side's clock domain, token-registered."""
    args = EnqueueArgs(
        id=new_job_id(),
        actor=actor,
        queue="default",
        payload={"value": 1},
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=side.ts(-1.0),
        max_pending=max_pending,
        idempotency_key=IdempotencyKey(idempotency_key) if idempotency_key is not None else None,
    )
    side.register_job_id(token, args.id)
    return args


async def _single_path_boundary(side: DiffSide) -> None:
    await side.enqueue("j1", max_pending=2)
    await side.enqueue("j2", max_pending=2)
    try:
        await side.enqueue("j3", max_pending=2)
        side.record("third", "admitted")
    except MaxPendingExceededError as exc:
        side.record(
            "third", ["MaxPendingExceededError", exc.actor, exc.current_count, exc.max_pending]
        )


async def test_diff_max_pending_single_boundary(pg_dsn: str) -> None:
    """Single-path cap: filling exactly to the cap admits; the next enqueue refuses."""
    mem, pg = await run_differential(_single_path_boundary, pg_dsn=pg_dsn)
    assert_mirror(
        "the single-path max_pending check refuses when the stored "
        "pending+scheduled count has reached the carried cap, with the same "
        "typed error and counts on both backends",
        mem,
        pg,
    )
    assert pg["records"]["third"] == ["MaxPendingExceededError", "test_actor", 2, 2]
    assert pg["status_counts"] == {"pending": 2}


async def _batch_partition_refusal(side: DiffSide) -> None:
    await side.enqueue("existing-a1", actor="actor_a", max_pending=2)
    batch = [
        _capped_args(side, "a2", "actor_a", max_pending=2),
        _capped_args(side, "a3", "actor_a", max_pending=2),
        _capped_args(side, "b1", "actor_b", max_pending=2),
    ]
    try:
        rows = await side.backend.enqueue_batch(batch)
        side.record("batch", "admitted-all")
        side.record("returned", [side.token_of(r.id) for r in rows])
    except BatchMaxPendingExceededError as exc:
        side.record(
            "batch",
            {
                "refused_actors": sorted(r.actor for r in exc.refusals),
                "refused_indices": dict(exc.refused_indices),
                "admitted_count": exc.admitted_count,
            },
        )
        side.record("returned", "raised")


async def test_diff_max_pending_batch_partition(pg_dsn: str) -> None:
    """Batch cap partition: the over-cap actor's items refuse as a group, the
    other actor's items still land, and the typed refusal raises AFTER."""
    mem, pg = await run_differential(
        _batch_partition_refusal,
        pg_dsn=pg_dsn,
        actors=("test_actor", "actor_a", "actor_b"),
    )
    assert_mirror(
        "the batch tier partitions cap admission per actor: over-cap actors' "
        "items are refused as a group, every other actor's items are stored, "
        "and BatchMaxPendingExceededError raises naming them",
        mem,
        pg,
    )
    assert pg["records"]["batch"] == {
        "refused_actors": ["actor_a"],
        "refused_indices": {"actor_a": [0, 1]},
        "admitted_count": 1,
    }
    assert pg["status_counts"] == {"pending": 2}  # existing-a1 + admitted b1


async def _batch_operator_override(side: DiffSide) -> None:
    # Stored operator override (actor_config.max_pending = 5) beats the
    # carried literal (2): three items admit where the literal would refuse.
    await side.register_actor_config(actor="actor_o", max_pending=5)
    batch = [
        _capped_args(side, "o1", "actor_o", max_pending=2),
        _capped_args(side, "o2", "actor_o", max_pending=2),
        _capped_args(side, "o3", "actor_o", max_pending=2),
    ]
    rows = await side.backend.enqueue_batch(batch)
    side.record("returned", [side.token_of(r.id) for r in rows])


async def test_diff_max_pending_batch_operator_override(pg_dsn: str) -> None:
    """The stored operator override wins over the carried literal in the batch cap."""
    mem, pg = await run_differential(
        _batch_operator_override,
        pg_dsn=pg_dsn,
        actors=("test_actor",),
    )
    assert_mirror(
        "the batch tier's effective cap resolves the stored actor_config "
        "override over the carried literal, identically on both backends",
        mem,
        pg,
    )
    assert pg["records"]["returned"] == ["o1", "o2", "o3"]
    assert pg["status_counts"] == {"pending": 3}


async def _batch_idempotency_discount(side: DiffSide) -> None:
    await side.enqueue("stored", idempotency_key="key-d", max_pending=2)
    batch = [
        _capped_args(side, "dup-1", "test_actor", max_pending=2, idempotency_key="key-d"),
        _capped_args(side, "dup-2", "test_actor", max_pending=2, idempotency_key="key-d"),
    ]
    rows = await side.backend.enqueue_batch(batch)
    side.record("returned", [side.token_of(r.id) for r in rows])


async def test_diff_max_pending_batch_idempotency_discount(pg_dsn: str) -> None:
    """Pairs that will dedupe consume no capacity: a capped batch of pure
    idempotent retries is admitted, not refused."""
    mem, pg = await run_differential(_batch_idempotency_discount, pg_dsn=pg_dsn)
    assert_mirror(
        "the batch cap discounts items whose (scope, key) pair is already "
        "stored or repeated in-batch - they dedupe instead of writing - on "
        "both backends",
        mem,
        pg,
    )
    assert pg["records"]["returned"] == ["stored", "stored"]
    assert pg["status_counts"] == {"pending": 1}


async def _batch_fast_duplicate_abort(side: DiffSide) -> None:
    await side.enqueue("stored", idempotency_key="key-f")
    batch: list[EnqueueArgs] = [
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=side.ts(-1.0),
        ),
        EnqueueArgs(
            id=new_job_id(),
            actor="test_actor",
            queue="default",
            payload={"value": 1},
            max_attempts=3,
            retry_kind="transient",
            scheduled_at=side.ts(-1.0),
            idempotency_key=IdempotencyKey("key-f"),
        ),
    ]
    side.register_job_id("fresh", batch[0].id)
    side.register_job_id("dupe", batch[1].id)
    try:
        count = await side.backend.enqueue_batch_fast(batch)
        side.record("fast", count)
    except Exception as exc:  # Why: the typed outcome IS the observable; recorded by name.
        side.record("fast", type(exc).__name__)


async def test_diff_enqueue_batch_fast_duplicate_aborts(pg_dsn: str) -> None:
    """The COPY tier has no ON CONFLICT arbiter: a stored pair aborts the
    whole fast batch on PG; the mirror must abort identically, not dedupe."""
    mem, pg = await run_differential(_batch_fast_duplicate_abort, pg_dsn=pg_dsn)
    assert_mirror(
        "enqueue_batch_fast aborts the entire batch on a duplicate "
        "(idempotency_scope, idempotency_key) pair with the same typed error "
        "on both backends; nothing is written",
        mem,
        pg,
    )
    assert pg["status_counts"] == {"pending": 1}  # only the pre-stored row
